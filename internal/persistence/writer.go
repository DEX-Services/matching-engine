package persistence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dex/matching-engine/internal/events"
	"github.com/dex/matching-engine/internal/models"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

const (
	// maxBatchSize caps events per transaction: ~2.5s of peak market-maker
	// churn. Large enough to amortize one commit/fsync across many events;
	// small enough that a single transaction stays well under lock and WAL
	// limits, and that redelivery after a failure stays cheap.
	maxBatchSize = 200
	// defaultConsumerGroup is the original group id. It is overridable via
	// KAFKA_WRITER_GROUP because this database and topic are SHARED with the
	// hosted deployment, which runs its own engine + writer: each
	// environment must pin a DISTINCT group (they otherwise split the single
	// partition between them, each seeing only part of the stream), and a
	// group that has already skip-chewed retained history should be reused
	// rather than re-created. Correctness under any split is preserved by
	// the claim protocol in applyEvent (concurrent writers converge on the
	// same rows instead of double-deriving them).
	defaultConsumerGroup = "postgres-writer"
)

// Writer consumes events from Kafka and writes them to Postgres.
// It is the sole reader of the TopicEvents topic in its consumer group
// (KAFKA_WRITER_GROUP, default "postgres-writer").
// On error, messages are retried until they succeed — Kafka offsets are only
// committed after the events persist, making this effectively an
// at-least-once writer. The raw-event claim (applyEvent) makes replays
// no-ops, so at-least-once yields exactly-once DB state.
//
// Batching: Run drains up to maxBatchSize messages per fetch and applies them
// in ONE transaction per batch instead of one transaction per event. At ~80
// events/s (4 MM desks requoting every second) the old per-event loop issued
// ~80 commits/s; at 10x that rate it would hit WAL/fsync limits while Kafka
// consumer lag grew unboundedly (redelivery loop). A batch commit amortizes a
// single fsync across up to 200 events — roughly an order of magnitude less
// DB work on the same stream. If a batch fails, each event is retried
// individually so at-least-once delivery holds up to the first event that
// cannot persist; Kafka redelivers the rest.
//
// Bootstrap: the group starts at FirstOffset (earliest retained) by default
// for a FRESH consumer group (one with no committed offset yet); a group
// that has committed offsets before simply resumes from there, as any Kafka
// consumer does. There is deliberately no time-based "skip messages older
// than X" heuristic here anymore (removed 2026-09-14 — see
// RECONCILE-BALANCE-WIPE-BUG.md's sibling incident,
// SEQUENCE-RESET-HISTORY-LOSS-BUG.md, or ask about "writer skipPreSession
// discarded real backlog" from that date for the full incident writeup).
// That heuristic committed (permanently discarded) any message whose
// broker timestamp predated this process's own start time, on the
// assumption that anything older must already have been persisted by a
// previous writer session. That assumption is only true if the previous
// session had FULLY drained Kafka before dying — restart it under real
// backlog (a burst of trading activity the writer hadn't yet caught up on,
// or several restarts close together each leaving a bit more lag than the
// last) and the "old" messages it skips were never persisted by anyone;
// they are gone the moment their offset is committed. Confirmed live: one
// restart discarded 17,291 backlogged events this way, including a real,
// executed customer trade that never appeared in order_history/fills
// anywhere. The claim protocol in applyEvent (ON CONFLICT ... DO NOTHING
// RETURNING true) already makes reprocessing a genuine duplicate a safe
// no-op — that mechanism, not a timestamp guess, is what should decide
// whether an event has already been persisted. Removing the time-based
// skip means a FRESH consumer group again replays the full retained
// history on first boot (as the writer originally did, per the commit that
// introduced FirstOffset — "fix Kafka offset" — before this heuristic was
// added purely as a performance shortcut and inadvertently broke
// correctness): slower to reach "caught up" once, for a brand-new group
// name, but nothing genuine is ever thrown away. Set
// KAFKA_WRITER_START_OFFSET=latest to accept that tradeoff deliberately
// for a fresh group (instant catch-up, skipping pre-existing retained
// history on purpose) — committed-offset restarts are unaffected either
// way, exactly as before.
type Writer struct {
	reader        *kafka.Reader
	pool          *pgxpool.Pool
	log           *slog.Logger
	persisted     atomic.Int64 // events persisted this session, for the status log
	bootDone      atomic.Bool  // set once the first message this session is processed
	statusStarted atomic.Bool  // guards the one-shot 30s status ticker
}

// NewWriter creates a Kafka→Postgres writer.
func NewWriter(pool *pgxpool.Pool) (*Writer, error) {
	host := os.Getenv("KAFKA_HOST")
	port := os.Getenv("KAFKA_PORT")
	if host == "" || port == "" {
		return nil, fmt.Errorf("KAFKA_HOST and KAFKA_PORT must be set")
	}

	tlsCfg, err := events.KafkaTLSConfig()
	if err != nil {
		return nil, err
	}

	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
		TLS:       tlsCfg,
		SASLMechanism: plain.Mechanism{
			Username: os.Getenv("KAFKA_USER"),
			Password: os.Getenv("KAFKA_PASSWORD"),
		},
	}

	// MinBytes/MaxWait tuning history: this was originally MinBytes=512KB,
	// MaxWait=1s, on the theory that MinBytes=1 made the broker answer every
	// fetch as soon as a single byte was ready, wasting round-trips during a
	// large backlog replay. In practice this made ordinary LIVE latency far
	// worse than intended: confirmed live 2026-09-14 (see
	// RECONCILE-BALANCE-WIPE-BUG.md and SEQUENCE-RESET-HISTORY-LOSS-BUG.md
	// for the same incident session) — a freshly placed order sat in a
	// consumer lag that only drained in ~200-message bursts roughly every
	// 90 SECONDS, not the ~1s MaxWait should have bounded this to. A
	// 512KB minimum on a topic whose messages are a few hundred bytes each
	// means the broker holds the fetch open far longer than MaxWait alone
	// would suggest is possible for kafka-go's batching semantics on this
	// specific hosted (Aiven) broker. Lowered MinBytes to 1KB (a handful of
	// messages, not a single byte) and MaxWait to 250ms: still batches
	// reasonably under load (Run's own 50ms opportunistic drain window on
	// top of this still coalesces a busy stream into large transactions),
	// but live orders now reach order_history/fills within roughly the
	// MaxWait bound instead of an unbounded, silently-growing wait.
	//
	// StartOffset applies only when the group has no committed offset yet.
	// Default is FirstOffset (earliest retained) — a fresh group replays
	// full retained history rather than skipping it (see the removed
	// timestamp-based skip in this file's git history for why that must
	// never silently discard real backlog); once the group has committed
	// offsets, restarts resume from there as before. Set
	// KAFKA_WRITER_START_OFFSET=latest to skip the replay on fresh groups
	// (instant catch-up; only sensible when losing pre-boot history on a
	// FRESH group is acceptable — committed-offset restarts are unaffected).
	startOffset := kafka.FirstOffset
	if strings.EqualFold(strings.TrimSpace(os.Getenv("KAFKA_WRITER_START_OFFSET")), "latest") {
		startOffset = kafka.LastOffset
	}
	group := os.Getenv("KAFKA_WRITER_GROUP")
	if group == "" {
		group = defaultConsumerGroup
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{fmt.Sprintf("%s:%s", host, port)},
		Topic:          events.TopicEvents,
		GroupID:        group,
		Dialer:         dialer,
		MinBytes:       1 << 10,  // 1 KB per fetch
		MaxBytes:       10 << 20, // 10 MB
		MaxWait:        250 * time.Millisecond,
		CommitInterval: 0, // manual commits only (CommitMessages in Run)
		StartOffset:    startOffset,
	})

	return &Writer{reader: reader, pool: pool, log: slog.Default()}, nil
}

// Run starts the consume-and-write loop. Call in a dedicated goroutine.
func (w *Writer) Run(ctx context.Context) {
	// Periodic status line: confirms liveness and surfaces consumer lag
	// (how far behind the live stream this writer currently is) when
	// investigating a quiet stream or a slow catch-up after a restart with
	// real backlog.
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				st := w.reader.Stats()
				w.log.Info("writer status",
					"persisted", w.persisted.Load(),
					"caught_up", w.bootDone.Load(),
					"fetches", st.Fetches,
					"msgs", st.Messages,
					"lag", st.Lag)
			}
		}
	}()

	for {
		// Block for the first message, then opportunistically drain up to
		// maxBatchSize-1 more within a short window: busy periods batch
		// heavily, idle periods commit promptly (≤50ms added latency).
		first, err := w.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			w.log.Error("kafka read error", "error", err)
			time.Sleep(time.Second) // back-off
			continue
		}
		if !w.bootDone.Swap(true) {
			w.log.Info("event writer started consuming")
		}

		msgs := make([]kafka.Message, 0, maxBatchSize)
		msgs = append(msgs, first)

		drainCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		for len(msgs) < maxBatchSize {
			msg, err := w.reader.FetchMessage(drainCtx)
			if err != nil {
				break
			}
			msgs = append(msgs, msg)
		}
		cancel()
		if len(msgs) == 0 {
			continue
		}

		// Unmarshal outside the transaction. A corrupt message is committed
		// and skipped permanently (logged): nothing was ever written for it
		// and a replay would fail identically, so wedging the whole stream
		// on it buys nothing — the same policy the OutboxSweeper applies to
		// corrupt rows.
		evts := make([]*models.Event, 0, len(msgs))
		valid := make([]kafka.Message, 0, len(msgs))
		for _, msg := range msgs {
			var evt models.Event
			if err := json.Unmarshal(msg.Value, &evt); err != nil {
				w.log.Error("unmarshal event, skipping", "offset", msg.Offset, "error", err)
				if cerr := w.reader.CommitMessages(ctx, msg); cerr != nil && ctx.Err() == nil {
					w.log.Error("kafka commit error", "error", cerr)
				}
				continue
			}
			evts = append(evts, &evt)
			valid = append(valid, msg)
		}
		if len(evts) == 0 {
			continue
		}

		if err := w.persistBatch(ctx, evts); err == nil {
			w.persisted.Add(int64(len(evts)))
			if err := w.reader.CommitMessages(ctx, valid...); err != nil && ctx.Err() == nil {
				w.log.Error("kafka commit error", "error", err)
			}
			continue
		}
		w.log.Warn("batch persist failed, writing event-by-event", "size", len(evts), "error", err)

		// Fallback: event-by-event, each retried until it succeeds, with
		// offsets committed in order as events persist. This keeps the
		// at-least-once boundary exact under every failure mode: a Postgres
		// outage retries in place (nothing is ever skipped) and a crash
		// mid-batch replays exactly the unpersisted tail. Committing each
		// event individually also isolates the position of any failure.
		for i, evt := range evts {
			for attempt := 0; ; attempt++ {
				perr := persist(ctx, w.pool, evt)
				if perr == nil {
					w.persisted.Add(1)
					break
				}
				if attempt == 0 || attempt%20 == 0 {
					w.log.Error("persist event, retrying", "symbol", evt.Symbol, "seq", evt.SequenceNumber, "attempt", attempt, "error", perr)
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(500 * time.Millisecond):
				}
			}
			if cerr := w.reader.CommitMessages(ctx, valid[i]); cerr != nil && ctx.Err() == nil {
				w.log.Error("kafka commit error", "error", cerr)
			}
		}
	}
}

// persistBatch applies a batch of events in one transaction. Statement order
// per event matches persist(): raw event row first (the claim — see
// applyEvent), then order/trade/funding/pnl.
func (w *Writer) persistBatch(ctx context.Context, evts []*models.Event) error {
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	for _, evt := range evts {
		stmts, err := eventStmts(evt)
		if err != nil {
			return err
		}
		if err := applyEvent(ctx, tx, evt, stmts); err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

// applyEvent runs the claim statement for evt and, if it won, the derived
// statements. Shared by the batch path and persistTx (single-event path).
//
// Dual-writer safety: this cloud Postgres/Kafka pair is shared with the
// hosted deployment, which runs its own engine + writer on the same topic.
// The raw events upsert doubles as an atomic claim: it carries RETURNING,
// and derived statements run only when THIS transaction inserted the raw row.
// On a conflict — the event was already persisted by the other writer or an
// earlier replay — the derived inserts are skipped: orders/trades are
// idempotent anyway, but funding_payments/realized_pnl are blind inserts, so
// without the claim two writers would double-pay. With it, concurrent
// writers converge instead of duplicating.
//
// KNOWN BUG (found 2026-09-14, not yet fixed): this claim's uniqueness key
// is (evt.Symbol, evt.SequenceNumber), but matching.Engine.seq (see its
// comment) restarts at 0 on every engine process boot instead of being
// restored from Postgres. After a restart, a genuinely NEW event can be
// assigned a sequence number that collides with an old, already-claimed row
// from before the restart — this function then sees ErrNoRows, assumes
// "already persisted", and returns nil WITHOUT writing that event's
// order/trade rows, silently. The trade still settles correctly (balances
// are unaffected — this is a Kafka/Postgres-side gap only), but it vanishes
// from order_history/fills. Confirmed live: a filled SPOT sell order was
// lost this way. Fix belongs in matching.Engine's startup (restore seq from
// MAX(sequence_number) per symbol), not here.
func applyEvent(ctx context.Context, tx pgx.Tx, evt *models.Event, stmts []eventStmt) error {
	var claimed bool
	err := tx.QueryRow(ctx, stmts[0].sql+" RETURNING true", stmts[0].args...).Scan(&claimed)
	if errors.Is(err, pgx.ErrNoRows) {
		// Raw row already exists: another writer (the hosted deployment
		// shares this database) or a replay persisted this event. Skip the
		// derived inserts — they were applied by whoever made the claim.
		return nil
	}
	if err != nil {
		return fmt.Errorf("claim event %s/%d (%s): %w", evt.Symbol, evt.SequenceNumber, evt.Type, err)
	}
	for _, st := range stmts[1:] {
		if _, err := tx.Exec(ctx, st.sql, st.args...); err != nil {
			return fmt.Errorf("apply event %s/%d (%s): %w", evt.Symbol, evt.SequenceNumber, evt.Type, err)
		}
	}
	return nil
}

// eventStmt is one statement to apply for an event, with its arguments.
type eventStmt struct {
	sql  string
	args []any
}

// eventStmts decomposes evt into its SQL statements with arguments. Extracted
// from the original persist() so the batch path, the single-event path, and
// the OutboxSweeper apply byte-identical, idempotent SQL. The first statement
// is always the raw events upsert (the claim — see applyEvent).
func eventStmts(evt *models.Event) ([]eventStmt, error) {
	payload, err := json.Marshal(evt)
	if err != nil {
		return nil, err
	}
	stmts := make([]eventStmt, 0, 2)

	// Upsert the raw event row (idempotent via unique index).
	stmts = append(stmts, eventStmt{
		sql: `INSERT INTO events (symbol, market, sequence_number, type, payload)
		      VALUES ($1, $2, $3, $4, $5)
		      ON CONFLICT (symbol, sequence_number) DO NOTHING`,
		args: []any{evt.Symbol, evt.Market, evt.SequenceNumber, string(evt.Type), payload},
	})

	// Upsert order if the event carries one.
	if evt.Order != nil {
		o := evt.Order
		stmts = append(stmts, eventStmt{
			sql: `INSERT INTO orders (id, client_order_id, account_id, symbol, market, side, type,
			                    time_in_force, price, quantity, filled, status, reject_reason, created_at, updated_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
			ON CONFLICT (id) DO UPDATE SET
			    filled        = EXCLUDED.filled,
			    status        = EXCLUDED.status,
			    reject_reason = EXCLUDED.reject_reason,
			    updated_at    = EXCLUDED.updated_at`,
			args: []any{o.ID, o.ClientOrderID, o.AccountID, o.Symbol, string(o.Market),
				string(o.Side), string(o.Type), string(o.TimeInForce),
				o.Price, o.Quantity, o.Filled, string(o.Status), nullableString(o.RejectReason), o.CreatedAt, o.UpdatedAt},
		})
	}

	// Insert trade if the event carries one.
	if evt.Trade != nil {
		t := evt.Trade
		stmts = append(stmts, eventStmt{
			sql: `INSERT INTO trades (id, symbol, market, maker_order_id, taker_order_id,
			                    maker_side, price, quantity, maker_fee_paid, taker_fee_paid, executed_at, sequence_number)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
			ON CONFLICT (id) DO NOTHING`,
			args: []any{t.ID, t.Symbol, string(t.Market), t.MakerOrderID, t.TakerOrderID,
				string(t.MakerSide), t.Price, t.Quantity, t.MakerFeePaid, t.TakerFeePaid, t.ExecutedAt, t.SequenceNumber},
		})
	}

	// Insert funding payment if the event carries one.
	if evt.Funding != nil {
		f := evt.Funding
		stmts = append(stmts, eventStmt{
			sql:  `INSERT INTO funding_payments (account_id, symbol, rate, amount) VALUES ($1, $2, $3, $4)`,
			args: []any{f.AccountID, f.Symbol, f.Rate, f.Payment},
		})
	}

	// Insert realized PnL if the event carries one.
	if evt.RealizedPnl != nil {
		p := evt.RealizedPnl
		stmts = append(stmts, eventStmt{
			sql:  `INSERT INTO realized_pnl (account_id, symbol, closed_qty, pnl, margin_returned, is_liquidation) VALUES ($1, $2, $3, $4, $5, $6)`,
			args: []any{p.AccountID, p.Symbol, p.ClosedQty, p.Pnl, p.MarginReturned, p.IsLiquidation},
		})
	}

	return stmts, nil
}

// persist writes one event's rows (order/trade/funding/pnl, plus the raw
// event itself) in a single transaction. Shared by the Writer's per-event
// fallback path and OutboxSweeper (the fallback path for events a broker
// outage kept out of Kafka entirely — it must stay all-or-nothing per row so
// one poison outbox row can't wedge the sweep) so both apply identical,
// idempotent upsert logic.
func persist(ctx context.Context, pool *pgxpool.Pool, evt *models.Event) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if err := persistTx(ctx, tx, evt); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// persistTx applies evt's statements inside an existing transaction. The
// first statement is the raw-event claim (RETURNING); derived statements run
// only if the claim succeeded — see applyEvent's dual-writer note.
func persistTx(ctx context.Context, tx pgx.Tx, evt *models.Event) error {
	stmts, err := eventStmts(evt)
	if err != nil {
		return err
	}
	return applyEvent(ctx, tx, evt, stmts)
}

// Close shuts down the Kafka reader.
func (w *Writer) Close() error {
	return w.reader.Close()
}

// nullableString maps an empty Go string to a real SQL NULL rather than
// writing an empty-string reject_reason for an order that succeeded.
func nullableString(s string) any {
	if s == "" {
		return nil
	}
	return s
}
