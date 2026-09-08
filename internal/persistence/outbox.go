package persistence

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/dex/matching-engine/internal/models"
	"github.com/jackc/pgx/v5/pgxpool"
)

// OutboxWriter is the write side of the durable outbox: it inserts a raw
// event's JSON into event_outbox. Called by events.KafkaPublisher when a
// bounded publish attempt fails (broker slow/unreachable) — the event was
// never handed to Kafka at all in that case, so it would otherwise be lost
// the instant it's logged and dropped. This only writes to Postgres, so an
// event still can't be recovered if Postgres itself is also down at that
// exact moment; that is the same limitation the normal Kafka path has (see
// Writer.Run's comment on why redelivery, not the outbox, is the recovery
// path when Postgres — not Kafka — is unavailable).
type OutboxWriter struct {
	pool *pgxpool.Pool
	log  *slog.Logger
}

// NewOutboxWriter wraps pool for writing to event_outbox.
func NewOutboxWriter(pool *pgxpool.Pool) *OutboxWriter {
	return &OutboxWriter{pool: pool, log: slog.Default()}
}

// Write inserts evt's JSON payload into event_outbox for later delivery by
// OutboxSweeper. Logs and returns the error rather than panicking — this is
// already the fallback path for a failure, so there's nowhere further to
// fall back to; the caller (KafkaPublisher) has already logged the original
// publish failure and can only log this one too.
func (o *OutboxWriter) Write(ctx context.Context, evt *models.Event) error {
	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}
	// A short timeout of our own: this runs on the KafkaPublisher's drain
	// loop (see publisher.go), and that loop must keep moving even if
	// Postgres is also having a bad moment — better to drop and log a
	// second time than let this call hang the same loop the Kafka timeout
	// was already protecting.
	writeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	_, err = o.pool.Exec(writeCtx, `INSERT INTO event_outbox (payload) VALUES ($1)`, payload)
	return err
}

// OutboxSweeper periodically drains event_outbox into the normal
// orders/trades/events/etc. tables via the same persist() logic Writer uses,
// then deletes delivered rows. This is the actual recovery mechanism: an
// event that only ever reached event_outbox (Kafka was unreachable when it
// was published) still ends up in order history / fills / PnL exactly as if
// it had gone through Kafka normally, once Postgres and this sweeper are
// both up — which, since it never depended on Kafka, is unaffected by
// however long the broker outage lasts.
type OutboxSweeper struct {
	pool     *pgxpool.Pool
	log      *slog.Logger
	interval time.Duration
}

// NewOutboxSweeper builds a sweeper that checks event_outbox every interval.
func NewOutboxSweeper(pool *pgxpool.Pool, interval time.Duration) *OutboxSweeper {
	return &OutboxSweeper{pool: pool, log: slog.Default(), interval: interval}
}

// Run starts the sweep loop. Call in a dedicated goroutine.
func (s *OutboxSweeper) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	// Sweep once immediately at startup rather than waiting a full interval —
	// if the engine restarted after an outage left rows behind, they should
	// drain as soon as this is running, not up to `interval` later.
	s.sweepOnce(ctx)
	for {
		select {
		case <-ticker.C:
			s.sweepOnce(ctx)
		case <-ctx.Done():
			return
		}
	}
}

// sweepOnce delivers every currently-outstanding outbox row. Rows are
// processed oldest-first so replay preserves publish order as closely as
// this fallback path can. Each row's persist() failure is logged and the
// row is left in place for the next sweep — same at-least-once, retry-until-
// success behavior as the normal Kafka-consuming Writer.
func (s *OutboxSweeper) sweepOnce(ctx context.Context) {
	for {
		delivered, more, err := s.sweepBatch(ctx)
		if err != nil {
			s.log.Error("outbox sweep failed", "error", err)
			return
		}
		if delivered > 0 {
			s.log.Info("outbox drained", "count", delivered)
		}
		if !more {
			return
		}
	}
}

// sweepBatch persists up to 100 outstanding rows and deletes the ones that
// succeeded. Returns how many were delivered and whether a full batch was
// read (a signal there may be more waiting, so the caller loops again).
func (s *OutboxSweeper) sweepBatch(ctx context.Context) (delivered int, more bool, err error) {
	const batchSize = 100
	rows, err := s.pool.Query(ctx, `SELECT id, payload FROM event_outbox ORDER BY id ASC LIMIT $1`, batchSize)
	if err != nil {
		return 0, false, err
	}
	type row struct {
		id      int64
		payload []byte
	}
	var batch []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.payload); err != nil {
			rows.Close()
			return 0, false, err
		}
		batch = append(batch, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, false, err
	}
	if len(batch) == 0 {
		return 0, false, nil
	}

	for _, r := range batch {
		var evt models.Event
		if err := json.Unmarshal(r.payload, &evt); err != nil {
			// Corrupt row: log and delete rather than retry forever on
			// something that will never unmarshal successfully.
			s.log.Error("outbox row unmarshal failed, discarding", "id", r.id, "error", err)
			if _, delErr := s.pool.Exec(ctx, `DELETE FROM event_outbox WHERE id = $1`, r.id); delErr != nil {
				s.log.Error("outbox discard delete failed", "id", r.id, "error", delErr)
			}
			continue
		}
		if err := persist(ctx, s.pool, &evt); err != nil {
			s.log.Error("outbox persist failed, will retry", "id", r.id, "symbol", evt.Symbol, "seq", evt.SequenceNumber, "error", err)
			continue
		}
		if _, err := s.pool.Exec(ctx, `DELETE FROM event_outbox WHERE id = $1`, r.id); err != nil {
			s.log.Error("outbox delete after persist failed", "id", r.id, "error", err)
			continue
		}
		delivered++
	}
	return delivered, len(batch) == batchSize, nil
}
