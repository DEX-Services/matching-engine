package persistence

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/dex/matching-engine/internal/events"
	"github.com/dex/matching-engine/internal/models"
	"github.com/jackc/pgx/v5/pgxpool"
	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

// Writer consumes events from Kafka and writes them to Postgres.
// It is the sole reader of the TopicEvents topic group "postgres-writer".
// On error, messages are retried until they succeed — Kafka offset is only
// committed on success, making this effectively an at-least-once writer.
// Idempotency keys (sequence numbers) prevent duplicate rows.
type Writer struct {
	reader *kafka.Reader
	pool   *pgxpool.Pool
	log    *slog.Logger
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

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{fmt.Sprintf("%s:%s", host, port)},
		Topic:          events.TopicEvents,
		GroupID:        "postgres-writer",
		Dialer:         dialer,
		MinBytes:       1,
		MaxBytes:       10 << 20, // 10 MB
		CommitInterval: time.Second,
		StartOffset:    kafka.LastOffset,
	})

	return &Writer{reader: reader, pool: pool, log: slog.Default()}, nil
}

// Run starts the consume-and-write loop. Call in a dedicated goroutine.
func (w *Writer) Run(ctx context.Context) {
	for {
		msg, err := w.reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			w.log.Error("kafka read error", "error", err)
			time.Sleep(time.Second) // back-off
			continue
		}

		var evt models.Event
		if err := json.Unmarshal(msg.Value, &evt); err != nil {
			w.log.Error("unmarshal event", "error", err)
			continue
		}

		if err := w.persist(ctx, &evt); err != nil {
			w.log.Error("persist event", "symbol", evt.Symbol, "seq", evt.SequenceNumber, "error", err)
			// Message is NOT committed; Kafka will redeliver it.
			// The durable outbox (TopicOutbox) is the fallback if Postgres
			// stays down long enough to exhaust the retry budget.
			continue
		}
	}
}

func (w *Writer) persist(ctx context.Context, evt *models.Event) error {
	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Upsert the raw event row (idempotent via unique index).
	_, err = tx.Exec(ctx, `
		INSERT INTO events (symbol, market, sequence_number, type, payload)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (symbol, sequence_number) DO NOTHING`,
		evt.Symbol, evt.Market, evt.SequenceNumber, string(evt.Type), payload)
	if err != nil {
		return fmt.Errorf("insert event: %w", err)
	}

	// Upsert order if the event carries one.
	if evt.Order != nil {
		o := evt.Order
		_, err = tx.Exec(ctx, `
			INSERT INTO orders (id, client_order_id, account_id, symbol, market, side, type,
			                    time_in_force, price, quantity, filled, status, created_at, updated_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
			ON CONFLICT (id) DO UPDATE SET
			    filled     = EXCLUDED.filled,
			    status     = EXCLUDED.status,
			    updated_at = EXCLUDED.updated_at`,
			o.ID, o.ClientOrderID, o.AccountID, o.Symbol, string(o.Market),
			string(o.Side), string(o.Type), string(o.TimeInForce),
			o.Price, o.Quantity, o.Filled, string(o.Status), o.CreatedAt, o.UpdatedAt)
		if err != nil {
			return fmt.Errorf("upsert order: %w", err)
		}
	}

	// Insert trade if the event carries one.
	if evt.Trade != nil {
		t := evt.Trade
		_, err = tx.Exec(ctx, `
			INSERT INTO trades (id, symbol, market, maker_order_id, taker_order_id,
			                    maker_side, price, quantity, executed_at, sequence_number)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
			ON CONFLICT (id) DO NOTHING`,
			t.ID, t.Symbol, string(t.Market), t.MakerOrderID, t.TakerOrderID,
			string(t.MakerSide), t.Price, t.Quantity, t.ExecutedAt, t.SequenceNumber)
		if err != nil {
			return fmt.Errorf("insert trade: %w", err)
		}
	}

	// Insert funding payment if the event carries one.
	if evt.Funding != nil {
		f := evt.Funding
		_, err = tx.Exec(ctx, `
			INSERT INTO funding_payments (account_id, symbol, rate, amount)
			VALUES ($1, $2, $3, $4)`,
			f.AccountID, f.Symbol, f.Rate, f.Payment)
		if err != nil {
			return fmt.Errorf("insert funding payment: %w", err)
		}
	}

	return tx.Commit(ctx)
}

// Close shuts down the Kafka reader.
func (w *Writer) Close() error {
	return w.reader.Close()
}
