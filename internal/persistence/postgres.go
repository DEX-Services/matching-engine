// Package persistence provides asynchronous Postgres writers that consume from
// Kafka and durably store orders, trades, and events in the database.
// Postgres is never on the hot matching path; it is an asynchronous replica of
// the Kafka event log.
package persistence

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

// NewPool creates a pgx connection pool from POSTGRES_SERVICE_URI.
func NewPool(ctx context.Context) (*pgxpool.Pool, error) {
	uri := os.Getenv("POSTGRES_SERVICE_URI")
	if uri == "" {
		uri = fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=require",
			os.Getenv("POSTGRES_USER"),
			os.Getenv("POSTGRES_PASSWORD"),
			os.Getenv("POSTGRES_HOST"),
			os.Getenv("POSTGRES_PORT"),
			os.Getenv("POSTGRES_DB"),
		)
	}
	cfg, err := pgxpool.ParseConfig(uri)
	if err != nil {
		return nil, fmt.Errorf("parse postgres config: %w", err)
	}
	cfg.MaxConns = 20 // matching engine needs few DB connections
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("open postgres pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("postgres ping: %w", err)
	}
	slog.Info("postgres connected", "host", os.Getenv("POSTGRES_HOST"))
	return pool, nil
}

// Migrate runs the DDL statements needed by the matching engine.
// Idempotent — safe to call on every startup.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, schema)
	return err
}

const schema = `
CREATE TABLE IF NOT EXISTS orders (
    id             TEXT PRIMARY KEY,
    client_order_id TEXT,
    account_id     TEXT        NOT NULL,
    symbol         TEXT        NOT NULL,
    market         TEXT        NOT NULL,
    side           TEXT        NOT NULL,
    type           TEXT        NOT NULL,
    time_in_force  TEXT,
    price          NUMERIC,
    quantity       NUMERIC     NOT NULL,
    filled         NUMERIC     NOT NULL DEFAULT 0,
    status         TEXT        NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS trades (
    id              TEXT PRIMARY KEY,
    symbol          TEXT        NOT NULL,
    market          TEXT        NOT NULL,
    maker_order_id  TEXT        NOT NULL,
    taker_order_id  TEXT        NOT NULL,
    maker_side      TEXT        NOT NULL,
    price           NUMERIC     NOT NULL,
    quantity        NUMERIC     NOT NULL,
    executed_at     TIMESTAMPTZ NOT NULL,
    sequence_number BIGINT      NOT NULL
);

CREATE INDEX IF NOT EXISTS trades_symbol_seq ON trades (symbol, sequence_number);

CREATE TABLE IF NOT EXISTS events (
    id             BIGSERIAL   PRIMARY KEY,
    symbol         TEXT        NOT NULL,
    market         TEXT        NOT NULL,
    sequence_number BIGINT     NOT NULL,
    type           TEXT        NOT NULL,
    payload        JSONB       NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS events_symbol_seq ON events (symbol, sequence_number);
`
