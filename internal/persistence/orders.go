package persistence

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
)

// OrderStatus is the durable terminal (or last-known) state of an order as
// recorded in Postgres by the async writer. It is the source of truth for
// orders that have left the live in-memory book (filled, cancelled, or lost to
// an engine restart): the book only holds resting orders, so a bot that sees
// its order vanish must consult this record to tell a real fill apart from a
// self-trade-prevention cancel or a restart that wiped the book.
type OrderStatus struct {
	Found  bool
	Status string
	Filled decimal.Decimal
}

// OrderStatusByID reads an order's persisted status and filled quantity. A
// missing row (order not yet flushed from the async event log, or never
// existed) returns Found=false with no error, so callers treat it as "unknown,
// account nothing" rather than "fully filled".
func OrderStatusByID(ctx context.Context, pool *pgxpool.Pool, orderID string) (OrderStatus, error) {
	if pool == nil {
		return OrderStatus{}, nil
	}
	var status string
	var filled decimal.Decimal
	err := pool.QueryRow(ctx,
		`SELECT status, filled FROM orders WHERE id = $1`, orderID,
	).Scan(&status, &filled)
	if errors.Is(err, pgx.ErrNoRows) {
		return OrderStatus{}, nil
	}
	if err != nil {
		return OrderStatus{}, err
	}
	return OrderStatus{Found: true, Status: status, Filled: filled}, nil
}
