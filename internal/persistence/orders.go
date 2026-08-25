package persistence

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
	"time"
)

type OrderHistoryItem struct {
	ID, Symbol, Market, Side, Type, Status string
	Price, Quantity, Filled                decimal.Decimal
	CreatedAt, UpdatedAt                   time.Time
}

func OrderHistory(ctx context.Context, pool *pgxpool.Pool, account string, limit int, before time.Time) ([]OrderHistoryItem, error) {
	if pool == nil {
		return []OrderHistoryItem{}, nil
	}
	if limit < 1 || limit > 100 {
		limit = 50
	}
	rows, err := pool.Query(ctx, `SELECT id,symbol,market,side,type,price,quantity,filled,status,created_at,updated_at FROM orders WHERE account_id=$1 AND ($2::timestamptz IS NULL OR created_at < $2) ORDER BY created_at DESC LIMIT $3`, account, nullableTime(before), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]OrderHistoryItem, 0)
	for rows.Next() {
		var o OrderHistoryItem
		if err := rows.Scan(&o.ID, &o.Symbol, &o.Market, &o.Side, &o.Type, &o.Price, &o.Quantity, &o.Filled, &o.Status, &o.CreatedAt, &o.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

func nullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

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
