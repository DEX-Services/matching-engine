package persistence

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
	"time"
)

// OrderHistoryItem is one row of an account's persisted order history.
// AvgFillPrice and FeePaid are aggregated from the trades table (an order
// can span multiple partial fills at different prices, each carrying its
// own maker/taker fee); both are zero for an order with no fills.
type OrderHistoryItem struct {
	ID, Symbol, Market, Side, Type, Status string
	RejectReason                           string
	Price, Quantity, Filled                decimal.Decimal
	AvgFillPrice, FeePaid                  decimal.Decimal
	CreatedAt, UpdatedAt                   time.Time
}

// OrderHistoryFilter narrows an OrderHistory query. Zero values (empty
// string / zero time) impose no filter on that field. Before is the cursor
// for pagination (strictly older than); After/Before together implement a
// time-range filter when both are set.
type OrderHistoryFilter struct {
	Account string
	Symbol  string
	Market  string
	// After/Before bound created_at: After is the range's start (inclusive),
	// Before is both the range's end (exclusive) AND the pagination cursor —
	// callers paginating pass the last-seen row's CreatedAt as Before on the
	// next page, same as before this filter struct existed.
	After, Before time.Time
	Limit         int
}

func OrderHistory(ctx context.Context, pool *pgxpool.Pool, f OrderHistoryFilter) ([]OrderHistoryItem, error) {
	if pool == nil {
		return []OrderHistoryItem{}, nil
	}
	limit := f.Limit
	if limit < 1 || limit > 100 {
		limit = 50
	}

	// Fee/avg-fill-price are aggregated per order from trades: an order's
	// own row can be either the maker or the taker side of any given trade
	// (o.id = maker_order_id for a resting order that got hit, or
	// = taker_order_id for the order that crossed the book), so the fee it
	// paid is maker_fee_paid when it was the maker leg and taker_fee_paid
	// when it was the taker leg of that trade.
	query := strings.Builder{}
	query.WriteString(`
		SELECT o.id, o.symbol, o.market, o.side, o.type, o.status, o.reject_reason,
		       o.price, o.quantity, o.filled,
		       COALESCE(tr.avg_fill_price, 0), COALESCE(tr.fee_paid, 0),
		       o.created_at, o.updated_at
		FROM orders o
		LEFT JOIN LATERAL (
			SELECT
				SUM(t.price * t.quantity) / NULLIF(SUM(t.quantity), 0) AS avg_fill_price,
				SUM(CASE WHEN t.maker_order_id = o.id THEN t.maker_fee_paid ELSE 0 END
				  + CASE WHEN t.taker_order_id = o.id THEN t.taker_fee_paid ELSE 0 END) AS fee_paid
			FROM trades t
			WHERE t.maker_order_id = o.id OR t.taker_order_id = o.id
		) tr ON true
		WHERE o.account_id = $1`)

	args := []any{f.Account}
	arg := func(v any) string {
		args = append(args, v)
		return "$" + strconv.Itoa(len(args))
	}
	if f.Symbol != "" {
		query.WriteString(" AND o.symbol = " + arg(f.Symbol))
	}
	if f.Market != "" {
		query.WriteString(" AND o.market = " + arg(f.Market))
	}
	if !f.After.IsZero() {
		query.WriteString(" AND o.created_at >= " + arg(f.After))
	}
	if !f.Before.IsZero() {
		query.WriteString(" AND o.created_at < " + arg(f.Before))
	}
	query.WriteString(" ORDER BY o.created_at DESC LIMIT " + arg(limit))

	rows, err := pool.Query(ctx, query.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]OrderHistoryItem, 0)
	for rows.Next() {
		var o OrderHistoryItem
		var reason *string
		if err := rows.Scan(&o.ID, &o.Symbol, &o.Market, &o.Side, &o.Type, &o.Status, &reason,
			&o.Price, &o.Quantity, &o.Filled, &o.AvgFillPrice, &o.FeePaid, &o.CreatedAt, &o.UpdatedAt); err != nil {
			return nil, err
		}
		if reason != nil {
			o.RejectReason = *reason
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// FillItem is one individual trade execution (fill) belonging to an
// account's order, as opposed to OrderHistoryItem's per-order aggregate.
type FillItem struct {
	TradeID, OrderID, Symbol, Market, Side string
	Price, Quantity, FeePaid               decimal.Decimal
	ExecutedAt                             time.Time
}

// FillsFilter narrows a Fills query. Semantics match OrderHistoryFilter.
type FillsFilter struct {
	Account       string
	Symbol        string
	Market        string
	After, Before time.Time
	Limit         int
}

// Fills returns individual trade executions for an account (the maker or
// taker leg of each trade), most recent first. This is the fill-level
// complement to OrderHistory's per-order aggregates — plan.md 4.1 asks for
// both an order-history endpoint and a fills endpoint.
func Fills(ctx context.Context, pool *pgxpool.Pool, f FillsFilter) ([]FillItem, error) {
	if pool == nil {
		return []FillItem{}, nil
	}
	limit := f.Limit
	if limit < 1 || limit > 200 {
		limit = 50
	}

	query := strings.Builder{}
	query.WriteString(`
		SELECT t.id, o.id, t.symbol, t.market,
		       CASE WHEN t.maker_order_id = o.id THEN t.maker_side ELSE
		            (CASE WHEN t.maker_side = 'BUY' THEN 'SELL' ELSE 'BUY' END)
		       END AS side,
		       t.price, t.quantity,
		       CASE WHEN t.maker_order_id = o.id THEN t.maker_fee_paid ELSE t.taker_fee_paid END AS fee_paid,
		       t.executed_at
		FROM trades t
		JOIN orders o ON o.id = t.maker_order_id OR o.id = t.taker_order_id
		WHERE o.account_id = $1`)

	args := []any{f.Account}
	arg := func(v any) string {
		args = append(args, v)
		return "$" + strconv.Itoa(len(args))
	}
	if f.Symbol != "" {
		query.WriteString(" AND t.symbol = " + arg(f.Symbol))
	}
	if f.Market != "" {
		query.WriteString(" AND t.market = " + arg(f.Market))
	}
	if !f.After.IsZero() {
		query.WriteString(" AND t.executed_at >= " + arg(f.After))
	}
	if !f.Before.IsZero() {
		query.WriteString(" AND t.executed_at < " + arg(f.Before))
	}
	query.WriteString(" ORDER BY t.executed_at DESC LIMIT " + arg(limit))

	rows, err := pool.Query(ctx, query.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]FillItem, 0)
	for rows.Next() {
		var f FillItem
		if err := rows.Scan(&f.TradeID, &f.OrderID, &f.Symbol, &f.Market, &f.Side,
			&f.Price, &f.Quantity, &f.FeePaid, &f.ExecutedAt); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
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
