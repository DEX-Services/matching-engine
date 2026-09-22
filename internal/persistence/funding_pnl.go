package persistence

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dex/matching-engine/internal/fixedpoint"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
)

// FundingPaymentItem is one persisted funding settlement for an account.
type FundingPaymentItem struct {
	Symbol    string
	Rate      fixedpoint.Fixed
	Amount    fixedpoint.Fixed
	CreatedAt time.Time
}

// HistoryFilter narrows a funding-history or realized-PnL query. Shared
// shape with OrderHistoryFilter/FillsFilter (symbol/market not applicable
// to funding — funding is always futures — so this omits Market).
type HistoryFilter struct {
	Account       string
	Symbol        string
	After, Before time.Time
	Limit         int
}

// FundingHistory returns an account's persisted funding payments, most
// recent first. This is what makes the funding tab survive a refresh,
// reconnect, or login on another device — previously the frontend only had
// live WebSocket FUNDING events with no REST source to repopulate from.
func FundingHistory(ctx context.Context, pool *pgxpool.Pool, f HistoryFilter) ([]FundingPaymentItem, error) {
	if pool == nil {
		return []FundingPaymentItem{}, nil
	}
	limit := f.Limit
	if limit < 1 || limit > 200 {
		limit = 50
	}
	query := strings.Builder{}
	query.WriteString(`SELECT symbol, rate, amount, created_at FROM funding_payments WHERE account_id = $1`)
	args := []any{f.Account}
	arg := func(v any) string {
		args = append(args, v)
		return "$" + strconv.Itoa(len(args))
	}
	if f.Symbol != "" {
		query.WriteString(" AND symbol = " + arg(f.Symbol))
	}
	if !f.After.IsZero() {
		query.WriteString(" AND created_at >= " + arg(f.After))
	}
	if !f.Before.IsZero() {
		query.WriteString(" AND created_at < " + arg(f.Before))
	}
	query.WriteString(" ORDER BY created_at DESC LIMIT " + arg(limit))

	rows, err := pool.Query(ctx, query.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]FundingPaymentItem, 0)
	for rows.Next() {
		var it FundingPaymentItem
		var rate, amount decimal.Decimal
		if err := rows.Scan(&it.Symbol, &rate, &amount, &it.CreatedAt); err != nil {
			return nil, err
		}
		var convErr error
		if it.Rate, convErr = fixedpoint.FromDecimal(rate); convErr != nil {
			return nil, fmt.Errorf("funding payment %s: rate: %w", it.Symbol, convErr)
		}
		if it.Amount, convErr = fixedpoint.FromDecimal(amount); convErr != nil {
			return nil, fmt.Errorf("funding payment %s: amount: %w", it.Symbol, convErr)
		}
		out = append(out, it)
	}
	return out, rows.Err()
}

// RealizedPnlItem is one persisted position-closing settlement.
type RealizedPnlItem struct {
	Symbol         string
	ClosedQty      fixedpoint.Fixed
	Pnl            fixedpoint.Fixed
	MarginReturned fixedpoint.Fixed
	IsLiquidation  bool
	CreatedAt      time.Time
}

// RealizedPnlHistory returns an account's persisted realized-PnL events
// (each full or partial position close), most recent first.
func RealizedPnlHistory(ctx context.Context, pool *pgxpool.Pool, f HistoryFilter) ([]RealizedPnlItem, error) {
	if pool == nil {
		return []RealizedPnlItem{}, nil
	}
	limit := f.Limit
	if limit < 1 || limit > 200 {
		limit = 50
	}
	query := strings.Builder{}
	query.WriteString(`SELECT symbol, closed_qty, pnl, margin_returned, is_liquidation, created_at FROM realized_pnl WHERE account_id = $1`)
	args := []any{f.Account}
	arg := func(v any) string {
		args = append(args, v)
		return "$" + strconv.Itoa(len(args))
	}
	if f.Symbol != "" {
		query.WriteString(" AND symbol = " + arg(f.Symbol))
	}
	if !f.After.IsZero() {
		query.WriteString(" AND created_at >= " + arg(f.After))
	}
	if !f.Before.IsZero() {
		query.WriteString(" AND created_at < " + arg(f.Before))
	}
	query.WriteString(" ORDER BY created_at DESC LIMIT " + arg(limit))

	rows, err := pool.Query(ctx, query.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]RealizedPnlItem, 0)
	for rows.Next() {
		var it RealizedPnlItem
		var closedQty, pnl, marginReturned decimal.Decimal
		if err := rows.Scan(&it.Symbol, &closedQty, &pnl, &marginReturned, &it.IsLiquidation, &it.CreatedAt); err != nil {
			return nil, err
		}
		var convErr error
		if it.ClosedQty, convErr = fixedpoint.FromDecimal(closedQty); convErr != nil {
			return nil, fmt.Errorf("realized pnl %s: closed_qty: %w", it.Symbol, convErr)
		}
		if it.Pnl, convErr = fixedpoint.FromDecimal(pnl); convErr != nil {
			return nil, fmt.Errorf("realized pnl %s: pnl: %w", it.Symbol, convErr)
		}
		if it.MarginReturned, convErr = fixedpoint.FromDecimal(marginReturned); convErr != nil {
			return nil, fmt.Errorf("realized pnl %s: margin_returned: %w", it.Symbol, convErr)
		}
		out = append(out, it)
	}
	return out, rows.Err()
}
