package settlement

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dex/matching-engine/internal/backendclient"
	"github.com/dex/matching-engine/internal/models"
	"github.com/dex/matching-engine/internal/risk"
	"github.com/shopspring/decimal"
)

// defaultLeverage is used when an order does not specify one (e.g. legacy
// callers or reduce-only closes), matching the previous hardcoded behaviour.
const defaultLeverage = 10

// Position tracks a single account's open position in a futures market.
type Position struct {
	AccountID  string
	Symbol     string
	Side       models.OrderSide
	Size       decimal.Decimal
	EntryPrice decimal.Decimal // volume-weighted average entry
	Margin     decimal.Decimal // initial margin held in the ledger
	Leverage   int
	UpdatedAt  time.Time
}

// MaintenanceMargin returns the minimum margin (in quote currency) the
// position must retain at the given mark price before liquidation triggers.
func (p *Position) MaintenanceMargin(markPrice, maintenanceMarginRate decimal.Decimal) decimal.Decimal {
	notional := markPrice.Mul(p.Size.Abs())
	return notional.Mul(maintenanceMarginRate)
}

// MarginRatio returns (margin + unrealizedPnL) / notional. Below the
// maintenance margin rate, the position is subject to liquidation.
func (p *Position) MarginRatio(markPrice decimal.Decimal) decimal.Decimal {
	notional := markPrice.Mul(p.Size.Abs())
	if notional.IsZero() {
		return decimal.Zero
	}
	equity := p.Margin.Add(p.PnL(markPrice))
	return equity.Div(notional)
}

// PnL returns unrealised profit/loss given the current mark price.
func (p *Position) PnL(markPrice decimal.Decimal) decimal.Decimal {
	if p.Size.IsZero() {
		return decimal.Zero
	}
	diff := markPrice.Sub(p.EntryPrice).Mul(p.Size)
	if p.Side == models.Sell {
		diff = diff.Neg()
	}
	return diff
}

// FuturesSettlement manages margin and position tracking for futures trades.
// No physical asset is transferred — only margin and position records are updated.
//
// Fields like leverage, mark price, and funding rate are futures-only concerns
// and are deliberately absent from the core Order/Trade structs.
type FuturesSettlement struct {
	ledger    *risk.Ledger
	backend   *backendclient.Client
	mu        sync.RWMutex
	positions map[string]*Position // key: accountID+":"+symbol
}

// NewFuturesSettlement creates a FuturesSettlement backed by the given ledger.
func NewFuturesSettlement(ledger *risk.Ledger) *FuturesSettlement {
	return &FuturesSettlement{
		ledger:    ledger,
		backend:   backendclient.New(),
		positions: make(map[string]*Position),
	}
}

// Settle updates positions and margin for both sides of a futures trade.
func (f *FuturesSettlement) Settle(trade *models.Trade) error {
	if trade.BuyOrder == nil || trade.SellOrder == nil {
		return fmt.Errorf("futures settle: missing order references on trade %s", trade.ID)
	}

	_, quote, err := parseSymbol(trade.Symbol)
	if err != nil {
		return err
	}

	buyerLeverage := effectiveLeverage(trade.BuyOrder.Leverage)
	sellerLeverage := effectiveLeverage(trade.SellOrder.Leverage)

	buyerID := trade.BuyOrder.AccountID
	sellerID := trade.SellOrder.AccountID

	if err := f.applyFill(buyerID, trade.Symbol, quote, models.Buy, trade.Quantity, trade.Price, buyerLeverage); err != nil {
		return fmt.Errorf("futures: apply buyer fill: %w", err)
	}
	if err := f.applyFill(sellerID, trade.Symbol, quote, models.Sell, trade.Quantity, trade.Price, sellerLeverage); err != nil {
		return fmt.Errorf("futures: apply seller fill: %w", err)
	}

	return nil
}

// applyFill applies one side of a trade fill to accountID's position. If the
// account holds no position, or a position in the same direction as side,
// this opens/adds to the position (original behavior). If the account holds
// an opposite-direction position, this closes it (fully or partially,
// realizing PnL and releasing margin) and, on overfill, opens a new position
// in the new direction with the remaining quantity.
func (f *FuturesSettlement) applyFill(accountID, symbol, quoteAsset string, side models.OrderSide,
	qty, price decimal.Decimal, leverage int) error {
	existing := f.GetPosition(accountID, symbol)

	if existing == nil || existing.Side == side {
		notional := price.Mul(qty)
		margin := risk.MarginRequired(notional, leverage)
		if err := f.ledger.Debit(accountID, quoteAsset, margin); err != nil {
			return err
		}
		backendclient.Async("settle", func(ctx context.Context) error {
			return f.backend.Settle(ctx, accountID, quoteAsset, backendclient.ToRawUnits(margin))
		})
		f.updatePosition(accountID, symbol, side, qty, price, margin, leverage)
		return nil
	}

	closeQty := decimal.Min(qty, existing.Size)
	openQty := qty.Sub(closeQty)

	f.closePortion(accountID, symbol, quoteAsset, price, closeQty)

	if openQty.IsPositive() {
		notional := price.Mul(openQty)
		margin := risk.MarginRequired(notional, leverage)
		if err := f.ledger.Debit(accountID, quoteAsset, margin); err != nil {
			return err
		}
		backendclient.Async("settle", func(ctx context.Context) error {
			return f.backend.Settle(ctx, accountID, quoteAsset, backendclient.ToRawUnits(margin))
		})
		f.updatePosition(accountID, symbol, side, openQty, price, margin, leverage)
	}
	return nil
}

// closePortion realizes PnL and releases a proportional slice of margin for
// closeQty of accountID's existing position at symbol, crediting the result
// to the ledger and (asynchronously) to Postgres. Deletes the position if it
// is fully closed.
func (f *FuturesSettlement) closePortion(accountID, symbol, quoteAsset string, price, closeQty decimal.Decimal) {
	key := accountID + ":" + symbol
	f.mu.Lock()
	pos, ok := f.positions[key]
	if !ok || closeQty.IsZero() {
		f.mu.Unlock()
		return
	}

	pnl := pos.PnL(price).Mul(closeQty).Div(pos.Size)
	releaseMargin := pos.Margin.Mul(closeQty).Div(pos.Size)

	pos.Size = pos.Size.Sub(closeQty)
	pos.Margin = pos.Margin.Sub(releaseMargin)
	fullyClosed := pos.Size.IsZero()
	if fullyClosed {
		delete(f.positions, key)
	}
	f.mu.Unlock()

	f.realizeAndCredit(accountID, quoteAsset, releaseMargin, pnl)
}

// realizeAndCredit settles a released margin amount plus realized PnL back
// to accountID's balance, in-memory and (best-effort) in Postgres. The net
// settlement may be negative (a loss exceeding the released margin); this
// still applies as a debit so the loss is actually collected, unlike a
// silent no-op.
func (f *FuturesSettlement) realizeAndCredit(accountID, quoteAsset string, margin, pnl decimal.Decimal) {
	settlement := margin.Add(pnl)
	if settlement.IsPositive() {
		f.ledger.Credit(accountID, quoteAsset, settlement)
	} else if settlement.IsNegative() {
		// Best-effort: a position's margin is expected to cover its own
		// losses under normal liquidation thresholds, but guard against the
		// in-memory ledger going negative from an outsized adverse move.
		_ = f.ledger.Debit(accountID, quoteAsset, settlement.Neg())
	}
	backendclient.Async("credit", func(ctx context.Context) error {
		return f.backend.Credit(ctx, accountID, quoteAsset, backendclient.ToRawUnits(settlement))
	})
}

// effectiveLeverage returns the order's leverage, or the default if unset.
func effectiveLeverage(orderLeverage int) int {
	if orderLeverage < 1 {
		return defaultLeverage
	}
	return orderLeverage
}

func (f *FuturesSettlement) updatePosition(accountID, symbol string, side models.OrderSide,
	qty, price, margin decimal.Decimal, leverage int) {
	key := accountID + ":" + symbol
	f.mu.Lock()
	defer f.mu.Unlock()

	pos, ok := f.positions[key]
	if !ok {
		pos = &Position{AccountID: accountID, Symbol: symbol, Side: side}
		f.positions[key] = pos
	}

	// Volume-weighted average entry price.
	if pos.Size.IsZero() {
		pos.EntryPrice = price
	} else {
		totalCost := pos.EntryPrice.Mul(pos.Size).Add(price.Mul(qty))
		pos.EntryPrice = totalCost.Div(pos.Size.Add(qty))
	}
	pos.Size = pos.Size.Add(qty)
	pos.Margin = pos.Margin.Add(margin)
	pos.Leverage = leverage
	pos.UpdatedAt = time.Now()
}

// AllPositions returns a snapshot of every open futures position, for the
// liquidation engine and funding scheduler to iterate over.
func (f *FuturesSettlement) AllPositions() []*Position {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]*Position, 0, len(f.positions))
	for _, p := range f.positions {
		cp := *p
		out = append(out, &cp)
	}
	return out
}

// ApplyFunding credits/debits the ledger for a funding payment on an existing
// position and records the new margin. Called by the funding scheduler.
func (f *FuturesSettlement) ApplyFunding(accountID, symbol string, payment decimal.Decimal, quoteAsset string) error {
	key := accountID + ":" + symbol
	f.mu.Lock()
	pos, ok := f.positions[key]
	f.mu.Unlock()
	if !ok {
		return nil
	}
	if payment.IsNegative() {
		if err := f.ledger.Debit(accountID, quoteAsset, payment.Neg()); err != nil {
			return err
		}
	} else if payment.IsPositive() {
		f.ledger.Credit(accountID, quoteAsset, payment)
	}
	f.mu.Lock()
	pos.Margin = pos.Margin.Add(payment)
	f.mu.Unlock()
	return nil
}

// ClosePosition removes a position after it has been fully closed (e.g. by
// liquidation), realizing PnL and released margin to the ledger. If the
// position was already closed (e.g. its closing fill already ran through
// Settle/applyFill), this is a no-op — safe to call defensively.
func (f *FuturesSettlement) ClosePosition(accountID, symbol, quoteAsset string, markPrice decimal.Decimal) {
	key := accountID + ":" + symbol
	f.mu.Lock()
	pos, ok := f.positions[key]
	if !ok {
		f.mu.Unlock()
		return
	}
	pnl := pos.PnL(markPrice)
	margin := pos.Margin
	delete(f.positions, key)
	f.mu.Unlock()

	f.realizeAndCredit(accountID, quoteAsset, margin, pnl)
}

// GetPosition returns the current position for an account/symbol, or nil.
func (f *FuturesSettlement) GetPosition(accountID, symbol string) *Position {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.positions[accountID+":"+symbol]
}

var _ Handler = (*FuturesSettlement)(nil)
