package settlement

import (
	"fmt"
	"sync"
	"time"

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
	mu        sync.RWMutex
	positions map[string]*Position // key: accountID+":"+symbol
}

// NewFuturesSettlement creates a FuturesSettlement backed by the given ledger.
func NewFuturesSettlement(ledger *risk.Ledger) *FuturesSettlement {
	return &FuturesSettlement{
		ledger:    ledger,
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

	notional := trade.Price.Mul(trade.Quantity)
	buyerMargin := risk.MarginRequired(notional, buyerLeverage)
	sellerMargin := risk.MarginRequired(notional, sellerLeverage)

	buyerID := trade.BuyOrder.AccountID
	sellerID := trade.SellOrder.AccountID

	// Debit initial margin from both sides.
	if err := f.ledger.Debit(buyerID, quote, buyerMargin); err != nil {
		return fmt.Errorf("futures: debit buyer margin: %w", err)
	}
	if err := f.ledger.Debit(sellerID, quote, sellerMargin); err != nil {
		return fmt.Errorf("futures: debit seller margin: %w", err)
	}

	// Update positions.
	f.updatePosition(buyerID, trade.Symbol, models.Buy, trade.Quantity, trade.Price, buyerMargin, buyerLeverage)
	f.updatePosition(sellerID, trade.Symbol, models.Sell, trade.Quantity, trade.Price, sellerMargin, sellerLeverage)

	return nil
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
// liquidation or a reduce-only order), realizing PnL to the ledger.
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

	settlement := margin.Add(pnl)
	if settlement.IsPositive() {
		f.ledger.Credit(accountID, quoteAsset, settlement)
	}
}

// GetPosition returns the current position for an account/symbol, or nil.
func (f *FuturesSettlement) GetPosition(accountID, symbol string) *Position {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.positions[accountID+":"+symbol]
}

var _ Handler = (*FuturesSettlement)(nil)
