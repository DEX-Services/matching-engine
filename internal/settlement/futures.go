package settlement

import (
	"fmt"
	"sync"
	"time"

	"github.com/dex/matching-engine/internal/models"
	"github.com/dex/matching-engine/internal/risk"
	"github.com/shopspring/decimal"
)

// Position tracks a single account's open position in a futures market.
type Position struct {
	AccountID  string
	Symbol     string
	Side       models.OrderSide
	Size       decimal.Decimal
	EntryPrice decimal.Decimal // volume-weighted average entry
	Margin     decimal.Decimal // initial margin held in the ledger
	UpdatedAt  time.Time
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

	// For a simple cross-margin futures settlement:
	// Margin required = notional / leverage. We default to 10x leverage here.
	// Phase 7 will read leverage from per-account FuturesOrderMeta.
	leverage := decimal.NewFromInt(10)
	notional := trade.Price.Mul(trade.Quantity)
	marginRequired := notional.Div(leverage)

	buyerID := trade.BuyOrder.AccountID
	sellerID := trade.SellOrder.AccountID

	// Debit initial margin from both sides.
	if err := f.ledger.Debit(buyerID, quote, marginRequired); err != nil {
		return fmt.Errorf("futures: debit buyer margin: %w", err)
	}
	if err := f.ledger.Debit(sellerID, quote, marginRequired); err != nil {
		return fmt.Errorf("futures: debit seller margin: %w", err)
	}

	// Update positions.
	f.updatePosition(buyerID, trade.Symbol, models.Buy, trade.Quantity, trade.Price, marginRequired)
	f.updatePosition(sellerID, trade.Symbol, models.Sell, trade.Quantity, trade.Price, marginRequired)

	return nil
}

func (f *FuturesSettlement) updatePosition(accountID, symbol string, side models.OrderSide,
	qty, price, margin decimal.Decimal) {
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
		pos.Size = pos.Size.Add(qty)
		pos.EntryPrice = totalCost.Div(pos.Size)
	}
	pos.Size = pos.Size.Add(qty)
	pos.Margin = pos.Margin.Add(margin)
	pos.UpdatedAt = time.Now()
}

// GetPosition returns the current position for an account/symbol, or nil.
func (f *FuturesSettlement) GetPosition(accountID, symbol string) *Position {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.positions[accountID+":"+symbol]
}

var _ Handler = (*FuturesSettlement)(nil)
