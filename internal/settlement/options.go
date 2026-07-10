package settlement

import (
	"fmt"
	"sync"
	"time"

	"github.com/dex/matching-engine/internal/models"
	"github.com/dex/matching-engine/internal/risk"
	"github.com/shopspring/decimal"
)

// OptionsMeta contains options-specific fields that the gateway attaches to an
// order before submission. They do NOT live in the core Order struct.
type OptionsMeta struct {
	StrikePrice decimal.Decimal
	Expiry      time.Time
	OptionType  string // "CALL" | "PUT"
}

// OptionsPosition tracks a holding in an options contract.
type OptionsPosition struct {
	AccountID   string
	Symbol      string
	OptionType  string
	StrikePrice decimal.Decimal
	Expiry      time.Time
	Size        decimal.Decimal // positive = long, negative = short
	Premium     decimal.Decimal // premium paid/received
}

// OptionsSettlement handles trade settlement for options contracts.
// At trade time, the option premium is transferred between buyer and seller.
// Exercise / assignment at expiry is handled by a separate expiry processor
// (not in scope for Phase 6 — left as a TODO).
type OptionsSettlement struct {
	ledger    *risk.Ledger
	metaStore sync.Map // orderID → *OptionsMeta
	mu        sync.RWMutex
	positions map[string]*OptionsPosition // key: accountID+":"+symbol
}

// NewOptionsSettlement creates an OptionsSettlement backed by the given ledger.
func NewOptionsSettlement(ledger *risk.Ledger) *OptionsSettlement {
	return &OptionsSettlement{
		ledger:    ledger,
		positions: make(map[string]*OptionsPosition),
	}
}

// RegisterMeta associates options metadata with an order ID before it is submitted.
// Called by the gateway when creating an options order.
func (o *OptionsSettlement) RegisterMeta(orderID string, meta *OptionsMeta) {
	o.metaStore.Store(orderID, meta)
}

// Settle transfers the option premium between buyer and seller and records positions.
func (o *OptionsSettlement) Settle(trade *models.Trade) error {
	if trade.BuyOrder == nil || trade.SellOrder == nil {
		return fmt.Errorf("options settle: missing order references on trade %s", trade.ID)
	}

	_, quote, err := parseSymbol(trade.Symbol)
	if err != nil {
		return err
	}

	// Premium = trade price × quantity (the price of an option is its premium).
	premium := trade.Price.Mul(trade.Quantity)
	buyerID := trade.BuyOrder.AccountID
	sellerID := trade.SellOrder.AccountID

	// Buyer pays premium; seller receives premium.
	if err := o.ledger.Debit(buyerID, quote, premium); err != nil {
		return fmt.Errorf("options settle debit buyer premium: %w", err)
	}
	o.ledger.Credit(sellerID, quote, premium)

	// Load meta for buyer order (if available).
	var meta *OptionsMeta
	if v, ok := o.metaStore.Load(trade.BuyOrder.ID); ok {
		meta = v.(*OptionsMeta)
		o.metaStore.Delete(trade.BuyOrder.ID)
	}

	// Record positions.
	o.recordPosition(buyerID, trade.Symbol, trade.Quantity, premium, meta, true)
	o.recordPosition(sellerID, trade.Symbol, trade.Quantity.Neg(), premium.Neg(), meta, false)

	return nil
}

func (o *OptionsSettlement) recordPosition(accountID, symbol string, size, premium decimal.Decimal, meta *OptionsMeta, isBuyer bool) {
	key := accountID + ":" + symbol
	o.mu.Lock()
	defer o.mu.Unlock()
	pos, ok := o.positions[key]
	if !ok {
		pos = &OptionsPosition{AccountID: accountID, Symbol: symbol}
		if meta != nil {
			pos.OptionType = meta.OptionType
			pos.StrikePrice = meta.StrikePrice
			pos.Expiry = meta.Expiry
		}
		o.positions[key] = pos
	}
	pos.Size = pos.Size.Add(size)
	pos.Premium = pos.Premium.Add(premium)
}

// GetPosition returns an options position for account/symbol, or nil.
func (o *OptionsSettlement) GetPosition(accountID, symbol string) *OptionsPosition {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.positions[accountID+":"+symbol]
}

var _ Handler = (*OptionsSettlement)(nil)
