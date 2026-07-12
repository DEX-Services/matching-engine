package settlement

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/dex/matching-engine/internal/backendclient"
	"github.com/dex/matching-engine/internal/events"
	"github.com/dex/matching-engine/internal/marketdata"
	"github.com/dex/matching-engine/internal/models"
	"github.com/dex/matching-engine/internal/risk"
	"github.com/shopspring/decimal"
)

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
// Strike/expiry/type travel on the order itself (models.Order), populated by
// the gateway from the option instrument the client selected.
type OptionsSettlement struct {
	ledger    *risk.Ledger
	backend   *backendclient.Client
	mu        sync.RWMutex
	positions map[string]*OptionsPosition // key: accountID+":"+symbol+":"+strike+":"+expiry+":"+type
}

// NewOptionsSettlement creates an OptionsSettlement backed by the given ledger.
func NewOptionsSettlement(ledger *risk.Ledger) *OptionsSettlement {
	return &OptionsSettlement{
		ledger:    ledger,
		backend:   backendclient.New(),
		positions: make(map[string]*OptionsPosition),
	}
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
	backendclient.Async("settle", func(ctx context.Context) error {
		return o.backend.Settle(ctx, buyerID, quote, premium.String())
	})
	o.ledger.Credit(sellerID, quote, premium)

	o.recordPosition(buyerID, trade.Symbol, trade.Quantity, premium, trade.BuyOrder)
	o.recordPosition(sellerID, trade.Symbol, trade.Quantity.Neg(), premium.Neg(), trade.SellOrder)

	return nil
}

func (o *OptionsSettlement) recordPosition(accountID, symbol string, size, premium decimal.Decimal, meta *models.Order) {
	key := positionKey(accountID, symbol, meta.StrikePrice, meta.Expiry, meta.OptionType)
	o.mu.Lock()
	defer o.mu.Unlock()
	pos, ok := o.positions[key]
	if !ok {
		pos = &OptionsPosition{
			AccountID:   accountID,
			Symbol:      symbol,
			OptionType:  meta.OptionType,
			StrikePrice: meta.StrikePrice,
			Expiry:      meta.Expiry,
		}
		o.positions[key] = pos
	}
	pos.Size = pos.Size.Add(size)
	pos.Premium = pos.Premium.Add(premium)
}

func positionKey(accountID, symbol string, strike decimal.Decimal, expiry time.Time, optionType string) string {
	return fmt.Sprintf("%s:%s:%s:%d:%s", accountID, symbol, strike.String(), expiry.Unix(), optionType)
}

// GetPosition returns an options position for account/symbol/strike/expiry/type, or nil.
func (o *OptionsSettlement) GetPosition(accountID, symbol string, strike decimal.Decimal, expiry time.Time, optionType string) *OptionsPosition {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.positions[positionKey(accountID, symbol, strike, expiry, optionType)]
}

// AllPositions returns a snapshot of every open options position, for the
// expiry processor and position-listing API to iterate over.
func (o *OptionsSettlement) AllPositions() []*OptionsPosition {
	o.mu.RLock()
	defer o.mu.RUnlock()
	out := make([]*OptionsPosition, 0, len(o.positions))
	for _, p := range o.positions {
		cp := *p
		out = append(out, &cp)
	}
	return out
}

// removePosition deletes a settled/expired position.
func (o *OptionsSettlement) removePosition(accountID, symbol string, strike decimal.Decimal, expiry time.Time, optionType string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.positions, positionKey(accountID, symbol, strike, expiry, optionType))
}

var _ Handler = (*OptionsSettlement)(nil)

// ExpiryProcessor sweeps options positions past their expiry, auto-exercising
// in-the-money contracts (cash-settled against the underlying's mark price)
// and expiring out-of-the-money contracts worthless.
type ExpiryProcessor struct {
	options    *OptionsSettlement
	ledger     *risk.Ledger
	backend    *backendclient.Client
	marketdata *marketdata.Service
	bus        *events.Bus
	log        *slog.Logger
}

// NewExpiryProcessor creates an ExpiryProcessor.
func NewExpiryProcessor(options *OptionsSettlement, ledger *risk.Ledger, md *marketdata.Service, bus *events.Bus) *ExpiryProcessor {
	return &ExpiryProcessor{options: options, ledger: ledger, backend: backendclient.New(), marketdata: md, bus: bus, log: slog.Default()}
}

// Run starts the expiry sweep loop; call in a goroutine. Stops when ctx is cancelled.
func (p *ExpiryProcessor) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.sweep()
		case <-ctx.Done():
			return
		}
	}
}

func (p *ExpiryProcessor) sweep() {
	now := time.Now()
	for _, pos := range p.options.AllPositions() {
		if pos.Size.IsZero() || pos.Expiry.IsZero() || pos.Expiry.After(now) {
			continue
		}
		p.settleExpiry(pos)
	}
}

func (p *ExpiryProcessor) settleExpiry(pos *OptionsPosition) {
	_, quote, err := parseSymbol(pos.Symbol)
	if err != nil {
		return
	}
	ticker, err := p.marketdata.Ticker(pos.Symbol, models.Spot)
	if err != nil || ticker.MidPrice.IsZero() {
		p.log.Error("expiry: no mark price available", "symbol", pos.Symbol)
		return
	}

	var intrinsic decimal.Decimal
	if pos.OptionType == "CALL" {
		intrinsic = decimal.Max(decimal.Zero, ticker.MidPrice.Sub(pos.StrikePrice))
	} else {
		intrinsic = decimal.Max(decimal.Zero, pos.StrikePrice.Sub(ticker.MidPrice))
	}

	if intrinsic.IsPositive() {
		payout := intrinsic.Mul(pos.Size.Abs())
		if pos.Size.IsPositive() {
			p.ledger.Credit(pos.AccountID, quote, payout)
		} else if err := p.ledger.Debit(pos.AccountID, quote, payout); err != nil {
			p.log.Error("expiry exercise debit failed", "account", pos.AccountID, "symbol", pos.Symbol, "error", err)
		} else {
			backendclient.Async("settle", func(ctx context.Context) error {
				return p.backend.Settle(ctx, pos.AccountID, quote, payout.String())
			})
		}
	}

	p.options.removePosition(pos.AccountID, pos.Symbol, pos.StrikePrice, pos.Expiry, pos.OptionType)
}
