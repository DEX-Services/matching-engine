package settlement

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
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
	AccountID     string
	Symbol        string
	OptionType    string
	StrikePrice   decimal.Decimal
	Expiry        time.Time
	Size          decimal.Decimal // positive = long, negative = short
	Premium       decimal.Decimal // premium paid (positive) or received (negative)
	QuoteCurrency string          // settlement currency (e.g. "BIUSD")
}

// PnL returns the position's unrealized profit/loss at the given current
// option mark price (the same theoretical/book-blended price /option-chain
// and /positions' portfolio-Greeks computation use), mirroring
// futures.Position.PnL's role for options — the input liquidation.Engine's
// mark-to-market options sweep needs to erode a writer's available margin
// exactly the way futures margin erodes as a position moves against it.
//
// For a LONG position, PnL is straightforward: current value minus what was
// paid (Premium is positive — cash paid out — for a long).
//
// For a SHORT (writer) position, Premium is stored negative (cash RECEIVED,
// see recordPosition): unrealized PnL = premium received (-Premium) minus
// the current cost to buy back the same position (markPrice * |Size|). A
// writer profits as the option's value falls toward zero (paying less to
// close than was received to open) and loses as it rises.
func (p *OptionsPosition) PnL(markPrice decimal.Decimal) decimal.Decimal {
	if p.Size.IsZero() {
		return decimal.Zero
	}
	currentValue := markPrice.Mul(p.Size.Abs())
	if p.Size.IsPositive() {
		return currentValue.Sub(p.Premium)
	}
	return p.Premium.Neg().Sub(currentValue)
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
// backend is the shared Postgres balance-lock bridge (may be a disabled no-op).
func NewOptionsSettlement(ledger *risk.Ledger, backend *backendclient.Client) *OptionsSettlement {
	if backend == nil {
		backend = &backendclient.Client{}
	}
	return &OptionsSettlement{
		ledger:    ledger,
		backend:   backend,
		positions: make(map[string]*OptionsPosition),
	}
}

// Settle transfers the option premium between buyer and seller and records positions.
func (o *OptionsSettlement) Settle(trade *models.Trade) error {
	if trade.BuyOrder == nil || trade.SellOrder == nil {
		return fmt.Errorf("options settle: missing order references on trade %s", trade.ID)
	}

	quote := trade.BuyOrder.QuoteCurrency
	if quote == "" {
		_, parsed, err := parseSymbol(trade.Symbol)
		if err != nil {
			return err
		}
		quote = parsed
	}

	// Premium = trade price × quantity (the price of an option is its premium).
	premium := trade.Price.Mul(trade.Quantity)
	buyerID := trade.BuyOrder.AccountID
	sellerID := trade.SellOrder.AccountID

	// Buyer pays premium.
	if err := o.ledger.Debit(buyerID, quote, premium); err != nil {
		return fmt.Errorf("options settle debit buyer premium: %w", err)
	}
	backendclient.Async("settle", func(ctx context.Context) error {
		return o.backend.Settle(ctx, buyerID, quote, backendclient.ToRawUnits(premium))
	})

	// Seller receives premium.
	o.ledger.Credit(sellerID, quote, premium)
	backendclient.Async("credit", func(ctx context.Context) error {
		return o.backend.Credit(ctx, sellerID, quote, backendclient.ToRawUnits(premium))
	})

	o.recordPosition(buyerID, trade.Symbol, trade.Quantity, premium, trade.BuyOrder, quote)
	o.recordPosition(sellerID, trade.Symbol, trade.Quantity.Neg(), premium.Neg(), trade.SellOrder, quote)

	return nil
}

func (o *OptionsSettlement) recordPosition(accountID, symbol string, size, premium decimal.Decimal, meta *models.Order, quote string) {
	key := positionKey(accountID, symbol, meta.StrikePrice, meta.Expiry, meta.OptionType)
	o.mu.Lock()
	defer o.mu.Unlock()
	pos, ok := o.positions[key]
	if !ok {
		pos = &OptionsPosition{
			AccountID:     accountID,
			Symbol:        symbol,
			OptionType:    meta.OptionType,
			StrikePrice:   meta.StrikePrice,
			Expiry:        meta.Expiry,
			QuoteCurrency: quote,
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

// ForceClosePosition force-closes a WRITER's (short) options position at the
// given current mark price, for liquidation.Engine's mark-to-market options
// sweep — the real force-close counterpart to FuturesSettlement.ClosePosition,
// filling the roadmap's "liquidation for options sellers" gap (Phase 2 item
// 5) instead of leaving it alert-only.
//
// Mechanically: releases the reservedCollateral (whatever was actually
// locked for this position at order time — see
// risk.RequiredOptionsMargin, recomputed by the caller against the SAME
// inputs the order-time check used) back to available, then debits the
// account for its realized loss (pos.PnL is negative for a losing writer)
// or credits it for a realized gain. This is a REAL, final settlement — the
// position is deleted and cannot be partially reopened; unlike the reduce-
// only order this is called alongside, it applies regardless of whether the
// option's own book has any liquidity to trade against, exactly the same
// way ClosePosition force-settles a futures position at mark when the
// reduce-only order can't fill it first.
//
// Returns the realized PnL applied, for the caller's event/logging use. If
// the debit fails (extreme case: account has less than the loss even after
// releasing its own collateral — should not happen since the position IS
// margined for exactly this, but ledger state can desync under real-world
// failure conditions), the position is NOT removed, mirroring
// ExpiryProcessor's exercise-debit-failure path: leave it for manual
// reconciliation rather than silently eat an unrecoverable loss.
func (o *OptionsSettlement) ForceClosePosition(accountID, symbol string, strike decimal.Decimal, expiry time.Time, optionType string, reservedCollateral, markPrice decimal.Decimal) (decimal.Decimal, error) {
	key := positionKey(accountID, symbol, strike, expiry, optionType)
	o.mu.RLock()
	pos, ok := o.positions[key]
	o.mu.RUnlock()
	if !ok || pos.Size.IsZero() {
		return decimal.Zero, nil
	}
	quote := pos.QuoteCurrency
	pnl := pos.PnL(markPrice)

	if reservedCollateral.IsPositive() {
		o.ledger.Release(accountID, quote, reservedCollateral)
	}
	if pnl.IsNegative() {
		if err := o.ledger.Debit(accountID, quote, pnl.Neg()); err != nil {
			return decimal.Zero, fmt.Errorf("force-close debit: %w", err)
		}
		backendclient.Async("settle", func(ctx context.Context) error {
			return o.backend.Settle(ctx, accountID, quote, backendclient.ToRawUnits(pnl.Neg()))
		})
	} else if pnl.IsPositive() {
		o.ledger.Credit(accountID, quote, pnl)
		backendclient.Async("credit", func(ctx context.Context) error {
			return o.backend.Credit(ctx, accountID, quote, backendclient.ToRawUnits(pnl))
		})
	}

	o.removePosition(accountID, symbol, strike, expiry, optionType)
	return pnl, nil
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
func NewExpiryProcessor(options *OptionsSettlement, ledger *risk.Ledger, md *marketdata.Service, bus *events.Bus, backend *backendclient.Client) *ExpiryProcessor {
	if backend == nil {
		backend = &backendclient.Client{}
	}
	return &ExpiryProcessor{options: options, ledger: ledger, backend: backend, marketdata: md, bus: bus, log: slog.Default()}
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
	quote := pos.QuoteCurrency
	if quote == "" {
		_, parsed, err := parseSymbol(pos.Symbol)
		if err != nil {
			p.log.Error("expiry: cannot determine quote currency", "symbol", pos.Symbol, "error", err)
			return
		}
		quote = parsed
	}

	// Use the underlying spot mark price for intrinsic-value computation.
	// The underlying symbol is the instrument's base pair (e.g. BTC-USDT for
	// a BTC-USDT-55000-...-CALL instrument). We look up the Spot ticker and
	// use MarkPrice (manipulation-resistant) rather than raw MidPrice.
	underlying := underlyingFromSymbol(pos.Symbol, pos.QuoteCurrency)
	ticker, err := p.marketdata.Ticker(underlying, models.Spot)
	if err != nil || ticker.MarkPrice.IsZero() {
		p.log.Error("expiry: no mark price available", "symbol", pos.Symbol, "underlying", underlying)
		return
	}

	markPrice := ticker.MarkPrice
	var intrinsic decimal.Decimal
	if pos.OptionType == "CALL" {
		intrinsic = decimal.Max(decimal.Zero, markPrice.Sub(pos.StrikePrice))
	} else {
		intrinsic = decimal.Max(decimal.Zero, pos.StrikePrice.Sub(markPrice))
	}

	if intrinsic.IsPositive() {
		payout := intrinsic.Mul(pos.Size.Abs())
		if pos.Size.IsPositive() {
			// Long ITM: credit the payout.
			p.ledger.Credit(pos.AccountID, quote, payout)
			backendclient.Async("credit", func(ctx context.Context) error {
				return p.backend.Credit(ctx, pos.AccountID, quote, backendclient.ToRawUnits(payout))
			})
		} else {
			// Short ITM: debit the payout from the writer. If the debit
			// fails (insufficient balance), do NOT remove the position —
			// leave it for manual reconciliation / retry. Otherwise the
			// exchange silently eats the loss with no recourse.
			if err := p.ledger.Debit(pos.AccountID, quote, payout); err != nil {
				p.log.Error("expiry exercise debit failed; position retained for reconciliation",
					"account", pos.AccountID, "symbol", pos.Symbol, "payout", payout, "error", err)
				p.publishExpiryEvent(pos, markPrice)
				return
			}
			backendclient.Async("settle", func(ctx context.Context) error {
				return p.backend.Settle(ctx, pos.AccountID, quote, backendclient.ToRawUnits(payout))
			})
		}
	}

	// Release the writer's cash-secured collateral (strike × |size|) that was
	// reserved at order time. For ITM shorts the exercise Debit above already
	// released reservation up to the payout; the residual here returns the
	// remainder. For OTM shorts (intrinsic == 0) this releases the full
	// collateral. Without this, expired option collateral stays locked forever.
	if pos.Size.IsNegative() {
		collateral := pos.StrikePrice.Mul(pos.Size.Abs())
		payout := intrinsic.Mul(pos.Size.Abs()) // 0 for OTM
		residual := collateral.Sub(payout)
		if residual.IsPositive() {
			p.ledger.Release(pos.AccountID, quote, residual)
			backendclient.Async("unlock", func(ctx context.Context) error {
				return p.backend.Unlock(ctx, pos.AccountID, quote, backendclient.ToRawUnits(residual))
			})
		}
	}

	p.publishExpiryEvent(pos, markPrice)
	p.options.removePosition(pos.AccountID, pos.Symbol, pos.StrikePrice, pos.Expiry, pos.OptionType)
}

// publishExpiryEvent publishes an EventOrderExpired for the settled position so
// downstream consumers (WS, Kafka→Postgres) are notified even when the
// option expires worthless (OTM, no payout).
func (p *ExpiryProcessor) publishExpiryEvent(pos *OptionsPosition, markPrice decimal.Decimal) {
	if p.bus == nil {
		return
	}
	p.bus.Publish(&models.Event{
		Type:           models.EventOrderExpired,
		Symbol:         pos.Symbol,
		Market:         string(models.Options),
		SequenceNumber: p.bus.NextOutOfBandSequence(),
		Order: &models.Order{
			AccountID:   pos.AccountID,
			Symbol:      pos.Symbol,
			Market:      models.Options,
			OptionType:  pos.OptionType,
			StrikePrice: pos.StrikePrice,
			Expiry:      pos.Expiry,
		},
	})
}

// underlyingFromSymbol extracts the underlying spot symbol from an option
// instrument symbol. For the new format BASE-QUOTE-STRIKE-EXPIRY-TYPE (5
// parts), the underlying is the first two segments. For legacy symbols
// without the quote (e.g. BTC-55000-20250102-CALL, 4 parts), the underlying
// is reconstructed from the base currency and the position's QuoteCurrency.
func underlyingFromSymbol(symbol, quoteCurrency string) string {
	parts := strings.Split(symbol, "-")
	if len(parts) >= 5 {
		return parts[0] + "-" + parts[1]
	}
	// Legacy or short format: use base + quote currency.
	if len(parts) >= 1 && quoteCurrency != "" {
		return parts[0] + "-" + quoteCurrency
	}
	return symbol
}
