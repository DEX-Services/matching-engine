// Package liquidation periodically sweeps open futures positions and force-
// closes any that have breached their maintenance margin requirement.
package liquidation

import (
	"context"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/dex/matching-engine/internal/config"
	"github.com/dex/matching-engine/internal/events"
	"github.com/dex/matching-engine/internal/marketdata"
	"github.com/dex/matching-engine/internal/matching"
	"github.com/dex/matching-engine/internal/models"
	"github.com/dex/matching-engine/internal/pricing"
	"github.com/dex/matching-engine/internal/risk"
	"github.com/dex/matching-engine/internal/settlement"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

// liquidationSlippageTolerance bounds how far a liquidation order's fill
// price may deviate from the mark price. The reduce-only order is submitted
// as an IOC limit at this capped price so it cannot fill at an arbitrarily
// bad level in a thin book; any unfilled remainder is then force-closed at
// the mark price by settlement.ClosePosition.
const liquidationSlippageTolerance = 0.01 // 1%

// Engine sweeps futures positions on a timer and force-closes any that have
// fallen below their maintenance margin requirement at the current mark price.
// It also (optionally) alerts on options writer positions whose re-evaluated
// collateral requirement has drifted close to what was originally reserved —
// see checkOptionsMarginCalls's doc comment for why that path is alert-only.
type Engine struct {
	registry   *matching.Registry
	settlement *settlement.FuturesSettlement
	marketdata *marketdata.Service
	symbols    *config.Registry
	checker    *risk.Checker
	ledger     *risk.Ledger
	bus        *events.Bus
	log        *slog.Logger
	// options is nil-safe: when unset (e.g. a deployment with options
	// disabled, or a test harness that never wires it), the options
	// margin-call sweep is simply skipped rather than panicking.
	options *settlement.OptionsSettlement
}

// New creates a liquidation Engine.
func New(registry *matching.Registry, fs *settlement.FuturesSettlement, md *marketdata.Service,
	symbols *config.Registry, checker *risk.Checker, bus *events.Bus, ledger *risk.Ledger) *Engine {
	return &Engine{
		registry:   registry,
		settlement: fs,
		marketdata: md,
		symbols:    symbols,
		checker:    checker,
		ledger:     ledger,
		bus:        bus,
		log:        slog.Default(),
	}
}

// SetOptionsSettlement wires the options margin-call sweep. Optional — call
// once at startup if options trading is enabled; the sweep is skipped
// entirely (nil-safe) if this is never called.
func (e *Engine) SetOptionsSettlement(os *settlement.OptionsSettlement) {
	e.options = os
}

// Run starts the sweep loop; call in a goroutine. Stops when ctx is cancelled.
func (e *Engine) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			e.sweep()
		case <-ctx.Done():
			return
		}
	}
}

func (e *Engine) sweep() {
	allPositions := e.settlement.AllPositions()

	// Separate positions into isolated (checked per-position) and cross
	// (checked per-account/quote, since cross positions share the account's
	// free balance as additional margin).
	type crossKey struct {
		accountID  string
		quoteAsset string
	}
	crossGroups := make(map[crossKey][]*settlement.Position)

	for _, pos := range allPositions {
		if pos.Size.IsZero() {
			continue
		}
		if pos.IsCrossMargin() {
			cfg, err := e.symbols.Get(pos.Symbol, models.Futures)
			if err != nil {
				continue
			}
			k := crossKey{accountID: pos.AccountID, quoteAsset: cfg.QuoteCurrency}
			crossGroups[k] = append(crossGroups[k], pos)
		} else {
			e.checkIsolated(pos)
		}
	}

	// Check each cross-margin account group for aggregate under-capitalisation.
	for k, positions := range crossGroups {
		e.checkCross(positions, k.accountID, k.quoteAsset)
	}

	e.checkOptionsMarginCalls()
}

// optionsMarginCallThresholdPct: warn (without force-closing yet) once a
// short position's equity has fallen to this fraction of its maintenance
// requirement — an early heads-up before the actual liquidation threshold
// (100% of maintenance) is reached, the same two-stage shape real options
// exchanges surface to writers (margin call warning, then liquidation).
const optionsMarginCallThresholdPct = 120

// checkOptionsMarginCalls mark-to-markets every open short (writer) options
// position and force-closes any that have breached maintenance margin — the
// real liquidation path for options sellers (roadmap Phase 2 item 5),
// replacing the earlier alert-only design.
//
// This mirrors checkIsolated/checkCross's shape exactly, with options'
// equivalent inputs:
//   - equity = reservedCollateral (what was actually locked for this
//     position at order time, recomputed via risk.RequiredOptionsMargin
//     against the position's OWN strike/premium — not assumed to be the
//     full cash-secured ceiling) + unrealized PnL (settlement.OptionsPosition.PnL,
//     mark-to-market against the current theoretical/book-blended option
//     price, the same price /option-chain and portfolio Greeks use).
//   - maintenance requirement = risk.RequiredOptionsMargin evaluated at the
//     CURRENT mark (today's floor-model requirement, which is what a fresh
//     order of the same size would need right now).
//
// A writer's reserved collateral was originally sized to the floor model's
// requirement AT OPEN. As the underlying moves against them, PnL erodes
// that equity below the requirement re-evaluated at the new mark — exactly
// analogous to how a futures position's margin+PnL erodes below maintenance
// margin. When that happens here, this force-closes the position: submits a
// reduce-only IOC on the option's own order book first (real fill prices,
// capped at mark ± slippage tolerance, same protection futures liquidation
// uses), then force-settles any unfilled remainder via
// OptionsSettlement.ForceClosePosition at the mark price.
func (e *Engine) checkOptionsMarginCalls() {
	if e.options == nil {
		return
	}
	for _, pos := range e.options.AllPositions() {
		if !pos.Size.IsNegative() {
			continue // only writers (short) carry margin risk; longs already paid in full
		}
		qty := pos.Size.Abs()
		mark, ok := e.optionMark(pos)
		if !ok {
			continue // cannot mark-to-market without a live theoretical/book price
		}
		premiumPerUnit := decimal.Zero
		if qty.IsPositive() {
			// Premium is stored negative for a writer (credit); use its
			// magnitude per unit as the "premium already received" input
			// RequiredOptionsMargin needs, mirroring shortOptionMargin's
			// order-time premium*qty term.
			premiumPerUnit = pos.Premium.Abs().Div(qty)
		}
		reserved := risk.RequiredOptionsMargin(pos.Symbol, pos.OptionType, pos.StrikePrice, qty, premiumPerUnit, pos.QuoteCurrency)
		maintenanceRequired := reserved // the current-mark floor requirement IS the maintenance bar
		equity := reserved.Add(pos.PnL(mark))

		if maintenanceRequired.IsZero() {
			continue
		}
		utilizationPct := maintenanceRequired.Div(equity.Abs().Add(decimal.NewFromFloat(0.00000001))).Mul(decimal.NewFromInt(100))
		if equity.GreaterThanOrEqual(maintenanceRequired) {
			continue // adequately capitalized
		}

		e.log.Warn("options writer margin breached; force-closing",
			"account", pos.AccountID, "symbol", pos.Symbol, "equity", equity, "maintenanceRequired", maintenanceRequired, "mark", mark)
		if e.bus != nil {
			e.bus.Publish(&models.Event{
				Type: models.EventMarginCallAlert, Symbol: pos.Symbol, Market: string(models.Options),
				SequenceNumber: e.bus.NextOutOfBandSequence(),
				MarginCallInfo: &models.MarginCallInfo{
					AccountID: pos.AccountID, Symbol: pos.Symbol, OptionType: pos.OptionType,
					StrikePrice: pos.StrikePrice, Size: pos.Size,
					RequiredMargin: maintenanceRequired, ReservedMargin: reserved, UtilizationPct: utilizationPct,
				},
			})
		}
		e.forceCloseOption(pos, reserved, mark)
	}
}

// optionMark returns the current mark price to value pos against: the
// option's own book mid if it has live two-sided quotes, otherwise the
// theoretical Black-Scholes price using the underlying's live mark — the
// same fallback order /option-chain uses (see cmd/engine/main.go). Returns
// false only when neither is available (no live underlying mark at all).
func (e *Engine) optionMark(pos *settlement.OptionsPosition) (decimal.Decimal, bool) {
	if bookTicker, err := e.marketdata.Ticker(pos.Symbol, models.Options); err == nil && bookTicker.MarkPrice.IsPositive() {
		return bookTicker.MarkPrice, true
	}
	underlying := underlyingFromOptionSymbol(pos.Symbol, pos.QuoteCurrency)
	spotTicker, err := e.marketdata.Ticker(underlying, models.Spot)
	if err != nil || !spotTicker.MarkPrice.IsPositive() {
		return decimal.Zero, false
	}
	spot, _ := spotTicker.MarkPrice.Float64()
	strike, _ := pos.StrikePrice.Float64()
	tYears := time.Until(pos.Expiry).Hours() / 24 / 365
	if tYears <= 0 {
		// Past expiry and not yet swept by ExpiryProcessor (runs on its own
		// 1-minute interval) — value at intrinsic, the only meaningful price
		// for an expired contract.
		theo := pricing.Intrinsic(spot, strike, pos.OptionType == "CALL")
		return decimal.NewFromFloat(theo), true
	}
	const assumedVol = 0.6
	const riskFreeRate = 0.03
	theo := pricing.Price(spot, strike, tYears, assumedVol, riskFreeRate, pos.OptionType == "CALL")
	return decimal.NewFromFloat(theo), true
}

// underlyingFromOptionSymbol extracts the underlying spot symbol from an
// option instrument symbol — duplicated from settlement.underlyingFromSymbol
// (unexported there) and risk.underlyingFromOrderSymbol (takes an *Order,
// not the raw fields this package has); see those two for why each package
// keeps its own tiny copy rather than a shared export.
func underlyingFromOptionSymbol(symbol, quoteCurrency string) string {
	parts := strings.Split(symbol, "-")
	if len(parts) >= 5 {
		return parts[0] + "-" + parts[1]
	}
	if len(parts) >= 1 && quoteCurrency != "" {
		return parts[0] + "-" + quoteCurrency
	}
	return symbol
}

// forceCloseOption submits a reduce-only IOC on the option's own order book
// (real fill prices, capped at mark ± slippage tolerance — same protection
// forceClose uses for futures), then force-settles any unfilled remainder
// via OptionsSettlement.ForceClosePosition at the mark price. reservedCollateral
// is released as part of that force-close (see its doc comment).
func (e *Engine) forceCloseOption(pos *settlement.OptionsPosition, reservedCollateral, mark decimal.Decimal) {
	qty := pos.Size.Abs()
	tol := decimal.NewFromFloat(liquidationSlippageTolerance)
	// Closing a short (buying back): cap the price no higher than mark*(1+tol).
	capPrice := mark.Mul(decimal.NewFromInt(1).Add(tol))
	if !capPrice.IsPositive() {
		capPrice = decimal.NewFromFloat(0.0001)
	}

	order := &models.Order{
		ID: uuid.NewString(), AccountID: pos.AccountID, Symbol: pos.Symbol, Market: models.Options,
		Side: models.Buy, Type: models.IOC, Price: capPrice, Quantity: qty,
		OptionType: pos.OptionType, StrikePrice: pos.StrikePrice, Expiry: pos.Expiry, QuoteCurrency: pos.QuoteCurrency,
		TimeInForce: models.GTC, Status: models.StatusPending, CreatedAt: time.Now(),
		InternalLiquidation: true,
	}
	if _, err := e.registry.Submit(order); err != nil {
		e.log.Error("options liquidation submit failed", "account", pos.AccountID, "symbol", pos.Symbol, "error", err)
	}

	// The IOC above closes whatever it could fill through Settle at real
	// trade prices, shrinking pos.Size accordingly. Only force-settle the
	// remainder at mark if the position still has any size left, mirroring
	// forceClose's identical remainder-only rule for futures.
	remaining := e.options.GetPosition(pos.AccountID, pos.Symbol, pos.StrikePrice, pos.Expiry, pos.OptionType)
	if remaining == nil || remaining.Size.IsZero() {
		return
	}
	pnl, err := e.options.ForceClosePosition(pos.AccountID, pos.Symbol, pos.StrikePrice, pos.Expiry, pos.OptionType, reservedCollateral, mark)
	if err != nil {
		e.log.Error("options force-close settlement failed; position retained for reconciliation",
			"account", pos.AccountID, "symbol", pos.Symbol, "error", err)
		return
	}
	e.log.Warn("options position liquidated", "account", pos.AccountID, "symbol", pos.Symbol, "size", qty.String(), "pnl", pnl.String())
	if e.bus != nil {
		e.bus.Publish(&models.Event{
			Type: models.EventLiquidation, Symbol: pos.Symbol, Market: string(models.Options),
			SequenceNumber: e.bus.NextOutOfBandSequence(),
			Liquidation: &models.Liquidation{
				AccountID: pos.AccountID, Symbol: pos.Symbol, Side: models.Sell, Size: qty, MarkPrice: mark,
			},
		})
	}
}

// checkIsolated evaluates a single isolated-margin position for liquidation.
func (e *Engine) checkIsolated(pos *settlement.Position) {
	cfg, err := e.symbols.Get(pos.Symbol, models.Futures)
	if err != nil || cfg.MaintenanceMarginRate.IsZero() {
		return
	}
	mark := e.markPrice(pos.Symbol)
	if mark.IsZero() {
		return
	}
	if pos.MarginRatio(mark).GreaterThanOrEqual(cfg.MaintenanceMarginRate) {
		return
	}
	e.forceClose(pos, mark, cfg)
}

// checkCross evaluates whether an account's aggregate cross-margin equity
// has fallen below the total maintenance margin across all its cross
// positions. If so, it force-closes positions (largest loss first) until the
// account is safe or all cross positions are closed.
//
// Cross equity = sum(position margin) + sum(unrealised PnL) + available
// balance. The available balance is the free balance (total − reserved −
// already-debited position margins) which acts as shared collateral for all
// cross positions on that quote asset.
func (e *Engine) checkCross(positions []*settlement.Position, accountID, quoteAsset string) {
	// Fetch mark prices and per-position maintenance margins.
	marks := make(map[string]decimal.Decimal, len(positions))
	var totalMargin, totalPnL, totalMM decimal.Decimal
	for _, pos := range positions {
		mark := e.markPrice(pos.Symbol)
		if mark.IsZero() {
			return // cannot evaluate without a mark price
		}
		marks[pos.Symbol] = mark
		totalMargin = totalMargin.Add(pos.Margin)
		totalPnL = totalPnL.Add(pos.PnL(mark))
		cfg, err := e.symbols.Get(pos.Symbol, models.Futures)
		if err != nil || cfg.MaintenanceMarginRate.IsZero() {
			return
		}
		totalMM = totalMM.Add(pos.MaintenanceMargin(mark, cfg.MaintenanceMarginRate))
	}

	available := e.ledger.Available(accountID, quoteAsset)
	equity := totalMargin.Add(totalPnL).Add(available)

	if equity.GreaterThanOrEqual(totalMM) {
		return // account is adequately capitalised
	}

	e.log.Warn("cross-margin account under maintenance; liquidating",
		"account", accountID, "quote", quoteAsset,
		"equity", equity, "maintenanceMargin", totalMM)

	// Sort by unrealised PnL ascending (most negative / biggest loss first).
	sort.Slice(positions, func(i, j int) bool {
		return positions[i].PnL(marks[positions[i].Symbol]).
			LessThan(positions[j].PnL(marks[positions[j].Symbol]))
	})

	for _, pos := range positions {
		// Re-check the account after each close: realising one position's
		// loss changes the available balance, which may bring the account
		// back above the (now reduced) maintenance margin.
		if e.crossSafe(positions, accountID, quoteAsset, marks) {
			break
		}
		// Skip positions already closed by a previous iteration.
		remaining := e.settlement.GetPosition(pos.AccountID, pos.Symbol)
		if remaining == nil || remaining.Size.IsZero() {
			continue
		}
		cfg, err := e.symbols.Get(pos.Symbol, models.Futures)
		if err != nil {
			continue
		}
		e.forceClose(pos, marks[pos.Symbol], cfg)
	}
}

// crossSafe recomputes cross equity vs total maintenance margin for the
// account's still-open cross positions and returns true if the account is
// no longer in liquidation.
func (e *Engine) crossSafe(positions []*settlement.Position, accountID, quoteAsset string,
	marks map[string]decimal.Decimal) bool {
	var totalMargin, totalPnL, totalMM decimal.Decimal
	for _, pos := range positions {
		cur := e.settlement.GetPosition(pos.AccountID, pos.Symbol)
		if cur == nil || cur.Size.IsZero() {
			continue
		}
		mark := marks[pos.Symbol]
		totalMargin = totalMargin.Add(cur.Margin)
		totalPnL = totalPnL.Add(cur.PnL(mark))
		cfg, err := e.symbols.Get(pos.Symbol, models.Futures)
		if err != nil || cfg.MaintenanceMarginRate.IsZero() {
			continue
		}
		totalMM = totalMM.Add(cur.MaintenanceMargin(mark, cfg.MaintenanceMarginRate))
	}
	if totalMM.IsZero() {
		return true
	}
	available := e.ledger.Available(accountID, quoteAsset)
	equity := totalMargin.Add(totalPnL).Add(available)
	return equity.GreaterThanOrEqual(totalMM)
}

// markPrice returns the blended mark price for a futures symbol, or zero if
// unavailable.
func (e *Engine) markPrice(symbol string) decimal.Decimal {
	ticker, err := e.marketdata.Ticker(symbol, models.Futures)
	if err != nil {
		return decimal.Zero
	}
	return ticker.MarkPrice
}

// forceClose submits a reduce-only IOC limit order (capped at mark ±
// slippage tolerance) to close the position through the matching engine at
// real fill prices, then force-closes any unfilled remainder at the mark
// price via settlement.ClosePosition.
func (e *Engine) forceClose(pos *settlement.Position, markPrice decimal.Decimal, cfg *config.SymbolConfig) {
	originalSize := pos.Size

	// closingSide is the opposite side of the held position.
	closingSide := models.Sell
	if pos.Side == models.Sell {
		closingSide = models.Buy
	}

	// Cap the fill price to mark ± slippage tolerance to protect against
	// filling at arbitrarily bad prices in a thin book.
	capPrice := markPrice
	tol := decimal.NewFromFloat(liquidationSlippageTolerance)
	if closingSide == models.Sell {
		// Closing a long: sell no lower than mark*(1-tol).
		capPrice = markPrice.Mul(decimal.NewFromInt(1).Sub(tol))
	} else {
		// Closing a short: buy no higher than mark*(1+tol).
		capPrice = markPrice.Mul(decimal.NewFromInt(1).Add(tol))
	}

	order := &models.Order{
		ID:                  uuid.NewString(),
		AccountID:           pos.AccountID,
		Symbol:              pos.Symbol,
		Market:              models.Futures,
		Side:                closingSide,
		Type:                models.IOC,
		Price:               capPrice,
		Quantity:            originalSize.Abs(),
		ReduceOnly:          true,
		TimeInForce:         models.GTC,
		Status:              models.StatusPending,
		CreatedAt:           time.Now(),
		InternalLiquidation: true,
	}

	if err := e.checker.Check(order); err != nil {
		e.log.Error("liquidation risk check failed", "account", pos.AccountID, "symbol", pos.Symbol, "error", err)
	} else if _, err := e.registry.Submit(order); err != nil {
		e.log.Error("liquidation submit failed", "account", pos.AccountID, "symbol", pos.Symbol, "error", err)
	}

	// The reduce-only IOC order above already closes the filled portion of
	// the position through Settle/applyFill at the actual fill prices. Only
	// force-close the remainder at the mark price if the position still
	// exists (i.e. the IOC order did not fully fill it). This avoids
	// realizing PnL twice or at inconsistent prices for the same quantity.
	if remaining := e.settlement.GetPosition(pos.AccountID, pos.Symbol); remaining != nil && !remaining.Size.IsZero() {
		e.settlement.ClosePosition(pos.AccountID, pos.Symbol, cfg.QuoteCurrency, markPrice)
	}

	// Compute the actual closed size for the event (may be less than the
	// original if the position was only partially closed due to an error).
	closedSize := originalSize
	if remaining := e.settlement.GetPosition(pos.AccountID, pos.Symbol); remaining != nil {
		closedSize = originalSize.Sub(remaining.Size).Abs()
	}
	if closedSize.IsZero() {
		return
	}

	e.log.Warn("position liquidated", "account", pos.AccountID, "symbol", pos.Symbol, "size", closedSize.String())
	if e.bus != nil {
		e.bus.Publish(&models.Event{
			Type:           models.EventLiquidation,
			Symbol:         pos.Symbol,
			Market:         string(models.Futures),
			SequenceNumber: e.bus.NextOutOfBandSequence(),
			Liquidation: &models.Liquidation{
				AccountID: pos.AccountID,
				Symbol:    pos.Symbol,
				Side:      pos.Side,
				Size:      closedSize,
				MarkPrice: markPrice,
			},
		})
	}
}
