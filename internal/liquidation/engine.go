// Package liquidation periodically sweeps open futures positions and force-
// closes any that have breached their maintenance margin requirement.
package liquidation

import (
	"context"
	"log/slog"
	"sort"
	"time"

	"github.com/dex/matching-engine/internal/config"
	"github.com/dex/matching-engine/internal/events"
	"github.com/dex/matching-engine/internal/marketdata"
	"github.com/dex/matching-engine/internal/matching"
	"github.com/dex/matching-engine/internal/models"
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

// optionsMarginCallThresholdPct: alert once a short position's re-evaluated
// margin requirement reaches this fraction of what was originally reserved
// for it (100% would mean the requirement has grown all the way back up to
// full cash-secured — the ceiling the floor model can never exceed, so 100%
// is the worst case, not a breach of it). 80% gives risk ops visibility
// before a position is one further adverse move away from full cash-secured.
const optionsMarginCallThresholdPct = 80

// checkOptionsMarginCalls re-evaluates every open short (writer) options
// position's collateral requirement against the CURRENT underlying mark and
// alerts (does not force-close) when it has drifted close to the original
// cash-secured reservation.
//
// This is deliberately alert-only, unlike checkIsolated/checkCross for
// futures: a writer's collateral is reserved in full (cash-secured, capped
// by risk.RequiredOptionsMargin at order time) and only ever released at
// expiry (settlement.ExpiryProcessor) — it is never partially debited the
// way futures margin is marked-to-market and eroded by unrealized losses.
// Because the floor-model requirement (risk.RequiredOptionsMargin) is
// mathematically bounded above by that same cash-secured amount, a position
// can never actually become UNDER-collateralized relative to what is locked
// specifically for it — there is no insolvency state to force-close out of.
// What CAN happen is the position's real risk growing toward that ceiling as
// the underlying moves against the writer, which is exactly what this alert
// surfaces for risk ops/admin visibility ahead of expiry, per the roadmap's
// explicit fallback ("document cash-secured-only as v1 scope" when a forced-
// close path isn't meaningful).
func (e *Engine) checkOptionsMarginCalls() {
	if e.options == nil {
		return
	}
	for _, pos := range e.options.AllPositions() {
		if !pos.Size.IsNegative() {
			continue // only writers (short) carry margin risk; longs already paid in full
		}
		qty := pos.Size.Abs()
		reserved := pos.StrikePrice.Mul(qty) // the cash-secured ceiling locked at order time
		if !reserved.IsPositive() {
			continue
		}
		premiumPerUnit := decimal.Zero
		if qty.IsPositive() {
			// Premium is stored negative for a writer (credit); use its
			// magnitude per unit as the "premium already received" input
			// RequiredOptionsMargin needs, mirroring shortOptionMargin's
			// order-time premium*qty term.
			premiumPerUnit = pos.Premium.Abs().Div(qty)
		}
		required := risk.RequiredOptionsMargin(pos.Symbol, pos.OptionType, pos.StrikePrice, qty, premiumPerUnit, pos.QuoteCurrency)
		utilization := required.Div(reserved).Mul(decimal.NewFromInt(100))
		if utilization.LessThan(decimal.NewFromInt(optionsMarginCallThresholdPct)) {
			continue
		}
		e.log.Warn("options writer margin call: requirement approaching reserved collateral",
			"account", pos.AccountID, "symbol", pos.Symbol, "required", required, "reserved", reserved,
			"utilizationPct", utilization.StringFixed(1))
		if e.bus == nil {
			continue
		}
		e.bus.Publish(&models.Event{
			Type:           models.EventMarginCallAlert,
			Symbol:         pos.Symbol,
			Market:         string(models.Options),
			SequenceNumber: e.bus.NextOutOfBandSequence(),
			MarginCallInfo: &models.MarginCallInfo{
				AccountID: pos.AccountID, Symbol: pos.Symbol, OptionType: pos.OptionType,
				StrikePrice: pos.StrikePrice, Size: pos.Size,
				RequiredMargin: required, ReservedMargin: reserved, UtilizationPct: utilization,
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
