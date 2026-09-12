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

// optionsLiquidationSlippageTolerance is the same cap for OPTIONS force-closes,
// deliberately much wider than the futures number above.
//
// Both paths try a reduce-only IOC on the real book first and fall back to a
// theoretical mark only for whatever doesn't fill. That fallback is the part
// worth avoiding: it settles a real liquidation against a MODEL price rather
// than a price anyone actually traded at, making the exchange — not the
// market — the counterparty of last resort for that fill. Whoever is on the
// wrong side of the model's error absorbs the gap.
//
// A 1% cap is reasonable on a deep futures book, where 1% away from mark is
// still well inside real resting liquidity. Option books here are far thinner
// and their quotes sit much further from theoretical value: the options market
// maker's own minimum half-spread is 100 bps of premium and it defaults to 300
// (see strategy.newOptionsMarketMaker), so a 1% cap frequently cannot even
// reach the desk's own resting quote — the IOC fills nothing and every
// liquidation drops to the theoretical mark by default. Widening to 10% lets
// the IOC actually cross a real quote in the common case, so the fallback
// becomes what it was meant to be (a last resort) instead of the normal path.
//
// This trades a wider worst-case fill for a real one. That is the right trade
// for a liquidation: a genuine traded price 10% from mark is more defensible
// than a model price that no counterparty ever agreed to, and the position is
// being force-closed precisely because it can no longer support itself.
const optionsLiquidationSlippageTolerance = 0.10 // 10%

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
	// liquidationFee resolves the (discount-adjusted) liquidation penalty
	// rate for an account — see feeconfig.KeyLiquidation. May be nil (no
	// liquidation fee charged), e.g. in tests that predate this feature.
	liquidationFee func(accountID string) decimal.Decimal
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

// SetLiquidationFee wires the (discount-adjusted) liquidation penalty rate
// lookup. Optional — call once at startup; if never called, liquidations
// charge no fee (matching this package's pre-existing behavior).
func (e *Engine) SetLiquidationFee(fn func(accountID string) decimal.Decimal) {
	e.liquidationFee = fn
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

// optionsMarginCallWarningLossPct: warn (without force-closing) once a short
// position has burned through this percentage of its loss buffer — the gap
// between the collateral currently required for it and the maintenance bar at
// which it gets liquidated (see optionsMaintenanceMarginPct). 75 means "three
// quarters of the cushion is gone"; the remaining quarter is the writer's
// window to post collateral or close out.
//
// This must stay strictly below 100, or the warning would coincide with (or
// follow) the liquidation and give no advance notice at all — the exact
// failure this whole two-stage design exists to prevent.
// TestWarningFiresBeforeLiquidation enforces the ordering.
//
// WHY A LOSS-BUFFER RATIO, NOT AN EQUITY RATIO. The obvious formulation —
// warn when equity < 120% of the maintenance requirement — cannot work in
// this model, and the constant that expressed it (optionsMarginCallThresholdPct
// = 120) was declared and never referenced anywhere, so nothing at all
// happened until equity fell below 100%, at which point the "warning" event
// and the force-close were emitted in the same instant. Writers got no
// advance notice whatsoever; the warning was in practice a liquidation
// receipt.
//
// Simply wiring that 120 in would have been worse than leaving it dead. Here
// equity is defined as reserved + unrealized PnL, and reserved IS the
// maintenance requirement (both are risk.RequiredOptionsMargin at the current
// mark), so:
//
//	equity / maintenance = 1 + PnL/reserved
//
// A writer's PnL is capped at the premium they received, which is tiny next
// to the 20%-of-underlying margin floor. Measured on a real position (60k
// strike call, 500 premium): even the best possible case — option worthless,
// writer keeps every cent of premium — reaches only ratio 1.11, and a writer
// at breakeven sits at exactly 1.00. A 120% band would therefore hold EVERY
// healthy writer in permanent warning, which is worse than no warning at all:
// an alert that always fires is one nobody reads.
//
// The buffer ratio is bounded by construction instead. It is 0% when the
// position is at its opening collateral and 100% at the liquidation point,
// regardless of strike, premium size, or how the margin floor is calibrated.
//
// The 75 is still an uncalibrated placeholder (see OPTIONS-KNOWN-ISSUES.md
// #4) — but it is a placeholder for a quantity that can actually reach its
// threshold, and it is now genuinely wired in.
const optionsMarginCallWarningLossPct = 75

// warningLossFraction is optionsMarginCallWarningLossPct as a ratio (75 -> 0.75).
var warningLossFraction = decimal.NewFromInt(optionsMarginCallWarningLossPct).Div(decimal.NewFromInt(100))

// optionsMaintenanceMarginPct: a writer is liquidated once equity falls below
// this percentage of the collateral currently required for the position. 50
// means "half the collateral is gone".
//
// WHY THIS EXISTS. The maintenance bar used to be `maintenanceRequired :=
// reserved` — the full current-mark requirement. Combined with
// `equity := reserved + PnL`, the liquidation test
//
//	equity < maintenanceRequired
//
// reduced algebraically to `PnL < 0`: a writer was force-closed the moment the
// position showed ANY unrealized loss large enough to survive rounding. That
// is not a maintenance margin, it is a stop-loss at breakeven, and it fired
// while the account was nowhere near unable to pay.
//
// Measured on a real position (60k strike call, 1 contract, 500 premium) at
// spot 61,000 — barely $1,000 in the money — the writer held equity of
// 11,836 against a required 12,700 and was liquidated having consumed only
// ~7% of their collateral. They still had ~12,000 posted against a ~1,000
// loss. It also made the warning stage almost unreachable: positions crossed
// into liquidation long before consuming 75% of their loss buffer, so the
// early warning was shadowed by the very trigger it was meant to precede.
//
// A maintenance bar set to a FRACTION of the requirement restores the normal
// two-tier shape every real venue uses: initial margin is what you must post
// to open, maintenance is the lower level you must stay above to keep the
// position, and the gap between them is the room the market is allowed to
// move against you before anyone intervenes. With 50%, a writer is closed
// once half their collateral is genuinely consumed — and the 75% warning at
// last sits strictly inside that gap, firing while the position is still
// solvent and closable.
//
// Like the warning threshold, 50 is a starting value rather than one derived
// from this exchange's own volatility and liquidation-slippage data (see
// OPTIONS-KNOWN-ISSUES.md #4); both should be retuned once there is real
// trading history. It is chosen conservatively: the exchange still closes the
// position with half the collateral intact, which is ample cover for the
// force-close slippage tolerance.
const optionsMaintenanceMarginPct = 50

// maintenanceFraction is optionsMaintenanceMarginPct as a ratio (50 -> 0.5).
var maintenanceFraction = decimal.NewFromInt(optionsMaintenanceMarginPct).Div(decimal.NewFromInt(100))

// maintenanceBar is the equity level below which a writer is force-closed:
// a fraction of the collateral their position currently requires. Extracted
// as a pure function so the relationship between the maintenance bar and the
// warning threshold is directly testable.
func maintenanceBar(reserved decimal.Decimal) decimal.Decimal {
	return reserved.Mul(maintenanceFraction)
}

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
		maintenanceRequired := maintenanceBar(reserved)
		equity := reserved.Add(pos.PnL(mark))

		if maintenanceRequired.IsZero() {
			continue
		}
		utilizationPct := maintenanceRequired.Div(equity.Abs().Add(decimal.NewFromFloat(0.00000001))).Mul(decimal.NewFromInt(100))

		// Three bands, checked strongest-condition-first.
		//
		// Previously there were only two — "equity >= maintenance: skip" and
		// everything else: warn AND liquidate together — which collapsed the
		// intended two-stage design into one and meant a writer's first
		// notification arrived at the same moment their position was closed.
		switch {
		case equity.LessThan(maintenanceRequired):
			// Band 3: undercapitalized. Force-close.
			e.log.Warn("options writer margin breached; force-closing",
				"account", pos.AccountID, "symbol", pos.Symbol, "equity", equity, "maintenanceRequired", maintenanceRequired, "mark", mark)
			e.publishMarginCall(pos, models.MarginCallLiquidated, maintenanceRequired, reserved, equity, utilizationPct)
			e.forceCloseOption(pos, reserved, mark)

		case lossBufferConsumed(reserved, equity).GreaterThanOrEqual(warningLossFraction):
			// Band 2: still solvent, but most of the loss buffer is gone.
			// Publish the early heads-up and leave the position alone — this
			// is the window in which the writer can post collateral or close
			// out voluntarily, which is the entire point of a margin call.
			e.log.Info("options writer approaching maintenance margin; warning issued",
				"account", pos.AccountID, "symbol", pos.Symbol, "equity", equity,
				"maintenanceRequired", maintenanceRequired,
				"lossBufferConsumedPct", lossBufferConsumed(reserved, equity).Mul(decimal.NewFromInt(100)).StringFixed(1),
				"mark", mark)
			e.publishMarginCall(pos, models.MarginCallWarning, maintenanceRequired, reserved, equity, utilizationPct)

		default:
			// Band 1: adequately capitalized.
			continue
		}
	}
}

// lossBufferConsumed reports how much of a writer's loss buffer is gone, as a
// fraction in [0,1]: 0 when the position still holds its full collateral, 1 at
// the liquidation point.
//
// The buffer is the distance between full collateral and the maintenance bar
// — i.e. exactly how far the market may move against the writer before anyone
// intervenes:
//
//	buffer   = reserved - maintenanceBar(reserved)
//	consumed = (reserved - equity) / buffer
//
// Measuring against the buffer rather than against `reserved` is what keeps
// the two thresholds commensurable. An earlier version of this divided by
// `reserved`, which silently put the 75% warning at equity = 25% of reserved
// — BELOW the 50% liquidation bar — so every position was force-closed before
// its warning could ever fire, reintroducing exactly the no-advance-notice
// behavior this two-stage design exists to remove. Dividing by the buffer
// makes both thresholds points on the same 0-to-1 scale, so a warning
// fraction < 1 provably precedes liquidation (see
// TestWarningFiresBeforeLiquidation).
//
// Extracted as a pure function so the band arithmetic is directly testable
// without an options settlement engine, a mark source, or a live position.
// See optionsMarginCallWarningLossPct for why the threshold is expressed
// against this bounded quantity rather than an equity/maintenance ratio.
func lossBufferConsumed(reserved, equity decimal.Decimal) decimal.Decimal {
	// A zero/negative requirement has nothing left to consume; report fully
	// consumed so such a position can never sit silently in the healthy band.
	if !reserved.IsPositive() {
		return decimal.NewFromInt(1)
	}
	buffer := reserved.Sub(maintenanceBar(reserved))
	if !buffer.IsPositive() {
		// Degenerate config (maintenance == full requirement): no room to
		// warn in, so treat anything not fully collateralized as consumed.
		if equity.GreaterThanOrEqual(reserved) {
			return decimal.Zero
		}
		return decimal.NewFromInt(1)
	}
	consumed := reserved.Sub(equity).Div(buffer)
	// A profitable writer (equity above reserved) has consumed nothing.
	if consumed.IsNegative() {
		return decimal.Zero
	}
	return consumed
}

// publishMarginCall emits one EventMarginCallAlert carrying the stage that
// says whether the position was merely warned about or actually closed.
// Extracted so both call sites populate MarginCallInfo identically — the two
// differ only in Stage, and a consumer reading the wrong field would
// misinterpret a warning as a liquidation or vice versa.
func (e *Engine) publishMarginCall(
	pos *settlement.OptionsPosition,
	stage models.MarginCallStage,
	maintenanceRequired, reserved, equity, utilizationPct decimal.Decimal,
) {
	if e.bus == nil {
		return
	}
	e.bus.Publish(&models.Event{
		Type: models.EventMarginCallAlert, Symbol: pos.Symbol, Market: string(models.Options),
		SequenceNumber: e.bus.NextOutOfBandSequence(),
		MarginCallInfo: &models.MarginCallInfo{
			AccountID: pos.AccountID, Symbol: pos.Symbol, OptionType: pos.OptionType,
			StrikePrice: pos.StrikePrice, Size: pos.Size, Stage: stage,
			RequiredMargin: maintenanceRequired, ReservedMargin: reserved,
			Equity: equity, UtilizationPct: utilizationPct,
		},
	})
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
// (real fill prices, capped at mark ± optionsLiquidationSlippageTolerance —
// the same shape of protection forceClose uses for futures, but with a much
// wider cap suited to thin option books; see that constant's doc), then
// force-settles any unfilled remainder via OptionsSettlement.ForceClosePosition
// at the mark price. reservedCollateral is released as part of that
// force-close (see its doc comment).
func (e *Engine) forceCloseOption(pos *settlement.OptionsPosition, reservedCollateral, mark decimal.Decimal) {
	qty := pos.Size.Abs()
	// Options use their own, much wider tolerance — see the constant's doc for
	// why the futures 1% cap made the theoretical-mark fallback the normal path
	// here rather than the last resort it was meant to be.
	tol := decimal.NewFromFloat(optionsLiquidationSlippageTolerance)
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
		fee := decimal.Zero
		if e.liquidationFee != nil {
			notional := markPrice.Mul(remaining.Size.Abs())
			fee = notional.Mul(e.liquidationFee(pos.AccountID))
		}
		e.settlement.ClosePosition(pos.AccountID, pos.Symbol, cfg.QuoteCurrency, markPrice, fee)
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
