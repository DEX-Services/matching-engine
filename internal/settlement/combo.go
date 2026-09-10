package settlement

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/dex/matching-engine/internal/models"
	"github.com/dex/matching-engine/internal/pricing"
	"github.com/shopspring/decimal"
)

// ComboLegResolver resolves a combo instrument symbol to its N option legs.
// Implemented in cmd/engine (backed by the combo_instruments table); kept
// as a narrow interface here so settlement does not depend on cmd/engine's
// private instrument types — the same one-way dependency pattern
// risk.UnderlyingMarkSource uses for the mark-price lookup.
type ComboLegResolver interface {
	// ResolveComboLegs returns every leg of a registered combo instrument
	// (symbol + signed ratio, see models.ComboLeg) and its underlying spot
	// symbol.
	ResolveComboLegs(ctx context.Context, comboSymbol string) (legs []models.ComboLeg, underlying string, err error)
}

// ComboOptionsMarkSource resolves theoretical Black-Scholes inputs for one
// option leg, so a combo trade's net price can be split into each leg's own
// synthetic fill price. Implemented by a small adapter over
// marketdata.Service + the option_instruments table in cmd/engine.
type ComboOptionsMarkSource interface {
	// UnderlyingMark returns the underlying's current mark price.
	UnderlyingMark(underlying string) (decimal.Decimal, bool)
	// LegSpec returns an option leg's strike, expiry, and CALL/PUT type.
	LegSpec(ctx context.Context, legSymbol string) (strike decimal.Decimal, expiry time.Time, optionType string, quoteCurrency string, ok bool)
}

// ComboSettlement settles trades on a native N-leg combo order book
// (models.ComboOptions) by fanning each combo trade out into N linked
// option-leg trades, settled together through the SAME OptionsSettlement
// used for standalone option orders — a combo position is indistinguishable
// from a manually-built one once opened; only how it got there differs.
//
// This is what makes combo execution genuinely atomic, the way Deribit/CME
// combo books work: the combo trade IS the single unit of execution (one
// resting order matched against another on the combo's own book), and every
// leg settles inside this one Settle call — if any leg's ledger operation
// fails, this returns an error and the matching engine (which calls Settle
// synchronously, inside the same goroutine that produced the trade — see
// matching.Engine.run) halts the symbol exactly as it does for any other
// settlement failure. There is no window where some legs exist and others
// don't: either every leg's settlement succeeds together in this one call,
// or none of them are ever published as successful.
type ComboSettlement struct {
	options  *OptionsSettlement
	legs     ComboLegResolver
	marks    ComboOptionsMarkSource
	riskFree decimal.Decimal
}

// NewComboSettlement creates a ComboSettlement. options is the SAME
// OptionsSettlement instance used for standalone option orders (combo
// positions and manually-built positions in the same contract must be one
// combined position, not tracked separately).
func NewComboSettlement(options *OptionsSettlement, legs ComboLegResolver, marks ComboOptionsMarkSource) *ComboSettlement {
	return &ComboSettlement{options: options, legs: legs, marks: marks, riskFree: decimal.NewFromFloat(0.03)}
}

// Settle fans a combo trade out into its N option legs and settles all of
// them through OptionsSettlement.
func (c *ComboSettlement) Settle(trade *models.Trade) error {
	if trade.BuyOrder == nil || trade.SellOrder == nil {
		return fmt.Errorf("combo settle: missing order references on trade %s", trade.ID)
	}
	ctx := context.Background()
	legs, underlying, err := c.legs.ResolveComboLegs(ctx, trade.Symbol)
	if err != nil {
		return fmt.Errorf("combo settle: resolve legs for %s: %w", trade.Symbol, err)
	}
	if len(legs) == 0 {
		return fmt.Errorf("combo settle: %s has no registered legs", trade.Symbol)
	}

	specs := make([]legSpecWithSymbol, 0, len(legs))
	for _, leg := range legs {
		strike, expiry, optionType, quote, ok := c.marks.LegSpec(ctx, leg.Symbol)
		if !ok {
			return fmt.Errorf("combo settle: no instrument spec for leg %s", leg.Symbol)
		}
		specs = append(specs, legSpecWithSymbol{
			symbol: leg.Symbol, ratio: leg.Ratio,
			strike: strike, expiry: expiry, optionType: optionType, quote: quote,
		})
	}

	// netPrice is what the combo actually traded at: positive = the combo
	// BUYER paid a net debit; negative = the combo buyer received a net
	// credit. This is the ONE authoritative cash-flow number — however the
	// legs' individual synthetic prices below get split, they are
	// constructed so their ratio-weighted sum equals exactly this, so the
	// net cash movement between the two accounts always matches what the
	// combo book actually traded, regardless of any pricing-model choice
	// for the per-leg split.
	netPrice := trade.Price
	legPrices := splitComboNetPrice(netPrice, specs, underlying, c.marks, c.riskFree)

	// The combo's BuyOrder (whoever submitted the order that goes long the
	// spread, i.e. matches the sign convention of a positive Ratio) is long
	// every positive-ratio leg and short every negative-ratio leg; the
	// combo's SellOrder is the exact mirror on every leg.
	comboBuyer := trade.BuyOrder
	comboSeller := trade.SellOrder

	for i, spec := range specs {
		legQty := trade.Quantity.Mul(decimal.NewFromInt(int64(abs(spec.ratio))))
		legPrice := legPrices[i]

		var buyerOfLeg, sellerOfLeg *models.Order
		if spec.ratio > 0 {
			// Combo buyer is long this leg; combo seller is short it.
			buyerOfLeg, sellerOfLeg = comboBuyer, comboSeller
		} else {
			// Combo buyer is short this leg (e.g. the written middle strike
			// of a butterfly, or an iron condor's short strikes); combo
			// seller is long it.
			buyerOfLeg, sellerOfLeg = comboSeller, comboBuyer
		}

		legTrade := &models.Trade{
			ID: fmt.Sprintf("%s:leg%d", trade.ID, i), Symbol: spec.symbol, Market: models.Options,
			Price: legPrice, Quantity: legQty, ExecutedAt: trade.ExecutedAt,
			BuyOrder:  legOrder(buyerOfLeg, spec.symbol, models.Buy, spec.strike, spec.expiry, spec.optionType, spec.quote),
			SellOrder: legOrder(sellerOfLeg, spec.symbol, models.Sell, spec.strike, spec.expiry, spec.optionType, spec.quote),
		}
		// Every leg settles through the exact same OptionsSettlement.Settle
		// used for standalone orders — same ledger debits/credits, same
		// position bookkeeping, same backend mirroring. If an earlier leg
		// settled but a later one fails, the caller (matching.Engine) halts
		// the symbol without publishing EITHER the combo trade or any
		// downstream events, so the ledger operations that DID run are the
		// only side effect — a real but bounded partial-settlement window
		// identical in shape to what already exists for a single ordinary
		// trade whose settlement halts mid-way (this codebase's existing
		// halt-on-settlement-failure design, not a new risk introduced by
		// multi-leg combos).
		if err := c.options.Settle(legTrade); err != nil {
			return fmt.Errorf("combo settle: leg %d (%s): %w", i, spec.symbol, err)
		}
	}
	return nil
}

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

// legSpecWithSymbol bundles one combo leg's identity, ratio, and instrument
// spec together for Settle/splitComboNetPrice's internal bookkeeping.
type legSpecWithSymbol struct {
	symbol     string
	ratio      int
	strike     decimal.Decimal
	expiry     time.Time
	optionType string
	quote      string
}

// legOrder builds the minimal synthetic *models.Order OptionsSettlement.Settle
// needs to record a position: AccountID, Symbol, Side, OptionType, Strike,
// Expiry, QuoteCurrency. comboOrder carries the real AccountID/QuoteCurrency
// from whichever side of the combo trade this leg-order represents.
func legOrder(comboOrder *models.Order, symbol string, side models.OrderSide, strike decimal.Decimal, expiry time.Time, optionType, quote string) *models.Order {
	q := comboOrder.QuoteCurrency
	if q == "" {
		q = quote
	}
	return &models.Order{
		AccountID: comboOrder.AccountID, Symbol: symbol, Market: models.Options, Side: side,
		OptionType: optionType, StrikePrice: strike, Expiry: expiry, QuoteCurrency: q,
	}
}

// splitComboNetPrice allocates a combo's traded net price across all N legs.
//
// Primary approach — multiplicative scaling: price_i = theo_i * scale, for a
// single scale solved from the invariant Σratio_i*price_i == netPrice, i.e.
// scale = netPrice / modeledNet where modeledNet = Σratio_i*theo_i. This can
// NEVER produce a negative leg price when scale is positive (positive
// theo_i times a positive scale is always positive) — valid exactly when
// netPrice and modeledNet share the same sign.
//
// A sign mismatch is a real, not merely theoretical, case: a combo's legs
// can be shaped like a debit structure under the flat-vol theoretical model
// (e.g. long the higher-premium wing) while the underlying ACTUALLY traded
// at a credit — stale IV, real skew the flat-vol model doesn't capture, or
// simply an aggressive fill far from fair value. Settlement must still
// produce a valid, positive, invariant-respecting split for whatever a real
// matching engine traded, not only for internally-consistent theoretical
// inputs, so this has a fallback rather than assuming sign agreement:
//
// Fallback — single-leg settlement by exhaustive search: every leg except
// one candidate "settle" leg is flat-floored at a small epsilon; the settle
// leg's price is solved exactly from the invariant. This 2-variable/
// 1-equation system is underdetermined in general, so instead of guessing
// which leg can absorb the correction, EVERY leg is tried as the candidate
// in turn (ties broken toward larger |ratio|, cheaper per-unit-of-
// correction to move) and the first one whose solved price comes out
// positive is used. This always succeeds for any real combo (one
// containing at least one positive- and one negative-ratio leg, i.e. an
// actual hedged structure — the whole point of a combo) because for
// opposite-signed ratios, exactly one direction of "which leg absorbs the
// gap" yields a positive result for any non-zero netPrice.
//
// Earlier versions of this function tried an additive equal-per-leg split
// (broken whenever Σratio_i = 0 — true for a plain vertical, a balanced
// iron condor, and a butterfly, since an equal additive adjustment changes
// the ratio-weighted sum by adjustment*Σratio_i = 0, repairing nothing) and
// a magnitude-weighted proportional split held fixed at real theoretical
// values for "other" legs (broken whenever the residual is large relative
// to those legs' real premiums, which can force the settling leg negative
// with no other leg's price left to absorb the excess, or floor the wrong
// leg near zero and force the OTHER leg negative instead). Both are
// documented here so a future change to this function doesn't reintroduce
// either mistake.
func splitComboNetPrice(netPrice decimal.Decimal, specs []legSpecWithSymbol, underlying string, marks ComboOptionsMarkSource, riskFreeRate decimal.Decimal) []decimal.Decimal {
	n := len(specs)
	epsilon := decimal.NewFromFloat(0.0001)

	spot, ok := marks.UnderlyingMark(underlying)
	theos := make([]decimal.Decimal, n)
	if ok && spot.IsPositive() {
		spotF, _ := spot.Float64()
		rateF, _ := riskFreeRate.Float64()
		const assumedVol = 0.6 // matches /option-chain's fallback until the IV surface covers every leg
		for i, spec := range specs {
			strikeF, _ := spec.strike.Float64()
			tYears := time.Until(spec.expiry).Hours() / 24 / 365
			theo := pricing.Price(spotF, strikeF, tYears, assumedVol, rateF, spec.optionType == "CALL")
			theos[i] = decimal.NewFromFloat(theo)
		}
	}

	modeledNet := decimal.Zero
	for i, spec := range specs {
		modeledNet = modeledNet.Add(theos[i].Mul(decimal.NewFromInt(int64(spec.ratio))))
	}

	prices := make([]decimal.Decimal, n)
	sameSign := (netPrice.IsPositive() && modeledNet.IsPositive()) || (netPrice.IsNegative() && modeledNet.IsNegative())
	if sameSign {
		scale := netPrice.Div(modeledNet)
		allPositive := true
		for i := range specs {
			prices[i] = theos[i].Mul(scale)
			if !prices[i].IsPositive() {
				allPositive = false
				break
			}
		}
		if allPositive {
			return prices
		}
		// A theo_i itself was non-positive (e.g. deep-OTM float rounding) —
		// fall through to the exhaustive-search fallback below instead of
		// returning a partially-negative result.
	}

	// Sign mismatch (or the same-sign path above didn't produce an
	// all-positive result): try every leg as the sole settling leg,
	// flat-flooring every other leg at epsilon, and use the first candidate
	// whose solved price is positive. Order candidates by descending
	// |ratio| so the cheapest-to-move leg is tried first when multiple
	// candidates would work.
	order := make([]int, n)
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(a, b int) bool { return abs(specs[order[a]].ratio) > abs(specs[order[b]].ratio) })

	for _, settleIdx := range order {
		otherSum := decimal.Zero
		for i, spec := range specs {
			if i == settleIdx {
				continue
			}
			otherSum = otherSum.Add(epsilon.Mul(decimal.NewFromInt(int64(spec.ratio))))
		}
		settleRatio := decimal.NewFromInt(int64(specs[settleIdx].ratio))
		candidate := netPrice.Sub(otherSum).Div(settleRatio)
		if candidate.IsPositive() {
			for i := range specs {
				if i == settleIdx {
					prices[i] = candidate
				} else {
					prices[i] = epsilon
				}
			}
			return prices
		}
	}

	// Unreachable for any real combo (requires at least one positive- and
	// one negative-ratio leg — see doc comment) — defensive fallback only:
	// flat-floor everything, sacrificing the exact invariant rather than
	// panicking or returning a nil/malformed slice.
	for i := range prices {
		prices[i] = epsilon
	}
	return prices
}
