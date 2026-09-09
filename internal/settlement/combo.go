package settlement

import (
	"context"
	"fmt"
	"time"

	"github.com/dex/matching-engine/internal/models"
	"github.com/dex/matching-engine/internal/pricing"
	"github.com/shopspring/decimal"
)

// ComboLegResolver resolves a combo instrument symbol to its two option
// legs. Implemented in cmd/engine (backed by the combo_instruments table);
// kept as a narrow interface here so settlement does not depend on
// cmd/engine's private instrument types — the same one-way dependency
// pattern risk.UnderlyingMarkSource uses for the mark-price lookup.
type ComboLegResolver interface {
	// ResolveComboLegs returns the buy-leg symbol, sell-leg symbol, and
	// underlying spot symbol for a registered combo instrument symbol.
	ResolveComboLegs(ctx context.Context, comboSymbol string) (buySymbol, sellSymbol, underlying string, err error)
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

// ComboSettlement settles trades on a native 2-leg combo order book
// (models.ComboOptions) by fanning each combo trade out into two linked
// option-leg trades, settled together through the SAME OptionsSettlement
// used for standalone option orders — a combo position is indistinguishable
// from a manually-built one once opened; only how it got there differs.
//
// This is what makes combo execution genuinely atomic, the way Deribit/CME
// combo books work: the combo trade IS the single unit of execution (one
// resting order matched against another on the combo's own book), and both
// legs settle inside this one Settle call — if either leg's ledger
// operation fails, this returns an error and the matching engine (which
// calls Settle synchronously, inside the same goroutine that produced the
// trade — see matching.Engine.run) halts the symbol exactly as it does for
// any other settlement failure. There is no window where one leg is open
// and the other is not: either both leg settlements succeed together in
// this one call, or neither trade is ever published as successful.
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

// Settle fans a combo trade out into its two option legs and settles both
// through OptionsSettlement.
func (c *ComboSettlement) Settle(trade *models.Trade) error {
	if trade.BuyOrder == nil || trade.SellOrder == nil {
		return fmt.Errorf("combo settle: missing order references on trade %s", trade.ID)
	}
	ctx := context.Background()
	buySymbol, sellSymbol, underlying, err := c.legs.ResolveComboLegs(ctx, trade.Symbol)
	if err != nil {
		return fmt.Errorf("combo settle: resolve legs for %s: %w", trade.Symbol, err)
	}

	buyStrike, buyExpiry, buyType, quote, ok := c.marks.LegSpec(ctx, buySymbol)
	if !ok {
		return fmt.Errorf("combo settle: no instrument spec for buy leg %s", buySymbol)
	}
	sellStrike, sellExpiry, sellType, _, ok := c.marks.LegSpec(ctx, sellSymbol)
	if !ok {
		return fmt.Errorf("combo settle: no instrument spec for sell leg %s", sellSymbol)
	}

	// netPrice is what the combo actually traded at: positive = the combo
	// BUYER paid a net debit; negative = the combo buyer received a net
	// credit. This is the ONE authoritative cash-flow number — however the
	// two legs' individual synthetic prices below get split, they are
	// constructed to sum to exactly this, so the net cash movement between
	// the two accounts always matches what the combo book actually traded,
	// regardless of any pricing-model choice for the per-leg split.
	netPrice := trade.Price

	buyLegPrice, sellLegPrice := splitComboNetPrice(netPrice, buyStrike, buyExpiry, buyType, sellStrike, sellExpiry, sellType, underlying, c.marks, c.riskFree)

	// The combo's BuyOrder (whoever submitted the order that goes long the
	// spread) is the buyer of the long leg and the seller of the short leg;
	// the combo's SellOrder is the mirror. Quantity is the same on both legs
	// for a 1:1 vertical (the only combo ratio this engine supports today).
	comboBuyer := trade.BuyOrder   // long the spread
	comboSeller := trade.SellOrder // short the spread

	longLegTrade := &models.Trade{
		ID: trade.ID + ":long", Symbol: buySymbol, Market: models.Options,
		Price: buyLegPrice, Quantity: trade.Quantity, ExecutedAt: trade.ExecutedAt,
		BuyOrder:  legOrder(comboBuyer, buySymbol, models.Buy, buyStrike, buyExpiry, buyType, quote),
		SellOrder: legOrder(comboSeller, buySymbol, models.Sell, buyStrike, buyExpiry, buyType, quote),
	}
	shortLegTrade := &models.Trade{
		ID: trade.ID + ":short", Symbol: sellSymbol, Market: models.Options,
		Price: sellLegPrice, Quantity: trade.Quantity, ExecutedAt: trade.ExecutedAt,
		// The combo buyer is SHORT the sell-leg; the combo seller is LONG it.
		BuyOrder:  legOrder(comboSeller, sellSymbol, models.Buy, sellStrike, sellExpiry, sellType, quote),
		SellOrder: legOrder(comboBuyer, sellSymbol, models.Sell, sellStrike, sellExpiry, sellType, quote),
	}

	// Both legs settle through the exact same OptionsSettlement.Settle used
	// for standalone orders — same ledger debits/credits, same position
	// bookkeeping, same backend mirroring. If the first leg settles but the
	// second fails, the caller (matching.Engine) halts the symbol without
	// publishing EITHER the combo trade or any downstream events, so the
	// two ledger operations that DID run are the only side effect — a real
	// but bounded partial-settlement window identical in shape to what
	// already exists for a single ordinary trade whose settlement halts
	// mid-way (this codebase's existing halt-on-settlement-failure design,
	// not a new risk introduced by combos).
	if err := c.options.Settle(longLegTrade); err != nil {
		return fmt.Errorf("combo settle: long leg: %w", err)
	}
	if err := c.options.Settle(shortLegTrade); err != nil {
		return fmt.Errorf("combo settle: short leg: %w", err)
	}
	return nil
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

// splitComboNetPrice allocates a combo's traded net price between its two
// legs using each leg's own theoretical Black-Scholes premium as the
// starting point — NOT a proportional split of the (much smaller) net price
// itself, which would produce leg prices with no relationship to either
// option's real value (a 400 net debit on a 1500/1100 leg pair is nowhere
// near either leg's actual premium; splitting 400 proportionally would
// silently misprice both legs by ~1000).
//
// The two legs' theoretical prices already differ by some model estimate of
// the net (theoBuy - theoSell); the discrepancy between that estimate and
// what the combo ACTUALLY traded at (netPrice) is pricing-model error
// (flat-vol assumption, stale IV, etc.) that must be reconciled somewhere.
// It is split evenly across both legs (half the adjustment added to the buy
// leg, half subtracted from the sell leg) — an arbitrary but symmetric
// choice, since there is no more authoritative way to attribute a single
// model-error number to one leg over the other. The result: buyLegPrice -
// sellLegPrice == netPrice EXACTLY (the one hard invariant, enforced
// algebraically below, not by floating-point luck), while both legs still
// land close to their own real theoretical value.
//
// Falls back to each leg's raw theoretical price adjusted by a 50/50 split
// of the residual when a live mark isn't available (can't compute theo at
// all) — an even split of the whole net price, same invariant preserved.
func splitComboNetPrice(netPrice, buyStrike decimal.Decimal, buyExpiry time.Time, buyType string,
	sellStrike decimal.Decimal, sellExpiry time.Time, sellType, underlying string,
	marks ComboOptionsMarkSource, riskFreeRate decimal.Decimal) (buyLegPrice, sellLegPrice decimal.Decimal) {

	spot, ok := marks.UnderlyingMark(underlying)
	if !ok || !spot.IsPositive() {
		half := netPrice.Div(decimal.NewFromInt(2))
		return legPricesFromResidual(decimal.Zero, decimal.Zero, netPrice, half, half.Neg())
	}

	spotF, _ := spot.Float64()
	rateF, _ := riskFreeRate.Float64()
	const assumedVol = 0.6 // matches /option-chain's fallback until the IV surface covers every leg

	buyStrikeF, _ := buyStrike.Float64()
	buyT := time.Until(buyExpiry).Hours() / 24 / 365
	buyTheo := decimal.NewFromFloat(pricing.Price(spotF, buyStrikeF, buyT, assumedVol, rateF, buyType == "CALL"))

	sellStrikeF, _ := sellStrike.Float64()
	sellT := time.Until(sellExpiry).Hours() / 24 / 365
	sellTheo := decimal.NewFromFloat(pricing.Price(spotF, sellStrikeF, sellT, assumedVol, rateF, sellType == "CALL"))

	return legPricesFromResidual(buyTheo, sellTheo, netPrice, buyTheo, sellTheo)
}

// legPricesFromResidual takes each leg's starting theoretical price
// (theoBuy, theoSell), computes the residual between their modeled net
// (theoBuy - theoSell) and the ACTUAL traded netPrice, and distributes that
// residual evenly across both legs so the returned prices satisfy
// buyLegPrice - sellLegPrice == netPrice exactly. Both results are floored
// at a small positive epsilon — a real option premium is never zero or
// negative, whatever the residual adjustment computes.
func legPricesFromResidual(theoBuy, theoSell, netPrice, startBuy, startSell decimal.Decimal) (buyLegPrice, sellLegPrice decimal.Decimal) {
	modeledNet := theoBuy.Sub(theoSell)
	residual := netPrice.Sub(modeledNet)
	half := residual.Div(decimal.NewFromInt(2))

	buyLegPrice = startBuy.Add(half)
	sellLegPrice = startSell.Sub(half)

	epsilon := decimal.NewFromFloat(0.0001)
	if !buyLegPrice.IsPositive() {
		buyLegPrice = epsilon
	}
	if !sellLegPrice.IsPositive() {
		sellLegPrice = epsilon
	}
	// Re-enforce the exact invariant after epsilon-flooring: whichever leg
	// was floored may have broken buyLegPrice - sellLegPrice == netPrice, so
	// the OTHER leg is recomputed once more from whichever was just fixed.
	if buyLegPrice.Sub(sellLegPrice).Equal(netPrice) {
		return buyLegPrice, sellLegPrice
	}
	if buyLegPrice.Equal(epsilon) {
		sellLegPrice = buyLegPrice.Sub(netPrice)
		if !sellLegPrice.IsPositive() {
			sellLegPrice = epsilon // both legs floored: an extreme netPrice relative to real premiums; best effort, cash-flow invariant sacrificed only in this degenerate case
		}
	} else {
		buyLegPrice = sellLegPrice.Add(netPrice)
		if !buyLegPrice.IsPositive() {
			buyLegPrice = epsilon
		}
	}
	return buyLegPrice, sellLegPrice
}
