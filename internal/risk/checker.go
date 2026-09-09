package risk

import (
	"fmt"
	"strings"

	"github.com/dex/matching-engine/internal/models"
	"github.com/shopspring/decimal"
)

// Checker performs pre-trade risk validation against the in-memory Ledger.
// It reads with a shared read-lock, so it never blocks the matching goroutine
// on writes.
type Checker struct {
	ledger *Ledger
}

// NewChecker creates a Checker backed by the given Ledger.
func NewChecker(ledger *Ledger) *Checker {
	return &Checker{ledger: ledger}
}

// UnderlyingMarkSource resolves the current mark price for an options
// underlying (e.g. "BTC-BIUSD"), so the options margin model can price a
// short writer's actual risk instead of always locking the full strike.
// Implemented by *marketdata.Service; kept as a narrow interface here to
// avoid risk importing marketdata (marketdata does not import risk either,
// but this keeps the dependency direction explicit and one-way).
type UnderlyingMarkSource interface {
	UnderlyingMark(symbol string) (decimal.Decimal, bool)
}

// markSource is package-level rather than a Checker field because notionalFor
// and friends (required, releaseAmount, FilledDebit, RequiredFor, ...) are
// free functions called from many packages without a Checker in hand — see
// their doc comments. Set once at startup via SetMarkSource; nil-safe (falls
// back to the pre-existing cash-secured strike×qty behavior) so tests and any
// call path that runs before wiring never panic.
var markSource UnderlyingMarkSource

// SetMarkSource wires the mark-price provider used for options margin. Call
// once during startup, after the marketdata.Service exists (main.go creates
// the Checker before the Service, so this can't be a constructor argument).
func SetMarkSource(src UnderlyingMarkSource) {
	markSource = src
}

// shortOptionMarginFloorPct is the fraction of underlying notional (strike ×
// qty is NOT the base here — it's spot × qty) added to the premium already
// received, per Phase 2a of the options roadmap: max(premium + pct% of
// underlying notional, ITM amount). This is the same shape retail brokers
// use for cash-secured-adjacent short option margin (not truly portfolio
// margin — no offsets for hedged positions yet, that's Phase 2b).
const shortOptionMarginFloorPct = "0.20"

// shortOptionMargin computes the margin required to write (sell) an option,
// replacing the naive strike×qty cash-secured hold with a floor model:
//
//	max(premium + 20% of underlying notional, ITM amount) × qty
//
// capped at the fully cash-secured strike×qty amount (this model should
// only ever require LESS collateral than full cash-securing, never more —
// if the computation somehow exceeds it, that's a bug in the estimate, not
// a real additional risk the floor is meant to capture).
// Falls back to the original strike×qty behavior when no mark price is
// available yet (source unset, or the underlying has no live book) so
// options trading never becomes *more* permissive than today by accident.
func shortOptionMargin(order *models.Order, qty, premiumPrice decimal.Decimal) decimal.Decimal {
	cashSecured := order.StrikePrice.Mul(qty)
	if markSource == nil {
		return cashSecured
	}
	underlying := underlyingFromOrderSymbol(order)
	spot, ok := markSource.UnderlyingMark(underlying)
	if !ok || !spot.IsPositive() {
		return cashSecured
	}

	// Premium already received by the writer for this order's notional.
	// premiumPrice is 0 for a resting order not yet priced against the book
	// (e.g. before a limit's own price is known) — required()/requiredAt()
	// always pass order.Price or an explicit estimate, so this is the same
	// price a buyer would pay, per unit.
	premium := premiumPrice.Mul(qty)

	underlyingNotional := spot.Mul(qty)
	floorPct, _ := decimal.NewFromString(shortOptionMarginFloorPct)
	floor := premium.Add(underlyingNotional.Mul(floorPct))

	var itm decimal.Decimal
	if strings.EqualFold(order.OptionType, "CALL") {
		itm = decimal.Max(decimal.Zero, spot.Sub(order.StrikePrice)).Mul(qty)
	} else {
		itm = decimal.Max(decimal.Zero, order.StrikePrice.Sub(spot)).Mul(qty)
	}

	required := decimal.Max(floor, itm)
	if required.GreaterThan(cashSecured) {
		return cashSecured
	}
	if required.IsNegative() {
		return decimal.Zero
	}
	return required
}

// RequiredOptionsMargin re-evaluates the collateral a short (writer)
// options position needs RIGHT NOW, using the same floor model
// shortOptionMargin applies at order-submission time. Exported for
// liquidation.Engine's options margin-call sweep: a position's collateral
// was computed once when the order that opened it was placed, against
// whatever the underlying's spot was at that moment — since then the spot
// may have moved enough that the position is now under-margined relative to
// what a FRESH order of the same size would require today, without the
// account having done anything wrong. premiumPerUnit should be the
// position's own historical average premium (Premium/|Size| from
// settlement.OptionsPosition) as the best available stand-in for "premium
// already received" — the position doesn't carry a live order price the way
// a fresh order does.
func RequiredOptionsMargin(symbol, optionType string, strike, qty, premiumPerUnit decimal.Decimal, quoteCurrency string) decimal.Decimal {
	order := &models.Order{
		Symbol: symbol, OptionType: optionType, StrikePrice: strike,
		QuoteCurrency: quoteCurrency, Side: models.Sell, Market: models.Options,
	}
	return shortOptionMargin(order, qty, premiumPerUnit)
}

// VerticalSpreadMargin computes the net margin required to open a vertical
// spread — buy one option, sell another, same underlying/expiry/option
// type, different strikes, same quantity — as a single defined-risk
// position, per the roadmap's Phase 2b ("portfolio-margin offsets for
// hedged positions... covered calls, spreads").
//
// A vertical's maximum possible loss is bounded by construction:
//   - CALL spread (buy lower strike, sell higher strike — a bull call
//     spread; a bear call spread is the mirror short side of the exact same
//     pair): max loss = strike distance × qty, MINUS the net premium
//     credit already banked (a net debit ADDS to that max loss instead).
//   - PUT spread: same shape, mirrored (buy higher strike, sell lower).
//
// This deliberately does NOT re-derive shortOptionMargin's flat 20%-of-
// underlying floor for the short leg — the whole point of a vertical is
// that its long leg caps the short leg's loss at the strike distance, which
// is almost always tighter than that floor. Margining each leg
// independently (the pre-9b behavior — buy leg costs its premium, sell leg
// locks its own floor/cash-secured amount) ignores that the two legs
// together can never lose more than the strike distance, and so
// overcharges margin for every textbook credit/debit spread.
//
// buyStrike/sellStrike are which leg is long/short; qty is the (equal, by
// construction — see cmd/engine's /spread handler) contract count on both
// legs; netCredit is (premium RECEIVED for the short leg) − (premium PAID
// for the long leg): positive means the spread was opened for a net credit,
// negative means a net debit was paid to open it.
//
// A net-debit spread needs NO additional collateral beyond the debit
// already paid — that debit is cash already handed over, and it IS the
// spread's maximum possible loss (if both legs expire worthless, the buyer
// loses exactly what they paid, no more). A net-credit spread's maximum
// loss is the strike distance minus the credit already banked (the credit
// offsets part of the worst case), so that residual is what must be posted
// as margin.
//
// Returns the quote-currency margin to reserve, always >= 0 and always <=
// the strike distance × qty (a vertical's collateral requirement can never
// exceed its own defined maximum loss).
func VerticalSpreadMargin(buyStrike, sellStrike, qty, netCredit decimal.Decimal) decimal.Decimal {
	if !qty.IsPositive() {
		return decimal.Zero
	}
	strikeDistance := buyStrike.Sub(sellStrike).Abs().Mul(qty)
	if !netCredit.IsPositive() {
		// Net debit (or exactly zero): no additional margin — the debit
		// paid up front already IS the maximum loss.
		return decimal.Zero
	}
	required := strikeDistance.Sub(netCredit)
	if required.IsNegative() {
		return decimal.Zero
	}
	return required
}

// underlyingFromOrderSymbol extracts the underlying spot symbol (e.g.
// "BTC-BIUSD") from an option instrument symbol, mirroring
// settlement.underlyingFromSymbol (duplicated here rather than imported —
// settlement already depends on risk, so the reverse import would cycle).
func underlyingFromOrderSymbol(order *models.Order) string {
	parts := strings.Split(order.Symbol, "-")
	if len(parts) >= 5 {
		return parts[0] + "-" + parts[1]
	}
	if len(parts) >= 1 && order.QuoteCurrency != "" {
		return parts[0] + "-" + order.QuoteCurrency
	}
	return order.Symbol
}

// Check validates an order before submission to the matching engine.
// Returns nil if all checks pass.
func (c *Checker) Check(order *models.Order) error {
	if order.InternalLiquidation {
		// Forced position close: the position already exists and is being
		// reduced, so no additional margin/collateral needs to be reserved.
		return nil
	}
	if order.AccountID == "" {
		return fmt.Errorf("order missing AccountID")
	}
	if !order.Quantity.IsPositive() {
		return fmt.Errorf("order quantity must be positive")
	}

	// Market orders (and stop-market orders, i.e. a STOP with no limit Price)
	// cannot be checked for exact notional without a mark price: both have
	// order.Price == 0, so notionalFor would otherwise compute a zero
	// requirement and let a fully unfunded account through. A worst-case
	// estimate is reserved later via ReserveMarket/the /order handler's
	// slippage-bounded conversion once the best opposite quote is known;
	// here we only verify the account has a positive available balance in
	// the required asset.
	if order.Type == models.Market || (order.Type == models.Stop && !order.Price.IsPositive()) {
		asset := assetFor(order)
		if c.ledger.Available(order.AccountID, asset).LessThanOrEqual(decimal.Zero) {
			return fmt.Errorf("insufficient %s: available=0", asset)
		}
		return nil
	}

	asset, notional := required(order)
	available := c.ledger.Available(order.AccountID, asset)
	if available.LessThan(notional) {
		return fmt.Errorf("insufficient %s: available=%s required=%s",
			asset, available, notional)
	}
	return nil
}

// Reserve places a soft hold on the funds required by the order.
// Must be called after Check passes and before Submit.
func (c *Checker) Reserve(order *models.Order) error {
	if order.Type == models.Market {
		return nil // market orders have no known notional at this stage
	}
	asset, notional := required(order)
	return c.ledger.Reserve(order.AccountID, asset, notional)
}

// ReserveMarket reserves funds for a market order using an estimated
// worst-case price (typically the best opposite quote), since market orders
// carry no price of their own. Returns the asset and amount reserved so the
// caller can release the unused residual after the order fills, or the full
// amount if it is rejected/unfilled.
func (c *Checker) ReserveMarket(order *models.Order, estPrice decimal.Decimal) (asset string, amount decimal.Decimal, err error) {
	asset, amount = requiredAt(order, estPrice)
	if amount.IsZero() {
		return asset, amount, nil
	}
	if err := c.ledger.Reserve(order.AccountID, asset, amount); err != nil {
		return asset, amount, err
	}
	return asset, amount, nil
}

// Release frees whatever remains reserved for the order's unfilled quantity
// (on cancel or rejection). It must NOT recompute from the original
// Quantity: as trades settle, Ledger.Debit already releases the reservation
// for the filled portion, so releasing the full original notional here would
// over-release funds that legitimately belong to other open orders on the
// same account+asset. Using RemainingQty ensures we only release what is
// still actually held for this order.
func (c *Checker) Release(order *models.Order) {
	if order.Type == models.Market {
		return
	}
	if order.Type == models.Stop && !order.Price.IsPositive() {
		// Stop-market orders are reserved at the worst-case estimated price
		// computed at submission time (see the /order handler), not at
		// order.Price (which is zero) — releaseAmount can't reconstruct that
		// estimate from the order alone, so a cancelled/never-triggered
		// stop-market's reservation must be released by the caller using the
		// same estimated amount it reserved, not through this generic path.
		return
	}
	asset, amount := releaseAmount(order)
	if amount.IsPositive() {
		c.ledger.Release(order.AccountID, asset, amount)
	}
}

// RequiredFor exposes the asset and amount that Reserve would lock for order,
// for callers outside this package (e.g. the Postgres balance-lock bridge)
// that must mirror the same reservation externally. Mirrors Reserve's market-
// order skip: market orders have no known notional at submission time, so no
// amount is returned.
func RequiredFor(order *models.Order) (asset string, amount decimal.Decimal) {
	if order.Type == models.Market || (order.Type == models.Stop && !order.Price.IsPositive()) {
		return "", decimal.Zero
	}
	return required(order)
}

// EstimatedRequired returns the asset and worst-case amount to reserve for a
// market order given an estimated (best opposite) price, since market orders
// carry no price of their own. Used by the order handler to reserve funds
// before a market order matches so an unfunded account can't receive base for
// free when settlement's debit later fails.
func EstimatedRequired(order *models.Order, estPrice decimal.Decimal) (asset string, amount decimal.Decimal) {
	return requiredAt(order, estPrice)
}

// FilledDebit returns the total amount settlement will debit for the filled
// portion of order across the given trades, using the same notional rules as
// Reserve so a residual release is always consistent with what was reserved.
func FilledDebit(order *models.Order, trades []*models.Trade) decimal.Decimal {
	total := decimal.Zero
	for _, t := range trades {
		debit := notionalFor(order, t.Quantity, t.Price)
		// Spot buyers pay their maker/taker fee in quote, so releasing only
		// notional would unlock the fee that must remain reserved until this
		// exact fill settles.
		if order.Market == models.Spot && order.Side == models.Buy {
			if t.MakerSide == models.Buy {
				debit = debit.Add(t.MakerFeePaid)
			} else {
				debit = debit.Add(t.TakerFeePaid)
			}
		}
		total = total.Add(debit)
	}
	return total
}

// ReleaseAmountFor exposes the asset and amount that Release would free for
// order, for callers outside this package that must mirror the same release
// externally. Mirrors Release's market-order skip.
func ReleaseAmountFor(order *models.Order) (asset string, amount decimal.Decimal) {
	if order.Type == models.Market {
		return "", decimal.Zero
	}
	if order.Type == models.Stop && !order.Price.IsPositive() {
		// An untriggered stop-market order still holds its worst-case
		// reservation (see the /order handler) until it either triggers and
		// fills, or is cancelled — there is no fixed limit price to compute
		// a resting-notional release amount from, so nothing is released
		// here. The full reservation is freed on cancel via the normal
		// order-cancellation path instead.
		return "", decimal.Zero
	}
	return releaseAmount(order)
}

// required returns the asset and amount that must be available for the order
// at submission time, based on the full original Quantity. Used by Check and
// Reserve, before anything has filled.
// Symbol format: "BASE-QUOTE" (e.g. "BTC-BIUSD").
// Buyers lock quote currency (price × qty); sellers lock base currency (qty).
func required(order *models.Order) (asset string, amount decimal.Decimal) {
	return assetFor(order), notionalFor(order, order.Quantity, order.Price)
}

// requiredAt is like required but evaluates the notional at an explicit price,
// used for market orders whose own Price is zero (the caller passes the best
// opposite quote as a worst-case estimate).
func requiredAt(order *models.Order, price decimal.Decimal) (asset string, amount decimal.Decimal) {
	return assetFor(order), notionalFor(order, order.Quantity, price)
}

// releaseAmount returns the asset and amount that should be released for an
// order being cancelled or rejected, based on the UNFILLED remainder only.
func releaseAmount(order *models.Order) (asset string, amount decimal.Decimal) {
	return assetFor(order), notionalFor(order, order.RemainingQty(), order.Price)
}

// releaseAmountAt is like releaseAmount but evaluates at an explicit price,
// for market orders whose own Price is zero.
func releaseAmountAt(order *models.Order, price decimal.Decimal) (asset string, amount decimal.Decimal) {
	return assetFor(order), notionalFor(order, order.RemainingQty(), price)
}

func assetFor(order *models.Order) string {
	// Options instrument symbols (BASE-STRIKE-EXPIRY-TYPE or
	// BASE-QUOTE-STRIKE-EXPIRY-TYPE) cannot be split into BASE-QUOTE, so
	// the quote currency must come from the order itself (set by the
	// handler from the instrument's underlying config).
	if order.Market == models.Options && order.QuoteCurrency != "" {
		return order.QuoteCurrency
	}

	parts := strings.SplitN(order.Symbol, "-", 2)
	if len(parts) != 2 {
		return order.Symbol
	}
	switch order.Market {
	case models.Futures:
		// Both sides post margin in the quote currency (cross/isolated margin, cash-settled).
		return parts[1]
	case models.Options:
		// Buyer pays premium in quote currency; seller (writer) posts cash-secured
		// collateral in quote currency too (first-pass: no physical covered calls).
		return parts[1]
	default:
		if order.IsBuy() {
			return parts[1] // quote
		}
		return parts[0] // base
	}
}

// MarginRequired returns the margin (in quote currency) needed to open a
// futures position of the given notional at the given leverage. Shared by
// the risk checker and futures settlement so the two never disagree.
func MarginRequired(notional decimal.Decimal, leverage int) decimal.Decimal {
	if leverage < 1 {
		leverage = 1
	}
	return notional.Div(decimal.NewFromInt(int64(leverage)))
}

func notionalFor(order *models.Order, qty, price decimal.Decimal) decimal.Decimal {
	switch order.Market {
	case models.Futures:
		notional := price.Mul(qty)
		return MarginRequired(notional, order.Leverage)
	case models.Options:
		if order.IsBuy() {
			// Premium owed by the buyer.
			return price.Mul(qty)
		}
		// Writer (short) margin: max(premium + 20% underlying notional, ITM
		// amount), capped at full cash-secured strike×qty — see
		// shortOptionMargin. Falls back to the original cash-secured
		// strike×qty when no live mark price is available for the
		// underlying (markSource unset or the book has no quotes yet).
		return shortOptionMargin(order, qty, price)
	default:
		if order.IsBuy() {
			return price.Mul(qty)
		}
		return qty
	}
}
