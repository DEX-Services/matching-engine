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

	// Market orders cannot be checked for exact notional without a mark price.
	// A worst-case estimate is reserved later via ReserveMarket in the order
	// handler, once the best opposite quote is known. Here we only verify the
	// account has a positive available balance in the required asset so a
	// totally unfunded account cannot submit market orders either.
	if order.Type == models.Market {
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
	if order.Type == models.Market {
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
		total = total.Add(notionalFor(order, t.Quantity, t.Price))
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
	return releaseAmount(order)
}

// required returns the asset and amount that must be available for the order
// at submission time, based on the full original Quantity. Used by Check and
// Reserve, before anything has filled.
// Symbol format: "BASE-QUOTE" (e.g. "BTC-USDT").
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
		// Cash-secured collateral for the writer (both CALL and PUT): lock
		// strike*qty in quote currency. No physical covered-call support yet.
		return order.StrikePrice.Mul(qty)
	default:
		if order.IsBuy() {
			return price.Mul(qty)
		}
		return qty
	}
}
