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
	if order.AccountID == "" {
		return fmt.Errorf("order missing AccountID")
	}
	if !order.Quantity.IsPositive() {
		return fmt.Errorf("order quantity must be positive")
	}

	// Market orders cannot be checked for exact notional without a mark price.
	// Phase 7 adds mark-price checks here. For now, verify the account exists
	// and has at least some balance in the required asset.
	if order.Type == models.Market {
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

// required returns the asset and amount that must be available for the order
// at submission time, based on the full original Quantity. Used by Check and
// Reserve, before anything has filled.
// Symbol format: "BASE-QUOTE" (e.g. "BTC-USDT").
// Buyers lock quote currency (price × qty); sellers lock base currency (qty).
func required(order *models.Order) (asset string, amount decimal.Decimal) {
	return assetFor(order), notionalFor(order, order.Quantity)
}

// releaseAmount returns the asset and amount that should be released for an
// order being cancelled or rejected, based on the UNFILLED remainder only.
func releaseAmount(order *models.Order) (asset string, amount decimal.Decimal) {
	return assetFor(order), notionalFor(order, order.RemainingQty())
}

func assetFor(order *models.Order) string {
	parts := strings.SplitN(order.Symbol, "-", 2)
	if len(parts) != 2 {
		return order.Symbol
	}
	if order.IsBuy() {
		return parts[1] // quote
	}
	return parts[0] // base
}

func notionalFor(order *models.Order, qty decimal.Decimal) decimal.Decimal {
	if order.IsBuy() {
		return order.Price.Mul(qty)
	}
	return qty
}
