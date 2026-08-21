package risk

import (
	"testing"

	"github.com/dex/matching-engine/internal/models"
	"github.com/shopspring/decimal"
)

// newStopMarketOrder builds a stop-market order: Type=Stop with StopPrice set
// but Price left zero (a stop-limit would also set Price, converting the
// order to a resting LIMIT once triggered — see orderbook.processStopTriggers).
func newStopMarketOrder(accountID string, side models.OrderSide, qty, stopPrice string) *models.Order {
	return &models.Order{
		ID: "stop-1", AccountID: accountID, Symbol: "BTC-USDT",
		Side: side, Type: models.Stop,
		Quantity:  decimal.RequireFromString(qty),
		StopPrice: decimal.RequireFromString(stopPrice),
	}
}

// A stop-market order has Price == 0 by construction (it takes whatever
// price the book offers once triggered, like a plain market order). Before
// the fix, Check/Reserve/Release all computed notional from order.Price
// directly, so a stop-market order's notional was silently zero — an
// account with zero balance could submit an unlimited-size stop-market
// order and it would pass every check.

func TestCheck_StopMarket_RejectsZeroBalance(t *testing.T) {
	ledger := NewLedger()
	checker := NewChecker(ledger)
	// No deposit: available balance is zero.

	order := newStopMarketOrder("trader", models.Buy, "1", "50000")
	if err := checker.Check(order); err == nil {
		t.Fatalf("Check() on a stop-market order from a zero-balance account should fail, got nil")
	}
}

func TestCheck_StopMarket_AllowsPositiveBalance(t *testing.T) {
	ledger := NewLedger()
	checker := NewChecker(ledger)
	ledger.Deposit("trader", "USDT", decimal.NewFromInt(1000))

	order := newStopMarketOrder("trader", models.Buy, "1", "50000")
	if err := checker.Check(order); err != nil {
		t.Fatalf("Check() on a funded account should pass, got: %v", err)
	}
}

// RequiredFor and ReleaseAmountFor must both treat a stop-market order like
// a market order (no fixed-price notional to compute) rather than falling
// through to the generic Limit-style path, which would silently return a
// zero amount and under-reserve/over-release funds.

func TestRequiredFor_StopMarket_ReturnsZero(t *testing.T) {
	order := newStopMarketOrder("trader", models.Buy, "2", "50000")
	asset, amount := RequiredFor(order)
	if asset != "" || !amount.IsZero() {
		t.Fatalf("RequiredFor(stop-market) = (%q, %s), want (\"\", 0) — reservation for a stop-market "+
			"order is computed by the caller against an estimated price, not derived from the order's own zero Price",
			asset, amount)
	}
}

func TestReleaseAmountFor_StopMarket_ReturnsZero(t *testing.T) {
	order := newStopMarketOrder("trader", models.Sell, "2", "40000")
	asset, amount := ReleaseAmountFor(order)
	if asset != "" || !amount.IsZero() {
		t.Fatalf("ReleaseAmountFor(stop-market) = (%q, %s), want (\"\", 0)", asset, amount)
	}
}

func TestChecker_Release_StopMarket_DoesNotTouchLedger(t *testing.T) {
	ledger := NewLedger()
	checker := NewChecker(ledger)
	ledger.Deposit("trader", "USDT", decimal.NewFromInt(1000))

	order := newStopMarketOrder("trader", models.Buy, "1", "50000")
	before := ledger.Available("trader", "USDT")
	checker.Release(order) // must be a no-op: the estimated reservation lives outside this order's fields
	after := ledger.Available("trader", "USDT")
	if !before.Equal(after) {
		t.Fatalf("Checker.Release on a stop-market order changed available balance: before=%s after=%s", before, after)
	}
}

// A stop-limit order (Type=Stop with a non-zero Price) has a real limit
// price to reserve against once triggered, so it must NOT take the
// stop-market zero-amount path — it should behave like an ordinary limit
// order for reservation purposes.

func TestRequiredFor_StopLimit_UsesLimitPrice(t *testing.T) {
	order := &models.Order{
		ID: "stop-2", AccountID: "trader", Symbol: "BTC-USDT",
		Side: models.Buy, Type: models.Stop,
		Price:     decimal.RequireFromString("49000"), // stop-limit: rests at this price once triggered
		Quantity:  decimal.RequireFromString("1"),
		StopPrice: decimal.RequireFromString("50000"),
	}
	asset, amount := RequiredFor(order)
	if asset != "USDT" || !amount.Equal(decimal.RequireFromString("49000")) {
		t.Fatalf("RequiredFor(stop-limit) = (%q, %s), want (\"USDT\", 49000)", asset, amount)
	}
}
