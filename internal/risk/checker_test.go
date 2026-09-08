package risk

import (
	"testing"

	"github.com/dex/matching-engine/internal/models"
	"github.com/shopspring/decimal"
)

func newBuyOrder(qty, price string) *models.Order {
	return &models.Order{
		ID: "o1", AccountID: "buyer", Symbol: "BTC-USDT",
		Side: models.Buy, Type: models.Limit,
		Price: decimal.RequireFromString(price), Quantity: decimal.RequireFromString(qty),
	}
}

func newSellOrder(qty, price string) *models.Order {
	return &models.Order{
		ID: "o1", AccountID: "seller", Symbol: "BTC-USDT",
		Side: models.Sell, Type: models.Limit,
		Price: decimal.RequireFromString(price), Quantity: decimal.RequireFromString(qty),
	}
}

func TestReserveRelease_FullCancel(t *testing.T) {
	ledger := NewLedger()
	checker := NewChecker(ledger)
	ledger.Deposit("buyer", "USDT", decimal.NewFromInt(1000))

	order := newBuyOrder("1", "100")
	if err := checker.Reserve(order); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if got := ledger.Available("buyer", "USDT"); !got.Equal(decimal.NewFromInt(900)) {
		t.Fatalf("available after reserve = %s, want 900", got)
	}

	// Simulate cancel with nothing filled.
	checker.Release(order)
	if got := ledger.Available("buyer", "USDT"); !got.Equal(decimal.NewFromInt(1000)) {
		t.Fatalf("available after release = %s, want 1000", got)
	}
	if got := ledger.Reserved("buyer", "USDT"); !got.IsZero() {
		t.Fatalf("reserved after release = %s, want 0", got)
	}
}

func TestReserveRelease_PartialFillThenCancel(t *testing.T) {
	ledger := NewLedger()
	checker := NewChecker(ledger)
	ledger.Deposit("buyer", "USDT", decimal.NewFromInt(1000))

	order := newBuyOrder("10", "100") // reserves 1000
	if err := checker.Reserve(order); err != nil {
		t.Fatalf("reserve: %v", err)
	}

	// Simulate a partial fill: 4 of 10 filled, settlement debits 400.
	order.Filled = decimal.NewFromInt(4)
	if err := ledger.Debit("buyer", "USDT", decimal.NewFromInt(400)); err != nil {
		t.Fatalf("debit: %v", err)
	}
	if got := ledger.Reserved("buyer", "USDT"); !got.Equal(decimal.NewFromInt(600)) {
		t.Fatalf("reserved after partial fill = %s, want 600", got)
	}

	// Cancel the remainder (6 unfilled @ 100 = 600).
	checker.Release(order)
	if got := ledger.Reserved("buyer", "USDT"); !got.IsZero() {
		t.Fatalf("reserved after cancel = %s, want 0 (no double release / residual)", got)
	}
	if got := ledger.Available("buyer", "USDT"); !got.Equal(decimal.NewFromInt(600)) {
		t.Fatalf("available after cancel = %s, want 600 (1000 - 400 debited)", got)
	}
	if got := ledger.Balance("buyer", "USDT"); !got.Equal(decimal.NewFromInt(600)) {
		t.Fatalf("balance after cancel = %s, want 600", got)
	}
}

func TestReserveRelease_RejectPath(t *testing.T) {
	ledger := NewLedger()
	checker := NewChecker(ledger)
	ledger.Deposit("buyer", "USDT", decimal.NewFromInt(1000))

	order := newBuyOrder("2", "100") // reserves 200
	if err := checker.Reserve(order); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	// FOK rejection: nothing filled, release full reservation.
	checker.Release(order)
	if got := ledger.Available("buyer", "USDT"); !got.Equal(decimal.NewFromInt(1000)) {
		t.Fatalf("available after reject-release = %s, want 1000", got)
	}
}

func TestReserveRelease_SellerSide(t *testing.T) {
	ledger := NewLedger()
	checker := NewChecker(ledger)
	ledger.Deposit("seller", "BTC", decimal.NewFromInt(10))

	order := newSellOrder("10", "100") // reserves 10 BTC
	if err := checker.Reserve(order); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if got := ledger.Available("seller", "BTC"); !got.IsZero() {
		t.Fatalf("available after reserve = %s, want 0", got)
	}

	// Partial fill: 3 of 10 filled, settlement debits 3 BTC.
	order.Filled = decimal.NewFromInt(3)
	if err := ledger.Debit("seller", "BTC", decimal.NewFromInt(3)); err != nil {
		t.Fatalf("debit: %v", err)
	}
	if got := ledger.Reserved("seller", "BTC"); !got.Equal(decimal.NewFromInt(7)) {
		t.Fatalf("reserved after partial fill = %s, want 7", got)
	}

	checker.Release(order) // releases remaining 7 BTC
	if got := ledger.Reserved("seller", "BTC"); !got.IsZero() {
		t.Fatalf("reserved after cancel = %s, want 0", got)
	}
	if got := ledger.Available("seller", "BTC"); !got.Equal(decimal.NewFromInt(7)) {
		t.Fatalf("available after cancel = %s, want 7 (10 - 3 debited)", got)
	}
}

func TestRelease_NeverGoesNegative(t *testing.T) {
	ledger := NewLedger()
	checker := NewChecker(ledger)
	ledger.Deposit("buyer", "USDT", decimal.NewFromInt(1000))

	order := newBuyOrder("1", "100")
	if err := checker.Reserve(order); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	checker.Release(order)
	checker.Release(order) // double release should be a safe no-op past zero
	if got := ledger.Reserved("buyer", "USDT"); !got.IsZero() {
		t.Fatalf("reserved after double release = %s, want 0", got)
	}
	if got := ledger.Available("buyer", "USDT"); !got.Equal(decimal.NewFromInt(1000)) {
		t.Fatalf("available after double release = %s, want 1000", got)
	}
}

func TestReserve_InsufficientBalance(t *testing.T) {
	ledger := NewLedger()
	checker := NewChecker(ledger)
	ledger.Deposit("buyer", "USDT", decimal.NewFromInt(50))

	order := newBuyOrder("1", "100") // needs 100, only has 50
	if err := checker.Reserve(order); err == nil {
		t.Fatal("expected reserve to fail with insufficient balance")
	}
	if got := ledger.Reserved("buyer", "USDT"); !got.IsZero() {
		t.Fatalf("reserved after failed reserve = %s, want 0", got)
	}
}
