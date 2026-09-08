package main

import (
	"testing"

	"github.com/dex/matching-engine/internal/models"
	"github.com/dex/matching-engine/internal/settlement"
	"github.com/shopspring/decimal"
)

func longPosition(size string) *settlement.Position {
	return &settlement.Position{Side: models.Buy, Size: decimal.RequireFromString(size)}
}

func shortPosition(size string) *settlement.Position {
	return &settlement.Position{Side: models.Sell, Size: decimal.RequireFromString(size)}
}

func reduceOnlyOrder(side models.OrderSide, qty string) *models.Order {
	return &models.Order{
		Side: side, Quantity: decimal.RequireFromString(qty), ReduceOnly: true,
	}
}

func TestCheckReduceOnly_NoPosition_Rejected(t *testing.T) {
	err := checkReduceOnly(reduceOnlyOrder(models.Sell, "1"), nil)
	if err == nil {
		t.Fatal("expected rejection: reduceOnly sell with no open position")
	}
}

func TestCheckReduceOnly_ZeroSizePosition_Rejected(t *testing.T) {
	err := checkReduceOnly(reduceOnlyOrder(models.Sell, "1"), longPosition("0"))
	if err == nil {
		t.Fatal("expected rejection: reduceOnly against a zero-size position")
	}
}

func TestCheckReduceOnly_ClosingLong_Allowed(t *testing.T) {
	// Long 2 BTC open; a reduceOnly SELL of 1 BTC shrinks it — must pass.
	if err := checkReduceOnly(reduceOnlyOrder(models.Sell, "1"), longPosition("2")); err != nil {
		t.Fatalf("expected a closing sell to be allowed, got: %v", err)
	}
}

func TestCheckReduceOnly_ClosingShort_Allowed(t *testing.T) {
	// Short 2 BTC open; a reduceOnly BUY of 1 BTC shrinks it — must pass.
	if err := checkReduceOnly(reduceOnlyOrder(models.Buy, "1"), shortPosition("2")); err != nil {
		t.Fatalf("expected a closing buy to be allowed, got: %v", err)
	}
}

func TestCheckReduceOnly_SameDirection_Rejected(t *testing.T) {
	// This is the bug the fix closes: before, ReduceOnly was declared on the
	// Order struct but never read anywhere, so an order that actually
	// increases the position (same side as the existing position) sailed
	// through with no error.
	err := checkReduceOnly(reduceOnlyOrder(models.Buy, "1"), longPosition("2"))
	if err == nil {
		t.Fatal("expected rejection: reduceOnly buy against an existing long would increase the position")
	}
}

func TestCheckReduceOnly_Flip_Rejected(t *testing.T) {
	// Opposite side but larger than the open position would close it and
	// then open a new position in the other direction — also not "reduce".
	err := checkReduceOnly(reduceOnlyOrder(models.Sell, "5"), longPosition("2"))
	if err == nil {
		t.Fatal("expected rejection: reduceOnly quantity exceeding the open position size would flip it")
	}
}

func TestCheckReduceOnly_ExactClose_Allowed(t *testing.T) {
	if err := checkReduceOnly(reduceOnlyOrder(models.Sell, "2"), longPosition("2")); err != nil {
		t.Fatalf("expected an exact full close to be allowed, got: %v", err)
	}
}
