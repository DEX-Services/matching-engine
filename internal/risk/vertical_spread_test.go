package risk

import (
	"testing"

	"github.com/shopspring/decimal"
)

func TestVerticalSpreadMargin_NetDebitNeedsNoAdditionalMargin(t *testing.T) {
	// Bull call spread: buy 60000 call, sell 65000 call, paid a 1000 net
	// debit to open. The debit itself is the max loss and is already spent
	// — no further collateral is needed.
	got := VerticalSpreadMargin(decimal.NewFromInt(60000), decimal.NewFromInt(65000), decimal.NewFromInt(1), decimal.NewFromInt(-1000))
	if !got.IsZero() {
		t.Fatalf("net-debit spread margin = %s, want 0", got)
	}
}

func TestVerticalSpreadMargin_ExactZeroPremiumNeedsFullMargin(t *testing.T) {
	// No premium changed hands at all (not a debit, not a credit) — nothing
	// is already paid to cap the loss, and nothing is already banked to
	// offset it. The full strike-distance worst case must be collateralized.
	// (An earlier version of this function incorrectly treated this the
	// same as a debit and returned 0 — a real margin gap.)
	got := VerticalSpreadMargin(decimal.NewFromInt(60000), decimal.NewFromInt(65000), decimal.NewFromInt(1), decimal.Zero)
	want := decimal.NewFromInt(5000)
	if !got.Equal(want) {
		t.Fatalf("zero-premium spread margin = %s, want %s (full strike distance)", got, want)
	}
}

func TestVerticalSpreadMargin_NetCreditNeedsStrikeDistanceMinusCredit(t *testing.T) {
	// Strike distance 5000, opened for a 500 net credit -> margin = 4500.
	got := VerticalSpreadMargin(decimal.NewFromInt(60000), decimal.NewFromInt(65000), decimal.NewFromInt(1), decimal.NewFromInt(500))
	want := decimal.NewFromInt(4500)
	if !got.Equal(want) {
		t.Fatalf("net-credit spread margin = %s, want %s", got, want)
	}
}

func TestVerticalSpreadMargin_CreditExceedingStrikeDistanceFloorsAtZero(t *testing.T) {
	// A credit larger than the strike distance itself (e.g. a very deep ITM
	// short leg) can never require negative margin.
	got := VerticalSpreadMargin(decimal.NewFromInt(60000), decimal.NewFromInt(65000), decimal.NewFromInt(1), decimal.NewFromInt(6000))
	if !got.IsZero() {
		t.Fatalf("over-credited spread margin = %s, want 0 (floored)", got)
	}
}

func TestVerticalSpreadMargin_ScalesWithQty(t *testing.T) {
	got := VerticalSpreadMargin(decimal.NewFromInt(60000), decimal.NewFromInt(65000), decimal.NewFromInt(3), decimal.NewFromInt(500))
	// Strike distance per contract 5000 * 3 = 15000, minus the (already
	// qty-scaled) 500 credit passed in -> 14500.
	want := decimal.NewFromInt(14500)
	if !got.Equal(want) {
		t.Fatalf("scaled spread margin = %s, want %s", got, want)
	}
}

func TestVerticalSpreadMargin_NeverExceedsStrikeDistance(t *testing.T) {
	// A negative "credit" (i.e. a debit) must never push required margin
	// above the strike-distance ceiling either — the debit branch already
	// returns 0 before reaching the ceiling check, but this documents the
	// invariant explicitly for the credit side too.
	strikeDistance := decimal.NewFromInt(5000)
	got := VerticalSpreadMargin(decimal.NewFromInt(60000), decimal.NewFromInt(65000), decimal.NewFromInt(1), decimal.NewFromInt(1))
	if got.GreaterThan(strikeDistance) {
		t.Fatalf("spread margin %s exceeds strike distance ceiling %s", got, strikeDistance)
	}
}

func TestVerticalSpreadMargin_ZeroQtyIsZero(t *testing.T) {
	got := VerticalSpreadMargin(decimal.NewFromInt(60000), decimal.NewFromInt(65000), decimal.Zero, decimal.NewFromInt(500))
	if !got.IsZero() {
		t.Fatalf("zero-qty spread margin = %s, want 0", got)
	}
}

func TestVerticalSpreadMargin_StrikeOrderDoesNotMatter(t *testing.T) {
	// Whether buyStrike is above or below sellStrike (bull call vs bear call
	// use of the same pair, or put spreads in either direction), the strike
	// DISTANCE is what matters, not which one is "buy".
	a := VerticalSpreadMargin(decimal.NewFromInt(60000), decimal.NewFromInt(65000), decimal.NewFromInt(1), decimal.NewFromInt(500))
	b := VerticalSpreadMargin(decimal.NewFromInt(65000), decimal.NewFromInt(60000), decimal.NewFromInt(1), decimal.NewFromInt(500))
	if !a.Equal(b) {
		t.Fatalf("margin should be symmetric in strike order: %s vs %s", a, b)
	}
}
