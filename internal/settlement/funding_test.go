package settlement

import (
	"testing"

	"github.com/shopspring/decimal"
)

func TestCurrentFundingRate_ZeroIndex_ReturnsZero(t *testing.T) {
	rate := CurrentFundingRate(decimal.RequireFromString("50000"), decimal.Zero)
	if !rate.IsZero() {
		t.Fatalf("expected zero rate for zero index price, got %s", rate)
	}
}

func TestCurrentFundingRate_MarkAboveIndex_PositiveRate(t *testing.T) {
	// mark 0.2% above index -> raw rate 0.002, under the 0.75% cap so it passes through uncapped.
	mark := decimal.RequireFromString("50100")
	index := decimal.RequireFromString("50000")
	rate := CurrentFundingRate(mark, index)
	want := decimal.RequireFromString("0.002")
	if !rate.Equal(want) {
		t.Fatalf("rate = %s, want %s", rate, want)
	}
}

func TestCurrentFundingRate_MarkBelowIndex_NegativeRate(t *testing.T) {
	mark := decimal.RequireFromString("49900")
	index := decimal.RequireFromString("50000")
	rate := CurrentFundingRate(mark, index)
	want := decimal.RequireFromString("-0.002")
	if !rate.Equal(want) {
		t.Fatalf("rate = %s, want %s", rate, want)
	}
}

func TestCurrentFundingRate_CapsAtPositiveBound(t *testing.T) {
	// mark 5% above index -> raw rate 0.05, must clamp to the 0.75% cap.
	mark := decimal.RequireFromString("52500")
	index := decimal.RequireFromString("50000")
	rate := CurrentFundingRate(mark, index)
	if !rate.Equal(fundingRateCap) {
		t.Fatalf("rate = %s, want cap %s", rate, fundingRateCap)
	}
}

func TestCurrentFundingRate_CapsAtNegativeBound(t *testing.T) {
	mark := decimal.RequireFromString("47500")
	index := decimal.RequireFromString("50000")
	rate := CurrentFundingRate(mark, index)
	if !rate.Equal(fundingRateCap.Neg()) {
		t.Fatalf("rate = %s, want cap %s", rate, fundingRateCap.Neg())
	}
}

func TestCurrentFundingRate_MarkEqualsIndex_ZeroRate(t *testing.T) {
	price := decimal.RequireFromString("50000")
	rate := CurrentFundingRate(price, price)
	if !rate.IsZero() {
		t.Fatalf("expected zero rate when mark == index, got %s", rate)
	}
}

// This is the exact bug pattern the seed.go fix addresses: before that fix,
// a futures symbol's UnderlyingSymbol pointed at itself, so callers always
// passed the SAME price as both mark and index — this test documents why
// that's indistinguishable from "no funding ever due" and pins the
// zero-drift behavior so a future regression back to that pattern is caught
// here rather than silently in production.
func TestCurrentFundingRate_SamePriceBothSides_AlwaysZero(t *testing.T) {
	for _, p := range []string{"1", "50000", "0.0001", "999999"} {
		price := decimal.RequireFromString(p)
		if rate := CurrentFundingRate(price, price); !rate.IsZero() {
			t.Fatalf("price=%s: expected zero rate when mark==index, got %s", p, rate)
		}
	}
}
