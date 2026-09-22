package risk

import (
	"testing"

	"github.com/dex/matching-engine/internal/fixedpoint"
)

func TestComboMaxLossMargin_MatchesVerticalSpreadMargin(t *testing.T) {
	// Every case VerticalSpreadMargin's own test suite covers, re-expressed
	// as a 2-leg ComboLegSpec combo, must produce the identical result —
	// this is the equivalence VerticalSpreadMargin's doc comment promises.
	//
	// buyStrike is always the LONG leg's strike, sellStrike the SHORT leg's.
	// A netCredit input is only physically realistic when buyStrike is the
	// HIGHER strike (long the higher/short the lower = a credit-collecting
	// short call spread, or the put-spread mirror); pairing a net CREDIT
	// with buyStrike < sellStrike (a debit-shaped bull call spread) is not a
	// real market configuration and is intentionally not exercised here.
	cases := []struct {
		name                  string
		buyStrike, sellStrike int64
		qty, netCredit        int64
	}{
		{"net debit needs no margin", 60000, 65000, 1, -1000},
		{"exact zero premium needs full strike distance", 65000, 60000, 1, 0},
		{"net credit needs strike distance minus credit", 65000, 60000, 1, 500},
		{"credit exceeding strike distance floors at zero", 65000, 60000, 1, 6000},
		{"scales with qty", 65000, 60000, 3, 1500},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buyStrike := fixedpoint.FromInt64(tc.buyStrike)
			sellStrike := fixedpoint.FromInt64(tc.sellStrike)
			qty := fixedpoint.FromInt64(tc.qty)
			netCredit := fixedpoint.FromInt64(tc.netCredit)

			want := VerticalSpreadMargin(buyStrike, sellStrike, qty, netCredit)
			got := ComboMaxLossMargin([]ComboLegSpec{
				{Strike: buyStrike, OptionType: "CALL", Ratio: 1},
				{Strike: sellStrike, OptionType: "CALL", Ratio: -1},
			}, qty, netCredit)
			if !got.Equal(want) {
				t.Fatalf("ComboMaxLossMargin = %s, VerticalSpreadMargin = %s (should match)", got, want)
			}
		})
	}
}

func TestComboMaxLossMargin_PutVerticalMatchesToo(t *testing.T) {
	// Bear put spread: buy 65000 put, sell 60000 put, opened for a 500 debit.
	buyStrike := fixedpoint.FromInt64(65000)
	sellStrike := fixedpoint.FromInt64(60000)
	qty := fixedpoint.FromInt64(1)
	netCredit := fixedpoint.FromInt64(-500) // debit paid

	want := VerticalSpreadMargin(buyStrike, sellStrike, qty, netCredit)
	got := ComboMaxLossMargin([]ComboLegSpec{
		{Strike: buyStrike, OptionType: "PUT", Ratio: 1},
		{Strike: sellStrike, OptionType: "PUT", Ratio: -1},
	}, qty, netCredit)
	if !got.Equal(want) {
		t.Fatalf("put vertical: ComboMaxLossMargin = %s, VerticalSpreadMargin = %s", got, want)
	}
}

func TestComboMaxLossMargin_IronCondorMaxLossIsWingWidth(t *testing.T) {
	// Classic iron condor: sell 55000 put, buy 50000 put (put spread, width 5000),
	// sell 65000 call, buy 70000 call (call spread, width 5000). Both wings
	// have the same width, so max loss = wing width - net credit received.
	legs := []ComboLegSpec{
		{Strike: fixedpoint.FromInt64(50000), OptionType: "PUT", Ratio: 1},   // long put wing
		{Strike: fixedpoint.FromInt64(55000), OptionType: "PUT", Ratio: -1},  // short put
		{Strike: fixedpoint.FromInt64(65000), OptionType: "CALL", Ratio: -1}, // short call
		{Strike: fixedpoint.FromInt64(70000), OptionType: "CALL", Ratio: 1},  // long call wing
	}
	netCredit := fixedpoint.FromInt64(800) // opened for an 800 credit
	qty := fixedpoint.FromInt64(1)

	got := ComboMaxLossMargin(legs, qty, netCredit)
	want := fixedpoint.FromInt64(5000).Sub(netCredit) // wing width (5000) - credit
	if !got.Equal(want) {
		t.Fatalf("iron condor max loss margin = %s, want %s", got, want)
	}
}

func TestComboMaxLossMargin_IronCondorBoundedEvenAtExtremeUnderlying(t *testing.T) {
	// The whole point of an iron condor: loss is capped even if the
	// underlying goes to a huge extreme, unlike a naked short strangle.
	// Verify the margin doesn't grow unboundedly by checking it against the
	// wing-width-based expectation from the previous test's structure,
	// independent of any assumption about "the underlying's current price"
	// (this margin model is price-independent by construction — it is the
	// true worst case over ALL possible prices, not a VaR estimate).
	legs := []ComboLegSpec{
		{Strike: fixedpoint.FromInt64(50000), OptionType: "PUT", Ratio: 1},
		{Strike: fixedpoint.FromInt64(55000), OptionType: "PUT", Ratio: -1},
		{Strike: fixedpoint.FromInt64(65000), OptionType: "CALL", Ratio: -1},
		{Strike: fixedpoint.FromInt64(70000), OptionType: "CALL", Ratio: 1},
	}
	got := ComboMaxLossMargin(legs, fixedpoint.FromInt64(1), fixedpoint.Zero)
	want := fixedpoint.FromInt64(5000) // wing width, zero credit case
	if !got.Equal(want) {
		t.Fatalf("iron condor margin at zero credit = %s, want %s", got, want)
	}
}

func TestComboMaxLossMargin_ButterflyMaxLossIsNetDebit(t *testing.T) {
	// Long call butterfly: buy 1x 55000 call, sell 2x 60000 call, buy 1x
	// 65000 call. This is a net-debit structure whose max loss is exactly
	// the debit paid (like a vertical debit spread) — verify the debit
	// branch applies correctly with a ratio-2 middle leg.
	legs := []ComboLegSpec{
		{Strike: fixedpoint.FromInt64(55000), OptionType: "CALL", Ratio: 1},
		{Strike: fixedpoint.FromInt64(60000), OptionType: "CALL", Ratio: -2},
		{Strike: fixedpoint.FromInt64(65000), OptionType: "CALL", Ratio: 1},
	}
	netCredit := fixedpoint.FromInt64(-200) // 200 debit paid to open
	got := ComboMaxLossMargin(legs, fixedpoint.FromInt64(1), netCredit)
	if !got.IsZero() {
		t.Fatalf("debit-financed butterfly margin = %s, want 0 (the debit paid already IS the max loss)", got)
	}
}

func TestComboMaxLossMargin_ScalesWithQty(t *testing.T) {
	legs := []ComboLegSpec{
		{Strike: fixedpoint.FromInt64(60000), OptionType: "CALL", Ratio: -1},
		{Strike: fixedpoint.FromInt64(65000), OptionType: "CALL", Ratio: 1},
	}
	one := ComboMaxLossMargin(legs, fixedpoint.FromInt64(1), fixedpoint.FromInt64(500))
	three := ComboMaxLossMargin(legs, fixedpoint.FromInt64(3), fixedpoint.FromInt64(1500)) // netCredit also scales with qty in real usage
	want := one.Mul(fixedpoint.FromInt64(3))
	if !three.Equal(want) {
		t.Fatalf("3x qty margin = %s, want %s (3x the 1-qty margin)", three, want)
	}
}

func TestComboMaxLossMargin_EmptyLegsIsZero(t *testing.T) {
	if got := ComboMaxLossMargin(nil, fixedpoint.FromInt64(1), fixedpoint.FromInt64(500)); !got.IsZero() {
		t.Fatalf("empty-legs margin = %s, want 0", got)
	}
}

func TestComboMaxLossMargin_ZeroQtyIsZero(t *testing.T) {
	legs := []ComboLegSpec{
		{Strike: fixedpoint.FromInt64(60000), OptionType: "CALL", Ratio: -1},
		{Strike: fixedpoint.FromInt64(65000), OptionType: "CALL", Ratio: 1},
	}
	if got := ComboMaxLossMargin(legs, fixedpoint.Zero, fixedpoint.FromInt64(500)); !got.IsZero() {
		t.Fatalf("zero-qty margin = %s, want 0", got)
	}
}
