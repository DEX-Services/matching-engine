package feeconfig

import (
	"testing"

	"github.com/dex/matching-engine/internal/fixedpoint"
)

// TestRatesEqual pins down ratesEqual's exact semantics — the comparison
// reload() now uses to decide whether to log "fee config reloaded" (see
// PERFORMANCE-CODE-REVIEW-FINDINGS.md's follow-up issue: reload() used to
// unconditionally log on every 10s tick even when nothing had changed).
// A wrong comparison here would either suppress a real change's log line
// (false "unchanged") or keep the exact log spam this was meant to fix
// (false "changed").
func TestRatesEqual(t *testing.T) {
	f := func(s string) fixedpoint.Fixed { return fixedpoint.MustFromString(s) }

	cases := []struct {
		name string
		a, b map[string]fixedpoint.Fixed
		want bool
	}{
		{"both empty", map[string]fixedpoint.Fixed{}, map[string]fixedpoint.Fixed{}, true},
		{
			"identical single entry",
			map[string]fixedpoint.Fixed{KeySpotMaker: f("0.0015")},
			map[string]fixedpoint.Fixed{KeySpotMaker: f("0.0015")},
			true,
		},
		{
			"identical multiple entries, map iteration order must not matter",
			map[string]fixedpoint.Fixed{KeySpotMaker: f("0.0015"), KeySpotTaker: f("0.0045"), KeyLiquidation: f("0.02")},
			map[string]fixedpoint.Fixed{KeyLiquidation: f("0.02"), KeySpotTaker: f("0.0045"), KeySpotMaker: f("0.0015")},
			true,
		},
		{
			"same keys, one value changed",
			map[string]fixedpoint.Fixed{KeySpotMaker: f("0.0015"), KeySpotTaker: f("0.0045")},
			map[string]fixedpoint.Fixed{KeySpotMaker: f("0.0020"), KeySpotTaker: f("0.0045")},
			false,
		},
		{
			"b has an extra key",
			map[string]fixedpoint.Fixed{KeySpotMaker: f("0.0015")},
			map[string]fixedpoint.Fixed{KeySpotMaker: f("0.0015"), KeySpotTaker: f("0.0045")},
			false,
		},
		{
			"a has an extra key",
			map[string]fixedpoint.Fixed{KeySpotMaker: f("0.0015"), KeySpotTaker: f("0.0045")},
			map[string]fixedpoint.Fixed{KeySpotMaker: f("0.0015")},
			false,
		},
		{
			"same key count, disjoint key sets",
			map[string]fixedpoint.Fixed{KeySpotMaker: f("0.0015")},
			map[string]fixedpoint.Fixed{KeySpotTaker: f("0.0015")},
			false,
		},
		{
			"a row disappeared entirely (e.g. deleted from fee_config)",
			map[string]fixedpoint.Fixed{KeySpotMaker: f("0.0015"), KeySwapOut: f("0.01")},
			map[string]fixedpoint.Fixed{KeySpotMaker: f("0.0015")},
			false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ratesEqual(tc.a, tc.b); got != tc.want {
				t.Fatalf("ratesEqual(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
			// Must be symmetric — a wrong implementation that only checks
			// "every key in a exists in b with the same value" would
			// silently miss b having EXTRA keys of the same count as a
			// missing ones, a real bug shape for this kind of comparison.
			if got := ratesEqual(tc.b, tc.a); got != tc.want {
				t.Fatalf("ratesEqual is not symmetric: ratesEqual(b, a) = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestNewInMemoryRegistry_SeedsLaunchDefaults(t *testing.T) {
	r := NewInMemoryRegistry()
	for _, key := range ValidKeys() {
		if r.Rate(key).IsNegative() {
			t.Errorf("Rate(%q) is negative: %s", key, r.Rate(key))
		}
	}
	// Spot maker specifically, since it's the most commonly read rate.
	want := fixedpoint.MustFromString(defaultRates[KeySpotMaker])
	if got := r.Rate(KeySpotMaker); !got.Equal(want) {
		t.Errorf("Rate(KeySpotMaker) = %s, want %s", got, want)
	}
}
