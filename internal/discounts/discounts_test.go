package discounts

import (
	"testing"

	"github.com/dex/matching-engine/internal/fixedpoint"
)

// TestDiscountsEqual pins down discountsEqual's exact semantics — the
// comparison reload() now uses to decide whether to log "discount registry
// reloaded" (see PERFORMANCE-CODE-REVIEW-FINDINGS.md's follow-up issue:
// reload() used to unconditionally log on every 10s tick even with a
// completely static set of active subscribers). A wrong comparison here
// would either suppress a real membership/discount change's log line
// (false "unchanged") or keep the exact log spam this was meant to fix
// (false "changed").
func TestDiscountsEqual(t *testing.T) {
	f := func(s string) fixedpoint.Fixed { return fixedpoint.MustFromString(s) }

	cases := []struct {
		name string
		a, b map[string]fixedpoint.Fixed
		want bool
	}{
		{"both empty", map[string]fixedpoint.Fixed{}, map[string]fixedpoint.Fixed{}, true},
		{
			"identical single user",
			map[string]fixedpoint.Fixed{"user1": f("0.15")},
			map[string]fixedpoint.Fixed{"user1": f("0.15")},
			true,
		},
		{
			"identical multiple users, map iteration order must not matter",
			map[string]fixedpoint.Fixed{"user1": f("0.15"), "user2": f("0.25"), "user3": f("0.05")},
			map[string]fixedpoint.Fixed{"user3": f("0.05"), "user1": f("0.15"), "user2": f("0.25")},
			true,
		},
		{
			"same users, one discount changed (e.g. a tier upgrade)",
			map[string]fixedpoint.Fixed{"user1": f("0.15")},
			map[string]fixedpoint.Fixed{"user1": f("0.25")},
			false,
		},
		{
			"a new subscriber appeared",
			map[string]fixedpoint.Fixed{"user1": f("0.15")},
			map[string]fixedpoint.Fixed{"user1": f("0.15"), "user2": f("0.25")},
			false,
		},
		{
			"a subscriber's discount expired and they dropped out",
			map[string]fixedpoint.Fixed{"user1": f("0.15"), "user2": f("0.25")},
			map[string]fixedpoint.Fixed{"user1": f("0.15")},
			false,
		},
		{
			"same count, one subscriber replaced by a different one (churn)",
			map[string]fixedpoint.Fixed{"user1": f("0.15")},
			map[string]fixedpoint.Fixed{"user2": f("0.15")},
			false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := discountsEqual(tc.a, tc.b); got != tc.want {
				t.Fatalf("discountsEqual(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
			if got := discountsEqual(tc.b, tc.a); got != tc.want {
				t.Fatalf("discountsEqual is not symmetric: discountsEqual(b, a) = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestNewInMemoryRegistry_NoActiveDiscounts(t *testing.T) {
	r := NewInMemoryRegistry()
	if got := r.ActiveDiscountFor("anyone"); !got.IsZero() {
		t.Errorf("ActiveDiscountFor on an empty in-memory registry = %s, want zero", got)
	}
}
