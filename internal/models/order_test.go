package models

import "testing"

// TestIsMarketMakerAccount is a regression test for the account-origin
// tagging introduced to route market-maker desk order events onto their
// own Kafka topic/writer, separate from real user order events — see
// Order.IsMarketMaker's doc comment. This platform's real user IDs are
// always "DEXUSER_<n>" (Dex-Backend/internal/repo/users.go); MM desk
// wallets are always "mm:<BASE>:<market>" (bots/internal/mm/service.go's
// walletFor) — the two id spaces never overlap, so a plain prefix check is
// sufficient and cannot be spoofed by a real user's own account id.
func TestIsMarketMakerAccount(t *testing.T) {
	cases := []struct {
		accountID string
		want      bool
	}{
		{"mm:BI2X:spot", true},
		{"mm:BTC:futures", true},
		{"mm:", true}, // degenerate but still matches the prefix
		{"DEXUSER_9", false},
		{"DEXUSER_252", false},
		{"buyer", false},  // the demo/no-wallet fallback account (see internal/account.DEMO_ACCOUNT-equivalent)
		{"seller", false}, // ditto
		{"", false},
		{"MM:BI2X:spot", false}, // case-sensitive: real desks always lowercase "mm:", never seen otherwise
	}
	for _, tc := range cases {
		t.Run(tc.accountID, func(t *testing.T) {
			if got := IsMarketMakerAccount(tc.accountID); got != tc.want {
				t.Errorf("IsMarketMakerAccount(%q) = %v, want %v", tc.accountID, got, tc.want)
			}
		})
	}
}
