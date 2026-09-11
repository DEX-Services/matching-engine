package main

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/dex/matching-engine/internal/models"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

// TestSubmitOrderPipeline_UnregisteredSymbolIs404 covers a real inconsistency
// found while disabling forex/commodities/stocks: with no early check, an
// order for a symbol/market that was never registered (either because it's a
// genuinely unknown pair, or a deliberately disabled one — see markets.go's
// disabledMarkets) fell through to whichever generic step happened to notice
// first. A funded account's MARKET order hit d.reg.Get() during its
// reservation-sizing lookup and got a 400 wrapped as a risk error; a LIMIT
// order could get much further before failing some other way. Neither gave a
// clean, consistent signal that the market simply doesn't exist.
//
// The fix moves the registration check early and unconditionally (for every
// non-options market), so every disabled/unknown symbol gets the exact same
// 404 "no engine registered for X/Y" regardless of order type — the same
// shape options already uses (see submit.go's optionsEnabled check right
// above this one), with nothing distinguishing "never existed" from
// "disabled by policy".
func TestSubmitOrderPipeline_UnregisteredSymbolIs404(t *testing.T) {
	d := newTestSubmitDeps(nil)

	cases := []struct {
		name  string
		order *models.Order
	}{
		{
			name: "market order for an unregistered symbol",
			order: &models.Order{
				ID: uuid.NewString(), AccountID: "acct1", Symbol: "EURUSD-BIUSD", Market: models.Futures,
				Side: models.Buy, Type: models.Market, Quantity: decimal.NewFromInt(1),
				TimeInForce: models.GTC, Status: models.StatusPending, CreatedAt: time.Now(),
			},
		},
		{
			name: "limit order for an unregistered symbol",
			order: &models.Order{
				ID: uuid.NewString(), AccountID: "acct1", Symbol: "GOLD-BIUSD", Market: models.Futures,
				Side: models.Buy, Type: models.Limit, Price: decimal.NewFromInt(2000), Quantity: decimal.NewFromInt(1),
				TimeInForce: models.GTC, Status: models.StatusPending, CreatedAt: time.Now(),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, status, err := submitOrderPipeline(context.Background(), d, tc.order, "")
			if err == nil {
				t.Fatal("expected a rejection for an unregistered symbol")
			}
			if status != http.StatusNotFound {
				t.Fatalf("status = %d, want %d", status, http.StatusNotFound)
			}
			if tc.order.Status != models.StatusRejected {
				t.Fatalf("order.Status = %s, want REJECTED", tc.order.Status)
			}
			wantMsg := "no engine registered for " + tc.order.Symbol + "/" + string(tc.order.Market)
			if tc.order.RejectReason != wantMsg {
				t.Fatalf("RejectReason = %q, want %q", tc.order.RejectReason, wantMsg)
			}
		})
	}
}

// TestSubmitOrderPipeline_RegisteredSymbolPassesTheCheck confirms the new
// early check does not itself become a false rejection for a symbol that IS
// registered — it should fail later (on funding), not here.
func TestSubmitOrderPipeline_RegisteredSymbolPassesTheCheck(t *testing.T) {
	d := newTestSubmitDeps(nil)
	if _, err := d.reg.Register("BTC-USDC", models.Spot); err != nil {
		t.Fatalf("register: %v", err)
	}

	o := &models.Order{
		ID: uuid.NewString(), AccountID: "acct1", Symbol: "BTC-USDC", Market: models.Spot,
		Side: models.Buy, Type: models.Market, Quantity: decimal.NewFromInt(1),
		TimeInForce: models.GTC, Status: models.StatusPending, CreatedAt: time.Now(),
	}
	_, _, status, err := submitOrderPipeline(context.Background(), d, o, "")
	if err == nil {
		t.Fatal("expected a rejection (unfunded account), just not a 404")
	}
	if status == http.StatusNotFound {
		t.Fatalf("a REGISTERED symbol must not be rejected as unregistered; RejectReason=%q", o.RejectReason)
	}
}
