package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dex/matching-engine/internal/config"
	"github.com/dex/matching-engine/internal/models"
)

func TestMarketsHandlerReturnsOnlyCurrentExecutionSet(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/markets", nil)
	rec := httptest.NewRecorder()
	marketsHandler(config.NewInMemoryRegistry()).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	var got []MarketMetadata
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	// 2026-09-12 market-list restructure: SPOT is BI2X + BTC only; FUTURES is
	// BI2X, BTC, ETH, AVAX, LINK, SOL, DOGE, TAO, ADA, XRP — 2 + 10 = 12
	// total. ETH/SOL/BNB spot and BNB futures were REMOVED (not just
	// disabled — see deactivateRemovedMarkets in seed.go); crypto-only
	// forex/commodities/stocks remain separately disabled (disabledMarkets
	// below, unrelated to this restructure). (Options engines register
	// lazily per contract and are not in this list.)
	if len(got) != 12 {
		t.Fatalf("got %d markets, want 12: %#v", len(got), got)
	}
	if got[0].DisplaySymbol != "BTC-BIUSDB" || got[0].Market != "SPOT" {
		t.Fatalf("expected BTC-BIUSDB SPOT first: %#v", got[0])
	}
	if got[1].DisplaySymbol != "BI2X-BIUSDB" || got[1].Market != "SPOT" {
		t.Fatalf("expected BI2X-BIUSDB SPOT second: %#v", got[1])
	}
	// No SPOT row beyond BTC/BI2X — the whole point of the restructure.
	for _, m := range got {
		if m.Market == "SPOT" && m.DisplaySymbol != "BTC-BIUSDB" && m.DisplaySymbol != "BI2X-BIUSDB" {
			t.Fatalf("unexpected SPOT market %q survived the restructure: %#v", m.DisplaySymbol, m)
		}
	}
	wantFutures := []string{"BTC-PERP", "BI2X-PERP", "ETH-PERP", "AVAX-PERP", "LINK-PERP", "SOL-PERP", "DOGE-PERP", "TAO-PERP", "ADA-PERP", "XRP-PERP"}
	var gotFutures []string
	for _, m := range got {
		if m.Market == "FUTURES" {
			gotFutures = append(gotFutures, m.DisplaySymbol)
		}
	}
	if len(gotFutures) != len(wantFutures) {
		t.Fatalf("got %d futures markets %v, want %v", len(gotFutures), gotFutures, wantFutures)
	}
	for i, want := range wantFutures {
		if gotFutures[i] != want {
			t.Fatalf("futures[%d] = %q, want %q (order: %v)", i, gotFutures[i], want, gotFutures)
		}
	}
	// BNB must not appear anywhere — removed from both spot and futures.
	for _, m := range got {
		if m.BaseCurrency == "BNB" {
			t.Fatalf("BNB must be fully removed, found: %#v", m)
		}
	}
	if got[0].TickSize != "0.01" || len(got[0].EnabledOrderTypes) != 6 {
		t.Fatalf("missing usable metadata: %#v", got[0])
	}
}

func TestValidateConfiguredLeverage(t *testing.T) {
	cfg := &config.SymbolConfig{MaxLeverage: 75}
	order := &models.Order{Market: models.Futures, Leverage: 76}
	if err := validateConfiguredLeverage(cfg, order); err == nil {
		t.Fatal("expected leverage above configured maximum to be rejected")
	}
	order.Leverage = 75
	if err := validateConfiguredLeverage(cfg, order); err != nil {
		t.Fatalf("configured maximum should be accepted: %v", err)
	}
}
