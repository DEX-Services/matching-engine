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
	// 4 spot + 13 futures (BTC, ETH, SOL, BNB, 3 FX, 3 commodities, 3 stocks).
	// The futures rows after ETH-PERP are the non-crypto perp set. (Options
	// engines register lazily per contract and are not in this list.)
	if len(got) != 17 || got[0].DisplaySymbol != "BTC-USDB" || got[4].Symbol != "BTC-USDB" || got[4].Market != "FUTURES" {
		t.Fatalf("unexpected current markets: %#v", got)
	}
	if got[0].TickSize != "0.01" || len(got[0].EnabledOrderTypes) != 6 {
		t.Fatalf("missing usable metadata: %#v", got[0])
	}
	// The 9 non-crypto perps close the set: 3 FX, 3 commodities, 3 stocks,
	// in the order declared in currentMarkets.
	if got[8].Symbol != "EURUSD-USDB" || got[11].Symbol != "GOLD-USDB" || got[14].Symbol != "AAPL.us-USDB" {
		t.Fatalf("unexpected non-crypto perp layout: %#v", got[9:])
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
