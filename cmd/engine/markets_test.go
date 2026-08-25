package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dex/matching-engine/internal/config"
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
	if len(got) != 5 || got[0].DisplaySymbol != "BTC-USDT" || got[3].Symbol != "BTC-USDC" || got[3].Market != "FUTURES" {
		t.Fatalf("unexpected current markets: %#v", got)
	}
	if got[0].TickSize != "0.01" || len(got[0].EnabledOrderTypes) != 6 {
		t.Fatalf("missing usable metadata: %#v", got[0])
	}
}
