package risk

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dex/matching-engine/internal/backendclient"
	"github.com/dex/matching-engine/internal/models"
	"github.com/shopspring/decimal"
)

func newMarketOrder() *models.Order {
	return &models.Order{
		ID: "o1", AccountID: "buyer", Symbol: "BTC-USDT",
		Side: models.Buy, Type: models.Market,
		Quantity: decimal.RequireFromString("1"),
	}
}

func TestRequiredFor_MarketOrderSkipped(t *testing.T) {
	asset, amount := RequiredFor(newMarketOrder())
	if asset != "" || !amount.IsZero() {
		t.Fatalf("RequiredFor(market) = (%q, %s), want (\"\", 0)", asset, amount)
	}
}

func TestReleaseAmountFor_MarketOrderSkipped(t *testing.T) {
	asset, amount := ReleaseAmountFor(newMarketOrder())
	if asset != "" || !amount.IsZero() {
		t.Fatalf("ReleaseAmountFor(market) = (%q, %s), want (\"\", 0)", asset, amount)
	}
}

func TestReserve_BackendLockFailure_RollsBackReservation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "insufficient real funds", http.StatusConflict)
	}))
	defer srv.Close()
	t.Setenv("DEX_BACKEND_URL", srv.URL)
	t.Setenv("DEX_BACKEND_ENGINE_SECRET", "s")
	backend := backendclient.New()

	ledger := NewLedger()
	checker := NewChecker(ledger)
	ledger.Deposit("buyer", "USDT", decimal.NewFromInt(1000))

	order := newBuyOrder("1", "100")
	if err := checker.Reserve(order); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if got := ledger.Available("buyer", "USDT"); !got.Equal(decimal.NewFromInt(900)) {
		t.Fatalf("available after reserve = %s, want 900", got)
	}

	lockAsset, lockAmount := RequiredFor(order)
	if err := backend.Lock(context.Background(), order.AccountID, lockAsset, lockAmount.String()); err == nil {
		t.Fatal("expected backend Lock to fail")
	}
	checker.Release(order)

	if got := ledger.Available("buyer", "USDT"); !got.Equal(decimal.NewFromInt(1000)) {
		t.Fatalf("available after rollback = %s, want 1000 (fully restored)", got)
	}
	if got := ledger.Reserved("buyer", "USDT"); !got.IsZero() {
		t.Fatalf("reserved after rollback = %s, want 0", got)
	}
}

func TestReserve_BackendLockSuccess_NoRollback(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	t.Setenv("DEX_BACKEND_URL", srv.URL)
	t.Setenv("DEX_BACKEND_ENGINE_SECRET", "s")
	backend := backendclient.New()

	ledger := NewLedger()
	checker := NewChecker(ledger)
	ledger.Deposit("buyer", "USDT", decimal.NewFromInt(1000))

	order := newBuyOrder("1", "100")
	if err := checker.Reserve(order); err != nil {
		t.Fatalf("reserve: %v", err)
	}

	lockAsset, lockAmount := RequiredFor(order)
	if err := backend.Lock(context.Background(), order.AccountID, lockAsset, lockAmount.String()); err != nil {
		t.Fatalf("expected backend Lock to succeed, got %v", err)
	}

	if got := ledger.Available("buyer", "USDT"); !got.Equal(decimal.NewFromInt(900)) {
		t.Fatalf("available after successful lock = %s, want 900 (reservation kept)", got)
	}
	if got := ledger.Reserved("buyer", "USDT"); !got.Equal(decimal.NewFromInt(100)) {
		t.Fatalf("reserved after successful lock = %s, want 100", got)
	}
}
