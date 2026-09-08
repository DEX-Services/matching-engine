package backendclient

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

func TestToRawUnits_ConvertsDollarDecimalToRawInteger(t *testing.T) {
	cases := map[string]string{
		"40":            "40000000",
		"9.97531483311": "9975314", // truncated, not rounded
		"0.000001":      "1",
		"0":             "0",
	}
	for in, want := range cases {
		got := ToRawUnits(decimal.RequireFromString(in))
		if got != want {
			t.Errorf("ToRawUnits(%s) = %s, want %s", in, got, want)
		}
	}
}

func TestNew_DisabledWhenUnconfigured(t *testing.T) {
	t.Setenv("DEX_BACKEND_URL", "")
	t.Setenv("DEX_BACKEND_ENGINE_SECRET", "")

	c := New()
	if c.Enabled() {
		t.Fatal("expected disabled client when env vars are unset")
	}
	if err := c.Lock(context.Background(), "u1", "USDC", "100"); err != nil {
		t.Fatalf("disabled Lock should no-op, got %v", err)
	}
	if err := c.Unlock(context.Background(), "u1", "USDC", "100"); err != nil {
		t.Fatalf("disabled Unlock should no-op, got %v", err)
	}
	if err := c.Settle(context.Background(), "u1", "USDC", "100"); err != nil {
		t.Fatalf("disabled Settle should no-op, got %v", err)
	}
}

func TestNew_EnabledWhenConfigured(t *testing.T) {
	t.Setenv("DEX_BACKEND_URL", "http://localhost:9999")
	t.Setenv("DEX_BACKEND_ENGINE_SECRET", "secret")

	c := New()
	if !c.Enabled() {
		t.Fatal("expected enabled client when both env vars are set")
	}
}

func TestClient_Lock_SuccessSendsCorrectRequest(t *testing.T) {
	var gotMethod, gotPath, gotSecret string
	var gotBody balanceReq
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotSecret = r.Header.Get("X-Engine-Secret")
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := &Client{baseURL: srv.URL, secret: "s3cr3t", http: srv.Client()}
	if err := c.Lock(context.Background(), "DEXUSER_1", "USDC", "500"); err != nil {
		t.Fatalf("Lock: %v", err)
	}
	if gotMethod != http.MethodPost {
		t.Fatalf("method = %s, want POST", gotMethod)
	}
	if gotPath != "/internal/balance/lock" {
		t.Fatalf("path = %s, want /internal/balance/lock", gotPath)
	}
	if gotSecret != "s3cr3t" {
		t.Fatalf("X-Engine-Secret = %q, want s3cr3t", gotSecret)
	}
	if gotBody.UserID != "DEXUSER_1" || gotBody.Asset != "USDC" || gotBody.Amount != "500" {
		t.Fatalf("body = %+v, want {DEXUSER_1 USDC 500}", gotBody)
	}
}

func TestClient_Lock_NonOKStatusReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "insufficient real funds", http.StatusConflict)
	}))
	defer srv.Close()

	c := &Client{baseURL: srv.URL, secret: "s", http: srv.Client()}
	if err := c.Lock(context.Background(), "u1", "USDC", "500"); err == nil {
		t.Fatal("expected error on non-200 status, got nil")
	}
}

func TestClient_UnlockAndSettle_HitCorrectPaths(t *testing.T) {
	var gotPaths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPaths = append(gotPaths, r.URL.Path)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := &Client{baseURL: srv.URL, secret: "s", http: srv.Client()}
	if err := c.Unlock(context.Background(), "u1", "USDC", "10"); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
	if err := c.Settle(context.Background(), "u1", "USDC", "10"); err != nil {
		t.Fatalf("Settle: %v", err)
	}
	if len(gotPaths) != 2 || gotPaths[0] != "/internal/balance/unlock" || gotPaths[1] != "/internal/balance/settle" {
		t.Fatalf("paths = %v, want [/internal/balance/unlock /internal/balance/settle]", gotPaths)
	}
}

func TestClient_Unreachable_ReturnsError(t *testing.T) {
	c := &Client{baseURL: "http://127.0.0.1:1", secret: "s", http: &http.Client{Timeout: time.Second}}
	if err := c.Lock(context.Background(), "u1", "USDC", "1"); err == nil {
		t.Fatal("expected error calling an unreachable backend, got nil")
	}
}

func TestClient_Backfill_SuccessParsesResponse(t *testing.T) {
	var gotPath, gotSecret string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotSecret = r.Header.Get("X-Engine-Secret")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]int{"synced": 7, "failed": 1, "total": 8})
	}))
	defer srv.Close()

	c := &Client{baseURL: srv.URL, secret: "s3cr3t", http: srv.Client()}
	synced, failed, total, err := c.Backfill(context.Background())
	if err != nil {
		t.Fatalf("Backfill: %v", err)
	}
	if synced != 7 || failed != 1 || total != 8 {
		t.Fatalf("got synced=%d failed=%d total=%d, want 7 1 8", synced, failed, total)
	}
	if gotPath != "/internal/engine-backfill" {
		t.Fatalf("path = %s, want /internal/engine-backfill", gotPath)
	}
	if gotSecret != "s3cr3t" {
		t.Fatalf("X-Engine-Secret = %q, want s3cr3t", gotSecret)
	}
}

func TestClient_Backfill_DisabledIsNoop(t *testing.T) {
	c := &Client{}
	synced, failed, total, err := c.Backfill(context.Background())
	if err != nil || synced != 0 || failed != 0 || total != 0 {
		t.Fatalf("disabled Backfill should no-op, got synced=%d failed=%d total=%d err=%v", synced, failed, total, err)
	}
}

func TestAsync_RunsFnWithoutBlockingCaller(t *testing.T) {
	done := make(chan struct{})
	start := time.Now()
	Async("test-op", func(ctx context.Context) error {
		defer close(done)
		return fmt.Errorf("simulated failure")
	})
	// Async must return immediately; the goroutine does the work.
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("Async blocked the caller for %s, want near-instant return", elapsed)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Async did not execute fn within timeout")
	}
}
