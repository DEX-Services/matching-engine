package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestWithRateLimit_ExemptsLoopback is a regression test for a live
// incident: bots and Dex-Backend both call this engine over 127.0.0.1
// (same-host deployment), and with 11+ market-maker desks each polling per
// index tick, their combined loopback traffic exceeded the 40 req/s
// public-facing budget — every desk started getting 429 "too many
// requests" and couldn't place orders. Loopback is this deployment's own
// trusted service mesh, not the untrusted public traffic the limiter
// exists to throttle, so it must never be rate-limited regardless of
// volume.
func TestWithRateLimit_ExemptsLoopback(t *testing.T) {
	h := withRateLimit(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Far more requests than the 40 req/s / burst-80 budget would allow for
	// any single non-exempt IP — every one of these must still succeed.
	for i := 0; i < 200; i++ {
		req := httptest.NewRequest(http.MethodGet, "/ticker", nil)
		req.RemoteAddr = "127.0.0.1:54321"
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("request %d from loopback got status %d, want %d (loopback must never be rate-limited)", i, w.Code, http.StatusOK)
		}
	}

	// IPv6 loopback must be exempt too.
	for i := 0; i < 200; i++ {
		req := httptest.NewRequest(http.MethodGet, "/ticker", nil)
		req.RemoteAddr = "[::1]:54321"
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("request %d from ::1 got status %d, want %d (IPv6 loopback must never be rate-limited)", i, w.Code, http.StatusOK)
		}
	}
}

// TestWithRateLimit_StillThrottlesNonLoopback confirms the loopback
// exemption didn't accidentally disable the limiter altogether — a
// non-loopback IP sending a burst well beyond the 40 req/s / burst-80
// budget must still see at least one 429, exactly as before this fix.
func TestWithRateLimit_StillThrottlesNonLoopback(t *testing.T) {
	h := withRateLimit(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	sawTooManyRequests := false
	for i := 0; i < 200; i++ {
		req := httptest.NewRequest(http.MethodGet, "/ticker", nil)
		req.RemoteAddr = "203.0.113.7:54321"
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code == http.StatusTooManyRequests {
			sawTooManyRequests = true
			break
		}
	}
	if !sawTooManyRequests {
		t.Fatal("expected a non-loopback IP sending 200 rapid requests to eventually get 429, got none — the limiter itself may be broken")
	}
}

// TestWithRateLimit_WSUpgradeExempt confirms /ws stays exempt for both
// loopback and non-loopback callers, unchanged by the loopback fix.
func TestWithRateLimit_WSUpgradeExempt(t *testing.T) {
	h := withRateLimit(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	for i := 0; i < 200; i++ {
		req := httptest.NewRequest(http.MethodGet, "/ws", nil)
		req.RemoteAddr = "203.0.113.7:54321"
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("request %d to /ws got status %d, want %d", i, w.Code, http.StatusOK)
		}
	}
}
