package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dex/matching-engine/internal/models"
)

func TestRequireEngineServiceAuth(t *testing.T) {
	t.Setenv("DEX_BACKEND_ENGINE_SECRET", "service-secret")
	h := requireEngineServiceAuth(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	unauthorized := httptest.NewRecorder()
	h.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/orders", nil))
	if unauthorized.Code != http.StatusForbidden {
		t.Fatalf("status without service secret = %d, want %d", unauthorized.Code, http.StatusForbidden)
	}

	authorizedRequest := httptest.NewRequest(http.MethodGet, "/orders", nil)
	authorizedRequest.Header.Set("X-Engine-Secret", "service-secret")
	authorized := httptest.NewRecorder()
	h.ServeHTTP(authorized, authorizedRequest)
	if authorized.Code != http.StatusNoContent {
		t.Fatalf("status with service secret = %d, want %d", authorized.Code, http.StatusNoContent)
	}
}

func TestRequireOrderOwner(t *testing.T) {
	order := &models.Order{AccountID: "owner"}
	if err := requireOrderOwner(order, "other-account"); err == nil {
		t.Fatal("expected cross-account cancellation to be rejected")
	}
	if err := requireOrderOwner(order, "owner"); err != nil {
		t.Fatalf("owner was rejected: %v", err)
	}
}
