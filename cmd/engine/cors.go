package main

import (
	"net/http"
	"os"
	"strings"
)

// withCORS wraps handler with an allowlist-based, credentialed CORS policy.
// The allowlist comes from CORS_ORIGIN (comma-separated), falling back to
// common local dev ports when unset. The request's Origin is echoed back
// only if it matches, since Access-Control-Allow-Origin can't be "*" when
// credentials are allowed.
func withCORS(next http.Handler) http.Handler {
	origins := os.Getenv("CORS_ORIGIN")
	if origins == "" {
		origins = "http://localhost:8080,http://localhost:8081,http://localhost:8082,http://localhost:8083,http://localhost:5173"
	}
	allowed := make(map[string]bool)
	for _, o := range strings.Split(origins, ",") {
		if o = strings.TrimSpace(o); o != "" {
			allowed[o] = true
		}
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if origin := r.Header.Get("Origin"); allowed[origin] {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
		}
		w.Header().Set("Access-Control-Allow-Credentials", "true")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
