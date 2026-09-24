package main

import (
	"net"
	"net/http"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// This engine has no rate limiting anywhere (grep-verified): every public
// route — /markets, /market-summary, /ticker, /depth, /trades, /ws — is
// reachable with no backoff, and this is a genuinely public-facing HTTP
// server (unlike most of the engine's other routes, which sit behind
// requireEngineServiceAuth's shared-secret check). Add a light, general
// per-IP limiter so scraping/DoS traffic against these read endpoints gets
// throttled instead of served at full rate forever. There's no auth
// endpoint on this service (no /auth/*, no /admin/login-equivalent reachable
// without the engine secret already), so unlike Dex-Backend's limiter there
// is only one tier here, not a stricter one for a login-style path.
type engineLimiterStore struct {
	mu       sync.Mutex
	limiters map[string]*engineRateEntry
	r        rate.Limit
	b        int
}

type engineRateEntry struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

func newEngineLimiterStore(r rate.Limit, b int) *engineLimiterStore {
	s := &engineLimiterStore{limiters: make(map[string]*engineRateEntry), r: r, b: b}
	go s.reapLoop()
	return s
}

func (s *engineLimiterStore) allow(key string) bool {
	s.mu.Lock()
	entry, ok := s.limiters[key]
	if !ok {
		entry = &engineRateEntry{limiter: rate.NewLimiter(s.r, s.b)}
		s.limiters[key] = entry
	}
	entry.lastSeen = time.Now()
	limiter := entry.limiter
	s.mu.Unlock()
	return limiter.Allow()
}

// reapLoop bounds memory to roughly the number of distinct IPs seen in the
// last 10 minutes, not the lifetime of the process.
func (s *engineLimiterStore) reapLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		cutoff := time.Now().Add(-10 * time.Minute)
		s.mu.Lock()
		for k, e := range s.limiters {
			if e.lastSeen.Before(cutoff) {
				delete(s.limiters, k)
			}
		}
		s.mu.Unlock()
	}
}

// engineMaxBodyBytes caps every request body (M2): no handler in this
// service set any body-size limit before this, so an oversized request
// (e.g. to /order or /attached-order) was fully buffered/decoded before any
// validation ran. 1 MB is generous for every legitimate JSON payload this
// engine accepts.
const engineMaxBodyBytes = 1 << 20

// withMaxBody wraps next so every request body is capped at
// engineMaxBodyBytes; /ws is exempt since it isn't a body-carrying request.
func withMaxBody(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ws" {
			r.Body = http.MaxBytesReader(w, r.Body, engineMaxBodyBytes)
		}
		next.ServeHTTP(w, r)
	})
}

// withRateLimit wraps next with a per-client-IP token bucket: 40 req/sec
// sustained, burst of 80 — generous enough for a trade page polling several
// markets' /ticker and /depth every second without ever hitting it, but
// enough to stop an unthrottled scraping/DoS loop from hammering the engine
// at line rate. /ws upgrades are exempt (a WS connection isn't a repeated
// HTTP request in the same sense; its own message-rate limits, if ever
// needed, are a separate concern from this per-request limiter).
func withRateLimit(next http.Handler) http.Handler {
	store := newEngineLimiterStore(40, 80)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ws" {
			next.ServeHTTP(w, r)
			return
		}
		key := r.RemoteAddr
		if host, _, err := net.SplitHostPort(key); err == nil {
			key = host
		}
		// Loopback is this deployment's own service mesh (bots' fill-detect +
		// requote polls, Dex-Backend's settle/credit callbacks all originate
		// from 127.0.0.1). With 11+ MM desks each polling per index tick, the
		// aggregate easily exceeds the public-facing 40 req/s budget and every
		// desk started 429-ing. The limiter exists to throttle untrusted public
		// traffic, not our own inter-service calls — exempt loopback entirely.
		if key == "127.0.0.1" || key == "::1" {
			next.ServeHTTP(w, r)
			return
		}
		if !store.allow(key) {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "too many requests", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}
