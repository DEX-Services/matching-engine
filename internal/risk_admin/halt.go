// Package risk_admin implements the symbol-wide halt mechanism (circuit breaker).
//
// This is deliberately separate from per-order risk rejection (Section 6 of spec).
// A halt stops the matching goroutine from consuming its input channel entirely.
// It is controlled via an admin API, not via risk rules.
package risk_admin

import (
	"fmt"
	"log/slog"
	"sync"
)

// HaltReason records why a symbol was halted.
type HaltReason string

const (
	HaltManual      HaltReason = "MANUAL"        // operator-initiated
	HaltCircuitBreak HaltReason = "CIRCUIT_BREAK" // automatic price-band breach
	HaltMaintenance HaltReason = "MAINTENANCE"
)

// HaltRecord stores the halt state for one symbol.
type HaltRecord struct {
	Symbol string
	Market string
	Reason HaltReason
	Note   string
}

// Registry is the global halt state store.
// The matching engine checks IsHalted() atomically via its engine.halted flag.
// This registry provides the admin control plane that flips that flag.
type Registry struct {
	mu     sync.RWMutex
	halted map[string]*HaltRecord // key: symbol+":"+market
	log    *slog.Logger

	// HaltFunc / ResumeFunc are injected callbacks that flip the matching
	// engine's atomic halt flag. Set these during wiring in main.go.
	HaltFunc   func(symbol, market string)
	ResumeFunc func(symbol, market string)
}

// NewRegistry creates an empty halt registry.
func NewRegistry() *Registry {
	return &Registry{
		halted: make(map[string]*HaltRecord),
		log:    slog.Default(),
	}
}

// Halt stops trading on symbol/market.
func (r *Registry) Halt(symbol, market string, reason HaltReason, note string) error {
	key := symbol + ":" + market
	r.mu.Lock()
	r.halted[key] = &HaltRecord{Symbol: symbol, Market: market, Reason: reason, Note: note}
	r.mu.Unlock()

	if r.HaltFunc != nil {
		r.HaltFunc(symbol, market)
	}
	r.log.Warn("symbol halted", "symbol", symbol, "market", market, "reason", reason, "note", note)
	return nil
}

// Resume re-enables trading on symbol/market.
func (r *Registry) Resume(symbol, market string) error {
	key := symbol + ":" + market
	r.mu.Lock()
	delete(r.halted, key)
	r.mu.Unlock()

	if r.ResumeFunc != nil {
		r.ResumeFunc(symbol, market)
	}
	r.log.Info("symbol resumed", "symbol", symbol, "market", market)
	return nil
}

// IsHalted reports whether trading on symbol/market is currently halted.
func (r *Registry) IsHalted(symbol, market string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.halted[symbol+":"+market]
	return ok
}

// HaltedSymbols returns all currently halted symbol/market pairs.
func (r *Registry) HaltedSymbols() []HaltRecord {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]HaltRecord, 0, len(r.halted))
	for _, rec := range r.halted {
		out = append(out, *rec)
	}
	return out
}

// HaltInfo returns the halt record for the given symbol/market, or an error if not halted.
func (r *Registry) HaltInfo(symbol, market string) (*HaltRecord, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	rec, ok := r.halted[symbol+":"+market]
	if !ok {
		return nil, fmt.Errorf("%s/%s is not halted", symbol, market)
	}
	cp := *rec
	return &cp, nil
}
