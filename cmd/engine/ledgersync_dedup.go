package main

import (
	"sync"
	"time"
)

// ledgerSyncDedup makes /internal/ledger/sync idempotent (M4): a caller that
// retries a credit/debit after a timeout (never knowing whether the first
// attempt actually landed) previously risked the engine applying the same
// balance change twice — Postgres would show the correct, single amount
// while the engine's in-memory ledger silently drifted by a duplicate
// credit or debit. Callers now send a requestId (see
// Dex-Backend's engineclient.Client.call); the second call with the same id
// is recognized and skipped instead of re-applied.
//
// In-memory, per-process, same reasoning as ratelimit.go's limiterStore:
// this deployment runs one engine instance, so a Redis-backed dedup set
// would be unnecessary complexity for no real benefit right now.
type ledgerSyncDedup struct {
	mu   sync.Mutex
	seen map[string]time.Time
}

func newLedgerSyncDedup() *ledgerSyncDedup {
	d := &ledgerSyncDedup{seen: make(map[string]time.Time)}
	go d.reapLoop()
	return d
}

// claim returns true the first time requestId is seen, false on every
// repeat — the caller should apply the ledger change only on true. If the
// change then fails (e.g. Debit rejects for insufficient balance), the
// caller must call release so a genuinely-failed operation can still be
// retried under the same id, rather than the id being permanently "used up"
// by an attempt that never actually took effect.
func (d *ledgerSyncDedup) claim(requestID string) bool {
	if requestID == "" {
		// No id supplied (an old/unpatched caller): can't dedup a call with
		// no identity, so let it through as before rather than blocking it.
		return true
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.seen[requestID]; ok {
		return false
	}
	d.seen[requestID] = time.Now()
	return true
}

// release un-claims requestId after its ledger change failed to apply.
func (d *ledgerSyncDedup) release(requestID string) {
	if requestID == "" {
		return
	}
	d.mu.Lock()
	delete(d.seen, requestID)
	d.mu.Unlock()
}

// reapLoop drops ids older than 10 minutes — comfortably longer than
// engineclient's own retry window (3 attempts, 2s apart), so a real retry
// always still finds its id, while memory doesn't grow for the process's
// entire lifetime.
func (d *ledgerSyncDedup) reapLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		cutoff := time.Now().Add(-10 * time.Minute)
		d.mu.Lock()
		for id, t := range d.seen {
			if t.Before(cutoff) {
				delete(d.seen, id)
			}
		}
		d.mu.Unlock()
	}
}
