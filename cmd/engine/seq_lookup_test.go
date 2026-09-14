package main

import (
	"context"
	"testing"
)

// TestNewEngineSeqLookupNilPool covers the "Postgres disabled" case (local
// dev without POSTGRES_HOST, or any other reason pgPool ended up nil) — see
// newEngineSeqLookup's doc comment. Every symbol must report 0 rather than
// panic on a nil pool: there is no persisted history to resume from, so
// starting at 0 is correct and cannot collide with anything.
//
// The pool != nil path (an actual MAX(sequence_number) query) needs a real
// Postgres instance and isn't covered here — see
// internal/matching/engine_test.go's TestNewEngineResumesFromStartSeq for
// the regression coverage on the consuming side (an engine constructed with
// a given startSeq resumes from startSeq+1, not from 1), which is the part
// that actually prevents the history-loss bug once a real seq value is
// supplied.
func TestNewEngineSeqLookupNilPool(t *testing.T) {
	lookup := newEngineSeqLookup(context.Background(), nil)
	if lookup == nil {
		t.Fatal("newEngineSeqLookup returned nil")
	}
	for _, symbol := range []string{"BI2X-BI2XUSD", "BTC-BI2XUSD", "does-not-exist"} {
		if got := lookup(symbol); got != 0 {
			t.Errorf("lookup(%q) with nil pool = %d, want 0", symbol, got)
		}
	}
}
