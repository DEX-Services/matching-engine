package attached

import (
	"fmt"
	"testing"

	"github.com/dex/matching-engine/internal/fixedpoint"
	"github.com/dex/matching-engine/internal/models"
)

func TestExecuteActivatesOnlyActualFill(t *testing.T) {
	r := NewRegistry()
	e := &models.Order{ID: "p", AccountID: "a", Symbol: "BTC-USDC"}
	g := Group{ID: "g", ParentOrderID: "p", StopLoss: &Leg{ID: "sl"}}
	submit := func(o *models.Order) (*models.Order, error) { o.Filled = fixedpoint.FromInt64(2); return o, nil }
	var placedLegs []*models.Order
	submitLeg := func(o *models.Order) error { placedLegs = append(placedLegs, o); return nil }
	out, got, err := Execute(r, Command{g, e}, submit, submitLeg, nil, nil)
	if err != nil || out == nil || got == nil || !got.ProtectedQty.Equal(fixedpoint.FromInt64(2)) {
		t.Fatalf("got %#v, %v", got, err)
	}
	if len(placedLegs) != 1 || placedLegs[0].GroupRole != "SL" {
		t.Fatalf("expected exactly one placed SL leg, got %#v", placedLegs)
	}
}

func TestExecuteSkipsLegsOnZeroFill(t *testing.T) {
	r := NewRegistry()
	e := &models.Order{ID: "p", AccountID: "a", Symbol: "BTC-USDC"}
	g := Group{ID: "g", ParentOrderID: "p", StopLoss: &Leg{ID: "sl"}}
	submit := func(o *models.Order) (*models.Order, error) { return o, nil } // stays unfilled
	legCalled := false
	submitLeg := func(o *models.Order) error { legCalled = true; return nil }
	out, got, err := Execute(r, Command{g, e}, submit, submitLeg, nil, nil)
	if err != nil || out == nil || got != nil {
		t.Fatalf("expected no group activated on zero fill, got %#v, %v", got, err)
	}
	if legCalled {
		t.Fatal("expected no leg to be submitted for an unfilled entry")
	}
}

// TestExecute_SpotCallsReserveGroupOnceBeforeLegs is a regression test for
// the 2026-09-16 SPOT TP/SL support: a SPOT group must call reserveGroup
// exactly once, BEFORE either leg is submitted — not once per leg. This is
// the core of how spot avoids double-locking the same base-asset holding
// for a TP+SL pair where only one leg can ever actually execute (see
// ReserveGroup's doc comment).
func TestExecute_SpotCallsReserveGroupOnceBeforeLegs(t *testing.T) {
	r := NewRegistry()
	e := &models.Order{ID: "p", AccountID: "a", Symbol: "BI2X-BI2XUSD", Side: models.Buy}
	g := Group{ID: "g", ParentOrderID: "p", Market: models.Spot, TakeProfit: &Leg{ID: "tp"}, StopLoss: &Leg{ID: "sl"}}
	submit := func(o *models.Order) (*models.Order, error) { o.Filled = fixedpoint.FromInt64(5); return o, nil }
	var placedLegs []*models.Order
	submitLeg := func(o *models.Order) error { placedLegs = append(placedLegs, o); return nil }

	reserveCalls := 0
	var reservedAt int // index into placedLegs at the moment reserveGroup was called (must be 0: before any leg)
	reserveGroup := func(rg Group) error {
		reserveCalls++
		reservedAt = len(placedLegs)
		if rg.Market != models.Spot {
			t.Fatalf("reserveGroup called with Market=%s, want Spot", rg.Market)
		}
		if !rg.ProtectedQty.Equal(fixedpoint.FromInt64(5)) {
			t.Fatalf("reserveGroup called with ProtectedQty=%s, want 5 (the actual fill)", rg.ProtectedQty)
		}
		return nil
	}

	_, got, err := Execute(r, Command{g, e}, submit, submitLeg, reserveGroup, nil)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if reserveCalls != 1 {
		t.Fatalf("reserveGroup called %d times, want exactly 1 (once for the whole pair, not once per leg)", reserveCalls)
	}
	if reservedAt != 0 {
		t.Fatalf("reserveGroup was called after %d leg(s) had already been placed, want it called BEFORE any leg", reservedAt)
	}
	if len(placedLegs) != 2 {
		t.Fatalf("expected both TP and SL legs to be placed once reservation succeeded, got %d", len(placedLegs))
	}
	if got.Market != models.Spot {
		t.Fatalf("activated group Market = %s, want Spot", got.Market)
	}
}

// TestExecute_SpotSkipsLegsWhenReserveGroupFails confirms a failed shared
// reservation leaves NO legs placed at all — never a partial pair (e.g. only
// the TP leg placed with no SL protecting the other side), since that would
// silently leave the user with less protection than they asked for and no
// visible error explaining why.
func TestExecute_SpotSkipsLegsWhenReserveGroupFails(t *testing.T) {
	r := NewRegistry()
	e := &models.Order{ID: "p", AccountID: "a", Symbol: "BI2X-BI2XUSD", Side: models.Buy}
	g := Group{ID: "g", ParentOrderID: "p", Market: models.Spot, TakeProfit: &Leg{ID: "tp"}, StopLoss: &Leg{ID: "sl"}}
	submit := func(o *models.Order) (*models.Order, error) { o.Filled = fixedpoint.FromInt64(5); return o, nil }
	legCalled := false
	submitLeg := func(o *models.Order) error { legCalled = true; return nil }
	reserveGroup := func(rg Group) error { return fmt.Errorf("insufficient balance") }

	_, _, err := Execute(r, Command{g, e}, submit, submitLeg, reserveGroup, nil)
	if err != nil {
		t.Fatalf("execute itself should not error (the entry already filled and must stand): %v", err)
	}
	if legCalled {
		t.Fatal("expected no leg to be placed when the shared reservation fails")
	}
}

// TestExecute_FuturesNeverCallsReserveGroup confirms the SPOT-only
// reservation step is never invoked for a FUTURES group, preserving
// futures' existing "no shared/extra reservation at all" behavior exactly.
func TestExecute_FuturesNeverCallsReserveGroup(t *testing.T) {
	r := NewRegistry()
	e := &models.Order{ID: "p", AccountID: "a", Symbol: "BI2X-BI2XUSD", Side: models.Buy}
	g := Group{ID: "g", ParentOrderID: "p", Market: models.Futures, StopLoss: &Leg{ID: "sl"}}
	submit := func(o *models.Order) (*models.Order, error) { o.Filled = fixedpoint.FromInt64(5); return o, nil }
	submitLeg := func(o *models.Order) error { return nil }
	reserveCalled := false
	reserveGroup := func(rg Group) error { reserveCalled = true; return nil }

	if _, _, err := Execute(r, Command{g, e}, submit, submitLeg, reserveGroup, nil); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if reserveCalled {
		t.Fatal("reserveGroup should never be called for a FUTURES group")
	}
}

// TestExecute_SpotReleasesReservationWhenAllLegsFailToPlace is a regression
// test for a live incident: a SPOT group's shared reservation succeeded via
// reserveGroup, but BOTH legs then failed to place (e.g. the symbol was
// halted at that exact moment) — the reservation stayed locked forever,
// since no leg ever existed anywhere for the user to cancel, and the
// registry kept the group as "active" describing legs that were never
// actually placed. Confirms: releaseGroup is called exactly once with the
// group's ProtectedQty, and the group is removed from the registry
// entirely (Get returns false) rather than lingering as a phantom entry.
func TestExecute_SpotReleasesReservationWhenAllLegsFailToPlace(t *testing.T) {
	r := NewRegistry()
	e := &models.Order{ID: "p", AccountID: "a", Symbol: "BI2X-BI2XUSD", Side: models.Buy}
	g := Group{ID: "g", ParentOrderID: "p", Market: models.Spot, TakeProfit: &Leg{ID: "tp"}, StopLoss: &Leg{ID: "sl"}}
	submit := func(o *models.Order) (*models.Order, error) { o.Filled = fixedpoint.FromInt64(5); return o, nil }
	submitLeg := func(o *models.Order) error { return fmt.Errorf("symbol halted") } // BOTH legs fail
	reserveGroup := func(rg Group) error { return nil }                             // reservation itself succeeds

	releaseCalls := 0
	var releasedQty fixedpoint.Fixed
	releaseGroup := func(rg Group) error {
		releaseCalls++
		releasedQty = rg.ProtectedQty
		return nil
	}

	_, got, err := Execute(r, Command{g, e}, submit, submitLeg, reserveGroup, releaseGroup)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if releaseCalls != 1 {
		t.Fatalf("releaseGroup called %d times, want exactly 1", releaseCalls)
	}
	if !releasedQty.Equal(fixedpoint.FromInt64(5)) {
		t.Fatalf("releaseGroup called with ProtectedQty=%s, want 5 (the actual fill, matching what reserveGroup locked)", releasedQty)
	}
	if got != nil {
		t.Fatalf("Execute returned a non-nil group after total leg failure, want nil (nothing left active): %#v", got)
	}
	if _, ok := r.Get("g"); ok {
		t.Fatal("group should be removed from the registry entirely after all legs failed to place, not left as a phantom entry")
	}
}

// TestExecute_SpotDoesNotReleaseWhenAtLeastOneLegPlaced confirms the
// cleanup is scoped precisely to TOTAL failure — a partial placement (one
// leg up, one down, the existing documented best-effort behavior) must NOT
// release the shared reservation or remove the group: the resting leg still
// protects the position and its own eventual cancel/fill releases the
// reservation normally.
func TestExecute_SpotDoesNotReleaseWhenAtLeastOneLegPlaced(t *testing.T) {
	r := NewRegistry()
	e := &models.Order{ID: "p", AccountID: "a", Symbol: "BI2X-BI2XUSD", Side: models.Buy}
	g := Group{ID: "g", ParentOrderID: "p", Market: models.Spot, TakeProfit: &Leg{ID: "tp"}, StopLoss: &Leg{ID: "sl"}}
	submit := func(o *models.Order) (*models.Order, error) { o.Filled = fixedpoint.FromInt64(5); return o, nil }
	submitLeg := func(o *models.Order) error {
		if o.GroupRole == "SL" {
			return fmt.Errorf("symbol halted") // only SL fails; TP succeeds
		}
		return nil
	}
	reserveGroup := func(rg Group) error { return nil }
	releaseCalled := false
	releaseGroup := func(rg Group) error { releaseCalled = true; return nil }

	_, got, err := Execute(r, Command{g, e}, submit, submitLeg, reserveGroup, releaseGroup)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if releaseCalled {
		t.Fatal("releaseGroup must not be called when at least one leg was placed successfully")
	}
	if got == nil {
		t.Fatal("group should remain active (the surviving TP leg still protects the position)")
	}
	if _, ok := r.Get("g"); !ok {
		t.Fatal("group should still be present in the registry after a partial (not total) leg-placement failure")
	}
}
