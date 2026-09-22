package attached

import (
	"github.com/dex/matching-engine/internal/fixedpoint"
	"testing"
)

func TestActivateUsesActualFillAndResizeNeverExceedsExposure(t *testing.T) {
	r := NewRegistry()
	g := Group{ID: "g", ParentOrderID: "p", TakeProfit: &Leg{ID: "tp"}}
	if err := r.Activate(g, fixedpoint.Zero); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.Get("g"); ok {
		t.Fatal("zero fill must not activate exits")
	}
	if err := r.Activate(g, fixedpoint.FromInt64(5)); err != nil {
		t.Fatal(err)
	}
	got, ok := r.Resize("g", fixedpoint.FromInt64(2))
	if !ok || !got.ProtectedQty.Equal(fixedpoint.FromInt64(2)) {
		t.Fatalf("got %#v", got)
	}
	if _, ok := r.Resize("g", fixedpoint.Zero); ok {
		t.Fatal("closed exposure must remove exits")
	}
}

func TestTriggerEnforcesOCO(t *testing.T) {
	r := NewRegistry()
	g := Group{ID: "g", ParentOrderID: "p", TakeProfit: &Leg{ID: "tp"}, StopLoss: &Leg{ID: "sl"}}
	if err := r.Activate(g, fixedpoint.FromInt64(1)); err != nil {
		t.Fatal(err)
	}
	got, peer, err := r.Trigger("g", "tp")
	if err != nil || peer != "sl" || got.StopLoss.Active {
		t.Fatalf("oco result=%#v peer=%q err=%v", got, peer, err)
	}
	if _, _, err := r.Trigger("g", "sl"); err == nil {
		t.Fatal("second leg must be rejected")
	}
}
