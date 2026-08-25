package attached

import (
	"github.com/shopspring/decimal"
	"testing"
)

func TestActivateUsesActualFillAndResizeNeverExceedsExposure(t *testing.T) {
	r := NewRegistry()
	g := Group{ID: "g", ParentOrderID: "p", TakeProfit: &Leg{ID: "tp"}}
	if err := r.Activate(g, decimal.Zero); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.Get("g"); ok {
		t.Fatal("zero fill must not activate exits")
	}
	if err := r.Activate(g, decimal.NewFromInt(5)); err != nil {
		t.Fatal(err)
	}
	got, ok := r.Resize("g", decimal.NewFromInt(2))
	if !ok || !got.ProtectedQty.Equal(decimal.NewFromInt(2)) {
		t.Fatalf("got %#v", got)
	}
	if _, ok := r.Resize("g", decimal.Zero); ok {
		t.Fatal("closed exposure must remove exits")
	}
}
