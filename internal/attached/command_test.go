package attached

import (
	"github.com/dex/matching-engine/internal/models"
	"github.com/shopspring/decimal"
	"testing"
)

func TestExecuteActivatesOnlyActualFill(t *testing.T) {
	r := NewRegistry()
	e := &models.Order{ID: "p", AccountID: "a", Symbol: "BTC-USDC"}
	g := Group{ID: "g", ParentOrderID: "p", StopLoss: &Leg{ID: "sl"}}
	submit := func(o *models.Order) (*models.Order, error) { o.Filled = decimal.NewFromInt(2); return o, nil }
	var placedLegs []*models.Order
	submitLeg := func(o *models.Order) error { placedLegs = append(placedLegs, o); return nil }
	out, got, err := Execute(r, Command{g, e}, submit, submitLeg)
	if err != nil || out == nil || got == nil || !got.ProtectedQty.Equal(decimal.NewFromInt(2)) {
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
	out, got, err := Execute(r, Command{g, e}, submit, submitLeg)
	if err != nil || out == nil || got != nil {
		t.Fatalf("expected no group activated on zero fill, got %#v, %v", got, err)
	}
	if legCalled {
		t.Fatal("expected no leg to be submitted for an unfilled entry")
	}
}
