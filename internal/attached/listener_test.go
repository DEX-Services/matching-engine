package attached

import (
	"testing"

	"github.com/dex/matching-engine/internal/models"
	"github.com/shopspring/decimal"
)

type fakeCanceller struct{ cancelled []string }

func (f *fakeCanceller) Cancel(symbol string, market models.MarketType, orderID string) (*models.Order, error) {
	f.cancelled = append(f.cancelled, orderID)
	return &models.Order{ID: orderID, Status: models.StatusCancelled}, nil
}

type fakeSubmitter struct{ submitted []*models.Order }

func (f *fakeSubmitter) SubmitSnapshot(o *models.Order) ([]*models.Trade, *models.Order, error) {
	f.submitted = append(f.submitted, o)
	return nil, o, nil
}

type fakePositionSizer struct{ size decimal.Decimal }

func (f *fakePositionSizer) CurrentSize(accountID, symbol string) decimal.Decimal { return f.size }

func TestListenerOCOCancelsSiblingOnFill(t *testing.T) {
	reg := NewRegistry()
	g := Group{ID: "g", AccountID: "acct", Symbol: "BTC-USDC", ParentOrderID: "p", TakeProfit: &Leg{ID: "tp"}, StopLoss: &Leg{ID: "sl"}}
	if err := reg.Activate(g, decimal.NewFromInt(1)); err != nil {
		t.Fatal(err)
	}
	cancel := &fakeCanceller{}
	submit := &fakeSubmitter{}
	pos := &fakePositionSizer{}
	l := NewListener(reg, cancel, submit, pos)

	// TP leg fills.
	l.handle(&models.Event{
		Type: models.EventOrderFilled,
		Order: &models.Order{ID: "tp", GroupID: "g", GroupRole: "TP", AccountID: "acct", Symbol: "BTC-USDC"},
	})

	if len(cancel.cancelled) != 1 || cancel.cancelled[0] != "sl" {
		t.Fatalf("expected SL leg cancelled via OCO, got %#v", cancel.cancelled)
	}
	if _, _, err := reg.Trigger("g", "sl"); err == nil {
		t.Fatal("group should already be marked triggered")
	}
}

func TestListenerResizesOnExternalFill(t *testing.T) {
	reg := NewRegistry()
	g := Group{ID: "g", AccountID: "acct", Symbol: "BTC-USDC", ParentOrderID: "p", TakeProfit: &Leg{ID: "tp"}, StopLoss: &Leg{ID: "sl"}}
	if err := reg.Activate(g, decimal.NewFromInt(5)); err != nil {
		t.Fatal(err)
	}
	cancel := &fakeCanceller{}
	submit := &fakeSubmitter{}
	pos := &fakePositionSizer{size: decimal.NewFromInt(2)} // position partially closed down to 2
	l := NewListener(reg, cancel, submit, pos)

	// A partial close fill on the account/symbol, unrelated to the group's own legs.
	l.handle(&models.Event{
		Type:  models.EventOrderFilled,
		Order: &models.Order{ID: "close1", AccountID: "acct", Symbol: "BTC-USDC"},
	})

	if len(cancel.cancelled) != 2 {
		t.Fatalf("expected both legs cancelled for resize-and-replace, got %#v", cancel.cancelled)
	}
	if len(submit.submitted) != 2 {
		t.Fatalf("expected both legs resubmitted at reduced qty, got %d", len(submit.submitted))
	}
	for _, o := range submit.submitted {
		if !o.Quantity.Equal(decimal.NewFromInt(2)) {
			t.Fatalf("expected resized leg qty=2, got %s", o.Quantity)
		}
	}
	got, ok := reg.Get("g")
	if !ok || !got.ProtectedQty.Equal(decimal.NewFromInt(2)) {
		t.Fatalf("expected registry to reflect resized protection, got %#v", got)
	}
}

func TestListenerRemovesGroupOnZeroExposure(t *testing.T) {
	reg := NewRegistry()
	g := Group{ID: "g", AccountID: "acct", Symbol: "BTC-USDC", ParentOrderID: "p", StopLoss: &Leg{ID: "sl"}}
	if err := reg.Activate(g, decimal.NewFromInt(3)); err != nil {
		t.Fatal(err)
	}
	cancel := &fakeCanceller{}
	submit := &fakeSubmitter{}
	pos := &fakePositionSizer{size: decimal.Zero} // position fully closed / liquidated
	l := NewListener(reg, cancel, submit, pos)

	l.handle(&models.Event{Type: models.EventLiquidation, Liquidation: &models.Liquidation{AccountID: "acct", Symbol: "BTC-USDC"}})

	if len(cancel.cancelled) != 1 || cancel.cancelled[0] != "sl" {
		t.Fatalf("expected orphaned SL leg cancelled, got %#v", cancel.cancelled)
	}
	if _, ok := reg.Get("g"); ok {
		t.Fatal("expected group removed after exposure hit zero")
	}
}

func TestGroupsForAndRelinkLeg(t *testing.T) {
	reg := NewRegistry()
	g := Group{ID: "g", AccountID: "acct", Symbol: "BTC-USDC", ParentOrderID: "p", TakeProfit: &Leg{ID: "tp"}}
	if err := reg.Activate(g, decimal.NewFromInt(1)); err != nil {
		t.Fatal(err)
	}
	found := reg.GroupsFor("acct", "BTC-USDC")
	if len(found) != 1 || found[0].ID != "g" {
		t.Fatalf("expected to find group, got %#v", found)
	}
	if len(reg.GroupsFor("other", "BTC-USDC")) != 0 {
		t.Fatal("expected no groups for a different account")
	}
	reg.RelinkLeg("g", "TP", "tp-new")
	got, _ := reg.Get("g")
	if got.TakeProfit.ID != "tp-new" {
		t.Fatalf("expected relinked leg ID, got %q", got.TakeProfit.ID)
	}
}
