package main

import (
	"context"
	"testing"
	"time"

	"github.com/dex/matching-engine/internal/backendclient"
	"github.com/dex/matching-engine/internal/config"
	"github.com/dex/matching-engine/internal/events"
	"github.com/dex/matching-engine/internal/matching"
	"github.com/dex/matching-engine/internal/models"
	"github.com/dex/matching-engine/internal/risk"
	"github.com/dex/matching-engine/internal/settlement"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

// These cover the fix for the previously-documented gap: an order rejected
// before it ever reaches reg.SubmitSnapshot (invalid slippageBps,
// reduce-only violation, config/risk check, reservation/lock failure)
// carries a real ID but never published an event or reached order history.
// rejectPipeline + submitDeps.bus now publish EventOrderRejected for every
// such path, exactly like the matching engine itself does for in-book
// rejections.

func newTestSubmitDeps(bus *events.Bus) submitDeps {
	ledger := risk.NewLedger()
	checker := risk.NewChecker(ledger)
	reg := matching.NewRegistry(bus, nil, checker.Release)
	futuresSettlement := settlement.NewFuturesSettlement(ledger, nil, bus)
	return submitDeps{
		reg: reg, ledger: ledger, backend: &backendclient.Client{}, checker: checker,
		symbolRegistry: config.NewInMemoryRegistry(), futuresSettlement: futuresSettlement,
		pgPool: nil, mdSvc: nil, bus: bus,
	}
}

func TestRejectPipeline_PublishesEventOrderRejected(t *testing.T) {
	bus := events.NewBus()
	ch := bus.Subscribe(10)
	d := newTestSubmitDeps(bus)

	o := &models.Order{
		ID: uuid.NewString(), AccountID: "acct1", Symbol: "BTC-USDC", Market: models.Futures,
		Side: models.Sell, Type: models.Market, Quantity: decimal.NewFromInt(1),
		ReduceOnly: true, TimeInForce: models.GTC, Status: models.StatusPending, CreatedAt: time.Now(),
	}

	// No open position -> checkReduceOnly rejects before the order ever
	// reaches reg.SubmitSnapshot.
	_, _, status, err := submitOrderPipeline(context.Background(), d, o, "")
	if err == nil {
		t.Fatal("expected a reduce-only rejection")
	}
	if status != 400 {
		t.Fatalf("status = %d, want 400", status)
	}
	if o.Status != models.StatusRejected {
		t.Fatalf("order.Status = %s, want REJECTED", o.Status)
	}
	if o.RejectReason == "" {
		t.Fatal("expected a non-empty RejectReason on the order itself")
	}

	select {
	case evt := <-ch:
		if evt.Type != models.EventOrderRejected {
			t.Fatalf("event type = %s, want ORDER_REJECTED", evt.Type)
		}
		if evt.Order == nil || evt.Order.ID != o.ID {
			t.Fatalf("expected the published event to carry the rejected order, got %#v", evt.Order)
		}
		if evt.Order.RejectReason == "" {
			t.Fatal("expected the published order to carry a RejectReason")
		}
		if evt.SequenceNumber == 0 {
			t.Fatal("expected a non-zero out-of-band sequence number (avoids colliding with a same-symbol book event in the events table's UNIQUE(symbol, sequence_number) index)")
		}
	default:
		t.Fatal("expected an EventOrderRejected to be published for a pre-book rejection")
	}
}

func TestRejectPipeline_InvalidSlippageBps_PublishesEvent(t *testing.T) {
	bus := events.NewBus()
	ch := bus.Subscribe(10)
	d := newTestSubmitDeps(bus)

	o := &models.Order{
		ID: uuid.NewString(), AccountID: "acct1", Symbol: "BTC-USDT", Market: models.Spot,
		Side: models.Buy, Type: models.Market, Quantity: decimal.NewFromInt(1),
		TimeInForce: models.GTC, Status: models.StatusPending, CreatedAt: time.Now(),
	}

	_, _, status, err := submitOrderPipeline(context.Background(), d, o, "not-a-number")
	if err == nil {
		t.Fatal("expected an invalid slippageBps rejection")
	}
	if status != 400 {
		t.Fatalf("status = %d, want 400", status)
	}

	select {
	case evt := <-ch:
		if evt.Type != models.EventOrderRejected || evt.Order.RejectReason != "invalid slippageBps" {
			t.Fatalf("unexpected event: %#v", evt)
		}
	default:
		t.Fatal("expected an EventOrderRejected to be published")
	}
}

func TestRejectPipeline_NilBus_DoesNotPanic(t *testing.T) {
	d := newTestSubmitDeps(nil)
	d.bus = nil // explicit: no bus configured
	o := &models.Order{
		ID: uuid.NewString(), AccountID: "acct1", Symbol: "BTC-USDC", Market: models.Futures,
		Side: models.Sell, Type: models.Market, Quantity: decimal.NewFromInt(1),
		ReduceOnly: true, TimeInForce: models.GTC, Status: models.StatusPending, CreatedAt: time.Now(),
	}
	if _, _, _, err := submitOrderPipeline(context.Background(), d, o, ""); err == nil {
		t.Fatal("expected rejection")
	}
	if o.RejectReason == "" {
		t.Fatal("expected RejectReason set on the order even with no bus configured")
	}
}
