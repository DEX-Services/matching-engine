package settlement

import (
	"testing"

	"github.com/dex/matching-engine/internal/events"
	"github.com/dex/matching-engine/internal/models"
	"github.com/dex/matching-engine/internal/risk"
	"github.com/shopspring/decimal"
)

// openLong opens a long position for accountID via a normal (non-closing)
// buy fill against a counter-seller, funding the buyer's margin first.
func openLong(t *testing.T, f *FuturesSettlement, ledger *risk.Ledger, accountID, symbol, quote string, qty, price decimal.Decimal) {
	t.Helper()
	ledger.Credit(accountID, quote, decimal.NewFromInt(1_000_000))
	ledger.Credit("counterparty", quote, decimal.NewFromInt(1_000_000))
	trade := &models.Trade{
		ID: "open1", Symbol: symbol, Market: models.Futures,
		Price: price, Quantity: qty,
		BuyOrder:  &models.Order{AccountID: accountID, Leverage: 10, MarginMode: models.MarginIsolated},
		SellOrder: &models.Order{AccountID: "counterparty", Leverage: 10, MarginMode: models.MarginIsolated},
	}
	if err := f.Settle(trade); err != nil {
		t.Fatalf("open fill settle: %v", err)
	}
}

func TestClosePortion_PublishesRealizedPnlEvent(t *testing.T) {
	ledger := risk.NewLedger()
	bus := events.NewBus()
	ch := bus.Subscribe(10)
	f := NewFuturesSettlement(ledger, nil, bus)

	symbol, quote := "BTC-USDC", "USDC"
	qty := decimal.NewFromInt(1)
	openLong(t, f, ledger, "acct1", symbol, quote, qty, decimal.NewFromInt(50000))
	// Opening a position publishes no event (only closePortion does), so
	// nothing to drain here before the closing fill below.

	// Close it at a higher price (profitable long) via an opposite (sell) fill.
	closeTrade := &models.Trade{
		ID: "close1", Symbol: symbol, Market: models.Futures,
		Price: decimal.NewFromInt(51000), Quantity: qty,
		BuyOrder:  &models.Order{AccountID: "counterparty", Leverage: 10, MarginMode: models.MarginIsolated},
		SellOrder: &models.Order{AccountID: "acct1", Leverage: 10, MarginMode: models.MarginIsolated},
	}
	if err := f.Settle(closeTrade); err != nil {
		t.Fatalf("close fill settle: %v", err)
	}

	// Settle closes both sides of the trade (the buyer/counterparty's short
	// and the seller/acct1's long), each publishing its own RealizedPnl
	// event; find acct1's among them.
	var acct1Evt *models.Event
	for i := 0; i < 2; i++ {
		evt := <-ch
		if evt.Type != models.EventRealizedPnl {
			t.Fatalf("expected EventRealizedPnl, got %s", evt.Type)
		}
		if evt.RealizedPnl != nil && evt.RealizedPnl.AccountID == "acct1" {
			acct1Evt = evt
		}
	}
	if acct1Evt == nil {
		t.Fatal("expected a RealizedPnl event for acct1")
	}
	if !acct1Evt.RealizedPnl.Pnl.IsPositive() {
		t.Fatalf("expected positive realized PnL for a profitable close, got %s", acct1Evt.RealizedPnl.Pnl)
	}
	if acct1Evt.RealizedPnl.IsLiquidation {
		t.Fatal("expected IsLiquidation=false for a voluntary close")
	}
	if !acct1Evt.RealizedPnl.ClosedQty.Equal(qty) {
		t.Fatalf("ClosedQty = %s, want %s", acct1Evt.RealizedPnl.ClosedQty, qty)
	}
}

func TestClosePortion_MarksLiquidationCloses(t *testing.T) {
	ledger := risk.NewLedger()
	bus := events.NewBus()
	ch := bus.Subscribe(10)
	f := NewFuturesSettlement(ledger, nil, bus)

	symbol, quote := "BTC-USDC", "USDC"
	qty := decimal.NewFromInt(1)
	openLong(t, f, ledger, "acct1", symbol, quote, qty, decimal.NewFromInt(50000))

	closeTrade := &models.Trade{
		ID: "liq1", Symbol: symbol, Market: models.Futures,
		Price: decimal.NewFromInt(49000), Quantity: qty,
		BuyOrder:  &models.Order{AccountID: "counterparty", Leverage: 10, MarginMode: models.MarginIsolated},
		SellOrder: &models.Order{AccountID: "acct1", Leverage: 10, MarginMode: models.MarginIsolated, InternalLiquidation: true},
	}
	if err := f.Settle(closeTrade); err != nil {
		t.Fatalf("liquidation close settle: %v", err)
	}

	// Two RealizedPnl events publish (counterparty's non-liquidation close of
	// their short, and acct1's liquidation close of their long); find acct1's.
	var acct1Evt *models.Event
	for i := 0; i < 2; i++ {
		evt := <-ch
		if evt.RealizedPnl != nil && evt.RealizedPnl.AccountID == "acct1" {
			acct1Evt = evt
		}
	}
	if acct1Evt == nil || !acct1Evt.RealizedPnl.IsLiquidation {
		t.Fatalf("expected IsLiquidation=true for acct1's forced close, got %#v", acct1Evt)
	}
}

func TestClosePortion_NoBusConfigured_DoesNotPanic(t *testing.T) {
	ledger := risk.NewLedger()
	f := NewFuturesSettlement(ledger, nil, nil) // nil bus
	symbol, quote := "BTC-USDC", "USDC"
	qty := decimal.NewFromInt(1)
	openLong(t, f, ledger, "acct1", symbol, quote, qty, decimal.NewFromInt(50000))

	closeTrade := &models.Trade{
		ID: "close1", Symbol: symbol, Market: models.Futures,
		Price: decimal.NewFromInt(51000), Quantity: qty,
		BuyOrder:  &models.Order{AccountID: "counterparty", Leverage: 10, MarginMode: models.MarginIsolated},
		SellOrder: &models.Order{AccountID: "acct1", Leverage: 10, MarginMode: models.MarginIsolated},
	}
	if err := f.Settle(closeTrade); err != nil {
		t.Fatalf("close fill settle with nil bus: %v", err)
	}
}
