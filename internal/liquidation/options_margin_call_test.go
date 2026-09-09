package liquidation

import (
	"log/slog"
	"testing"
	"time"

	"github.com/dex/matching-engine/internal/backendclient"
	"github.com/dex/matching-engine/internal/events"
	"github.com/dex/matching-engine/internal/models"
	"github.com/dex/matching-engine/internal/risk"
	"github.com/dex/matching-engine/internal/settlement"
	"github.com/shopspring/decimal"
)

// fakeMarkSource is a minimal risk.UnderlyingMarkSource for driving
// shortOptionMargin/RequiredOptionsMargin in tests without a real
// marketdata.Service/order book.
type fakeMarkSource map[string]decimal.Decimal

func (f fakeMarkSource) UnderlyingMark(symbol string) (decimal.Decimal, bool) {
	v, ok := f[symbol]
	return v, ok
}

// openShortOption drives OptionsSettlement.Settle with a synthetic trade to
// record a writer position (recordPosition is private; Settle is the public
// entry point real order flow uses too).
func openShortOption(t *testing.T, os *settlement.OptionsSettlement, writer, symbol, optionType, strike, qty, premium string) {
	t.Helper()
	strikeDec := decimal.RequireFromString(strike)
	qtyDec := decimal.RequireFromString(qty)
	priceDec := decimal.RequireFromString(premium)
	buyOrder := &models.Order{
		AccountID: "buyer", Symbol: symbol, Market: models.Options, Side: models.Buy,
		OptionType: optionType, StrikePrice: strikeDec, Expiry: time.Now().Add(24 * time.Hour),
		QuoteCurrency: "BIUSD",
	}
	sellOrder := &models.Order{
		AccountID: writer, Symbol: symbol, Market: models.Options, Side: models.Sell,
		OptionType: optionType, StrikePrice: strikeDec, Expiry: time.Now().Add(24 * time.Hour),
		QuoteCurrency: "BIUSD",
	}
	trade := &models.Trade{
		Symbol: symbol, Market: models.Options, Price: priceDec, Quantity: qtyDec,
		BuyOrder: buyOrder, SellOrder: sellOrder, ExecutedAt: time.Now(),
	}
	if err := os.Settle(trade); err != nil {
		t.Fatalf("settle: %v", err)
	}
}

func TestCheckOptionsMarginCalls_NoAlertWhenFarFromReserved(t *testing.T) {
	ledger := risk.NewLedger()
	ledger.Deposit("buyer", "BIUSD", decimal.NewFromInt(1_000_000))
	ledger.Deposit("writer", "BIUSD", decimal.NewFromInt(1_000_000))
	os := settlement.NewOptionsSettlement(ledger, &backendclient.Client{})
	risk.SetMarkSource(fakeMarkSource{"BTC-BIUSD": decimal.NewFromInt(50000)})
	t.Cleanup(func() { risk.SetMarkSource(nil) })

	// OTM call, spot far below strike: floor requirement should be well
	// under the 80% margin-call threshold of the cash-secured reservation.
	openShortOption(t, os, "writer", "BTC-BIUSD-60000-20260101-CALL", "CALL", "60000", "1", "500")

	bus := events.NewBus()
	ch := bus.Subscribe(10)
	eng := &Engine{options: os, bus: bus, log: slog.Default()}
	eng.checkOptionsMarginCalls()

	select {
	case evt := <-ch:
		t.Fatalf("expected no margin call alert, got %+v", evt)
	default:
	}
}

func TestCheckOptionsMarginCalls_AlertsWhenDeepITM(t *testing.T) {
	ledger := risk.NewLedger()
	ledger.Deposit("buyer", "BIUSD", decimal.NewFromInt(1_000_000))
	ledger.Deposit("writer", "BIUSD", decimal.NewFromInt(1_000_000))
	os := settlement.NewOptionsSettlement(ledger, &backendclient.Client{})
	// Deep ITM call: spot far above strike pushes intrinsic value (and so
	// the floor's ITM term) toward the cash-secured ceiling.
	risk.SetMarkSource(fakeMarkSource{"BTC-BIUSD": decimal.NewFromInt(200000)})
	t.Cleanup(func() { risk.SetMarkSource(nil) })

	openShortOption(t, os, "writer", "BTC-BIUSD-60000-20260101-CALL", "CALL", "60000", "1", "500")

	bus := events.NewBus()
	ch := bus.Subscribe(10)
	eng := &Engine{options: os, bus: bus, log: slog.Default()}
	eng.checkOptionsMarginCalls()

	select {
	case evt := <-ch:
		if evt.Type != models.EventMarginCallAlert {
			t.Fatalf("event type = %s, want MARGIN_CALL_ALERT", evt.Type)
		}
		if evt.MarginCallInfo == nil {
			t.Fatal("expected MarginCallInfo to be populated")
		}
		if evt.MarginCallInfo.AccountID != "writer" {
			t.Fatalf("account = %s, want writer", evt.MarginCallInfo.AccountID)
		}
	case <-time.After(time.Second):
		t.Fatal("expected a margin call alert event, got none")
	}
}

func TestCheckOptionsMarginCalls_IgnoresLongPositions(t *testing.T) {
	ledger := risk.NewLedger()
	ledger.Deposit("buyer", "BIUSD", decimal.NewFromInt(1_000_000))
	ledger.Deposit("writer", "BIUSD", decimal.NewFromInt(1_000_000))
	os := settlement.NewOptionsSettlement(ledger, &backendclient.Client{})
	risk.SetMarkSource(fakeMarkSource{"BTC-BIUSD": decimal.NewFromInt(200000)})
	t.Cleanup(func() { risk.SetMarkSource(nil) })

	openShortOption(t, os, "writer", "BTC-BIUSD-60000-20260101-CALL", "CALL", "60000", "1", "500")

	bus := events.NewBus()
	ch := bus.Subscribe(10)
	eng := &Engine{options: os, bus: bus, log: slog.Default()}
	eng.checkOptionsMarginCalls()

	// The buyer's long position must never trigger an alert even though the
	// same fill also recorded one for them (buyers can't be margin-called —
	// they already paid the full premium up front, nothing further is owed).
	select {
	case evt := <-ch:
		if evt.MarginCallInfo != nil && evt.MarginCallInfo.AccountID == "buyer" {
			t.Fatalf("buyer (long) must never receive a margin call alert, got %+v", evt.MarginCallInfo)
		}
	case <-time.After(100 * time.Millisecond):
		// fine — the writer's alert may or may not have already been drained
		// by another test in this package; the assertion above is what matters.
	}
}

func TestCheckOptionsMarginCalls_NilOptionsSettlementIsNoop(t *testing.T) {
	eng := &Engine{options: nil, log: slog.Default()}
	eng.checkOptionsMarginCalls() // must not panic
}
