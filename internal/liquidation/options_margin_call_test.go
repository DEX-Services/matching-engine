package liquidation

import (
	"log/slog"
	"testing"
	"time"

	"github.com/dex/matching-engine/internal/backendclient"
	"github.com/dex/matching-engine/internal/events"
	"github.com/dex/matching-engine/internal/marketdata"
	"github.com/dex/matching-engine/internal/matching"
	"github.com/dex/matching-engine/internal/models"
	"github.com/dex/matching-engine/internal/orderbook"
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

// fakeBook is a minimal marketdata.BookReader with a fixed best bid/ask, so
// a real marketdata.Service can be driven to a known Ticker() output without
// a live matching.Engine.
type fakeBook struct{ bid, ask decimal.Decimal }

func (f fakeBook) BestBid() decimal.Decimal                         { return f.bid }
func (f fakeBook) BestAsk() decimal.Decimal                         { return f.ask }
func (f fakeBook) Depth(int) (bids, asks []orderbook.LevelSnapshot) { return nil, nil }

// mdWithUnderlyingSpot builds a real marketdata.Service with a registered
// SPOT book for underlying at the given price (as both best bid and ask, so
// MidPrice/MarkPrice resolve to exactly that price) and NO registered book
// for any option instrument — driving checkOptionsMarginCalls's optionMark
// down its theoretical-Black-Scholes fallback path, which is what a
// never-quoted option correctly falls back to.
func mdWithUnderlyingSpot(underlying string, spot decimal.Decimal) *marketdata.Service {
	md := marketdata.NewService()
	md.Register(underlying, models.Spot, fakeBook{bid: spot, ask: spot})
	return md
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

func newTestOptionsEngine(os *settlement.OptionsSettlement, md *marketdata.Service, bus *events.Bus) *Engine {
	reg := matching.NewRegistry(bus, nil, nil)
	return &Engine{options: os, marketdata: md, registry: reg, bus: bus, log: slog.Default()}
}

func TestCheckOptionsMarginCalls_NoForceCloseWhenFarFromReserved(t *testing.T) {
	ledger := risk.NewLedger()
	ledger.Deposit("buyer", "BIUSD", decimal.NewFromInt(1_000_000))
	ledger.Deposit("writer", "BIUSD", decimal.NewFromInt(1_000_000))
	os := settlement.NewOptionsSettlement(ledger, &backendclient.Client{})
	risk.SetMarkSource(fakeMarkSource{"BTC-BIUSD": decimal.NewFromInt(50000)})
	t.Cleanup(func() { risk.SetMarkSource(nil) })

	// OTM call, spot far below strike: the writer's equity (reserved +
	// unrealized gain, since an OTM short call is profitable for the
	// writer) stays comfortably above the maintenance requirement.
	openShortOption(t, os, "writer", "BTC-BIUSD-60000-20260101-CALL", "CALL", "60000", "1", "500")

	md := mdWithUnderlyingSpot("BTC-BIUSD", decimal.NewFromInt(50000))
	bus := events.NewBus()
	ch := bus.Subscribe(10)
	eng := newTestOptionsEngine(os, md, bus)
	eng.checkOptionsMarginCalls()

	select {
	case evt := <-ch:
		t.Fatalf("expected no liquidation/alert event, got %+v", evt)
	default:
	}

	pos := os.GetPosition("writer", "BTC-BIUSD-60000-20260101-CALL", decimal.NewFromInt(60000), time.Now().Add(24*time.Hour), "CALL")
	if pos == nil || pos.Size.IsZero() {
		t.Fatal("expected the writer's position to still be open (not liquidated)")
	}
}

func TestCheckOptionsMarginCalls_ForceClosesWhenDeepITM(t *testing.T) {
	ledger := risk.NewLedger()
	ledger.Deposit("buyer", "BIUSD", decimal.NewFromInt(1_000_000))
	ledger.Deposit("writer", "BIUSD", decimal.NewFromInt(1_000_000))
	os := settlement.NewOptionsSettlement(ledger, &backendclient.Client{})
	// Deep ITM call: spot far above strike makes the short call a large
	// unrealized loss for the writer, eroding equity well below maintenance.
	risk.SetMarkSource(fakeMarkSource{"BTC-BIUSD": decimal.NewFromInt(200000)})
	t.Cleanup(func() { risk.SetMarkSource(nil) })

	openShortOption(t, os, "writer", "BTC-BIUSD-60000-20260101-CALL", "CALL", "60000", "1", "500")

	md := mdWithUnderlyingSpot("BTC-BIUSD", decimal.NewFromInt(200000))
	bus := events.NewBus()
	ch := bus.Subscribe(10)
	eng := newTestOptionsEngine(os, md, bus)
	eng.checkOptionsMarginCalls()

	// Drain events looking for the liquidation event (a margin-call alert
	// may also be published first, at the higher warning threshold).
	var sawLiquidation bool
	deadline := time.After(time.Second)
	for !sawLiquidation {
		select {
		case evt := <-ch:
			if evt.Type == models.EventLiquidation {
				sawLiquidation = true
				if evt.Liquidation.AccountID != "writer" {
					t.Fatalf("liquidated account = %s, want writer", evt.Liquidation.AccountID)
				}
			}
		case <-deadline:
			t.Fatal("expected an EventLiquidation for the deep-ITM short position, got none")
		}
	}

	pos := os.GetPosition("writer", "BTC-BIUSD-60000-20260101-CALL", decimal.NewFromInt(60000), time.Now().Add(24*time.Hour), "CALL")
	if pos != nil && !pos.Size.IsZero() {
		t.Fatalf("expected the writer's position to be force-closed, still open: %+v", pos)
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

	md := mdWithUnderlyingSpot("BTC-BIUSD", decimal.NewFromInt(200000))
	bus := events.NewBus()
	eng := newTestOptionsEngine(os, md, bus)
	eng.checkOptionsMarginCalls()

	// The buyer's long position must never be force-closed — buyers can't be
	// margin-called; they already paid the full premium up front.
	buyerPos := os.GetPosition("buyer", "BTC-BIUSD-60000-20260101-CALL", decimal.NewFromInt(60000), time.Now().Add(24*time.Hour), "CALL")
	if buyerPos == nil || buyerPos.Size.IsZero() {
		t.Fatal("expected the buyer's long position to remain untouched")
	}
}

func TestCheckOptionsMarginCalls_NilOptionsSettlementIsNoop(t *testing.T) {
	eng := &Engine{options: nil, log: slog.Default()}
	eng.checkOptionsMarginCalls() // must not panic
}
