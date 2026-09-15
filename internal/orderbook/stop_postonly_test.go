package orderbook

import (
	"testing"
	"time"

	"github.com/dex/matching-engine/internal/models"
	"github.com/shopspring/decimal"
)

// These tests exercise the STOP and POST_ONLY code paths inside the book
// directly. The correctness bug found in the audit was not here — this
// logic has always worked — it was that the HTTP /order handler in
// cmd/engine/main.go never let a client reach these paths (no "STOP" case
// in its type switch, and no stopPrice query param at all). These tests
// pin down the underlying behaviour so the API-layer fix has something
// correct to expose.

func mkOrder(id string, side models.OrderSide, typ models.OrderType, price, qty string) *models.Order {
	return &models.Order{
		ID: id, AccountID: "acct-" + id, Symbol: "BTC-USDT", Market: models.Spot,
		Side: side, Type: typ, TimeInForce: models.GTC,
		Price: decimal.RequireFromString(price), Quantity: decimal.RequireFromString(qty),
		Status: models.StatusPending, CreatedAt: time.Now(),
	}
}

func TestStopMarket_RestsUntriggered_ThenFiresOnLastTradePrice(t *testing.T) {
	b := New("BTC-USDT", models.Spot)

	stop := mkOrder("stop1", models.Buy, models.Stop, "0", "1")
	stop.StopPrice = decimal.RequireFromString("100")
	if _, _, err := b.Submit(stop); err != nil {
		t.Fatalf("stop order submission failed: %v", err)
	}
	if stop.Status != models.StatusOpen {
		t.Fatalf("stop order status = %s, want OPEN (resting untriggered)", stop.Status)
	}
	// Not yet in the matchable book or its own resting order.
	if _, ok := b.OrderByID("stop1"); ok {
		t.Fatal("untriggered stop should not appear in the live order index")
	}

	// A trade at 99 (below trigger) must not fire it.
	sell99 := mkOrder("s99", models.Sell, models.Limit, "99", "1")
	buy99 := mkOrder("b99", models.Buy, models.Limit, "99", "1")
	if _, _, err := b.Submit(sell99); err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.Submit(buy99); err != nil {
		t.Fatal(err)
	}
	if stop.Status != models.StatusOpen {
		t.Fatalf("stop should still be untriggered after a 99 trade, status=%s", stop.Status)
	}

	// A trade AT 100 (at/above trigger) must fire it, converting it to a
	// market order and matching it against the book immediately. Rest a
	// second ask at 101 first — this is what the triggered stop-buy will
	// match against once activated — then trade the market up to 100 via a
	// separate bid/ask pair so lastTradePrice actually moves to 100.
	restingAsk := mkOrder("ask101", models.Sell, models.Limit, "101", "1")
	if _, _, err := b.Submit(restingAsk); err != nil {
		t.Fatal(err)
	}
	buy100 := mkOrder("b100", models.Buy, models.Limit, "100", "1")
	if _, _, err := b.Submit(buy100); err != nil {
		t.Fatal(err)
	}
	sell100 := mkOrder("s100", models.Sell, models.Limit, "100", "1")
	trades, _, err := b.Submit(sell100)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, tr := range trades {
		if tr.TakerOrderID == "stop1" || tr.MakerOrderID == "stop1" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected the triggered stop order to trade after the incoming sell hit 100, trades=%+v", trades)
	}
	if stop.Type != models.Market {
		t.Fatalf("triggered stop-market order should have Type=MARKET after activation, got %s", stop.Type)
	}
}

func TestPostOnly_RejectsWhenCrossing(t *testing.T) {
	b := New("BTC-USDT", models.Spot)

	// Resting ask at 100.
	ask := mkOrder("ask1", models.Sell, models.Limit, "100", "1")
	if _, _, err := b.Submit(ask); err != nil {
		t.Fatal(err)
	}

	// A post-only buy at 101 would cross the resting ask — must be rejected,
	// not silently filled as a taker.
	po := mkOrder("po1", models.Buy, models.PostOnly, "101", "1")
	_, _, err := b.Submit(po)
	if err == nil {
		t.Fatal("expected a crossing post-only order to be rejected")
	}
	if po.Status != models.StatusRejected {
		t.Fatalf("crossing post-only order status = %s, want REJECTED", po.Status)
	}
	if _, ok := b.OrderByID("po1"); ok {
		t.Fatal("a rejected post-only order must not rest on the book")
	}
}

func TestPostOnly_RestsWhenNotCrossing(t *testing.T) {
	b := New("BTC-USDT", models.Spot)

	ask := mkOrder("ask1", models.Sell, models.Limit, "100", "1")
	if _, _, err := b.Submit(ask); err != nil {
		t.Fatal(err)
	}

	// A post-only buy at 99 does not cross — it should rest normally.
	po := mkOrder("po2", models.Buy, models.PostOnly, "99", "1")
	if _, _, err := b.Submit(po); err != nil {
		t.Fatalf("non-crossing post-only order should be accepted, got: %v", err)
	}
	if po.Status != models.StatusOpen {
		t.Fatalf("post-only order status = %s, want OPEN", po.Status)
	}
	if _, ok := b.OrderByID("po2"); !ok {
		t.Fatal("accepted post-only order should be resting on the book")
	}
}

// TestCheckMarkPriceTriggers_FiresWithoutAnyTrade is a regression test for
// the 2026-09-16 fix: a resting stop order (this is how a futures TP/SL leg
// rests on the book) previously only re-evaluated as a side effect of a real
// trade printing on this symbol via processStopTriggers — a completely
// quiet book (zero trades) could never trigger a stop no matter how far the
// price conceptually moved, since lastTradePrice stays at its zero-value
// forever. This test submits a stop-buy and confirms it stays untriggered
// with NO trades at all, then fires purely from a mark-price check with
// STILL zero trades on the book, proving this path is independent of
// processStopTriggers/lastTradePrice entirely.
func TestCheckMarkPriceTriggers_FiresWithoutAnyTrade(t *testing.T) {
	b := New("BI2X-BI2XUSD", models.Futures)

	stop := mkOrder("stop-mp1", models.Buy, models.Stop, "0", "1")
	stop.StopPrice = decimal.RequireFromString("100")
	if _, _, err := b.Submit(stop); err != nil {
		t.Fatalf("stop order submission failed: %v", err)
	}
	if stop.Status != models.StatusOpen {
		t.Fatalf("stop order status = %s, want OPEN (resting untriggered)", stop.Status)
	}

	// Mark price below trigger: must not fire.
	if trades, _ := b.CheckMarkPriceTriggers(decimal.RequireFromString("99")); len(trades) != 0 {
		t.Fatalf("mark price 99 (below trigger 100) fired %d trades, want 0", len(trades))
	}
	if stop.Status != models.StatusOpen {
		t.Fatalf("stop should still be untriggered at mark price 99, status=%s", stop.Status)
	}

	// Rest an ask for the triggered stop-buy to match against once activated
	// — same setup as the trade-triggered test, but note NO trade has
	// occurred anywhere in this test yet (lastTradePrice is still its zero
	// value), proving this path does not depend on it.
	ask := mkOrder("ask-mp101", models.Sell, models.Limit, "101", "1")
	if _, _, err := b.Submit(ask); err != nil {
		t.Fatal(err)
	}

	// Mark price at/above trigger: must fire, purely from the mark-price
	// check, with the book's lastTradePrice still untouched by any trade.
	trades, _ := b.CheckMarkPriceTriggers(decimal.RequireFromString("100"))
	found := false
	for _, tr := range trades {
		if tr.TakerOrderID == "stop-mp1" || tr.MakerOrderID == "stop-mp1" {
			found = true
		}
	}
	if !found {
		t.Fatal("stop order should have triggered and traded at mark price 100")
	}
	if stop.Status != models.StatusFilled {
		t.Fatalf("triggered stop-market order status = %s, want FILLED", stop.Status)
	}
}

// TestCheckMarkPriceTriggers_NoOpWhenNothingCrosses confirms the sweep is
// harmless to call repeatedly/frequently (as the periodic production sweep
// does) when nothing is actually triggered — no trades, no panics, no state
// changes, whether there are zero stop orders or several that don't cross.
func TestCheckMarkPriceTriggers_NoOpWhenNothingCrosses(t *testing.T) {
	b := New("BI2X-BI2XUSD", models.Futures)

	if trades, cancelled := b.CheckMarkPriceTriggers(decimal.RequireFromString("50")); len(trades) != 0 || len(cancelled) != 0 {
		t.Fatalf("empty book: got %d trades, %d cancelled, want 0/0", len(trades), len(cancelled))
	}

	stopBuy := mkOrder("stop-mp-nc1", models.Buy, models.Stop, "0", "1")
	stopBuy.StopPrice = decimal.RequireFromString("200")
	stopSell := mkOrder("stop-mp-nc2", models.Sell, models.Stop, "0", "1")
	stopSell.StopPrice = decimal.RequireFromString("50")
	if _, _, err := b.Submit(stopBuy); err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.Submit(stopSell); err != nil {
		t.Fatal(err)
	}

	// Mark price 100 is between both triggers (buy needs >=200, sell needs <=50) — neither should fire.
	if trades, cancelled := b.CheckMarkPriceTriggers(decimal.RequireFromString("100")); len(trades) != 0 || len(cancelled) != 0 {
		t.Fatalf("mark price between both triggers: got %d trades, %d cancelled, want 0/0", len(trades), len(cancelled))
	}
	if stopBuy.Status != models.StatusOpen || stopSell.Status != models.StatusOpen {
		t.Fatalf("both stops should remain untriggered, got buy=%s sell=%s", stopBuy.Status, stopSell.Status)
	}

	// Also confirm an invalid (non-positive) mark price is a safe no-op,
	// e.g. before any real price has ever been reported for this symbol.
	if trades, cancelled := b.CheckMarkPriceTriggers(decimal.Zero); len(trades) != 0 || len(cancelled) != 0 {
		t.Fatalf("zero mark price: got %d trades, %d cancelled, want 0/0 (must not panic or misfire)", len(trades), len(cancelled))
	}
}
