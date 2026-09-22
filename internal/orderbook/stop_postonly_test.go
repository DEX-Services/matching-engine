package orderbook

import (
	"testing"
	"time"

	"github.com/dex/matching-engine/internal/fixedpoint"
	"github.com/dex/matching-engine/internal/models"
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
		Price: fixedpoint.MustFromString(price), Quantity: fixedpoint.MustFromString(qty),
		Status: models.StatusPending, CreatedAt: time.Now(),
	}
}

func TestStopMarket_RestsUntriggered_ThenFiresOnLastTradePrice(t *testing.T) {
	b := New("BTC-USDT", models.Spot)

	stop := mkOrder("stop1", models.Buy, models.Stop, "0", "1")
	stop.StopPrice = fixedpoint.MustFromString("100")
	if _, _, err := b.Submit(stop); err != nil {
		t.Fatalf("stop order submission failed: %v", err)
	}
	if stop.Status != models.StatusOpen {
		t.Fatalf("stop order status = %s, want OPEN (resting untriggered)", stop.Status)
	}
	// Not yet in the matchable book (bids/asks) — it only rests in
	// stopOrders until triggered. It IS still a real, resting, cancellable
	// order, and OrderByID must find it there: fixed 2026-09-16 after a live
	// incident where a resting Stop-Loss leg (from internal/attached) could
	// never be cancelled through /cancel at all, because OrderByID's
	// pre-check only looked in orderIndex — this assertion used to require
	// the OLD, buggy "not found" behavior; it now requires the fixed one.
	if _, ok := b.OrderByID("stop1"); !ok {
		t.Fatal("untriggered stop should still be findable via OrderByID — it is a real, resting, cancellable order, just not part of the matchable book")
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
	stop.StopPrice = fixedpoint.MustFromString("100")
	if _, _, err := b.Submit(stop); err != nil {
		t.Fatalf("stop order submission failed: %v", err)
	}
	if stop.Status != models.StatusOpen {
		t.Fatalf("stop order status = %s, want OPEN (resting untriggered)", stop.Status)
	}

	// Mark price below trigger: must not fire.
	if trades, _ := b.CheckMarkPriceTriggers(fixedpoint.MustFromString("99")); len(trades) != 0 {
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
	trades, _ := b.CheckMarkPriceTriggers(fixedpoint.MustFromString("100"))
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

	if trades, cancelled := b.CheckMarkPriceTriggers(fixedpoint.MustFromString("50")); len(trades) != 0 || len(cancelled) != 0 {
		t.Fatalf("empty book: got %d trades, %d cancelled, want 0/0", len(trades), len(cancelled))
	}

	stopBuy := mkOrder("stop-mp-nc1", models.Buy, models.Stop, "0", "1")
	stopBuy.StopPrice = fixedpoint.MustFromString("200")
	stopSell := mkOrder("stop-mp-nc2", models.Sell, models.Stop, "0", "1")
	stopSell.StopPrice = fixedpoint.MustFromString("50")
	if _, _, err := b.Submit(stopBuy); err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.Submit(stopSell); err != nil {
		t.Fatal(err)
	}

	// Mark price 100 is between both triggers (buy needs >=200, sell needs <=50) — neither should fire.
	if trades, cancelled := b.CheckMarkPriceTriggers(fixedpoint.MustFromString("100")); len(trades) != 0 || len(cancelled) != 0 {
		t.Fatalf("mark price between both triggers: got %d trades, %d cancelled, want 0/0", len(trades), len(cancelled))
	}
	if stopBuy.Status != models.StatusOpen || stopSell.Status != models.StatusOpen {
		t.Fatalf("both stops should remain untriggered, got buy=%s sell=%s", stopBuy.Status, stopSell.Status)
	}

	// Also confirm an invalid (non-positive) mark price is a safe no-op,
	// e.g. before any real price has ever been reported for this symbol.
	if trades, cancelled := b.CheckMarkPriceTriggers(fixedpoint.Zero); len(trades) != 0 || len(cancelled) != 0 {
		t.Fatalf("zero mark price: got %d trades, %d cancelled, want 0/0 (must not panic or misfire)", len(trades), len(cancelled))
	}
}

// TestUntriggeredStop_CancellableViaOrderByIDThenCancel is a regression test
// for a live incident: a resting (untriggered) STOP order — this is exactly
// how a Stop-Loss leg from internal/attached rests on the book — could never
// be cancelled through the normal user-facing action at all. cmd/engine's
// /cancel handler always calls OrderByID FIRST as an existence/ownership
// pre-check before ever calling Cancel; OrderByID used to only look in
// orderIndex, never stopOrders, so this pre-check always reported "not
// found" for any untriggered stop and the handler returned 404 before
// Cancel — which has always correctly handled stopOrders — was ever reached.
// This test exercises the exact two-call sequence the real handler uses.
func TestUntriggeredStop_CancellableViaOrderByIDThenCancel(t *testing.T) {
	b := New("BTC-USDT", models.Spot)

	stop := mkOrder("stop-cancel-1", models.Sell, models.Stop, "0", "1")
	stop.StopPrice = fixedpoint.MustFromString("50")
	if _, _, err := b.Submit(stop); err != nil {
		t.Fatalf("stop order submission failed: %v", err)
	}

	// Step 1, exactly what /cancel's pre-check does: look the order up
	// without removing it, to verify it exists (and, in the real handler,
	// that the caller owns it) before attempting the actual cancel.
	found, ok := b.OrderByID("stop-cancel-1")
	if !ok {
		t.Fatal("OrderByID pre-check failed to find a resting untriggered stop order — this is the exact bug: /cancel would 404 here and never even attempt Cancel")
	}
	if found.Status != models.StatusOpen {
		t.Fatalf("found order status = %s, want OPEN", found.Status)
	}

	// Step 2, exactly what /cancel does next: the actual cancel.
	cancelled, err := b.Cancel("stop-cancel-1")
	if err != nil {
		t.Fatalf("cancel failed: %v", err)
	}
	if cancelled.Status != models.StatusCancelled {
		t.Fatalf("cancelled order status = %s, want CANCELLED", cancelled.Status)
	}

	// Cancelled, so a further lookup must correctly report "not found" (not
	// still resting, not double-cancellable).
	if _, ok := b.OrderByID("stop-cancel-1"); ok {
		t.Fatal("cancelled stop order should no longer be findable via OrderByID")
	}
}

// TestOrderByID_MatchableAndStopOrders_BothFindable is a broader sanity
// check that OrderByID now correctly covers both maps in general, not just
// the specific stop-order case above — a regular resting LIMIT order (in
// orderIndex) and an untriggered STOP order (in stopOrders) coexisting on
// the same book must both be findable, and a genuinely unknown ID must
// still correctly report not-found from either map.
func TestOrderByID_MatchableAndStopOrders_BothFindable(t *testing.T) {
	b := New("BTC-USDT", models.Spot)

	limit := mkOrder("limit-1", models.Buy, models.Limit, "50", "1")
	if _, _, err := b.Submit(limit); err != nil {
		t.Fatal(err)
	}
	stop := mkOrder("stop-1", models.Sell, models.Stop, "0", "1")
	stop.StopPrice = fixedpoint.MustFromString("40")
	if _, _, err := b.Submit(stop); err != nil {
		t.Fatal(err)
	}

	if _, ok := b.OrderByID("limit-1"); !ok {
		t.Fatal("resting LIMIT order should be findable via OrderByID")
	}
	if _, ok := b.OrderByID("stop-1"); !ok {
		t.Fatal("untriggered STOP order should be findable via OrderByID")
	}
	if _, ok := b.OrderByID("nonexistent"); ok {
		t.Fatal("a genuinely unknown order ID should not be findable")
	}
}
