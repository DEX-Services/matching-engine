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
