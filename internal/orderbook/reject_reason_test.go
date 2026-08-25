package orderbook

import (
	"testing"

	"github.com/dex/matching-engine/internal/models"
	"github.com/shopspring/decimal"
)

// These pin down that every StatusRejected/StatusCancelled transition inside
// the book also sets RejectReason to something non-empty (plan.md 4.1 item
// 2: order history must include a rejection/cancel reason).

func TestRejectReason_PostOnlyCrossing(t *testing.T) {
	b := New("BTC-USDT", models.Spot)
	ask := mkOrder("ask1", models.Sell, models.Limit, "100", "1")
	if _, _, err := b.Submit(ask); err != nil {
		t.Fatal(err)
	}
	po := mkOrder("po1", models.Buy, models.PostOnly, "101", "1")
	if _, _, err := b.Submit(po); err == nil {
		t.Fatal("expected rejection")
	}
	if po.RejectReason == "" {
		t.Fatal("expected a non-empty RejectReason for a crossing post-only order")
	}
}

func TestRejectReason_FOKNotFillable(t *testing.T) {
	b := New("BTC-USDT", models.Spot)
	fok := mkOrder("fok1", models.Buy, models.FOK, "100", "5") // no liquidity at all
	if _, _, err := b.Submit(fok); err == nil {
		t.Fatal("expected FOK rejection")
	}
	if fok.Status != models.StatusCancelled {
		t.Fatalf("status = %s, want CANCELLED", fok.Status)
	}
	if fok.RejectReason == "" {
		t.Fatal("expected a non-empty RejectReason for an unfillable FOK order")
	}
}

func TestRejectReason_IOCUnfilledRemainder(t *testing.T) {
	b := New("BTC-USDT", models.Spot)
	ask := mkOrder("ask1", models.Sell, models.Limit, "100", "1")
	if _, _, err := b.Submit(ask); err != nil {
		t.Fatal(err)
	}
	ioc := mkOrder("ioc1", models.Buy, models.IOC, "100", "5") // only 1 available, wants 5
	if _, _, err := b.Submit(ioc); err != nil {
		t.Fatal(err)
	}
	if ioc.Status != models.StatusCancelled {
		t.Fatalf("status = %s, want CANCELLED", ioc.Status)
	}
	if ioc.RejectReason == "" {
		t.Fatal("expected a non-empty RejectReason for an IOC order with an unfilled remainder")
	}
}

func TestRejectReason_MarketUnfilledRemainder(t *testing.T) {
	b := New("BTC-USDT", models.Spot)
	mkt := mkOrder("mkt1", models.Buy, models.Market, "0", "5") // empty book
	if _, _, err := b.Submit(mkt); err != nil {
		t.Fatal(err)
	}
	if mkt.Status != models.StatusCancelled {
		t.Fatalf("status = %s, want CANCELLED", mkt.Status)
	}
	if mkt.RejectReason == "" {
		t.Fatal("expected a non-empty RejectReason for a market order with no liquidity")
	}
}

func TestRejectReason_ExplicitCancel(t *testing.T) {
	b := New("BTC-USDT", models.Spot)
	lim := mkOrder("lim1", models.Buy, models.Limit, "50", "1")
	if _, _, err := b.Submit(lim); err != nil {
		t.Fatal(err)
	}
	cancelled, err := b.Cancel("lim1")
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.RejectReason == "" {
		t.Fatal("expected a non-empty RejectReason for an explicit user cancel")
	}
}

func TestRejectReason_SelfTradePrevention(t *testing.T) {
	b := New("BTC-USDT", models.Spot)
	resting := &models.Order{
		ID: "maker1", AccountID: "same-account", Symbol: "BTC-USDT", Market: models.Spot,
		Side: models.Sell, Type: models.Limit, TimeInForce: models.GTC,
		Price:    decimal.RequireFromString("100"),
		Quantity: decimal.RequireFromString("1"), Status: models.StatusPending,
	}
	if _, _, err := b.Submit(resting); err != nil {
		t.Fatal(err)
	}
	taker := &models.Order{
		ID: "taker1", AccountID: "same-account", Symbol: "BTC-USDT", Market: models.Spot,
		Side: models.Buy, Type: models.Limit, TimeInForce: models.GTC,
		Price:    decimal.RequireFromString("100"),
		Quantity: decimal.RequireFromString("1"), Status: models.StatusPending,
	}
	trades, cancelled, err := b.Submit(taker)
	if err != nil {
		t.Fatal(err)
	}
	if len(trades) != 0 {
		t.Fatalf("expected no trade between an account and itself, got %d", len(trades))
	}
	if len(cancelled) != 1 || cancelled[0].RejectReason == "" {
		t.Fatalf("expected the resting order to be self-trade-cancelled with a reason, got %#v", cancelled)
	}
}
