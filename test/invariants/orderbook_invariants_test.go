// Package invariants contains property-based and scenario tests that assert
// the core invariants from Section 9 of the matching engine spec.
//
// These tests must pass under any valid sequence of order operations, not
// just the fixed scenarios used in unit tests.
package invariants

import (
	"fmt"
	"testing"
	"time"

	"github.com/dex/matching-engine/internal/models"
	"github.com/dex/matching-engine/internal/orderbook"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── Helpers ─────────────────────────────────────────────────────────────────

func newOrder(id string, side models.OrderSide, price, qty string, orderType models.OrderType) *models.Order {
	return &models.Order{
		ID:          id,
		AccountID:   "acct-" + id,
		Symbol:      "BTC-USDT",
		Market:      models.Spot,
		Side:        side,
		Type:        orderType,
		Price:       decimal.RequireFromString(price),
		Quantity:    decimal.RequireFromString(qty),
		TimeInForce: models.GTC,
		Status:      models.StatusPending,
		CreatedAt:   time.Now(),
	}
}

func limitBuy(id, price, qty string) *models.Order {
	return newOrder(id, models.Buy, price, qty, models.Limit)
}

func limitSell(id, price, qty string) *models.Order {
	return newOrder(id, models.Sell, price, qty, models.Limit)
}

func newBook() *orderbook.Book {
	return orderbook.New("BTC-USDT", models.Spot)
}

// ─── Invariant 1: Price-time priority ────────────────────────────────────────
// An order must never be filled ahead of another resting order at a better
// or equal price with an earlier timestamp.

func TestPriceTimePriority_SamePrice(t *testing.T) {
	b := newBook()

	// Two bids at the same price; first one in should be matched first.
	first := limitBuy("buy-1", "100", "1")
	second := limitBuy("buy-2", "100", "1")
	_, _, err := b.Submit(first)
	require.NoError(t, err)
	_, _, err = b.Submit(second)
	require.NoError(t, err)

	// A sell that fills exactly one unit should match buy-1 (earlier).
	sell := limitSell("sell-1", "100", "1")
	trades, _, err := b.Submit(sell)
	require.NoError(t, err)
	require.Len(t, trades, 1)
	assert.Equal(t, "buy-1", trades[0].MakerOrderID, "first order should be matched first (time priority)")

	// buy-2 should still be resting.
	_, stillOpen := b.OrderByID("buy-2")
	assert.True(t, stillOpen, "second order should still be resting")
}

func TestPriceTimePriority_BetterPriceFirst(t *testing.T) {
	b := newBook()

	// Two bids at different prices; better price should be matched first.
	low := limitBuy("buy-low", "99", "1")
	high := limitBuy("buy-high", "101", "1")

	_, _, err := b.Submit(low)
	require.NoError(t, err)
	_, _, err = b.Submit(high)
	require.NoError(t, err)

	// Sell at 99 should match the 101 bid first (better price for seller = higher bid).
	sell := limitSell("sell-1", "99", "1")
	trades, _, err := b.Submit(sell)
	require.NoError(t, err)
	require.Len(t, trades, 1)
	assert.Equal(t, "buy-high", trades[0].MakerOrderID, "higher bid must be matched first")
}

// ─── Invariant 2: Conservation ───────────────────────────────────────────────
// Sum of filled qty on the buy side equals sum of matched qty on the sell side.

func TestConservation_SingleTrade(t *testing.T) {
	b := newBook()

	buy := limitBuy("buy-1", "100", "5")
	_, _, err := b.Submit(buy)
	require.NoError(t, err)

	sell := limitSell("sell-1", "100", "5")
	trades, _, err := b.Submit(sell)
	require.NoError(t, err)

	totalBuyFilled := decimal.Zero
	totalSellFilled := decimal.Zero
	for _, t := range trades {
		if t.MakerSide == models.Buy {
			totalBuyFilled = totalBuyFilled.Add(t.Quantity)
		} else {
			totalSellFilled = totalSellFilled.Add(t.Quantity)
		}
	}

	// In each trade, maker qty == taker qty, so aggregate both sides are equal.
	totalQty := decimal.Zero
	for _, trade := range trades {
		totalQty = totalQty.Add(trade.Quantity)
	}
	assert.True(t, buy.Filled.Equal(totalQty), "buy filled must equal total traded qty")
	assert.True(t, sell.Filled.Equal(totalQty), "sell filled must equal total traded qty")
}

func TestConservation_MultiplePartialFills(t *testing.T) {
	b := newBook()

	// Rest three small sells.
	for i := 1; i <= 3; i++ {
		s := limitSell(fmt.Sprintf("sell-%d", i), "100", "2")
		_, _, err := b.Submit(s)
		require.NoError(t, err)
	}

	// One big buy sweeps all three.
	buy := limitBuy("buy-1", "100", "6")
	trades, _, err := b.Submit(buy)
	require.NoError(t, err)
	require.Len(t, trades, 3, "should generate 3 trades")

	totalTraded := decimal.Zero
	for _, t := range trades {
		totalTraded = totalTraded.Add(t.Quantity)
	}
	assert.True(t, buy.Filled.Equal(totalTraded), "buyer filled qty must equal sum of trade quantities")
	assert.True(t, totalTraded.Equal(decimal.NewFromInt(6)), "total traded must be 6")
}

// ─── Invariant 3: Limit price never violated ─────────────────────────────────
// No order executes at a price worse than its own limit price.

func TestLimitPriceNeverViolated_Buy(t *testing.T) {
	b := newBook()

	// Rest a sell at 105.
	sell := limitSell("sell-1", "105", "1")
	_, _, err := b.Submit(sell)
	require.NoError(t, err)

	// Buy limit at 100 must NOT match (105 > 100).
	buy := limitBuy("buy-1", "100", "1")
	trades, _, err := b.Submit(buy)
	require.NoError(t, err)
	assert.Empty(t, trades, "buy at 100 must not match a resting sell at 105")

	// Buy limit at 110 CAN match the sell at 105; fill price should be 105 (maker).
	buy2 := limitBuy("buy-2", "110", "1")
	trades2, _, err := b.Submit(buy2)
	require.NoError(t, err)
	require.Len(t, trades2, 1)
	assert.True(t, trades2[0].Price.Equal(decimal.NewFromInt(105)),
		"fill price must be maker price (105), not aggressor limit (110)")
	// Aggressor got 105 which is ≤ their limit 110 — not worse.
	assert.True(t, trades2[0].Price.LessThanOrEqual(buy2.Price),
		"fill price must not exceed buyer's limit")
}

func TestLimitPriceNeverViolated_Sell(t *testing.T) {
	b := newBook()

	// Rest a buy at 95.
	buy := limitBuy("buy-1", "95", "1")
	_, _, err := b.Submit(buy)
	require.NoError(t, err)

	// Sell limit at 100 must NOT match (95 < 100).
	sell := limitSell("sell-1", "100", "1")
	trades, _, err := b.Submit(sell)
	require.NoError(t, err)
	assert.Empty(t, trades, "sell at 100 must not match a resting buy at 95")

	// Sell limit at 90 CAN match at 95; fill price must be ≥ seller's limit.
	sell2 := limitSell("sell-2", "90", "1")
	trades2, _, err := b.Submit(sell2)
	require.NoError(t, err)
	require.Len(t, trades2, 1)
	assert.True(t, trades2[0].Price.GreaterThanOrEqual(sell2.Price),
		"fill price must not be below seller's limit")
}

// ─── Invariant 4: Book is never left in a crossed state ──────────────────────
// After any operation, bestBid must be strictly less than bestAsk (or one/both
// sides are empty).

func TestBookNeverCrossed(t *testing.T) {
	b := newBook()

	ops := []struct {
		order *models.Order
	}{
		{limitBuy("b1", "100", "3")},
		{limitBuy("b2", "101", "2")},
		{limitSell("s1", "102", "1")},
		{limitSell("s2", "103", "4")},
		{limitBuy("b3", "102", "5")}, // crosses s1 at 102
		{limitSell("s3", "100", "2")}, // crosses b1 at 100
	}

	for _, op := range ops {
		_, _, err := b.Submit(op.order)
		require.NoError(t, err)
		assertNotCrossed(t, b)
	}
}

func assertNotCrossed(t *testing.T, b *orderbook.Book) {
	t.Helper()
	bestBid := b.BestBid()
	bestAsk := b.BestAsk()
	if bestBid.IsZero() || bestAsk.IsZero() {
		return // one side empty → cannot be crossed
	}
	assert.True(t, bestBid.LessThan(bestAsk),
		"book is crossed: bestBid=%s bestAsk=%s", bestBid, bestAsk)
}

// ─── Invariant 5: Every accepted order reaches a terminal state ───────────────
// After all matching, no order is silently dropped or stuck in a non-terminal
// status when there is nothing more to do.

func TestOrderTerminalState_MarketOrderFullyFilled(t *testing.T) {
	b := newBook()

	// Rest enough liquidity.
	_, _, err := b.Submit(limitSell("s1", "100", "10"))
	require.NoError(t, err)

	mkt := &models.Order{
		ID: "mkt-1", AccountID: "acct-mkt-1", Symbol: "BTC-USDT", Market: models.Spot,
		Side: models.Buy, Type: models.Market,
		Quantity: decimal.NewFromInt(5), Status: models.StatusPending,
		CreatedAt: time.Now(),
	}
	_, _, err = b.Submit(mkt)
	require.NoError(t, err)
	assert.True(t, mkt.IsTerminal(), "market order must reach terminal state after submission")
	assert.Equal(t, models.StatusFilled, mkt.Status)
}

func TestOrderTerminalState_MarketOrderNoLiquidity(t *testing.T) {
	b := newBook()
	mkt := &models.Order{
		ID: "mkt-2", Symbol: "BTC-USDT", Market: models.Spot,
		Side: models.Buy, Type: models.Market,
		Quantity: decimal.NewFromInt(1), Status: models.StatusPending,
		CreatedAt: time.Now(),
	}
	_, _, err := b.Submit(mkt)
	require.NoError(t, err)
	assert.True(t, mkt.IsTerminal(), "market order with no liquidity must still reach terminal state")
	assert.Equal(t, models.StatusCancelled, mkt.Status)
}

func TestOrderTerminalState_IOCPartiallyFilled(t *testing.T) {
	b := newBook()
	_, _, err := b.Submit(limitSell("s1", "100", "3"))
	require.NoError(t, err)

	ioc := newOrder("ioc-1", models.Buy, "100", "10", models.IOC)
	_, _, err = b.Submit(ioc)
	require.NoError(t, err)
	assert.True(t, ioc.IsTerminal(), "IOC order must reach terminal state")
	// Partially filled → remainder cancelled.
	assert.Equal(t, models.StatusCancelled, ioc.Status)
	assert.True(t, ioc.Filled.Equal(decimal.NewFromInt(3)))
}

func TestOrderTerminalState_FOKCancelledWhenCannotFill(t *testing.T) {
	b := newBook()
	_, _, err := b.Submit(limitSell("s1", "100", "3"))
	require.NoError(t, err)

	fok := newOrder("fok-1", models.Buy, "100", "10", models.FOK)
	_, _, err = b.Submit(fok)
	assert.ErrorIs(t, err, orderbook.ErrFOKNotFilled)
	assert.True(t, fok.IsTerminal())
	assert.Equal(t, models.StatusCancelled, fok.Status)
	// Book must be untouched.
	assertNotCrossed(t, b)
}

// ─── Invariant 6: Partial fills update state correctly ───────────────────────

func TestPartialFill_StatusAndQuantities(t *testing.T) {
	b := newBook()
	buy := limitBuy("buy-1", "100", "10")
	_, _, err := b.Submit(buy)
	require.NoError(t, err)

	// Partial fill with a smaller sell.
	sell := limitSell("sell-1", "100", "4")
	trades, _, err := b.Submit(sell)
	require.NoError(t, err)
	require.Len(t, trades, 1)
	assert.True(t, trades[0].Quantity.Equal(decimal.NewFromInt(4)))

	assert.Equal(t, models.StatusPartiallyFilled, buy.Status)
	assert.True(t, buy.Filled.Equal(decimal.NewFromInt(4)))
	assert.True(t, buy.RemainingQty().Equal(decimal.NewFromInt(6)))

	// buy-1 must still be resting.
	_, open := b.OrderByID("buy-1")
	assert.True(t, open, "partially-filled order must remain in the book")
}

// ─── Invariant 7: Cancel removes order from the book ─────────────────────────

func TestCancel_RemovesOrderFromBook(t *testing.T) {
	b := newBook()
	buy := limitBuy("buy-1", "100", "5")
	_, _, err := b.Submit(buy)
	require.NoError(t, err)

	cancelled, err := b.Cancel("buy-1")
	require.NoError(t, err)
	assert.Equal(t, models.StatusCancelled, cancelled.Status)
	assert.True(t, cancelled.IsTerminal())

	_, stillOpen := b.OrderByID("buy-1")
	assert.False(t, stillOpen, "cancelled order must not remain in the book")
}

// ─── Invariant 8: Post-only rejection when crossing ──────────────────────────

func TestPostOnly_RejectsWhenCrossing(t *testing.T) {
	b := newBook()
	_, _, err := b.Submit(limitSell("s1", "100", "1"))
	require.NoError(t, err)

	po := newOrder("po-1", models.Buy, "100", "1", models.PostOnly)
	_, _, err = b.Submit(po)
	assert.ErrorIs(t, err, orderbook.ErrPostOnlyCrossing)
	assert.Equal(t, models.StatusRejected, po.Status)
	assertNotCrossed(t, b)
}

func TestPostOnly_RestsWhenNotCrossing(t *testing.T) {
	b := newBook()
	_, _, err := b.Submit(limitSell("s1", "105", "1"))
	require.NoError(t, err)

	po := newOrder("po-1", models.Buy, "100", "1", models.PostOnly)
	_, _, err = b.Submit(po)
	require.NoError(t, err)
	assert.Equal(t, models.StatusOpen, po.Status)
}
