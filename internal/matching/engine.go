// Package matching wraps the single-threaded order book (Phase 1) with a
// goroutine-per-symbol concurrency model. All order book operations happen
// inside the engine's private goroutine; no mutex is ever held on the book.
package matching

import (
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dex/matching-engine/internal/models"
	"github.com/dex/matching-engine/internal/orderbook"
	"github.com/shopspring/decimal"
)

// ── Request types ─────────────────────────────────────────────────────────────

type reqKind uint8

const (
	reqSubmit reqKind = iota
	reqCancel
	reqModify
	reqAllOrders
	reqDepth
)

type request struct {
	kind     reqKind
	order    *models.Order   // reqSubmit
	orderID  string          // reqCancel / reqModify
	newPrice decimal.Decimal // reqModify
	newQty   decimal.Decimal // reqModify
	levels   int             // reqDepth
	snapshot bool            // reqSubmit: also populate result.orderSnapshot
	resultCh chan<- result
}

type result struct {
	order         *models.Order
	orderSnapshot *models.Order
	orders        []*models.Order
	trades        []*models.Trade
	err           error
	bids          []orderbook.LevelSnapshot
	asks          []orderbook.LevelSnapshot
	bestBid       decimal.Decimal
	bestAsk       decimal.Decimal
}

// ReleaseFunc returns an order's reserved funds to the risk ledger. Invoked
// for any resting maker order cancelled by self-trade prevention.
type ReleaseFunc func(order *models.Order)

// ── Interfaces ────────────────────────────────────────────────────────────────

// EventPublisher receives outbound events non-blocking. Satisfied by events.Bus.
type EventPublisher interface {
	Publish(e *models.Event)
}

// SettlementHandler is invoked after each trade, before the trade event is
// published. Concrete implementations live in internal/settlement (Phase 6).
type SettlementHandler interface {
	Settle(trade *models.Trade) error
}

// NoopSettlement is the default until Phase 6 provides real handlers.
type NoopSettlement struct{}

func (NoopSettlement) Settle(_ *models.Trade) error { return nil }

// ── Engine ────────────────────────────────────────────────────────────────────

const inputBufSize = 4096

// Engine is a single-symbol, single-goroutine matching engine.
// The book, order index, and sequence counter are all owned exclusively by
// the engine's goroutine — no external locking is required.
type Engine struct {
	symbol string
	market models.MarketType
	book   *orderbook.Book

	inputCh chan request
	seq     atomic.Uint64
	halted  atomic.Bool

	pub        EventPublisher
	settlement SettlementHandler
	release    ReleaseFunc

	stopOnce sync.Once
	stopCh   chan struct{}
	done     chan struct{}
}

// NewEngine creates and immediately starts a matching engine goroutine.
// release may be nil (defaults to a no-op).
func NewEngine(symbol string, market models.MarketType, pub EventPublisher, sh SettlementHandler, release ReleaseFunc) *Engine {
	if sh == nil {
		sh = NoopSettlement{}
	}
	if release == nil {
		release = func(*models.Order) {}
	}
	e := &Engine{
		symbol:     symbol,
		market:     market,
		book:       orderbook.New(symbol, market),
		inputCh:    make(chan request, inputBufSize),
		pub:        pub,
		settlement: sh,
		release:    release,
		stopCh:     make(chan struct{}),
		done:       make(chan struct{}),
	}
	go e.run()
	return e
}

// ── Public API ────────────────────────────────────────────────────────────────

// Submit sends an order to the engine goroutine and blocks until it is
// processed. The order argument is mutated in place by the engine goroutine
// (status/filled/etc.) and, if it rests on the book, may be mutated further
// by later matches from other goroutines after Submit returns — callers
// that retain the passed-in pointer and read its fields after Submit
// returns are racing the engine goroutine. Use SubmitSnapshot for a safe
// point-in-time copy instead.
func (e *Engine) Submit(order *models.Order) ([]*models.Trade, error) {
	ch := make(chan result, 1)
	e.inputCh <- request{kind: reqSubmit, order: order, resultCh: ch}
	r := <-ch
	return r.trades, r.err
}

// SubmitSnapshot behaves like Submit but also returns a copy of the order's
// state immediately after processing, safe to read without racing later
// mutation of the resting order by other goroutines. The copy is taken
// inside the engine goroutine before the result is sent, so it cannot race
// a subsequent request that mutates the same underlying order.
func (e *Engine) SubmitSnapshot(order *models.Order) ([]*models.Trade, *models.Order, error) {
	ch := make(chan result, 1)
	e.inputCh <- request{kind: reqSubmit, order: order, snapshot: true, resultCh: ch}
	r := <-ch
	return r.trades, r.orderSnapshot, r.err
}

// Cancel cancels a resting order. Blocks until processed.
func (e *Engine) Cancel(orderID string) (*models.Order, error) {
	ch := make(chan result, 1)
	e.inputCh <- request{kind: reqCancel, orderID: orderID, resultCh: ch}
	r := <-ch
	return r.order, r.err
}

// Modify replaces price/qty on a resting order (cancel-and-replace).
func (e *Engine) Modify(orderID string, newPrice, newQty decimal.Decimal) (*models.Order, []*models.Trade, error) {
	ch := make(chan result, 1)
	e.inputCh <- request{kind: reqModify, orderID: orderID, newPrice: newPrice, newQty: newQty, resultCh: ch}
	r := <-ch
	return r.order, r.trades, r.err
}

// AllOrders returns every resting order in this engine's book. Blocks until
// processed by the engine goroutine, consistent with Submit/Cancel/Modify.
func (e *Engine) AllOrders() []*models.Order {
	ch := make(chan result, 1)
	e.inputCh <- request{kind: reqAllOrders, resultCh: ch}
	r := <-ch
	return r.orders
}

// Halt stops the engine from accepting new orders (symbol-wide circuit breaker, Phase 7).
func (e *Engine) Halt() { e.halted.Store(true) }

// Resume re-enables the engine after a halt.
func (e *Engine) Resume() { e.halted.Store(false) }

// IsHalted reports whether the engine is currently halted.
func (e *Engine) IsHalted() bool { return e.halted.Load() }

// Stop shuts down the engine goroutine and waits for it to exit.
func (e *Engine) Stop() {
	e.stopOnce.Do(func() { close(e.stopCh) })
	<-e.done
}

// BestBid returns the current best bid price. Routed through the engine
// goroutine so it never races with concurrent book mutation.
func (e *Engine) BestBid() decimal.Decimal {
	ch := make(chan result, 1)
	e.inputCh <- request{kind: reqDepth, levels: 0, resultCh: ch}
	r := <-ch
	return r.bestBid
}

// BestAsk returns the current best ask price. Routed through the engine
// goroutine so it never races with concurrent book mutation.
func (e *Engine) BestAsk() decimal.Decimal {
	ch := make(chan result, 1)
	e.inputCh <- request{kind: reqDepth, levels: 0, resultCh: ch}
	r := <-ch
	return r.bestAsk
}

// Depth returns up to `levels` price levels for each side as immutable
// snapshots. Routed through the engine goroutine so it never races with
// concurrent book mutation.
func (e *Engine) Depth(levels int) (bids, asks []orderbook.LevelSnapshot) {
	ch := make(chan result, 1)
	e.inputCh <- request{kind: reqDepth, levels: levels, resultCh: ch}
	r := <-ch
	return r.bids, r.asks
}

// ── Internal goroutine ────────────────────────────────────────────────────────

func (e *Engine) run() {
	defer close(e.done)
	for {
		select {
		case req := <-e.inputCh:
			e.handle(req)
		case <-e.stopCh:
			// Drain any buffered requests so their callers don't block
			// forever on resultCh; respond with a shutdown error.
			e.drain()
			return
		}
	}
}

// drain empties the input channel, replying to each pending request with a
// shutdown error so callers waiting on Submit/Cancel/Depth return promptly.
func (e *Engine) drain() {
	for {
		select {
		case req := <-e.inputCh:
			res := result{err: fmt.Errorf("engine %s/%s stopped", e.symbol, e.market)}
			if req.kind == reqSubmit && req.snapshot {
				res.orderSnapshot = req.order.Copy()
			}
			req.resultCh <- res
		default:
			return
		}
	}
}

func (e *Engine) handle(req request) {
	var res result
	switch req.kind {
	case reqSubmit:
		if e.halted.Load() {
			req.order.Status = models.StatusRejected
			res.err = fmt.Errorf("symbol %s/%s is halted", e.symbol, e.market)
			res.order = req.order
			if req.snapshot {
				res.orderSnapshot = req.order.Copy()
			}
		} else {
			trades, cancelled, err := e.book.Submit(req.order)
			res.trades = trades
			res.err = err
			res.order = req.order
			if req.snapshot {
				res.orderSnapshot = req.order.Copy()
			}
			if err == nil {
				e.postProcessAndCancel(req.order, trades, cancelled)
			} else if errors.Is(err, orderbook.ErrFOKNotFilled) {
				e.publishEvent(models.EventOrderCancelled, req.order, nil)
			} else {
				e.publishEvent(models.EventOrderRejected, req.order, nil)
			}
		}

	case reqCancel:
		order, err := e.book.Cancel(req.orderID)
		res.order = order
		res.err = err
		if err == nil {
			e.publishEvent(models.EventOrderCancelled, order, nil)
		}

	case reqModify:
		order, trades, cancelled, err := e.book.Modify(req.orderID, req.newPrice, req.newQty)
		res.order = order
		res.trades = trades
		res.err = err
		if err == nil {
			e.postProcessAndCancel(order, trades, cancelled)
		}

	case reqAllOrders:
		res.orders = e.book.AllOrders()

	case reqDepth:
		res.bids, res.asks = e.book.Depth(req.levels)
		res.bestBid = e.book.BestBid()
		res.bestAsk = e.book.BestAsk()
	}
	req.resultCh <- res
}

// postProcessAndCancel runs the taker/trade event pipeline and releases +
// publishes cancellation events for any self-trade-cancelled maker orders,
// interleaving all events in chronological (UpdatedAt/ExecutedAt) order.
func (e *Engine) postProcessAndCancel(order *models.Order, trades []*models.Trade, cancelled []*models.Order) {
	e.postProcess(order, trades, cancelled)
	for _, c := range cancelled {
		e.release(c)
	}
}

func (e *Engine) postProcess(order *models.Order, trades []*models.Trade, cancelled []*models.Order) {
	// Publish order state event for the incoming (taker) order.
	switch order.Status {
	case models.StatusOpen:
		e.publishEvent(models.EventOrderOpen, order, nil)
	case models.StatusPartiallyFilled:
		e.publishEvent(models.EventOrderPartial, order, nil)
	case models.StatusFilled:
		e.publishEvent(models.EventOrderFilled, order, nil)
	case models.StatusCancelled:
		e.publishEvent(models.EventOrderCancelled, order, nil)
	}

	// Interleave trade and self-trade-cancellation events in the chronological
	// order they actually occurred inside matchAggressively (by ExecutedAt /
	// UpdatedAt, both stamped via time.Now() in that single-goroutine loop),
	// rather than always publishing all trades before all cancellations.
	type postItem struct {
		isTrade bool
		trade   *models.Trade
		maker   *models.Order
	}
	items := make([]postItem, 0, len(trades)+len(cancelled))
	for _, t := range trades {
		items = append(items, postItem{isTrade: true, trade: t})
	}
	for _, c := range cancelled {
		items = append(items, postItem{isTrade: false, maker: c})
	}
	timestamp := func(it postItem) time.Time {
		if it.isTrade {
			return it.trade.ExecutedAt
		}
		return it.maker.UpdatedAt
	}
	sort.SliceStable(items, func(i, j int) bool {
		return timestamp(items[i]).Before(timestamp(items[j]))
	})

	for _, it := range items {
		if !it.isTrade {
			e.publishEvent(models.EventOrderCancelled, it.maker, nil)
			continue
		}
		trade := it.trade
		// Settlement is synchronous and runs inside the matching goroutine so
		// that the ledger is always consistent before the event is published.
		// If settlement fails (e.g. insufficient balance for a debit), the
		// ledger and the published trade event would diverge, so log it as a
		// critical error for manual reconciliation rather than swallowing it.
		if err := e.settlement.Settle(trade); err != nil {
			slog.Error("settlement failed; ledger may be inconsistent with trade event",
				"tradeId", trade.ID, "symbol", trade.Symbol, "price", trade.Price, "qty", trade.Quantity, "err", err)
		}
		e.publishEvent(models.EventTrade, nil, trade)

		maker := trade.BuyOrder
		if maker.ID == order.ID {
			maker = trade.SellOrder
		}
		switch maker.Status {
		case models.StatusPartiallyFilled:
			e.publishEvent(models.EventOrderPartial, maker, nil)
		case models.StatusFilled:
			e.publishEvent(models.EventOrderFilled, maker, nil)
		}
	}
}

func (e *Engine) publishEvent(typ models.EventType, order *models.Order, trade *models.Trade) {
	seq := e.seq.Add(1)
	evt := &models.Event{
		Type:           typ,
		Symbol:         e.symbol,
		Market:         string(e.market),
		SequenceNumber: seq,
		Order:          order.Copy(),
		Trade:          trade.Copy(),
	}
	// Non-blocking: if the publisher is backed up, the event is dropped.
	// The publisher (events.Bus) handles backpressure with a buffered channel.
	e.pub.Publish(evt)
}
