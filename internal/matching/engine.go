// Package matching wraps the single-threaded order book (Phase 1) with a
// goroutine-per-symbol concurrency model. All order book operations happen
// inside the engine's private goroutine; no mutex is ever held on the book.
package matching

import (
	"fmt"
	"sync"
	"sync/atomic"

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
)

type request struct {
	kind     reqKind
	order    *models.Order  // reqSubmit
	orderID  string         // reqCancel / reqModify
	newPrice decimal.Decimal // reqModify
	newQty   decimal.Decimal // reqModify
	resultCh chan<- result
}

type result struct {
	order  *models.Order
	orders []*models.Order
	trades []*models.Trade
	err    error
}

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

	stopOnce sync.Once
	stopCh   chan struct{}
	done     chan struct{}
}

// NewEngine creates and immediately starts a matching engine goroutine.
func NewEngine(symbol string, market models.MarketType, pub EventPublisher, sh SettlementHandler) *Engine {
	if sh == nil {
		sh = NoopSettlement{}
	}
	e := &Engine{
		symbol:     symbol,
		market:     market,
		book:       orderbook.New(symbol, market),
		inputCh:    make(chan request, inputBufSize),
		pub:        pub,
		settlement: sh,
		stopCh:     make(chan struct{}),
		done:       make(chan struct{}),
	}
	go e.run()
	return e
}

// ── Public API ────────────────────────────────────────────────────────────────

// Submit sends an order to the engine goroutine and blocks until it is processed.
func (e *Engine) Submit(order *models.Order) ([]*models.Trade, error) {
	ch := make(chan result, 1)
	e.inputCh <- request{kind: reqSubmit, order: order, resultCh: ch}
	r := <-ch
	return r.trades, r.err
}

// Cancel cancels a resting order. Blocks until processed.
func (e *Engine) Cancel(orderID string) (*models.Order, error) {
	ch := make(chan result, 1)
	e.inputCh <- request{kind: reqCancel, orderID: orderID, resultCh: ch}
	r := <-ch
	return r.order, r.err
}

// Modify replaces price/qty on a resting order (cancel-and-replace).
func (e *Engine) Modify(orderID string, newPrice, newQty decimal.Decimal) (*models.Order, error) {
	ch := make(chan result, 1)
	e.inputCh <- request{kind: reqModify, orderID: orderID, newPrice: newPrice, newQty: newQty, resultCh: ch}
	r := <-ch
	return r.order, r.err
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

// BestBid returns the current best bid price (safe to call after Stop).
func (e *Engine) BestBid() decimal.Decimal { return e.book.BestBid() }

// BestAsk returns the current best ask price (safe to call after Stop).
func (e *Engine) BestAsk() decimal.Decimal { return e.book.BestAsk() }

// Depth returns up to `levels` price levels for each side.
func (e *Engine) Depth(levels int) (bids, asks []*orderbook.PriceLevel) {
	return e.book.Depth(levels)
}

// ── Internal goroutine ────────────────────────────────────────────────────────

func (e *Engine) run() {
	defer close(e.done)
	for {
		select {
		case req := <-e.inputCh:
			e.handle(req)
		case <-e.stopCh:
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
		} else {
			trades, err := e.book.Submit(req.order)
			res.trades = trades
			res.err = err
			res.order = req.order
			if err == nil {
				e.postProcess(req.order, trades)
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
		order, err := e.book.Modify(req.orderID, req.newPrice, req.newQty)
		res.order = order
		res.err = err
		if err == nil {
			e.publishEvent(models.EventOrderOpen, order, nil)
		}

	case reqAllOrders:
		res.orders = e.book.AllOrders()
	}
	req.resultCh <- res
}

func (e *Engine) postProcess(order *models.Order, trades []*models.Trade) {
	// Publish order state event.
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

	// For each trade: run settlement then publish the trade event.
	for _, trade := range trades {
		// Settlement is synchronous and runs inside the matching goroutine so
		// that the ledger is always consistent before the event is published.
		if err := e.settlement.Settle(trade); err != nil {
			// Phase 7 will emit a structured log here.
			_ = err
		}
		e.publishEvent(models.EventTrade, nil, trade)
	}
}

func (e *Engine) publishEvent(typ models.EventType, order *models.Order, trade *models.Trade) {
	seq := e.seq.Add(1)
	evt := &models.Event{
		Type:           typ,
		Symbol:         e.symbol,
		Market:         string(e.market),
		SequenceNumber: seq,
		Order:          order,
		Trade:          trade,
	}
	// Non-blocking: if the publisher is backed up, the event is dropped.
	// The publisher (events.Bus) handles backpressure with a buffered channel.
	e.pub.Publish(evt)
}
