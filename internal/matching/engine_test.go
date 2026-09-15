package matching

import (
	"sync"
	"testing"
	"time"

	"github.com/dex/matching-engine/internal/models"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

// capturingBus records every published event's sequence number, in order,
// for direct assertion (unlike noopBus elsewhere, which discards them).
type capturingBus struct {
	seqs chan uint64
}

func newCapturingBus() *capturingBus {
	return &capturingBus{seqs: make(chan uint64, 64)}
}

func (b *capturingBus) Publish(e *models.Event) {
	b.seqs <- e.SequenceNumber
}

func testOrder(symbol string, side models.OrderSide, price, qty string) *models.Order {
	return &models.Order{
		ID:          uuid.NewString(),
		AccountID:   "acc-test",
		Symbol:      symbol,
		Market:      models.Spot,
		Side:        side,
		Type:        models.Limit,
		Price:       decimal.RequireFromString(price),
		Quantity:    decimal.RequireFromString(qty),
		TimeInForce: models.GTC,
		Status:      models.StatusPending,
		CreatedAt:   time.Now(),
	}
}

// TestNewEngineResumesFromStartSeq is a regression test for
// SEQUENCE-RESET-HISTORY-LOSS-BUG.md: an engine constructed with a non-zero
// startSeq (as cmd/engine/main.go now does, via newEngineSeqLookup restoring
// the last persisted sequence_number for the symbol) must number its first
// published event startSeq+1, never startSeq or lower — a collision with
// any sequence number already used and persisted before a restart is
// exactly the bug that let the Kafka→Postgres writer silently drop an
// order's history (see internal/persistence/writer.go's applyEvent).
func TestNewEngineResumesFromStartSeq(t *testing.T) {
	const startSeq = uint64(12628) // an arbitrary "already persisted up to here" value
	bus := newCapturingBus()
	eng := NewEngine("BI2X-BI2XUSD", models.Spot, bus, nil, nil, startSeq)
	defer eng.Stop()

	// A single resting LIMIT order with nothing to match against still
	// publishes an EventOrderOpen — enough to observe the first sequence
	// number this engine instance actually assigns.
	_, err := eng.Submit(testOrder("BI2X-BI2XUSD", models.Buy, "8.70", "1"))
	require.NoError(t, err)

	select {
	case seq := <-bus.seqs:
		require.Equal(t, startSeq+1, seq, "first event after a restored startSeq must be startSeq+1, not restart at 1 — a lower or equal value would collide with a pre-restart sequence_number already persisted for this symbol")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the engine to publish an event")
	}
}

// TestNewEngineDefaultsToZeroStartSeq confirms the zero-value / unset case
// (Postgres disabled, or any existing caller that hasn't been updated to
// pass a real startSeq) behaves exactly as before this change: sequence
// numbers start at 1, same as the pre-fix zero-initialized atomic.Uint64.
func TestNewEngineDefaultsToZeroStartSeq(t *testing.T) {
	bus := newCapturingBus()
	eng := NewEngine("BI2X-BI2XUSD", models.Spot, bus, nil, nil, 0)
	defer eng.Stop()

	_, err := eng.Submit(testOrder("BI2X-BI2XUSD", models.Buy, "8.70", "1"))
	require.NoError(t, err)

	select {
	case seq := <-bus.seqs:
		require.Equal(t, uint64(1), seq)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the engine to publish an event")
	}
}

// countingRelease is a ReleaseFunc that records every order it was called
// with, safe for concurrent use — used below to prove release happens
// exactly once per cancelled order no matter how many concurrent Cancel
// calls raced for it.
type countingRelease struct {
	mu    sync.Mutex
	calls []string // order IDs release was invoked for, in call order
}

func (c *countingRelease) release(o *models.Order) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, o.ID)
}

func (c *countingRelease) countFor(orderID string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, id := range c.calls {
		if id == orderID {
			n++
		}
	}
	return n
}

// TestCancel_ReleasesExactlyOnceUnderConcurrentDuplicateCancels is a
// regression test for a live incident: a resting SPOT order vanished from
// the book (no fill, no error logged anywhere) while its reservation stayed
// locked forever. Root cause: the release call used to happen in the HTTP
// handler (cmd/engine/main.go's /cancel), as a THIRD, separate round-trip
// into this same single-threaded engine goroutine, after a first
// existence-check round-trip and a second Cancel round-trip — a genuine
// check-then-act race window in which a duplicate/retried cancel (or a
// fill, or an STP-cancel) for the same order could interleave. Fixed by
// moving the release into the SAME atomic step that removes the order from
// the book (the reqCancel case in Engine.handle), exactly like
// postProcessAndCancel's e.release(c) call for STP-cancelled orders.
//
// This test fires many concurrent Cancel calls for the same resting order
// (simulating a duplicate/retried cancel racing itself) and asserts: exactly
// one succeeds, release is invoked for that order exactly once (not zero,
// not more than once), and every other call gets a clean "already
// gone"-shaped error rather than silently doing nothing.
func TestCancel_ReleasesExactlyOnceUnderConcurrentDuplicateCancels(t *testing.T) {
	bus := newCapturingBus()
	rel := &countingRelease{}
	eng := NewEngine("BI2X-BI2XUSD", models.Spot, bus, nil, rel.release, 0)
	defer eng.Stop()

	order := testOrder("BI2X-BI2XUSD", models.Buy, "8.70", "1")
	_, err := eng.Submit(order)
	require.NoError(t, err)

	const concurrentCancels = 20
	var wg sync.WaitGroup
	successes := make(chan *models.Order, concurrentCancels)
	errs := make(chan error, concurrentCancels)
	wg.Add(concurrentCancels)
	for i := 0; i < concurrentCancels; i++ {
		go func() {
			defer wg.Done()
			o, err := eng.Cancel(order.ID)
			if err != nil {
				errs <- err
				return
			}
			successes <- o
		}()
	}
	wg.Wait()
	close(successes)
	close(errs)

	successCount := 0
	for range successes {
		successCount++
	}
	errCount := 0
	for range errs {
		errCount++
	}
	require.Equal(t, 1, successCount, "exactly one of the concurrent duplicate cancels should succeed")
	require.Equal(t, concurrentCancels-1, errCount, "every other duplicate cancel should cleanly fail, not silently succeed or hang")
	require.Equal(t, 1, rel.countFor(order.ID), "release must be invoked exactly once for the order regardless of how many duplicate cancels raced for it — either 0 (never released, funds stuck) or >1 (over-released, stealing a different order's reservation) would both be the bug this test guards against")
}

// TestEngine_CheckMarkPriceTriggers_FiresThroughTheGoroutine is an
// integration-level check that Engine.CheckMarkPriceTriggers (the new
// reqCheckMarkPriceTriggers request kind, added 2026-09-16 alongside
// orderbook.Book.CheckMarkPriceTriggers) correctly routes through the
// engine's single-threaded request/response plumbing and publishes the
// triggered order's resulting event — not just that the underlying Book
// method itself works (covered directly in internal/orderbook's tests).
func TestEngine_CheckMarkPriceTriggers_FiresThroughTheGoroutine(t *testing.T) {
	bus := newCapturingBus()
	eng := NewEngine("BI2X-BI2XUSD", models.Futures, bus, nil, nil, 0)
	defer eng.Stop()

	stop := testOrder("BI2X-BI2XUSD", models.Buy, "0", "1")
	stop.Market = models.Futures
	stop.AccountID = "buyer-acct" // distinct from the resting ask below: same account would self-trade-prevent and cancel instead of fill
	stop.Type = models.Stop
	stop.StopPrice = decimal.RequireFromString("100")
	_, err := eng.Submit(stop)
	require.NoError(t, err)
	// Drain the OPEN event for the resting stop itself before asserting on
	// what CheckMarkPriceTriggers publishes below.
	select {
	case <-bus.seqs:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the resting stop order's OPEN event")
	}

	// Rest an ask for the triggered stop-buy to match against.
	ask := testOrder("BI2X-BI2XUSD", models.Sell, "101", "1")
	ask.Market = models.Futures
	ask.AccountID = "seller-acct"
	_, err = eng.Submit(ask)
	require.NoError(t, err)
	select {
	case <-bus.seqs: // the resting ask's own OPEN event
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the resting ask order's OPEN event")
	}

	// Mark price at/above trigger: CheckMarkPriceTriggers should fire the
	// stop and return the resulting trade, with NO trade ever having
	// occurred on this book (lastTradePrice is still its zero value) —
	// proving this really goes through the mark-price path, not
	// processStopTriggers/lastTradePrice.
	trades := eng.CheckMarkPriceTriggers(decimal.RequireFromString("100"))
	require.Len(t, trades, 1, "mark price crossing the stop's trigger should produce exactly one trade")

	// The engine must have published at least one resulting event for the
	// activated stop — confirms handle()'s reqCheckMarkPriceTriggers case
	// correctly drains and publishes activations rather than silently
	// mutating the book with no visible event at all.
	select {
	case <-bus.seqs:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a post-trigger event; the activated stop's fill should have been published")
	}
}
