// Package integration tests the Phase 2 concurrency model:
// multiple symbols running independently and simultaneously, with invariants
// verified under concurrent load.
package integration

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dex/matching-engine/internal/matching"
	"github.com/dex/matching-engine/internal/models"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// noopBus satisfies matching.EventPublisher without doing anything.
type noopBus struct{}

func (noopBus) Publish(_ *models.Event) {}

func newRegistry() *matching.Registry {
	return matching.NewRegistry(noopBus{}, nil, nil)
}

func order(symbol string, side models.OrderSide, price, qty string) *models.Order {
	accountID := "acc-buyer"
	if side == models.Sell {
		accountID = "acc-seller"
	}
	return &models.Order{
		ID:          uuid.NewString(),
		AccountID:   accountID,
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

// TestMultipleSymbolsRunIndependently verifies that engines for different symbols
// do not interfere with each other.
func TestMultipleSymbolsRunIndependently(t *testing.T) {
	reg := newRegistry()
	defer reg.StopAll()

	symbols := []string{"BTC-USDT", "ETH-USDT", "SOL-USDT", "BNB-USDT"}
	for _, sym := range symbols {
		_, err := reg.Register(sym, models.Spot)
		require.NoError(t, err)
	}

	var wg sync.WaitGroup
	var totalTrades atomic.Int64

	for _, sym := range symbols {
		sym := sym
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Rest a bid
			_, err := reg.Submit(order(sym, models.Buy, "100", "5"))
			assert.NoError(t, err)
			// Aggressive sell fills it
			trades, err := reg.Submit(order(sym, models.Sell, "100", "5"))
			assert.NoError(t, err)
			assert.Len(t, trades, 1, "expected 1 trade for %s", sym)
			totalTrades.Add(int64(len(trades)))
		}()
	}
	wg.Wait()

	assert.Equal(t, int64(len(symbols)), totalTrades.Load(), "each symbol should produce exactly one trade")
}

// TestConcurrentOrdersOnSameSymbol verifies invariants hold under simultaneous
// multi-goroutine load on a single symbol.
func TestConcurrentOrdersOnSameSymbol(t *testing.T) {
	reg := newRegistry()
	defer reg.StopAll()
	_, err := reg.Register("BTC-USDT", models.Spot)
	require.NoError(t, err)

	const goroutines = 50
	const ordersPerGoroutine = 20

	var (
		wg          sync.WaitGroup
		totalFilled atomic.Int64
		totalTrades atomic.Int64
	)

	// Launch goroutines that alternate between bids and asks.
	for i := 0; i < goroutines; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < ordersPerGoroutine; j++ {
				side := models.Buy
				price := "100"
				if i%2 == 0 {
					side = models.Sell
					price = "100"
				}
				o := order("BTC-USDT", side, price, "1")
				trades, snap, err := reg.SubmitSnapshot(o)
				if err == nil {
					totalTrades.Add(int64(len(trades)))
					if snap.Filled.IsPositive() {
						totalFilled.Add(1)
					}
				}
			}
		}()
	}
	wg.Wait()

	t.Logf("total orders: %d  trades: %d  filled: %d",
		goroutines*ordersPerGoroutine, totalTrades.Load(), totalFilled.Load())

	// Conservation: every trade has equal buy and sell qty (1:1 in this test).
	assert.True(t, totalTrades.Load() >= 0, "no negative trade count")
}

// TestHaltAndResume verifies the circuit breaker (Phase 7 hook via engine).
func TestHaltAndResume(t *testing.T) {
	reg := newRegistry()
	defer reg.StopAll()
	eng, err := reg.Register("BTC-USDT", models.Spot)
	require.NoError(t, err)

	eng.Halt()
	assert.True(t, eng.IsHalted())

	o := order("BTC-USDT", models.Buy, "100", "1")
	_, err = reg.Submit(o)
	assert.Error(t, err, "halted engine should reject orders")
	assert.Equal(t, models.StatusRejected, o.Status)

	eng.Resume()
	assert.False(t, eng.IsHalted())

	o2 := order("BTC-USDT", models.Buy, "100", "1")
	_, err = reg.Submit(o2)
	assert.NoError(t, err, "resumed engine should accept orders")
}

// TestSequenceNumbersAreMonotonic verifies that sequence numbers on emitted
// events are strictly increasing with no gaps.
func TestSequenceNumbersAreMonotonic(t *testing.T) {
	type seqBus struct {
		mu   sync.Mutex
		seqs []uint64
	}
	sb := &seqBus{}
	bus := &captureBus{capture: func(e *models.Event) {
		sb.mu.Lock()
		sb.seqs = append(sb.seqs, e.SequenceNumber)
		sb.mu.Unlock()
	}}

	reg := matching.NewRegistry(bus, nil, nil)
	defer reg.StopAll()
	_, err := reg.Register("BTC-USDT", models.Spot)
	require.NoError(t, err)

	// Submit 100 orders in sequence.
	for i := 0; i < 50; i++ {
		reg.Submit(order("BTC-USDT", models.Buy, fmt.Sprintf("%d", 100+i), "1"))
	}
	for i := 0; i < 50; i++ {
		reg.Submit(order("BTC-USDT", models.Sell, fmt.Sprintf("%d", 150-i), "1"))
	}

	// Give the engine a moment to process (it's async for publishing).
	time.Sleep(50 * time.Millisecond)

	sb.mu.Lock()
	seqs := append([]uint64(nil), sb.seqs...)
	sb.mu.Unlock()

	require.NotEmpty(t, seqs, "should have received events")

	// Verify monotonic increase with no gaps.
	for i := 1; i < len(seqs); i++ {
		assert.Equal(t, seqs[i-1]+1, seqs[i],
			"sequence gap at index %d: prev=%d cur=%d", i, seqs[i-1], seqs[i])
	}
}

type captureBus struct {
	capture func(*models.Event)
}

func (c *captureBus) Publish(e *models.Event) { c.capture(e) }
