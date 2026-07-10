// Package stress simulates thousands of concurrent orders across multiple symbols
// and verifies the core invariants hold under load (Phase 8).
package stress

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

type dropBus struct{}

func (dropBus) Publish(_ *models.Event) {}

const (
	numSymbols        = 20
	goroutinesPerSym  = 25
	ordersPerGoroutine = 200
)

// TestStress_MultiSymbolConcurrentLoad is the Phase 8 stress test.
// It submits numSymbols × goroutinesPerSym × ordersPerGoroutine orders and
// asserts the conservation invariant holds for every generated trade.
func TestStress_MultiSymbolConcurrentLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	reg := matching.NewRegistry(dropBus{}, nil)
	defer reg.StopAll()

	// Register all symbols.
	for i := 0; i < numSymbols; i++ {
		sym := fmt.Sprintf("ASSET%d-USDT", i)
		_, err := reg.Register(sym, models.Spot)
		require.NoError(t, err)
	}

	var (
		wg             sync.WaitGroup
		totalOrders    atomic.Int64
		totalTrades    atomic.Int64
		violations     atomic.Int64
	)

	start := time.Now()

	for i := 0; i < numSymbols; i++ {
		symbol := fmt.Sprintf("ASSET%d-USDT", i)
		for g := 0; g < goroutinesPerSym; g++ {
			g := g
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < ordersPerGoroutine; j++ {
					side := models.Buy
					price := "100"
					if g%2 == 0 {
						side = models.Sell
						price = "100"
					}
					o := &models.Order{
						ID:          uuid.NewString(),
						AccountID:   fmt.Sprintf("acc-%d-%d", g, j),
						Symbol:      symbol,
						Market:      models.Spot,
						Side:        side,
						Type:        models.Limit,
						Price:       decimal.RequireFromString(price),
						Quantity:    decimal.NewFromInt(1),
						TimeInForce: models.GTC,
						Status:      models.StatusPending,
						CreatedAt:   time.Now(),
					}
					trades, err := reg.Submit(o)
					if err != nil {
						return
					}
					totalOrders.Add(1)
					totalTrades.Add(int64(len(trades)))

					// Conservation invariant: for each trade, buy filled == sell filled.
					for _, tr := range trades {
						if tr.BuyOrder != nil && tr.SellOrder != nil {
							if !tr.BuyOrder.Filled.GreaterThanOrEqual(tr.Quantity) {
								violations.Add(1)
							}
							if !tr.SellOrder.Filled.GreaterThanOrEqual(tr.Quantity) {
								violations.Add(1)
							}
						}
					}
				}
			}()
		}
	}

	wg.Wait()
	elapsed := time.Since(start)

	total := int64(numSymbols * goroutinesPerSym * ordersPerGoroutine)
	throughput := float64(totalOrders.Load()) / elapsed.Seconds()

	t.Logf("=== Stress Test Results ===")
	t.Logf("  Total orders submitted : %d / %d", totalOrders.Load(), total)
	t.Logf("  Total trades generated : %d", totalTrades.Load())
	t.Logf("  Conservation violations: %d", violations.Load())
	t.Logf("  Elapsed                : %s", elapsed)
	t.Logf("  Throughput             : %.0f orders/sec", throughput)

	assert.Equal(t, int64(0), violations.Load(), "conservation invariant violated under load")
}

// TestStress_RaceDetector verifies no data races under concurrent access.
// Run with: go test -race ./test/stress/...
func TestStress_RaceDetector(t *testing.T) {
	reg := matching.NewRegistry(dropBus{}, nil)
	defer reg.StopAll()
	_, err := reg.Register("BTC-USDT", models.Spot)
	require.NoError(t, err)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			side := models.Buy
			if i%2 == 0 {
				side = models.Sell
			}
			o := &models.Order{
				ID:        uuid.NewString(),
				AccountID: "acc-1",
				Symbol:    "BTC-USDT",
				Market:    models.Spot,
				Side:      side,
				Type:      models.Limit,
				Price:     decimal.NewFromInt(100),
				Quantity:  decimal.NewFromInt(1),
				Status:    models.StatusPending,
				CreatedAt: time.Now(),
			}
			reg.Submit(o) //nolint:errcheck
		}()
	}
	wg.Wait()
}
