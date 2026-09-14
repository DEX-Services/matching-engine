package matching

import (
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
