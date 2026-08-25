// Package events provides the event bus and Kafka publisher that carry every
// order state change and trade from the matching goroutines to downstream
// consumers (Postgres writer, WebSocket broadcaster, analytics).
package events

import (
	"sync"
	"sync/atomic"

	"github.com/dex/matching-engine/internal/models"
)

// Bus is a blocking fan-out publisher. The matching goroutine calls Publish;
// each registered subscriber receives every event on its own buffered channel.
// Events are NEVER silently dropped: these events (fills, trades, cancels,
// liquidations, balance changes) carry per-symbol monotonic sequence numbers
// that downstream consumers (Postgres, WebSocket) rely on to be gapless. If a
// subscriber's channel is full, Publish blocks until the subscriber drains it,
// applying backpressure to matching rather than corrupting downstream state.
//
// Consumers MUST therefore keep up (drain promptly into their own durable
// buffer). A pathologically stuck consumer will back-pressure matching for its
// symbol — that is the intended failure mode: stall, don't desync.
type Bus struct {
	mu           sync.RWMutex
	subs         []chan *models.Event
	outOfBandSeq atomic.Uint64
}

// NewBus creates an empty Bus.
func NewBus() *Bus { return &Bus{} }

// outOfBandSeqBase separates out-of-band sequence numbers from real
// book-sequenced ones. persistence.events has a UNIQUE (symbol,
// sequence_number) index; book-sequenced order/trade events for a symbol
// start at 1 and increment per matching-goroutine event, so in any
// realistic run they stay many orders of magnitude below this base -
// guaranteeing NextOutOfBandSequence() never collides with a same-symbol
// book event, without needing to coordinate with each Engine's own counter.
const outOfBandSeqBase = uint64(1) << 62

// NextOutOfBandSequence returns a process-wide monotonically increasing
// number for events published outside a matching engine's own per-symbol
// sequence (funding payments, liquidations, realized PnL, and pre-book
// order rejections). These events don't participate in any single symbol's
// book-sequenced stream, so they can't reuse Engine's per-symbol counter,
// but still need *some* sequence_number that won't collide with a
// same-symbol sibling under the UNIQUE constraint above.
func (b *Bus) NextOutOfBandSequence() uint64 {
	return outOfBandSeqBase + b.outOfBandSeq.Add(1)
}

// Subscribe registers a consumer and returns its receive channel.
// bufSize controls how many events can be queued before Publish blocks.
func (b *Bus) Subscribe(bufSize int) <-chan *models.Event {
	ch := make(chan *models.Event, bufSize)
	b.mu.Lock()
	b.subs = append(b.subs, ch)
	b.mu.Unlock()
	return ch
}

// Publish sends evt to all subscribers, blocking on any full channel so that
// no event is ever dropped and sequence numbers stay gapless.
func (b *Bus) Publish(evt *models.Event) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, sub := range b.subs {
		sub <- evt
	}
}

// Close drains and closes all subscriber channels.
func (b *Bus) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, sub := range b.subs {
		close(sub)
	}
	b.subs = nil
}
