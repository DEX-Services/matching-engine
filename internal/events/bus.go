// Package events provides the event bus and Kafka publisher that carry every
// order state change and trade from the matching goroutines to downstream
// consumers (Postgres writer, WebSocket broadcaster, analytics).
package events

import (
	"sync"

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
	mu   sync.RWMutex
	subs []chan *models.Event
}

// NewBus creates an empty Bus.
func NewBus() *Bus { return &Bus{} }

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
