// Package events provides the event bus and Kafka publisher that carry every
// order state change and trade from the matching goroutines to downstream
// consumers (Postgres writer, WebSocket broadcaster, analytics).
package events

import (
	"sync"

	"github.com/dex/matching-engine/internal/models"
)

// Bus is a non-blocking fan-out publisher. The matching goroutine calls
// Publish; each registered subscriber receives events on its own buffered
// channel. If a subscriber's channel is full the event is dropped for that
// subscriber — the matching goroutine must never block.
type Bus struct {
	mu   sync.RWMutex
	subs []chan *models.Event
}

// NewBus creates an empty Bus.
func NewBus() *Bus { return &Bus{} }

// Subscribe registers a consumer and returns its receive channel.
// bufSize controls how many events can be queued before drops occur.
func (b *Bus) Subscribe(bufSize int) <-chan *models.Event {
	ch := make(chan *models.Event, bufSize)
	b.mu.Lock()
	b.subs = append(b.subs, ch)
	b.mu.Unlock()
	return ch
}

// Publish sends evt to all subscribers without blocking.
func (b *Bus) Publish(evt *models.Event) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, sub := range b.subs {
		select {
		case sub <- evt:
		default:
			// Slow subscriber — drop; Phase 7 adds a metric counter here.
		}
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
