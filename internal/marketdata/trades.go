package marketdata

import (
	"sync"

	"github.com/dex/matching-engine/internal/models"
)

// TradeHistory is a bounded, in-memory ring buffer of recent trades per
// symbol/market, fed by subscribing to the event bus. It is read-only from
// the HTTP layer's perspective and never touches the matching goroutines.
type TradeHistory struct {
	mu      sync.RWMutex
	maxLen  int
	buffers map[string][]*models.Trade // key: symbol+":"+market, newest first
}

// NewTradeHistory creates a TradeHistory retaining up to maxLen trades per
// symbol/market key.
func NewTradeHistory(maxLen int) *TradeHistory {
	return &TradeHistory{maxLen: maxLen, buffers: make(map[string][]*models.Trade)}
}

// Run consumes events from ch until it is closed. Call in its own goroutine.
func (t *TradeHistory) Run(ch <-chan *models.Event) {
	for evt := range ch {
		if evt.Type != models.EventTrade || evt.Trade == nil {
			continue
		}
		key := evt.Symbol + ":" + evt.Market
		t.mu.Lock()
		buf := append([]*models.Trade{evt.Trade}, t.buffers[key]...)
		if len(buf) > t.maxLen {
			buf = buf[:t.maxLen]
		}
		t.buffers[key] = buf
		t.mu.Unlock()
	}
}

// Recent returns up to `limit` most recent trades for symbol/market, newest first.
func (t *TradeHistory) Recent(symbol, market string, limit int) []*models.Trade {
	t.mu.RLock()
	defer t.mu.RUnlock()
	buf := t.buffers[symbol+":"+market]
	if limit <= 0 || limit > len(buf) {
		limit = len(buf)
	}
	out := make([]*models.Trade, limit)
	copy(out, buf[:limit])
	return out
}
