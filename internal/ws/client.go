package ws

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

const (
	writeWait  = 10 * time.Second
	pongWait   = 60 * time.Second
	pingPeriod = (pongWait * 9) / 10
)

// droppedMessages counts messages that could not be enqueued to a client
// before it was disconnected. Exposed for metrics/observability.
var droppedMessages atomic.Uint64

// DroppedMessages returns the total number of messages dropped across all
// clients (each drop also forcibly disconnects the offending client).
func DroppedMessages() uint64 { return droppedMessages.Load() }

// client represents a single WebSocket connection.
type client struct {
	conn     *websocket.Conn
	sendCh   chan []byte
	overflow atomic.Bool

	// mu guards the subscription set below. The hub's broadcast loops iterate
	// clients under the hub lock; per-client subscription changes arrive from
	// this client's own readPump, so a dedicated lock per connection keeps
	// that contention-free and avoids lock-ordering questions entirely.
	mu sync.RWMutex
	// Symbol streams this connection explicitly subscribed to. Inbound
	// subscribe/unsubscribe frames (subscribeSymbols/unsubscribeSymbols)
	// mutate it; filtering in the hub reads it. An empty set means "no
	// filter applied" — the legacy full-broadcast behavior — so clients
	// that never send subscription frames (older frontends, monitoring
	// tools, tests) keep receiving everything and nothing breaks.
	wantStreams map[string]struct{}
}

func newClient(conn *websocket.Conn) *client {
	return &client{conn: conn, sendCh: make(chan []byte, 512), wantStreams: make(map[string]struct{})}
}

// subscribeSymbols registers interest in additional "symbol|market" streams.
func (c *client) subscribeSymbols(keys []string) {
	c.mu.Lock()
	for _, k := range keys {
		c.wantStreams[k] = struct{}{}
	}
	c.mu.Unlock()
}

// unsubscribeSymbols removes interest in "symbol|market" streams.
func (c *client) unsubscribeSymbols(keys []string) {
	c.mu.Lock()
	for _, k := range keys {
		delete(c.wantStreams, k)
	}
	c.mu.Unlock()
}

// wantsEvent reports whether this client should receive evt. It is a filter
// only — every client still receives everything until it declares at least
// one stream. The stream key matches the frontend's per-stream sequence
// tracking ("symbol|market"), so one subscription covers both the order and
// trade events of that market.
func (c *client) wantsEvent(evt *eventView) bool {
	c.mu.RLock()
	if len(c.wantStreams) == 0 {
		c.mu.RUnlock()
		return true // unfiltered connection: legacy full broadcast
	}
	_, ok := c.wantStreams[evt.streamKey]
	c.mu.RUnlock()
	return ok
}

// eventView is the minimal projection the filtering path needs from a bus
// event: its stream key. Declared here (rather than taking *models.Event)
// so filtering and broadcasting stay decoupled from the event type.
type eventView struct {
	streamKey string
}

// send enqueues a message for the client. If the client's buffer is full the
// client is forcibly disconnected rather than silently skipping events: a
// client that misses events has a gapped view of orders/trades and MUST
// resynchronize with a fresh snapshot on reconnect. Disconnecting makes the
// gap explicit instead of silent.
func (c *client) send(msg []byte) {
	if c.overflow.Load() {
		return // already being torn down
	}
	select {
	case c.sendCh <- msg:
	default:
		if c.overflow.CompareAndSwap(false, true) {
			droppedMessages.Add(1)
			// Force-close the connection; readPump exits and unregisters.
			c.conn.Close()
		}
	}
}

// writePump reads from sendCh and writes to the WebSocket connection.
func (c *client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()
	for {
		select {
		case msg, ok := <-c.sendCh:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
