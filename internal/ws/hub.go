// Package ws implements the WebSocket broadcast hub. It reads events from the
// event bus and pushes them to all connected WebSocket clients. The hub runs
// in its own goroutine and never touches the matching goroutines directly.
package ws

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/dex/matching-engine/internal/models"
	"github.com/gorilla/websocket"
)

// loadAllowedOrigins reads WS_ALLOWED_ORIGINS from the environment lazily.
// It MUST be called after godotenv.Load() (i.e. from main, not package init),
// otherwise values defined in .env would not be visible yet.
func loadAllowedOrigins() map[string]bool {
	m := map[string]bool{}
	for _, o := range strings.Split(os.Getenv("WS_ALLOWED_ORIGINS"), ",") {
		if o = strings.TrimSpace(o); o != "" {
			m[o] = true
		}
	}
	return m
}

var allowedOriginsOnce sync.Once
var allowedOrigins map[string]bool

// origins returns the allowlist, building it on first use (after godotenv has
// loaded .env). When empty, only same-origin requests are accepted so
// cross-site WebSocket hijacking is not possible by default.
func origins() map[string]bool {
	allowedOriginsOnce.Do(func() { allowedOrigins = loadAllowedOrigins() })
	return allowedOrigins
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	// Permessage-deflate: the event stream is extremely repetitive JSON
	// (same field names, symbols, and ladder prices within a few bps), so
	// negotiated compression shrinks the broadcast payload roughly 5-10x.
	// Servers compress each message independently (no context takeover);
	// browsers negotiate the extension automatically, so no client change
	// is needed. CPU cost is negligible at this frame rate.
	EnableCompression: true,
	CheckOrigin: func(r *http.Request) bool {
		allowed := origins()
		if len(allowed) == 0 {
			// No allowlist configured: only accept same-origin upgrades.
			host := r.Host
			origin := r.Header.Get("Origin")
			if origin == "" {
				return true // non-browser clients have no Origin header
			}
			return strings.HasPrefix(origin, "http://"+host) || strings.HasPrefix(origin, "https://"+host)
		}
		return allowed[r.Header.Get("Origin")]
	},
}

// Hub manages all active WebSocket connections and broadcasts events to them.
type Hub struct {
	mu      sync.RWMutex
	clients map[*client]struct{}
	eventCh <-chan *models.Event
	log     *slog.Logger
}

// NewHub creates a Hub that reads events from eventCh.
func NewHub(eventCh <-chan *models.Event) *Hub {
	return &Hub{
		clients: make(map[*client]struct{}),
		eventCh: eventCh,
		log:     slog.Default(),
	}
}

// Run starts the broadcast loop. Call in a dedicated goroutine.
func (h *Hub) Run() {
	for evt := range h.eventCh {
		h.broadcast(evt)
	}
}

// BroadcastJSON sends a raw payload to every connected client. It is for
// aggregated market-data frames (e.g. the 1s TICKER snapshot) that do NOT
// originate from the event bus — deliberately so: bus-driven broadcasts are
// persisted downstream (Postgres writer, Kafka publisher, attached-order
// listener) and carry gapless per-symbol sequence numbers, neither of which
// applies to a periodic UI snapshot.
func (h *Hub) BroadcastJSON(payload []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.clients {
		c.send(payload)
	}
}

// ClientCount returns the number of connected WebSocket clients.
func (h *Hub) ClientCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

// ServeWS upgrades an HTTP connection to WebSocket and registers the client.
func (h *Hub) ServeWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.log.Error("ws upgrade failed", "error", err)
		return
	}
	c := newClient(conn)
	h.register(c)
	go c.writePump()
	go h.readPump(c)
}

// broadcast serialises evt and sends it to every interested client
// non-blocking. A client that has declared stream subscriptions (see
// readPump) only receives events for its subscribed "symbol|market" streams;
// clients with no subscriptions get the legacy full broadcast.
func (h *Hub) broadcast(evt *models.Event) {
	payload, err := json.Marshal(evt)
	if err != nil {
		return
	}
	view := &eventView{streamKey: evt.Symbol + "|" + evt.Market}
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.clients {
		if !c.wantsEvent(view) {
			continue
		}
		c.send(payload)
	}
}

func (h *Hub) register(c *client) {
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()
}

func (h *Hub) unregister(c *client) {
	h.mu.Lock()
	delete(h.clients, c)
	h.mu.Unlock()
	close(c.sendCh)
}

// wsInboundMessage is the (tiny) client→server control protocol. Clients may
// subscribe to specific "symbol|market" streams so the hub stops fanning out
// all markets' churn to every connection — per-user bandwidth becomes O(active
// markets) instead of O(all markets).
type wsInboundMessage struct {
	// "subscribe" or "unsubscribe".
	Action string `json:"action"`
	// Streams in the same "symbol|market" form the frontend uses for its
	// per-stream sequence tracking. Unknown streams are harmless (they just
	// never match an event).
	Streams []string `json:"streams"`
}

// readPump consumes inbound frames. Two duties: keep the pong/read-deadline
// machinery alive (the original purpose), and apply subscription control
// frames. Anything else is ignored.
func (h *Hub) readPump(c *client) {
	defer func() {
		h.unregister(c)
		c.conn.Close()
	}()
	c.conn.SetReadLimit(512)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})
	for {
		msgType, msg, err := c.conn.ReadMessage()
		if err != nil {
			break
		}
		if msgType != websocket.TextMessage || len(msg) == 0 {
			continue
		}
		var m wsInboundMessage
		if json.Unmarshal(msg, &m) != nil {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(m.Action)) {
		case "subscribe":
			if keys := normalizeStreams(m.Streams); len(keys) > 0 {
				c.subscribeSymbols(keys)
			}
		case "unsubscribe":
			if keys := normalizeStreams(m.Streams); len(keys) > 0 {
				c.unsubscribeSymbols(keys)
			}
		}
	}
}

// normalizeStreams trims and lowercases stream keys (symbols/markets are
// upper-case in events; accepting case-insensitively makes client typos
// harmless in the direction of MORE data, never less).
func normalizeStreams(in []string) []string {
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s = strings.ToUpper(strings.TrimSpace(s)); s != "" {
			out = append(out, s)
		}
	}
	return out
}
