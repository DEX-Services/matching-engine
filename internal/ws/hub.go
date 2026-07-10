// Package ws implements the WebSocket broadcast hub. It reads events from the
// event bus and pushes them to all connected WebSocket clients. The hub runs
// in its own goroutine and never touches the matching goroutines directly.
package ws

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/dex/matching-engine/internal/models"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	// Phase 7: replace with proper origin allowlist.
	CheckOrigin: func(r *http.Request) bool { return true },
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

// ServeWS upgrades an HTTP connection to WebSocket and registers the client.
func (h *Hub) ServeWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.log.Error("ws upgrade failed", "error", err)
		return
	}
	c := &client{conn: conn, sendCh: make(chan []byte, 512)}
	h.register(c)
	go c.writePump()
	go h.readPump(c)
}

// broadcast serialises evt and sends it to all connected clients non-blocking.
func (h *Hub) broadcast(evt *models.Event) {
	payload, err := json.Marshal(evt)
	if err != nil {
		return
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.clients {
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
		if _, _, err := c.conn.ReadMessage(); err != nil {
			break
		}
	}
}
