// Package ws implements the WebSocket broadcast hub. It reads events from the
// event bus and pushes them to all connected WebSocket clients. The hub runs
// in its own goroutine and never touches the matching goroutines directly.
package ws

import (
	"crypto/subtle"
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

// wsInternalSecretOnce/wsInternalSecret cache WS_INTERNAL_SECRET, read lazily
// for the same godotenv-ordering reason as loadAllowedOrigins above.
var wsInternalSecretOnce sync.Once
var wsInternalSecret string

func internalSecret() string {
	wsInternalSecretOnce.Do(func() { wsInternalSecret = os.Getenv("WS_INTERNAL_SECRET") })
	return wsInternalSecret
}

// hasValidInternalSecret reports whether the request carries the correct
// X-Engine-Ws-Secret header. This is a SEPARATE mechanism from the browser
// Origin allowlist below, deliberately: browser CORS answers "is this a
// trusted website," which is meaningless for a server-to-server caller (the
// bots service) that has no Origin header and isn't a browser at all —
// conflating the two would mean either loosening the browser allowlist to
// admit non-browser traffic, or the bots service impersonating a browser
// Origin it doesn't have. A shared secret is the right primitive for "is
// this our own other backend service," the same way DEX_BACKEND_ENGINE_SECRET
// already authenticates Dex-Backend's calls elsewhere in this codebase — kept
// as its own env var rather than reusing that one so the two trust
// boundaries (ledger-sync callers vs. WS-stream callers) stay independently
// rotatable.
func hasValidInternalSecret(r *http.Request) bool {
	secret := internalSecret()
	if secret == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Engine-Ws-Secret")), []byte(secret)) == 1
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin: func(r *http.Request) bool {
		if hasValidInternalSecret(r) {
			return true
		}
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
