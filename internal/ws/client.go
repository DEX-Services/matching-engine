package ws

import (
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
