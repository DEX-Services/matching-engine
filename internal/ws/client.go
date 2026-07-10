package ws

import (
	"time"

	"github.com/gorilla/websocket"
)

const (
	writeWait  = 10 * time.Second
	pongWait   = 60 * time.Second
	pingPeriod = (pongWait * 9) / 10
)

// client represents a single WebSocket connection.
type client struct {
	conn   *websocket.Conn
	sendCh chan []byte
}

// send enqueues a message for the client non-blocking. Drops on overflow.
func (c *client) send(msg []byte) {
	select {
	case c.sendCh <- msg:
	default:
		// Client is too slow; drop the message.
		// Phase 7 will add a dropped-messages counter here.
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
