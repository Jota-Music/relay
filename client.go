package main

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

const (
	// idleTimeout drops a member whose app-level ping stopped arriving. The
	// client pings every second, so this only catches dead connections.
	idleTimeout = 30 * time.Second
	// sendBuffer bounds the per-client write queue; a slower consumer than this
	// is dropped instead of stalling the room.
	sendBuffer = 64
)

var (
	errClosed       = errors.New("client closed")
	errSlowConsumer = errors.New("slow consumer")
)

// client owns one websocket and a writer goroutine, so a room broadcast never
// blocks on a slow peer.
type client struct {
	conn     *websocket.Conn
	role     string
	room     *room
	out      chan []byte
	done     chan struct{}
	once     sync.Once
	lastSeen atomic.Int64
	inFlight atomic.Int64
}

func newClient(conn *websocket.Conn, role string) *client {
	c := &client{
		conn: conn,
		role: role,
		out:  make(chan []byte, sendBuffer),
		done: make(chan struct{}),
	}
	c.touch()
	return c
}

// run starts the writer goroutine. Call it only after join succeeded, so a
// rejected connection can write its error without a concurrent writer.
func (c *client) run() {
	go c.writeLoop()
}

func (c *client) touch() {
	c.lastSeen.Store(time.Now().UnixNano())
}

func (c *client) seen() time.Time {
	return time.Unix(0, c.lastSeen.Load())
}

// send enqueues a frame, never blocking the caller. A full queue means the peer
// is too slow, so it is dropped.
func (c *client) send(data []byte) error {
	select {
	case <-c.done:
		return errClosed
	default:
	}
	c.inFlight.Add(1)
	select {
	case c.out <- data:
		return nil
	case <-c.done:
		c.inFlight.Add(-1)
		return errClosed
	default:
		c.inFlight.Add(-1)
		c.close()
		return errSlowConsumer
	}
}

func (c *client) writeLoop() {
	for {
		select {
		case <-c.done:
			return
		case msg := <-c.out:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := c.conn.Write(ctx, websocket.MessageText, msg)
			cancel()
			c.inFlight.Add(-1)
			if err != nil {
				c.close()
				return
			}
		}
	}
}

func (c *client) close() {
	c.once.Do(func() {
		close(c.done)
		_ = c.conn.Close(websocket.StatusNormalClosure, "")
	})
}

// closeAfterFlush waits until every enqueued frame has been written before
// closing, so a final notice (like "host left") is not lost to the close.
func (c *client) closeAfterFlush(timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for c.inFlight.Load() > 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	c.close()
}

func (h *hub) readLoop(c *client, r *room) {
	defer func() {
		rm, gen := h.leave(c)
		c.close()
		if rm != nil {
			h.sendMembers(rm)
			if gen != "" {
				h.broadcastPlay(rm, gen)
			}
		}
	}()

	for {
		_, data, err := c.conn.Read(context.Background())
		if err != nil {
			return
		}
		c.touch()

		var head struct {
			T string `json:"t"`
		}
		if json.Unmarshal(data, &head) != nil {
			continue
		}

		switch head.T {
		case "ping":
			var p struct {
				ID int   `json:"id"`
				At int64 `json:"at"`
			}
			_ = json.Unmarshal(data, &p)
			pong, _ := json.Marshal(map[string]any{
				"t":    "pong",
				"id":   p.ID,
				"at":   p.At,
				"echo": time.Now().UnixMilli(),
			})
			_ = c.send(pong)
			continue
		case "ready":
			// Any member answers a round; a missing flag means it loaded.
			var p struct {
				Gen string `json:"gen"`
				OK  *bool  `json:"ok"`
			}
			_ = json.Unmarshal(data, &p)
			h.markReady(c, r, p.Gen, p.OK == nil || *p.OK)
			continue
		case "prepare":
			if c.role != roleHost {
				continue
			}
			var p struct {
				Gen string `json:"gen"`
			}
			_ = json.Unmarshal(data, &p)
			h.startRound(r, p.Gen)
		case "state", "queue", "heartbeat":
			if c.role != roleHost {
				continue
			}
			h.remember(r, head.T, data)
		}

		h.mu.Lock()
		targets := r.others(c)
		h.mu.Unlock()
		for _, t := range targets {
			_ = t.send(data)
		}
	}
}
