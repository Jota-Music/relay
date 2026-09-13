package main

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/coder/websocket"
)

type client struct {
	conn *websocket.Conn
	role string
	room *room
	mu   sync.Mutex
}

func (c *client) send(data []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn.Write(ctx, websocket.MessageText, data)
}

func (h *hub) readLoop(c *client, r *room) {
	defer func() {
		rm := h.leave(c)
		c.conn.Close(websocket.StatusNormalClosure, "")
		if rm != nil {
			h.sendMembers(rm)
		}
	}()

	for {
		_, data, err := c.conn.Read(context.Background())
		if err != nil {
			return
		}

		var head struct {
			T string `json:"t"`
		}
		if json.Unmarshal(data, &head) != nil {
			continue
		}

		if head.T == "ping" {
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
		}

		if head.T == "state" || head.T == "queue" {
			h.mu.Lock()
			if h.rooms[r.code] == r {
				r.cache[head.T] = append(json.RawMessage(nil), data...)
			}
			h.mu.Unlock()
		}

		h.mu.Lock()
		targets := r.others(c)
		h.mu.Unlock()
		for _, t := range targets {
			_ = t.send(data)
		}
	}
}
