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
		rm, gen := h.leave(c)
		c.conn.Close(websocket.StatusNormalClosure, "")
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
		case "prepare":
			var p struct {
				Gen string `json:"gen"`
			}
			_ = json.Unmarshal(data, &p)
			h.startRound(r, p.Gen)
		case "ready":
			var p struct {
				Gen string `json:"gen"`
			}
			_ = json.Unmarshal(data, &p)
			h.markReady(c, r, p.Gen)
			continue
		case "state", "queue":
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
