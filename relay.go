package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
)

const (
	roleHost   = "host"
	roleGuest  = "guest"
	maxCodeLen = 64
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

type room struct {
	code   string
	host   *client
	guests map[*client]struct{}
	cache  map[string]json.RawMessage
	timer  *time.Timer
}

func newRoom(code string) *room {
	return &room{
		code:   code,
		guests: make(map[*client]struct{}),
		cache:  make(map[string]json.RawMessage),
	}
}

func (r *room) members() int {
	n := len(r.guests)
	if r.host != nil {
		n++
	}
	return n
}

// others returns every member except of. Pass nil to get all members.
func (r *room) others(of *client) []*client {
	out := make([]*client, 0, len(r.guests)+1)
	if r.host != nil && r.host != of {
		out = append(out, r.host)
	}
	for g := range r.guests {
		if g != of {
			out = append(out, g)
		}
	}
	return out
}

type hub struct {
	mu        sync.Mutex
	rooms     map[string]*room
	maxGuests int
	ttl       time.Duration
}

func newHub(maxGuests int, ttl time.Duration) *hub {
	return &hub{rooms: make(map[string]*room), maxGuests: maxGuests, ttl: ttl}
}

func (h *hub) join(code, role string, c *client) (*room, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	r := h.rooms[code]
	if r == nil {
		r = newRoom(code)
		h.rooms[code] = r
	}

	if role == roleHost {
		if r.host != nil {
			return nil, errors.New("room already has a host")
		}
		r.host = c
		if r.timer != nil {
			r.timer.Stop()
			r.timer = nil
		}
	} else {
		if len(r.guests) >= h.maxGuests {
			return nil, errors.New("room is full")
		}
		r.guests[c] = struct{}{}
	}
	c.room = r
	return r, nil
}

// leave removes c from its room and returns the room if it still lives.
func (h *hub) leave(c *client) *room {
	h.mu.Lock()
	defer h.mu.Unlock()

	r := c.room
	if r == nil {
		return nil
	}
	if c.role == roleHost {
		if r.host == c {
			r.host = nil
		}
	} else {
		delete(r.guests, c)
	}
	c.room = nil

	if r.host == nil && len(r.guests) == 0 {
		delete(h.rooms, r.code)
		return nil
	}
	if r.host == nil && r.timer == nil {
		r.timer = time.AfterFunc(h.ttl, func() { h.expire(r) })
	}
	return r
}

func (h *hub) expire(r *room) {
	h.mu.Lock()
	if h.rooms[r.code] != r {
		h.mu.Unlock()
		return
	}
	delete(h.rooms, r.code)
	orphans := make([]*client, 0, len(r.guests))
	for g := range r.guests {
		orphans = append(orphans, g)
	}
	h.mu.Unlock()

	msg, _ := json.Marshal(map[string]any{"t": "error", "reason": "host left"})
	for _, g := range orphans {
		_ = g.send(msg)
		g.conn.Close(websocket.StatusGoingAway, "host left")
	}
}

func (h *hub) sendMembers(r *room) {
	h.mu.Lock()
	if h.rooms[r.code] != r {
		h.mu.Unlock()
		return
	}
	count := r.members()
	targets := r.others(nil)
	h.mu.Unlock()

	msg, _ := json.Marshal(map[string]any{"t": "members", "count": count})
	for _, c := range targets {
		_ = c.send(msg)
	}
}

func (h *hub) replay(r *room, c *client) {
	h.mu.Lock()
	queue := r.cache["queue"]
	state := r.cache["state"]
	h.mu.Unlock()

	if queue != nil {
		_ = c.send(queue)
	}
	if state != nil {
		_ = c.send(state)
	}
}

func (h *hub) handleWS(w http.ResponseWriter, r *http.Request) {
	code := strings.TrimSpace(r.URL.Query().Get("room"))
	role := r.URL.Query().Get("role")
	if !validCode(code) {
		http.Error(w, "invalid room", http.StatusBadRequest)
		return
	}
	if role != roleHost && role != roleGuest {
		http.Error(w, "invalid role", http.StatusBadRequest)
		return
	}

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns: []string{"*"},
	})
	if err != nil {
		return
	}

	c := &client{conn: conn, role: role}
	rm, err := h.join(code, role, c)
	if err != nil {
		msg, _ := json.Marshal(map[string]any{"t": "error", "reason": err.Error()})
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = conn.Write(ctx, websocket.MessageText, msg)
		cancel()
		conn.Close(websocket.StatusPolicyViolation, err.Error())
		return
	}

	h.sendMembers(rm)
	h.replay(rm, c)
	h.readLoop(c, rm)
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

func validCode(code string) bool {
	if len(code) == 0 || len(code) > maxCodeLen {
		return false
	}
	for _, r := range code {
		switch {
		case r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r >= '0' && r <= '9',
			r == '-', r == '_':
		default:
			return false
		}
	}
	return true
}
