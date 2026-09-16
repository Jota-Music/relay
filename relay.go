package main

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/coder/websocket"
)

const (
	roleHost  = "host"
	roleGuest = "guest"
)

type room struct {
	code     string
	host     *client
	guests   map[*client]struct{}
	cache    map[string]json.RawMessage
	timer    *time.Timer
	passHash string

	// Consensus round for the next track: the host announces a generation with
	// `prepare`, every member answers `ready`, and the relay broadcasts `play`
	// once all of them have the track loaded.
	pendingGen   string
	pendingCount int
	ready        map[*client]bool
}

func hashPass(p string) string {
	sum := sha256.Sum256([]byte(p))
	return hex.EncodeToString(sum[:])
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
	token     string
}

func newHub(maxGuests int, ttl time.Duration) *hub {
	return &hub{rooms: make(map[string]*room), maxGuests: maxGuests, ttl: ttl}
}

// join adds c to a room. An empty role means "auto": c becomes host when the
// room has none, otherwise a guest. It returns the role c was granted.
// pass is the optional room password: the first member sets it, the rest must
// match it (only when it was set).
func (h *hub) join(code, role, pass string, c *client) (*room, string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	r := h.rooms[code]
	if r == nil {
		r = newRoom(code)
		if pass != "" {
			r.passHash = hashPass(pass)
		}
		h.rooms[code] = r
	} else if r.passHash != "" {
		if subtle.ConstantTimeCompare([]byte(hashPass(pass)), []byte(r.passHash)) != 1 {
			return nil, "", errors.New("invalid room password")
		}
	}

	if role == "" {
		if r.host == nil {
			role = roleHost
		} else {
			role = roleGuest
		}
	}

	if role == roleHost {
		if r.host != nil {
			return nil, "", errors.New("room already has a host")
		}
		r.host = c
		if r.timer != nil {
			r.timer.Stop()
			r.timer = nil
		}
	} else {
		if len(r.guests) >= h.maxGuests {
			return nil, "", errors.New("room is full")
		}
		r.guests[c] = struct{}{}
	}
	c.role = role
	c.room = r
	return r, role, nil
}

// leave removes c from its room and returns the room if it still lives, plus the
// generation to release if c's departure completed a consensus round.
func (h *hub) leave(c *client) (*room, string) {
	h.mu.Lock()

	r := c.room
	if r == nil {
		h.mu.Unlock()
		return nil, ""
	}
	if c.role == roleHost {
		if r.host == c {
			r.host = nil
		}
	} else {
		delete(r.guests, c)
	}
	c.room = nil

	release := ""
	if r.pendingGen != "" {
		delete(r.ready, c)
		if r.members() < r.pendingCount {
			r.pendingCount = r.members()
		}
		if r.members() > 0 && len(r.ready) >= r.pendingCount {
			release = r.pendingGen
			r.pendingGen = ""
			r.ready = nil
		}
	}

	if r.host == nil && len(r.guests) == 0 {
		delete(h.rooms, r.code)
		h.mu.Unlock()
		return nil, ""
	}
	if r.host == nil && r.timer == nil {
		r.timer = time.AfterFunc(h.ttl, func() { h.expire(r) })
	}
	h.mu.Unlock()
	return r, release
}

// startRound opens a consensus round for gen. The member count is frozen here so
// a late joiner cannot extend a round it never received a prepare for.
func (h *hub) startRound(r *room, gen string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.rooms[r.code] != r || gen == "" {
		return
	}
	r.pendingGen = gen
	r.pendingCount = r.members()
	r.ready = make(map[*client]bool)
}

// markReady records c's ack for the round and releases it once every expected
// member has answered.
func (h *hub) markReady(c *client, r *room, gen string) {
	h.mu.Lock()
	if h.rooms[r.code] != r || r.pendingGen == "" || gen != r.pendingGen {
		h.mu.Unlock()
		return
	}
	r.ready[c] = true
	if len(r.ready) < r.pendingCount {
		h.mu.Unlock()
		return
	}
	r.pendingGen = ""
	r.ready = nil
	targets := r.others(nil)
	h.mu.Unlock()

	h.play(targets, gen)
}

func (h *hub) play(targets []*client, gen string) {
	msg, _ := json.Marshal(map[string]any{"t": "play", "gen": gen})
	for _, t := range targets {
		_ = t.send(msg)
	}
}

// broadcastPlay releases a round to every room member.
func (h *hub) broadcastPlay(r *room, gen string) {
	h.mu.Lock()
	if h.rooms[r.code] != r {
		h.mu.Unlock()
		return
	}
	targets := r.others(nil)
	h.mu.Unlock()
	h.play(targets, gen)
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
