package main

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"sync"
	"time"
)

type hub struct {
	mu           sync.Mutex
	rooms        map[string]*room
	maxGuests    int
	ttl          time.Duration
	lead         time.Duration
	joinTimeout  time.Duration
	roundTimeout time.Duration
	token        string
}

func newHub(maxGuests int, ttl time.Duration) *hub {
	return &hub{
		rooms:        make(map[string]*room),
		maxGuests:    maxGuests,
		ttl:          ttl,
		lead:         defaultLead,
		joinTimeout:  defaultJoinTimeout,
		roundTimeout: defaultRoundTimeout,
	}
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
			// A host whose socket is gone, or that stopped pinging, is gone: let
			// the reconnect reclaim it instead of demoting it to guest.
			if !r.host.closed() && time.Since(r.host.seen()) <= hostStale {
				return nil, "", errors.New("room already has a host")
			}
			old := r.host
			old.room = nil
			r.host = nil
			go old.close()
		}
		r.host = c
		r.epoch = randHex(16)
		// The snapshot is the room's, not the host's: keep it so a member that
		// rejoins (the host included) syncs to the state the room still holds,
		// rather than resetting the room to whoever reconnected.
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

// leave removes c from its room and returns the room if it still lives. When the
// host leaves with guests still in the room, the room is scheduled to end after
// the grace period unless a host rejoins. A departure that completes a consensus
// round releases it to the remaining members, and a host departure flushes any
// joiner waiting on an answer that will never come.
func (h *hub) leave(c *client) *room {
	h.mu.Lock()

	r := c.room
	if r == nil {
		h.mu.Unlock()
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

	var play []byte
	var out []outbound
	if r.pending != nil {
		delete(r.pending.expected, c)
		delete(r.pending.answers, c)
		play, out = h.finishRoundLocked(r)
	}
	if c.role == roleHost && r.host == nil {
		out = append(out, h.flushJoinsLocked(r)...)
	}
	var targets []*client
	if play != nil {
		targets = r.all()
	}

	// An empty room is kept until the TTL expires instead of being dropped at
	// once, so a member that steps out and rejoins still syncs to the snapshot.
	if r.host == nil && r.timer == nil {
		r.timer = time.AfterFunc(h.ttl, func() { h.endRoom(r) })
	}
	h.mu.Unlock()

	if play != nil {
		h.sendAll(targets, play)
	}
	h.sendOut(out)
	return r
}

func (h *hub) sendAll(targets []*client, msg []byte) {
	for _, t := range targets {
		t.send(msg)
	}
}

func (h *hub) sendOut(out []outbound) {
	for _, o := range out {
		o.c.send(o.msg)
	}
}

// endRoom closes the room and tells the guests the host never came back.
func (h *hub) endRoom(r *room) {
	h.mu.Lock()
	if h.rooms[r.code] != r || r.host != nil {
		// A host that reconnected just as the timer fired reclaims the room; its
		// join stopped the timer, but a fired timer cannot be cancelled.
		h.mu.Unlock()
		return
	}
	delete(h.rooms, r.code)
	orphans := make([]*client, 0, len(r.guests))
	for g := range r.guests {
		orphans = append(orphans, g)
	}
	for _, pj := range r.joins {
		if pj.timer != nil {
			pj.timer.Stop()
		}
	}
	r.joins = make(map[string]*pendingJoin)
	h.mu.Unlock()

	msg, _ := json.Marshal(map[string]any{"t": "error", "reason": "host left"})
	for _, g := range orphans {
		g.send(msg)
		g.closeAfterFlush(500 * time.Millisecond)
	}
}

// reap closes clients that stopped answering, so a dead connection can never
// hold a consensus round open forever.
func (h *hub) reap() {
	now := time.Now()
	h.mu.Lock()
	var stale []*client
	for _, r := range h.rooms {
		for _, c := range r.all() {
			if now.Sub(c.seen()) > idleTimeout {
				stale = append(stale, c)
			}
		}
	}
	h.mu.Unlock()

	for _, c := range stale {
		c.close()
	}
}

func (h *hub) sendMembers(r *room) {
	h.mu.Lock()
	if h.rooms[r.code] != r {
		h.mu.Unlock()
		return
	}
	count := r.members()
	epoch := r.epoch
	targets := r.all()
	h.mu.Unlock()

	msg, _ := json.Marshal(map[string]any{"t": "members", "count": count, "epoch": epoch})
	h.sendAll(targets, msg)
}

// roomStatus is the public snapshot of a room: counts and flags only, never the
// queue or the playback state.
type roomStatus struct {
	Active  bool `json:"active"`
	Members int  `json:"members"`
	HasHost bool `json:"hasHost"`
	Locked  bool `json:"locked"`
}

// status reports the live state of a room by exact code. An unknown code returns
// the zero value; there is no way to enumerate rooms.
func (h *hub) status(code string) roomStatus {
	h.mu.Lock()
	defer h.mu.Unlock()

	r := h.rooms[code]
	if r == nil {
		return roomStatus{}
	}
	members := r.members()
	return roomStatus{
		Active:  members > 0,
		Members: members,
		HasHost: r.host != nil,
		Locked:  r.passHash != "",
	}
}
