package main

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
	"time"
)

const (
	roleHost  = "host"
	roleGuest = "guest"

	// hostStale is how long a host may go without a ping before a new host
	// connection may evict it. Clients ping every second, so a live host is
	// never replaced; a dead one is reclaimed within a few seconds.
	hostStale = 5 * time.Second

	defaultLead = 800 * time.Millisecond
)

// round is one consensus attempt for the next track: the host announces a gen
// with the full next state, every member present at that moment answers (loaded
// or not), and the relay releases the round once they all have. It releases on a
// shared instant so every member starts together instead of on frame arrival.
// A member that fails to load answers with ok=false so it cannot stall the room;
// a member that never answers is only removed by the liveness sweep.
type round struct {
	gen      string
	next     []byte
	expected map[*client]bool
	answers  map[*client]bool
}

type room struct {
	code     string
	host     *client
	guests   map[*client]struct{}
	timer    *time.Timer
	passHash string

	// epoch identifies the current host session. It is regenerated whenever a
	// host joins so stale playback from a previous host is rejected by clients.
	epoch   string
	pending *round

	// Snapshot for late joiners: the host queue and its latest playback state,
	// stamped with the relay clock so a newcomer projects the live position.
	queue []byte
	state []byte
}

func hashPass(p string) string {
	sum := sha256.Sum256([]byte(p))
	return hex.EncodeToString(sum[:])
}

func newEpoch() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func newRoom(code string) *room {
	return &room{
		code:   code,
		guests: make(map[*client]struct{}),
		epoch:  newEpoch(),
	}
}

func (r *room) members() int {
	n := len(r.guests)
	if r.host != nil {
		n++
	}
	return n
}

// all returns every member, host first.
func (r *room) all() []*client {
	out := make([]*client, 0, len(r.guests)+1)
	if r.host != nil {
		out = append(out, r.host)
	}
	for g := range r.guests {
		out = append(out, g)
	}
	return out
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
	lead      time.Duration
	token     string
}

func newHub(maxGuests int, ttl time.Duration) *hub {
	return &hub{
		rooms:     make(map[string]*room),
		maxGuests: maxGuests,
		ttl:       ttl,
		lead:      defaultLead,
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
		r.epoch = newEpoch()
		// The snapshot and any in-flight round belonged to the previous host
		// session: drop them so a late joiner cannot apply stale playback under
		// the new epoch.
		r.queue = nil
		r.state = nil
		r.pending = nil
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
// round releases it to the remaining members.
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
	if r.pending != nil {
		delete(r.pending.expected, c)
		delete(r.pending.answers, c)
		play = h.finishRoundLocked(r)
	}
	var targets []*client
	if play != nil {
		targets = r.all()
	}

	if r.host == nil && len(r.guests) == 0 {
		delete(h.rooms, r.code)
		h.mu.Unlock()
	} else {
		if r.host == nil && r.timer == nil {
			r.timer = time.AfterFunc(h.ttl, func() { h.endRoom(r) })
		}
		h.mu.Unlock()
	}

	if play != nil {
		h.sendAll(targets, play)
	}
	return r
}

// startRound opens a consensus round for gen over the members present right now,
// so a late joiner cannot extend a round it never received a prepare for. next is
// the host's prepare payload: the full next state, promoted to the room snapshot
// when the round releases.
func (h *hub) startRound(r *room, gen string, next []byte) {
	if r == nil || gen == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.rooms[r.code] != r {
		return
	}
	expected := make(map[*client]bool, len(r.guests)+1)
	for _, c := range r.all() {
		expected[c] = true
	}
	r.pending = &round{
		gen:      gen,
		next:     append([]byte(nil), next...),
		expected: expected,
		answers:  make(map[*client]bool),
	}
}

// finishRoundLocked releases the pending round once everyone expected has
// answered, promoting its next state to the room snapshot so a late joiner lands
// on the new track, and returns the `play` frame to broadcast. The caller holds
// h.mu. It returns nil when the round is not ready.
func (h *hub) finishRoundLocked(r *room) []byte {
	p := r.pending
	if p == nil {
		return nil
	}
	if len(p.expected) != 0 && len(p.answers) < len(p.expected) {
		return nil
	}
	r.pending = nil

	at := time.Now().Add(h.lead).UnixMilli()
	if p.next != nil {
		var m map[string]any
		if json.Unmarshal(p.next, &m) == nil {
			m["t"] = "state"
			m["at"] = at
			m["positionMs"] = 0
			m["playing"] = true
			delete(m, "gen")
			if b, err := json.Marshal(m); err == nil {
				r.state = b
			}
		}
	}

	msg, _ := json.Marshal(map[string]any{
		"t":          "play",
		"gen":        p.gen,
		"epoch":      r.epoch,
		"at":         at,
		"positionMs": 0,
	})
	return msg
}

// markReady records c's answer for the round and releases it once every expected
// member has answered (ok=false members count as answered, so they never stall).
func (h *hub) markReady(c *client, r *room, gen string, ok bool) {
	h.mu.Lock()
	if h.rooms[r.code] != r || r.pending == nil || gen != r.pending.gen || !r.pending.expected[c] {
		h.mu.Unlock()
		return
	}
	r.pending.answers[c] = ok
	msg := h.finishRoundLocked(r)
	var targets []*client
	if msg != nil {
		targets = r.all()
	}
	h.mu.Unlock()

	if msg != nil {
		h.sendAll(targets, msg)
	}
}

func (h *hub) sendAll(targets []*client, msg []byte) {
	for _, t := range targets {
		_ = t.send(msg)
	}
}

// endRoom closes the room and tells the guests the host never came back.
func (h *hub) endRoom(r *room) {
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
	targets := r.others(nil)
	h.mu.Unlock()

	msg, _ := json.Marshal(map[string]any{"t": "members", "count": count, "epoch": epoch})
	for _, c := range targets {
		_ = c.send(msg)
	}
}

// remember updates the late-joiner snapshot and returns the frame to forward.
// The relay is the room clock: it stamps `at` on every state so clients only need
// their own offset, and it keeps the frame verbatim otherwise. A state is dropped
// while a round is in flight, since the pending next state is promoted on release.
func (h *hub) remember(r *room, typ string, data []byte) []byte {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.rooms[r.code] != r {
		return nil
	}
	switch typ {
	case "queue":
		r.queue = append([]byte(nil), data...)
		return r.queue
	case "state":
		if r.pending != nil {
			return nil
		}
		var m map[string]any
		if json.Unmarshal(data, &m) != nil {
			return nil
		}
		m["at"] = time.Now().UnixMilli()
		b, err := json.Marshal(m)
		if err != nil {
			return nil
		}
		r.state = b
		return b
	}
	return nil
}

func (h *hub) replay(r *room, c *client) {
	h.mu.Lock()
	if h.rooms[r.code] != r {
		h.mu.Unlock()
		return
	}
	queue := r.queue
	state := r.state
	pending := r.pending != nil
	h.mu.Unlock()

	if queue != nil {
		_ = c.send(queue)
	}
	// While a round is in flight the cached state is the previous track: sending
	// it would start the newcomer on the wrong song, so it waits for the release.
	if state != nil && !pending {
		_ = c.send(state)
	}
}
