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
)

// round is one consensus attempt for the next track: the host announces a gen,
// every member present at that moment answers (loaded or not), and the relay
// releases the round once they all have. A member that fails to load answers
// with ok=false so it cannot stall the room; a member that never answers is only
// removed by the liveness sweep.
type round struct {
	gen      string
	expected map[*client]bool
	answers  map[*client]bool // value reports whether the member loaded the track
}

func (r *round) release() string {
	if len(r.expected) == 0 || len(r.answers) < len(r.expected) {
		return ""
	}
	return r.gen
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
	// re-stamped with the relay clock so a newcomer projects the live position.
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
		r.epoch = newEpoch()
		// The snapshot belonged to the previous host session: drop it so a late
		// joiner cannot apply stale playback under the new epoch.
		r.queue = nil
		r.state = nil
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
// generation to release if c's departure completed a consensus round. When the
// host leaves with guests still in the room, the room is scheduled to end after
// the grace period unless a host rejoins.
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
	if r.pending != nil {
		delete(r.pending.expected, c)
		delete(r.pending.answers, c)
		if gen := r.pending.release(); gen != "" {
			r.pending = nil
			release = gen
		}
	}

	if r.host == nil && len(r.guests) == 0 {
		delete(h.rooms, r.code)
		h.mu.Unlock()
		return nil, ""
	}
	if r.host == nil && r.timer == nil {
		r.timer = time.AfterFunc(h.ttl, func() { h.endRoom(r) })
	}
	h.mu.Unlock()
	return r, release
}

// startRound opens a consensus round for gen over the members present right now,
// so a late joiner cannot extend a round it never received a prepare for.
func (h *hub) startRound(r *room, gen string) {
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
	r.pending = &round{gen: gen, expected: expected, answers: make(map[*client]bool)}
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
	release := r.pending.release()
	var targets []*client
	epoch := r.epoch
	if release != "" {
		r.pending = nil
		targets = r.others(nil)
	}
	h.mu.Unlock()

	if release != "" {
		h.play(targets, release, epoch)
	}
}

// play releases a round: everyone starts the announced track together.
func (h *hub) play(targets []*client, gen, epoch string) {
	msg, _ := json.Marshal(map[string]any{
		"t":          "play",
		"gen":        gen,
		"epoch":      epoch,
		"at":         time.Now().UnixMilli(),
		"positionMs": 0,
	})
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
	epoch := r.epoch
	targets := r.others(nil)
	h.mu.Unlock()
	h.play(targets, gen, epoch)
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

// remember updates the late-joiner snapshot. Playback state is re-stamped with
// the relay clock so a newcomer projects the live position, and heartbeats keep
// that position fresh between state changes.
func (h *hub) remember(r *room, typ string, data []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.rooms[r.code] != r {
		return
	}
	switch typ {
	case "queue":
		r.queue = append([]byte(nil), data...)
	case "state":
		var m map[string]any
		if json.Unmarshal(data, &m) != nil {
			return
		}
		m["at"] = time.Now().UnixMilli()
		if b, err := json.Marshal(m); err == nil {
			r.state = b
		}
	case "heartbeat":
		if r.state == nil {
			return
		}
		var hb, st map[string]any
		if json.Unmarshal(data, &hb) != nil || json.Unmarshal(r.state, &st) != nil {
			return
		}
		if hb["songId"] != st["songId"] {
			return
		}
		for _, k := range []string{"playing", "positionMs", "color", "binary"} {
			if v, ok := hb[k]; ok {
				st[k] = v
			}
		}
		st["at"] = time.Now().UnixMilli()
		if b, err := json.Marshal(st); err == nil {
			r.state = b
		}
	}
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
