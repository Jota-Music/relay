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

	defaultLead        = 800 * time.Millisecond
	defaultJoinTimeout = 1500 * time.Millisecond
	// maxJoinDeferrals bounds how many join timeouts a round in flight may push
	// back before the relay gives up and answers from the cache anyway.
	maxJoinDeferrals = 8
)

// round is one consensus attempt for the next track: a member announces a gen
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

// pendingJoin is a member waiting for the room snapshot. The relay stamps t1 on
// arrival, has the host answer with its live playback, and stamps t2 on the way
// out so the joiner can compute its clock offset (NTP over four timestamps).
type pendingJoin struct {
	c         *client
	t0        int64
	t1        int64
	deferrals int
	timer     *time.Timer
}

// outbound is a frame addressed to a single client, sent after the hub lock is
// released.
type outbound struct {
	c   *client
	msg []byte
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

	// Snapshot for joiners: the shared queue as serialized by a member and the
	// host playback object, relay-stamped. Both are injected into a join reply.
	queue string
	state []byte

	// joins holds the members waiting for that snapshot, keyed by client id.
	joins map[string]*pendingJoin
}

func hashPass(p string) string {
	sum := sha256.Sum256([]byte(p))
	return hex.EncodeToString(sum[:])
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func newRoom(code string) *room {
	return &room{
		code:   code,
		guests: make(map[*client]struct{}),
		epoch:  randHex(16),
		joins:  make(map[string]*pendingJoin),
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
	return r.others(nil)
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
	mu          sync.Mutex
	rooms       map[string]*room
	maxGuests   int
	ttl         time.Duration
	lead        time.Duration
	joinTimeout time.Duration
	token       string
}

func newHub(maxGuests int, ttl time.Duration) *hub {
	return &hub{
		rooms:       make(map[string]*room),
		maxGuests:   maxGuests,
		ttl:         ttl,
		lead:        defaultLead,
		joinTimeout: defaultJoinTimeout,
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

// startRound opens a consensus round for gen over the members present right now,
// so a late joiner cannot extend a round it never received a prepare for. next is
// the announcing member's prepare payload: the full next state, promoted to the
// room snapshot when the round releases. A second prepare while a round is in
// flight is refused (and not forwarded), so concurrent track changes resolve to
// a single round.
func (h *hub) startRound(r *room, gen string, next []byte) bool {
	if r == nil || gen == "" {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.rooms[r.code] != r {
		return false
	}
	if r.pending != nil {
		return false
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
	return true
}

// finishRoundLocked releases the pending round once everyone expected has
// answered, promoting its next state to the room snapshot so a late joiner lands
// on the new track, and returns the `play` frame plus any deferred join replies.
// The caller holds h.mu, and must send the frames after unlocking.
func (h *hub) finishRoundLocked(r *room) ([]byte, []outbound) {
	p := r.pending
	if p == nil {
		return nil, nil
	}
	if len(p.expected) != 0 && len(p.answers) < len(p.expected) {
		return nil, nil
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
	// A joiner held back by this round waits for its release: hand it the track
	// the room is actually starting on, not the outgoing one.
	return msg, h.flushJoinsLocked(r)
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
	msg, out := h.finishRoundLocked(r)
	var targets []*client
	if msg != nil {
		targets = r.all()
	}
	h.mu.Unlock()

	if msg != nil {
		h.sendAll(targets, msg)
	}
	h.sendOut(out)
}

// joinRequest answers a member asking for the room snapshot. The relay stamps t1,
// forwards the request to the host (t0/t1/from), and the host answers with its
// live playback; the relay stamps t2 on the way out. With no host to ask (the
// joiner is the host, or the room has none) it replies from the cache.
func (h *hub) joinRequest(r *room, c *client, t0 int64) {
	h.mu.Lock()
	if h.rooms[r.code] != r {
		h.mu.Unlock()
		return
	}
	t1 := time.Now().UnixMilli()
	host := r.host
	if host == nil || host == c {
		queue := r.queue
		state := r.state
		h.mu.Unlock()
		c.send(buildSnapshot(t1, time.Now().UnixMilli(), queue, state))
		return
	}
	pj := &pendingJoin{c: c, t0: t0, t1: t1}
	pj.timer = time.AfterFunc(h.joinTimeout, func() { h.expireJoin(r, c.id) })
	r.joins[c.id] = pj
	// A round is in flight: its release is the authoritative next state, so hold
	// the join instead of answering with the track that is being replaced.
	pending := r.pending != nil
	h.mu.Unlock()

	if pending {
		return
	}
	host.send(joinForward(t0, t1, c.id))
}

// joinAnswer routes the host's playback to the waiting joiner, stamped with t2.
func (h *hub) joinAnswer(r *room, data []byte) {
	var p struct {
		To    string          `json:"to"`
		State json.RawMessage `json:"state"`
	}
	if json.Unmarshal(data, &p) != nil || p.To == "" {
		return
	}
	h.mu.Lock()
	if h.rooms[r.code] != r {
		h.mu.Unlock()
		return
	}
	pj := r.joins[p.To]
	delete(r.joins, p.To)
	queue := r.queue
	h.mu.Unlock()
	if pj == nil {
		return
	}
	if pj.timer != nil {
		pj.timer.Stop()
	}
	pj.c.send(buildSnapshot(pj.t1, time.Now().UnixMilli(), queue, p.State))
}

// expireJoin answers a join the host never answered, so a silent host can never
// leave a newcomer waiting.
func (h *hub) expireJoin(r *room, id string) {
	h.mu.Lock()
	if h.rooms[r.code] != r {
		h.mu.Unlock()
		return
	}
	pj := r.joins[id]
	if pj == nil {
		h.mu.Unlock()
		return
	}
	// A round in flight owns the next state, so the cache still holds the
	// outgoing track: answering now would strand the newcomer there. Defer until
	// the round releases (flushJoinsLocked answers from the promoted state) or
	// the liveness sweep drops the silent member. Only after a bounded number of
	// deferrals is the cache sent as a last resort.
	if r.pending != nil && pj.deferrals < maxJoinDeferrals {
		pj.deferrals++
		pj.timer = time.AfterFunc(h.joinTimeout, func() { h.expireJoin(r, id) })
		h.mu.Unlock()
		return
	}
	delete(r.joins, id)
	queue := r.queue
	state := r.state
	h.mu.Unlock()
	pj.c.send(buildSnapshot(pj.t1, time.Now().UnixMilli(), queue, state))
}

// flushJoinsLocked answers every waiting joiner from the current snapshot and
// clears them. The caller holds h.mu and sends the returned frames after
// unlocking.
func (h *hub) flushJoinsLocked(r *room) []outbound {
	if len(r.joins) == 0 {
		return nil
	}
	now := time.Now().UnixMilli()
	out := make([]outbound, 0, len(r.joins))
	for id, pj := range r.joins {
		if pj.timer != nil {
			pj.timer.Stop()
		}
		out = append(out, outbound{pj.c, buildSnapshot(pj.t1, now, r.queue, r.state)})
		delete(r.joins, id)
	}
	return out
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
	if h.rooms[r.code] != r {
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

// remember updates the room snapshot and returns the frame to forward. The relay
// is the room clock: it stamps `at` on every state so clients only need their own
// offset. A queue is shared by every member and forwarded; a state is the host's
// playback feed, cached for the join fallback and never broadcast (members follow
// the round and the join snapshot instead). A state is dropped while a round is
// in flight, since the pending next state is promoted on release.
func (h *hub) remember(r *room, typ string, data []byte) []byte {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.rooms[r.code] != r {
		return nil
	}
	switch typ {
	case "queue":
		var m struct {
			Data string `json:"data"`
		}
		if json.Unmarshal(data, &m) != nil {
			return nil
		}
		r.queue = m.Data
		return data
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
		return nil
	}
	return nil
}

func joinForward(t0, t1 int64, from string) []byte {
	b, _ := json.Marshal(map[string]any{
		"t":    "join",
		"at":   t0,
		"t1":   t1,
		"from": from,
	})
	return b
}

func buildSnapshot(t1, t2 int64, queue string, state []byte) []byte {
	m := map[string]any{
		"t":    "snapshot",
		"echo": t1,
		"at":   t2,
	}
	if queue != "" {
		m["queue"] = json.RawMessage(queue)
	}
	if len(state) > 0 {
		m["state"] = json.RawMessage(state)
	}
	b, _ := json.Marshal(m)
	return b
}
