package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
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
	// defaultRoundTimeout bounds a consensus round: a member that answered the
	// liveness ping but never sent ready is force-released instead of holding the
	// room open forever.
	defaultRoundTimeout = 3 * time.Second
	// maxJoinDeferrals bounds how many join timeouts a round in flight may push
	// back before the relay gives up and answers from the cache anyway.
	maxJoinDeferrals = 8
)

// round is one consensus attempt for the next track: a member announces a gen
// with the full next state, every member present at that moment answers (loaded
// or not), and the relay releases the round once they all have. It releases on a
// shared instant so every member starts together instead of on frame arrival.
// A member that fails to load answers with ok=false so it cannot stall the room;
// a member that never answers is dropped by the round deadline.
type round struct {
	gen      string
	next     []byte
	expected map[*client]bool
	answers  map[*client]bool
	timer    *time.Timer
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
