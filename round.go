package main

import (
	"encoding/json"
	"time"
)

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
