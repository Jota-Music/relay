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
	// A member that stops answering ready must not hold the room open: the round
	// releases on its own after roundTimeout, treating non-answers as failures.
	if h.roundTimeout > 0 {
		r.pending.timer = time.AfterFunc(h.roundTimeout, func() { h.expireRound(r, gen) })
	}
	return true
}

// releaseLocked finishes the pending round when it is complete, delivering the
// play frame to every member and any joiner that waited on it. send never
// blocks, so it is safe to run under h.mu.
func (h *hub) releaseLocked(r *room) {
	play := h.finishRoundLocked(r)
	if play == nil {
		return
	}
	for _, t := range r.all() {
		t.send(play)
	}
	h.flushJoinsLocked(r)
}

// expireRound force-releases a round that ran past its deadline. Members that
// never answered ready are simply left out, so the room keeps playing instead of
// waiting on a straggler that is alive but not cooperating.
func (h *hub) expireRound(r *room, gen string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.rooms[r.code] != r || r.pending == nil || r.pending.gen != gen {
		return
	}
	// Count the members that never answered as failures so the round releases.
	for c := range r.pending.expected {
		if _, ok := r.pending.answers[c]; !ok {
			r.pending.answers[c] = false
		}
	}
	h.releaseLocked(r)
}

// finishRoundLocked releases the pending round once everyone expected has
// answered, promoting its next state to the room snapshot so a late joiner lands
// on the new track, and returns the `play` frame. The caller holds h.mu; play is
// nil when the round is not ready to release.
func (h *hub) finishRoundLocked(r *room) []byte {
	p := r.pending
	if p == nil {
		return nil
	}
	if len(p.expected) != 0 && len(p.answers) < len(p.expected) {
		return nil
	}
	if p.timer != nil {
		p.timer.Stop()
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
	defer h.mu.Unlock()
	if h.rooms[r.code] != r || r.pending == nil || gen != r.pending.gen || !r.pending.expected[c] {
		return
	}
	r.pending.answers[c] = ok
	h.releaseLocked(r)
}
