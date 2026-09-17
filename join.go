package main

import (
	"encoding/json"
	"time"
)

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
