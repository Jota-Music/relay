package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	_, srv := newTestHub(t, 8, time.Minute)
	return srv
}

func newTestHub(t *testing.T, maxGuests int, ttl time.Duration) (*hub, *httptest.Server) {
	t.Helper()
	h := newHub(maxGuests, ttl)
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", h.handleWS)
	mux.HandleFunc("GET /rooms/{code}", h.handleRoomStatus)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return h, srv
}

func dial(t *testing.T, base, room, role string) *websocket.Conn {
	t.Helper()
	url := "ws" + strings.TrimPrefix(base, "http") + "/ws?room=" + room + "&role=" + role
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("dial %s: %v", role, err)
	}
	c.SetReadLimit(maxMessageBytes)
	return c
}

func dialPass(t *testing.T, base, room, role, pass string) (*websocket.Conn, *http.Response, error) {
	t.Helper()
	url := "ws" + strings.TrimPrefix(base, "http") + "/ws?room=" + room + "&role=" + role
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	header := http.Header{}
	if pass != "" {
		header.Set("X-Room-Password", pass)
	}
	c, resp, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: header})
	if c != nil {
		c.SetReadLimit(maxMessageBytes)
	}
	return c, resp, err
}

func read(t *testing.T, c *websocket.Conn) map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, data, err := c.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("unmarshal %q: %v", data, err)
	}
	return m
}

// readWithin reads with a short deadline and reports whether a frame arrived.
func readWithin(t *testing.T, c *websocket.Conn, d time.Duration) (map[string]any, bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	_, data, err := c.Read(ctx)
	if err != nil {
		return nil, false
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("unmarshal %q: %v", data, err)
	}
	return m, true
}

func send(t *testing.T, c *websocket.Conn, msg string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Write(ctx, websocket.MessageText, []byte(msg)); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func TestMembersBroadcast(t *testing.T) {
	srv := newTestServer(t)

	host := dial(t, srv.URL, "party", roleHost)
	if m := read(t, host); m["t"] != "members" || m["count"].(float64) != 1 {
		t.Fatalf("host members = %v", m)
	}

	guest := dial(t, srv.URL, "party", roleGuest)
	if m := read(t, guest); m["t"] != "members" || m["count"].(float64) != 2 {
		t.Fatalf("guest members = %v", m)
	}
	if m := read(t, host); m["t"] != "members" || m["count"].(float64) != 2 {
		t.Fatalf("host update = %v", m)
	}

	send(t, guest, `{"t":"ping","id":7,"at":123}`)
	if m := read(t, guest); m["t"] != "pong" || m["id"].(float64) != 7 || m["echo"].(float64) == 0 {
		t.Fatalf("pong = %v", m)
	}
}

func TestHostBusy(t *testing.T) {
	srv := newTestServer(t)

	first := dial(t, srv.URL, "solo", roleHost)
	read(t, first)

	second := dial(t, srv.URL, "solo", roleHost)
	m := read(t, second)
	if m["t"] != "error" || !strings.Contains(m["reason"].(string), "host") {
		t.Fatalf("busy host = %v", m)
	}
}

func TestAutoRole(t *testing.T) {
	srv := newTestServer(t)

	host := dial(t, srv.URL, "auto", "")
	if m := read(t, host); m["t"] != "role" || m["role"] != "host" {
		t.Fatalf("first role = %v", m)
	}
	if m := read(t, host); m["t"] != "members" || m["count"].(float64) != 1 {
		t.Fatalf("host members = %v", m)
	}

	guest := dial(t, srv.URL, "auto", "")
	if m := read(t, guest); m["t"] != "role" || m["role"] != "guest" {
		t.Fatalf("second role = %v", m)
	}
	if m := read(t, guest); m["t"] != "members" || m["count"].(float64) != 2 {
		t.Fatalf("guest members = %v", m)
	}
}

func TestAuthToken(t *testing.T) {
	h := newHub(8, time.Minute)
	h.token = "secret"
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", h.handleWS)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws?room=secure&role=host"

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, resp, err := websocket.Dial(ctx, url, nil); err == nil ||
		resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 without token, got err=%v resp=%v", err, resp)
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()
	conn, _, err := websocket.Dial(ctx2, url, &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": {"Bearer secret"}},
	})
	if err != nil {
		t.Fatalf("dial with token: %v", err)
	}
	conn.Close(websocket.StatusNormalClosure, "")
}

func TestRoomPassword(t *testing.T) {
	srv := newTestServer(t)

	host, _, err := dialPass(t, srv.URL, "locked", roleHost, "pw")
	if err != nil {
		t.Fatalf("host dial: %v", err)
	}
	read(t, host)

	bad, _, err := dialPass(t, srv.URL, "locked", roleGuest, "")
	if err != nil {
		t.Fatalf("guest dial: %v", err)
	}
	if m := read(t, bad); m["t"] != "error" ||
		!strings.Contains(m["reason"].(string), "password") {
		t.Fatalf("expected password error, got %v", m)
	}

	good, _, err := dialPass(t, srv.URL, "locked", roleGuest, "pw")
	if err != nil {
		t.Fatalf("guest dial: %v", err)
	}
	if m := read(t, good); m["t"] != "members" || m["count"].(float64) != 2 {
		t.Fatalf("guest members = %v", m)
	}
}

// Every member publishes: the queue and the round are no longer host-only.
func TestAnyMemberCanPublish(t *testing.T) {
	srv := newTestServer(t)

	host := dial(t, srv.URL, "free", roleHost)
	read(t, host)
	guest := dial(t, srv.URL, "free", roleGuest)
	read(t, guest)
	read(t, host)

	send(t, guest, `{"t":"queue","data":"[]"}`)
	if m := read(t, host); m["t"] != "queue" || m["data"] != "[]" {
		t.Fatalf("relayed queue = %v", m)
	}

	send(t, guest, `{"t":"prepare","gen":"g1","song":{"id":"s1"}}`)
	if m := read(t, host); m["t"] != "prepare" || m["gen"] != "g1" {
		t.Fatalf("relayed prepare = %v", m)
	}
}

// A join with no host to ask (the joiner is the host) is answered from the cache,
// with both relay timestamps so the client can compute its offset.
func TestJoinFromHostUsesCache(t *testing.T) {
	srv := newTestServer(t)

	host := dial(t, srv.URL, "cache", roleHost)
	read(t, host)

	send(t, host, `{"t":"state","rev":1,"at":1,"playing":true,"positionMs":5000,"songId":"s1","index":0}`)
	time.Sleep(50 * time.Millisecond)

	send(t, host, `{"t":"join","at":1000}`)
	m := read(t, host)
	if m["t"] != "snapshot" {
		t.Fatalf("join reply = %v", m)
	}
	if m["echo"].(float64) < 1e12 || m["at"].(float64) < m["echo"].(float64) {
		t.Fatalf("join stamps = %v", m)
	}
	state, ok := m["state"].(map[string]any)
	if !ok || state["songId"] != "s1" {
		t.Fatalf("join state = %v", m)
	}
}

// A join is forwarded to the host, which answers with its live playback; the
// relay routes only to that joiner and stamps the outgoing time.
func TestJoinRoutesToHost(t *testing.T) {
	srv := newTestServer(t)

	host := dial(t, srv.URL, "route", roleHost)
	read(t, host)
	guest := dial(t, srv.URL, "route", roleGuest)
	read(t, guest)
	read(t, host)

	send(t, guest, `{"t":"join","at":1000}`)
	req := read(t, host)
	if req["t"] != "join" || req["at"].(float64) != 1000 {
		t.Fatalf("forwarded join = %v", req)
	}
	if t1, ok := req["t1"].(float64); !ok || t1 < 1e12 {
		t.Fatalf("join t1 not relay-stamped: %v", req["t1"])
	}
	from, ok := req["from"].(string)
	if !ok || from == "" {
		t.Fatalf("join from = %v", req["from"])
	}

	send(t, host, `{"t":"snapshot","to":"`+from+`","state":{"songId":"s1","playing":true}}`)
	m := read(t, guest)
	if m["t"] != "snapshot" {
		t.Fatalf("snapshot = %v", m)
	}
	if m["echo"].(float64) != req["t1"].(float64) {
		t.Fatalf("snapshot echo %v != t1 %v", m["echo"], req["t1"])
	}
	if m["at"].(float64) < 1e12 {
		t.Fatalf("snapshot not relay-stamped: %v", m["at"])
	}
	if state, ok := m["state"].(map[string]any); !ok || state["songId"] != "s1" {
		t.Fatalf("snapshot state = %v", m)
	}
}

// A silent host must not leave a newcomer hanging: the relay answers from the
// cache after the join timeout.
func TestJoinFallsBackWhenHostSilent(t *testing.T) {
	h, srv := newTestHub(t, 8, time.Minute)
	h.joinTimeout = 50 * time.Millisecond

	host := dial(t, srv.URL, "silent", roleHost)
	read(t, host)
	guest := dial(t, srv.URL, "silent", roleGuest)
	read(t, guest)
	read(t, host)

	send(t, guest, `{"t":"join","at":1000}`)
	read(t, host) // forwarded request, no answer

	if m, ok := readWithin(t, guest, 2*time.Second); !ok || m["t"] != "snapshot" {
		t.Fatalf("join fallback = %v", m)
	}
}

// A join arriving while a round is in flight waits for the release and lands on
// the promoted track, not the one being replaced.
func TestJoinDuringRoundGetsPromotedTrack(t *testing.T) {
	h, srv := newTestHub(t, 8, time.Minute)
	h.joinTimeout = 50 * time.Millisecond

	host := dial(t, srv.URL, "defer", roleHost)
	read(t, host)
	guest := dial(t, srv.URL, "defer", roleGuest)
	read(t, guest)
	read(t, host)

	send(t, host, `{"t":"prepare","gen":"g1","songId":"s2","song":{"id":"s2"}}`)
	read(t, guest)

	late := dial(t, srv.URL, "defer", roleGuest)
	if m := read(t, late); m["t"] != "members" || m["count"].(float64) != 3 {
		t.Fatalf("late members = %v", m)
	}
	send(t, late, `{"t":"join","at":1000}`)

	send(t, host, `{"t":"ready","gen":"g1"}`)
	send(t, guest, `{"t":"ready","gen":"g1"}`)

	// The round releases to the whole room, late joiner included.
	if m := read(t, late); m["t"] != "play" {
		t.Fatalf("late play = %v", m)
	}
	m := read(t, late)
	if m["t"] != "snapshot" {
		t.Fatalf("deferred snapshot = %v", m)
	}
	state, ok := m["state"].(map[string]any)
	if !ok || state["songId"] != "s2" {
		t.Fatalf("deferred snapshot state = %v", m)
	}
}

// A join arriving while a round is in flight must not be answered from the cache
// (which still holds the outgoing track); the fallback defers until the release.
func TestJoinFallbackDefersWhileRoundPending(t *testing.T) {
	h, srv := newTestHub(t, 8, time.Minute)
	h.joinTimeout = 50 * time.Millisecond

	host := dial(t, srv.URL, "defer-fallback", roleHost)
	read(t, host)
	guest := dial(t, srv.URL, "defer-fallback", roleGuest)
	read(t, guest)
	read(t, host)

	send(t, host, `{"t":"prepare","gen":"g1","songId":"s2","song":{"id":"s2"}}`)
	read(t, guest)

	late := dial(t, srv.URL, "defer-fallback", roleGuest)
	if m := read(t, late); m["t"] != "members" {
		t.Fatalf("late members = %v", m)
	}
	send(t, late, `{"t":"join","at":1000}`)

	// Past several join timeouts: a fallback that answered mid-round would have
	// delivered the cached (outgoing) state by now.
	time.Sleep(120 * time.Millisecond)

	send(t, host, `{"t":"ready","gen":"g1"}`)
	send(t, guest, `{"t":"ready","gen":"g1"}`)

	if m := read(t, late); m["t"] != "play" {
		t.Fatalf("late first frame = %v, want play (no stale snapshot)", m)
	}
	m := read(t, late)
	if m["t"] != "snapshot" {
		t.Fatalf("late snapshot = %v", m)
	}
	state, ok := m["state"].(map[string]any)
	if !ok || state["songId"] != "s2" {
		t.Fatalf("late snapshot state = %v", m)
	}
}

// A host that drops and comes back must sync to the room it left behind, not
// reset it: the snapshot (queue and paused playback) survives the reclaim.
func TestHostReclaimKeepsSnapshot(t *testing.T) {
	_, srv := newTestHub(t, 8, time.Minute)

	host := dial(t, srv.URL, "keep", roleHost)
	read(t, host)
	guest := dial(t, srv.URL, "keep", roleGuest)
	read(t, guest)
	read(t, host)

	send(t, host, `{"t":"queue","data":"[{\"id\":\"s1\"}]"}`)
	if m := read(t, guest); m["t"] != "queue" {
		t.Fatalf("relayed queue = %v", m)
	}
	send(t, host, `{"t":"state","playing":false,"positionMs":5000,"songId":"s1","index":0}`)
	time.Sleep(50 * time.Millisecond)

	host.Close(websocket.StatusNormalClosure, "")
	if m := read(t, guest); m["t"] != "members" || m["count"].(float64) != 1 {
		t.Fatalf("guest members after host left = %v", m)
	}

	again := dial(t, srv.URL, "keep", roleHost)
	if m := read(t, again); m["t"] != "members" || m["count"].(float64) != 2 {
		t.Fatalf("reclaim members = %v", m)
	}
	send(t, again, `{"t":"join","at":1000}`)
	m := read(t, again)
	if m["t"] != "snapshot" {
		t.Fatalf("reclaim snapshot = %v", m)
	}
	state, ok := m["state"].(map[string]any)
	if !ok || state["songId"] != "s1" || state["playing"] != false ||
		state["positionMs"].(float64) != 5000 {
		t.Fatalf("reclaim lost the paused state: %v", m)
	}
	if q, ok := m["queue"].([]any); !ok || len(q) != 1 {
		t.Fatalf("reclaim lost the queue: %v", m)
	}
}

// A room keeps its snapshot for ROOM_TTL after the last member leaves, so a
// rejoin inside the window syncs instead of starting a fresh room.
func TestEmptyRoomSurvivesForRejoin(t *testing.T) {
	h, srv := newTestHub(t, 8, time.Minute)

	host := dial(t, srv.URL, "survive", roleHost)
	read(t, host)
	send(t, host, `{"t":"state","playing":true,"positionMs":1000,"songId":"s9","index":0}`)
	time.Sleep(50 * time.Millisecond)
	host.Close(websocket.StatusNormalClosure, "")

	// The leave is processed asynchronously: wait for the room to empty.
	for i := 0; i < 200; i++ {
		h.mu.Lock()
		r := h.rooms["survive"]
		empty := r != nil && r.host == nil && len(r.guests) == 0
		h.mu.Unlock()
		if empty {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	again := dial(t, srv.URL, "survive", roleHost)
	read(t, again)
	send(t, again, `{"t":"join","at":1000}`)
	m := read(t, again)
	if m["t"] != "snapshot" {
		t.Fatalf("rejoin snapshot = %v", m)
	}
	state, ok := m["state"].(map[string]any)
	if !ok || state["songId"] != "s9" {
		t.Fatalf("empty room lost its snapshot: %v", m)
	}
}

// Joining a paused room brings the current track paused at its position.
func TestJoinGetsPausedSnapshot(t *testing.T) {
	h, srv := newTestHub(t, 8, time.Minute)
	h.joinTimeout = 50 * time.Millisecond

	host := dial(t, srv.URL, "paused", roleHost)
	read(t, host)
	send(t, host, `{"t":"state","playing":false,"positionMs":42000,"songId":"s1","index":2}`)
	time.Sleep(50 * time.Millisecond)

	guest := dial(t, srv.URL, "paused", roleGuest)
	read(t, guest)
	read(t, host)
	send(t, guest, `{"t":"join","at":1000}`)
	read(t, host) // forwarded; the host stays silent and the cache answers

	m := read(t, guest)
	if m["t"] != "snapshot" {
		t.Fatalf("paused snapshot = %v", m)
	}
	state, ok := m["state"].(map[string]any)
	if !ok || state["playing"] != false || state["positionMs"].(float64) != 42000 {
		t.Fatalf("paused state = %v", m)
	}
}

// Any member feeds the playback cache, so a guest's pause reaches a joiner that
// is answered from the cache while the host stays silent.
func TestAnyMemberFeedsState(t *testing.T) {
	h, srv := newTestHub(t, 8, time.Minute)
	h.joinTimeout = 50 * time.Millisecond

	host := dial(t, srv.URL, "feed", roleHost)
	read(t, host)
	guest := dial(t, srv.URL, "feed", roleGuest)
	read(t, guest)
	read(t, host)

	// The guest, not the host, sets the room state.
	send(t, guest, `{"t":"state","playing":false,"positionMs":7000,"songId":"sg","index":1}`)
	time.Sleep(50 * time.Millisecond)

	late := dial(t, srv.URL, "feed", roleGuest)
	read(t, late)
	read(t, host)
	read(t, guest)
	send(t, late, `{"t":"join","at":1000}`)
	read(t, host) // forwarded; the host stays silent and the cache answers

	m := read(t, late)
	state, ok := m["state"].(map[string]any)
	if !ok || state["songId"] != "sg" {
		t.Fatalf("guest state not in the cache: %v", m)
	}
}

// A second prepare while a round is open is refused and not forwarded, so a
// simultaneous track change cannot fork the room.
func TestConcurrentPrepareKeepsFirstRound(t *testing.T) {
	srv := newTestServer(t)

	host := dial(t, srv.URL, "race", roleHost)
	read(t, host)
	guest := dial(t, srv.URL, "race", roleGuest)
	read(t, guest)
	read(t, host)

	send(t, host, `{"t":"prepare","gen":"g1","song":{"id":"s1"}}`)
	if m := read(t, guest); m["t"] != "prepare" || m["gen"] != "g1" {
		t.Fatalf("first prepare = %v", m)
	}

	send(t, guest, `{"t":"prepare","gen":"g2","song":{"id":"s2"}}`)
	send(t, host, `{"t":"ready","gen":"g1"}`)
	send(t, guest, `{"t":"ready","gen":"g1"}`)

	// If the refused prepare had been forwarded, the host would read it before
	// the release instead of the play for the round it accepted.
	if m := read(t, host); m["t"] != "play" || m["gen"] != "g1" {
		t.Fatalf("host play = %v", m)
	}
	if m := read(t, guest); m["t"] != "play" || m["gen"] != "g1" {
		t.Fatalf("guest play = %v", m)
	}
}

// The sender never receives its own frame back, so it cannot double-apply it.
func TestControlNotEchoedToSender(t *testing.T) {
	srv := newTestServer(t)

	host := dial(t, srv.URL, "echo", roleHost)
	read(t, host)
	guest := dial(t, srv.URL, "echo", roleGuest)
	read(t, guest)
	read(t, host)

	send(t, guest, `{"t":"control","action":"toggle"}`)
	if m := read(t, host); m["t"] != "control" || m["action"] != "toggle" {
		t.Fatalf("relayed control = %v", m)
	}
	if m, ok := readWithin(t, guest, 200*time.Millisecond); ok {
		t.Fatalf("control echoed to sender: %v", m)
	}
}

func TestQueueForwardedOnce(t *testing.T) {
	srv := newTestServer(t)

	host := dial(t, srv.URL, "once", roleHost)
	read(t, host)
	guest := dial(t, srv.URL, "once", roleGuest)
	read(t, guest)
	read(t, host)

	send(t, guest, `{"t":"queue","data":"[1,2,3]"}`)
	if m := read(t, host); m["t"] != "queue" {
		t.Fatalf("relayed queue = %v", m)
	}
	if m, ok := readWithin(t, guest, 200*time.Millisecond); ok {
		t.Fatalf("queue echoed to sender: %v", m)
	}
}

// A member that leaves mid-round still releases it for the rest.
func TestInitiatorLeavesMidRoundReleases(t *testing.T) {
	srv := newTestServer(t)

	host := dial(t, srv.URL, "drop", roleHost)
	read(t, host)
	guest := dial(t, srv.URL, "drop", roleGuest)
	read(t, guest)
	read(t, host)

	send(t, guest, `{"t":"prepare","gen":"g1","song":{"id":"s1"}}`)
	if m := read(t, host); m["t"] != "prepare" {
		t.Fatalf("prepare = %v", m)
	}

	guest.Close(websocket.StatusNormalClosure, "")
	if m := read(t, host); m["t"] != "members" || m["count"].(float64) != 1 {
		t.Fatalf("members after leave = %v", m)
	}

	send(t, host, `{"t":"ready","gen":"g1"}`)
	if m := read(t, host); m["t"] != "play" || m["gen"] != "g1" {
		t.Fatalf("play after initiator left = %v", m)
	}
}

// A host that leaves must not strand a pending join: it is flushed from the cache.
func TestHostLeftFlushesPendingJoin(t *testing.T) {
	_, srv := newTestHub(t, 8, time.Minute)

	host := dial(t, srv.URL, "flush", roleHost)
	read(t, host)
	guest := dial(t, srv.URL, "flush", roleGuest)
	read(t, guest)
	read(t, host)

	send(t, guest, `{"t":"join","at":1000}`)
	read(t, host) // forwarded request, no answer

	host.Close(websocket.StatusNormalClosure, "")

	if m := read(t, guest); m["t"] != "snapshot" {
		t.Fatalf("flushed join = %v", m)
	}
	if m := read(t, guest); m["t"] != "members" {
		t.Fatalf("members after host left = %v", m)
	}
}

func TestConsensusPlay(t *testing.T) {
	srv := newTestServer(t)

	host := dial(t, srv.URL, "jam", roleHost)
	read(t, host)
	guest := dial(t, srv.URL, "jam", roleGuest)
	read(t, guest)
	read(t, host)

	send(t, host, `{"t":"prepare","gen":"g1","song":{"id":"s1"}}`)
	if m := read(t, guest); m["t"] != "prepare" || m["gen"] != "g1" {
		t.Fatalf("relayed prepare = %v", m)
	}

	send(t, host, `{"t":"ready","gen":"g1"}`)
	send(t, guest, `{"t":"ready","gen":"g1"}`)

	if m := read(t, host); m["t"] != "play" || m["gen"] != "g1" {
		t.Fatalf("host play = %v", m)
	}
	if m := read(t, guest); m["t"] != "play" || m["gen"] != "g1" {
		t.Fatalf("guest play = %v", m)
	}
}

func TestConsensusLateJoinerDoesNotExtendRound(t *testing.T) {
	srv := newTestServer(t)

	host := dial(t, srv.URL, "late-jam", roleHost)
	read(t, host)
	guest := dial(t, srv.URL, "late-jam", roleGuest)
	read(t, guest)
	read(t, host)

	send(t, host, `{"t":"prepare","gen":"g1","song":{"id":"s1"}}`)
	if m := read(t, guest); m["t"] != "prepare" {
		t.Fatalf("relayed prepare = %v", m)
	}

	late := dial(t, srv.URL, "late-jam", roleGuest)
	if m := read(t, late); m["t"] != "members" || m["count"].(float64) != 3 {
		t.Fatalf("late members = %v", m)
	}
	read(t, host)
	read(t, guest)

	// The round was opened with two members: the late joiner must not be needed
	// for it to release.
	send(t, host, `{"t":"ready","gen":"g1"}`)
	send(t, guest, `{"t":"ready","gen":"g1"}`)

	if m := read(t, host); m["t"] != "play" || m["gen"] != "g1" {
		t.Fatalf("host play = %v", m)
	}
	if m := read(t, guest); m["t"] != "play" || m["gen"] != "g1" {
		t.Fatalf("guest play = %v", m)
	}
}

func TestMembersCarriesEpoch(t *testing.T) {
	srv := newTestServer(t)

	host := dial(t, srv.URL, "epoch", roleHost)
	m := read(t, host)
	if m["t"] != "members" || m["epoch"] == "" || m["epoch"] == nil {
		t.Fatalf("members epoch = %v", m)
	}
}

func TestConsensusReadyFailureStillReleases(t *testing.T) {
	srv := newTestServer(t)

	host := dial(t, srv.URL, "fail-jam", roleHost)
	read(t, host)
	guest := dial(t, srv.URL, "fail-jam", roleGuest)
	read(t, guest)
	read(t, host)

	send(t, host, `{"t":"prepare","gen":"g1"}`)
	if m := read(t, guest); m["t"] != "prepare" {
		t.Fatalf("relayed prepare = %v", m)
	}
	send(t, host, `{"t":"ready","gen":"g1","ok":true}`)
	// The guest could not load: it must not stall the room.
	send(t, guest, `{"t":"ready","gen":"g1","ok":false}`)

	if m := read(t, host); m["t"] != "play" {
		t.Fatalf("host play = %v", m)
	}
	if m := read(t, guest); m["t"] != "play" {
		t.Fatalf("guest play = %v", m)
	}
}

// A released round must schedule a shared start instant: every member gets the
// same future `at` and starts on it, not when its own frame arrives.
func TestPlaySchedulesSharedStart(t *testing.T) {
	srv := newTestServer(t)

	host := dial(t, srv.URL, "clocked", roleHost)
	read(t, host)
	guest := dial(t, srv.URL, "clocked", roleGuest)
	read(t, guest)
	read(t, host)

	before := time.Now().UnixMilli()
	send(t, host, `{"t":"prepare","gen":"g1","song":{"id":"s1"}}`)
	read(t, guest)
	send(t, host, `{"t":"ready","gen":"g1"}`)
	send(t, guest, `{"t":"ready","gen":"g1"}`)

	plays := map[string]map[string]any{"host": read(t, host), "guest": read(t, guest)}
	for name, m := range plays {
		if m["t"] != "play" || m["gen"] != "g1" {
			t.Fatalf("%s play = %v", name, m)
		}
		if m["positionMs"].(float64) != 0 {
			t.Fatalf("%s positionMs = %v", name, m["positionMs"])
		}
		if at := m["at"].(float64); at <= float64(before) {
			t.Fatalf("%s at not scheduled ahead: %v", name, m["at"])
		}
	}
	if plays["host"]["at"] != plays["guest"]["at"] {
		t.Fatalf("start instants differ: %v", plays)
	}
}

// After a release the cached state carries the new track, so a joiner answered
// from the cache lands on it.
func TestReleasePromotesSnapshot(t *testing.T) {
	h, srv := newTestHub(t, 8, time.Minute)
	h.joinTimeout = 50 * time.Millisecond

	host := dial(t, srv.URL, "promote", roleHost)
	read(t, host)
	guest := dial(t, srv.URL, "promote", roleGuest)
	read(t, guest)
	read(t, host)

	send(t, host, `{"t":"prepare","gen":"g1","songId":"s2","index":3,"song":{"id":"s2","name":"Next"}}`)
	read(t, guest)
	send(t, host, `{"t":"ready","gen":"g1"}`)
	send(t, guest, `{"t":"ready","gen":"g1"}`)
	play := read(t, host)
	read(t, guest)

	late := dial(t, srv.URL, "promote", roleGuest)
	if m := read(t, late); m["t"] != "members" {
		t.Fatalf("late members = %v", m)
	}
	send(t, late, `{"t":"join","at":1000}`)
	// The host does not answer: the relay falls back to the promoted cache.
	m := read(t, late)
	if m["t"] != "snapshot" {
		t.Fatalf("promoted snapshot = %v", m)
	}
	state, ok := m["state"].(map[string]any)
	if !ok {
		t.Fatalf("promoted state missing: %v", m)
	}
	if state["songId"] != "s2" || state["positionMs"].(float64) != 0 || state["playing"] != true {
		t.Fatalf("promoted state fields = %v", state)
	}
	if state["at"] != play["at"] {
		t.Fatalf("promoted at %v != play at %v", state["at"], play["at"])
	}
}

// The cached state carries the whole track so a joiner can load it without
// depending on its own queue.
func TestSnapshotKeepsTrack(t *testing.T) {
	srv := newTestServer(t)

	host := dial(t, srv.URL, "track", roleHost)
	read(t, host)

	send(t, host, `{"t":"state","seq":1,"at":1,"playing":true,"positionMs":0,"songId":"s1","index":0,"song":{"id":"s1","name":"Track"}}`)
	time.Sleep(50 * time.Millisecond)

	send(t, host, `{"t":"join","at":1000}`)
	m := read(t, host)
	state, ok := m["state"].(map[string]any)
	if !ok || state["songId"] != "s1" {
		t.Fatalf("snapshot state = %v", m)
	}
	song, ok := state["song"].(map[string]any)
	if !ok || song["id"] != "s1" {
		t.Fatalf("snapshot dropped the track: %v", state)
	}
}

// A host that stopped pinging may be evicted by the reconnecting host instead of
// being demoted to guest.
func TestHostReclaimWhenStale(t *testing.T) {
	h, srv := newTestHub(t, 8, time.Minute)

	first := dial(t, srv.URL, "reclaim", roleHost)
	read(t, first)

	h.mu.Lock()
	for _, r := range h.rooms {
		for _, c := range r.all() {
			c.lastSeen.Store(time.Now().Add(-time.Minute).UnixNano())
		}
	}
	h.mu.Unlock()

	second := dial(t, srv.URL, "reclaim", roleHost)
	if m := read(t, second); m["t"] != "members" || m["count"].(float64) != 1 {
		t.Fatalf("reclaim members = %v", m)
	}
	if _, ok := readWithin(t, first, time.Second); ok {
		t.Fatal("stale host should have been evicted")
	}
}

func TestReapClosesIdleClients(t *testing.T) {
	h, srv := newTestHub(t, 8, time.Minute)

	host := dial(t, srv.URL, "idle", roleHost)
	read(t, host)

	h.mu.Lock()
	for _, r := range h.rooms {
		for _, c := range r.all() {
			c.lastSeen.Store(time.Now().Add(-time.Minute).UnixNano())
		}
	}
	h.mu.Unlock()

	h.reap()
	if _, ok := readWithin(t, host, time.Second); ok {
		t.Fatal("idle client should have been closed")
	}
}

func TestHostLeftEndsRoom(t *testing.T) {
	_, srv := newTestHub(t, 8, 100*time.Millisecond)

	host := dial(t, srv.URL, "bye", roleHost)
	read(t, host)
	guest := dial(t, srv.URL, "bye", roleGuest)
	read(t, guest)
	read(t, host)

	host.Close(websocket.StatusNormalClosure, "")

	if m := read(t, guest); m["t"] != "members" {
		t.Fatalf("guest members after host left = %v", m)
	}
	if m := read(t, guest); m["t"] != "error" {
		t.Fatalf("expected host-left error, got %v", m)
	}
}

func TestLargeMessageRelayed(t *testing.T) {
	srv := newTestServer(t)

	host := dial(t, srv.URL, "big", roleHost)
	read(t, host)
	guest := dial(t, srv.URL, "big", roleGuest)
	read(t, guest)
	read(t, host)

	big := `{"t":"queue","data":"` + strings.Repeat("a", 100_000) + `"}`
	send(t, host, big)
	if m := read(t, guest); m["t"] != "queue" {
		t.Fatalf("relayed = %v", m)
	}
}

func getStatus(t *testing.T, base, code, token string) roomStatus {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, base+"/rooms/"+code, nil)
	if err != nil {
		t.Fatalf("status request: %v", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status code = %d", resp.StatusCode)
	}
	var st roomStatus
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		t.Fatalf("status decode: %v", err)
	}
	return st
}

func TestRoomStatus(t *testing.T) {
	_, srv := newTestHub(t, 8, time.Minute)

	if st := getStatus(t, srv.URL, "nope", ""); st.Active || st.Members != 0 {
		t.Fatalf("unknown room = %+v", st)
	}

	host := dial(t, srv.URL, "party", roleHost)
	defer host.Close(websocket.StatusNormalClosure, "")
	read(t, host)

	if st := getStatus(t, srv.URL, "party", ""); !st.Active || !st.HasHost || st.Members != 1 || st.Locked {
		t.Fatalf("host room = %+v", st)
	}

	guest := dial(t, srv.URL, "party", roleGuest)
	defer guest.Close(websocket.StatusNormalClosure, "")
	read(t, guest)

	if st := getStatus(t, srv.URL, "party", ""); st.Members != 2 || !st.HasHost {
		t.Fatalf("guest room = %+v", st)
	}
}

func TestRoomStatusLocked(t *testing.T) {
	_, srv := newTestHub(t, 8, time.Minute)

	host, _, err := dialPass(t, srv.URL, "secret", roleHost, "pw")
	if err != nil {
		t.Fatalf("host dial: %v", err)
	}
	defer host.Close(websocket.StatusNormalClosure, "")
	read(t, host)

	if st := getStatus(t, srv.URL, "secret", ""); !st.Locked {
		t.Fatalf("locked room = %+v", st)
	}
}

func TestRoomStatusAuth(t *testing.T) {
	h, srv := newTestHub(t, 8, time.Minute)
	h.token = "sesame"

	resp, err := http.Get(srv.URL + "/rooms/party")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 without token, got %d", resp.StatusCode)
	}

	if st := getStatus(t, srv.URL, "party", "sesame"); st.Active {
		t.Fatalf("authorized room = %+v", st)
	}
}
