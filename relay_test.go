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

func TestMembersSnapshotAndRelay(t *testing.T) {
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

	send(t, host, `{"t":"state","at":1000,"songId":"abc"}`)
	if m := read(t, guest); m["t"] != "state" || m["songId"] != "abc" {
		t.Fatalf("relayed state = %v", m)
	} else if at, ok := m["at"].(float64); !ok || at < 1e12 {
		t.Fatalf("forwarded state not relay-stamped: %v", m["at"])
	}

	late := dial(t, srv.URL, "party", roleGuest)
	if m := read(t, late); m["t"] != "members" || m["count"].(float64) != 3 {
		t.Fatalf("late members = %v", m)
	}
	if m := read(t, late); m["t"] != "state" || m["songId"] != "abc" {
		t.Fatalf("late snapshot = %v", m)
	}

	send(t, late, `{"t":"ping","id":7,"at":123}`)
	if m := read(t, late); m["t"] != "pong" || m["id"].(float64) != 7 || m["echo"].(float64) == 0 {
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

func TestGuestCannotPublish(t *testing.T) {
	srv := newTestServer(t)

	host := dial(t, srv.URL, "ro", roleHost)
	read(t, host)
	guest := dial(t, srv.URL, "ro", roleGuest)
	read(t, guest)
	read(t, host)

	// A guest must not be able to inject playback state.
	send(t, guest, `{"t":"state","at":1,"songId":"hack"}`)
	send(t, guest, `{"t":"prepare","gen":"hack"}`)
	if m, ok := readWithin(t, host, 300*time.Millisecond); ok {
		t.Fatalf("guest message was forwarded: %v", m)
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

// A late joiner after a release must land on the new track, not the previous
// snapshot: the relay promotes the pending prepare into the cached state.
func TestReleasePromotesSnapshot(t *testing.T) {
	srv := newTestServer(t)

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
	m := read(t, late)
	if m["t"] != "state" || m["songId"] != "s2" {
		t.Fatalf("promoted snapshot = %v", m)
	}
	if m["index"].(float64) != 3 || m["playing"] != true || m["positionMs"].(float64) != 0 {
		t.Fatalf("promoted snapshot fields = %v", m)
	}
	if m["at"] != play["at"] {
		t.Fatalf("promoted at %v != play at %v", m["at"], play["at"])
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

func TestSnapshotStampsRelayClock(t *testing.T) {
	srv := newTestServer(t)

	host := dial(t, srv.URL, "snap", roleHost)
	read(t, host)

	send(t, host, `{"t":"state","rev":1,"at":1,"playing":true,"positionMs":5000,"songId":"s1","index":0}`)
	time.Sleep(50 * time.Millisecond)

	late := dial(t, srv.URL, "snap", roleGuest)
	if m := read(t, late); m["t"] != "members" {
		t.Fatalf("late members = %v", m)
	}
	m := read(t, late)
	if m["t"] != "state" || m["songId"] != "s1" {
		t.Fatalf("late snapshot = %v", m)
	}
	if at, ok := m["at"].(float64); !ok || at < 1e12 {
		t.Fatalf("snapshot at not stamped by the relay: %v", m["at"])
	}
}

// The cached state carries the whole track so a late joiner (or a guest that
// missed the prepare) can load it without depending on its own queue. The relay
// re-marshals state to re-stamp `at`, so this guards the nested payload survives.
func TestSnapshotKeepsTrack(t *testing.T) {
	srv := newTestServer(t)

	host := dial(t, srv.URL, "track", roleHost)
	read(t, host)

	send(t, host, `{"t":"state","seq":1,"at":1,"playing":true,"positionMs":0,"songId":"s1","index":0,"song":{"id":"s1","name":"Track"}}`)
	time.Sleep(50 * time.Millisecond)

	late := dial(t, srv.URL, "track", roleGuest)
	if m := read(t, late); m["t"] != "members" {
		t.Fatalf("late members = %v", m)
	}
	m := read(t, late)
	if m["t"] != "state" || m["songId"] != "s1" {
		t.Fatalf("late snapshot = %v", m)
	}
	song, ok := m["song"].(map[string]any)
	if !ok || song["id"] != "s1" {
		t.Fatalf("snapshot dropped the track: %v", m)
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
