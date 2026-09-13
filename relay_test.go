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
	h := newHub(8, time.Minute)
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", h.handleWS)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
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
	return c
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
