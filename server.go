package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/coder/websocket"
)

const (
	maxCodeLen      = 64
	maxMessageBytes = 16 << 20
)

// authorized reports whether r may open a WebSocket. With no token configured
// the relay is open, mirroring the previous behaviour.
func (h *hub) authorized(r *http.Request) bool {
	if h.token == "" {
		return true
	}
	got := bearerToken(r)
	if got == "" {
		got = r.URL.Query().Get("token")
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(h.token)) == 1
}

func bearerToken(r *http.Request) string {
	const prefix = "bearer "
	header := r.Header.Get("Authorization")
	if len(header) <= len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return ""
	}
	return strings.TrimSpace(header[len(prefix):])
}

func (h *hub) handleWS(w http.ResponseWriter, r *http.Request) {
	if !h.authorized(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	code := strings.TrimSpace(r.URL.Query().Get("room"))
	role := r.URL.Query().Get("role")
	pass := r.Header.Get("X-Room-Password")
	if pass == "" {
		pass = r.URL.Query().Get("pass")
	}
	if !validCode(code) {
		http.Error(w, "invalid room", http.StatusBadRequest)
		return
	}
	if role != "" && role != roleHost && role != roleGuest {
		http.Error(w, "invalid role", http.StatusBadRequest)
		return
	}

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns: []string{"*"},
	})
	if err != nil {
		return
	}
	conn.SetReadLimit(maxMessageBytes)

	c := newClient(conn, role)
	rm, assigned, err := h.join(code, role, pass, c)
	if err != nil {
		msg, _ := json.Marshal(map[string]any{"t": "error", "reason": err.Error()})
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = conn.Write(ctx, websocket.MessageText, msg)
		cancel()
		c.close()
		return
	}
	c.run()

	if role == "" {
		msg, _ := json.Marshal(map[string]any{"t": "role", "role": assigned})
		_ = c.send(msg)
	}
	h.sendMembers(rm)
	h.readLoop(c, rm)
}

func validCode(code string) bool {
	if len(code) == 0 || len(code) > maxCodeLen {
		return false
	}
	for _, r := range code {
		switch {
		case r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r >= '0' && r <= '9',
			r == '-', r == '_':
		default:
			return false
		}
	}
	return true
}
