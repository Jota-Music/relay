package main

import (
	"context"
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

func (h *hub) handleWS(w http.ResponseWriter, r *http.Request) {
	code := strings.TrimSpace(r.URL.Query().Get("room"))
	role := r.URL.Query().Get("role")
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

	c := &client{conn: conn, role: role}
	rm, assigned, err := h.join(code, role, c)
	if err != nil {
		msg, _ := json.Marshal(map[string]any{"t": "error", "reason": err.Error()})
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = conn.Write(ctx, websocket.MessageText, msg)
		cancel()
		conn.Close(websocket.StatusPolicyViolation, err.Error())
		return
	}

	if role == "" {
		msg, _ := json.Marshal(map[string]any{"t": "role", "role": assigned})
		_ = c.send(msg)
	}
	h.sendMembers(rm)
	h.replay(rm, c)
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
