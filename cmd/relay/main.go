package main

import (
	"cmp"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	port := cmp.Or(os.Getenv("PORT"), "8080")
	maxGuests := 8
	if v, err := strconv.Atoi(os.Getenv("MAX_GUESTS")); err == nil {
		maxGuests = v
	}
	// How long a room survives without its host before the room ends. Long enough
	// for an automatic reconnect, short enough to not linger.
	ttl := getdur("ROOM_TTL", 30*time.Second)

	h := newHub(maxGuests, ttl)
	h.lead = getdur("PLAY_LEAD", defaultLead)
	h.roundTimeout = getdur("ROUND_TIMEOUT", defaultRoundTimeout)
	h.token = os.Getenv("AUTH_TOKEN")

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		for range ticker.C {
			h.reap()
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", h.handleWS)
	mux.HandleFunc("GET /rooms/{code}", h.handleRoomStatus)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]bool{"auth": h.token != ""})
	})

	log.Printf("relay listening on :%s (max_guests=%d room_ttl=%s auth=%t)", port, maxGuests, ttl, h.token != "")
	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

func getdur(key string, fallback time.Duration) time.Duration {
	if v, err := time.ParseDuration(os.Getenv(key)); err == nil {
		return v
	}
	return fallback
}
