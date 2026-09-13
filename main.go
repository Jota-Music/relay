package main

import (
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	port := getenv("PORT", "8080")
	maxGuests := getint("MAX_GUESTS", 8)
	ttl := getdur("ROOM_TTL", 10*time.Minute)

	h := newHub(maxGuests, ttl)
	h.token = getenv("AUTH_TOKEN", "")

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", h.handleWS)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	log.Printf("relay listening on :%s (max_guests=%d room_ttl=%s auth=%t)", port, maxGuests, ttl, h.token != "")
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatal(err)
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getint(key string, fallback int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return v
	}
	return fallback
}

func getdur(key string, fallback time.Duration) time.Duration {
	if v, err := time.ParseDuration(os.Getenv(key)); err == nil {
		return v
	}
	return fallback
}
