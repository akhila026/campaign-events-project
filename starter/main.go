package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
)

func setupRoutes(store *Store) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /events", func(w http.ResponseWriter, r *http.Request) {
		handlePostEventsWithStore(w, r, store)
	})
	mux.HandleFunc("GET /campaigns/{campaignID}/stats", func(w http.ResponseWriter, r *http.Request) {
		handleGetStatsWithStore(w, r, store)
	})
	mux.HandleFunc("GET /campaigns/{campaignID}/events", func(w http.ResponseWriter, r *http.Request) {
		handleGetEventsWithStore(w, r, store)
	})
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return mux
}

func main() {
	store, err := NewStore("events.db")
	if err != nil {
		log.Fatalf("failed to initialize sqlite store: %v", err)
	}
	defer store.Close()

	mux := setupRoutes(store)
	addr := ":8080"
	fmt.Printf("Campaign Events service (SQLite) listening on %s\n", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func handlePostEventsWithStore(w http.ResponseWriter, r *http.Request, store *Store) {
	defer r.Body.Close()

	bodyBytes, err := io.ReadAll(io.LimitReader(r.Body, 10*1024*1024)) // 10MB limit
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to read request body"})
		return
	}

	if len(bodyBytes) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "empty request body"})
		return
	}

	var rawItems []json.RawMessage
	if err := json.Unmarshal(bodyBytes, &rawItems); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid json payload: expected a JSON array of events",
		})
		return
	}

	summary := store.IngestBatch(rawItems)
	writeJSON(w, http.StatusOK, summary)
}

func handleGetStatsWithStore(w http.ResponseWriter, r *http.Request, store *Store) {
	campaignID := r.PathValue("campaignID")
	if campaignID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "campaignID path parameter is required"})
		return
	}

	stats, found := store.GetStats(campaignID)
	if !found {
		writeJSON(w, http.StatusNotFound, map[string]string{
			"error": fmt.Sprintf("campaign %q not found", campaignID),
		})
		return
	}

	writeJSON(w, http.StatusOK, stats)
}

func handleGetEventsWithStore(w http.ResponseWriter, r *http.Request, store *Store) {
	campaignID := r.PathValue("campaignID")
	if campaignID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "campaignID path parameter is required"})
		return
	}

	query := r.URL.Query()
	limit := 20
	offset := 0

	if lStr := query.Get("limit"); lStr != "" {
		if parsed, err := strconv.Atoi(lStr); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	if oStr := query.Get("offset"); oStr != "" {
		if parsed, err := strconv.Atoi(oStr); err == nil && parsed >= 0 {
			offset = parsed
		}
	}

	eventsPage, found := store.GetEvents(campaignID, limit, offset)
	if !found {
		writeJSON(w, http.StatusNotFound, map[string]string{
			"error": fmt.Sprintf("campaign %q not found", campaignID),
		})
		return
	}

	writeJSON(w, http.StatusOK, eventsPage)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("writeJSON: %v", err)
	}
}
