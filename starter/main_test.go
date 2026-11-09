package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	store, err := NewStore(dbPath)
	if err != nil {
		t.Fatalf("failed to create test store: %v", err)
	}
	t.Cleanup(func() {
		store.Close()
	})
	return store
}

func TestDuplicates(t *testing.T) {
	store := newTestStore(t)
	mux := setupRoutes(store)

	// Batch 1: Ingests an event and attempts a duplicate in the same batch
	payload1 := `[
		{"event_id":"evt_dup_1","campaign_id":"cmp_promo","contact_id":"ct_101","type":"sent","timestamp":"2026-08-10T10:00:00Z"},
		{"event_id":"evt_dup_1","campaign_id":"cmp_promo","contact_id":"ct_101","type":"sent","timestamp":"2026-08-10T10:00:00Z"}
	]`

	req1 := httptest.NewRequest("POST", "/events", bytes.NewBufferString(payload1))
	req1.Header.Set("Content-Type", "application/json")
	w1 := httptest.NewRecorder()
	mux.ServeHTTP(w1, req1)

	if w1.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w1.Code, w1.Body.String())
	}

	var sum1 IngestionSummary
	if err := json.Unmarshal(w1.Body.Bytes(), &sum1); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if sum1.Accepted != 1 || sum1.Duplicates != 1 {
		t.Errorf("batch 1: expected Accepted=1, Duplicates=1; got Accepted=%d, Duplicates=%d", sum1.Accepted, sum1.Duplicates)
	}

	// Batch 2: Retry from provider sending the exact same event again in a subsequent request
	payload2 := `[
		{"event_id":"evt_dup_1","campaign_id":"cmp_promo","contact_id":"ct_101","type":"sent","timestamp":"2026-08-10T10:00:00Z"}
	]`
	req2 := httptest.NewRequest("POST", "/events", bytes.NewBufferString(payload2))
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, req2)

	var sum2 IngestionSummary
	_ = json.Unmarshal(w2.Body.Bytes(), &sum2)
	if sum2.Accepted != 0 || sum2.Duplicates != 1 {
		t.Errorf("batch 2: expected Accepted=0, Duplicates=1; got Accepted=%d, Duplicates=%d", sum2.Accepted, sum2.Duplicates)
	}

	// Verify database count has not been inflated
	stats, found := store.GetStats("cmp_promo")
	if !found {
		t.Fatalf("expected campaign cmp_promo to exist")
	}
	if stats.Counts.Sent != 1 {
		t.Errorf("expected Sent count in DB to be 1, got %d", stats.Counts.Sent)
	}
}

func TestInvalidEventsIndependentHandling(t *testing.T) {
	store := newTestStore(t)
	mux := setupRoutes(store)

	// Batch contains:
	// 1. Valid sent
	// 2. Bad timestamp
	// 3. Missing event_id
	// 4. Missing campaign_id
	// 5. Missing contact_id
	// 6. Unknown event type
	// 7. Valid delivered
	payload := `[
		{"event_id":"evt_v1","campaign_id":"cmp_mix","contact_id":"ct_1","type":"sent","timestamp":"2026-08-10T08:00:00Z"},
		{"event_id":"evt_b1","campaign_id":"cmp_mix","contact_id":"ct_2","type":"opened","timestamp":"not-a-valid-time"},
		{"event_id":"","campaign_id":"cmp_mix","contact_id":"ct_3","type":"opened","timestamp":"2026-08-10T08:05:00Z"},
		{"event_id":"evt_b2","campaign_id":"","contact_id":"ct_4","type":"opened","timestamp":"2026-08-10T08:05:00Z"},
		{"event_id":"evt_b3","campaign_id":"cmp_mix","contact_id":"","type":"delivered","timestamp":"2026-08-10T08:06:00Z"},
		{"event_id":"evt_b4","campaign_id":"cmp_mix","contact_id":"ct_5","type":"spam_report","timestamp":"2026-08-10T08:07:00Z"},
		{"event_id":"evt_v2","campaign_id":"cmp_mix","contact_id":"ct_1","type":"delivered","timestamp":"2026-08-10T08:08:00Z"}
	]`

	req := httptest.NewRequest("POST", "/events", bytes.NewBufferString(payload))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", w.Code, w.Body.String())
	}

	var summary IngestionSummary
	if err := json.Unmarshal(w.Body.Bytes(), &summary); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if summary.Total != 7 {
		t.Errorf("expected Total=7, got %d", summary.Total)
	}
	if summary.Accepted != 2 {
		t.Errorf("expected Accepted=2, got %d", summary.Accepted)
	}
	if summary.Rejected != 5 {
		t.Errorf("expected Rejected=5, got %d", summary.Rejected)
	}
	if len(summary.Errors) != 5 {
		t.Errorf("expected 5 ItemErrors, got %d", len(summary.Errors))
	}

	// Verify DB only persisted the 2 valid events
	stats, found := store.GetStats("cmp_mix")
	if !found {
		t.Fatalf("expected cmp_mix to be present")
	}
	if stats.Counts.Sent != 1 || stats.Counts.Delivered != 1 {
		t.Errorf("expected Sent=1, Delivered=1; got Sent=%d, Delivered=%d", stats.Counts.Sent, stats.Counts.Delivered)
	}
}

func TestStatisticsAndCalculatedRates(t *testing.T) {
	store := newTestStore(t)
	mux := setupRoutes(store)

	// Ingest events out of order:
	// Opened arrives before Delivered; Clicked arrives before Delivered
	// Contact c1 opens twice; Contact c2 opens once
	payload := `[
		{"event_id":"e_open1","campaign_id":"cmp_stats","contact_id":"c1","type":"opened","timestamp":"2026-08-10T12:00:00Z"},
		{"event_id":"e_click1","campaign_id":"cmp_stats","contact_id":"c1","type":"clicked","timestamp":"2026-08-10T12:05:00Z"},
		{"event_id":"e_sent1","campaign_id":"cmp_stats","contact_id":"c1","type":"sent","timestamp":"2026-08-10T11:00:00Z"},
		{"event_id":"e_del1","campaign_id":"cmp_stats","contact_id":"c1","type":"delivered","timestamp":"2026-08-10T11:05:00Z"},
		{"event_id":"e_open2","campaign_id":"cmp_stats","contact_id":"c1","type":"opened","timestamp":"2026-08-10T12:10:00Z"},
		{"event_id":"e_sent2","campaign_id":"cmp_stats","contact_id":"c2","type":"sent","timestamp":"2026-08-10T11:00:00Z"},
		{"event_id":"e_del2","campaign_id":"cmp_stats","contact_id":"c2","type":"delivered","timestamp":"2026-08-11T09:00:00Z"},
		{"event_id":"e_open3","campaign_id":"cmp_stats","contact_id":"c2","type":"opened","timestamp":"2026-08-11T09:30:00Z"}
	]`

	req := httptest.NewRequest("POST", "/events", bytes.NewBufferString(payload))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("ingest failed with code %d", w.Code)
	}

	// Fetch Stats endpoint
	reqStats := httptest.NewRequest("GET", "/campaigns/cmp_stats/stats", nil)
	wStats := httptest.NewRecorder()
	mux.ServeHTTP(wStats, reqStats)

	if wStats.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", wStats.Code, wStats.Body.String())
	}

	var resp CampaignStatsResponse
	if err := json.Unmarshal(wStats.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal stats error: %v", err)
	}

	// Verify Counts
	if resp.Counts.Sent != 2 {
		t.Errorf("expected Sent=2, got %d", resp.Counts.Sent)
	}
	if resp.Counts.Delivered != 2 {
		t.Errorf("expected Delivered=2, got %d", resp.Counts.Delivered)
	}
	if resp.Counts.Opened != 3 {
		t.Errorf("expected Opened=3 (raw), got %d", resp.Counts.Opened)
	}
	if resp.Counts.UniqueOpens != 2 {
		t.Errorf("expected UniqueOpens=2 (c1, c2), got %d", resp.Counts.UniqueOpens)
	}
	if resp.Counts.Clicked != 1 {
		t.Errorf("expected Clicked=1, got %d", resp.Counts.Clicked)
	}
	if resp.Counts.UniqueClicks != 1 {
		t.Errorf("expected UniqueClicks=1, got %d", resp.Counts.UniqueClicks)
	}

	// Verify Rates
	// Delivery Rate = 2 / 2 = 1.0
	if resp.Rates.DeliveryRate != 1.0 {
		t.Errorf("expected DeliveryRate 1.0, got %f", resp.Rates.DeliveryRate)
	}
	// Open Rate = 2 unique opens / 2 delivered = 1.0
	if resp.Rates.OpenRate != 1.0 {
		t.Errorf("expected OpenRate 1.0, got %f", resp.Rates.OpenRate)
	}
	// Click-To-Open Rate = 1 unique click / 2 unique opens = 0.5
	if resp.Rates.ClickToOpenRate != 0.5 {
		t.Errorf("expected ClickToOpenRate 0.5, got %f", resp.Rates.ClickToOpenRate)
	}
	// Click-Through Rate = 1 unique click / 2 delivered = 0.5
	if resp.Rates.ClickThroughRate != 0.5 {
		t.Errorf("expected ClickThroughRate 0.5, got %f", resp.Rates.ClickThroughRate)
	}

	// Verify Daily Delivered Buckets
	if resp.DailyDelivered["2026-08-10"] != 1 {
		t.Errorf("expected 1 delivered on 2026-08-10, got %d", resp.DailyDelivered["2026-08-10"])
	}
	if resp.DailyDelivered["2026-08-11"] != 1 {
		t.Errorf("expected 1 delivered on 2026-08-11, got %d", resp.DailyDelivered["2026-08-11"])
	}
}

func TestConcurrentRequests(t *testing.T) {
	store := newTestStore(t)
	mux := setupRoutes(store)

	var wg sync.WaitGroup
	workers := 15
	eventsPerWorker := 20

	// Multiple goroutines concurrently posting batches
	for i := 0; i < workers; i++ {
		workerID := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < eventsPerWorker; j++ {
				evID := fmt.Sprintf("evt_c_%d_%d", workerID, j)
				contactID := fmt.Sprintf("ct_%d", j%4)
				payload := fmt.Sprintf(`[{"event_id":"%s","campaign_id":"cmp_concurrent","contact_id":"%s","type":"sent","timestamp":"2026-08-10T14:00:00Z"}]`,
					evID, contactID)

				req := httptest.NewRequest("POST", "/events", bytes.NewBufferString(payload))
				w := httptest.NewRecorder()
				mux.ServeHTTP(w, req)
				if w.Code != http.StatusOK {
					t.Errorf("worker %d failed with code %d", workerID, w.Code)
				}
			}
		}()
	}

	// Concurrently query stats while writes are in progress
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 25; j++ {
				req := httptest.NewRequest("GET", "/campaigns/cmp_concurrent/stats", nil)
				w := httptest.NewRecorder()
				mux.ServeHTTP(w, req)
				// Code can be 200 or 404 (if no events inserted yet) but should never panic or 500
				if w.Code != http.StatusOK && w.Code != http.StatusNotFound {
					t.Errorf("unexpected query status: %d", w.Code)
				}
			}
		}()
	}

	wg.Wait()

	stats, found := store.GetStats("cmp_concurrent")
	if !found {
		t.Fatalf("expected campaign cmp_concurrent to exist")
	}

	expectedTotal := workers * eventsPerWorker
	if stats.Counts.Sent != expectedTotal {
		t.Errorf("expected total Sent=%d, got %d (possible race condition or lost update!)", expectedTotal, stats.Counts.Sent)
	}
}

func TestNotFoundCampaign(t *testing.T) {
	store := newTestStore(t)
	mux := setupRoutes(store)

	req := httptest.NewRequest("GET", "/campaigns/unknown_campaign/stats", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404 Not Found, got %d", w.Code)
	}

	var errResp map[string]string
	_ = json.Unmarshal(w.Body.Bytes(), &errResp)
	if errResp["error"] == "" {
		t.Errorf("expected error message in response body")
	}
}

func TestMalformedJSONBody(t *testing.T) {
	store := newTestStore(t)
	mux := setupRoutes(store)

	// Single JSON object instead of JSON array
	req := httptest.NewRequest("POST", "/events", bytes.NewBufferString(`{"event_id":"1"}`))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request, got %d", w.Code)
	}
}

func TestSeedDatasetEndToEnd(t *testing.T) {
	seedBytes, err := os.ReadFile("seed/events.json")
	if err != nil {
		t.Fatalf("failed to read seed/events.json: %v", err)
	}

	store := newTestStore(t)
	mux := setupRoutes(store)

	req := httptest.NewRequest("POST", "/events", bytes.NewReader(seedBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", w.Code, w.Body.String())
	}

	var summary IngestionSummary
	if err := json.Unmarshal(w.Body.Bytes(), &summary); err != nil {
		t.Fatalf("failed to parse summary: %v", err)
	}

	if summary.Total != 235 {
		t.Errorf("expected Total=235, got %d", summary.Total)
	}
	if summary.Accepted != 211 {
		t.Errorf("expected Accepted=211, got %d", summary.Accepted)
	}
	if summary.Duplicates != 18 {
		t.Errorf("expected Duplicates=18, got %d", summary.Duplicates)
	}
	if summary.Rejected != 6 {
		t.Errorf("expected Rejected=6, got %d", summary.Rejected)
	}

	// Verify stats for cmp_summer_sale
	statsSummer, found := store.GetStats("cmp_summer_sale")
	if !found {
		t.Fatalf("cmp_summer_sale not found")
	}
	if statsSummer.Counts.Sent != 40 || statsSummer.Counts.Delivered != 35 {
		t.Errorf("cmp_summer_sale counts mismatch: Sent=%d, Delivered=%d",
			statsSummer.Counts.Sent, statsSummer.Counts.Delivered)
	}
	if statsSummer.Counts.UniqueOpens != 20 {
		t.Errorf("expected 20 unique opens, got %d", statsSummer.Counts.UniqueOpens)
	}
	if statsSummer.DailyDelivered["2026-08-10"] != 35 {
		t.Errorf("expected 35 daily delivered on 2026-08-10, got %d", statsSummer.DailyDelivered["2026-08-10"])
	}
}
