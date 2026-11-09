package main

import (
	"errors"
	"strings"
	"time"
)

// Event represents one webhook event received from a delivery provider.
type Event struct {
	EventID    string            `json:"event_id"`
	CampaignID string            `json:"campaign_id"`
	ContactID  string            `json:"contact_id"`
	Type       string            `json:"type"` // sent | delivered | opened | clicked
	Timestamp  string            `json:"timestamp"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

// Validate validates event IDs, campaign IDs, contact IDs, event types, and timestamps.
func (e *Event) Validate() (time.Time, error) {
	e.EventID = strings.TrimSpace(e.EventID)
	if e.EventID == "" {
		return time.Time{}, errors.New("missing or empty event_id")
	}

	e.CampaignID = strings.TrimSpace(e.CampaignID)
	if e.CampaignID == "" {
		return time.Time{}, errors.New("missing or empty campaign_id")
	}

	e.ContactID = strings.TrimSpace(e.ContactID)
	if e.ContactID == "" {
		return time.Time{}, errors.New("missing or empty contact_id")
	}

	e.Type = strings.ToLower(strings.TrimSpace(e.Type))
	switch e.Type {
	case "sent", "delivered", "opened", "clicked":
		// Valid known event type
	default:
		return time.Time{}, errors.New("unsupported or invalid event type: " + e.Type)
	}

	tsStr := strings.TrimSpace(e.Timestamp)
	if tsStr == "" {
		return time.Time{}, errors.New("missing timestamp")
	}

	// Validate RFC3339 / ISO8601 timestamp (e.g. 2026-08-10T09:15:04Z)
	t, err := time.Parse(time.RFC3339, tsStr)
	if err != nil {
		return time.Time{}, errors.New("invalid RFC3339 timestamp format: " + tsStr)
	}

	return t.UTC(), nil
}

// IngestionSummary provides the outcome of a batch ingestion request.
type IngestionSummary struct {
	Total      int         `json:"total"`
	Accepted   int         `json:"accepted"`
	Duplicates int         `json:"duplicates"`
	Rejected   int         `json:"rejected"`
	Errors     []ItemError `json:"errors,omitempty"`
}

// ItemError describes an error on a specific item in an ingestion batch.
type ItemError struct {
	Index   int    `json:"index"`
	EventID string `json:"event_id,omitempty"`
	Error   string `json:"error"`
}

// StatsCounts holds aggregate numbers for a campaign.
type StatsCounts struct {
	Sent         int `json:"sent"`
	Delivered    int `json:"delivered"`
	Opened       int `json:"opened"`
	UniqueOpens  int `json:"unique_opens"`
	Clicked      int `json:"clicked"`
	UniqueClicks int `json:"unique_clicks"`
}

// StatsRates holds derived conversion and engagement rates.
type StatsRates struct {
	DeliveryRate     float64 `json:"delivery_rate"`      // delivered / sent
	OpenRate         float64 `json:"open_rate"`          // unique_opens / delivered
	ClickToOpenRate  float64 `json:"click_to_open_rate"` // unique_clicks / unique_opens
	ClickThroughRate float64 `json:"click_through_rate"` // unique_clicks / delivered
}

// CampaignStatsResponse is the dashboard representation of campaign metrics.
type CampaignStatsResponse struct {
	CampaignID     string         `json:"campaign_id"`
	Counts         StatsCounts    `json:"counts"`
	Rates          StatsRates     `json:"rates"`
	DailyDelivered map[string]int `json:"daily_delivered"`
}

// PaginatedEventsResponse holds a paginated page of events for audit/viewing.
type PaginatedEventsResponse struct {
	CampaignID string  `json:"campaign_id"`
	Total      int     `json:"total"`
	Limit      int     `json:"limit"`
	Offset     int     `json:"offset"`
	Events     []Event `json:"events"`
}
