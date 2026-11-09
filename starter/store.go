package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// Store handles persistent SQLite storage for events, deduplication, and statistics.
type Store struct {
	db *sql.DB
	mu sync.Mutex // Serializes write transactions to prevent SQLite busy locks
}

// NewStore initializes a SQLite database connection, applies WAL pragmas, and ensures schema migrations.
func NewStore(dbPath string) (*Store, error) {
	if dbPath == "" {
		dbPath = "events.db"
	}

	dsn := dbPath
	if dbPath != ":memory:" {
		dsn = fmt.Sprintf("%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)", dbPath)
	}

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite database: %w", err)
	}

	// For SQLite, limiting open write connections prevents database locked errors
	db.SetMaxOpenConns(1)

	s := &Store{
		db: db,
	}

	if err := s.initSchema(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize database schema: %w", err)
	}

	return s, nil
}

// Close closes the underlying SQLite database.
func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) initSchema() error {
	schema := `
	CREATE TABLE IF NOT EXISTS events (
		event_id TEXT PRIMARY KEY,
		campaign_id TEXT NOT NULL,
		contact_id TEXT NOT NULL,
		type TEXT NOT NULL,
		timestamp DATETIME NOT NULL,
		metadata TEXT,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);

	CREATE INDEX IF NOT EXISTS idx_events_campaign ON events(campaign_id);
	CREATE INDEX IF NOT EXISTS idx_events_campaign_type ON events(campaign_id, type);
	CREATE INDEX IF NOT EXISTS idx_events_campaign_contact ON events(campaign_id, contact_id);
	CREATE INDEX IF NOT EXISTS idx_events_daily_delivered ON events(campaign_id, type, timestamp);
	`
	_, err := s.db.Exec(schema)
	return err
}

// IngestBatch processes an array of raw JSON events. Each event is parsed and validated
// independently. Valid events are inserted with ON CONFLICT(event_id) DO NOTHING to prevent duplicates.
func (s *Store) IngestBatch(rawItems []json.RawMessage) IngestionSummary {
	summary := IngestionSummary{
		Total:  len(rawItems),
		Errors: make([]ItemError, 0),
	}

	// We lock our mutex to serialize write transactions across concurrent webhooks
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		summary.Rejected = len(rawItems)
		summary.Errors = append(summary.Errors, ItemError{
			Index: 0,
			Error: "database transaction failure: " + err.Error(),
		})
		return summary
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO events (event_id, campaign_id, contact_id, type, timestamp, metadata)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(event_id) DO NOTHING;
	`)
	if err != nil {
		summary.Rejected = len(rawItems)
		summary.Errors = append(summary.Errors, ItemError{
			Index: 0,
			Error: "failed to prepare insert statement: " + err.Error(),
		})
		return summary
	}
	defer stmt.Close()

	for idx, raw := range rawItems {
		var ev Event
		if err := json.Unmarshal(raw, &ev); err != nil {
			summary.Rejected++
			summary.Errors = append(summary.Errors, ItemError{
				Index: idx,
				Error: "json decode error: " + err.Error(),
			})
			continue
		}

		utcTime, err := ev.Validate()
		if err != nil {
			summary.Rejected++
			summary.Errors = append(summary.Errors, ItemError{
				Index:   idx,
				EventID: ev.EventID,
				Error:   err.Error(),
			})
			continue
		}

		var metadataJSON sql.NullString
		if len(ev.Metadata) > 0 {
			if b, err := json.Marshal(ev.Metadata); err == nil {
				metadataJSON = sql.NullString{String: string(b), Valid: true}
			}
		}

		// Insert into SQLite. Timestamp is stored formatted as UTC ISO8601 string
		res, err := stmt.Exec(
			ev.EventID,
			ev.CampaignID,
			ev.ContactID,
			ev.Type,
			utcTime.Format(time.RFC3339),
			metadataJSON,
		)
		if err != nil {
			summary.Rejected++
			summary.Errors = append(summary.Errors, ItemError{
				Index:   idx,
				EventID: ev.EventID,
				Error:   "database error: " + err.Error(),
			})
			continue
		}

		rowsAffected, err := res.RowsAffected()
		if err != nil {
			summary.Rejected++
			summary.Errors = append(summary.Errors, ItemError{
				Index:   idx,
				EventID: ev.EventID,
				Error:   "failed to read rows affected: " + err.Error(),
			})
			continue
		}

		if rowsAffected == 0 {
			// Row already existed: event_id was duplicate
			summary.Duplicates++
		} else {
			summary.Accepted++
		}
	}

	if err := tx.Commit(); err != nil {
		summary.Rejected = len(rawItems)
		summary.Accepted = 0
		summary.Duplicates = 0
		summary.Errors = append(summary.Errors, ItemError{
			Index: 0,
			Error: "transaction commit failure: " + err.Error(),
		})
	}

	return summary
}

// GetStats queries SQLite for campaign statistics and calculates derived marketing rates.
func (s *Store) GetStats(campaignID string) (*CampaignStatsResponse, bool) {
	// First check if campaign has any recorded events
	var exists int
	err := s.db.QueryRow("SELECT 1 FROM events WHERE campaign_id = ? LIMIT 1;", campaignID).Scan(&exists)
	if err == sql.ErrNoRows {
		return nil, false
	} else if err != nil {
		return nil, false
	}

	// Aggregate all core counts in a single efficient SQL query
	query := `
	SELECT 
		COUNT(CASE WHEN type = 'sent' THEN 1 END) AS sent,
		COUNT(CASE WHEN type = 'delivered' THEN 1 END) AS delivered,
		COUNT(CASE WHEN type = 'opened' THEN 1 END) AS opened,
		COUNT(CASE WHEN type = 'clicked' THEN 1 END) AS clicked,
		COUNT(DISTINCT CASE WHEN type = 'opened' AND contact_id != '' THEN contact_id END) AS unique_opens,
		COUNT(DISTINCT CASE WHEN type = 'clicked' AND contact_id != '' THEN contact_id END) AS unique_clicks
	FROM events 
	WHERE campaign_id = ?;
	`

	var counts StatsCounts
	err = s.db.QueryRow(query, campaignID).Scan(
		&counts.Sent,
		&counts.Delivered,
		&counts.Opened,
		&counts.Clicked,
		&counts.UniqueOpens,
		&counts.UniqueClicks,
	)
	if err != nil {
		return nil, false
	}

	// Query UTC daily delivered counts
	dailyQuery := `
	SELECT 
		strftime('%Y-%m-%d', timestamp) AS day,
		COUNT(*) AS count
	FROM events
	WHERE campaign_id = ? AND type = 'delivered'
	GROUP BY day
	ORDER BY day ASC;
	`
	rows, err := s.db.Query(dailyQuery, campaignID)
	if err != nil {
		return nil, false
	}
	defer rows.Close()

	dailyDelivered := make(map[string]int)
	for rows.Next() {
		var day string
		var count int
		if err := rows.Scan(&day, &count); err == nil && day != "" {
			dailyDelivered[day] = count
		}
	}

	// Calculate marketing conversion and engagement rates
	rates := StatsRates{}
	if counts.Sent > 0 {
		rates.DeliveryRate = roundFourDecimals(float64(counts.Delivered) / float64(counts.Sent))
	}
	if counts.Delivered > 0 {
		rates.OpenRate = roundFourDecimals(float64(counts.UniqueOpens) / float64(counts.Delivered))
		rates.ClickThroughRate = roundFourDecimals(float64(counts.UniqueClicks) / float64(counts.Delivered))
	}
	if counts.UniqueOpens > 0 {
		rates.ClickToOpenRate = roundFourDecimals(float64(counts.UniqueClicks) / float64(counts.UniqueOpens))
	}

	return &CampaignStatsResponse{
		CampaignID:     campaignID,
		Counts:         counts,
		Rates:          rates,
		DailyDelivered: dailyDelivered,
	}, true
}

// GetEvents returns a paginated list of events for the campaign.
func (s *Store) GetEvents(campaignID string, limit, offset int) (*PaginatedEventsResponse, bool) {
	var total int
	err := s.db.QueryRow("SELECT COUNT(*) FROM events WHERE campaign_id = ?;", campaignID).Scan(&total)
	if err != nil || total == 0 {
		return nil, false
	}

	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}

	query := `
	SELECT event_id, campaign_id, contact_id, type, timestamp, metadata
	FROM events
	WHERE campaign_id = ?
	ORDER BY timestamp ASC, event_id ASC
	LIMIT ? OFFSET ?;
	`
	rows, err := s.db.Query(query, campaignID, limit, offset)
	if err != nil {
		return nil, false
	}
	defer rows.Close()

	events := make([]Event, 0)
	for rows.Next() {
		var ev Event
		var metaStr sql.NullString
		if err := rows.Scan(&ev.EventID, &ev.CampaignID, &ev.ContactID, &ev.Type, &ev.Timestamp, &metaStr); err == nil {
			if metaStr.Valid && metaStr.String != "" {
				_ = json.Unmarshal([]byte(metaStr.String), &ev.Metadata)
			}
			events = append(events, ev)
		}
	}

	return &PaginatedEventsResponse{
		CampaignID: campaignID,
		Total:      total,
		Limit:      limit,
		Offset:     offset,
		Events:     events,
	}, true
}

func roundFourDecimals(val float64) float64 {
	return math.Round(val*10000) / 10000
}
