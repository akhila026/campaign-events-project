# Campaign Events Service (Go + SQLite)

A robust, concurrent Go webhook ingestion service and real-time campaign analytics API for Relay marketing campaigns, backed by **SQLite persistence**.

---

## Architecture & Implementation Overview

- **Storage Engine:** Pure Go SQLite (`modernc.org/sqlite`) with **Write-Ahead Logging (WAL)** and busy timeouts (`PRAGMA busy_timeout = 5000`) for crash-resilient persistence and concurrent read/write support.
- **Batch Ingestion (`POST /events`):** Decodes incoming JSON batches element-by-element using `[]json.RawMessage`. One malformed event does not fail or abort the remaining valid events in the batch.
- **Idempotency & Deduplication:** Enforced by SQLite `PRIMARY KEY (event_id)` and `INSERT INTO events ... ON CONFLICT(event_id) DO NOTHING`. Duplicate provider retries are tracked and never inflate campaign metrics.
- **Marketer Dashboard API (`GET /campaigns/{campaign_id}/stats`):** Serves real-time numbers (sent, delivered, opened, clicked, unique opens, unique clicks), conversion rates (delivery rate, open rate, click-to-open rate, click-through rate), and UTC daily delivery breakdowns.
- **Event Audit Log (`GET /campaigns/{campaign_id}/events`):** Paginated event endpoint supporting `limit` and `offset` query parameters.

---

## SQLite Database Schema

```sql
CREATE TABLE IF NOT EXISTS events (
    event_id    TEXT PRIMARY KEY,
    campaign_id TEXT NOT NULL,
    contact_id  TEXT NOT NULL,
    type        TEXT NOT NULL,
    timestamp   DATETIME NOT NULL,
    metadata    TEXT,
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_events_campaign ON events(campaign_id);
CREATE INDEX IF NOT EXISTS idx_events_campaign_type ON events(campaign_id, type);
CREATE INDEX IF NOT EXISTS idx_events_campaign_contact ON events(campaign_id, contact_id);
CREATE INDEX IF NOT EXISTS idx_events_daily_delivered ON events(campaign_id, type, timestamp);
```

---

## Requirements

- **Go 1.22+** (tested with Go 1.27 / 1.26).
- Zero CGO / C compiler required (uses pure Go transpiled SQLite driver).

---

## Quick Start

### 1. Run the Service

From the `starter/` directory:

```bash
cd starter
go run .
```

The service will create/connect to `events.db` and listen on port `8080`:

```
Campaign Events service (SQLite) listening on :8080
```

### 2. Ingest Events

```bash
curl -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -d '[
    {"event_id":"evt_001","campaign_id":"cmp_summer_sale","contact_id":"ct_010","type":"sent","timestamp":"2026-08-10T06:00:00Z"},
    {"event_id":"evt_002","campaign_id":"cmp_summer_sale","contact_id":"ct_010","type":"delivered","timestamp":"2026-08-10T06:05:00Z"},
    {"event_id":"evt_003","campaign_id":"cmp_summer_sale","contact_id":"ct_010","type":"opened","timestamp":"2026-08-10T07:15:00Z"}
  ]'
```

**Response (HTTP 200 OK):**
```json
{
  "total": 3,
  "accepted": 3,
  "duplicates": 0,
  "rejected": 0
}
```

### 3. Ingest the Seed Dataset

Ingest the full 235-line seed file containing duplicates, bad timestamps, and missing IDs:

```bash
curl -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  --data-binary @seed/events.json
```

**Response (HTTP 200 OK):**
```json
{
  "total": 235,
  "accepted": 211,
  "duplicates": 18,
  "rejected": 6,
  "errors": [
    {"index": 27, "event_id": "evt_00212", "error": "invalid RFC3339 timestamp format: not-a-time"},
    {"index": 59, "event_id": "evt_00211", "error": "invalid RFC3339 timestamp format: 10-08-2026 09:00"},
    {"index": 81, "error": "missing or empty event_id"},
    {"index": 127, "event_id": "evt_00213", "error": "missing or empty contact_id"},
    {"index": 169, "event_id": "evt_00214", "error": "missing or empty contact_id"},
    {"index": 175, "event_id": "evt_00215", "error": "unsupported or invalid event type: spam_report"}
  ]
}
```

### 4. Fetch Campaign Statistics

```bash
curl http://localhost:8080/campaigns/cmp_summer_sale/stats
```

**Response:**
```json
{
  "campaign_id": "cmp_summer_sale",
  "counts": {
    "sent": 40,
    "delivered": 35,
    "opened": 28,
    "unique_opens": 20,
    "clicked": 12,
    "unique_clicks": 12
  },
  "rates": {
    "delivery_rate": 0.875,
    "open_rate": 0.5714,
    "click_to_open_rate": 0.6,
    "click_through_rate": 0.3429
  },
  "daily_delivered": {
    "2026-08-10": 35
  }
}
```

### 5. Fetch Paginated Events

```bash
curl "http://localhost:8080/campaigns/cmp_summer_sale/events?limit=5&offset=0"
```

---

## Running Automated Tests

Run the test suite (covering duplicates, invalid event isolation, rate calculations, daily bucketing, and concurrent requests):

```bash
cd starter
go test -vet=off -v .
```
