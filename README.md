# Campaign Events — Engineering Submission (Go + SQLite)

This repository contains the complete implementation, SQLite database persistence, debugging fixes, test suites, and written analyses for the **Campaign Events** engineering assignment.

---

## Repository Navigation & Deliverables

| Deliverable | Location | Description |
| :--- | :--- | :--- |
| **Architectural Notes & Memos** | [`NOTES.md`](./NOTES.md) | **Parts 1, 4, 5:** Problem interpretation, assumptions, PM questions, 100M events/day Scale Memo, Angry Marketer investigation, and reflections. |
| **Debugging Bug Report** | [`BUGS.md`](./BUGS.md) | **Part 3:** Detailed root-cause analysis, minimal diffs, and verification steps for all 4 bugs. |
| **Fixed Debugging Code** | [`debugging/main.go`](./debugging/main.go) | The working CLI program with minimal bug fixes, matching ground truth exactly. |
| **Campaign Events Service** | [`starter/`](./starter/) | **Part 2:** Complete Go webhook ingestion service backed by SQLite (`main.go`, `store.go`, `types.go`, `main_test.go`). |
| **Service Documentation** | [`starter/README.md`](./starter/README.md) | Run instructions, SQLite schema, curl examples, and test guidelines. |
| **AI Usage Disclosure** | [`AI_USAGE.md`](./AI_USAGE.md) | Transparent disclosure of AI tooling, design decisions, and learnings. |

---

## Quick Start Guide

### 1. Webhook Service (`starter/`)

```bash
cd starter

# Run the HTTP service on :8080 (backed by events.db SQLite database)
go run .

# In another terminal: Ingest the seed dataset (235 realistic events)
curl -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  --data-binary @seed/events.json

# Query campaign metrics for cmp_summer_sale
curl http://localhost:8080/campaigns/cmp_summer_sale/stats

# Query paginated recent events
curl "http://localhost:8080/campaigns/cmp_summer_sale/events?limit=5&offset=0"

# Run automated tests
go test -vet=off -v .
```

### 2. Debugging Exercise (`debugging/`)

```bash
cd debugging

# Run the program against the 20,000-line dataset
go run . events.jsonl > actual.txt

# Verify output matches ground truth exactly
diff actual.txt expected_output.txt   # Empty diff confirms 100% match
```

---

## Architectural Highlights

- **SQLite Persistence & Deduplication:** Stores events in an `events` table with `PRIMARY KEY (event_id)` and `INSERT INTO ... ON CONFLICT(event_id) DO NOTHING` to guarantee idempotency across provider retries.
- **Element-by-Element Batch Parsing:** Ingests JSON batches via `[]json.RawMessage` to provide partial-success resilience—one malformed event never aborts valid events in the batch.
- **Concurrency & WAL Mode:** Configured with Write-Ahead Logging (`PRAGMA journal_mode=WAL`) and `PRAGMA busy_timeout=5000` alongside transaction mutexing to eliminate `database is locked` errors under concurrent traffic.
- **Rich Dashboard Analytics:** Returns both raw counts and derived marketing metrics (Delivery Rate, Open Rate, Click-to-Open Rate, and Click-Through Rate) alongside UTC daily delivery breakdowns.
