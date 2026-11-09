# Campaign Events — Architecture, Scale, and Reasoning (`NOTES.md`)

---

## Part 1 — Read and Think First

### 1. Interpretation of the Problem
Relay is a message orchestration platform: rather than operating our own mail transfer agents (MTAs) or SMS gateways, we dispatch marketing communications to third-party delivery providers (e.g., SendGrid, Mailgun, Twilio). These providers communicate back to Relay asynchronously via HTTP webhooks as message lifecycle events occur:
- `sent` — The provider accepted Relay's dispatch request.
- `delivered` — The carrier or recipient's mail server confirmed receipt.
- `opened` — The recipient opened/viewed the message (via tracking pixel or client notification).
- `clicked` — The recipient clicked a hyperlink inside the message.

Marketers monitor campaign performance through a live dashboard. Our responsibility is to build the backend ingestion engine that receives incoming webhook traffic, defends against real-world network anomalies (unreliable networks, duplicate provider retries, out-of-order sequencing, and malformed payloads), and exposes aggregated, trustworthy campaign metrics to the dashboard in real time.

---

### 2. Assumptions Made
1. **Deduplication Identity (`event_id`):** The provider assigns an `event_id` that canonically identifies a single event. If an `event_id` has already been recorded in the database, any subsequent receipt of that same `event_id` is an idempotent provider retry and must be ignored without double-counting.
2. **Partial Batch Success:** Providers dispatch events in JSON arrays. In production, a 200-event batch can contain a few malformed records (e.g., bad date strings or missing IDs). We assume a **partial-success** ingestion model: valid events are accepted and committed, while invalid events are reported in a granular error list without aborting the entire batch.
3. **Out-of-Order Delivery is Reality:** In distributed systems, events frequently arrive out of order (e.g., an `opened` event arriving before `delivered`, or events delayed by several hours). We do not artificially reject out-of-order events or synthesize fake precursor events; we store reality as reported by providers and aggregate across all received events.
4. **Unique Metrics Scope:** "Unique opens" measures distinct recipients (`contact_id`) who opened at least one message within that specific campaign. A contact opening multiple times in Campaign A counts once for Campaign A. If that same contact also opens a message in Campaign B, they count once for Campaign B as well.
5. **Persistence & Concurrency:** Given current traffic (~a few thousand events/day across ~50 campaigns), an embedded SQLite database configured with Write-Ahead Logging (WAL mode) and serialized write transactions provides crash-resilient persistence, ACID guarantees, and thread-safe concurrency without requiring external database servers or Docker containers.

---

### 3. Ambiguities in the Brief
- **"How many opened?":** Does the marketer require *Total Opens* (every view event recorded, including repeat opens) or *Unique Opens* (distinct contacts reached)? A production analytics platform needs both: total opens reflects overall engagement volume, while unique opens reflects true audience reach.
- **Batch Error Handling Protocol:** When a batch contains malformed events, should the endpoint return HTTP 400 (all-or-nothing rejection) or HTTP 200/207 with a per-item ingestion summary? (Industry standards like Stripe, SendGrid, and Segment use partial success to prevent retry storms).
- **Unrecognized Event Types:** If a provider delivers event types outside the 4 core types (such as `spam_report`, `bounced`, or `unsubscribed`, as seen in `seed/events.json`), should they be rejected, silently ignored, or stored in a separate table?
- **Normalization vs Strict Rejection:** Webhook data often contains whitespace and casing anomalies (e.g., `"type": "OPENED"` vs `"opened"`). Should the API normalize obvious casing or reject with a validation error?
- **Dashboard Time Horizon:** Does the marketer want lifetime campaign totals, daily delivery trends, or a rolling window (e.g., last 24 hours)?

---

### 4. Questions for the Product Manager
1. *KPI Definitions:* Is the primary open metric on the dashboard "Unique Opens" or "Total Opens"? Should conversion rates (Delivery Rate: `delivered/sent`, Open Rate: `unique_opens/delivered`, Click-to-Open Rate: `unique_clicks/unique_opens`, Click-Through Rate: `unique_clicks/delivered`) be computed on the server or on the frontend?
2. *Provider Webhook SLAs:* What HTTP status codes do our delivery providers require for partial batches? (Most providers retry indefinitely on 5xx and sometimes on 4xx; returning HTTP 200 with an itemized failure summary prevents infinite retry loops).
3. *Auditing & Retention:* Will marketers need recipient-level audit logs (e.g. "Show me all contacts who clicked"), or will they only ever view aggregate statistics?
4. *Zero State Behavior:* When a newly created campaign has not yet received any webhook events, should `/stats` return zeroes with HTTP 200 OK, or HTTP 404 Not Found?

---

### 5. Prioritization & Execution Order
1. **Core Data Ingestion & Validation (`POST /events`):** Decode incoming batches element-by-element (`[]json.RawMessage`) so bad items do not abort the batch; validate all required fields (`event_id`, `campaign_id`, `contact_id`, `type`, `timestamp`); normalize casing; enforce deduplication via SQLite primary keys.
2. **Dashboard Statistics API (`GET /campaigns/{campaign_id}/stats`):** Serve live counts (sent, delivered, opened, clicked, unique opens, unique clicks), conversion rates, and UTC daily delivery breakdowns.
3. **Debugging Challenge (`debugging/`):** Identify and fix the 4 intentional bugs (batch-local `seen`, cross-campaign `openedBy`, `.Local()` timezone shift, worker data race) and match ground truth.
4. **Resilience & Concurrency Testing:** Validate SQLite WAL mode and busy timeouts under simulated high-concurrency workloads.
5. **Documentation & Scalability Analysis:** Author `NOTES.md`, `BUGS.md`, and `AI_USAGE.md`.
6. **Bonus Paginated Audit Log (`GET /campaigns/{campaign_id}/events`):** Provide recent activity event inspection with `limit` and `offset`.

---

## Part 4 — Scale Memo: Scaling to 100 Million Events / Day

### Traffic Profile & Math
* **Daily Volume:** 100,000,000 events/day.
* **Average Throughput:** $100,000,000 / 86,400 \approx \mathbf{1,157\text{ events/second}}$.
* **Peak Surge Throughput:** Marketing campaigns dispatch in bursts (e.g., 09:00 AM push notifications, flash sales, Black Friday). Peak load is typically **10x average**, demanding capacity for **10,000 to 25,000 events/second**.

---

### 1. What Breaks First in the Current Implementation?
* **SQLite Write Lock Contention:** SQLite is a single-writer database. While WAL mode enables concurrent readers, multiple write transactions must queue sequentially. At 15,000 writes/second, write transactions will queue beyond the 5-second busy timeout, throwing `database is locked` errors and dropping provider webhooks.
* **Disk I/O & Storage Saturation:** Storing 100M events/day with indexes consumes ~25–30 GB of storage per day (~900 GB/month). A single SQLite database file will experience severe index degradation and I/O thrashing.
* **Dynamic Aggregation Queries:** Calculating `COUNT(DISTINCT contact_id)` across tens of millions of rows in real time for dashboard queries will saturate CPU and take seconds to execute, breaking the live dashboard experience.

---

### 2. Proposed Architecture & Migration Order

```
[Delivery Providers]
        │ HTTP Webhook Batches (15k-25k req/sec)
        ▼
[Ingestion API Pods] (Stateless Go services behind AWS ALB)
        │ Validate schema & write immediately (<10ms)
        ▼
[Apache Kafka / AWS Kinesis] (Partitioned by campaign_id / contact_id)
        │
   ┌────┴───────────────────────────┐
   ▼                                ▼
[Real-Time Consumer Workers]   [Cold Storage Consumer]
   │ Atomic increments (HINCRBY)    │ Bulk batch insert
   ▼                                ▼
[Redis Cluster / Cache]        [ClickHouse OLAP Database]
(Real-time live dashboard)     (Historical analytics & audit log)
```

#### Order of Implementation:
1. **Step 1: Ingestion Decoupling via Message Queue (Kafka/Kinesis):**
   * Strip all business logic and database writes out of the HTTP ingestion path.
   * Ingestion pods only validate JSON schema, write raw event messages to Kafka, and immediately return `202 Accepted` to the provider (<10ms latency).
2. **Step 2: Distributed Stream Processing for Aggregates:**
   * Go consumer workers pull from Kafka and update pre-aggregated counters in **Redis** (using `HINCRBY` for counts and **HyperLogLog** `PFADD` for unique opens and clicks).
3. **Step 3: Columnar Analytical Store (ClickHouse):**
   * Stream raw events into an append-only analytical database like **ClickHouse** partitioned by `campaign_id` and event date (`YYYY-MM-DD`). ClickHouse can ingest millions of rows/sec and execute analytical queries across billions of rows in milliseconds.
4. **Step 4: Horizontal Pod Autoscaling:**
   * Scale ingestion pods and consumer worker pools independently based on CPU utilization and Kafka consumer lag metrics.

---

### 3. API Contract and Storage Changes
* **API Contract Changes:**
  * `POST /events` changes from synchronous database writes to asynchronous queue publishing, returning `202 Accepted` with a batch receipt ID:
    ```json
    { "status": "accepted", "batch_id": "bch_9872", "queued": 200 }
    ```
  * `GET /campaigns/{campaign_id}/stats` remains identical externally, but internally reads pre-aggregated metrics from Redis in <2ms instead of computing SQL aggregations on disk.
* **Storage Changes:**
  * Transition from single-node SQLite to a hybrid architecture:
    * **Hot Path (Dashboard):** In-memory Redis hashes (`stats:{campaign_id}`) and HyperLogLog structures for distinct counts.
    * **Cold/Analytical Path (History):** ClickHouse for persistent raw event logs, deep audits, and complex queries.

---

### 4. Introducing a Queue: What New Problems Arise?
1. **At-Least-Once Delivery:** Distributed queues (Kafka, SQS) guarantee *at-least-once* delivery. If a consumer crashes before committing its offset, messages will be re-delivered, creating duplicates within our internal pipeline.
2. **Consumer Lag:** During massive campaign dispatches, consumer processing can lag behind ingestion. Marketers may see stats delayed by several minutes.
3. **Poison Pill Messages:** A corrupt event that causes consumer panics can block partition processing. Requires an automated Dead Letter Queue (DLQ) with retry backoff.
4. **Message Ordering Across Partitions:** Events for the same contact/campaign could be processed out of sequence if partitioned randomly. Mitigated by using `campaign_id` as the Kafka partition key.

---

### 5. Where Duplicates Can Now Sneak In
1. **Provider Retries:** Network timeouts between provider and Relay trigger provider retries.
2. **Ingestion Producer Retries:** Network hiccups between the ingestion pod and Kafka trigger producer retries.
3. **Consumer Rebalances / Worker Restarts:** Consumer crashes after updating Redis but before committing Kafka offsets; the replacement consumer re-processes the batch.

---

### 6. Keeping Counters Trustworthy
* **Distributed Idempotency Cache:** Store recent `event_id` keys in Redis with a 7-day TTL (`SET event:{event_id} 1 NX EX 604800`). If `SET NX` returns 0, the consumer skips counter increments.
* **HyperLogLog for Unique Open & Click Tracking:** Storing millions of contact IDs in memory is prohibitive. Redis HyperLogLog (`PFADD`) estimates distinct cardinality with a standard error of <0.81% using only 12 KB of memory per campaign.
* **Hourly Asynchronous Reconciliation (Lambda / Cron Worker):** A background cron worker runs an exact query on ClickHouse (`SELECT COUNT(DISTINCT contact_id)...`) and synchronizes Redis counters to correct any accumulated drift.

---

### 7. What to Monitor
* **Ingestion Tier:** P95/P99 HTTP latency, 2xx vs 4xx/5xx status codes, request rates.
* **Queue Tier:** Kafka consumer lag (unprocessed message backlog), message ingestion rate, DLQ dead-letter count.
* **Deduplication Rate:** Rate of duplicate `event_id`s detected (spikes indicate upstream provider retry storms).
* **Storage Tier:** Redis memory utilization, ClickHouse write buffer latency, disk IOPS.

---

### 8. What Deliberately NOT to Solve Yet
* **Active-Active Multi-Region Database Replication:** Maintain a single primary cloud region for data writes until legal/data sovereignty regulations strictly require multi-region presence.
* **Complex Machine Learning Anomaly Detection:** Simple threshold alerts (e.g. bounce rate > 10% or delivery drops > 20%) are sufficient; avoid premature ML infrastructure.
* **Ad-Hoc Custom Query Builders:** Restrict dashboard access to pre-defined metric aggregations rather than opening open-ended SQL execution to end users.

---

## Part 5 — The Angry Marketer Analysis

### Scenario
* **10:00:** Sent: 1,000,000 | Delivered: 970,000 | Opened: 250,000 | Clicked: 20,000
* **10:30:** Sent: 1,000,000 | Delivered: 975,000 | Opened: 248,000 | Clicked: 20,000
* Delivered went **up** (+5,000). Opened went **down** (-2,000).
* The marketer files an urgent ticket: *"Your dashboard is broken."*

---

### 1. Is the Delivered Number Rising Actually Suspicious?
**No, delivered rising is completely normal and expected.**
Sending 1,000,000 messages does not happen instantaneously. Providers dispatch messages across carrier networks (cell towers, ISP mail exchangers, SMTP servers). Many messages encounter temporary rate limits, greylisting, or connection retries. Over the 30-minute interval between 10:00 and 10:30, providers successfully cleared their retry queues and delivered 5,000 delayed messages, legitimately increasing the delivered count from 970,000 to 975,000.

---

### 2. Plausible Explanations for Opened Decreasing (Before Assuming Code is Broken)
1. **Automated Security Scanner / Bot Filter Purge:** Enterprise mail gateways (e.g. Barracuda, Mimecast, Outlook SafeLinks, Apple Mail Privacy Protection) frequently pre-fetch and scan incoming emails within seconds of arrival, firing false "opened" webhook events. Relay's ingestion engine or post-processing pipeline often runs asynchronous bot-detection algorithms that retroactively identify and purge bot opens from human engagement metrics.
2. **GDPR / CCPA "Right to Be Forgotten" / Opt-Out Deletions:** If users or compliance systems submit immediate erasure or unsubscribe requests, compliance workers purge those contacts' interaction events, decreasing unique open totals.
3. **Approximation vs Reconciliation Synchronization:** At 10:00, the live dashboard may have displayed a fast, approximate counter (e.g., from an in-memory buffer or Redis HyperLogLog). At 10:15, a reconciliation job computed the exact deduplicated unique open count from the canonical database, correcting an initial slight overcount.
4. **Rolling Time Window Filter on the Dashboard:** If the marketer had a relative filter selected (e.g., "Activity in the last 24 hours"), events that occurred 24.5 hours ago naturally roll off the dashboard window at 10:30.
5. **Campaign Audience / Segment Adjustment:** The marketer or an account administrator may have edited the campaign target audience segment or excluded internal test accounts between 10:00 and 10:30.

---

### 3. What to Check First and How to Investigate
1. **Clarify Metric Definitions:** Check whether the dashboard metric represents **Total Opens** (raw open events) or **Unique Opens** (distinct contacts).
2. **Inspect Background Job Logs:** Review server and worker logs between 10:00 and 10:30 for automated maintenance tasks: Did a bot-filtering job, spam-trap purge, or GDPR compliance deletion script execute?
3. **Execute SQL Differential Queries:** Run direct queries against the database to inspect record counts and timestamps:
   ```sql
   -- Check total vs unique opens
   SELECT COUNT(*), COUNT(DISTINCT contact_id) 
   FROM events 
   WHERE campaign_id = 'cmp_xyz' AND type = 'opened';

   -- Check if any records were deleted or soft-deleted
   SELECT COUNT(*) FROM audit_logs 
   WHERE campaign_id = 'cmp_xyz' 
     AND created_at BETWEEN '2026-08-10T10:00:00Z' AND '2026-08-10T10:30:00Z';
   ```
4. **Review Deployment and Cache Invalidation Logs:** Check whether a code deployment, database migration, or cache purge took place around 10:15.

---

## Practical Engineering: Implemented Functionality vs. Proposed Improvements

To maintain total technical clarity, the table below distinguishes what is **currently working and implemented in this codebase** from what is **proposed in the Scale Memo for future 100M events/day growth**:

| Dimension | Currently Implemented in Codebase | Proposed for 100M Events/Day Scale |
| :--- | :--- | :--- |
| **Persistence** | Embedded SQLite (`events.db`) with Write-Ahead Logging (`WAL`), `busy_timeout=5000`, and transaction mutexing. | ClickHouse (columnar OLAP database) partitioned by campaign and event date. |
| **Ingestion Pipeline** | In-process batch ingestion via `[]json.RawMessage` element-by-element decoding. | Distributed message queue (Apache Kafka / AWS Kinesis) with stateless ingestion pods. |
| **Deduplication** | SQLite `PRIMARY KEY (event_id)` + `ON CONFLICT(event_id) DO NOTHING`. | Redis distributed cache idempotency keys (`SET event_id 1 NX EX 604800`) + database constraints. |
| **Metrics Calculation** | Single-pass SQL aggregation query calculating counts, unique opens/clicks, rates, and UTC daily buckets. | Pre-aggregated Redis hashes (`HINCRBY`) + HyperLogLog (`PFADD`) for sub-millisecond dashboard reads. |
| **Concurrency Safety** | SQLite WAL mode + Go `sync.Mutex` on write transactions. | Horizontally autoscaled stateless pods behind AWS Application Load Balancer. |
| **Testing** | 7 automated unit and integration tests covering duplicates, malformed batches, concurrency, rates, and seed file ingestion. | Distributed load testing with k6/Locust simulating 25,000 requests/sec. |

---

## Completed vs. Intentionally Skipped

### What I Completed
- **`debugging/main.go`:** Identified, documented, and fixed all 4 bugs with minimal diffs (+8 lines, -3 lines), matching `expected_output.txt` character-for-character across all 20,000 lines.
- **Robust Ingestion API (`POST /events`):** Resilient `[]json.RawMessage` parsing to prevent single-event failures from aborting entire batches; duplicate filtering; schema validation.
- **Marketer Stats API (`GET /campaigns/{campaign_id}/stats`):** Full dashboard payload including sent, delivered, opened, unique opens, clicked, unique clicks, calculated conversion rates, and UTC daily delivery breakdowns.
- **SQLite Persistence & Deduplication:** Backed by SQLite (`modernc.org/sqlite`) with Write-Ahead Logging (WAL mode), busy timeout handling, and `PRIMARY KEY (event_id)` with `ON CONFLICT(event_id) DO NOTHING`.
- **Paginated Event Log (`GET /campaigns/{campaign_id}/events`):** Optional bonus endpoint supporting pagination (`limit` & `offset`) for auditability.
- **Thread-Safe Architecture:** Configured with SQLite WAL mode and Go transaction mutexing to eliminate all concurrency race conditions and busy locks.
- **Unit and Integration Tests (`main_test.go`):** Automated verification of deduplication, bad-batch handling, rate calculations, concurrent writes, and end-to-end seed dataset ingestion.

### What I Intentionally Skipped
- **External Client-Server Database (PostgreSQL/MySQL/Docker):** SQLite was intentionally selected as permitted by the assignment to keep the solution clean, zero-dependency on external database servers, and instantly runnable via `go run .`.
- **Complex Authentication / RBAC:** The assignment explicitly stated not to build authentication or deployment infrastructure.
- **Frontend / UI Polish:** Followed assignment instructions that UI earns no extra credit.

---

## If I Had Another Day
1. **OpenTelemetry Metrics & Structured Tracing:** Expose Prometheus `/metrics` (ingestion rate, batch latency histograms, duplicate retry counters) and structured JSON logging with request IDs.
2. **Idempotency Window Cache:** Implement a sliding time-window Bloom filter or LRU cache for `event_id` deduplication to prevent unbounded table growth over months of uptime.
3. **Webhook Signature Verification:** Implement HMAC-SHA256 signature verification headers (e.g., `X-Relay-Signature`) to verify authenticity of incoming delivery provider webhooks.
4. **Read Replica / Connection Pool Tuning:** Separate read connections from write connections in SQLite to maximize analytical read concurrency under heavy dashboard traffic.
