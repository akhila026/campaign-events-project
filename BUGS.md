# Debugging Exercise — Bug Report (`BUGS.md`)

This document records the four bugs identified and fixed in `debugging/main.go`. Each bug is analyzed according to the requirements: what it is, why it happened, the minimal fix applied, and how it was verified.

---

## Bug 1: Local Batch Deduplication Instead of Global Deduplication

### 1. What the bug is
The deduplication map `seen` was declared locally inside `processBatch()`, causing `event_id` tracking to reset after every batch of 200 events. As a result, duplicate events arriving across different batches were processed repeatedly, corrupting all campaign metrics.

### 2. Why it happens
Requirement 1 specifies: *"Each `event_id` must be counted at most once across the whole file."*
In the original code:
```go
func processBatch(events []Event) {
    seen := make(map[string]bool) // re-created for every batch of 200!
    ...
```
Because `seen` was re-initialized on every invocation of `processBatch()`, an event that appeared in Batch #1 and was retried by the provider in Batch #2 or later was not recognized as a duplicate and was counted multiple times.

### 3. Minimal Fix
Moved `seen` from local function scope to package scope so that it persists across all batches for the lifetime of the process:

```diff
+// seen tracks event IDs processed across the entire file, so duplicates
+// spanning multiple batches are not processed more than once.
+var seen = map[string]bool{}

 func processBatch(events []Event) {
-	seen := make(map[string]bool) // event IDs we have already processed
 	unique := make([]Event, 0, len(events))
```

### 4. How you verified the fix
- Verified that duplicate events appearing across batch boundaries (such as retried event IDs) are filtered out and only the first occurrence reaches `unique`.
- Confirmed against `events.jsonl` that total sent, delivered, opened, and clicked counts decreased to match `expected_output.txt`.

---

## Bug 2: Global Unique Opens Across All Campaigns

### 1. What the bug is
`openedBy` tracked contact IDs globally without partitioning by campaign ID. When a contact opened messages in two different campaigns, the contact was only counted in the first campaign and falsely ignored in all subsequent campaigns.

### 2. Why it happens
Requirement 2 specifies: *"unique_opens = number of distinct contacts that opened, per campaign. One contact opening five times in a campaign counts once. The same contact opening in two campaigns counts once in each."*
In the original code:
```go
var openedBy = map[string]bool{}
...
func track(ev Event) {
    switch ev.Type {
    case "opened":
        if !openedBy[ev.ContactID] {
            openedBy[ev.ContactID] = true
            cs.UniqueOpens++
        }
```
Because the map key was only `ev.ContactID`, once contact `ct_1224` opened an email in Campaign A, `openedBy["ct_1224"]` became `true`. When that same contact later opened an email in Campaign B, the check `!openedBy[ev.ContactID]` evaluated to `false`, causing Campaign B's unique open count to be undercounted.

### 3. Minimal Fix
Scoped the map key by both `CampaignID` and `ContactID`:

```diff
 	case "opened":
-		if !openedBy[ev.ContactID] {
-			openedBy[ev.ContactID] = true
+		key := ev.CampaignID + ":" + ev.ContactID
+		if !openedBy[key] {
+			openedBy[key] = true
 			cs.UniqueOpens++
 		}
```

### 4. How you verified the fix
- Cross-checked contacts who received messages across multiple campaigns (e.g. `cmp_A`, `cmp_B`, `cmp_C`).
- Verified that `unique_opens` for each campaign accurately reflects distinct contacts within that campaign, matching `expected_output.txt` exactly.

---

## Bug 3: Daily Delivery Bucketing Uses Local Timezone Instead of UTC

### 1. What the bug is
`track()` used `.Local()` when formatting event timestamps for daily delivery bucketing, converting UTC timestamps into the machine's local timezone.

### 2. Why it happens
Requirement 3 specifies: *"Daily delivered buckets use the event's UTC date. The `timestamp` field is UTC."*
In the original code:
```go
case "delivered":
    day := ev.Timestamp.Local().Format("2006-01-02")
    cs.DailyDelivered[day]++
```
Depending on the local machine's timezone offset from UTC (for example, UTC+05:30 in India or UTC-08:00 in US Pacific), events near midnight UTC shift into the previous or next calendar day. This caused non-deterministic daily bucket counts across different machines and geographical regions.

### 3. Minimal Fix
Changed `.Local()` to `.UTC()`:

```diff
 	case "delivered":
-		day := ev.Timestamp.Local().Format("2006-01-02")
+		day := ev.Timestamp.UTC().Format("2006-01-02")
 		cs.DailyDelivered[day]++
```

### 4. How you verified the fix
- Tested delivery timestamp conversion against events near 00:00:00Z and 23:59:59Z.
- Verified that daily delivered bucket counts for all campaigns (`cmp_A` through `cmp_E`) from `2026-08-01` to `2026-08-07` matched `expected_output.txt` exactly regardless of local timezone.

---

## Bug 4: Concurrency Data Race in Counter Increments

### 1. What the bug is
`processBatch()` spawns a worker pool of 8 concurrent goroutines that pull events from a shared channel and execute `apply(ev)`. In `apply()`, multiple workers concurrently increment counter fields (`cs.Sent++`, `cs.Delivered++`, etc.) on the shared `*CampaignStats` pointer without synchronization.

### 2. Why it happens
Requirement 4 specifies: *"Output must be identical across runs on the same input."*
In Go, the `++` operator on an integer is not atomic—it is a non-atomic read-modify-write operation. When multiple goroutines execute `cs.Sent++` on the same campaign struct concurrently:
1. Goroutine A reads current value (e.g., 10).
2. Goroutine B reads current value (10).
3. Goroutine A writes 11.
4. Goroutine B writes 11 (lost update!).

This creates a severe **data race** (detectable with `go run -race . events.jsonl`), causing inconsistent, run-to-run variable counts and undefined behavior.

### 3. Minimal Fix
Added a `sync.Mutex` to `CampaignStats` and locked it inside `apply()` during counter updates:

```diff
 type CampaignStats struct {
+	mu             sync.Mutex
 	Sent           int
 	Delivered      int
 	Opened         int
 	Clicked        int
 	UniqueOpens    int
 	DailyDelivered map[string]int
 }
...
 func apply(ev Event) {
 	cs := stats[ev.CampaignID]
+	cs.mu.Lock()
+	defer cs.mu.Unlock()
 	switch ev.Type {
 	case "sent":
 		cs.Sent++
 	case "delivered":
 		cs.Delivered++
 	case "opened":
 		cs.Opened++
 	case "clicked":
 		cs.Clicked++
 	}
 }
```

Using a mutex on `CampaignStats` (rather than a single global lock) allows events for different campaigns to be updated in parallel without contention, while ensuring thread safety for events belonging to the same campaign.

### 4. How you verified the fix
- Ran the simulation against `debugging/events.jsonl` (20,000 lines) and confirmed that the generated output matches `expected_output.txt` character-for-character every time.
- Verified absence of data races under Go's race detector model.

