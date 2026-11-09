# AI Usage Disclosure (`AI_USAGE.md`)

This document discloses how AI tools were utilized during the development, debugging, and architectural design of the Campaign Events project, as required by the assignment guidelines.

---

## 1. Tools Used
- **Claude / Antigravity Assistant:** Used for codebase exploration, reviewing PDF and markdown assignment instructions, generating unit test scenarios, drafting initial architecture diagrams, and verifying concurrency race conditions.
- **Python Standard Library (as a scratchpad verification tool):** Used to simulate ground truth outputs on large input datasets (`events.jsonl` and `seed/events.json`) to confirm bug fixes mathematically before editing Go files.

---

## 2. What Tools Were Used For
1. **Assignment Requirement Mapping:** Analyzing ambiguous requirements in the PM brief (e.g. definitions of unique opens, error-handling contracts for mixed batches, and concurrency constraints).
2. **Identifying Intentional Bugs in `debugging/`:** Pinpointing the four subtle bugs in `debugging/main.go`—specifically the batch-local `seen` map, cross-campaign `openedBy` tracking, `.Local()` timezone conversion, and unsynchronized data race in `apply()`.
3. **Designing Resilient Batch Ingestion:** Formulating the `[]json.RawMessage` pattern in Go to allow partial batch acceptance rather than failing an entire 200-event array when a single element is malformed.
4. **Scale Memo & Marketer Scenario Reasoning:** Brainstorming edge-case explanations for real-world anomalies (such as security scanner bot pre-fetching causing retroactive open count adjustments, and at-least-once queue delivery challenges at 100M events/day).

---

## 3. One Suggestion From AI That Was Rejected or Changed (And Why)
* **The AI Suggestion:** When addressing Bug #4 (concurrency data race across workers in `debugging/main.go`), an initial AI prompt suggested wrapping the entire worker loop in a single global mutex lock or converting all worker counters to `sync/atomic` 64-bit integer operations.
* **Why It Was Rejected/Changed:** 
  1. A single global lock creates unnecessary contention across different campaigns that don't share state.
  2. `sync/atomic` only works on basic scalar primitives and doesn't protect complex types like maps (`DailyDelivered`), leaving the code partially vulnerable if expanded.
  3. Replacing `CampaignStats` fields with atomics would have required rewriting struct types, violating the assignment constraint: *"Make minimal fixes. Do not rewrite or restructure the program."*
  * **Our Final Decision:** Added a mutex directly to `CampaignStats` and locked it cleanly inside `apply()`. This was a minimal 4-line change, kept the struct fields clean `int`s, and allowed parallel processing across different campaigns.

---

## 4. One Thing AI Helped Me Understand
* **Go JSON Array Decoding Pitfall:** When using standard `json.Unmarshal(body, &[]Event{})`, Go's decoder halts at the first syntax or field type error (such as `"timestamp": "not-a-time"`). This would reject an entire batch of 200 events even if 199 were completely valid. The AI demonstrated how decoding into `[]json.RawMessage` first allows element-by-element deserialization, providing granular error reporting while saving valid events.

---

## 5. Anything AI Generated That Had to Be Debugged
* During the implementation of derived rates in `store.go`, an early AI draft calculated `ClickToOpenRate` as `counts.Clicked / counts.Opened`.
* **The Debug:** In email marketing analytics, click-to-open rate (CTOR) is specifically defined as **Unique Clicks divided by Unique Opens** (or Total Clicks / Total Opens depending on configuration, but never mixed). More importantly, if a campaign has 0 opens, dividing by zero in Go produces `NaN` or panic on integer division. We updated the logic to check `if counts.UniqueOpens > 0` before calculating, and rounded all rates cleanly to 4 decimal places.

