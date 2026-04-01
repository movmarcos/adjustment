# Sequential Per-Scope Processing Pipeline — Design Spec
**Date:** 2026-04-01  
**Status:** Approved

---

## Problem

The legacy processing system has a single global queue. A long-running FRTB adjustment (15+ minutes) blocks all other scopes. Non-overlapping adjustments within the same scope also queue behind each other unnecessarily. Different teams (VaR, Stress, FRTB, Sensitivity) have no isolation from each other, causing complaints and gaps in the process.

---

## Goals

1. Independent processing pipelines per Data Scope — a 15-minute FRTB run has zero impact on VaR, Stress, or Sensitivity.
2. Within a scope, non-overlapping adjustments process in parallel (same task run). Only adjustments that would affect the same source rows are blocked behind the running one.
3. Deduplication enforced at the fact table level using `FACT_TABLE_PK` from `ADJUSTMENTS_SETTINGS` — only the most recent adjustment delta per source row is kept.
4. Clean, observable statuses: `Pending → Running → Processed / Failed`.

---

## Scope Pipelines

Four independent pipelines:

| Pipeline | PROCESS_TYPE values |
|----------|-------------------|
| VaR | `VaR` |
| Stress | `Stress` |
| FRTB | `FRTB`, `FRTBDRC`, `FRTBRRAO`, `FRTBALL` |
| Sensitivity | `Sensitivity` |

Each pipeline has its own: **view → stream → task**.

---

## Schema Changes

### 1. New column on `ADJ_HEADER`

```sql
BLOCKED_BY_ADJ_ID   NUMBER(38,0)   DEFAULT NULL
```

- `NULL` = adjustment is eligible to be picked up (not blocked)
- Set to a Running `ADJ_ID` = adjustment is waiting for that one to finish before it can be processed

### 2. Status simplification

Processing pipeline statuses (applies after any approval workflow):

| Status | Meaning |
|--------|---------|
| `Pending` | Submitted, waiting to be picked up |
| `Running` | Currently being processed |
| `Processed` | Successfully applied to the fact table |
| `Failed` | Processing error — see `ERRORMESSAGE` |

> The approval statuses (`Pending Approval`, `Approved`) remain unchanged — they precede the pipeline and are a separate concern.

---

## Pipeline Objects

### Views (one per scope)

Simple single-table views on `ADJ_HEADER`. Snowflake change-tracking is enabled on each view so streams work reliably.

```sql
-- Example: VaR queue view
CREATE VIEW VW_QUEUE_VAR AS
SELECT * FROM ADJ_HEADER
WHERE PROCESS_TYPE = 'VaR'
  AND RUN_STATUS = 'Pending'
  AND BLOCKED_BY_ADJ_ID IS NULL
  AND IS_DELETED = FALSE;
```

Views for Stress, FRTB (IN clause for the 4 PROCESS_TYPEs), and Sensitivity follow the same pattern.

### Streams (one per view)

```sql
CREATE STREAM STREAM_QUEUE_VAR ON VIEW VW_QUEUE_VAR APPEND_ONLY = TRUE;
```

Streams detect when new rows become eligible (i.e., when `BLOCKED_BY_ADJ_ID` is cleared or a new Pending adjustment is submitted with no overlap). `APPEND_ONLY = TRUE` because we only care about rows becoming eligible, not rows leaving the view.

### Tasks (one per scope)

Each task:
- Is triggered by `SYSTEM$STREAM_HAS_DATA('STREAM_QUEUE_<SCOPE>')`
- Runs on its own warehouse (scopes don't share compute)
- Processes all currently eligible adjustments in a single run

```
TASK_PROCESS_VAR          → triggered by STREAM_QUEUE_VAR
TASK_PROCESS_STRESS       → triggered by STREAM_QUEUE_STRESS
TASK_PROCESS_FRTB         → triggered by STREAM_QUEUE_FRTB
TASK_PROCESS_SENSITIVITY  → triggered by STREAM_QUEUE_SENSITIVITY
```

---

## Task Execution Logic

Each task runs the following steps in order:

### Step 1 — Claim all eligible adjustments atomically

```sql
UPDATE ADJ_HEADER
SET RUN_STATUS = 'Running', PROCESS_DATE = CURRENT_TIMESTAMP()
WHERE PROCESS_TYPE IN (<scope types>)
  AND RUN_STATUS = 'Pending'
  AND BLOCKED_BY_ADJ_ID IS NULL
  AND IS_DELETED = FALSE
RETURNING ADJ_ID;
```

All eligible adjustments are claimed in one shot. This prevents a second task run from double-processing while the first is still executing.

### Step 2 — Set blocking for newly Running adjustments

For each newly-Running adjustment, find all other `Pending` adjustments in the same scope + same COBID that overlap with it (using the wildcard dimension logic below). Set their `BLOCKED_BY_ADJ_ID` to this Running `ADJ_ID`.

### Step 3 — Process each Running adjustment

For each claimed adjustment (ordered by `CREATED_DATE ASC`), call the processing engine (`SP_PROCESS_ADJUSTMENT`). The engine reads the fact table, applies the delta, and writes to the adjustment fact table using MERGE on `FACT_TABLE_PK`.

### Step 4 — Mark complete and unblock

On success: `RUN_STATUS = 'Processed'`.  
On failure: `RUN_STATUS = 'Failed'`, write error to `ERRORMESSAGE`.

Then: find all adjustments where `BLOCKED_BY_ADJ_ID = this ADJ_ID`. For each:
- Check if any *other* currently `Running` adjustment in the same scope still overlaps with it
- If yes → reassign `BLOCKED_BY_ADJ_ID` to that other Running adjustment
- If no → set `BLOCKED_BY_ADJ_ID = NULL` (row reappears in the view, stream fires, task re-triggers)

---

## Overlap Detection Logic

Two adjustments overlap if they share the same `COBID` + pipeline scope **and** their dimension filters are not mutually exclusive. A `NULL` on any dimension means "all values" (wildcard), so it overlaps with any value on the other side.

Dimensions checked: `ENTITY_CODE`, `SOURCE_SYSTEM_CODE`, `DEPARTMENT_CODE`, `BOOK_CODE`, `CURRENCY_CODE`, `TRADE_TYPOLOGY`, `STRATEGY`.

Overlap condition (per dimension):
```
dimension_A = dimension_B  OR  dimension_A IS NULL  OR  dimension_B IS NULL
```

**Example:**
- adj1: Book = `AAA`, Dept = `a123` (specific)
- adj2: Book = `AAA`, Dept = `NULL` (all depts)
- → They overlap because Book matches AND Dept: `a123` vs `NULL` (wildcard) = match.
- adj2 is blocked until adj1 finishes.

**adj3 (Book = `BBB`)** has no overlap with adj1 (Book = `AAA`) → not blocked, processed in parallel.

---

## Deduplication — FACT_TABLE_PK enforcement

When writing adjustment rows, the processing engine uses `MERGE INTO` on the target adjustment fact table, keyed on `FACT_TABLE_PK` from `ADJUSTMENTS_SETTINGS`.

Rule: **if a row with the same PK already exists in the adjustment table, UPDATE it with the newer values.** This ensures that if two adjustments affected the same source row (e.g., due to overlapping history or re-submission), only the most recent delta is kept — no accumulation of duplicate deltas on the same fact row.

---

## Worked Example

```
09:00  adj1 submitted (VaR, Book AAA)
       → Pending, BLOCKED_BY_ADJ_ID = NULL
       → VW_QUEUE_VAR shows adj1, STREAM_QUEUE_VAR has data

09:00  TASK_PROCESS_VAR fires
       → Claims adj1 → Running
       → No other Pending adjustments → no blocking to set

09:05  adj2 submitted (VaR, Book AAA — overlaps adj1)
       → Pending; check Running adjustments in VaR scope
       → adj1 is Running and overlaps → BLOCKED_BY_ADJ_ID = adj1.ADJ_ID
       → VW_QUEUE_VAR does NOT show adj2 (blocked)

09:06  adj3 submitted (VaR, Book BBB — no overlap with adj1)
       → Pending, BLOCKED_BY_ADJ_ID = NULL
       → VW_QUEUE_VAR shows adj3, STREAM_QUEUE_VAR has data
       → TASK_PROCESS_VAR fires, claims adj3 → Running
       → adj1 and adj3 now Running concurrently

09:15  adj1 finishes → Processed
       → Unblock: adj2 has BLOCKED_BY_ADJ_ID = adj1
       → Check: any other Running adj in VaR overlaps adj2? adj3 (Book BBB) does not
       → adj2: BLOCKED_BY_ADJ_ID = NULL
       → VW_QUEUE_VAR shows adj2, stream fires, task picks up adj2

09:15  adj3 also finishes (independently) → Processed
```

---

## Status Updates Across the App

All places that reference status values must be updated:

| Object | Change |
|--------|--------|
| `ADJ_HEADER.RUN_STATUS` | Add `Running`, `Failed`; rename `Error` → `Failed` |
| `VW_DASHBOARD_KPI` | Replace `Error` count with `Failed`, add `Running` count |
| `DT_DASHBOARD` | Status group by includes new values |
| `DT_OVERLAP_ALERTS` | Filter includes `Running` alongside `Pending` |
| `VW_PROCESSING_QUEUE` | Replace `Processing` → `Running`, `Error` → `Failed` |
| `4_Processing_Queue.py` | Update status labels and colours |
| `app.py` (dashboard) | Update KPI cards for new status names |
| `utils/styles.py` | Update `status_badge()` colour map |

---

## Files to Create / Modify

| File | Action |
|------|--------|
| `new_adjustment_db_objects/01_tables.sql` | Add `BLOCKED_BY_ADJ_ID` column to `ADJ_HEADER` |
| `new_adjustment_db_objects/02_streams.sql` | Replace old streams with 4 scope-specific view streams |
| `new_adjustment_db_objects/05_sp_process_adjustment.sql` | Add blocking logic (set/clear `BLOCKED_BY_ADJ_ID`), MERGE dedup |
| `new_adjustment_db_objects/06_tasks.sql` | Replace single task with 4 scope-specific tasks |
| `new_adjustment_db_objects/07_dynamic_tables.sql` | Update `DT_OVERLAP_ALERTS` to include `Running` status |
| `new_adjustment_db_objects/08_views.sql` | Add 4 queue views; update status references |
| `new_adjustment_db_objects/09_sp_submit_adjustment_blocking.sql` | New SP helper: set `BLOCKED_BY_ADJ_ID` on submit |
| `streamlit_app/app.py` | Update KPI status labels |
| `streamlit_app/pages/4_Processing_Queue.py` | Update status labels/colours |
| `streamlit_app/utils/styles.py` | Update `status_badge()` map |
| `context/unified_adjustment_design.md` | Update architecture section |

---

## Blocking Set at Submit Time

When `SP_SUBMIT_ADJUSTMENT` inserts a new `ADJ_HEADER` row, it immediately checks for any `Running` adjustment in the same scope + COBID that overlaps. If found, it sets `BLOCKED_BY_ADJ_ID` on the new row before the INSERT completes. This means the row **never appears in the queue view** if it is blocked — the stream never fires for it spuriously.

The task's Step 2 (set blocking for newly-claimed Running adjustments) handles the **other direction**: blocking Pending adjustments that were submitted before the current task run claimed the Running one.

---

## Open Items

- `FRTBALL` must be added to `ADJUSTMENTS_SETTINGS` seed data with its fact table mapping before the FRTB pipeline can process it.

---

## Out of Scope

- Changing the approval workflow (`Pending Approval` / `Approved`) — untouched
- Adding per-scope warehouses (can be done later by changing the warehouse name in each task)
- True async parallelism within a task run (current design: all eligible adjustments are claimed atomically then processed sequentially within one task run — this already eliminates artificial blocking between non-overlapping adjustments)
