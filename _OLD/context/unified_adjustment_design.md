# Unified Adjustment Process — Architecture Design

> **Created:** 2026-03-28  |  **Updated:** 2026-04-01
>
> **Requirements:** [requirements.md](requirements.md)
>
> **Purpose:** Define the target-state architecture for the unified adjustment process.
> This document is the blueprint for all Snowflake objects in `new_adjustment_db_objects/`.
>
> **Key change:** The file-based interface is replaced by **Streamlit on Snowflake**.
> Tables in the ADJUSTMENT schema are the single point of entry — no CDC views on
> staging tables, no Raven file ingestion.

---

## 1. Design Principles

| # | Principle | How |
|---|---|---|
| P1 | **Streamlit is the UI** | Users create, preview, and manage adjustments via Streamlit on Snowflake. No CSV files as the primary interface. |
| P2 | **Tables are the entry point** | `ADJUSTMENT.ADJ_HEADER` is where every adjustment starts. Streamlit writes here via `SP_SUBMIT_ADJUSTMENT`. |
| P3 | **One process, multiple scopes** | A single procedure handles VaR, Stress, ES, FRTB, Sensitivity — driven by config in `ADJUSTMENTS_SETTINGS` |
| P4 | **Configuration over code** | Table names, metrics, PKs all in `ADJUSTMENTS_SETTINGS`. Adding a scope = adding a row. |
| P5 | **Python 3.11 + Snowpark** | All stored procedures in Python. `EXECUTE AS CALLER`. |
| P6 | **Overlap = keep most recent** | DENSE_RANK by `CREATED_DATE DESC, ADJ_ID DESC` — same pattern as existing proc |
| P7 | **Idempotent processing** | DELETE existing + re-INSERT for every adjustment (safe to re-run) |
| P8 | **Sign-off is a gate** | Signed-off adjustments are recorded (`Rejected - SignedOff`) but never processed |
| P9 | **Queue-driven processing** | All adjustments enter a Pending queue. `SP_SUBMIT_ADJUSTMENT` checks for Running overlaps and sets `BLOCKED_BY_ADJ_ID` if needed. Processing is driven by 4 independent scope tasks. |
| P10 | **4 independent scope pipelines** | VaR, Stress, FRTB, Sensitivity each have their own queue view → stream → task → `SP_RUN_PIPELINE`. A slow FRTB run has zero impact on VaR. |
| P11 | **ADJUSTMENT schema** | All new objects live under `ADJUSTMENT` (except `BATCH.RUN_LOG` which already exists) |

---

## 2. Architecture Diagram

```
 ════════════════════════════════════════════════════════════════════════════
  INPUT LAYER — Streamlit on Snowflake is the sole point of entry
 ════════════════════════════════════════════════════════════════════════════

  ┌─────────────────────────────────────────────────────────────────────────┐
  │                     STREAMLIT ON SNOWFLAKE                             │
  │                                                                        │
  │   ┌───────────┐  ┌───────────┐  ┌───────────┐  ┌───────────────────┐  │
  │   │ New       │  │ My Work   │  │ Approval  │  │ Dashboard /       │  │
  │   │ Adjustment│  │ (view/    │  │ Queue     │  │ Processing Queue  │  │
  │   │ Wizard    │  │ edit/     │  │ (approve/ │  │ (monitor status)  │  │
  │   │           │  │ delete)   │  │ reject)   │  │                   │  │
  │   └─────┬─────┘  └─────┬─────┘  └─────┬─────┘  └─────┬─────────────┘  │
  │         │              │              │              │                 │
  └─────────┼──────────────┼──────────────┼──────────────┼─────────────────┘
            │              │              │              │
            ▼              ▼              ▼              ▼
  ┌──────────────────┐ ┌────────────┐ ┌────────────┐ ┌──────────────────────┐
  │ SP_SUBMIT_       │ │ Direct     │ │ Direct     │ │ Read from:           │
  │ ADJUSTMENT       │ │ UPDATE on  │ │ UPDATE on  │ │ • DT_DASHBOARD       │
  │ (creates header, │ │ ADJ_HEADER │ │ ADJ_HEADER │ │ • DT_OVERLAP_ALERTS  │
  │  validates,      │ │            │ │ (status    │ │ • VW_DASHBOARD_KPI   │
  │  triggers proc)  │ │            │ │  change)   │ │ • VW_RECENT_ACTIVITY │
  └────────┬─────────┘ └────────────┘ └────────────┘ └──────────────────────┘
           │
           ▼
  ┌────────────────────────────────────────────────────────────────────────┐
  │  ADJUSTMENT.ADJ_HEADER  (table — single source of truth)             │
  │  + ADJUSTMENT.ADJ_LINE_ITEM  (for Direct/Upload values)              │
  │  + ADJUSTMENT.ADJ_STATUS_HISTORY  (audit trail)                      │
  └────────────────────────────┬─────────────────────────────────────────┘
                               │
                               │ 4 queue views → 4 streams (one per scope)
                               ▼

 ════════════════════════════════════════════════════════════════════════════
  PROCESSING LAYER — ADJ_HEADER → fact adjustment tables
 ════════════════════════════════════════════════════════════════════════════

  ┌────────────────────────────────────────────────────────────────────────┐
  │  4 independent pipelines — one per scope                             │
  │                                                                      │
  │  ADJ_HEADER → VW_QUEUE_VAR ──────── STREAM_QUEUE_VAR ──────── TASK_PROCESS_VAR        │
  │             → VW_QUEUE_STRESS ────── STREAM_QUEUE_STRESS ────── TASK_PROCESS_STRESS    │
  │             → VW_QUEUE_FRTB ─────── STREAM_QUEUE_FRTB ─────── TASK_PROCESS_FRTB      │
  │             → VW_QUEUE_SENSITIVITY ─ STREAM_QUEUE_SENSITIVITY ─ TASK_PROCESS_SENSITIVITY│
  │                                                                      │
  │  Each task calls SP_RUN_PIPELINE: claim → block → process → unblock  │
  └────────────────────────────┬─────────────────────────────────────────┘
                               │
                               ▼
  ┌────────────────────────────────────────────────────────────────────────┐
  │  SP_PROCESS_ADJUSTMENT  (Python / Snowpark)                          │
  │                                                                      │
  │  1. Read config from ADJUSTMENTS_SETTINGS                            │
  │  2. Filter pending adjustments from ADJ_HEADER                       │
  │  3. IF Direct:                                                       │
  │     • Read from ADJ_LINE_ITEM → map to fact adj columns → INSERT     │
  │  4. IF Scale:                                                        │
  │     • 3-way UNION ALL (same-COB, cross-COB, flatten)                 │
  │     • DENSE_RANK overlap resolution                                  │
  │     • Cross-COB: SCD2 dimension key fix                              │
  │     • Summary rebuild                                                │
  │  5. Update ADJ_HEADER.RUN_STATUS → 'Processed' / 'Failed'           │
  │  6. Log to ADJ_STATUS_HISTORY                                        │
  └────────────────────────────┬─────────────────────────────────────────┘
                               │
                               ▼
  ┌────────────────────┐ ┌────────────────────┐ ┌────────────────────┐
  │ FACT.VAR_MEASURES_ │ │ FACT.STRESS_       │ │ FACT.<FRTB/SENSI>_ │
  │ ADJUSTMENT         │ │ MEASURES_          │ │ ADJUSTMENT         │
  │ + _SUMMARY         │ │ ADJUSTMENT         │ │ + _SUMMARY         │
  │                    │ │ + _SUMMARY         │ │                    │
  └────────────────────┘ └────────────────────┘ └────────────────────┘


 ════════════════════════════════════════════════════════════════════════════
  VISIBILITY LAYER — auto-refreshing objects for the Streamlit dashboard
 ════════════════════════════════════════════════════════════════════════════

  ┌─────────────────────────────┐  ┌─────────────────────────────┐
  │ DT_DASHBOARD (dynamic tbl)  │  │ DT_OVERLAP_ALERTS (dyn tbl) │
  │ • Status counts per scope   │  │ • Detects overlapping adjs   │
  │ • Per COB, entity, user     │  │ • Shows superseding adj      │
  │ • Auto-refresh: 1 min       │  │ • Auto-refresh: 1 min        │
  └─────────────────────────────┘  └─────────────────────────────┘

  ┌─────────────────────────────┐  ┌─────────────────────────────┐
  │ VW_SIGNOFF_STATUS (view)    │  │ VW_DASHBOARD_KPI (view)     │
  │ • Real-time sign-off check  │  │ • KPI cards for dashboard   │
  │ • IS_SIGNED_OFF flag        │  │ • Totals, averages, counts  │
  └─────────────────────────────┘  └─────────────────────────────┘

  ┌─────────────────────────────┐  ┌─────────────────────────────┐
  │ VW_APPROVAL_QUEUE (view)    │  │ VW_MY_WORK (view)           │
  │ • Pending Approval items    │  │ • All adjustments for user  │
  │ • Overlap + signoff flags   │  │ • Full detail + status      │
  └─────────────────────────────┘  └─────────────────────────────┘

  ┌─────────────────────────────┐  ┌─────────────────────────────┐
  │ VW_PROCESSING_QUEUE (view)  │  │ VW_RECENT_ACTIVITY (view)   │
  │ • Live pipeline view        │  │ • Activity feed / timeline  │
  │ • Queue position estimate   │  │ • Submissions + transitions │
  └─────────────────────────────┘  └─────────────────────────────┘

  ┌─────────────────────────────┐
  │ VW_ERRORS (view)            │
  │ • Current Error status adjs │
  │ • Error messages + context  │
  └─────────────────────────────┘
```

---

## 3. Object Design Detail

### 3.1 — Tables (01_tables.sql)

#### ADJ_HEADER — Single point of entry

Every adjustment (ad-hoc or recurring) starts as a row here. Streamlit writes here
via `SP_SUBMIT_ADJUSTMENT`. The processing procedure reads from this table.

Columns mirror `DIMENSION.ADJUSTMENT` filter dimensions so the processing procedure
can join to fact tables using the same column names.

| Column Group | Key Columns | Notes |
|---|---|---|
| **Identity** | `ADJ_ID` (autoincrement start 200000) | Separate sequence from `DIMENSION.ADJUSTMENT` |
| **Scope & Type** | `COBID`, `PROCESS_TYPE`, `ADJUSTMENT_TYPE`, `ADJUSTMENT_ACTION` | Action derived: Upload→Direct, Scale/Flatten→Scale |
| **Scale** | `SCALE_FACTOR`, `SCALE_FACTOR_ADJUSTED`, `SOURCE_COBID` | Adjusted computed at submit time |
| **Filter Dims** | 20+ columns matching `DIMENSION.ADJUSTMENT` | NULL = no filter on that dimension |
| **Workflow** | `RUN_STATUS`, `IS_POSITIVE_ADJUSTMENT`, `PROCESS_DATE` | Status: Pending → Processing → Processed / Error |
| **Audit** | `USERNAME`, `CREATED_DATE`, `IS_DELETED`, `ERRORMESSAGE` | Soft delete, London timezone |
| **Mode** | `ADJUSTMENT_OCCURRENCE` (ADHOC / RECURRING) | Drives ad-hoc vs task-driven processing |

#### ADJ_LINE_ITEM — Upload/Direct detail rows

For VaR_Upload: user uploads CSV via Streamlit, app parses + UNPIVOTs the 21 VaR
columns, then writes one row per (entity, book, scenario, VaR component) here.

For Scale/Flatten: **NOT used** — the processing procedure reads fact tables directly.

#### ADJ_STATUS_HISTORY — Complete audit trail

Every status transition is recorded (who, when, old→new, comment).

#### ADJUSTMENTS_SETTINGS — Config table

One row per scope. Adding a new scope = adding a new row, no code changes.
`ADJUSTMENT_BASE_TABLE` now points to `ADJUSTMENT.ADJ_HEADER` (not `DIMENSION.ADJUSTMENT`).

#### ADJ_RECURRING_TEMPLATE — Recurring templates

Admin configures templates; `INSTANTIATE_RECURRING_TASK` creates `ADJ_HEADER` entries
from them when dependencies are met.

### 3.2 — Streams (02_streams.sql)

| Stream | On | Mode | Purpose |
|---|---|---|---|
| `STREAM_QUEUE_VAR` | `VW_QUEUE_VAR` | APPEND_ONLY | Fires when eligible VaR adjustments appear. Triggers TASK_PROCESS_VAR. |
| `STREAM_QUEUE_STRESS` | `VW_QUEUE_STRESS` | APPEND_ONLY | Fires when eligible Stress adjustments appear. Triggers TASK_PROCESS_STRESS. |
| `STREAM_QUEUE_FRTB` | `VW_QUEUE_FRTB` | APPEND_ONLY | Fires when eligible FRTB-pipeline adjustments appear (all sub-types). Triggers TASK_PROCESS_FRTB. |
| `STREAM_QUEUE_SENSITIVITY` | `VW_QUEUE_SENSITIVITY` | APPEND_ONLY | Fires when eligible Sensitivity adjustments appear. Triggers TASK_PROCESS_SENSITIVITY. |

### 3.3 — Stored Procedures

#### SP_SUBMIT_ADJUSTMENT (03_sp_submit_adjustment.sql)

Entry point from Streamlit. Accepts a JSON string with all adjustment details.

**Flow:**
1. Parse JSON → validate required fields (`cobid`, `process_type`, `adjustment_type`, `username`)
2. Compute derived values (`ADJUSTMENT_ACTION`, `SCALE_FACTOR_ADJUSTED`)
3. Validate scope is active in `ADJUSTMENTS_SETTINGS`
4. Check sign-off status → if signed off, set status `Rejected - SignedOff`
5. Check for Running overlaps → set `BLOCKED_BY_ADJ_ID` on new row if blocked
6. Insert into `ADJ_HEADER`
7. Record in `ADJ_STATUS_HISTORY`
8. Return JSON: `{ adj_id, status, message }` (includes blocked notice if applicable)

#### SP_PREVIEW_ADJUSTMENT (04_sp_preview_adjustment.sql)

Preview impact without modifying any data. Returns a result set showing:
current values → adjustment → projected values. Read-only.

**For Scale/Flatten:** Queries the fact table with the same filters and shows
`CURRENT_VALUE_USD`, `ADJUSTMENT_USD`, `PROJECTED_VALUE_USD`.

**For Direct/Upload:** Returns the line items from `ADJ_LINE_ITEM`.

#### SP_PROCESS_ADJUSTMENT (05_sp_process_adjustment.sql)

Core processing engine. Adapted from the existing `ADJUSTMENT.PROCESS_ADJUSTMENT` procedure.

**Signature:** `CALL SP_PROCESS_ADJUSTMENT('VaR', 'Scale', 20250328)`

**Key changes from legacy:**
- Reads from `ADJ_HEADER` (via config `ADJUSTMENT_BASE_TABLE`)
- For Direct: reads values from `ADJ_LINE_ITEM` (not from the base table)
- Updates `ADJ_HEADER.RUN_STATUS` (not `DIMENSION.ADJUSTMENT`)
- Records transitions in `ADJ_STATUS_HISTORY`
- Filters by `RUN_STATUS = 'Running'` (adjustments already claimed by SP_RUN_PIPELINE)
- Sets `RUN_STATUS = 'Failed'` on error (not 'Error')

### 3.4 — Tasks (06_tasks.sql)

| Task | Schedule | Guard | Purpose |
|---|---|---|---|
| `TASK_PROCESS_VAR` | 1 min | `STREAM_QUEUE_VAR` has data | Calls `SP_RUN_PIPELINE('VaR', '["VaR"]')` |
| `TASK_PROCESS_STRESS` | 1 min | `STREAM_QUEUE_STRESS` has data | Calls `SP_RUN_PIPELINE('Stress', '["Stress"]')` |
| `TASK_PROCESS_FRTB` | 1 min | `STREAM_QUEUE_FRTB` has data | Calls `SP_RUN_PIPELINE('FRTB', '["FRTB","FRTBDRC","FRTBRRAO","FRTBALL"]')` |
| `TASK_PROCESS_SENSITIVITY` | 1 min | `STREAM_QUEUE_SENSITIVITY` has data | Calls `SP_RUN_PIPELINE('Sensitivity', '["Sensitivity"]')` |

All start SUSPENDED. Enable with `ALTER TASK ... RESUME` when ready.

### 3.5 — Dynamic Tables (07_dynamic_tables.sql)

| Dynamic Table | Target Lag | Purpose |
|---|---|---|
| `DT_DASHBOARD` | 1 min | Aggregated status summary by COB, scope, type, entity, user |
| `DT_OVERLAP_ALERTS` | 1 min | Self-join on `ADJ_HEADER` to find overlapping filter combinations |

### 3.6 — Views (08_views.sql)

| View | Purpose | Used By |
|---|---|---|
| `VW_SIGNOFF_STATUS` | Unified sign-off check (real-time) | Submit procedure, Streamlit form |
| `VW_DASHBOARD_KPI` | KPI cards (totals, averages, overlaps) | Dashboard page |
| `VW_RECENT_ACTIVITY` | Activity feed (submissions + transitions) | Dashboard timeline |
| `VW_ERRORS` | Error panel (current errors) | Dashboard error tab |
| `VW_APPROVAL_QUEUE` | Pending Approval items with context | Approval Queue page |
| `VW_MY_WORK` | All adjustments for a user | My Work page |
| `VW_PROCESSING_QUEUE` | Pipeline view with queue position | Processing Queue page |
| `VW_QUEUE_VAR` | Eligible VaR adjustments (Pending + unblocked) | STREAM_QUEUE_VAR source |
| `VW_QUEUE_STRESS` | Eligible Stress adjustments (Pending + unblocked) | STREAM_QUEUE_STRESS source |
| `VW_QUEUE_FRTB` | Eligible FRTB-pipeline adjustments (Pending + unblocked) | STREAM_QUEUE_FRTB source |
| `VW_QUEUE_SENSITIVITY` | Eligible Sensitivity adjustments (Pending + unblocked) | STREAM_QUEUE_SENSITIVITY source |

---

## 4. Adjustment Flows

### 4.1 — Ad-Hoc Scale/Flatten (user clicks "Save")

```
1. User fills form in Streamlit New Adjustment wizard
2. User clicks "Preview" → CALL SP_PREVIEW_ADJUSTMENT(JSON)
   → Returns projected impact table
3. User confirms → clicks "Save"
4. Streamlit calls: CALL SP_SUBMIT_ADJUSTMENT(JSON)
   a. Validates input, checks sign-off
   b. Computes ADJUSTMENT_ACTION = 'Scale', SCALE_FACTOR_ADJUSTED
   c. Checks for Running overlaps → sets BLOCKED_BY_ADJ_ID if applicable
   d. INSERT INTO ADJ_HEADER → returns ADJ_ID
   e. INSERT INTO ADJ_STATUS_HISTORY (NULL → Pending)
   f. Returns: { adj_id: 200001, status: 'Pending', message: 'Adjustment queued.' }
5. Streamlit shows "queued" confirmation to user
6. STREAM_QUEUE_VAR detects the new Pending+unblocked row → TASK_PROCESS_VAR wakes
7. SP_RUN_PIPELINE claims → blocks overlaps → processes → unblocks → status: Processed/Failed
```

### 4.2 — Ad-Hoc Upload (VaR CSV via Streamlit)

```
1. User selects "Upload" type in Streamlit wizard
2. User uploads CSV via st.file_uploader()
3. Streamlit Python code:
   a. Parses CSV (pd.read_csv)
   b. UNPIVOTs 21 VaR columns into individual rows
   c. Calls: CALL SP_SUBMIT_ADJUSTMENT(JSON) → returns ADJ_ID
   d. Writes unpivoted rows to ADJ_LINE_ITEM with ADJ_ID
      (session.write_pandas or INSERT)
   e. Calls: CALL SP_PROCESS_ADJUSTMENT('VaR', 'Direct', COBID)
4. Processing procedure:
   a. Reads ADJ_HEADER (Pending, Direct)
   b. Reads ADJ_LINE_ITEM for those ADJ_IDs
   c. Maps columns → FACT.VAR_MEASURES_ADJUSTMENT columns
   d. DELETE old + INSERT new
   e. Updates status → Processed
```

### 4.3 — Recurring (template-driven)

```
1. Admin creates template in ADJ_RECURRING_TEMPLATE
2. INSTANTIATE_RECURRING_TASK (5-min) checks templates:
   a. Is template active?
   b. Does an adjustment for today's COB already exist? (dedup)
   c. Are dependencies met? (external signal)
   d. If yes → INSERT INTO ADJ_HEADER from template
3. Scope task (1-min, stream-guarded) picks up new header:
   a. STREAM_QUEUE_<SCOPE> detects the new Pending row
   b. TASK_PROCESS_<SCOPE> calls SP_RUN_PIPELINE
   c. SP_RUN_PIPELINE claims → blocks → processes → unblocks
4. Status updates + audit trail logged automatically
```

### 4.4 — Approval Flow (optional)

```
1. User submits with requires_approval = true
2. SP_SUBMIT_ADJUSTMENT sets status = 'Pending Approval'
3. Approver sees it in VW_APPROVAL_QUEUE
4. Approver updates ADJ_HEADER.RUN_STATUS:
   → 'Approved' (ready for processing)
   → 'Rejected' (with comment)
5. Approver sets status to 'Pending' → next scope task cycle picks it up via the queue view
```

---

## 5. Processing Procedure — Detailed Algorithm

```
PROCEDURE SP_PROCESS_ADJUSTMENT(process_type, adjustment_action, cobid)

  1. READ config from ADJUSTMENTS_SETTINGS
     → fact_table, fact_adjusted_table, adjustments_table, summary_table, metrics, pk

  2. GET Running adjustments from ADJ_HEADER
     WHERE COBID = cobid
       AND (PROCESS_TYPE = process_type OR PROCESS_TYPE = 'FRTBALL')
       AND RUN_STATUS = 'Running'
     (already claimed by SP_RUN_PIPELINE before this call)

  3. IF no pending adjustments → RETURN early

  4. IF adjustment_action = 'Direct':
     a. Collect ADJ_IDs
     b. Read ADJ_LINE_ITEM for those ADJ_IDs
     c. Map LINE_ITEM columns to fact adj table columns (check_columns)
     d. DELETE existing from adjustments_table WHERE (COBID, ADJUSTMENT_ID) match
     e. Exclude soft-deleted line items
     f. INSERT valid rows into adjustments_table
     g. UPDATE ADJ_HEADER.RUN_STATUS → 'Processed'
     h. INSERT into ADJ_STATUS_HISTORY

  5. IF adjustment_action = 'Scale':
     a. Get join columns (fact ∩ adj_header, minus exclusions)
     b. Build dynamic SQL with 3 UNION ALL branches:
        ① Scale current COB   (COBID = SOURCE_COBID, scale_factor_adjusted)
        ② Scale other COB     (from adjusted table, different COBIDs)
        ③ Flatten current COB (scale = -1, for cross-COB only)
     c. Apply dimension filters via EXISTS (BOOK→dept, TRADE→strategy/typology)
     d. CREATE TEMP TABLE with DENSE_RANK dedup (overlap resolution)
     e. DELETE existing from adjustments_table + summary_table
     f. INSERT from temp → adjustments_table
     g. Cross-COB: UPDATE dimension keys via SCD2 lookups
     h. IF summary_table configured:
        INSERT INTO summary_table (GROUP BY ALL, SUM metrics)
     i. UPDATE ADJ_HEADER.RUN_STATUS → 'Processed'
     j. INSERT into ADJ_STATUS_HISTORY

  6. ON ERROR: set RUN_STATUS = 'Failed', record error message

  7. RETURN result JSON
```

---

## 6. Status State Machine

```
                              ┌──────────────────┐
                              │                  │
  ┌─────────┐  submit   ┌────▼─────┐  approve  ┌┴──────────┐
  │ (start) │──────────►│ Pending  │──────────►│ Approved  │
  └─────────┘           │          │           │           │
                        └────┬─────┘           └─────┬─────┘
                             │                       │
       requires_approval     │                       │ set back to Pending
                        ┌────▼──────────┐            │
                        │ Pending       │  ◄─────────┘
                        │ Approval      │
                        └────┬──────────┘
                             │ reject
                        ┌────▼─────┐
                        │ Rejected │
                        └──────────┘

  Once Pending (and unblocked):
  scope task claims → Running
            │
       ┌────▼──────┐   success   ┌───────────┐
       │ Running   │───────────►│ Processed │
       └────┬──────┘            └───────────┘
            │ failure
       ┌────▼─────┐
       │ Failed   │──── (manual retry: re-submit as new adjustment)
       └──────────┘

  Special:
    └── Rejected - SignedOff  (COB already signed off, recorded but not processed)
    └── Deleted               (soft delete via IS_DELETED flag)
    └── Blocked               (BLOCKED_BY_ADJ_ID set — not a status but a state;
                               row is Pending but invisible in queue view until blocker finishes)
```

---

## 7. Migration Strategy

| Phase | What | Risk |
|---|---|---|
| **Phase 1** | Deploy all new objects in ADJUSTMENT schema. Both systems run in parallel. | Low |
| **Phase 2** | Route FRTB/Sensitivity through new process (already started via Task 2c). Validate. | Low |
| **Phase 3** | Build Streamlit app pages (New Adjustment, My Work, Approval Queue, Dashboard). Connect to new objects. | Low |
| **Phase 4** | Route VaR through new process. Compare output with legacy. | Medium |
| **Phase 5** | Route Stress through new process. | Medium |
| **Phase 6** | Retire legacy JS procedures, file-based tasks, staging table streams. | High |

---

## 8. Files Created

All scripts in `new_adjustment_db_objects/`:

| # | File | Objects | Purpose |
|---|---|---|---|
| 1 | `01_tables.sql` | `ADJ_HEADER`, `ADJ_LINE_ITEM`, `ADJ_STATUS_HISTORY`, `ADJUSTMENTS_SETTINGS`, `ADJ_RECURRING_TEMPLATE` + seed | Foundation tables — point of entry |
| 2 | `02_streams.sql` | `STREAM_QUEUE_VAR`, `STREAM_QUEUE_STRESS`, `STREAM_QUEUE_FRTB`, `STREAM_QUEUE_SENSITIVITY` | APPEND_ONLY streams on queue views — one per scope pipeline |
| 3 | `03_sp_submit_adjustment.sql` | `SP_SUBMIT_ADJUSTMENT` | Streamlit → table (validates, inserts, triggers) |
| 4 | `04_sp_preview_adjustment.sql` | `SP_PREVIEW_ADJUSTMENT` | Preview impact without applying |
| 5 | `05_sp_process_adjustment.sql` | `SP_PROCESS_ADJUSTMENT` | Core processing engine (Direct + Scale); reads Running adjustments |
| 5b | `05b_sp_run_pipeline.sql` | `SP_RUN_PIPELINE` | Pipeline orchestrator: claim → block → process → unblock |
| 6 | `06_tasks.sql` | `TASK_PROCESS_VAR`, `TASK_PROCESS_STRESS`, `TASK_PROCESS_FRTB`, `TASK_PROCESS_SENSITIVITY` | 4 independent stream-triggered scope pipeline tasks |
| 7 | `07_dynamic_tables.sql` | `DT_DASHBOARD`, `DT_OVERLAP_ALERTS` | Auto-refreshing visibility |
| 8 | `08_views.sql` | `VW_SIGNOFF_STATUS`, `VW_DASHBOARD_KPI`, `VW_RECENT_ACTIVITY`, `VW_ERRORS`, `VW_APPROVAL_QUEUE`, `VW_MY_WORK`, `VW_PROCESSING_QUEUE` | Real-time views for Streamlit pages |
