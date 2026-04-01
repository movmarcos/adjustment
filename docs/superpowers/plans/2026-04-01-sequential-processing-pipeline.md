# Sequential Per-Scope Processing Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the single global adjustment queue with 4 independent scope pipelines (VaR, Stress, FRTB, Sensitivity), each with its own view → stream → task, with overlap-based blocking between adjustments.

**Architecture:** Each scope has a queue view (Pending + unblocked rows from ADJ_HEADER), a Snowflake stream on that view, and a task that atomically claims eligible adjustments, blocks overlapping ones, calls `SP_PROCESS_ADJUSTMENT` per adjustment, then unblocks waiting adjustments. A new `BLOCKED_BY_ADJ_ID` column on `ADJ_HEADER` is the blocking signal. A new `SP_RUN_PIPELINE` stored procedure encapsulates the full claim→block→process→unblock logic so the 4 task bodies stay simple. Statuses simplify to `Pending → Running → Processed / Failed`.

**Tech Stack:** Snowflake SQL, Snowpark Python 3.11, Streamlit in Snowflake. All SQL runs in `DVLP_RAPTOR_NEWADJ.ADJUSTMENT_APP`.

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `new_adjustment_db_objects/01_tables.sql` | Modify | Add `BLOCKED_BY_ADJ_ID` column; enable `CHANGE_TRACKING` on `ADJ_HEADER` |
| `new_adjustment_db_objects/02_streams.sql` | Replace | 4 scope-specific streams on queue views |
| `new_adjustment_db_objects/03_sp_submit_adjustment.sql` | Modify | Blocking check at submit time (before INSERT) |
| `new_adjustment_db_objects/05_sp_process_adjustment.sql` | Modify | Read `Running` (not Pending/Approved); set `Failed` (not Error) |
| `new_adjustment_db_objects/05b_sp_run_pipeline.sql` | Create | `SP_RUN_PIPELINE` — claim, block, process, unblock |
| `new_adjustment_db_objects/06_tasks.sql` | Replace | 4 scope-specific tasks, each calling `SP_RUN_PIPELINE` |
| `new_adjustment_db_objects/07_dynamic_tables.sql` | Modify | `DT_OVERLAP_ALERTS` includes `Running` status |
| `new_adjustment_db_objects/08_views.sql` | Modify | Add 4 `VW_QUEUE_*` views; update status labels |
| `streamlit_app/utils/styles.py` | Modify | `STATUS_COLORS` / `STATUS_ICONS` for `Running` / `Failed` |
| `streamlit_app/app.py` | Modify | KPI cards: add `Running`, rename `Errors` → `Failed` |
| `streamlit_app/pages/4_Processing_Queue.py` | Modify | Status labels, pipeline diagram text, stat counters |

---

## Task 1: Schema — Add BLOCKED_BY_ADJ_ID and enable change tracking

**Files:**
- Modify: `new_adjustment_db_objects/01_tables.sql`

- [ ] **Step 1: Add the blocking column and enable change tracking**

Append the following block at the end of `01_tables.sql`, before the `VERIFY` section:

```sql
-- ═══════════════════════════════════════════════════════════════════════════
-- MIGRATION: Sequential pipeline additions
-- Run once after initial table creation.
-- ═══════════════════════════════════════════════════════════════════════════

-- Blocking column: NULL = eligible; populated = waiting for that ADJ_ID to finish
ALTER TABLE ADJUSTMENT_APP.ADJ_HEADER
    ADD COLUMN IF NOT EXISTS BLOCKED_BY_ADJ_ID NUMBER(38,0) DEFAULT NULL;

-- Change tracking required for streams on views derived from this table
ALTER TABLE ADJUSTMENT_APP.ADJ_HEADER SET CHANGE_TRACKING = TRUE;
```

- [ ] **Step 2: Verify**

Run against Snowflake:
```sql
DESCRIBE TABLE ADJUSTMENT_APP.ADJ_HEADER;
-- Expected: BLOCKED_BY_ADJ_ID column visible with type NUMBER(38,0), nullable

SHOW TABLES LIKE 'ADJ_HEADER' IN SCHEMA ADJUSTMENT_APP;
-- Expected: CHANGE_TRACKING column shows TRUE
```

- [ ] **Step 3: Commit**

```bash
git add new_adjustment_db_objects/01_tables.sql
git commit -m "feat: add BLOCKED_BY_ADJ_ID column and enable change tracking on ADJ_HEADER"
```

---

## Task 2: Queue Views — 4 scope-specific eligible-adjustment views

**Files:**
- Modify: `new_adjustment_db_objects/08_views.sql`

- [ ] **Step 1: Add the 4 queue views**

Append the following block to `08_views.sql` before the `VERIFY` section. Also add `VW_QUEUE_*` to the final `SHOW VIEWS` call.

```sql
-- ═══════════════════════════════════════════════════════════════════════════
-- QUEUE VIEWS — one per processing pipeline
--
-- Show only adjustments that are:
--   • Pending (not yet claimed by a task)
--   • Not blocked (BLOCKED_BY_ADJ_ID IS NULL)
--   • Not soft-deleted
--
-- Stream-ready: change tracking on ADJ_HEADER (Task 1) enables streams on
-- these simple single-table filtered views.
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW ADJUSTMENT_APP.VW_QUEUE_VAR
    COMMENT = 'Eligible VaR adjustments: Pending + unblocked. Stream source for TASK_PROCESS_VAR.'
AS
SELECT * FROM ADJUSTMENT_APP.ADJ_HEADER
WHERE PROCESS_TYPE = 'VaR'
  AND RUN_STATUS = 'Pending'
  AND BLOCKED_BY_ADJ_ID IS NULL
  AND IS_DELETED = FALSE;

CREATE OR REPLACE VIEW ADJUSTMENT_APP.VW_QUEUE_STRESS
    COMMENT = 'Eligible Stress adjustments: Pending + unblocked. Stream source for TASK_PROCESS_STRESS.'
AS
SELECT * FROM ADJUSTMENT_APP.ADJ_HEADER
WHERE PROCESS_TYPE = 'Stress'
  AND RUN_STATUS = 'Pending'
  AND BLOCKED_BY_ADJ_ID IS NULL
  AND IS_DELETED = FALSE;

CREATE OR REPLACE VIEW ADJUSTMENT_APP.VW_QUEUE_FRTB
    COMMENT = 'Eligible FRTB-pipeline adjustments (FRTB + FRTBDRC + FRTBRRAO + FRTBALL): Pending + unblocked.'
AS
SELECT * FROM ADJUSTMENT_APP.ADJ_HEADER
WHERE PROCESS_TYPE IN ('FRTB', 'FRTBDRC', 'FRTBRRAO', 'FRTBALL')
  AND RUN_STATUS = 'Pending'
  AND BLOCKED_BY_ADJ_ID IS NULL
  AND IS_DELETED = FALSE;

CREATE OR REPLACE VIEW ADJUSTMENT_APP.VW_QUEUE_SENSITIVITY
    COMMENT = 'Eligible Sensitivity adjustments: Pending + unblocked. Stream source for TASK_PROCESS_SENSITIVITY.'
AS
SELECT * FROM ADJUSTMENT_APP.ADJ_HEADER
WHERE PROCESS_TYPE = 'Sensitivity'
  AND RUN_STATUS = 'Pending'
  AND BLOCKED_BY_ADJ_ID IS NULL
  AND IS_DELETED = FALSE;
```

- [ ] **Step 2: Verify**

```sql
-- Should return 4 rows
SHOW VIEWS LIKE 'VW_QUEUE_%' IN SCHEMA ADJUSTMENT_APP;

-- With a Pending+unblocked ADJ_HEADER row for VaR, this returns it
SELECT COUNT(*) FROM ADJUSTMENT_APP.VW_QUEUE_VAR;
```

- [ ] **Step 3: Update status references in existing views in 08_views.sql**

In `VW_DASHBOARD_KPI`, change the `ERROR_COUNT` alias line and add `RUNNING_COUNT`:
```sql
-- Replace:
COUNT(DISTINCT CASE WHEN h.RUN_STATUS = 'Error'              THEN h.ADJ_ID END) AS ERROR_COUNT,
-- With:
COUNT(DISTINCT CASE WHEN h.RUN_STATUS = 'Running'            THEN h.ADJ_ID END) AS RUNNING_COUNT,
COUNT(DISTINCT CASE WHEN h.RUN_STATUS = 'Failed'             THEN h.ADJ_ID END) AS FAILED_COUNT,
```

In `VW_ERRORS`, change the WHERE clause:
```sql
-- Replace:
WHERE h.RUN_STATUS = 'Error'
-- With:
WHERE h.RUN_STATUS = 'Failed'
```

In `VW_PROCESSING_QUEUE`, update the WHERE and CASE:
```sql
-- Replace:
WHERE h.RUN_STATUS IN ('Pending', 'Approved', 'Processing')
-- With:
WHERE h.RUN_STATUS IN ('Pending', 'Running')

-- Replace the ROW_NUMBER CASE:
CASE h.ADJUSTMENT_OCCURRENCE WHEN 'ADHOC' THEN 0 ELSE 1 END,
-- (keep as-is, ordering logic is still valid)
```

- [ ] **Step 4: Commit**

```bash
git add new_adjustment_db_objects/08_views.sql
git commit -m "feat: add VW_QUEUE_* views per scope; update status labels (Error→Failed, Processing→Running)"
```

---

## Task 3: Streams — 4 scope-specific view streams

**Files:**
- Replace: `new_adjustment_db_objects/02_streams.sql`

- [ ] **Step 1: Replace the entire file content**

```sql
-- =============================================================================
-- 02_STREAMS.SQL
-- One stream per scope pipeline, each on its queue view.
--
-- APPEND_ONLY = TRUE: we only care about rows becoming eligible (appearing
-- in the view). Rows leaving the view (claimed as Running) are not tracked.
--
-- PREREQUISITE: Task 1 must run first (CHANGE_TRACKING = TRUE on ADJ_HEADER).
-- PREREQUISITE: Task 2 must run first (VW_QUEUE_* views must exist).
-- =============================================================================

USE DATABASE DVLP_RAPTOR_NEWADJ;
USE SCHEMA ADJUSTMENT_APP;

CREATE OR REPLACE STREAM ADJUSTMENT_APP.STREAM_QUEUE_VAR
    ON VIEW ADJUSTMENT_APP.VW_QUEUE_VAR
    APPEND_ONLY = TRUE
    COMMENT = 'Fires when new eligible VaR adjustments appear (Pending + unblocked). Triggers TASK_PROCESS_VAR.';

CREATE OR REPLACE STREAM ADJUSTMENT_APP.STREAM_QUEUE_STRESS
    ON VIEW ADJUSTMENT_APP.VW_QUEUE_STRESS
    APPEND_ONLY = TRUE
    COMMENT = 'Fires when new eligible Stress adjustments appear. Triggers TASK_PROCESS_STRESS.';

CREATE OR REPLACE STREAM ADJUSTMENT_APP.STREAM_QUEUE_FRTB
    ON VIEW ADJUSTMENT_APP.VW_QUEUE_FRTB
    APPEND_ONLY = TRUE
    COMMENT = 'Fires when new eligible FRTB-pipeline adjustments appear (all sub-types). Triggers TASK_PROCESS_FRTB.';

CREATE OR REPLACE STREAM ADJUSTMENT_APP.STREAM_QUEUE_SENSITIVITY
    ON VIEW ADJUSTMENT_APP.VW_QUEUE_SENSITIVITY
    APPEND_ONLY = TRUE
    COMMENT = 'Fires when new eligible Sensitivity adjustments appear. Triggers TASK_PROCESS_SENSITIVITY.';

-- ═══════════════════════════════════════════════════════════════════════════
-- VERIFY
-- ═══════════════════════════════════════════════════════════════════════════
SHOW STREAMS LIKE 'STREAM_QUEUE_%' IN SCHEMA ADJUSTMENT_APP;
```

- [ ] **Step 2: Verify**

```sql
SHOW STREAMS LIKE 'STREAM_QUEUE_%' IN SCHEMA ADJUSTMENT_APP;
-- Expected: 4 streams, mode APPEND_ONLY, source_type VIEW

SELECT SYSTEM$STREAM_HAS_DATA('ADJUSTMENT_APP.STREAM_QUEUE_VAR');
-- Expected: FALSE (no pending unblocked data yet)
```

- [ ] **Step 3: Commit**

```bash
git add new_adjustment_db_objects/02_streams.sql
git commit -m "feat: replace single ADJ_HEADER stream with 4 scope-specific view streams"
```

---

## Task 4: SP_PROCESS_ADJUSTMENT — Read Running, set Failed

**Files:**
- Modify: `new_adjustment_db_objects/05_sp_process_adjustment.sql`

The SP is now called **after** the task has already claimed adjustments as `Running`. It reads `Running` rows, processes them, then marks them `Processed` or `Failed`.

- [ ] **Step 1: Update `update_header_status` — change condition guard**

Find and replace the condition inside `update_header_status`:
```python
# Replace:
(adj_header["RUN_STATUS"].isin(["Pending", "Approved"]))
# With:
(adj_header["RUN_STATUS"] == "Running")
```

- [ ] **Step 2: Update `df_adj` filter — read Running not Pending**

Find this block (around line 163):
```python
df_adj = session.table(adj_base_tbl_name).filter(
    (col('COBID') == cobid) &
    ((upper(col('PROCESS_TYPE')) == process_type.upper()) |
     (upper(col('PROCESS_TYPE')) == 'FRTBALL')) &
    (col('RUN_STATUS').isin(['Pending', 'Approved']))
)
```
Replace with:
```python
df_adj = session.table(adj_base_tbl_name).filter(
    (col('COBID') == cobid) &
    ((upper(col('PROCESS_TYPE')) == process_type.upper()) |
     (upper(col('PROCESS_TYPE')) == 'FRTBALL')) &
    (col('RUN_STATUS') == 'Running')
)
```

- [ ] **Step 3: Update `from_where` SQL — Running filter**

Find (around line 346):
```python
AND adjust.RUN_STATUS IN ('Pending', 'Approved')
```
Replace with:
```python
AND adjust.RUN_STATUS = 'Running'
```

- [ ] **Step 4: Update `log_status_history` calls — old_status is now Running**

Find both calls to `log_status_history`:
```python
# Direct path (line ~233):
log_status_history(session, adj_ids, "Pending", "Processed")
# Scale path (line ~556):
log_status_history(session, adj_ids, "Pending", "Processed")
```
Replace both with:
```python
log_status_history(session, adj_ids, "Running", "Processed")
```

- [ ] **Step 5: Update error handler — set Failed not Error**

Find the error block at the bottom (around line 604):
```python
update_header_status(session, df_adj_err, cobid, "Error", error_msg)
```
Replace with:
```python
update_header_status(session, df_adj_err, cobid, "Failed", error_msg)
```

Also update the filter for the error fallback:
```python
# Replace:
(col('RUN_STATUS').isin(['Pending', 'Approved']))
# With:
(col('RUN_STATUS') == 'Running')
```

- [ ] **Step 6: Verify — deploy and smoke test**

```sql
-- After deploying, check the SP compiles without error:
DESCRIBE PROCEDURE ADJUSTMENT_APP.SP_PROCESS_ADJUSTMENT(VARCHAR, VARCHAR, NUMBER);

-- Manually set a test adjustment to Running, then call:
UPDATE ADJUSTMENT_APP.ADJ_HEADER SET RUN_STATUS = 'Running' WHERE ADJ_ID = <test_id>;
CALL ADJUSTMENT_APP.SP_PROCESS_ADJUSTMENT('VaR', 'Scale', <test_cobid>);
SELECT RUN_STATUS FROM ADJUSTMENT_APP.ADJ_HEADER WHERE ADJ_ID = <test_id>;
-- Expected: 'Processed' (or 'Failed' with error message if fact data missing)
```

- [ ] **Step 7: Commit**

```bash
git add new_adjustment_db_objects/05_sp_process_adjustment.sql
git commit -m "feat: SP_PROCESS_ADJUSTMENT reads Running adjustments and sets Failed on error"
```

---

## Task 5: SP_RUN_PIPELINE — Claim, block, process, unblock

**Files:**
- Create: `new_adjustment_db_objects/05b_sp_run_pipeline.sql`

This SP is the brain of each scope pipeline. The 4 tasks call it with their scope name and pipeline type list.

- [ ] **Step 1: Create the file**

```sql
-- =============================================================================
-- 05B_SP_RUN_PIPELINE.SQL
-- Pipeline orchestrator: claim → block → process → unblock.
--
-- Called by the 4 scope tasks. One call per task run.
-- Parameters:
--   scope           e.g. 'VaR', 'FRTB'
--   pipeline_types  JSON array of PROCESS_TYPE strings in this pipeline
--                   e.g. '["FRTB","FRTBDRC","FRTBRRAO","FRTBALL"]'
--
-- FRTBALL note: FRTBALL has no settings row. It is applied as a fan-out
-- during FRTB / FRTBDRC / FRTBRRAO processing. The loop skips FRTBALL as
-- a standalone call target — SP_PROCESS_ADJUSTMENT picks it up automatically
-- when processing any real FRTB sub-type.
-- =============================================================================

USE DATABASE DVLP_RAPTOR_NEWADJ;
USE SCHEMA ADJUSTMENT_APP;

CREATE OR REPLACE PROCEDURE ADJUSTMENT_APP.SP_RUN_PIPELINE(
    scope          VARCHAR,
    pipeline_types VARCHAR   -- JSON array string: '["VaR"]' or '["FRTB","FRTBDRC","FRTBRRAO","FRTBALL"]'
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
COMMENT = 'Pipeline orchestrator per scope. Claims eligible Pending adjustments, blocks overlapping ones, calls SP_PROCESS_ADJUSTMENT, then unblocks waiting adjustments.'
EXECUTE AS CALLER
AS
$$
import json

# Dimensions used for overlap detection (NULL = wildcard = matches any value)
OVERLAP_DIMS = [
    'ENTITY_CODE', 'SOURCE_SYSTEM_CODE', 'DEPARTMENT_CODE',
    'BOOK_CODE', 'CURRENCY_CODE', 'TRADE_TYPOLOGY', 'STRATEGY',
]

# FRTBALL is a fan-out tag — it is applied within real FRTB* SP calls,
# not as a standalone SP_PROCESS_ADJUSTMENT target.
FRTBALL_SKIP = {'FRTBALL'}


def main(session, scope, pipeline_types):
    types = json.loads(pipeline_types)
    pipeline_in = ", ".join(f"'{t}'" for t in types)
    real_types = [t for t in types if t not in FRTBALL_SKIP]
    real_in = ", ".join(f"'{t}'" for t in real_types) if real_types else pipeline_in

    results = []

    # ── 1. Atomically claim all eligible Pending → Running ───────────────
    session.sql(f"""
        UPDATE ADJUSTMENT_APP.ADJ_HEADER
        SET RUN_STATUS = 'Running',
            PROCESS_DATE = CONVERT_TIMEZONE('Europe/London', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9)
        WHERE PROCESS_TYPE IN ({pipeline_in})
          AND RUN_STATUS = 'Pending'
          AND BLOCKED_BY_ADJ_ID IS NULL
          AND IS_DELETED = FALSE
    """).collect()

    # ── 2. Read all currently Running adjustments for blocking setup ──────
    all_running = session.sql(f"""
        SELECT ADJ_ID, COBID, PROCESS_TYPE,
               ENTITY_CODE, SOURCE_SYSTEM_CODE, DEPARTMENT_CODE,
               BOOK_CODE, CURRENCY_CODE, TRADE_TYPOLOGY, STRATEGY
        FROM ADJUSTMENT_APP.ADJ_HEADER
        WHERE PROCESS_TYPE IN ({pipeline_in})
          AND RUN_STATUS = 'Running'
          AND IS_DELETED = FALSE
    """).collect()

    if not all_running:
        return json.dumps({"scope": scope, "message": "No eligible adjustments found"})

    # ── 3. For each Running adjustment, block overlapping Pending ones ────
    for r in all_running:
        _block_overlapping(session, r, pipeline_in)

    # ── 4. Process each real process-type combination (oldest first) ──────
    to_process = session.sql(f"""
        SELECT DISTINCT PROCESS_TYPE, ADJUSTMENT_ACTION, COBID
        FROM ADJUSTMENT_APP.ADJ_HEADER
        WHERE PROCESS_TYPE IN ({real_in})
          AND RUN_STATUS = 'Running'
          AND IS_DELETED = FALSE
        ORDER BY PROCESS_TYPE, ADJUSTMENT_ACTION, COBID
    """).collect()

    for row in to_process:
        pt  = row["PROCESS_TYPE"]
        act = row["ADJUSTMENT_ACTION"]
        cob = row["COBID"]
        try:
            session.sql(f"""
                CALL ADJUSTMENT_APP.SP_PROCESS_ADJUSTMENT('{pt}', '{act}', {cob})
            """).collect()
            results.append({"process_type": pt, "cobid": cob, "status": "ok"})
        except Exception as e:
            err = str(e)[:990].replace("'", "")
            # Mark the Running adjustments for this specific call as Failed
            session.sql(f"""
                UPDATE ADJUSTMENT_APP.ADJ_HEADER
                SET RUN_STATUS = 'Failed', ERRORMESSAGE = '{err}'
                WHERE PROCESS_TYPE IN ('{pt}', 'FRTBALL')
                  AND ADJUSTMENT_ACTION = '{act}'
                  AND COBID = {cob}
                  AND RUN_STATUS = 'Running'
                  AND IS_DELETED = FALSE
            """).collect()
            results.append({"process_type": pt, "cobid": cob, "status": "failed", "error": err})

    # ── 5. Unblock adjustments whose blocker has now finished ─────────────
    _unblock_resolved(session, pipeline_in, all_running)

    return json.dumps({"scope": scope, "processed": results})


def _block_overlapping(session, running_row, pipeline_in):
    """
    For the given Running adjustment, find all Pending adjustments in the
    same pipeline + same COBID that overlap with it, and set their
    BLOCKED_BY_ADJ_ID.

    Overlap rule per dimension: NULL (wildcard) overlaps with any value.
    """
    adj_id = running_row["ADJ_ID"]
    cobid  = running_row["COBID"]

    dim_conditions = []
    for dim in OVERLAP_DIMS:
        val = running_row[dim]
        if val is None:
            # Running adj is wildcard → overlaps with everything → no restriction
            dim_conditions.append("TRUE")
        else:
            escaped = str(val).replace("'", "''")
            dim_conditions.append(
                f"(p.{dim} IS NULL OR UPPER(p.{dim}) = UPPER('{escaped}'))"
            )

    where_dims = " AND ".join(dim_conditions)

    session.sql(f"""
        UPDATE ADJUSTMENT_APP.ADJ_HEADER p
        SET BLOCKED_BY_ADJ_ID = {adj_id}
        WHERE p.COBID = {cobid}
          AND p.PROCESS_TYPE IN ({pipeline_in})
          AND p.RUN_STATUS = 'Pending'
          AND p.BLOCKED_BY_ADJ_ID IS NULL
          AND p.IS_DELETED = FALSE
          AND p.ADJ_ID != {adj_id}
          AND {where_dims}
    """).collect()


def _unblock_resolved(session, pipeline_in, previously_running):
    """
    After processing, clear BLOCKED_BY_ADJ_ID for any adjustment whose
    blocker has finished (Processed or Failed) and that no other Running
    adjustment still overlaps.

    Two-step:
      1. Collect adj_ids that were Running (now Processed/Failed).
      2. Find Pending adjs blocked by any of those → try to reassign to a
         still-Running overlapping adj, or clear to NULL if none.
    """
    finished_ids = [str(r["ADJ_ID"]) for r in previously_running]
    if not finished_ids:
        return

    finished_in = ", ".join(finished_ids)

    # Find all Pending adjustments blocked by a now-finished adj
    blocked = session.sql(f"""
        SELECT b.ADJ_ID, b.COBID, b.BLOCKED_BY_ADJ_ID,
               b.ENTITY_CODE, b.SOURCE_SYSTEM_CODE, b.DEPARTMENT_CODE,
               b.BOOK_CODE, b.CURRENCY_CODE, b.TRADE_TYPOLOGY, b.STRATEGY
        FROM ADJUSTMENT_APP.ADJ_HEADER b
        WHERE b.BLOCKED_BY_ADJ_ID IN ({finished_in})
          AND b.RUN_STATUS = 'Pending'
          AND b.IS_DELETED = FALSE
    """).collect()

    for b in blocked:
        b_adj_id = b["ADJ_ID"]
        cobid    = b["COBID"]

        # Build overlap conditions to check against still-Running adjustments
        dim_conditions = []
        for dim in OVERLAP_DIMS:
            val = b[dim]
            if val is None:
                dim_conditions.append("TRUE")
            else:
                escaped = str(val).replace("'", "''")
                dim_conditions.append(
                    f"(r.{dim} IS NULL OR UPPER(r.{dim}) = UPPER('{escaped}'))"
                )
        where_dims = " AND ".join(dim_conditions)

        # Is there another Running adj that still overlaps?
        other_running = session.sql(f"""
            SELECT ADJ_ID FROM ADJUSTMENT_APP.ADJ_HEADER r
            WHERE r.COBID = {cobid}
              AND r.PROCESS_TYPE IN ({pipeline_in})
              AND r.RUN_STATUS = 'Running'
              AND r.ADJ_ID NOT IN ({finished_in})
              AND r.IS_DELETED = FALSE
              AND {where_dims}
            LIMIT 1
        """).collect()

        if other_running:
            new_blocker = other_running[0]["ADJ_ID"]
            session.sql(f"""
                UPDATE ADJUSTMENT_APP.ADJ_HEADER
                SET BLOCKED_BY_ADJ_ID = {new_blocker}
                WHERE ADJ_ID = {b_adj_id}
            """).collect()
        else:
            session.sql(f"""
                UPDATE ADJUSTMENT_APP.ADJ_HEADER
                SET BLOCKED_BY_ADJ_ID = NULL
                WHERE ADJ_ID = {b_adj_id}
            """).collect()
$$;
```

- [ ] **Step 2: Verify deployment**

```sql
DESCRIBE PROCEDURE ADJUSTMENT_APP.SP_RUN_PIPELINE(VARCHAR, VARCHAR);
-- Expected: procedure exists, LANGUAGE PYTHON, HANDLER main
```

- [ ] **Step 3: Smoke test with a Pending VaR adjustment**

```sql
-- Insert a test Pending VaR adj (use a real COBID from your environment)
INSERT INTO ADJUSTMENT_APP.ADJ_HEADER
    (COBID, PROCESS_TYPE, ADJUSTMENT_TYPE, ADJUSTMENT_ACTION, SCALE_FACTOR,
     SCALE_FACTOR_ADJUSTED, RUN_STATUS, USERNAME, BOOK_CODE)
VALUES
    (20250328, 'VaR', 'Flatten', 'Scale', 1, -1, 'Pending', 'TEST_USER', 'TESTBOOK');

-- Run the pipeline manually
CALL ADJUSTMENT_APP.SP_RUN_PIPELINE('VaR', '["VaR"]');

-- Verify the adj was claimed and processed
SELECT ADJ_ID, RUN_STATUS, BLOCKED_BY_ADJ_ID
FROM ADJUSTMENT_APP.ADJ_HEADER
WHERE USERNAME = 'TEST_USER' ORDER BY ADJ_ID DESC LIMIT 1;
-- Expected: RUN_STATUS = 'Processed' (or 'Failed' if no fact data), BLOCKED_BY_ADJ_ID = NULL
```

- [ ] **Step 4: Smoke test blocking behaviour**

```sql
-- Insert two overlapping Pending VaR adjs (same BOOK_CODE)
INSERT INTO ADJUSTMENT_APP.ADJ_HEADER
    (COBID, PROCESS_TYPE, ADJUSTMENT_TYPE, ADJUSTMENT_ACTION, SCALE_FACTOR,
     SCALE_FACTOR_ADJUSTED, RUN_STATUS, USERNAME, BOOK_CODE)
VALUES
    (20250328, 'VaR', 'Flatten', 'Scale', 1, -1, 'Pending', 'TEST_USER', 'BLOCKTEST');

INSERT INTO ADJUSTMENT_APP.ADJ_HEADER
    (COBID, PROCESS_TYPE, ADJUSTMENT_TYPE, ADJUSTMENT_ACTION, SCALE_FACTOR,
     SCALE_FACTOR_ADJUSTED, RUN_STATUS, USERNAME, BOOK_CODE)
VALUES
    (20250328, 'VaR', 'Scale', 'Scale', 1.1, 0.1, 'Pending', 'TEST_USER', 'BLOCKTEST');

-- Note the two ADJ_IDs:
SELECT ADJ_ID, RUN_STATUS, BLOCKED_BY_ADJ_ID FROM ADJUSTMENT_APP.ADJ_HEADER
WHERE USERNAME = 'TEST_USER' AND BOOK_CODE = 'BLOCKTEST' ORDER BY ADJ_ID;

-- Manually set the first one Running (simulating mid-task state):
UPDATE ADJUSTMENT_APP.ADJ_HEADER SET RUN_STATUS = 'Running'
WHERE ADJ_ID = <first_adj_id>;

-- Run _block_overlapping logic (the full pipeline call will also trigger it):
CALL ADJUSTMENT_APP.SP_RUN_PIPELINE('VaR', '["VaR"]');

-- The second adj should now be blocked:
SELECT ADJ_ID, RUN_STATUS, BLOCKED_BY_ADJ_ID FROM ADJUSTMENT_APP.ADJ_HEADER
WHERE USERNAME = 'TEST_USER' AND BOOK_CODE = 'BLOCKTEST' ORDER BY ADJ_ID;
-- Expected: second adj has BLOCKED_BY_ADJ_ID = first adj's ADJ_ID

-- Clean up test rows
DELETE FROM ADJUSTMENT_APP.ADJ_HEADER WHERE USERNAME = 'TEST_USER';
```

- [ ] **Step 5: Commit**

```bash
git add new_adjustment_db_objects/05b_sp_run_pipeline.sql
git commit -m "feat: add SP_RUN_PIPELINE — claim/block/process/unblock orchestrator for scope pipelines"
```

---

## Task 6: SP_SUBMIT_ADJUSTMENT — Block at submit time

**Files:**
- Modify: `new_adjustment_db_objects/03_sp_submit_adjustment.sql`

When a new adjustment is submitted, check for Running adjustments in the same pipeline that overlap. If found, set `BLOCKED_BY_ADJ_ID` in the INSERT so the row never appears in the queue view while blocked.

- [ ] **Step 1: Add pipeline type map and overlap helper at the top of the Python handler**

After the `ACTION_MAP` dict (around line 33), add:

```python
# Pipeline groupings — used for blocking checks
PIPELINE_TYPES = {
    'VaR':         ['VaR'],
    'Stress':      ['Stress'],
    'Sensitivity': ['Sensitivity'],
    'ES':          ['ES'],
    'FRTB':        ['FRTB', 'FRTBDRC', 'FRTBRRAO', 'FRTBALL'],
    'FRTBDRC':     ['FRTB', 'FRTBDRC', 'FRTBRRAO', 'FRTBALL'],
    'FRTBRRAO':    ['FRTB', 'FRTBDRC', 'FRTBRRAO', 'FRTBALL'],
    'FRTBALL':     ['FRTB', 'FRTBDRC', 'FRTBRRAO', 'FRTBALL'],
}

OVERLAP_DIMS_SUBMIT = [
    'ENTITY_CODE', 'SOURCE_SYSTEM_CODE', 'DEPARTMENT_CODE',
    'BOOK_CODE', 'CURRENCY_CODE', 'TRADE_TYPOLOGY', 'STRATEGY',
]

def find_blocking_adj(session, process_type, cobid, adj_values):
    """
    Return ADJ_ID of a Running adjustment in the same pipeline that overlaps
    with the new adjustment being submitted, or None if no blocker found.

    adj_values: dict with optional dimension values for the new adjustment.
    """
    pipeline = PIPELINE_TYPES.get(process_type.upper(), [process_type])
    pipeline_in = ", ".join(f"'{t}'" for t in pipeline)

    dim_conditions = []
    for dim in OVERLAP_DIMS_SUBMIT:
        new_val = adj_values.get(dim.lower())
        if new_val is None:
            # New adj is wildcard → overlaps with everything → no dim restriction
            dim_conditions.append("TRUE")
        else:
            escaped = str(new_val).replace("'", "''")
            dim_conditions.append(
                f"(r.{dim} IS NULL OR UPPER(r.{dim}) = UPPER('{escaped}'))"
            )

    where_dims = " AND ".join(dim_conditions)

    sql = f"""
        SELECT ADJ_ID FROM ADJUSTMENT_APP.ADJ_HEADER r
        WHERE r.COBID = {cobid}
          AND r.PROCESS_TYPE IN ({pipeline_in})
          AND r.RUN_STATUS = 'Running'
          AND r.IS_DELETED = FALSE
          AND {where_dims}
        ORDER BY r.ADJ_ID ASC
        LIMIT 1
    """
    rows = session.sql(sql).collect()
    return rows[0]["ADJ_ID"] if rows else None
```

- [ ] **Step 2: Call the blocker check before INSERT**

In the `main` function, find the section that builds `col_map` (around line 168). Just before that, add the blocking check. The check should only run when `initial_status == STATUS_PENDING` (blocked-at-submit only applies to adjustments entering the processing queue):

```python
# ── Blocking check — done before INSERT ─────────────────────────────
blocked_by_adj_id = None
if initial_status == STATUS_PENDING:
    dim_vals = {
        "entity_code":          adj.get("entity_code"),
        "source_system_code":   adj.get("source_system_code"),
        "department_code":      adj.get("department_code"),
        "book_code":            adj.get("book_code"),
        "currency_code":        adj.get("currency_code"),
        "trade_typology":       adj.get("trade_typology"),
        "strategy":             adj.get("strategy"),
    }
    blocked_by_adj_id = find_blocking_adj(session, process_type, cobid, dim_vals)
```

- [ ] **Step 3: Include BLOCKED_BY_ADJ_ID in col_map**

In the `col_map` dict, add after `"APPROVAL_ID"`:

```python
"BLOCKED_BY_ADJ_ID": blocked_by_adj_id,
```

- [ ] **Step 4: Remove the immediate processing call for ad-hoc adjustments**

The old behaviour called `SP_PROCESS_ADJUSTMENT` immediately for ad-hoc adjustments. With the new pipeline, processing is handled entirely by the scope tasks (triggered by the stream). Remove the block:

```python
# REMOVE THIS ENTIRE BLOCK:
# ── Ad-hoc + Pending → process immediately ──────────────────────
if occurrence == "ADHOC" and initial_status == STATUS_PENDING:
    if adj_action == "Scale":
        session.sql(f"""
            CALL ADJUSTMENT_APP.SP_PROCESS_ADJUSTMENT(
                '{process_type}', 'Scale', {cobid})
        """).collect()
    elif adj_action == "Direct":
        session.sql(f"""
            CALL ADJUSTMENT_APP.SP_PROCESS_ADJUSTMENT(
                '{process_type}', 'Direct', {cobid})
        """).collect()
```

Also update the return message (remove the "Processing triggered" suffix):
```python
return {
    "adj_id":  adj_id,
    "status":  initial_status,
    "message": f"Adjustment {adj_id} created with status '{initial_status}'."
               + (" Blocked by ADJ #{}.".format(blocked_by_adj_id) if blocked_by_adj_id else "")
}
```

- [ ] **Step 5: Verify**

```sql
-- Insert a Running adj manually
UPDATE ADJUSTMENT_APP.ADJ_HEADER SET RUN_STATUS = 'Running' WHERE ADJ_ID = <some_id>;

-- Submit a new overlapping adj via the SP
CALL ADJUSTMENT_APP.SP_SUBMIT_ADJUSTMENT('{
    "cobid": <same_cobid>,
    "process_type": "VaR",
    "adjustment_type": "Flatten",
    "username": "TEST",
    "book_code": "<same_book>"
}');

-- Check result: new adj should have BLOCKED_BY_ADJ_ID set
SELECT ADJ_ID, RUN_STATUS, BLOCKED_BY_ADJ_ID
FROM ADJUSTMENT_APP.ADJ_HEADER
WHERE USERNAME = 'TEST' ORDER BY ADJ_ID DESC LIMIT 1;
-- Expected: BLOCKED_BY_ADJ_ID = <some_id>, RUN_STATUS = 'Pending'

-- Check it does NOT appear in the queue view:
SELECT COUNT(*) FROM ADJUSTMENT_APP.VW_QUEUE_VAR WHERE ADJ_ID = <new_adj_id>;
-- Expected: 0

-- Clean up
DELETE FROM ADJUSTMENT_APP.ADJ_HEADER WHERE USERNAME = 'TEST';
UPDATE ADJUSTMENT_APP.ADJ_HEADER SET RUN_STATUS = 'Processed' WHERE ADJ_ID = <some_id>;
```

- [ ] **Step 6: Commit**

```bash
git add new_adjustment_db_objects/03_sp_submit_adjustment.sql
git commit -m "feat: block overlapping adjustments at submit time; remove immediate processing call"
```

---

## Task 7: Tasks — 4 scope-specific pipeline tasks

**Files:**
- Replace: `new_adjustment_db_objects/06_tasks.sql`

- [ ] **Step 1: Replace the entire file content**

```sql
-- =============================================================================
-- 06_TASKS.SQL
-- Four independent scope-pipeline tasks.
-- Each task:
--   • Is triggered by its scope's stream (stream-guarded, not time-only)
--   • Calls SP_RUN_PIPELINE with its scope name and pipeline type list
--   • Runs on DVLP_RAPTOR_WH_XS (can be changed per scope independently)
--
-- FRTB pipeline covers: FRTB, FRTBDRC, FRTBRRAO, FRTBALL
-- FRTBALL is a fan-out tag applied within each real FRTB sub-type call.
-- =============================================================================

USE DATABASE DVLP_RAPTOR_NEWADJ;
USE SCHEMA ADJUSTMENT_APP;

-- ─── VaR ───────────────────────────────────────────────────────────────────

CREATE OR REPLACE TASK ADJUSTMENT_APP.TASK_PROCESS_VAR
    WAREHOUSE = DVLP_RAPTOR_WH_XS
    SCHEDULE  = '1 MINUTE'
    COMMENT   = 'Processes eligible VaR adjustments. Stream-triggered via STREAM_QUEUE_VAR.'
    WHEN SYSTEM$STREAM_HAS_DATA('ADJUSTMENT_APP.STREAM_QUEUE_VAR')
AS
    CALL ADJUSTMENT_APP.SP_RUN_PIPELINE('VaR', '["VaR"]');


-- ─── Stress ────────────────────────────────────────────────────────────────

CREATE OR REPLACE TASK ADJUSTMENT_APP.TASK_PROCESS_STRESS
    WAREHOUSE = DVLP_RAPTOR_WH_XS
    SCHEDULE  = '1 MINUTE'
    COMMENT   = 'Processes eligible Stress adjustments. Stream-triggered via STREAM_QUEUE_STRESS.'
    WHEN SYSTEM$STREAM_HAS_DATA('ADJUSTMENT_APP.STREAM_QUEUE_STRESS')
AS
    CALL ADJUSTMENT_APP.SP_RUN_PIPELINE('Stress', '["Stress"]');


-- ─── FRTB (all sub-types) ───────────────────────────────────────────────────

CREATE OR REPLACE TASK ADJUSTMENT_APP.TASK_PROCESS_FRTB
    WAREHOUSE = DVLP_RAPTOR_WH_XS
    SCHEDULE  = '1 MINUTE'
    COMMENT   = 'Processes eligible FRTB-pipeline adjustments (FRTB, FRTBDRC, FRTBRRAO, FRTBALL). Stream-triggered via STREAM_QUEUE_FRTB.'
    WHEN SYSTEM$STREAM_HAS_DATA('ADJUSTMENT_APP.STREAM_QUEUE_FRTB')
AS
    CALL ADJUSTMENT_APP.SP_RUN_PIPELINE('FRTB', '["FRTB","FRTBDRC","FRTBRRAO","FRTBALL"]');


-- ─── Sensitivity ───────────────────────────────────────────────────────────

CREATE OR REPLACE TASK ADJUSTMENT_APP.TASK_PROCESS_SENSITIVITY
    WAREHOUSE = DVLP_RAPTOR_WH_XS
    SCHEDULE  = '1 MINUTE'
    COMMENT   = 'Processes eligible Sensitivity adjustments. Stream-triggered via STREAM_QUEUE_SENSITIVITY.'
    WHEN SYSTEM$STREAM_HAS_DATA('ADJUSTMENT_APP.STREAM_QUEUE_SENSITIVITY')
AS
    CALL ADJUSTMENT_APP.SP_RUN_PIPELINE('Sensitivity', '["Sensitivity"]');


-- ═══════════════════════════════════════════════════════════════════════════
-- SUSPEND ALL (enable when ready)
-- ═══════════════════════════════════════════════════════════════════════════

ALTER TASK ADJUSTMENT_APP.TASK_PROCESS_VAR         SUSPEND;
ALTER TASK ADJUSTMENT_APP.TASK_PROCESS_STRESS       SUSPEND;
ALTER TASK ADJUSTMENT_APP.TASK_PROCESS_FRTB         SUSPEND;
ALTER TASK ADJUSTMENT_APP.TASK_PROCESS_SENSITIVITY  SUSPEND;

-- To enable all four:
-- ALTER TASK ADJUSTMENT_APP.TASK_PROCESS_VAR         RESUME;
-- ALTER TASK ADJUSTMENT_APP.TASK_PROCESS_STRESS       RESUME;
-- ALTER TASK ADJUSTMENT_APP.TASK_PROCESS_FRTB         RESUME;
-- ALTER TASK ADJUSTMENT_APP.TASK_PROCESS_SENSITIVITY  RESUME;

-- ═══════════════════════════════════════════════════════════════════════════
-- VERIFY
-- ═══════════════════════════════════════════════════════════════════════════
SHOW TASKS LIKE 'TASK_PROCESS_%' IN SCHEMA ADJUSTMENT_APP;
```

- [ ] **Step 2: Verify**

```sql
SHOW TASKS LIKE 'TASK_PROCESS_%' IN SCHEMA ADJUSTMENT_APP;
-- Expected: 4 tasks, all SUSPENDED, each with correct WHEN clause
```

- [ ] **Step 3: Commit**

```bash
git add new_adjustment_db_objects/06_tasks.sql
git commit -m "feat: replace single PROCESS_PENDING_TASK with 4 independent scope pipeline tasks"
```

---

## Task 8: Dynamic Tables — Include Running in overlap alerts

**Files:**
- Modify: `new_adjustment_db_objects/07_dynamic_tables.sql`

`DT_OVERLAP_ALERTS` currently filters `RUN_STATUS IN ('Pending', 'Pending Approval', 'Approved', 'Processed')`. Add `'Running'` so in-progress adjustments are shown as overlapping warnings.

- [ ] **Step 1: Update the filter in `DT_OVERLAP_ALERTS`**

Find:
```sql
AND RUN_STATUS IN ('Pending', 'Pending Approval', 'Approved', 'Processed')
```
Replace with:
```sql
AND RUN_STATUS IN ('Pending', 'Pending Approval', 'Approved', 'Running', 'Processed')
```

- [ ] **Step 2: Verify**

```sql
-- Confirm dynamic table recreated successfully
SHOW DYNAMIC TABLES LIKE 'DT_OVERLAP_ALERTS' IN SCHEMA ADJUSTMENT_APP;
-- Check no compilation error by triggering a refresh:
ALTER DYNAMIC TABLE ADJUSTMENT_APP.DT_OVERLAP_ALERTS REFRESH;
SELECT COUNT(*) FROM ADJUSTMENT_APP.DT_OVERLAP_ALERTS;
```

- [ ] **Step 3: Commit**

```bash
git add new_adjustment_db_objects/07_dynamic_tables.sql
git commit -m "feat: include Running status in DT_OVERLAP_ALERTS detection"
```

---

## Task 9: Streamlit — Update styles for new statuses

**Files:**
- Modify: `streamlit_app/utils/styles.py`

- [ ] **Step 1: Update `STATUS_COLORS`**

Find `STATUS_COLORS` dict and replace:
```python
STATUS_COLORS = {
    "Pending":              "#FB8C00",
    "Pending Approval":     "#1565C0",
    "Approved":             "#00897B",
    "Running":              "#1565C0",   # ← add (blue = in-progress)
    "Processed":            "#388E3C",
    "Failed":               "#D32F2F",   # ← replaces "Error"
    "Rejected":             "#C62828",
    "Rejected - SignedOff": "#7B1FA2",
}
```

- [ ] **Step 2: Update `STATUS_ICONS`**

Find `STATUS_ICONS` dict and replace:
```python
STATUS_ICONS = {
    "Pending":              "⏳",
    "Pending Approval":     "📝",
    "Approved":             "✅",
    "Running":              "⚡",         # ← add
    "Processed":            "✔️",
    "Failed":               "❌",         # ← replaces "Error"
    "Rejected":             "❌",
    "Rejected - SignedOff": "🔒",
}
```

- [ ] **Step 3: Verify**

```python
# In a Python shell or Streamlit session:
from utils.styles import STATUS_COLORS, STATUS_ICONS
assert "Running" in STATUS_COLORS
assert "Failed" in STATUS_COLORS
assert "Error" not in STATUS_COLORS
assert STATUS_ICONS["Running"] == "⚡"
print("styles OK")
```

- [ ] **Step 4: Commit**

```bash
git add streamlit_app/utils/styles.py
git commit -m "feat: add Running/Failed to STATUS_COLORS and STATUS_ICONS"
```

---

## Task 10: Streamlit app.py — Update KPI cards

**Files:**
- Modify: `streamlit_app/app.py`

- [ ] **Step 1: Update the KPI query**

Find the KPI query block (around line 38) and replace the entire query string with:
```python
df_kpi = run_query_df("""
    SELECT
        COALESCE(SUM(TOTAL_ADJUSTMENTS), 0)       AS TOTAL,
        COALESCE(SUM(PENDING_COUNT), 0)            AS PENDING,
        COALESCE(SUM(PENDING_APPROVAL_COUNT), 0)   AS PENDING_APPROVAL,
        COALESCE(SUM(APPROVED_COUNT), 0)           AS APPROVED,
        COALESCE(SUM(RUNNING_COUNT), 0)            AS RUNNING,
        COALESCE(SUM(PROCESSED_COUNT), 0)          AS PROCESSED,
        COALESCE(SUM(FAILED_COUNT), 0)             AS FAILED,
        COALESCE(SUM(OVERLAP_ALERTS), 0)           AS OVERLAPS
    FROM ADJUSTMENT_APP.VW_DASHBOARD_KPI
""")
```

- [ ] **Step 2: Update the KPI cards list**

Find `kpi_data` (around line 55) and replace with:
```python
cols = st.columns(8)
kpi_data = [
    ("Total",            kpis.get("TOTAL", 0),            "All adjustments",  "primary"),
    ("Pending",          kpis.get("PENDING", 0),           "Awaiting queue",   "warning"),
    ("Awaiting Approval",kpis.get("PENDING_APPROVAL", 0),  "Need approval",    "info"),
    ("Approved",         kpis.get("APPROVED", 0),          "Ready to process", "success"),
    ("Running",          kpis.get("RUNNING", 0),           "Processing now",   "info"),
    ("Processed",        kpis.get("PROCESSED", 0),         "In the data",      "success"),
    ("Failed",           kpis.get("FAILED", 0),            "Need attention",   "danger"),
    ("Overlaps",         kpis.get("OVERLAPS", 0),          "Overlap alerts",   "purple"),
]
```

- [ ] **Step 3: Update status colour map in Recent Activity**

Find `_colour_status` function (around line 331) and replace:
```python
def _colour_status(val):
    colours = {
        "Processed":        f"color:{P['success']};font-weight:600",
        "Failed":           f"color:{P['danger']};font-weight:600",
        "Running":          f"color:{P['info']};font-weight:600",
        "Pending":          f"color:{P['warning']};font-weight:600",
        "Pending Approval": f"color:{P['info']};font-weight:600",
        "Approved":         "color:#00897B;font-weight:600",
    }
    return colours.get(val, "")
```

- [ ] **Step 4: Update status bar chart in Adjustments by Scope**

Find `statuses` list (around line 89) and replace:
```python
statuses = ["Pending", "Pending Approval", "Approved", "Running", "Processed", "Failed"]
color_map = {
    "Pending":          P["warning"],
    "Pending Approval": P["info"],
    "Approved":         "#00897B",
    "Running":          "#1565C0",
    "Processed":        P["success"],
    "Failed":           P["danger"],
}
```

- [ ] **Step 5: Commit**

```bash
git add streamlit_app/app.py
git commit -m "feat: update dashboard KPI cards and charts for Running/Failed statuses"
```

---

## Task 11: Streamlit Processing Queue — Update status labels and pipeline description

**Files:**
- Modify: `streamlit_app/pages/4_Processing_Queue.py`

- [ ] **Step 1: Update stat counters query and display**

Find the stats query (around line 79) and replace:
```python
df_stats = run_query_df("""
    SELECT
        COUNT(*)                                                  AS TOTAL,
        SUM(CASE WHEN RUN_STATUS = 'Pending'   THEN 1 ELSE 0 END) AS PENDING,
        SUM(CASE WHEN RUN_STATUS = 'Running'   THEN 1 ELSE 0 END) AS RUNNING,
        SUM(CASE WHEN RUN_STATUS = 'Processed' THEN 1 ELSE 0 END) AS PROCESSED,
        SUM(CASE WHEN RUN_STATUS = 'Failed'    THEN 1 ELSE 0 END) AS FAILED
    FROM ADJUSTMENT_APP.ADJ_HEADER
    WHERE IS_DELETED = FALSE
""")
```

Replace the `stat_items` list (around line 95):
```python
c1, c2, c3, c4 = st.columns(4)
stat_items = [
    ("Pending",    qs.get("PENDING", 0),    P["warning"], "⏸"),
    ("Running",    qs.get("RUNNING", 0),    "#1565C0",    "⚡"),
    ("Processed",  qs.get("PROCESSED", 0),  P["success"], "✔"),
    ("Failed",     qs.get("FAILED", 0),     P["danger"],  "✗"),
]
for col, (label, val, color, icon) in zip([c1, c2, c3, c4], stat_items):
```

- [ ] **Step 2: Update queue stage detection**

Find the stage detection block (around line 47):
```python
# Replace:
processing_count = int(df_q[df_q["RUN_STATUS"] == "Processing"].shape[0]) if not df_q.empty else 0
# With:
running_count = int(df_q[df_q["RUN_STATUS"] == "Running"].shape[0]) if not df_q.empty else 0

# Replace stage logic:
if running_count > 0:
    stage = 4   # SP executing
elif pending_count > 0:
    stage = 2   # Stream captured, task about to wake
else:
    stage = 5   # All done / idle
```

- [ ] **Step 3: Update active queue item status colours**

Find `status_color` and `status_icon` assignments (around line 137):
```python
# Replace:
status_color = P["info"] if run_status == "Processing" else ("#00897B" if run_status == "Approved" else P["warning"])
status_icon  = "⚡" if run_status == "Processing" else ("✅" if run_status == "Approved" else "⏸")
# With:
status_color = "#1565C0" if run_status == "Running" else ("#00897B" if run_status == "Approved" else P["warning"])
status_icon  = "⚡" if run_status == "Running" else ("✅" if run_status == "Approved" else "⏸")
```

- [ ] **Step 4: Update recently processed query and colour function**

Find the `df_recent` query (around line 170):
```python
# Replace:
WHERE RUN_STATUS IN ('Processed', 'Error')
# With:
WHERE RUN_STATUS IN ('Processed', 'Failed')
```

Find `color_status` function:
```python
def color_status(val):
    if val == "Processed":
        return f"color:{P['success']};font-weight:600"
    if val == "Failed":
        return f"color:{P['danger']};font-weight:600"
    return ""
```

- [ ] **Step 5: Update the pipeline description text**

Find the pipeline description grid (around line 62) and replace with:
```python
st.markdown(
    f'<div style="display:grid;grid-template-columns:repeat(5,1fr);gap:10px;'
    f'font-size:0.78rem;color:{P["grey_700"]};text-align:center;margin-top:0.3rem">'
    f'<div>Adjustment saved to<br/><strong>ADJ_HEADER</strong></div>'
    f'<div>CDC captured by<br/><strong>4 scope streams</strong><br/>(VaR/Stress/FRTB/Sensitivity)</div>'
    f'<div><strong>Scope task</strong><br/>wakes on stream data</div>'
    f'<div><strong>SP_RUN_PIPELINE</strong><br/>claim → block → process → unblock</div>'
    f'<div><strong>Dynamic Tables</strong><br/>auto-refresh (1 min lag)</div>'
    f'</div>',
    unsafe_allow_html=True)
```

- [ ] **Step 6: Update the task schedule reference table**

Find the markdown table in the expander (around line 211) and replace:
```python
st.markdown("""
| Task | Schedule | Trigger | Action |
|------|----------|---------|--------|
| `TASK_PROCESS_VAR` | Every 1 min | `STREAM_QUEUE_VAR` has data | Calls `SP_RUN_PIPELINE('VaR', ...)` |
| `TASK_PROCESS_STRESS` | Every 1 min | `STREAM_QUEUE_STRESS` has data | Calls `SP_RUN_PIPELINE('Stress', ...)` |
| `TASK_PROCESS_FRTB` | Every 1 min | `STREAM_QUEUE_FRTB` has data | Calls `SP_RUN_PIPELINE('FRTB', ...)` — covers FRTB, FRTBDRC, FRTBRRAO, FRTBALL |
| `TASK_PROCESS_SENSITIVITY` | Every 1 min | `STREAM_QUEUE_SENSITIVITY` has data | Calls `SP_RUN_PIPELINE('Sensitivity', ...)` |

**Processing Flow:**
1. User submits adjustment → `SP_SUBMIT_ADJUSTMENT` inserts into `ADJ_HEADER` (checks for Running overlaps → sets `BLOCKED_BY_ADJ_ID` if blocked)
2. Eligible adjustment appears in scope queue view (`VW_QUEUE_*`)
3. Stream detects new row → scope task wakes
4. `SP_RUN_PIPELINE`: atomically claims eligible Pending → Running, blocks overlapping Pending adjustments, calls `SP_PROCESS_ADJUSTMENT` per adjustment, unblocks waiting adjustments
5. Status updated to `Processed` (or `Failed` on error)
6. If an adjustment was blocked, `BLOCKED_BY_ADJ_ID` cleared → it appears in queue view → stream fires again

**Dynamic Table Refresh:**
- `DT_DASHBOARD` refreshes with **1-minute lag**
- `DT_OVERLAP_ALERTS` refreshes with **1-minute lag** — detects overlapping adjustments (includes Running)
""")
```

- [ ] **Step 7: Commit**

```bash
git add streamlit_app/pages/4_Processing_Queue.py
git commit -m "feat: update Processing Queue page for Running/Failed statuses and 4-pipeline architecture"
```

---

## Task 12: Documentation — Update unified_adjustment_design.md

**Files:**
- Modify: `context/unified_adjustment_design.md`

- [ ] **Step 1: Read the current file and update the processing architecture section**

Open `context/unified_adjustment_design.md`. Find the section describing the processing pipeline (streams, tasks, status flow). Replace it with the following content (adapt section headings to match the existing document style):

```markdown
## Processing Pipeline

### Architecture

Four independent pipelines run in parallel, one per Data Scope. A 15-minute FRTB adjustment has zero impact on VaR, Stress, or Sensitivity teams.

```
ADJ_HEADER
  ├── VW_QUEUE_VAR ──────── STREAM_QUEUE_VAR ──────── TASK_PROCESS_VAR
  ├── VW_QUEUE_STRESS ────── STREAM_QUEUE_STRESS ────── TASK_PROCESS_STRESS
  ├── VW_QUEUE_FRTB ─────── STREAM_QUEUE_FRTB ─────── TASK_PROCESS_FRTB
  └── VW_QUEUE_SENSITIVITY ─ STREAM_QUEUE_SENSITIVITY ─ TASK_PROCESS_SENSITIVITY
```

### Status Flow

| Status | Meaning |
|--------|---------|
| `Pending` | Submitted, waiting to be claimed by the scope task |
| `Running` | Claimed and actively being processed |
| `Processed` | Successfully written to the fact adjustment table |
| `Failed` | Error during processing — see `ERRORMESSAGE` on ADJ_HEADER |

Approval statuses (`Pending Approval`, `Approved`) are separate and precede the pipeline.

### Blocking (BLOCKED_BY_ADJ_ID)

Non-overlapping adjustments within the same scope can run concurrently. Overlapping adjustments are serialised using the `BLOCKED_BY_ADJ_ID` column on `ADJ_HEADER`.

- At **submit time**: `SP_SUBMIT_ADJUSTMENT` checks for Running adjustments in the same pipeline that overlap dimensionally. If found, sets `BLOCKED_BY_ADJ_ID` on the new row → the row is invisible in the queue view and never triggers the stream spuriously.
- At **task time**: `SP_RUN_PIPELINE` claims all eligible Pending → Running atomically, then blocks any remaining Pending adjustments that overlap with the newly Running ones.
- At **completion**: `SP_RUN_PIPELINE` clears `BLOCKED_BY_ADJ_ID` for adjustments whose blocker has finished. If no other Running adjustment still overlaps, the row becomes visible in the queue view → stream fires → task wakes.

### FRTBALL (fan-out)

`FRTBALL` is a special `PROCESS_TYPE` tag meaning "apply to all FRTB sub-types". When `SP_PROCESS_ADJUSTMENT` runs for `FRTB`, `FRTBDRC`, or `FRTBRRAO`, it also picks up `FRTBALL` adjustments for the same COBID. `FRTBALL` has no row in `ADJUSTMENTS_SETTINGS`; it borrows the sub-type's config at runtime.

### Deduplication

`SP_PROCESS_ADJUSTMENT` uses `DENSE_RANK() OVER (PARTITION BY <FACT_TABLE_PK> ORDER BY CREATED_DATE DESC)` to keep only the most recent adjustment delta per fact table row. The `FACT_TABLE_PK` is read from `ADJUSTMENTS_SETTINGS` per scope.
```

- [ ] **Step 2: Commit**

```bash
git add context/unified_adjustment_design.md
git commit -m "docs: update unified_adjustment_design.md with 4-pipeline architecture and blocking design"
```

---

## Task 13: New Adjustment page — update success screen and blocked message

**Files:**
- Modify: `streamlit_app/pages/1_New_Adjustment.py`

Two changes: (1) the Step 3 success screen should tell the user the adjustment is **queued** not immediately processed; (2) when the SP returns a `BLOCKED_BY_ADJ_ID`, show it clearly.

- [ ] **Step 1: Update Step 3 success screen**

Find the Step 3 block (around line 759). The `msg` variable comes from the SP response. Update the success subtitle line:

```python
# Find:
f'<div style="font-size:0.9rem;color:#388E3C;margin-top:0.4rem">{msg}</div>'
f'<div style="font-size:0.82rem;color:{P["grey_700"]};margin-top:0.8rem">'
# Replace with:
f'<div style="font-size:0.9rem;color:#388E3C;margin-top:0.4rem">{msg}</div>'
f'<div style="font-size:0.82rem;color:{P["info"]};margin-top:0.5rem">'
f'⚡ Your adjustment is queued and will be processed automatically by the scope pipeline.</div>'
f'<div style="font-size:0.82rem;color:{P["grey_700"]};margin-top:0.8rem">'
```

- [ ] **Step 2: Show blocked-by indicator on success**

Just after the `msg` lines in the Step 3 block, add a conditional warning if the returned `adj_id` is blocked:

```python
result = wiz.get("result") or {}
msg    = result.get("message", "Adjustment created successfully")
blocked_msg = ""
if "Blocked by ADJ #" in msg:
    blocked_msg = msg  # already contains "Blocked by ADJ #<id>" from SP

# In the HTML block, add after the queue message line:
# (add inside the same mcard div, before the closing </div>)
+ (f'<div style="font-size:0.8rem;color:{P["warning"]};margin-top:0.3rem">'
   f'⏳ {blocked_msg}</div>' if blocked_msg else '')
```

- [ ] **Step 3: Update error check — keep as-is**

The line `wiz["step"] = 3 if result.get("status") != "Error" else 2` uses `"Error"` as the SP response status (not the DB `RUN_STATUS`). This is correct and should **not** be changed — the SP still returns `{"status": "Error"}` for submission failures.

- [ ] **Step 4: Verify**

Submit a new adjustment in the UI. The Step 3 screen should show:
- The green success card
- The "queued and will be processed automatically" blue note
- `BLOCKED_BY_ADJ_ID` warning if applicable

```sql
-- Confirm the submitted adj is Pending (not Processed immediately)
SELECT ADJ_ID, RUN_STATUS, BLOCKED_BY_ADJ_ID
FROM ADJUSTMENT_APP.ADJ_HEADER
ORDER BY ADJ_ID DESC LIMIT 1;
-- Expected: RUN_STATUS = 'Pending'
```

- [ ] **Step 5: Commit**

```bash
git add streamlit_app/pages/1_New_Adjustment.py
git commit -m "feat: update New Adjustment success screen — queued message and blocked indicator"
```

---

## Task 14: Documentation page — update architecture, status flow, and pipeline description

**Files:**
- Modify: `streamlit_app/pages/6_Documentation.py`

Four sections need updating: Architecture tab (single stream → 4 pipelines), Workflow & Status tab (old statuses → new), Processing Engine tab (old task description → new), and the architecture diagram.

- [ ] **Step 1: Update Architecture tab — pipeline diagram text**

Find the STREAM box description (around line 199):
```python
# Replace:
f'<div style="font-weight:700;font-size:0.82rem;color:#1976D2">STREAM</div>'
f'<div style="font-size:0.7rem;color:{P["grey_700"]}">CDC Capture</div>'
# With:
f'<div style="font-weight:700;font-size:0.82rem;color:#1976D2">4 STREAMS</div>'
f'<div style="font-size:0.7rem;color:{P["grey_700"]}">One per scope</div>'
```

Find the TASK box description (around line 211):
```python
# Replace:
f'<div style="font-weight:700;font-size:0.82rem;color:{P["purple"]}">TASK</div>'
f'<div style="font-size:0.7rem;color:{P["grey_700"]}">Every 60 seconds</div>'
# With:
f'<div style="font-weight:700;font-size:0.82rem;color:{P["purple"]}">4 TASKS</div>'
f'<div style="font-size:0.7rem;color:{P["grey_700"]}">Independent per scope</div>'
```

- [ ] **Step 2: Find and update the Workflow & Status tab status table**

Search for the status table in the Workflow tab (look for "Pending Approval" or "Error" status descriptions). Replace any status table or list with:

```python
st.markdown("""
| Status | Meaning |
|--------|---------|
| `Pending` | Submitted, waiting to be claimed by the scope task |
| `Pending Approval` | Awaiting approver action before entering the queue |
| `Approved` | Approved, enters the processing queue as Pending |
| `Running` | Claimed by the scope task — actively being processed |
| `Processed` | Successfully written to the fact adjustment table |
| `Failed` | Processing error — check ERRORMESSAGE on the adjustment |
| `Rejected` | Rejected by an approver |
| `Rejected - SignedOff` | Submitted after COB was signed off |
""")
```

- [ ] **Step 3: Find and update the Processing Engine tab**

Search for the Processing Engine tab content (look for `tab_processing`). Replace or update the task/stream description to reflect 4 independent pipelines:

```python
st.markdown("""
### 4 Independent Scope Pipelines

Each Data Scope (VaR, Stress, FRTB, Sensitivity) has its own independent pipeline:

```
ADJ_HEADER → VW_QUEUE_<SCOPE> → STREAM_QUEUE_<SCOPE> → TASK_PROCESS_<SCOPE>
                                                              ↓
                                                    SP_RUN_PIPELINE
                                              (claim → block → process → unblock)
```

**Why independent?** A 15-minute FRTB adjustment has zero impact on VaR, Stress, or Sensitivity teams. Each scope runs on its own Snowflake task and warehouse.

### Blocking (BLOCKED_BY_ADJ_ID)

Non-overlapping adjustments within the same scope run concurrently. Overlapping adjustments are serialised:

- **At submit time**: `SP_SUBMIT_ADJUSTMENT` checks for Running adjustments that overlap dimensionally. If found, `BLOCKED_BY_ADJ_ID` is set on the new row — it stays invisible in the queue view until the blocker finishes.
- **At task time**: `SP_RUN_PIPELINE` blocks any remaining Pending adjustments that overlap with the newly claimed Running ones.
- **At completion**: `BLOCKED_BY_ADJ_ID` is cleared — the row reappears in the queue view, the stream fires, the task wakes.

### FRTBALL

`FRTBALL` is a fan-out tag: an adjustment submitted as `FRTBALL` is applied to **all** FRTB sub-types (FRTB, FRTBDRC, FRTBRRAO). It has no dedicated settings row — it borrows the sub-type config at runtime.

### Deduplication

`SP_PROCESS_ADJUSTMENT` uses `DENSE_RANK() OVER (PARTITION BY <FACT_TABLE_PK> ORDER BY CREATED_DATE DESC)` to keep only the most recent delta per source fact row. `FACT_TABLE_PK` is configured per scope in `ADJUSTMENTS_SETTINGS`.
""")
```

- [ ] **Step 4: Verify**

Open the Documentation page in Streamlit. Check:
- Architecture tab: "4 STREAMS / One per scope" and "4 TASKS / Independent per scope"
- Workflow tab: status table includes `Running` and `Failed`, no `Error` or `Processing`
- Processing Engine tab: shows the 4-pipeline architecture and blocking description

- [ ] **Step 5: Commit**

```bash
git add streamlit_app/pages/6_Documentation.py
git commit -m "docs: update Documentation page — 4-pipeline architecture, new statuses, blocking description"
```

---

## Self-Review Notes

**Spec coverage:**
- ✅ `BLOCKED_BY_ADJ_ID` column — Task 1
- ✅ `CHANGE_TRACKING` for streams on views — Task 1
- ✅ Status: Pending/Running/Processed/Failed — Tasks 4, 9, 10, 11
- ✅ 4 queue views (simple, stream-safe) — Task 2
- ✅ 4 scope streams — Task 3
- ✅ SP_PROCESS_ADJUSTMENT reads Running — Task 4
- ✅ SP_RUN_PIPELINE (claim/block/process/unblock) — Task 5
- ✅ Blocking at submit time — Task 6
- ✅ 4 scope tasks calling SP_RUN_PIPELINE — Task 7
- ✅ DT_OVERLAP_ALERTS includes Running — Task 8
- ✅ Streamlit styles — Task 9
- ✅ app.py KPI cards — Task 10
- ✅ Processing Queue page — Task 11
- ✅ Documentation — Task 12
- ✅ New Adjustment success screen — Task 13
- ✅ Documentation page (6_Documentation.py) — Task 14
- ✅ FRTBALL fan-out handled in SP_RUN_PIPELINE (skip in direct-call loop)
- ✅ Deduplication via DENSE_RANK already in SP_PROCESS_ADJUSTMENT (unchanged)

**Type consistency:**
- `BLOCKED_BY_ADJ_ID` — used consistently across Tasks 1, 2, 5, 6
- `SP_RUN_PIPELINE(scope VARCHAR, pipeline_types VARCHAR)` — called in Task 7 with correct signature
- `_block_overlapping` / `_unblock_resolved` — defined in Task 5, not referenced externally
- `OVERLAP_DIMS` / `OVERLAP_DIMS_SUBMIT` — same 7 dimensions in both Task 5 and Task 6
- Status values: `'Running'`, `'Processed'`, `'Failed'` — consistent across all tasks
