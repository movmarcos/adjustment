-- =============================================================================
-- 05B_SP_RUN_PIPELINE.SQL
-- Polling pipeline orchestrator.
--
-- Called every minute by the 4 scope tasks (06_tasks.sql). It POLLS the queue
-- (reads ADJ_HEADER directly) rather than consuming a stream — so every eligible
-- adjustment is found on every run regardless of when it was submitted, and
-- nothing can be stranded by a stream drain (the previous design lost rows that
-- arrived mid-run, which got worse with concurrent users).
--
-- Parameters:
--   scope           e.g. 'VaR', 'Stress', 'FRTB', 'Sensitivity'
--   pipeline_types  JSON array of PROCESS_TYPE strings in this pipeline
--                   e.g. '["FRTB","FRTBDRC","FRTBRRAO","FRTBALL"]'
--
-- FLOW (block first, then claim):
--   1a. Block overlapping Pending rows against any currently-Running adjustment.
--   1b. Block overlapping Pending rows against EACH OTHER (oldest proceeds,
--       newer ones wait). Overlap is purely data scope (COBID + dimensions),
--       NOT ADJUSTMENT_TYPE/ADJUSTMENT_ACTION — a Flatten and a Scale on the same
--       entity/book/COB overlap and must serialise.
--   2. Enumerate the eligible set: Pending/Approved, unblocked, not deleted, in
--      this pipeline. Read straight from ADJ_HEADER into a TEMP table.
--   3. Claim: promote those rows to RUN_STATUS='Running' (+ START_DATE),
--      re-checking eligibility in the WHERE so the claim is atomic per row.
--   4. Process each distinct (PROCESS_TYPE, ADJUSTMENT_ACTION, COBID) combo in
--      parallel via Snowpark async (collect_nowait); each call processes all
--      Running rows for its combo in one batched operation.
--   5. Unblock resolved — clear BLOCKED_BY_ADJ_ID for any Pending row whose
--      blocker just finished; the next 1-minute poll picks it up.
--
-- FRTBALL note: FRTBALL has no settings row. It is applied as a fan-out during
-- FRTB / FRTBDRC / FRTBRRAO processing, so it is skipped as a standalone call
-- target — SP_PROCESS_ADJUSTMENT picks it up when processing any real FRTB type.
-- =============================================================================

USE SCHEMA ADJUSTMENT_APP;

CREATE OR ALTER PROCEDURE ADJUSTMENT_APP.SP_RUN_PIPELINE(
    scope          VARCHAR,
    pipeline_types VARCHAR   -- JSON array string: '["VaR"]' or '["FRTB","FRTBDRC","FRTBRRAO","FRTBALL"]'
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
COMMENT = 'Polling pipeline orchestrator. Blocks overlapping rows, enumerates the eligible Pending/Approved queue from ADJ_HEADER, claims them to Running, processes via SP_PROCESS_ADJUSTMENT, and unblocks resolved rows. Polled every minute by the scope tasks; no streams.'
EXECUTE AS CALLER
AS
$$
import json

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

# Dimensions used for overlap detection (NULL = wildcard = matches any value)
OVERLAP_DIMS = [
    'ENTITY_CODE', 'SOURCE_SYSTEM_CODE', 'DEPARTMENT_CODE',
    'BOOK_CODE', 'CURRENCY_CODE', 'TRADE_TYPOLOGY', 'TRADE_CODE',
    'STRATEGY', 'TRADER_CODE', 'INSTRUMENT_CODE',
    'SIMULATION_NAME', 'SIMULATION_SOURCE',
    'TENOR_CODE', 'UNDERLYING_TENOR_CODE', 'CURVE_CODE',
    'MEASURE_TYPE_CODE', 'PRODUCT_CATEGORY_ATTRIBUTES',
    'BATCH_REGION_AREA', 'MUREX_FAMILY', 'MUREX_GROUP',
    'GUARANTEED_ENTITY',
]

# SELECT fragment for overlap queries — always includes all OVERLAP_DIMS
_OVERLAP_SELECT_COLS = ', '.join(['ADJ_ID', 'COBID', 'PROCESS_TYPE', 'ADJUSTMENT_ACTION'] + OVERLAP_DIMS)

# Direct adjustments (Direct, Upload) don't participate in overlap checking —
# they are explicit value insertions and their only "overlap" is same
# COBID + GLOBAL_REFERENCE, which is handled at submission time.
_OVERLAP_ACTION_FILTER = "AND ADJUSTMENT_ACTION NOT IN ('Direct')"

# FRTBALL is a fan-out tag — applied within real FRTB* SP calls, not on its own.
FRTBALL_SKIP = {'FRTBALL'}

# Scopes a task can drive (matches the per-scope tasks in 06_tasks.sql).
QUEUE_SCOPES = {'VAR', 'STRESS', 'FRTB', 'SENSITIVITY'}

ALLOWED_TYPES = {'VaR', 'Stress', 'Sensitivity',
                 'FRTB', 'FRTBDRC', 'FRTBRRAO', 'FRTBALL'}


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main(session, scope, pipeline_types):
    types = json.loads(pipeline_types)
    for t in types:
        if t not in ALLOWED_TYPES:
            raise ValueError(f"Unknown pipeline_type: {repr(t)}")

    if scope.upper() not in QUEUE_SCOPES:
        raise ValueError(
            f"Unknown scope: {repr(scope)}. Expected one of {sorted(QUEUE_SCOPES)}"
        )

    pipeline_in = ", ".join(f"'{t}'" for t in types)
    real_types  = [t for t in types if t not in FRTBALL_SKIP]
    if not real_types:
        return json.dumps({
            "scope": scope,
            "message": "No real process types after FRTBALL exclusion — check pipeline_types",
        })
    real_in = ", ".join(f"'{t}'" for t in real_types)

    results = []

    # ── 1. OVERLAP BLOCKING ─────────────────────────────────────────────────
    #    Wrapped in try/except: overlap blocking is best-effort. If it fails,
    #    we still proceed to enumerate and process adjustments. The worst case
    #    is two overlapping Scale adjustments run concurrently, but that's better
    #    than the entire pipeline halting.
    try:
        # ── 1a. BLOCK PENDING vs RUNNING ─────────────────────────────────
        all_running = session.sql(f"""
            SELECT {_OVERLAP_SELECT_COLS}
            FROM ADJUSTMENT_APP.ADJ_HEADER
            WHERE PROCESS_TYPE IN ({pipeline_in})
              AND RUN_STATUS = 'Running'
              AND IS_DELETED = FALSE
              {_OVERLAP_ACTION_FILTER}
        """).collect()

        for r in all_running:
            _block_overlapping(session, r, pipeline_in)

        # ── 1b. BLOCK PENDING vs PENDING (overlap serialisation) ─────────
        _block_pending_overlaps(session, pipeline_in)
    except Exception as block_err:
        results.append({"step": "overlap_blocking", "status": "failed",
                        "error": str(block_err)[:500]})

    # ── 2. ENUMERATE ELIGIBLE ROWS (poll the queue — NOT a stream) ────────────
    #    The eligible set is just ADJ_HEADER filtered the same way as the queue
    #    views: in this pipeline, Pending/Approved, unblocked, not deleted. Step 1
    #    already blocked overlapping rows (they fail BLOCKED_BY_ADJ_ID IS NULL),
    #    so they are excluded here. Reading directly from the table (instead of
    #    consuming a stream) means every eligible row is found on every run — no
    #    matter when it was submitted — so nothing can be stranded by a drain.
    session.sql("""
        CREATE OR REPLACE TEMPORARY TABLE TEMP_QUEUE (
            ADJ_ID            VARCHAR(36),
            COBID             NUMBER(38,0),
            PROCESS_TYPE      VARCHAR(30),
            ADJUSTMENT_ACTION VARCHAR(10)
        )
    """).collect()

    session.sql(f"""
        INSERT INTO TEMP_QUEUE (ADJ_ID, COBID, PROCESS_TYPE, ADJUSTMENT_ACTION)
        SELECT ADJ_ID, COBID, PROCESS_TYPE, ADJUSTMENT_ACTION
        FROM ADJUSTMENT_APP.ADJ_HEADER
        WHERE PROCESS_TYPE IN ({pipeline_in})
          AND RUN_STATUS IN ('Pending', 'Approved')
          AND BLOCKED_BY_ADJ_ID IS NULL
          AND IS_DELETED = FALSE
    """).collect()

    eligible_count = session.sql(
        "SELECT COUNT(*) AS C FROM TEMP_QUEUE"
    ).collect()[0]["C"]

    if eligible_count == 0:
        # Nothing eligible — still try to release anything whose blocker finished
        # so it is picked up next run, then exit fast.
        _unblock_resolved(session, pipeline_in)
        return json.dumps({"scope": scope, "message": "No eligible adjustments"})

    # ── 3. CLAIM: PROMOTE ELIGIBLE ROWS TO Running ───────────────────────────
    #    Re-check eligibility in the WHERE so the claim is atomic per row — only
    #    rows still Pending/Approved + unblocked are promoted.
    session.sql("""
        UPDATE ADJUSTMENT_APP.ADJ_HEADER h
        SET RUN_STATUS = 'Running',
            START_DATE = CONVERT_TIMEZONE('Europe/London', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9)
        WHERE h.ADJ_ID IN (SELECT ADJ_ID FROM TEMP_QUEUE)
          AND h.RUN_STATUS IN ('Pending', 'Approved')
          AND h.BLOCKED_BY_ADJ_ID IS NULL
          AND h.IS_DELETED = FALSE
    """).collect()

    # ── 4. PROCESS EACH DISTINCT (pt, action, cob) COMBO — IN PARALLEL ──────
    #    SP_PROCESS_ADJUSTMENT is batch-oriented: one call processes ALL
    #    Running rows matching a given (pt, action, cob) combo in a single
    #    batched SQL operation.
    #
    #    Different combos are INDEPENDENT (they target different row sets in
    #    ADJ_HEADER and write to different target rows in the fact adjustment
    #    tables), so we submit each CALL asynchronously via Snowpark's
    #    `collect_nowait()` and collect the results after all submissions.
    #    Snowflake runs the queries concurrently on the warehouse — the only
    #    ceiling is the warehouse's query concurrency limit (default 8) and
    #    compute capacity.
    #
    #    Parallelism example:
    #      • (VaR, Scale,   20260326)  ─┐
    #      • (VaR, Flatten, 20260326)  ─┼─→ all 3 run concurrently
    #      • (VaR, Scale,   20260325)  ─┘
    to_process = session.sql(f"""
        SELECT DISTINCT PROCESS_TYPE, ADJUSTMENT_ACTION, COBID
        FROM TEMP_QUEUE
        WHERE PROCESS_TYPE IN ({real_in})
        ORDER BY PROCESS_TYPE, ADJUSTMENT_ACTION, COBID
    """).collect()

    # ── 4a. SUBMIT every CALL asynchronously ─────────────────────────────────
    async_jobs = []  # list of (pt, act, cob, AsyncJob)
    for row in to_process:
        pt  = row["PROCESS_TYPE"]
        act = row["ADJUSTMENT_ACTION"]
        cob = row["COBID"]
        job = session.sql(f"""
            CALL ADJUSTMENT_APP.SP_PROCESS_ADJUSTMENT('{pt}', '{act}', {cob})
        """).collect_nowait()
        async_jobs.append((pt, act, cob, job))

    # ── 4b. COLLECT results — blocks until each job finishes ─────────────────
    #    .result() on an AsyncJob waits for that specific query. Because we
    #    submitted all jobs before calling .result() on any of them, they
    #    execute concurrently on the warehouse; we simply harvest the outcomes
    #    in submission order.
    for pt, act, cob, job in async_jobs:
        try:
            job.result()
            results.append({"process_type": pt, "cobid": cob, "status": "ok"})
        except Exception as e:
            err = str(e)[:990].replace("'", "''")
            # Mark the Running adjustments for this specific combo as Failed
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

    # ── 5. UNBLOCK RESOLVED ──────────────────────────────────────────────────
    #    Any Pending row whose blocker just finished → clear BLOCKED_BY_ADJ_ID
    #    (or reassign to another still-Running overlap). The next 1-minute run
    #    re-polls the queue and picks it up. Best-effort: a failure here doesn't
    #    undo the work already done above.
    try:
        _unblock_resolved(session, pipeline_in)
    except Exception as ub_err:
        results.append({"step": "unblock_resolved", "status": "failed",
                        "error": str(ub_err)[:500]})

    return json.dumps({"scope": scope, "processed": results})


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _block_pending_overlaps(session, pipeline_in):
    """
    Among unblocked Pending/Approved rows in the pipeline, the oldest
    adjustment in each overlapping group proceeds; newer overlapping ones
    get blocked.

    Overlap is based purely on data scope (COBID + OVERLAP_DIMS). It does
    NOT consider ADJUSTMENT_TYPE or ADJUSTMENT_ACTION — a Flatten and a
    Scale targeting the same entity/book/COB DO overlap and must serialise.

    Algorithm (oldest-first, in-memory):
      1. Fetch all unblocked Pending/Approved rows, ordered by CREATED_DATE.
      2. Maintain a 'proceeding' list of rows that will NOT be blocked.
      3. For each row: if it overlaps any proceeding row → block it
         (UPDATE BLOCKED_BY_ADJ_ID to the proceeding row's ADJ_ID).
         Otherwise → add it to the proceeding list.
    """
    unblocked = session.sql(f"""
        SELECT {_OVERLAP_SELECT_COLS}, CREATED_DATE
        FROM ADJUSTMENT_APP.ADJ_HEADER
        WHERE PROCESS_TYPE IN ({pipeline_in})
          AND RUN_STATUS IN ('Pending', 'Approved')
          AND BLOCKED_BY_ADJ_ID IS NULL
          AND IS_DELETED = FALSE
          {_OVERLAP_ACTION_FILTER}
        ORDER BY CREATED_DATE ASC, ADJ_ID ASC
    """).collect()

    proceeding = []

    for r in unblocked:
        blocker_id = _find_overlap_in_list(r, proceeding)
        if blocker_id:
            adj_id = str(r["ADJ_ID"]).replace("'", "''")
            safe_blocker = str(blocker_id).replace("'", "''")
            session.sql(f"""
                UPDATE ADJUSTMENT_APP.ADJ_HEADER
                SET BLOCKED_BY_ADJ_ID = '{safe_blocker}'
                WHERE ADJ_ID = '{adj_id}'
            """).collect()
        else:
            proceeding.append(r)


def _find_overlap_in_list(row, proceeding_list):
    """
    Check if `row` overlaps with any row in `proceeding_list`.
    Return the ADJ_ID of the first overlapping row, or None.

    Overlap = same COBID + each OVERLAP_DIM either matches or one side is NULL
    (wildcard). ADJUSTMENT_TYPE / ADJUSTMENT_ACTION are NOT checked.
    """
    for p in proceeding_list:
        if row["COBID"] != p["COBID"]:
            continue
        overlaps = True
        for dim in OVERLAP_DIMS:
            r_val = row[dim]
            p_val = p[dim]
            # NULL (wildcard) overlaps with anything
            if r_val is not None and p_val is not None:
                if str(r_val).upper() != str(p_val).upper():
                    overlaps = False
                    break
        if overlaps:
            return p["ADJ_ID"]
    return None


def _block_overlapping(session, running_row, pipeline_in):
    """
    For the given Running adjustment, find all Pending adjustments in the
    same pipeline + same COBID that overlap with it, and set their
    BLOCKED_BY_ADJ_ID.

    Overlap rule per dimension: NULL (wildcard) overlaps with any value.
    """
    adj_id = str(running_row["ADJ_ID"]).replace("'", "''")
    cobid  = int(running_row["COBID"])

    dim_conditions = []
    for dim in OVERLAP_DIMS:
        val = running_row[dim]
        if val is None:
            dim_conditions.append("TRUE")
        else:
            escaped = str(val).replace("'", "''")
            dim_conditions.append(
                f"(p.{dim} IS NULL OR UPPER(p.{dim}) = UPPER('{escaped}'))"
            )

    where_dims = " AND ".join(dim_conditions)

    session.sql(f"""
        UPDATE ADJUSTMENT_APP.ADJ_HEADER p
        SET BLOCKED_BY_ADJ_ID = '{adj_id}'
        WHERE p.COBID = {cobid}
          AND p.PROCESS_TYPE IN ({pipeline_in})
          AND p.RUN_STATUS IN ('Pending', 'Approved')
          AND p.BLOCKED_BY_ADJ_ID IS NULL
          AND p.IS_DELETED = FALSE
          AND p.ADJ_ID != '{adj_id}'
          AND p.ADJUSTMENT_ACTION NOT IN ('Direct')
          AND {where_dims}
    """).collect()


def _unblock_resolved(session, pipeline_in):
    """
    Clear BLOCKED_BY_ADJ_ID for any Pending/Approved row whose blocker is no
    longer Running (i.e. Processed, Failed, or deleted). If another Running
    adjustment still overlaps, reassign the block; otherwise set NULL.

    Two-pass approach (avoids N+1 per-row queries):
      Pass 1: Bulk-clear all blocked rows whose blocker is no longer Running.
      Pass 2: Re-block any of those newly-released rows that still overlap
              with a currently-Running adjustment (using _block_overlapping).
    """
    # ── Pass 1: bulk-clear blocks where the blocker has finished ──────────
    #    A single UPDATE with a subquery — no per-row queries needed.
    session.sql(f"""
        UPDATE ADJUSTMENT_APP.ADJ_HEADER b
        SET BLOCKED_BY_ADJ_ID = NULL
        WHERE b.BLOCKED_BY_ADJ_ID IS NOT NULL
          AND b.PROCESS_TYPE IN ({pipeline_in})
          AND b.RUN_STATUS IN ('Pending', 'Approved')
          AND b.IS_DELETED = FALSE
          AND NOT EXISTS (
              SELECT 1 FROM ADJUSTMENT_APP.ADJ_HEADER blocker
              WHERE blocker.ADJ_ID = b.BLOCKED_BY_ADJ_ID
                AND blocker.RUN_STATUS = 'Running'
          )
    """).collect()

    # ── Pass 2: re-block if another Running adj still overlaps ────────────
    #    Check currently-Running adjustments and block any newly-released
    #    Pending rows that overlap with them.
    still_running = session.sql(f"""
        SELECT {_OVERLAP_SELECT_COLS}
        FROM ADJUSTMENT_APP.ADJ_HEADER
        WHERE PROCESS_TYPE IN ({pipeline_in})
          AND RUN_STATUS = 'Running'
          AND IS_DELETED = FALSE
          {_OVERLAP_ACTION_FILTER}
    """).collect()

    for r in still_running:
        _block_overlapping(session, r, pipeline_in)
$$;

-- ═══════════════════════════════════════════════════════════════════════════
-- VERIFY
-- ═══════════════════════════════════════════════════════════════════════════
DESCRIBE PROCEDURE ADJUSTMENT_APP.SP_RUN_PIPELINE(VARCHAR, VARCHAR);
