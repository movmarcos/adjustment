-- =============================================================================
-- 05B_SP_RUN_PIPELINE.SQL
-- Stream-driven pipeline orchestrator.
--
-- Called by the 4 scope tasks via WHEN SYSTEM$STREAM_HAS_DATA(...).
-- Parameters:
--   scope           e.g. 'VaR', 'Stress', 'FRTB', 'Sensitivity'
--                   → maps to STREAM_QUEUE_<SCOPE> (stream on VW_QUEUE_<SCOPE>)
--   pipeline_types  JSON array of PROCESS_TYPE strings in this pipeline
--                   e.g. '["FRTB","FRTBDRC","FRTBRRAO","FRTBALL"]'
--
-- STREAMS ARE ON QUEUE VIEWS (NOT ON ADJ_HEADER):
--   Each stream sits on a view that already filters by PROCESS_TYPE,
--   RUN_STATUS IN ('Pending','Approved'), BLOCKED_BY_ADJ_ID IS NULL,
--   and IS_DELETED = FALSE. The stream tracks rows ENTERING or LEAVING
--   that view's result set — so SYSTEM$STREAM_HAS_DATA only fires for
--   changes relevant to THIS scope. No cross-scope noise.
--
-- FLOW (stream-driven — block first, not claim first):
--   1. Block overlapping Pending rows against any currently-Running adjustment.
--      Because Snowflake auto-commits each statement, blocked rows immediately
--      LEAVE the queue view → the stream records them as DELETE. By the time
--      we consume the stream in step 2, blocked rows are already excluded.
--   2. Consume the stream into a TEMP table. We SELECT only
--      METADATA$ACTION = 'INSERT' rows (= rows that entered the view and
--      stayed). No JOIN to ADJ_HEADER needed — the view's WHERE clause
--      guarantees eligibility.
--   3. Promote only those rows to RUN_STATUS='Running' + set START_DATE.
--   4. Submit SP_PROCESS_ADJUSTMENT calls in PARALLEL via Snowpark async
--      (collect_nowait) — one call per distinct (PROCESS_TYPE,
--      ADJUSTMENT_ACTION, COBID) combo. Each call processes all Running rows
--      for its combo in a single batched SQL operation.
--   5. Drain the stream with a no-op consume (`WHERE 1=0`) so that events
--      from steps 1/3/4 don't cause the next task run to re-fire on state
--      we've already handled.
--   6. Unblock resolved — clear BLOCKED_BY_ADJ_ID for any Pending row whose
--      blocker is now Processed/Failed. The row re-enters the queue view →
--      the stream records an INSERT → the next task run picks it up.
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
COMMENT = 'Stream-driven pipeline orchestrator. Blocks overlapping rows first, consumes STREAM_QUEUE_<scope> into a temp table, promotes to Running, processes via SP_PROCESS_ADJUSTMENT, drains the stream, and unblocks resolved rows.'
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
    'BOOK_CODE', 'CURRENCY_CODE', 'TRADE_TYPOLOGY', 'STRATEGY',
]

# FRTBALL is a fan-out tag — applied within real FRTB* SP calls, not on its own.
FRTBALL_SKIP = {'FRTBALL'}

# scope → stream name (matches streams defined in 02_streams.sql)
STREAM_MAP = {
    'VAR':         'ADJUSTMENT_APP.STREAM_QUEUE_VAR',
    'STRESS':      'ADJUSTMENT_APP.STREAM_QUEUE_STRESS',
    'FRTB':        'ADJUSTMENT_APP.STREAM_QUEUE_FRTB',
    'SENSITIVITY': 'ADJUSTMENT_APP.STREAM_QUEUE_SENSITIVITY',
}

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

    stream_name = STREAM_MAP.get(scope.upper())
    if stream_name is None:
        raise ValueError(
            f"Unknown scope: {repr(scope)}. Expected one of {list(STREAM_MAP.keys())}"
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

    # ── 1. BLOCK OVERLAPPING PENDING ROWS ────────────────────────────────────
    #    Runs BEFORE we consume the stream so blocked rows are already excluded
    #    when we read the stream in step 2.
    #
    #    This handles two cases:
    #      • Stuck Running rows from a crashed previous run
    #      • Long-running adjustments spanning multiple task fires
    all_running = session.sql(f"""
        SELECT ADJ_ID, COBID, PROCESS_TYPE,
               ENTITY_CODE, SOURCE_SYSTEM_CODE, DEPARTMENT_CODE,
               BOOK_CODE, CURRENCY_CODE, TRADE_TYPOLOGY, STRATEGY
        FROM ADJUSTMENT_APP.ADJ_HEADER
        WHERE PROCESS_TYPE IN ({pipeline_in})
          AND RUN_STATUS = 'Running'
          AND IS_DELETED = FALSE
    """).collect()

    for r in all_running:
        _block_overlapping(session, r, pipeline_in)

    # ── 2. CONSUME THE STREAM → TEMP TABLE ────────────────────────────────────
    #    The stream is on a queue VIEW (e.g. VW_QUEUE_VAR) that already filters
    #    by PROCESS_TYPE, RUN_STATUS IN ('Pending','Approved'), BLOCKED_BY_ADJ_ID
    #    IS NULL, and IS_DELETED = FALSE. The stream only tracks rows that ENTER
    #    or LEAVE that view's result set:
    #
    #      • METADATA$ACTION = 'INSERT' → row entered the view
    #        (new submission, or unblocked adjustment re-entering the view)
    #      • METADATA$ACTION = 'DELETE' → row left the view
    #        (promoted to Running, blocked, or soft-deleted)
    #
    #    Step 1's blocking UPDATEs already committed (auto-commit). Blocked rows
    #    LEFT the view → the stream shows them as DELETE, not INSERT. So we only
    #    pick up rows that are still eligible after the block check — no JOIN
    #    back to ADJ_HEADER needed.
    session.sql("""
        CREATE OR REPLACE TEMPORARY TABLE TEMP_STREAM_QUEUE (
            ADJ_ID            VARCHAR(36),
            COBID             NUMBER(38,0),
            PROCESS_TYPE      VARCHAR(30),
            ADJUSTMENT_ACTION VARCHAR(10)
        )
    """).collect()

    session.sql(f"""
        INSERT INTO TEMP_STREAM_QUEUE (ADJ_ID, COBID, PROCESS_TYPE, ADJUSTMENT_ACTION)
        SELECT DISTINCT ADJ_ID, COBID, PROCESS_TYPE, ADJUSTMENT_ACTION
        FROM {stream_name}
        WHERE METADATA$ACTION = 'INSERT'
    """).collect()

    eligible_count = session.sql(
        "SELECT COUNT(*) AS C FROM TEMP_STREAM_QUEUE"
    ).collect()[0]["C"]

    if eligible_count == 0:
        # Nothing to promote — but the block step in (1) and the stream consume
        # in (2) may have left UPDATE events in the stream. Drain and unblock,
        # then exit cleanly.
        _drain_stream(session, stream_name)
        _unblock_resolved(session, pipeline_in, [])
        return json.dumps({
            "scope": scope,
            "message": "No eligible adjustments in stream (all blocked or empty)",
        })

    # ── 3. PROMOTE TEMP-TABLE ROWS TO Running ────────────────────────────────
    session.sql("""
        UPDATE ADJUSTMENT_APP.ADJ_HEADER h
        SET RUN_STATUS = 'Running',
            START_DATE = CONVERT_TIMEZONE('Europe/London', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9)
        WHERE h.ADJ_ID IN (SELECT ADJ_ID FROM TEMP_STREAM_QUEUE)
    """).collect()

    # Capture the adj_ids we just claimed — used by _unblock_resolved below.
    claimed_ids = [
        r["ADJ_ID"]
        for r in session.sql("SELECT ADJ_ID FROM TEMP_STREAM_QUEUE").collect()
    ]

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
        FROM TEMP_STREAM_QUEUE
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

    # ── 5. DRAIN THE STREAM ──────────────────────────────────────────────────
    #    Between step 2 and now, ADJ_HEADER has been UPDATEd repeatedly:
    #      • Pending → Running (step 3)
    #      • Running → Processed/Failed (step 4)
    #    Every one of those UPDATEs landed in the stream. We drain with a
    #    no-op DML (`WHERE 1=0`) so the next task fire only sees genuinely
    #    new work (either new submissions or unblock events from step 6).
    _drain_stream(session, stream_name)

    # ── 6. UNBLOCK RESOLVED ──────────────────────────────────────────────────
    #    Any Pending row with BLOCKED_BY_ADJ_ID pointing to something we just
    #    finished → clear it (or reassign to another still-Running overlap).
    #    The UPDATE produces a stream event → next task fire re-evaluates it.
    _unblock_resolved(session, pipeline_in, claimed_ids)

    return json.dumps({"scope": scope, "processed": results})


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _drain_stream(session, stream_name):
    """
    Consume the stream with a `WHERE 1=0` filter. The INSERT still references
    the stream in its FROM clause, so Snowflake advances the offset even though
    zero rows are actually written. This is the standard "drain" pattern.

    This clears any events produced by our own UPDATEs (block, promote to
    Running, mark Processed/Failed) so the next task fire only sees genuinely
    new work (new submissions or unblock events from step 6).

    TEMP_STREAM_QUEUE must already exist (created in main() step 2).
    """
    session.sql(f"""
        INSERT INTO TEMP_STREAM_QUEUE (ADJ_ID, COBID, PROCESS_TYPE, ADJUSTMENT_ACTION)
        SELECT ADJ_ID, COBID, PROCESS_TYPE, ADJUSTMENT_ACTION
        FROM {stream_name}
        WHERE 1 = 0
    """).collect()


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
        SET BLOCKED_BY_ADJ_ID = '{adj_id}'
        WHERE p.COBID = {cobid}
          AND p.PROCESS_TYPE IN ({pipeline_in})
          AND p.RUN_STATUS IN ('Pending', 'Approved')
          AND p.BLOCKED_BY_ADJ_ID IS NULL
          AND p.IS_DELETED = FALSE
          AND p.ADJ_ID != '{adj_id}'
          AND {where_dims}
    """).collect()


def _unblock_resolved(session, pipeline_in, claimed_ids):
    """
    Clear BLOCKED_BY_ADJ_ID for any Pending/Approved row whose blocker is now
    Processed or Failed. If a still-Running adjustment in the pipeline still
    overlaps, reassign the block to that one instead of clearing it.

    claimed_ids: adj_ids that this run just claimed and processed. If empty,
    the function still scans for any stale blocks whose blocker has finished
    (covers leftover state from a prior crashed run).
    """
    # Find all currently-blocked Pending rows in this pipeline.
    blocked = session.sql(f"""
        SELECT b.ADJ_ID, b.COBID, b.BLOCKED_BY_ADJ_ID,
               b.ENTITY_CODE, b.SOURCE_SYSTEM_CODE, b.DEPARTMENT_CODE,
               b.BOOK_CODE, b.CURRENCY_CODE, b.TRADE_TYPOLOGY, b.STRATEGY
        FROM ADJUSTMENT_APP.ADJ_HEADER b
        WHERE b.BLOCKED_BY_ADJ_ID IS NOT NULL
          AND b.PROCESS_TYPE IN ({pipeline_in})
          AND b.RUN_STATUS IN ('Pending', 'Approved')
          AND b.IS_DELETED = FALSE
    """).collect()

    if not blocked:
        return

    for b in blocked:
        b_adj_id   = b["ADJ_ID"]
        blocker_id = b["BLOCKED_BY_ADJ_ID"]
        cobid      = b["COBID"]

        # Is the blocker still Running? If yes, leave this row blocked.
        blocker_status = session.sql(f"""
            SELECT RUN_STATUS FROM ADJUSTMENT_APP.ADJ_HEADER
            WHERE ADJ_ID = '{blocker_id}'
        """).collect()
        if blocker_status and blocker_status[0]["RUN_STATUS"] == 'Running':
            continue

        # Blocker has finished — check if any OTHER still-Running adj overlaps.
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

        other_running = session.sql(f"""
            SELECT ADJ_ID FROM ADJUSTMENT_APP.ADJ_HEADER r
            WHERE r.COBID = {cobid}
              AND r.PROCESS_TYPE IN ({pipeline_in})
              AND r.RUN_STATUS = 'Running'
              AND r.ADJ_ID != '{b_adj_id}'
              AND r.IS_DELETED = FALSE
              AND {where_dims}
            LIMIT 1
        """).collect()

        if other_running:
            new_blocker = other_running[0]["ADJ_ID"]
            session.sql(f"""
                UPDATE ADJUSTMENT_APP.ADJ_HEADER
                SET BLOCKED_BY_ADJ_ID = '{new_blocker}'
                WHERE ADJ_ID = '{b_adj_id}'
            """).collect()
        else:
            # No remaining blockers → release this row.
            # This UPDATE lands in the stream and wakes the next task run.
            session.sql(f"""
                UPDATE ADJUSTMENT_APP.ADJ_HEADER
                SET BLOCKED_BY_ADJ_ID = NULL
                WHERE ADJ_ID = '{b_adj_id}'
            """).collect()
$$;

-- ═══════════════════════════════════════════════════════════════════════════
-- VERIFY
-- ═══════════════════════════════════════════════════════════════════════════
DESCRIBE PROCEDURE ADJUSTMENT_APP.SP_RUN_PIPELINE(VARCHAR, VARCHAR);
