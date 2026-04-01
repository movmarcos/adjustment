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
    ALLOWED_TYPES = {'VaR', 'Stress', 'Sensitivity', 'FRTB', 'FRTBDRC', 'FRTBRRAO', 'FRTBALL'}
    for t in types:
        if t not in ALLOWED_TYPES:
            raise ValueError(f"Unknown pipeline_type: {repr(t)}")
    pipeline_in = ", ".join(f"'{t}'" for t in types)
    real_types = [t for t in types if t not in FRTBALL_SKIP]
    if not real_types:
        return json.dumps({"scope": scope, "message": "No real process types after FRTBALL exclusion — check pipeline_types"})
    real_in = ", ".join(f"'{t}'" for t in real_types)

    results = []

    # ── 1. Atomically claim all eligible Pending/Approved → Running ─────
    # 'Approved' is treated identically to 'Pending' — it just went through
    # an approval step before reaching the queue.
    session.sql(f"""
        UPDATE ADJUSTMENT_APP.ADJ_HEADER
        SET RUN_STATUS = 'Running',
            PROCESS_DATE = CONVERT_TIMEZONE('Europe/London', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9)
        WHERE PROCESS_TYPE IN ({pipeline_in})
          AND RUN_STATUS IN ('Pending', 'Approved')
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
        # Note: if a previous run left adjustments in a Blocked state and their blocker
        # has since finished (from a separate run), those rows won't be unblocked by
        # this invocation. A periodic cleanup task or manual intervention is needed
        # to handle permanently-stuck blocked rows in that edge case.
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
            err = str(e)[:990].replace("'", "''")
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
        SET BLOCKED_BY_ADJ_ID = '{adj_id}'
        WHERE p.COBID = {cobid}
          AND p.PROCESS_TYPE IN ({pipeline_in})
          AND p.RUN_STATUS IN ('Pending', 'Approved')
          AND p.BLOCKED_BY_ADJ_ID IS NULL
          AND p.IS_DELETED = FALSE
          AND p.ADJ_ID != '{adj_id}'
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
    candidate_ids = [r["ADJ_ID"] for r in previously_running]
    if not candidate_ids:
        return

    candidate_in = ", ".join(f"'{c}'" for c in candidate_ids)

    # Re-query actual statuses — some may still be Running if SP_PROCESS_ADJUSTMENT
    # raised an exception before updating status. Only treat truly finished rows as released.
    actually_finished = session.sql(f"""
        SELECT ADJ_ID FROM ADJUSTMENT_APP.ADJ_HEADER
        WHERE ADJ_ID IN ({candidate_in})
          AND RUN_STATUS IN ('Processed', 'Failed')
    """).collect()

    finished_ids = [r["ADJ_ID"] for r in actually_finished]
    if not finished_ids:
        return

    finished_in = ", ".join(f"'{f}'" for f in finished_ids)

    # Find all Pending adjustments blocked by a now-finished adj
    blocked = session.sql(f"""
        SELECT b.ADJ_ID, b.COBID, b.BLOCKED_BY_ADJ_ID,
               b.ENTITY_CODE, b.SOURCE_SYSTEM_CODE, b.DEPARTMENT_CODE,
               b.BOOK_CODE, b.CURRENCY_CODE, b.TRADE_TYPOLOGY, b.STRATEGY
        FROM ADJUSTMENT_APP.ADJ_HEADER b
        WHERE b.BLOCKED_BY_ADJ_ID IN ({finished_in})
          AND b.RUN_STATUS IN ('Pending', 'Approved')
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
                SET BLOCKED_BY_ADJ_ID = '{new_blocker}'
                WHERE ADJ_ID = '{b_adj_id}'
            """).collect()
        else:
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
