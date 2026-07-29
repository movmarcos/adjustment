-- =============================================================================
-- 05C_SP_FORCE_PROCESS_ADJUSTMENT.SQL
-- Manual escape hatch for a stuck adjustment.
--
-- The normal path is the polling pipeline: a scope task runs every minute and
-- calls SP_RUN_PIPELINE, which claims eligible rows and processes them. If an
-- adjustment is stranded anyway, this procedure pushes it through immediately:
--   1. Claim it with this run's own CLAIM_TOKEN and promote it to Running.
--   2. Record the manual transition in ADJ_STATUS_HISTORY.
--   3. Call SP_PROCESS_ADJUSTMENT for its (scope, action, COB) with the token,
--      so only this adjustment's claim is processed.
--
-- Forceable states:
--   • Pending / Approved — normal stuck-queue rescue. Refused while an
--     overlapping blocker is actively Running (the block exists to serialise
--     conflicting writes); the pipeline's reaper clears dead blockers.
--   • Running, but stale (START_DATE older than STALE_RUNNING_MINUTES) — the
--     run that claimed it died; this re-claims and re-processes it.
--
--   CALL ADJUSTMENT_APP.SP_FORCE_PROCESS_ADJUSTMENT('<adj_id>');
-- =============================================================================

USE SCHEMA ADJUSTMENT_APP;

CREATE OR ALTER PROCEDURE ADJUSTMENT_APP.SP_FORCE_PROCESS_ADJUSTMENT(p_adj_id VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
COMMENT = 'Manual escape hatch: force a stuck Pending/Approved (or stale Running) adjustment through processing now. Claims it with its own token, promotes to Running, and calls SP_PROCESS_ADJUSTMENT for its (scope, action, COB).'
EXECUTE AS CALLER
AS
$$
import json
import uuid

# Keep in sync with SP_RUN_PIPELINE's reaper threshold.
STALE_RUNNING_MINUTES = 240


def main(session, p_adj_id):
    adj_id = str(p_adj_id).replace("'", "''")

    rows = session.sql(f"""
        SELECT PROCESS_TYPE, ADJUSTMENT_ACTION, COBID, RUN_STATUS, IS_DELETED,
               BLOCKED_BY_ADJ_ID,
               (RUN_STATUS = 'Running' AND START_DATE < DATEADD(minute,
                    -{STALE_RUNNING_MINUTES},
                    CONVERT_TIMEZONE('Europe/London', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9))
               ) AS IS_STALE_RUNNING
        FROM ADJUSTMENT_APP.ADJ_HEADER
        WHERE ADJ_ID = '{adj_id}'
    """).collect()

    if not rows:
        return json.dumps({"adj_id": p_adj_id, "status": "not_found"})

    r = rows[0]
    if r["IS_DELETED"]:
        return json.dumps({"adj_id": p_adj_id, "status": "deleted"})

    run_status    = r["RUN_STATUS"]
    stale_running = bool(r["IS_STALE_RUNNING"])
    if run_status not in ("Pending", "Approved") and not stale_running:
        return json.dumps({
            "adj_id": p_adj_id, "status": "not_forceable",
            "message": (f"Only Pending/Approved (or Running stuck for over "
                        f"{STALE_RUNNING_MINUTES} minutes) can be forced "
                        f"(current: {run_status})."),
        })

    # A block exists to serialise overlapping writes — refuse to bypass it
    # while the blocker is actively Running. (A dead blocker is reaped by the
    # pipeline, which also releases this row's block.)
    blocker_id = r["BLOCKED_BY_ADJ_ID"]
    if blocker_id:
        esc_blocker = str(blocker_id).replace("'", "''")
        blocker_live = session.sql(f"""
            SELECT 1 FROM ADJUSTMENT_APP.ADJ_HEADER
            WHERE ADJ_ID = '{esc_blocker}'
              AND RUN_STATUS = 'Running'
              AND IS_DELETED = FALSE
              AND START_DATE >= DATEADD(minute, -{STALE_RUNNING_MINUTES},
                    CONVERT_TIMEZONE('Europe/London', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9))
        """).collect()
        if blocker_live:
            return json.dumps({
                "adj_id": p_adj_id, "status": "blocked",
                "message": (f"Blocked by ADJ {blocker_id}, which is currently "
                            f"processing. Wait for it to finish — forcing now "
                            f"would run two overlapping adjustments at once."),
            })

    pt  = r["PROCESS_TYPE"]
    act = r["ADJUSTMENT_ACTION"]
    cob = int(r["COBID"])
    claim_token = str(uuid.uuid4())

    # 1. Claim with this run's token and promote to Running. The WHERE re-checks
    #    the state read above so the claim is atomic — if the pipeline (or
    #    another force call) got there first, zero rows update and we stop.
    session.sql(f"""
        UPDATE ADJUSTMENT_APP.ADJ_HEADER
        SET BLOCKED_BY_ADJ_ID = NULL,
            RUN_STATUS        = 'Running',
            CLAIM_TOKEN       = '{claim_token}',
            START_DATE        = CONVERT_TIMEZONE('Europe/London', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9)
        WHERE ADJ_ID = '{adj_id}'
          AND IS_DELETED = FALSE
          AND (RUN_STATUS IN ('Pending', 'Approved')
               OR (RUN_STATUS = 'Running' AND START_DATE < DATEADD(minute,
                       -{STALE_RUNNING_MINUTES},
                       CONVERT_TIMEZONE('Europe/London', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9))))
    """).collect()

    claimed = session.sql(f"""
        SELECT 1 FROM ADJUSTMENT_APP.ADJ_HEADER
        WHERE ADJ_ID = '{adj_id}' AND CLAIM_TOKEN = '{claim_token}'
    """).collect()
    if not claimed:
        return json.dumps({
            "adj_id": p_adj_id, "status": "not_forceable",
            "message": "Another run claimed this adjustment first — nothing to do.",
        })

    # 2. Audit the manual transition.
    session.sql(f"""
        INSERT INTO ADJUSTMENT_APP.ADJ_STATUS_HISTORY
            (ADJ_ID, OLD_STATUS, NEW_STATUS, CHANGED_BY, CHANGED_AT, COMMENT)
        SELECT '{adj_id}', '{run_status}', 'Running', CURRENT_USER(), CURRENT_TIMESTAMP(),
               'Forced to process via SP_FORCE_PROCESS_ADJUSTMENT'
    """).collect()

    # 3. Process its (scope, action, COB) under our token — SP_PROCESS only
    #    touches rows claimed by this call.
    try:
        proc = session.sql(f"""
            CALL ADJUSTMENT_APP.SP_PROCESS_ADJUSTMENT('{pt}', '{act}', {cob}, '{claim_token}')
        """).collect()
        detail = proc[0][0] if proc else None
    except Exception as e:
        err = str(e)[:990].replace("'", "''")
        session.sql(f"""
            UPDATE ADJUSTMENT_APP.ADJ_HEADER
            SET RUN_STATUS = 'Failed', ERRORMESSAGE = '{err}'
            WHERE ADJ_ID = '{adj_id}'
              AND CLAIM_TOKEN = '{claim_token}'
              AND RUN_STATUS = 'Running'
        """).collect()
        return json.dumps({"adj_id": p_adj_id, "status": "failed", "error": err})

    return json.dumps({
        "adj_id": p_adj_id, "process_type": pt, "adjustment_action": act,
        "cobid": cob, "status": "forced", "detail": detail,
    })
$$;

-- ═══════════════════════════════════════════════════════════════════════════
-- VERIFY
-- ═══════════════════════════════════════════════════════════════════════════
DESCRIBE PROCEDURE ADJUSTMENT_APP.SP_FORCE_PROCESS_ADJUSTMENT(VARCHAR);
