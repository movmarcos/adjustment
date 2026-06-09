-- =============================================================================
-- 05C_SP_FORCE_PROCESS_ADJUSTMENT.SQL
-- Manual escape hatch for a stuck adjustment.
--
-- The normal path is stream-driven: a scope task polls every minute and calls
-- SP_RUN_PIPELINE, which consumes the queue stream. If a Pending adjustment is
-- left stranded (e.g. its unblock event didn't re-fire the task), this procedure
-- pushes it through immediately, bypassing the stream:
--   1. Clear any BLOCKED_BY_ADJ_ID and promote it to Running.
--   2. Record the manual transition in ADJ_STATUS_HISTORY.
--   3. Call SP_PROCESS_ADJUSTMENT for its (scope, action, COB).
--
-- Only a Pending/Approved, non-deleted adjustment can be forced.
--   CALL ADJUSTMENT_APP.SP_FORCE_PROCESS_ADJUSTMENT('<adj_id>');
-- =============================================================================

USE DATABASE DVLP_RAPTOR_NEWADJ;
USE SCHEMA ADJUSTMENT_APP;

CREATE OR ALTER PROCEDURE ADJUSTMENT_APP.SP_FORCE_PROCESS_ADJUSTMENT(p_adj_id VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
COMMENT = 'Manual escape hatch: force a stuck Pending/Approved adjustment through processing now, bypassing the stream/task. Clears any block, promotes to Running, and calls SP_PROCESS_ADJUSTMENT for its (scope, action, COB).'
EXECUTE AS CALLER
AS
$$
import json


def main(session, p_adj_id):
    adj_id = str(p_adj_id).replace("'", "''")

    rows = session.sql(f"""
        SELECT PROCESS_TYPE, ADJUSTMENT_ACTION, COBID, RUN_STATUS, IS_DELETED
        FROM ADJUSTMENT_APP.ADJ_HEADER
        WHERE ADJ_ID = '{adj_id}'
    """).collect()

    if not rows:
        return json.dumps({"adj_id": p_adj_id, "status": "not_found"})

    r = rows[0]
    if r["IS_DELETED"]:
        return json.dumps({"adj_id": p_adj_id, "status": "deleted"})

    run_status = r["RUN_STATUS"]
    if run_status not in ("Pending", "Approved"):
        return json.dumps({
            "adj_id": p_adj_id, "status": "not_forceable",
            "message": f"Only Pending/Approved can be forced (current: {run_status}).",
        })

    pt  = r["PROCESS_TYPE"]
    act = r["ADJUSTMENT_ACTION"]
    cob = int(r["COBID"])

    # 1. Clear any block and promote to Running so SP_PROCESS_ADJUSTMENT picks it up.
    session.sql(f"""
        UPDATE ADJUSTMENT_APP.ADJ_HEADER
        SET BLOCKED_BY_ADJ_ID = NULL,
            RUN_STATUS        = 'Running',
            START_DATE        = CONVERT_TIMEZONE('Europe/London', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9)
        WHERE ADJ_ID = '{adj_id}'
          AND RUN_STATUS IN ('Pending', 'Approved')
          AND IS_DELETED = FALSE
    """).collect()

    # 2. Audit the manual transition.
    session.sql(f"""
        INSERT INTO ADJUSTMENT_APP.ADJ_STATUS_HISTORY
            (ADJ_ID, OLD_STATUS, NEW_STATUS, CHANGED_BY, CHANGED_AT, COMMENT)
        SELECT '{adj_id}', '{run_status}', 'Running', CURRENT_USER(), CURRENT_TIMESTAMP(),
               'Forced to process via SP_FORCE_PROCESS_ADJUSTMENT'
    """).collect()

    # 3. Process its (scope, action, COB). SP_PROCESS_ADJUSTMENT batches every
    #    Running row for the combo; the forced row is now one of them.
    try:
        proc = session.sql(f"""
            CALL ADJUSTMENT_APP.SP_PROCESS_ADJUSTMENT('{pt}', '{act}', {cob})
        """).collect()
        detail = proc[0][0] if proc else None
    except Exception as e:
        err = str(e)[:990].replace("'", "''")
        session.sql(f"""
            UPDATE ADJUSTMENT_APP.ADJ_HEADER
            SET RUN_STATUS = 'Failed', ERRORMESSAGE = '{err}'
            WHERE ADJ_ID = '{adj_id}' AND RUN_STATUS = 'Running'
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
