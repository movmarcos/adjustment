-- =============================================================================
-- 12_SP_WORKFLOW.SQL
-- Server-side enforcement of the 4-eyes controls.
--
-- Until now approve/reject were raw UPDATEs issued by the Streamlit pages —
-- the maker-checker rule lived only in the UI, so any session with UPDATE
-- rights on ADJ_HEADER could bypass it. These procedures make the control
-- real at the database level:
--
--   • The acting identity is CURRENT_USER() — resolved by Snowflake inside
--     the procedure (EXECUTE AS CALLER; in Streamlit-in-Snowflake this is the
--     viewer, provided READ SESSION is granted). It is never taken from an
--     argument the client could fake.
--   • The caller must be an ACTIVE approver for the scope (ADJ_APPROVERS,
--     NULL scope = all scopes).
--   • Self-approval is refused (submitter/requester ≠ approver).
--   • Transitions are guarded (status re-checked in the UPDATE) and history
--     is written only when the row actually moved.
--
-- The Approval Queue page calls these instead of updating tables directly.
-- =============================================================================

USE SCHEMA ADJUSTMENT_APP;

-- ─────────────────────────────────────────────────────────────────────────────
-- SP_DECIDE_ADJUSTMENT — approve or reject a Pending Approval adjustment
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR ALTER PROCEDURE ADJUSTMENT_APP.SP_DECIDE_ADJUSTMENT(
    p_adj_id   VARCHAR,
    p_decision VARCHAR,   -- 'Approved' | 'Rejected'
    p_comment  VARCHAR
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
COMMENT = '4-eyes decision on an adjustment, enforced server-side: caller identity from CURRENT_USER(), active approver + scope check, self-approval refused, guarded transition + audit.'
EXECUTE AS CALLER
AS
$$
import json


def _esc(v):
    return str(v).replace("'", "''") if v is not None else ""


def _caller(session):
    row = session.sql("SELECT CURRENT_USER() AS U").collect()
    u = str(row[0]["U"]) if row and row[0]["U"] else ""
    return u.strip()


def _is_approver(session, username, process_type):
    rows = session.sql(f"""
        SELECT 1 FROM ADJUSTMENT_APP.ADJ_APPROVERS
        WHERE UPPER(USERNAME) = UPPER('{_esc(username)}')
          AND IS_ACTIVE = TRUE
          AND (PROCESS_TYPE IS NULL
               OR UPPER(PROCESS_TYPE) = UPPER('{_esc(process_type)}'))
        LIMIT 1
    """).collect()
    return bool(rows)


def main(session, p_adj_id, p_decision, p_comment):
    decision = str(p_decision or "").strip().capitalize()
    if decision not in ("Approved", "Rejected"):
        return json.dumps({"status": "error",
                           "message": f"Invalid decision '{p_decision}' — "
                                      f"expected Approved or Rejected."})

    caller = _caller(session)
    if not caller or caller.lower() == "unknown":
        return json.dumps({"status": "no_identity",
                           "message": "Caller identity could not be resolved "
                                      "(READ SESSION grant missing?) — "
                                      "approvals are blocked."})

    adj_id = _esc(p_adj_id)
    rows = session.sql(f"""
        SELECT PROCESS_TYPE, USERNAME, RUN_STATUS, IS_DELETED
        FROM ADJUSTMENT_APP.ADJ_HEADER
        WHERE ADJ_ID = '{adj_id}'
    """).collect()
    if not rows:
        return json.dumps({"status": "not_found",
                           "message": "Adjustment not found."})
    r = rows[0]
    if r["IS_DELETED"]:
        return json.dumps({"status": "not_pending",
                           "message": "Adjustment has been deleted."})
    if str(r["RUN_STATUS"]) != "Pending Approval":
        return json.dumps({"status": "not_pending",
                           "message": f"No longer awaiting approval "
                                      f"(current status: {r['RUN_STATUS']})."})

    submitter = str(r["USERNAME"] or "")
    scope     = str(r["PROCESS_TYPE"] or "")
    if submitter.strip().upper() == caller.upper():
        return json.dumps({"status": "self_approval",
                           "message": "You cannot decide your own adjustment."})
    if not _is_approver(session, caller, scope):
        return json.dumps({"status": "not_authorized",
                           "message": f"{caller} is not an active approver "
                                      f"for scope {scope}."})

    upd = session.sql(f"""
        UPDATE ADJUSTMENT_APP.ADJ_HEADER
        SET RUN_STATUS = '{decision}'
        WHERE ADJ_ID = '{adj_id}'
          AND RUN_STATUS = 'Pending Approval'
          AND IS_DELETED = FALSE
    """).collect()
    try:
        n = int(upd[0][0]) if upd else 0
    except (TypeError, ValueError, IndexError):
        n = 0
    if n == 0:
        return json.dumps({"status": "not_pending",
                           "message": "The adjustment's status changed before "
                                      "the decision could be applied."})

    comment = _esc(p_comment) if p_comment else f"{decision} by {_esc(caller)}"
    session.sql(f"""
        INSERT INTO ADJUSTMENT_APP.ADJ_STATUS_HISTORY
            (ADJ_ID, OLD_STATUS, NEW_STATUS, CHANGED_BY, COMMENT)
        VALUES ('{adj_id}', 'Pending Approval', '{decision}',
                '{_esc(caller)}', '{comment[:990]}')
    """).collect()

    return json.dumps({"status": "ok", "decision": decision, "by": caller})
$$;


-- ─────────────────────────────────────────────────────────────────────────────
-- SP_DECIDE_REOPEN — approve or reject a COB sign-off re-open request
-- ─────────────────────────────────────────────────────────────────────────────

-- Entity granularity changed the signature; drop the old 4-arg overload so
-- stale callers fail loudly instead of deciding the wrong row.
DROP PROCEDURE IF EXISTS ADJUSTMENT_APP.SP_DECIDE_REOPEN(INT, VARCHAR, VARCHAR, VARCHAR);

CREATE OR ALTER PROCEDURE ADJUSTMENT_APP.SP_DECIDE_REOPEN(
    p_cobid        INT,
    p_process_type VARCHAR,
    p_entity_code  VARCHAR,   -- '*' = whole-scope row
    p_decision     VARCHAR,   -- 'Approved' | 'Rejected'
    p_comment      VARCHAR
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
COMMENT = '4-eyes decision on a COB re-open request, enforced server-side: caller from CURRENT_USER(), active approver + scope check, requester cannot decide, guarded transition + sign-off history.'
EXECUTE AS CALLER
AS
$$
import json


def _esc(v):
    return str(v).replace("'", "''") if v is not None else ""


def main(session, p_cobid, p_process_type, p_entity_code, p_decision, p_comment):
    decision = str(p_decision or "").strip().capitalize()
    if decision not in ("Approved", "Rejected"):
        return json.dumps({"status": "error",
                           "message": f"Invalid decision '{p_decision}'."})

    row = session.sql("SELECT CURRENT_USER() AS U").collect()
    caller = str(row[0]["U"]).strip() if row and row[0]["U"] else ""
    if not caller or caller.lower() == "unknown":
        return json.dumps({"status": "no_identity",
                           "message": "Caller identity could not be resolved — "
                                      "decisions are blocked."})

    cobid  = int(p_cobid)
    scope  = _esc(p_process_type)
    entity = _esc(p_entity_code) if p_entity_code and str(p_entity_code).strip() else "*"

    rows = session.sql(f"""
        SELECT SIGN_OFF_STATUS, REOPEN_REQUESTED_BY
        FROM ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS
        WHERE COBID = {cobid} AND UPPER(PROCESS_TYPE) = UPPER('{scope}')
          AND UPPER(ENTITY_CODE) = UPPER('{entity}')
    """).collect()
    if not rows or str(rows[0]["SIGN_OFF_STATUS"]).upper() != "REOPEN_REQUESTED":
        return json.dumps({"status": "not_pending",
                           "message": "This COB/scope has no pending re-open "
                                      "request (it may already be decided)."})
    requester = str(rows[0]["REOPEN_REQUESTED_BY"] or "")
    if requester.strip().upper() == caller.upper():
        return json.dumps({"status": "self_approval",
                           "message": "You cannot decide your own re-open request."})

    approver = session.sql(f"""
        SELECT 1 FROM ADJUSTMENT_APP.ADJ_APPROVERS
        WHERE UPPER(USERNAME) = UPPER('{_esc(caller)}')
          AND IS_ACTIVE = TRUE
          AND (PROCESS_TYPE IS NULL
               OR UPPER(PROCESS_TYPE) = UPPER('{scope}'))
        LIMIT 1
    """).collect()
    if not approver:
        return json.dumps({"status": "not_authorized",
                           "message": f"{caller} is not an active approver "
                                      f"for scope {p_process_type}."})

    if decision == "Approved":
        set_clause = (f"SIGN_OFF_STATUS = 'REOPENED', "
                      f"REOPEN_APPROVED_BY = '{_esc(caller)}', "
                      f"REOPEN_APPROVED_AT = CURRENT_TIMESTAMP()")
        new_status = "REOPENED"
    else:
        set_clause = "SIGN_OFF_STATUS = 'SIGNED_OFF'"
        new_status = "SIGNED_OFF"

    upd = session.sql(f"""
        UPDATE ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS
        SET {set_clause}, UPDATED_DATE = CURRENT_TIMESTAMP()
        WHERE COBID = {cobid}
          AND UPPER(PROCESS_TYPE) = UPPER('{scope}')
          AND UPPER(ENTITY_CODE) = UPPER('{entity}')
          AND UPPER(SIGN_OFF_STATUS) = 'REOPEN_REQUESTED'
    """).collect()
    try:
        n = int(upd[0][0]) if upd else 0
    except (TypeError, ValueError, IndexError):
        n = 0
    if n == 0:
        return json.dumps({"status": "not_pending",
                           "message": "The request was decided by someone else "
                                      "a moment ago."})

    comment = _esc(p_comment) if p_comment else f"Re-open {decision.lower()} by {_esc(caller)}"
    session.sql(f"""
        INSERT INTO ADJUSTMENT_APP.ADJ_SIGNOFF_HISTORY
            (COBID, PROCESS_TYPE, ENTITY_CODE, OLD_STATUS, NEW_STATUS, ACTION_BY, COMMENT)
        VALUES ({cobid}, '{scope}', '{entity}', 'REOPEN_REQUESTED', '{new_status}',
                '{_esc(caller)}', '{comment[:990]}')
    """).collect()

    return json.dumps({"status": "ok", "decision": decision, "by": caller,
                       "new_status": new_status})
$$;

-- ═══════════════════════════════════════════════════════════════════════════
-- VERIFY
-- ═══════════════════════════════════════════════════════════════════════════
DESCRIBE PROCEDURE ADJUSTMENT_APP.SP_DECIDE_ADJUSTMENT(VARCHAR, VARCHAR, VARCHAR);
DESCRIBE PROCEDURE ADJUSTMENT_APP.SP_DECIDE_REOPEN(INT, VARCHAR, VARCHAR, VARCHAR, VARCHAR);
