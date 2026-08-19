-- =============================================================================
-- 12_SP_WORKFLOW.SQL
-- Server-side enforcement of the 4-eyes controls.
--
-- Until now approve/reject were raw UPDATEs issued by the Streamlit pages —
-- the maker-checker rule lived only in the UI, so any session with UPDATE
-- rights on ADJ_HEADER could bypass it. These procedures make the control
-- real at the database level:
--
--   • The acting identity mirrors SP_SUBMIT_ADJUSTMENT's precedence: the
--     app-resolved viewer name (p_caller) first, CURRENT_USER() as fallback.
--     Submitter names (ADJ_HEADER.USERNAME) and the ADJ_APPROVERS list are
--     both maintained in the app-resolved format, so maker-checker compares
--     names from the same source. NOTE this trusts the client-supplied name
--     until table write access is locked down in Snowflake (planned) — at
--     that point flip the precedence back to CURRENT_USER()-only (requires
--     READ SESSION granted to the app owner role).
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
    p_comment  VARCHAR,
    p_caller   VARCHAR    -- app-resolved viewer name (same source as submit)
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
COMMENT = '4-eyes decision on an adjustment: caller = p_caller (app-resolved, same source as submit) with CURRENT_USER() fallback, active approver + scope check, self-approval refused, guarded transition + audit.'
EXECUTE AS CALLER
AS
$$
import json


def _esc(v):
    return str(v).replace("\\", "\\\\").replace("'", "''") if v is not None else ""


def _caller(session, p_caller):
    """App-resolved viewer name first (same precedence as SP_SUBMIT_ADJUSTMENT,
    so submitter/approver names compare like-for-like), CURRENT_USER() as
    fallback."""
    u = str(p_caller or "").strip()
    if u and u.lower() != "unknown":
        return u
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


def main(session, p_adj_id, p_decision, p_comment, p_caller):
    decision = str(p_decision or "").strip().capitalize()
    if decision not in ("Approved", "Rejected"):
        return json.dumps({"status": "error",
                           "message": f"Invalid decision '{p_decision}' — "
                                      f"expected Approved or Rejected."})

    caller = _caller(session, p_caller)
    if not caller or caller.lower() == "unknown":
        return json.dumps({"status": "no_identity",
                           "message": "Caller identity could not be resolved "
                                      "(no app identity and CURRENT_USER() "
                                      "empty) — approvals are blocked."})

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

    comment = _esc(str(p_comment)[:990]) if p_comment else f"{decision} by {_esc(caller)}"
    session.sql(f"""
        INSERT INTO ADJUSTMENT_APP.ADJ_STATUS_HISTORY
            (ADJ_ID, OLD_STATUS, NEW_STATUS, CHANGED_BY, COMMENT)
        VALUES ('{adj_id}', 'Pending Approval', '{decision}',
                '{_esc(caller)}', '{comment[:1980]}')
    """).collect()

    return json.dumps({"status": "ok", "decision": decision, "by": caller})
$$;


-- ─────────────────────────────────────────────────────────────────────────────
-- SP_REQUEST_SIGNOFF_CHANGE — raise a sign-off OR re-open request on a COB
--
-- There is no standalone "open the COB" concept: open rows come from the
-- upstream publish feed (materialised by SP_SYNC_SIGNOFF_STATUS). This proc
-- applies the two lifecycle changes the app can make — SIGNOFF (open →
-- signed off) and REOPEN (signed off → open). Approval routing per
-- p_requires_approval (Marcos's policy): SIGNOFF is OPTIONAL-approval —
-- the UI checkbox is unchecked by default so it applies directly; REOPEN
-- is ALWAYS approval-gated (the UI checkbox is ticked and locked).
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR ALTER PROCEDURE ADJUSTMENT_APP.SP_REQUEST_SIGNOFF_CHANGE(
    p_cobid             INT,
    p_process_type      VARCHAR,
    p_entity_code       VARCHAR,   -- '*' = whole-scope row
    p_action            VARCHAR,   -- 'SIGNOFF' | 'REOPEN'
    p_reason            VARCHAR,
    p_requires_approval BOOLEAN,
    p_caller            VARCHAR    -- app-resolved viewer name (same source as submit)
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
COMMENT = 'Raise a COB sign-off or re-open request (approval-gated by default): guarded transition from the compatible current status + sign-off history. Requester recorded in REOPEN_REQUESTED_BY/AT/REASON for both request types.'
EXECUTE AS CALLER
AS
$$
import json

# action → (pending status, direct-apply status, statuses it can start from)
ACTIONS = {
    "SIGNOFF": ("SIGNOFF_REQUESTED", "SIGNED_OFF", ("OPEN", "REOPENED")),
    "REOPEN":  ("REOPEN_REQUESTED",  "REOPENED",   ("SIGNED_OFF",)),
}


def _esc(v):
    return str(v).replace("\\", "\\\\").replace("'", "''") if v is not None else ""


def main(session, p_cobid, p_process_type, p_entity_code, p_action, p_reason,
         p_requires_approval, p_caller):
    action = str(p_action or "").strip().upper()
    if action not in ACTIONS:
        return json.dumps({"status": "error",
                           "message": f"Invalid action '{p_action}' — "
                                      f"expected SIGNOFF or REOPEN."})
    pending_status, direct_status, from_statuses = ACTIONS[action]

    caller = str(p_caller or "").strip()
    if not caller or caller.lower() == "unknown":
        row = session.sql("SELECT CURRENT_USER() AS U").collect()
        caller = str(row[0]["U"]).strip() if row and row[0]["U"] else ""
    if not caller or caller.lower() == "unknown":
        return json.dumps({"status": "no_identity",
                           "message": "Caller identity could not be resolved — "
                                      "requests are blocked."})

    cobid  = int(p_cobid)
    scope  = _esc(p_process_type)
    entity = _esc(p_entity_code) if p_entity_code and str(p_entity_code).strip() else "*"

    rows = session.sql(f"""
        SELECT SIGN_OFF_STATUS FROM ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS
        WHERE COBID = {cobid} AND UPPER(PROCESS_TYPE) = UPPER('{scope}')
          AND UPPER(ENTITY_CODE) = UPPER('{entity}')
    """).collect()
    if not rows and action == "REOPEN":
        # Signed off upstream but not yet synced into the app table: create
        # the row directly in the requested state (source EXTERNAL) — the
        # same behaviour the New Adjustment page's MERGE used to provide.
        # Only for REOPEN with approval: a sign-off request needs a real
        # OPEN row from the feed, and a direct re-open must see the row.
        if not (True if p_requires_approval is None else bool(p_requires_approval)):
            return json.dumps({"status": "not_found",
                               "message": "No sign-off row for this "
                                          "COB/scope/entity — run the "
                                          "upstream sync first."})
        session.sql(f"""
            INSERT INTO ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS
                (COBID, PROCESS_TYPE, ENTITY_CODE, SIGN_OFF_STATUS, SIGN_OFF_BY,
                 SIGN_OFF_TIMESTAMP, SIGNOFF_SOURCE, REOPEN_REQUESTED_BY,
                 REOPEN_REQUESTED_AT, REOPEN_REASON)
            SELECT {cobid}, '{scope}', '{entity}', 'REOPEN_REQUESTED',
                   'EXTERNAL FEED', CURRENT_TIMESTAMP(), 'EXTERNAL',
                   '{_esc(caller)}', CURRENT_TIMESTAMP(),
                   '{_esc(str(p_reason or "")[:490])}'
            WHERE NOT EXISTS (
                SELECT 1 FROM ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS
                WHERE COBID = {cobid} AND UPPER(PROCESS_TYPE) = UPPER('{scope}')
                  AND UPPER(ENTITY_CODE) = UPPER('{entity}'))
        """).collect()
        session.sql(f"""
            INSERT INTO ADJUSTMENT_APP.ADJ_SIGNOFF_HISTORY
                (COBID, PROCESS_TYPE, ENTITY_CODE, OLD_STATUS, NEW_STATUS, ACTION_BY, COMMENT)
            VALUES ({cobid}, '{scope}', '{entity}', 'SIGNED_OFF', 'REOPEN_REQUESTED',
                    '{_esc(caller)}', 'Re-open requested (upstream sign-off, not yet synced)')
        """).collect()
        return json.dumps({"status": "ok", "action": action,
                           "new_status": "REOPEN_REQUESTED", "by": caller,
                           "pending_approval": True})
    if not rows:
        return json.dumps({"status": "not_found",
                           "message": "No sign-off row for this COB/scope/entity "
                                      "— run the upstream sync first."})
    cur = str(rows[0]["SIGN_OFF_STATUS"]).upper()
    if cur not in from_statuses:
        return json.dumps({"status": "wrong_state",
                           "message": f"Cannot request {action.lower()} from "
                                      f"status {cur} (needs "
                                      f"{' or '.join(from_statuses)})."})

    requires_approval = True if p_requires_approval is None else bool(p_requires_approval)
    new_status = pending_status if requires_approval else direct_status
    from_in = ", ".join(f"'{s}'" for s in from_statuses)
    # Slice the RAW text first, escape after — slicing an escaped string can
    # split a doubled quote/backslash pair and break the literal.
    reason = _esc(str(p_reason or "")[:490])

    if requires_approval:
        set_clause = (f"SIGN_OFF_STATUS = '{new_status}', "
                      f"PREV_STATUS = '{cur}', "
                      f"REOPEN_REQUESTED_BY = '{_esc(caller)}', "
                      f"REOPEN_REQUESTED_AT = CURRENT_TIMESTAMP(), "
                      f"REOPEN_REASON = '{reason[:990]}'")
    elif action == "SIGNOFF":
        set_clause = (f"SIGN_OFF_STATUS = '{new_status}', "
                      f"SIGN_OFF_BY = '{_esc(caller)}', "
                      f"SIGN_OFF_TIMESTAMP = CURRENT_TIMESTAMP(), "
                      f"SIGNOFF_SOURCE = 'APP'")
    else:
        set_clause = (f"SIGN_OFF_STATUS = '{new_status}', "
                      f"REOPEN_APPROVED_BY = '{_esc(caller)}', "
                      f"REOPEN_APPROVED_AT = CURRENT_TIMESTAMP()")

    upd = session.sql(f"""
        UPDATE ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS
        SET {set_clause}, UPDATED_DATE = CURRENT_TIMESTAMP()
        WHERE COBID = {cobid}
          AND UPPER(PROCESS_TYPE) = UPPER('{scope}')
          AND UPPER(ENTITY_CODE) = UPPER('{entity}')
          AND UPPER(SIGN_OFF_STATUS) IN ({from_in})
    """).collect()
    try:
        n = int(upd[0][0]) if upd else 0
    except (TypeError, ValueError, IndexError):
        n = 0
    if n == 0:
        return json.dumps({"status": "wrong_state",
                           "message": "The status changed a moment ago — "
                                      "refresh and try again."})

    verb = "Sign-off" if action == "SIGNOFF" else "Re-open"
    comment = (f"{verb} requested by {_esc(caller)}"
               + (f": {reason[:900]}" if reason else "")) if requires_approval \
              else f"{verb} applied directly by {_esc(caller)} (approval not required)"
    session.sql(f"""
        INSERT INTO ADJUSTMENT_APP.ADJ_SIGNOFF_HISTORY
            (COBID, PROCESS_TYPE, ENTITY_CODE, OLD_STATUS, NEW_STATUS, ACTION_BY, COMMENT)
        VALUES ({cobid}, '{scope}', '{entity}', '{cur}', '{new_status}',
                '{_esc(caller)}', '{comment[:1980]}')
    """).collect()

    return json.dumps({"status": "ok", "action": action,
                       "new_status": new_status, "by": caller,
                       "pending_approval": requires_approval})
$$;


-- ─────────────────────────────────────────────────────────────────────────────
-- SP_DECIDE_SIGNOFF_CHANGE — approve/reject a pending sign-off OR re-open
-- request. Replaces SP_DECIDE_REOPEN (dropped below): one decision path for
-- both request types. Rejection returns the row to its pre-request status.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR ALTER PROCEDURE ADJUSTMENT_APP.SP_DECIDE_SIGNOFF_CHANGE(
    p_cobid        INT,
    p_process_type VARCHAR,
    p_entity_code  VARCHAR,   -- '*' = whole-scope row
    p_decision     VARCHAR,   -- 'Approved' | 'Rejected'
    p_comment      VARCHAR,
    p_caller       VARCHAR    -- app-resolved viewer name (same source as submit)
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
COMMENT = '4-eyes decision on a pending COB sign-off/re-open request: caller = p_caller with CURRENT_USER() fallback, active approver + scope check, requester cannot decide, guarded transition + sign-off history.'
EXECUTE AS CALLER
AS
$$
import json

# pending status → (approved status, rejected/revert status, label)
PENDING = {
    "REOPEN_REQUESTED":  ("REOPENED",   "SIGNED_OFF", "re-open"),
    "SIGNOFF_REQUESTED": ("SIGNED_OFF", "OPEN",       "sign-off"),
}


def _esc(v):
    return str(v).replace("\\", "\\\\").replace("'", "''") if v is not None else ""


def main(session, p_cobid, p_process_type, p_entity_code, p_decision, p_comment,
         p_caller):
    decision = str(p_decision or "").strip().capitalize()
    if decision not in ("Approved", "Rejected"):
        return json.dumps({"status": "error",
                           "message": f"Invalid decision '{p_decision}'."})

    caller = str(p_caller or "").strip()
    if not caller or caller.lower() == "unknown":
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
        SELECT SIGN_OFF_STATUS, REOPEN_REQUESTED_BY, PREV_STATUS
        FROM ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS
        WHERE COBID = {cobid} AND UPPER(PROCESS_TYPE) = UPPER('{scope}')
          AND UPPER(ENTITY_CODE) = UPPER('{entity}')
    """).collect()
    cur = str(rows[0]["SIGN_OFF_STATUS"]).upper() if rows else ""
    if cur not in PENDING:
        return json.dumps({"status": "not_pending",
                           "message": "This COB/scope has no pending sign-off "
                                      "or re-open request (it may already be "
                                      "decided)."})
    approved_status, rejected_status, label = PENDING[cur]
    # Rejection reverts to the status the request was raised FROM: a sign-off
    # request from REOPENED must go back to REOPENED, not OPEN (OPEN falls
    # through to the upstream feed, which may still say signed off).
    prev = str(rows[0]["PREV_STATUS"] or "").upper()
    if cur == "SIGNOFF_REQUESTED" and prev in ("OPEN", "REOPENED"):
        rejected_status = prev

    requester = str(rows[0]["REOPEN_REQUESTED_BY"] or "")
    if requester.strip().upper() == caller.upper():
        return json.dumps({"status": "self_approval",
                           "message": f"You cannot decide your own {label} request."})

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
        new_status = approved_status
        if new_status == "SIGNED_OFF":
            set_clause = (f"SIGN_OFF_STATUS = 'SIGNED_OFF', "
                          f"SIGN_OFF_BY = '{_esc(caller)}', "
                          f"SIGN_OFF_TIMESTAMP = CURRENT_TIMESTAMP(), "
                          f"SIGNOFF_SOURCE = 'APP'")
        else:
            set_clause = (f"SIGN_OFF_STATUS = 'REOPENED', "
                          f"REOPEN_APPROVED_BY = '{_esc(caller)}', "
                          f"REOPEN_APPROVED_AT = CURRENT_TIMESTAMP()")
    else:
        new_status = rejected_status
        set_clause = f"SIGN_OFF_STATUS = '{new_status}'"

    upd = session.sql(f"""
        UPDATE ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS
        SET {set_clause}, UPDATED_DATE = CURRENT_TIMESTAMP()
        WHERE COBID = {cobid}
          AND UPPER(PROCESS_TYPE) = UPPER('{scope}')
          AND UPPER(ENTITY_CODE) = UPPER('{entity}')
          AND UPPER(SIGN_OFF_STATUS) = '{cur}'
    """).collect()
    try:
        n = int(upd[0][0]) if upd else 0
    except (TypeError, ValueError, IndexError):
        n = 0
    if n == 0:
        return json.dumps({"status": "not_pending",
                           "message": "The request was decided by someone else "
                                      "a moment ago."})

    comment = _esc(str(p_comment)[:990]) if p_comment \
              else f"{label.capitalize()} {decision.lower()} by {_esc(caller)}"
    session.sql(f"""
        INSERT INTO ADJUSTMENT_APP.ADJ_SIGNOFF_HISTORY
            (COBID, PROCESS_TYPE, ENTITY_CODE, OLD_STATUS, NEW_STATUS, ACTION_BY, COMMENT)
        VALUES ({cobid}, '{scope}', '{entity}', '{cur}', '{new_status}',
                '{_esc(caller)}', '{comment[:1980]}')
    """).collect()

    return json.dumps({"status": "ok", "decision": decision, "by": caller,
                       "request_type": label, "new_status": new_status})
$$;

-- ═══════════════════════════════════════════════════════════════════════════
-- VERIFY
-- ═══════════════════════════════════════════════════════════════════════════
DESCRIBE PROCEDURE ADJUSTMENT_APP.SP_DECIDE_ADJUSTMENT(VARCHAR, VARCHAR, VARCHAR, VARCHAR);
DESCRIBE PROCEDURE ADJUSTMENT_APP.SP_REQUEST_SIGNOFF_CHANGE(INT, VARCHAR, VARCHAR, VARCHAR, VARCHAR, BOOLEAN, VARCHAR);
DESCRIBE PROCEDURE ADJUSTMENT_APP.SP_DECIDE_SIGNOFF_CHANGE(INT, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR);
