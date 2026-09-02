"""
SGN — Sign-off lifecycle (UAT plan §7), fully automated.

Ordered scenario on the isolation COB: feed rows are seeded, synced into the
app, then driven through every lifecycle transition with feed-propagation
checks in both directions. Tests build on one another top-to-bottom (pytest
runs them in file order); an early failure will cascade — read the report
from the first ❌.

Entities: ZUATA (direct sign-off + re-open), ZUATB (sign-off via approval,
then blocked-submission check), ZUATC (reject/revert), ZUATD (SUB_TYPE pair).
"""
import pytest

from conftest import (FAKE_COB, ENT_A, ENT_B, ENT_C, ENT_D,
                      U_SUBMIT, U_APPROVE, call_sp, lit)

SP_REQ = "ADJUSTMENT_APP.SP_REQUEST_SIGNOFF_CHANGE"
SP_DEC = "ADJUSTMENT_APP.SP_DECIDE_SIGNOFF_CHANGE"
SP_SYNC = "ADJUSTMENT_APP.SP_SYNC_SIGNOFF_STATUS"


def _status(ev, entity, sub=""):
    r = ev.sql(
        f"App status for {entity}/{sub or 'no sub'}",
        f"""SELECT SIGN_OFF_STATUS FROM ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS
            WHERE COBID = {FAKE_COB} AND UPPER(PROCESS_TYPE) = 'FRTB'
              AND UPPER(ENTITY_CODE) = '{entity}'
              AND COALESCE(UPPER(SUB_TYPE), '') = '{sub.upper()}'""")
    return r[0]["SIGN_OFF_STATUS"].upper() if r else None


def _feed_status(ev, feed, entity, sub=""):
    r = ev.sql(
        f"Feed status for {entity}/{sub or 'no sub'}",
        f"""SELECT PUBLISH_STATUS FROM {feed}
            WHERE COBID = {FAKE_COB} AND UPPER(PROCESS_TYPE) = 'FRTB'
              AND UPPER(ENTITY_CODE) = '{entity}'
              AND COALESCE(UPPER(TRIM(SUB_TYPE)), '') = '{sub.upper()}'""")
    return r[0]["PUBLISH_STATUS"] if r else None


@pytest.mark.uat("SGN-01", title="Sync from feed creates OPEN rows", priority="P1")
def test_sgn01_sync_creates_open_rows(session, feed_table, ev):
    # Seed: FRTB feed rows for the four test entities, none signed off.
    for ent, sub in [(ENT_A, None), (ENT_B, None), (ENT_C, None),
                     (ENT_D, "CVA"), (ENT_D, None)]:
        session.sql(f"""
            INSERT INTO {feed_table}
                (COBID, PROCESS_TYPE, ENTITY_CODE, SUB_TYPE, PUBLISH_STATUS)
            SELECT {FAKE_COB}, 'FRTB', '{ent}', {lit(sub)}, 'InProgress'
            WHERE NOT EXISTS (
                SELECT 1 FROM {feed_table}
                WHERE COBID = {FAKE_COB} AND UPPER(PROCESS_TYPE) = 'FRTB'
                  AND UPPER(ENTITY_CODE) = '{ent}'
                  AND COALESCE(UPPER(TRIM(SUB_TYPE)), '') = COALESCE(UPPER({lit(sub)}), ''))
        """).collect()
    ev.note("Seeded", f"5 FRTB feed rows on COB {FAKE_COB} (incl. one CVA sub-type)")

    res = call_sp(session, SP_SYNC)
    ev.note("Sync result", str(res)[:300])

    r = ev.sql("OPEN rows materialised",
               f"""SELECT ENTITY_CODE, SUB_TYPE, SIGN_OFF_STATUS, SIGNOFF_SOURCE
                   FROM ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS
                   WHERE COBID = {FAKE_COB} AND UPPER(PROCESS_TYPE) = 'FRTB'""")
    ev.check("all 5 seeded rows exist in the app as OPEN/EXTERNAL",
             len(r) == 5 and all(x["SIGN_OFF_STATUS"].upper() == "OPEN" for x in r))


@pytest.mark.uat("SGN-02", title="Exact scope match — no FRTB→DRC/RRAO fan-out", priority="P1")
def test_sgn02_exact_scope_match(session, feed_table, ev):
    r = ev.sql("DRC/RRAO rows after FRTB-only sync",
               f"""SELECT PROCESS_TYPE, ENTITY_CODE FROM ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS
                   WHERE COBID = {FAKE_COB}
                     AND UPPER(PROCESS_TYPE) IN ('FRTBDRC', 'FRTBRRAO')""")
    ev.check("FRTB feed rows created NO FRTBDRC/FRTBRRAO app rows", len(r) == 0)

    session.sql(f"""
        INSERT INTO {feed_table}
            (COBID, PROCESS_TYPE, ENTITY_CODE, SUB_TYPE, PUBLISH_STATUS)
        VALUES ({FAKE_COB}, 'FRTBDRC', '{ENT_A}', NULL, 'InProgress')
    """).collect()
    call_sp(session, SP_SYNC)
    r = ev.sql("DRC row after its own feed row exists",
               f"""SELECT ENTITY_CODE, SIGN_OFF_STATUS FROM ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS
                   WHERE COBID = {FAKE_COB} AND UPPER(PROCESS_TYPE) = 'FRTBDRC'""")
    ev.check("FRTBDRC appears only from its own feed row", len(r) == 1)


@pytest.mark.uat("SGN-03", title="Direct sign-off (no approval) applies immediately", priority="P1")
def test_sgn03_direct_signoff(session, ev):
    res = call_sp(session, SP_REQ, FAKE_COB, "FRTB", ENT_A, "",
                  "SIGNOFF", "UAT automation — direct sign-off", False, U_SUBMIT)
    ev.note("SP result", str(res)[:300])
    ev.check("status is SIGNED_OFF immediately", _status(ev, ENT_A) == "SIGNED_OFF")
    h = ev.sql("History event recorded",
               f"""SELECT NEW_STATUS, ACTION_BY FROM ADJUSTMENT_APP.ADJ_SIGNOFF_HISTORY
                   WHERE COBID = {FAKE_COB} AND UPPER(PROCESS_TYPE) = 'FRTB'
                     AND UPPER(ENTITY_CODE) = '{ENT_A}'
                   ORDER BY ACTION_AT DESC LIMIT 1""")
    ev.check("history records SIGNED_OFF by the caller",
             bool(h) and h[0]["NEW_STATUS"].upper() == "SIGNED_OFF"
             and h[0]["ACTION_BY"] == U_SUBMIT)


@pytest.mark.uat("SGN-04", title="Sign-off propagates SignedOff to the feed", priority="P1")
def test_sgn04_feed_propagation_signoff(session, feed_table, ev):
    fs = _feed_status(ev, feed_table, ENT_A)
    ev.check("feed row updated to SignedOff",
             fs is not None and fs.upper() == "SIGNEDOFF")


@pytest.mark.uat("SGN-05", title="Sign-off via approval (request → approve)", priority="P1")
def test_sgn05_signoff_via_approval(session, feed_table, approvers, ev):
    call_sp(session, SP_REQ, FAKE_COB, "FRTB", ENT_B, "",
            "SIGNOFF", "UAT automation — via approval", True, U_SUBMIT)
    ev.check("status parks at SIGNOFF_REQUESTED",
             _status(ev, ENT_B) == "SIGNOFF_REQUESTED")

    res = call_sp(session, SP_DEC, FAKE_COB, "FRTB", ENT_B, "",
                  "Approved", "UAT automation — approving", U_APPROVE)
    ev.note("Decision result", str(res)[:300])
    ev.check("status is SIGNED_OFF after approval", _status(ev, ENT_B) == "SIGNED_OFF")
    fs = _feed_status(ev, feed_table, ENT_B)
    ev.check("feed row updated to SignedOff after approval",
             fs is not None and fs.upper() == "SIGNEDOFF")


@pytest.mark.uat("SGN-06", title="Rejected request reverts to previous status", priority="P1")
def test_sgn06_reject_reverts(session, feed_table, approvers, ev):
    call_sp(session, SP_REQ, FAKE_COB, "FRTB", ENT_C, "",
            "SIGNOFF", "UAT automation — will be rejected", True, U_SUBMIT)
    ev.check("request parked", _status(ev, ENT_C) == "SIGNOFF_REQUESTED")

    call_sp(session, SP_DEC, FAKE_COB, "FRTB", ENT_C, "",
            "Rejected", "UAT automation — rejecting", U_APPROVE)
    ev.check("status reverted to OPEN (PREV_STATUS)", _status(ev, ENT_C) == "OPEN")
    fs = _feed_status(ev, feed_table, ENT_C)
    ev.check("feed untouched by the rejected request",
             fs is not None and fs.upper() != "SIGNEDOFF")


@pytest.mark.uat("SGN-07", title="Re-open needs approval; approved → feed InProgress", priority="P1")
def test_sgn07_reopen_flow(session, feed_table, approvers, ev):
    call_sp(session, SP_REQ, FAKE_COB, "FRTB", ENT_A, "",
            "REOPEN", "UAT automation — re-open", True, U_SUBMIT)
    ev.check("status parks at REOPEN_REQUESTED",
             _status(ev, ENT_A) == "REOPEN_REQUESTED")

    call_sp(session, SP_DEC, FAKE_COB, "FRTB", ENT_A, "",
            "Approved", "UAT automation — approving re-open", U_APPROVE)
    ev.check("status is REOPENED", _status(ev, ENT_A) == "REOPENED")
    fs = _feed_status(ev, feed_table, ENT_A)
    ev.check("feed row back to InProgress after approved re-open",
             fs is not None and fs.upper() == "INPROGRESS")


@pytest.mark.uat("SGN-08", title="Signed-off blocks submission; REOPENED allows it", priority="P1")
def test_sgn08_submission_gate(session, ev):
    # ENT_B is SIGNED_OFF (SGN-05); ENT_A is REOPENED (SGN-07).
    blocked = call_sp(session, "ADJUSTMENT_APP.SP_SUBMIT_ADJUSTMENT", json_payload(ENT_B))
    ev.note("Submit against SIGNED_OFF entity", str(blocked)[:300])
    ev.check("submission against signed-off entity is rejected",
             isinstance(blocked, dict)
             and "SIGNEDOFF" in str(blocked.get("status", "")).upper().replace(" ", ""))

    allowed = call_sp(session, "ADJUSTMENT_APP.SP_SUBMIT_ADJUSTMENT", json_payload(ENT_A))
    ev.note("Submit against REOPENED entity", str(allowed)[:300])
    ev.check("submission against re-opened entity is accepted",
             isinstance(allowed, dict)
             and str(allowed.get("status", "")).upper().startswith("PENDING"))


def json_payload(entity):
    import json as _j
    return _j.dumps({
        "cobid": FAKE_COB, "process_type": "FRTB", "adjustment_type": "Direct",
        "username": U_SUBMIT, "entity_code": entity,
        "adjustment_value_in_usd": 1.0, "requires_approval": True,
        "reason": "UAT automation — submission gate check",
    })


@pytest.mark.uat("SGN-09", title="SUB_TYPE rows are independent keys; NULL ≡ empty", priority="P1")
def test_sgn09_subtype_granularity(session, feed_table, ev):
    call_sp(session, SP_REQ, FAKE_COB, "FRTB", ENT_D, "CVA",
            "SIGNOFF", "UAT automation — CVA sub-type only", False, U_SUBMIT)
    ev.check("CVA sub-type row signed off", _status(ev, ENT_D, "CVA") == "SIGNED_OFF")
    ev.check("no-sub-type row for the same entity is untouched (still OPEN)",
             _status(ev, ENT_D, "") == "OPEN")
    fs = _feed_status(ev, feed_table, ENT_D, "CVA")
    ev.check("feed updated for the CVA row only",
             fs is not None and fs.upper() == "SIGNEDOFF")
    fs2 = _feed_status(ev, feed_table, ENT_D, "")
    ev.check("feed row without sub-type untouched",
             fs2 is not None and fs2.upper() == "INPROGRESS")

    # NULL ≡ '' : target the no-sub row passing empty string explicitly.
    call_sp(session, SP_REQ, FAKE_COB, "FRTB", ENT_D, "",
            "SIGNOFF", "UAT automation — empty sub targets NULL row", False, U_SUBMIT)
    ev.check("empty-string sub-type addresses the NULL-sub row",
             _status(ev, ENT_D, "") == "SIGNED_OFF")
