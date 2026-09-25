"""
APP — Approval workflow (UAT plan §5), automated via SP_SUBMIT_ADJUSTMENT /
SP_DECIDE_ADJUSTMENT with distinct p_caller identities.

All submissions use the isolation COB and requires_approval=True, so nothing
reaches the processing pipeline; approved headers are deleted at session
cleanup (and the approved one immediately, to keep the poller away from it).
"""
import json

import pytest

AREA = "Approval Workflow"

from conftest import FAKE_COB, U_SUBMIT, U_APPROVE, call_sp

SP_SUBMIT = "ADJUSTMENT_APP.SP_SUBMIT_ADJUSTMENT"
SP_DECIDE = "ADJUSTMENT_APP.SP_DECIDE_ADJUSTMENT"


def _submit(session, note):
    payload = json.dumps({
        "cobid": FAKE_COB, "process_type": "VaR", "adjustment_type": "Scale",
        "scale_factor": 1.01, "username": U_SUBMIT, "requires_approval": True,
        "reason": f"UAT automation — {note}",
    })
    return call_sp(session, SP_SUBMIT, payload)


def _header(ev, adj_id, label):
    return ev.sql(label,
                  f"""SELECT ADJ_ID, STATUS FROM ADJUSTMENT_APP.ADJ_HEADER
                      WHERE ADJ_ID = '{adj_id}'""")


@pytest.mark.uat("APP-01", title="Submit lands in Pending Approval", priority="P1")
def test_app01_submit_pending(session, approvers, ev):
    res = _submit(session, "approval queue entry")
    ev.note("Submit result", str(res)[:300])
    ev.check("submit returned an adj_id",
             isinstance(res, dict) and res.get("adj_id"))
    h = _header(ev, res["adj_id"], "Header status after submit")
    ev.check("header status is Pending Approval",
             bool(h) and h[0]["STATUS"] == "Pending Approval")
    ev.note("adj_id", res["adj_id"])
    pytest.app01_adj_id = res["adj_id"]


@pytest.mark.uat("APP-02", title="Second user approves (p_caller identity)", priority="P1")
def test_app02_approve_other_user(session, approvers, ev):
    adj_id = getattr(pytest, "app01_adj_id", None)
    ev.check("APP-01 produced an adjustment to approve", adj_id is not None)
    res = call_sp(session, SP_DECIDE, adj_id, "Approved",
                  "UAT automation — approval", U_APPROVE)
    ev.note("Decision result", str(res)[:300])
    h = _header(ev, adj_id, "Header status after approval")
    ev.check("status left Pending Approval (identity resolved, no error)",
             bool(h) and h[0]["STATUS"] != "Pending Approval")
    # Keep the pipeline poller away from an approved row on a fake COB.
    session.sql(f"DELETE FROM ADJUSTMENT_APP.ADJ_HEADER WHERE ADJ_ID = '{adj_id}'").collect()
    ev.note("Housekeeping", "approved test header deleted before the poller cycle")


@pytest.mark.uat("APP-02b", title="Self-approval is refused", priority="P1")
def test_app02b_self_approval_refused(session, approvers, ev):
    res = _submit(session, "self-approval check")
    adj_id = res.get("adj_id") if isinstance(res, dict) else None
    ev.check("submitted", adj_id is not None)
    dec = call_sp(session, SP_DECIDE, adj_id, "Approved",
                  "UAT automation — self-approve attempt", U_SUBMIT)
    ev.note("Self-approval attempt result", str(dec)[:300])
    h = _header(ev, adj_id, "Header status after self-approval attempt")
    ev.check("status still Pending Approval — self-approval blocked",
             bool(h) and h[0]["STATUS"] == "Pending Approval")


@pytest.mark.uat("APP-03", title="Reject stores decision + audit trail", priority="P1")
def test_app03_reject(session, approvers, ev):
    res = _submit(session, "rejection check")
    adj_id = res.get("adj_id") if isinstance(res, dict) else None
    ev.check("submitted", adj_id is not None)
    call_sp(session, SP_DECIDE, adj_id, "Rejected",
            "UAT automation — rejected on purpose", U_APPROVE)
    h = _header(ev, adj_id, "Header status after rejection")
    ev.check("header status is Rejected",
             bool(h) and "REJECT" in h[0]["STATUS"].upper())
    a = ev.sql("Audit trail row",
               f"""SELECT * FROM ADJUSTMENT_APP.ADJ_STATUS_HISTORY
                   WHERE ADJ_ID = '{adj_id}' ORDER BY 1 DESC LIMIT 3""")
    ev.check("status history recorded the decision", len(a) >= 1)


@pytest.mark.uat("APP-04", title="Approve works after reject + resubmit (change flow)", priority="P1")
def test_app04_approve_after_change(session, approvers, ev):
    r1 = _submit(session, "change flow v1")
    call_sp(session, SP_DECIDE, r1["adj_id"], "Rejected",
            "UAT automation — v1 rejected", U_APPROVE)
    r2 = _submit(session, "change flow v2")
    dec = call_sp(session, SP_DECIDE, r2["adj_id"], "Approved",
                  "UAT automation — v2 approved", U_APPROVE)
    ev.note("v2 decision result", str(dec)[:300])
    h = _header(ev, r2["adj_id"], "v2 header status")
    ev.check("resubmitted adjustment approved cleanly (dead-button regression)",
             bool(h) and h[0]["STATUS"] != "Pending Approval")
    session.sql(f"DELETE FROM ADJUSTMENT_APP.ADJ_HEADER WHERE ADJ_ID = '{r2['adj_id']}'").collect()
