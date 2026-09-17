"""Unit tests for the pure fan-out helper (utils/submit_fanout.py).

No Streamlit, no Snowflake: submit_one / is_success / scope_label are fakes
passed in directly, so this exercises the exact branch logic used by the
page's `_submit_fanout(payload, scopes)` wrapper."""
from utils.submit_fanout import submit_fanout

SUCCESS_STATUSES = {"Pending", "Pending Approval"}


def _scope_label(code):
    return {"VaR": "VaR", "Stress": "Stress", "FRTB": "FRTBSBM"}.get(code, code)


def _is_success(res):
    return (res or {}).get("status") in SUCCESS_STATUSES


def test_all_scopes_succeed_calls_once_per_scope_and_names_them():
    calls = []

    def submit_one(payload):
        calls.append(payload["process_type"])
        return {"status": "Pending", "adj_id": "x"}

    out = submit_fanout({"adjustment_type": "Flatten"}, ["VaR", "Stress"],
                         submit_one, _is_success, _scope_label)

    assert calls == ["VaR", "Stress"]
    assert out["status"] == "Pending"
    assert "2 adjustments" in out["message"]
    assert "VaR" in out["message"] and "Stress" in out["message"]


def test_payload_fields_preserved_per_call_with_scope_swapped():
    seen = []

    def submit_one(payload):
        seen.append(dict(payload))
        return {"status": "Pending"}

    submit_fanout({"adjustment_type": "Scale", "cobid": 20260101}, ["VaR", "Stress"],
                   submit_one, _is_success, _scope_label)

    assert seen[0] == {"adjustment_type": "Scale", "cobid": 20260101, "process_type": "VaR"}
    assert seen[1] == {"adjustment_type": "Scale", "cobid": 20260101, "process_type": "Stress"}


def test_partial_failure_names_created_and_failed_no_rollback():
    def submit_one(payload):
        if payload["process_type"] == "VaR":
            return {"status": "Pending"}
        return {"status": "Error", "message": "blocked"}

    out = submit_fanout({}, ["VaR", "Stress"], submit_one, _is_success, _scope_label)

    assert out["status"] == "Error"
    assert "Stress: blocked" in out["message"]
    assert "Already created: VaR" in out["message"]
    assert "delete them from the Adjustments page" in out["message"]


def test_all_scopes_fail_no_partial_note():
    def submit_one(payload):
        return {"status": "Error", "message": "not accepted"}

    out = submit_fanout({}, ["VaR", "Stress"], submit_one, _is_success, _scope_label)

    assert out["status"] == "Error"
    assert "Already created" not in out["message"]
    assert "VaR: not accepted" in out["message"]
    assert "Stress: not accepted" in out["message"]


def test_single_scope_still_works():
    def submit_one(payload):
        return {"status": "Pending Approval"}

    out = submit_fanout({}, ["FRTB"], submit_one, _is_success, _scope_label)

    assert out["status"] == "Pending Approval"
    assert "1 adjustments" in out["message"]
    assert "FRTBSBM" in out["message"]
