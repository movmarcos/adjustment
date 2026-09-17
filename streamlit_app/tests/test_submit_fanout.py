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
    # Explicit marker the success screen keys off — never text-matched.
    assert out["fanout"] is True
    assert out["created"] == 2


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
    assert out["fanout"] is True
    assert out["created"] == 1


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
    assert out["fanout"] is True
    assert out["created"] == 1


def test_mixed_statuses_surface_the_stricter_one_and_are_listed():
    def submit_one(payload):
        if payload["process_type"] == "VaR":
            return {"status": "Pending"}
        return {"status": "Pending Approval"}

    out = submit_fanout({}, ["VaR", "Stress"], submit_one, _is_success, _scope_label)

    # A "Pending Approval" scope still needs an approver — surface that,
    # not whichever scope happened to run first.
    assert out["status"] == "Pending Approval"
    assert out["fanout"] is True
    assert out["created"] == 2
    assert "Statuses: VaR Pending, Stress Pending Approval." in out["message"]


def test_uniform_statuses_do_not_add_a_statuses_note():
    def submit_one(payload):
        return {"status": "Pending"}

    out = submit_fanout({}, ["VaR", "Stress"], submit_one, _is_success, _scope_label)

    assert out["status"] == "Pending"
    assert "Statuses:" not in out["message"]


def test_empty_scope_list_is_guarded():
    def submit_one(payload):
        raise AssertionError("submit_one must not be called with no scopes")

    out = submit_fanout({}, [], submit_one, _is_success, _scope_label)

    assert out == {"status": "Error", "message": "No scope selected.",
                    "fanout": True, "created": 0}
