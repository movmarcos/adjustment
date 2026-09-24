"""A Direct or Upload run that wrote nothing must not report success.

Reported 2026-09-24: a Direct adjustment went through the queue, showed
Running, and finished with the header saying it was done. The fact
adjustment table was empty, and there was no error anywhere to explain it.

The cause of the empty write is per-file and is diagnosed separately. The
defect pinned here is the reporting: both the Upload and the Direct branches
called update_header_status(..., "Processed") without ever looking at how
many rows the insert produced. Every uploaded row can be filtered out before
the insert — an empty or zero metric column, or a payload field the scope's
schema no longer carries — and the result read as a clean success.

An empty load is now Failed, with a message naming the metric column to
check. Silence was the actual problem: there was nothing to find.
"""
import ast
import os
import re

import pytest

ENGINE_SQL = os.path.join(os.path.dirname(__file__), "..", "..",
                          "new_adjustment_db_objects",
                          "05_sp_process_adjustment.sql")


def _source():
    with open(ENGINE_SQL, encoding="utf-8") as fh:
        return fh.read()


def _body():
    """The stored procedure's Python body."""
    src = _source()
    bodies = re.findall(r"\$\$(.*?)\$\$", src, re.S)
    assert len(bodies) == 1, "expected one Python body in this procedure"
    return bodies[0]


def test_the_body_is_valid_python():
    ast.parse(_body())


@pytest.mark.parametrize("noun", ["Upload", "Direct"])
def test_both_branches_check_the_row_count(noun):
    """Both file branches had the same defect, so both need the guard."""
    src = _source()
    success = ('result["message"] = "%s adjustments processed successfully"'
               % noun)
    assert success in src, noun + " success branch not found"
    before = src[:src.index(success)]
    guard = before.rindex("if not rows_count:")
    processed = before.rindex(
        'update_header_status(session, df_adj_direct, cobid, "Processed")')
    assert guard < processed, (
        "The " + noun + " branch marks the header Processed before checking "
        "whether anything was written.")


def test_the_guard_marks_it_failed_not_processed():
    body = _body()
    i = body.index("if not rows_count:")
    block = body[i:i + 1400]
    assert 'update_header_status(session, df_adj_direct, cobid, "Failed"' in block, (
        "A zero-row load must be Failed. Leaving it Processed is what made "
        "this invisible in the first place.")
    assert 'log_status_history(session, adj_ids, "Running", "Failed")' in block, (
        "The transition must be recorded, or the audit trail shows a run "
        "that went Running and then nowhere.")


def test_the_guard_stops_before_the_downstream_handoff():
    """Refreshing reports off an empty load would publish nothing, loudly."""
    body = _body()
    i = body.index("if not rows_count:")
    block = body[i:i + 1400]
    assert "return json.dumps(result)" in block, (
        "The guard does not return, so processing continues into the "
        "downstream hand-off and the success notification.")


def test_the_message_names_the_metric_column_to_check():
    body = _body()
    i = body.index("if not rows_count:")
    block = body[i:i + 1400]
    assert "{metric_name}" in block, (
        "The failure message must name the metric column. 'No rows written' "
        "with no pointer is barely better than the silence it replaced.")
    assert "upload template" in block


def test_the_guard_exists_exactly_twice():
    """One per file branch. A third would suggest it drifted somewhere odd."""
    assert _body().count("if not rows_count:") == 2


def test_zero_rows_is_not_treated_as_a_valid_outcome_elsewhere():
    """The count query itself must still run before the guard.

    The guard is only meaningful if rows_count is a real count of what landed
    in the fact table, not a value carried over from the writer's own return.
    """
    src = _source()
    assert "SELECT COUNT(*) AS CNT FROM {fact_adj_tbl_name}" in src, (
        "The row count is no longer read back from the fact table, so the "
        "guard could pass on a number that does not reflect what was "
        "actually written.")
