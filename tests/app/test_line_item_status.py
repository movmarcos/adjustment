"""ADJ_LINE_ITEM_JSON.RUN_STATUS must say what happened to each row.

Reported 2026-09-24, twice: the column defaulted to 'Pending' and nothing
ever updated it, so every uploaded row read as still queued forever —
including rows processed months earlier. It was a permanently misleading
signal sitting next to the data it was supposed to describe.

It is now the per-row answer to "what happened to my file":

    Processed        the row was written
    Skipped - zero   the row's metric was zero or blank, so the insert
                     filtered it out
    Failed           the adjustment failed
    Deleted          the adjustment was deleted (set by the delete path)

"Skipped - zero" is the useful one. It is the difference between a file that
did nothing because of a blank column and a file that did nothing for a
reason you cannot see, which is where this whole investigation started.

These tests EXECUTE the helper out of the stored procedure and assert on the
SQL it builds, rather than pattern-matching the source.
"""
import os
import re

import pytest

ENGINE_SQL = os.path.join(os.path.dirname(__file__), "..", "..",
                          "streamlit", "adjustment_engine", "sql",
                          "05_sp_process_adjustment.sql")
DELETE_PAGE = os.path.join(os.path.dirname(__file__), "..", "..", "streamlit", "adjustment_engine", "pages",
                           "2_Adjustments.py")


def _engine_source():
    with open(ENGINE_SQL, encoding="utf-8") as fh:
        return fh.read()


class _Captured(Exception):
    pass


def _run_helper(status, metric=None, usd=None, adj_ids="'a'"):
    """Execute _mark_line_items from the procedure and return the SQL."""
    body = re.findall(r"\$\$(.*?)\$\$", _engine_source(), re.S)[0]
    fn = body[body.index("def _mark_line_items("):]
    fn = fn[:fn.index("\ndef ")]
    ns = {}
    exec(fn, ns)
    seen = {}

    class _Res:
        def collect(self):
            return []

    class _Session:
        def sql(self, q):
            seen["sql"] = q
            return _Res()

    ns["_mark_line_items"](_Session(), adj_ids, status, metric, usd)
    return " ".join(seen.get("sql", "").split())


# ── What the helper writes ───────────────────────────────────────────────────

def test_a_written_row_is_marked_processed():
    sql = _run_helper("Processed", "AMOUNT", "AMOUNT_IN_USD")
    assert "ELSE 'Processed' END" in sql


def test_a_zero_metric_row_is_marked_skipped_not_processed():
    """The distinction that makes the column worth having."""
    sql = _run_helper("Processed", "AMOUNT", "AMOUNT_IN_USD")
    assert "'Skipped - zero'" in sql, (
        "Rows the insert filtered out are marked the same as rows that "
        "landed, which hides exactly the case people need to see.")
    assert 'PAYLOAD:"AMOUNT_IN_USD"' in sql
    assert 'PAYLOAD:"AMOUNT"' in sql


def test_the_zero_test_requires_both_metrics_to_be_zero():
    """A row counts as written if EITHER leg is non-zero, matching the insert."""
    sql = _run_helper("Processed", "AMOUNT", "AMOUNT_IN_USD")
    between = sql[sql.index("CASE WHEN"):sql.index("THEN")]
    assert " AND " in between and " OR " not in between, (
        "The zero test must be AND across the metrics: the writer keeps a "
        "row when either leg is non-zero, so marking on OR would label "
        "written rows as skipped.")


def test_a_blank_metric_counts_as_zero():
    sql = _run_helper("Processed", "AMOUNT", "AMOUNT_IN_USD")
    assert sql.count("COALESCE(TRY_TO_NUMBER") == 2, (
        "A missing or non-numeric payload field must coalesce to zero, or a "
        "blank row would be reported as Processed.")


def test_a_failure_marks_every_row_failed_without_a_zero_test():
    sql = _run_helper("Failed")
    assert "SET RUN_STATUS = 'Failed'" in sql
    assert "CASE WHEN" not in sql, (
        "When the adjustment failed, no row was written — splitting them "
        "into Processed and Skipped would be a lie.")


def test_the_metric_fields_are_deduplicated():
    """Some scopes name the local and USD metric identically."""
    sql = _run_helper("Processed", "AMOUNT_IN_USD", "AMOUNT_IN_USD")
    assert sql.count("COALESCE(TRY_TO_NUMBER") == 1


def test_it_is_scoped_to_the_adjustment_and_skips_deleted_rows():
    sql = _run_helper("Processed", "AMOUNT", "AMOUNT_IN_USD", adj_ids="'x','y'")
    assert "WHERE ADJ_ID IN ('x','y')" in sql
    assert "AND IS_DELETED = FALSE" in sql, (
        "A deleted row must keep its Deleted status, not be restamped.")


def test_a_status_string_cannot_break_out_of_the_literal():
    sql = _run_helper("O'Brien")
    assert "'O''Brien'" in sql


def test_it_never_fails_the_adjustment():
    """The numbers are already written by then; this is reporting, not state."""
    body = re.findall(r"\$\$(.*?)\$\$", _engine_source(), re.S)[0]
    fn = body[body.index("def _mark_line_items("):]
    fn = fn[:fn.index("\ndef ")]
    assert "except Exception" in fn and "Warning: line-item status" in fn, (
        "A failure stamping a status would fail an adjustment whose rows are "
        "already in the fact table.")


# ── Where it is called ───────────────────────────────────────────────────────

@pytest.mark.parametrize("noun", ["Upload", "Direct"])
def test_both_file_branches_stamp_on_success(noun):
    src = _engine_source()
    marker = ('result["message"] = "%s adjustments processed successfully"'
              % noun)
    before = src[:src.index(marker)]
    assert '_mark_line_items(session, adj_ids_str, "Processed"' in before[-600:], (
        "The " + noun + " branch does not stamp its line items on success.")


def test_the_zero_row_failure_stamps_too():
    src = _engine_source()
    assert src.count('_mark_line_items(session, adj_ids_str, "Failed")') == 2, (
        "Both zero-row failure guards must mark their rows Failed, or a "
        "failed upload leaves rows reading Pending forever.")


def test_delete_stamps_the_rows_deleted():
    with open(DELETE_PAGE, encoding="utf-8") as fh:
        src = fh.read()
    block = src[src.index("def _do_delete()"):]
    block = block[:block.index("def ", 10)]
    assert "SET IS_DELETED = TRUE, RUN_STATUS = 'Deleted'" in block, (
        "Delete flags the rows but leaves their status saying Pending, so "
        "the row contradicts itself.")
