"""Deleting an adjustment must delete its line items with it.

Reported 2026-09-24: "I also tried to delete the adjustment and the
adj_line_item_json is not flag as deleted."

File-flow uploads (VaR Upload, FRTB Direct) keep one row per CSV line in
ADJ_LINE_ITEM_JSON. The delete on the Adjustments page soft-deleted the
header, the dimension row and the fact rows, but never touched those line
items. They outlived the adjustment, still counted as live rows, and the
enriched views that feed the engine filter on IS_DELETED = FALSE — so a
reprocessed id would have read them again.

It is a SOFT delete, matching the header, so the audit trail survives.
And it happens inside the same transaction: the header and its rows must
never disagree about whether the adjustment still exists.
"""
import os
import re

PAGE = os.path.join(os.path.dirname(__file__), "..", "..", "streamlit", "adjustment_engine", "pages",
                    "2_Adjustments.py")


def _delete_block():
    with open(PAGE, encoding="utf-8") as fh:
        src = fh.read()
    start = src.index("def _do_delete()")
    end = src.index("def ", start + 10)
    return src[start:end]


def test_the_delete_flags_the_line_items():
    block = _delete_block()
    assert "UPDATE ADJUSTMENT_APP.ADJ_LINE_ITEM_JSON" in block, (
        "Deleting an adjustment leaves its ADJ_LINE_ITEM_JSON rows active. "
        "They outlive the adjustment and the enriched views would read them "
        "again on a reprocess.")
    assert "SET IS_DELETED = TRUE" in block


def test_it_is_a_soft_delete_not_a_hard_one():
    """The header is soft-deleted; its rows must match, for the audit trail."""
    block = _delete_block()
    assert "DELETE FROM ADJUSTMENT_APP.ADJ_LINE_ITEM_JSON" not in block, (
        "The line items are hard-deleted. The header is only soft-deleted, "
        "so the evidence behind a deleted adjustment would be gone while the "
        "ticket itself remains.")


def test_it_happens_inside_the_same_transaction():
    """A half-applied delete is the failure this whole block guards against."""
    block = _delete_block()
    begin = block.index('run_query("BEGIN")')
    update = block.index("UPDATE ADJUSTMENT_APP.ADJ_LINE_ITEM_JSON")
    commit = block.index('run_query("COMMIT")')
    assert begin < update < commit, (
        "The line-item update is outside the delete transaction, so a "
        "failure could leave the header deleted and its rows live, or the "
        "reverse.")
    assert 'run_query("ROLLBACK")' in block


def test_it_only_touches_this_adjustment():
    block = _delete_block()
    stmt = block[block.index("UPDATE ADJUSTMENT_APP.ADJ_LINE_ITEM_JSON"):]
    stmt = stmt[:stmt.index('"""', stmt.index("SET IS_DELETED"))]
    assert "WHERE ADJ_ID = '{_aid}'" in stmt, (
        "The update is not scoped to the adjustment being deleted.")


def test_it_does_not_rewrite_rows_already_deleted():
    block = _delete_block()
    stmt = block[block.index("UPDATE ADJUSTMENT_APP.ADJ_LINE_ITEM_JSON"):]
    stmt = stmt[:stmt.index('"""', stmt.index("SET IS_DELETED"))]
    assert "AND IS_DELETED = FALSE" in stmt, (
        "Re-deleting should touch no rows, so a repeated delete is a no-op.")


def test_the_engine_still_filters_on_the_flag():
    """The soft delete only works because the read side honours it."""
    views = os.path.join(os.path.dirname(__file__), "..", "..",
                         "streamlit", "adjustment_engine", "sql",
                         "15_direct_frtb_upload.sql")
    with open(views, encoding="utf-8") as fh:
        src = fh.read()
    assert src.count("j.IS_DELETED = FALSE") >= 3, (
        "The enriched views no longer filter deleted line items, which makes "
        "the soft delete meaningless.")
