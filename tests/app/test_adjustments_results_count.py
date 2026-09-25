"""The Adjustments grid must count and reach everything that matches.

Two reports on 2026-09-24, from the same session:

  "I deleted 4 of 5 and it was still showing 5 in that top part of the grid."
  "the grid is not good, there is limitation to show the grid."

Both came from the same design. The query fetched 200 rows, pandas hid the
soft-deleted ones afterwards, and the header counted the fetched frame. So
the total included rows the user had just deleted, and "200" silently meant
"at least 200" with no way to see how many more there were or to reach them.

Now: deleted rows are excluded in SQL, the total is a server-side COUNT of
everything matching, and the list is paged.

The ordering matters and is asserted below. Filtering deleted rows in SQL is
what lets the count be a real count and a page be a full page — hide them
after a LIMIT and a page of 50 can render 12.
"""
import os
import re

import pytest

PAGE = os.path.join(os.path.dirname(__file__), "..", "..", "streamlit", "adjustment_engine", "pages",
                    "2_Adjustments.py")


def _source():
    with open(PAGE, encoding="utf-8") as fh:
        return fh.read()


def _rendered(text):
    """Join Python's implicit string concatenation.

    Captions are split across literals to fit the line, so a phrase the USER
    sees may not exist contiguously in the source. Asserting on the raw
    source would fail for a formatting reason, or pass only while the
    wrapping happens to line up.
    """
    return re.sub(r'"\s*\n\s*"', "", text)


# ── The count must mean what a person reads it to mean ──────────────────────

def test_the_headline_is_not_the_fetched_frame():
    src = _source()
    assert "total = len(df_adjs)" not in src, (
        "The header counts the fetched page, which is capped and used to "
        "include rows the user had just deleted.")


def test_the_total_comes_from_a_server_side_count():
    src = _source()
    assert "SELECT COUNT(*) AS N" in src, (
        "Without a COUNT over the same filter, the header can only ever "
        "report the size of the page it fetched.")
    assert "match_total" in src


def test_an_unavailable_count_is_not_shown_as_a_number():
    src = _source()
    assert 'match_total = None' in src and '"?" if match_total is None' in src, (
        "A failed count must render as unknown, not as a confident 0 or as "
        "the page size.")


# ── Deleted rows leave in SQL, before the limit ─────────────────────────────

def test_deleted_rows_are_excluded_in_sql():
    src = _source()
    assert 'where_clauses.append("COALESCE(IS_DELETED, FALSE) = FALSE")' in src, (
        "Deleted rows must be filtered by the query. Hiding them in pandas "
        "after a LIMIT is what made the total wrong and pages short.")


def test_pandas_no_longer_hides_rows_after_the_fetch():
    src = _source()
    assert "df_adjs[~is_del]" not in src, (
        "Rows are still being dropped after the fetch, so the page size and "
        "the count disagree with what is rendered.")


def test_choosing_a_deleted_status_still_shows_them():
    """Filtering FOR deleted rows must override the checkbox."""
    src = _source()
    assert "_wants_deleted = bool(set(filter_status or []) & _DELETEDISH)" in src
    assert "include_deleted = bool(show_deleted or _wants_deleted)" in src, (
        "Picking the Deleted status would otherwise match rows and then have "
        "them filtered straight back out, showing an empty grid.")


def test_the_exclusion_is_applied_before_the_count_and_the_page():
    src = _source()
    excl = src.index('COALESCE(IS_DELETED, FALSE) = FALSE')
    count = src.index("SELECT COUNT(*) AS N")
    fetch = src.index("LIMIT {int(page_size)} OFFSET")
    assert excl < count < fetch, (
        "The deleted filter must be in place before the count and the page "
        "query, or they measure and fetch different row sets.")


# ── Everything must be reachable ────────────────────────────────────────────

def test_the_query_is_paged_not_capped():
    src = _source()
    assert "LIMIT {int(page_size)} OFFSET {int(page_no * page_size)}" in src, (
        "A bare LIMIT with no OFFSET means older adjustments cannot be "
        "reached at all except by narrowing filters until they fit.")


def test_there_are_controls_to_change_page():
    src = _source()
    for key in ('key="adj_prev"', 'key="adj_next"'):
        assert key in src, "no " + key + " control"
    assert "Page {page_no + 1} of {page_count}" in src, (
        "Paging without telling the user where they are is worse than not "
        "paging.")


def test_the_page_resets_when_the_filters_change():
    src = _source()
    assert '_filter_sig' in src and 'st.session_state["_adj_page"] = 0' in src, (
        "Changing a filter while on page 4 would otherwise show an empty "
        "page and look like no results.")


def test_the_page_is_clamped_to_the_last_page():
    src = _source()
    assert "min(int(st.session_state.get(\"_adj_page\", 0)), page_count - 1)" in src, (
        "Deleting rows can shrink the result set below the current page; "
        "without clamping the grid goes blank.")


def test_the_page_size_is_the_users_choice():
    src = _source()
    assert 'key="mw_page_size"' in src


def test_turning_a_page_does_not_throw_away_the_read_cache():
    """Paging changes no data, so there is nothing to invalidate."""
    src = _source()
    block = src[src.index('key="adj_prev"'):src.index('key="adj_next"') + 400]
    # Negative lookbehind: "safe_rerun()" contains "_rerun()" as a substring,
    # so a plain `in` check can never tell the two apart.
    assert not re.search(r"(?<!safe)_rerun\(\)", block), (
        "Turning a page calls the cache-busting rerun, so every other read "
        "on the page is re-queried for nothing.")
    assert "safe_rerun()" in block


# ── The user is told what is hidden ─────────────────────────────────────────

def test_the_caption_says_deleted_rows_are_hidden():
    src = _rendered(_source())
    assert "Deleted adjustments are hidden" in src
    assert "Show deleted" in src, (
        "Saying rows are hidden without saying how to see them just moves "
        "the confusion.")
