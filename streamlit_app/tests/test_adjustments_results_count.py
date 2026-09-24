"""The Adjustments grid header must count what you can actually see.

Reported 2026-09-24: filtered by Type, deleted four of five, and the header
still read 5. It said "Results - 1 of 5", because the denominator was
len(df_adjs) and the query RETURNS soft-deleted rows — Python hides them
afterwards. So the four just deleted were still being counted, and the
number people read as "how many are there" was the one number that had not
changed.

The headline is now what is on screen. Anything hidden is named in its own
line instead of folded into a denominator nobody can decompose.
"""
import os
import re

PAGE = os.path.join(os.path.dirname(__file__), "..", "pages",
                    "2_Adjustments.py")


def _source():
    with open(PAGE, encoding="utf-8") as fh:
        return fh.read()


def _rendered(text):
    """Join Python's implicit string concatenation.

    Long captions are split across literals to fit the line, so a phrase the
    USER sees ("use the COB filter") does not exist contiguously in the
    source. Asserting on the source would then fail for a formatting reason,
    or worse, pass only while the wrapping happens to line up.
    """
    return re.sub(r'"\s*\n\s*"', "", text)


def test_the_headline_count_is_not_the_unfiltered_frame():
    src = _source()
    assert "total = len(df_adjs)" not in src, (
        "The header counts df_adjs, which still contains the rows the user "
        "just deleted. That is the number they read as 'how many are left'.")


def test_the_headline_shows_the_visible_count():
    src = _source()
    assert 'section_title(f"Results — {shown}", "table")' in src, (
        "The header should show the number of rows actually displayed.")
    assert "shown = len(view_df)" in src


def test_hidden_deleted_rows_are_named_not_hidden_in_a_denominator():
    src = _source()
    assert "hidden_deleted = len(df_adjs) - shown" in src
    assert "deleted" in src[src.index("hidden_deleted ="):][:600].lower()
    assert "Show deleted" in src, (
        "The caption must tell the user how to see the hidden rows, or the "
        "count is just as opaque as before.")


def test_the_hidden_line_only_appears_when_something_is_hidden():
    src = _source()
    block = src[src.index("hidden_deleted = len(df_adjs) - shown"):]
    block = block[:block.index("with _rh2:")]
    assert "if hidden_deleted > 0:" in block, (
        "A '0 deleted hidden' line on every normal page is noise.")


def test_the_row_cap_message_does_not_reuse_the_deleted_inflated_count():
    """The cap is about the SQL LIMIT, so it reads the raw frame on purpose."""
    src = _source()
    block = src[src.index("hidden_deleted = len(df_adjs) - shown"):]
    block = block[:block.index("with _rh2:")]
    assert "if len(df_adjs) >= 200:" in block, (
        "The 200-row cap warns about the query limit, which applies to the "
        "rows the query returned, deleted ones included.")


def test_the_cap_message_says_how_to_see_more():
    src = _rendered(_source())
    assert "Narrow the filters" in src or "narrow the filters" in src
    assert "COB filter" in src, (
        "Telling someone the list is capped without telling them how to get "
        "at the rest just moves the confusion.")
