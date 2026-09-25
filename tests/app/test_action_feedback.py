"""Every action on the Adjustments page must show it is working.

Marcos, 2026-09-24: "when I click in delete a adjustment. I don't see any
message that is processing. it is strange how the page works."

The page had no spinner anywhere. Clicking Delete, Retry, Recall or Clone
did several round trips and then repainted, with nothing in between, so the
app looked hung. Delete is the worst of them: a multi-statement transaction
plus the report hand-off procedure.

Streamlit reruns after each action, so the feedback has to wrap the WORK,
not the rerun. These tests assert exactly that, because wrapping the rerun
instead is the natural mistake and shows nothing.
"""
import os
import re

import pytest

PAGE = os.path.join(os.path.dirname(__file__), "..", "..", "streamlit", "adjustment_engine", "pages",
                    "2_Adjustments.py")


def _source():
    with open(PAGE, encoding="utf-8") as fh:
        return fh.read()


def _handler(button_key):
    """The code between a button's `if st.button(... key=...)` and its rerun."""
    src = _source()
    i = src.index(button_key)
    start = src.rindex("if st.button(", 0, i)
    # The handler ends at its rerun call (either flavour).
    m = re.search(r"\n\s+(safe_)?_?rerun\(\)", src[start:])
    end = start + (m.end() if m else 2000)
    return src[start:end]


# ── Each action reports that it is working ──────────────────────────────────

@pytest.mark.parametrize("key,word", [
    ('key=f"del_{adj_id}"', "Deleting"),
    ('key=f"retry_{adj_id}"', "retry"),
    ('key=f"recall_{adj_id}"', "Recalling"),
    ('key=f"clone_btn_{adj_id}"', "Cloning"),
])
def test_the_action_shows_progress(key, word):
    block = _handler(key)
    assert "st.spinner(" in block, (
        "The " + word + " action runs with no feedback. The click does "
        "several round trips and then repaints, which reads as a hung page.")
    assert word.lower() in block.lower(), (
        "The spinner should name what is happening, not just spin.")


def test_delete_says_it_is_also_refreshing_reports():
    """Delete now queues a report refresh, which is most of the wait."""
    block = _handler('key=f"del_{adj_id}"')
    assert "reports" in block.lower(), (
        "Delete queues the downstream hand-off, so the wait is partly the "
        "report refresh. Saying so explains why it takes a moment.")


# ── The feedback must wrap the work ─────────────────────────────────────────

def test_the_spinner_wraps_the_work_not_the_rerun():
    """A spinner around the rerun shows nothing: the rerun repaints at once."""
    block = _handler('key=f"del_{adj_id}"')
    spinner = block.index("st.spinner(")
    work = block.index("_do_delete()")
    assert spinner < work, "the spinner must open before the work starts"
    assert "_rerun()" not in block[spinner:work], (
        "The rerun is inside the spinner. It has to come after the work, "
        "outside it, or the spinner never appears.")


# ── Bulk retry gets a progress bar, not a spinner ───────────────────────────

def test_bulk_retry_reports_how_far_along_it_is():
    """Known-length loop, two statements per row — a spinner is not enough."""
    src = _source()
    block = src[src.index('key="bulk_retry_btn"'):]
    block = block[:block.index("_rerun()")]
    assert "st.progress(" in block, (
        "Bulk retry loops over every failed row doing two statements each. "
        "A 40-row retry is 80 round trips; the user needs to see progress, "
        "not just that something is happening.")
    assert "of {_total}" in block, "the progress text must show the total"


def test_the_bulk_progress_bar_advances_inside_the_loop():
    src = _source()
    block = src[src.index('key="bulk_retry_btn"'):]
    block = block[:block.index("_rerun()")]
    loop = block.index("for _i, (_, _fr) in enumerate(")
    tick = block.index("_bar.progress(_i / _total")
    close = block.index("_bar.empty()")
    assert loop < tick < close, (
        "The bar must advance inside the loop and be cleared after it, or it "
        "either never moves or is left on screen.")
