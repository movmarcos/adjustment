"""A scope whose preview matched nothing is skipped, not a blocker.

Marcos, 2026-09-23, after a six-scope Transfer Book where FRTBRRAO previewed
zero: "I should not block the adjustment to be created because there is one
scope with zero, just don't create the adjustment for it."

Before this, ANY zero scope disabled Submit for the whole draft. The user had
to deselect it by hand, which is fiddly with six pills and easy to get wrong.
Now the empty scope is dropped from the fan-out and every other scope is
still created.

The rule lives in utils/submit_fanout.scopes_to_submit so it can be tested
without a Streamlit session, the same way submit_fanout and transfer_jobs are.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from utils.submit_fanout import scopes_to_submit
from utils.transfer_book import transfer_jobs

SIX = ["VaR", "Stress", "Sensitivity", "FRTB", "FRTBDRC", "FRTBRRAO"]


# ── The reported case ────────────────────────────────────────────────────────

def test_the_reported_case_one_empty_scope_of_six():
    """FRTBRRAO empty, the other five still submit."""
    counts = {"VaR": 1200, "Stress": 340, "Sensitivity": 90,
              "FRTB": 7, "FRTBDRC": 4, "FRTBRRAO": 0}
    kept, skipped = scopes_to_submit(SIX, counts)
    assert kept == ["VaR", "Stress", "Sensitivity", "FRTB", "FRTBDRC"]
    assert skipped == ["FRTBRRAO"]


def test_the_reported_case_produces_five_transfer_jobs():
    """End to end with the Transfer fan-out: five submissions, not blocked."""
    counts = dict.fromkeys(SIX, 10)
    counts["FRTBRRAO"] = 0
    kept, _ = scopes_to_submit(SIX, counts)
    assert len(transfer_jobs(kept, [])) == 5


def test_skipping_survives_trade_code_fan_out():
    """Scope x trade: the empty scope contributes no jobs at all."""
    counts = {"VaR": 5, "Stress": 0}
    kept, skipped = scopes_to_submit(["VaR", "Stress"], counts)
    jobs = transfer_jobs(kept, ["T1", "T2"])
    assert jobs == [("VaR", "T1"), ("VaR", "T2")]
    assert skipped == ["Stress"]


# ── Ordering and shape ───────────────────────────────────────────────────────

def test_selection_order_is_preserved():
    counts = dict.fromkeys(SIX, 1)
    counts["Stress"] = 0
    kept, _ = scopes_to_submit(SIX, counts)
    assert kept == ["VaR", "Sensitivity", "FRTB", "FRTBDRC", "FRTBRRAO"]


def test_several_empty_scopes_are_all_skipped():
    counts = dict.fromkeys(SIX, 0)
    counts["VaR"] = 11
    counts["Stress"] = 22
    kept, skipped = scopes_to_submit(SIX, counts)
    assert kept == ["VaR", "Stress"]
    assert skipped == ["Sensitivity", "FRTB", "FRTBDRC", "FRTBRRAO"]


def test_nothing_is_skipped_when_every_scope_has_rows():
    kept, skipped = scopes_to_submit(SIX, dict.fromkeys(SIX, 3))
    assert kept == SIX
    assert skipped == []


def test_all_zero_reports_everything_skipped():
    """The caller blocks on this: there is nothing worth creating."""
    kept, skipped = scopes_to_submit(SIX, dict.fromkeys(SIX, 0))
    assert kept == []
    assert skipped == SIX


# ── Refusing to skip on weak evidence ────────────────────────────────────────

def test_no_preview_skips_nothing():
    """A stale or absent preview must not drop a scope.

    Losing an adjustment the user asked for is worse than creating an empty
    one, and the counts would not belong to the current filters anyway.
    """
    for empty in ({}, None):
        kept, skipped = scopes_to_submit(SIX, empty)
        assert kept == SIX
        assert skipped == []


def test_a_scope_missing_from_the_counts_is_kept():
    """Absence is not evidence of zero."""
    kept, skipped = scopes_to_submit(["VaR", "Stress"], {"VaR": 4})
    assert kept == ["VaR", "Stress"]
    assert skipped == []


def test_counts_for_unselected_scopes_are_ignored():
    """A leftover zero for a deselected scope must not affect the result."""
    kept, skipped = scopes_to_submit(["VaR"], {"VaR": 5, "FRTBRRAO": 0})
    assert kept == ["VaR"]
    assert skipped == []


@pytest.mark.parametrize("count", [1, 0.5, -3])
def test_only_exactly_zero_is_skipped(count):
    kept, _ = scopes_to_submit(["VaR"], {"VaR": count})
    assert kept == ["VaR"]


def test_an_unknown_count_is_kept_not_skipped():
    """A scope whose per-scope preview failed carries None, not 0.

    Truthiness would skip it, silently dropping an adjustment because a
    preview query errored. Only a count that is actually zero may skip.
    """
    kept, skipped = scopes_to_submit(["VaR", "Stress"],
                                     {"VaR": 5, "Stress": None})
    assert kept == ["VaR", "Stress"], (
        "A scope with an unknown row count must still be submitted — we do "
        "not know that it is empty.")
    assert skipped == []


def test_empty_selection_returns_empty():
    assert scopes_to_submit([], {"VaR": 0}) == ([], [])
    assert scopes_to_submit(None, {}) == ([], [])


def test_falsy_entries_in_the_selection_are_dropped():
    kept, _ = scopes_to_submit(["VaR", None, "", "Stress"], {})
    assert kept == ["VaR", "Stress"]


def test_the_helper_does_not_mutate_its_inputs():
    selected = list(SIX)
    counts = dict.fromkeys(SIX, 0)
    counts["VaR"] = 1
    scopes_to_submit(selected, counts)
    assert selected == SIX
    assert counts["Stress"] == 0
