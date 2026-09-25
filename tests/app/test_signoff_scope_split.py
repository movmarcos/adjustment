"""Every scope signs off on its own — an FRTB sign-off is not a DRC one.

Marcos, 2026-09-25: "there is an issue with the sign-off adjustment check.
If FRTB is signed off you are considering FRTBRRAO and FRTBDRC, now they
have their own sign-off process, they can be split." And, on the spelling:
"everything in our database is FRTB and not FRTBSBM, it is just in the UI
that we translate FRTB to FRTBSBM."

So the rule is simply EXACT match on the process-type code, everywhere.
FRTBSBM is a label this app prints; no table, no feed and no procedure
stores it, so nothing needs to match it.

THE BUG was in the New Adjustment page, which matched the upstream feed
with `PROCESS_TYPE IN ('FRTB', <scope>)`. A signed-off FRTB made the page
report DRC and RRAO as signed off too — and the page then disagreed with
SP_SUBMIT_ADJUSTMENT, which had always matched exactly. Users saw "signed
off" on a COB the engine would have accepted.

The three procedures were already right and are untouched; these tests pin
all four to the same rule so they cannot drift apart again.
"""
import os
import re

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
APP = os.path.join(ROOT, "streamlit", "adjustment_engine")
SQL = os.path.join(APP, "sql")

PAGE = os.path.join(APP, "pages", "1_New_Adjustment.py")
PROCS = ("03_sp_submit_adjustment.sql", "14_sp_submit_direct_batch.sql",
         "10_sp_signoff_sync.sql")
FRTB_SCOPES = ("FRTB", "FRTBDRC", "FRTBRRAO")


def _source(path):
    with open(path, encoding="utf-8") as fh:
        return fh.read()


# ── The page, where the bug was ──────────────────────────────────────────────

@pytest.fixture(scope="module")
def page():
    return _source(PAGE)


def test_the_page_no_longer_fans_an_frtb_signoff_out(page):
    assert "IN ('FRTB', '{esc_scope}')" not in page, (
        "the reported bug is back: an upstream FRTB row would make the page "
        "report FRTBDRC and FRTBRRAO as signed off")
    assert 'if esc_scope in ("FRTB", "FRTBDRC", "FRTBRRAO"):' not in page


def test_the_page_matches_the_scope_exactly_in_all_three_lookups(page):
    """The app table, the upstream feed and the REOPENED override.

    All three, or the parts disagree: a re-open recorded for one scope would
    fail to clear a feed row the page had matched more widely.
    """
    body = page[page.index("def _signoff_state("):page.index("def _request_reopen(")]
    assert body.count("= '{esc_scope}'") == 3


def test_the_page_and_the_engine_make_the_same_match(page):
    engine = _source(os.path.join(SQL, "03_sp_submit_adjustment.sql"))
    assert """pt_match = f"UPPER(u.PROCESS_TYPE) = '{pt_esc}'\"""" in engine
    assert """pt_match = f"UPPER(u.PROCESS_TYPE) = '{esc_scope}'\"""" in page


# ── The procedures were already right; keep them that way ────────────────────

def _scope_predicates(sql_file):
    """Just the expression deciding which feed rows count as the scope.

    Narrow on purpose: elsewhere these files legitimately list the three
    FRTB codes together — 03 carries PIPELINE_TYPES (one task processes all
    three, so they serialise) and 10 names every scope in its map. Neither
    is the sign-off match.
    """
    src = _source(os.path.join(SQL, sql_file))
    if sql_file == "10_sp_signoff_sync.sql":
        block = src[src.index("SCOPE_MATCH = {"):]
        return block[:block.index("}") + 1]
    return "\n".join(ln for ln in src.splitlines() if "pt_match =" in ln)


@pytest.mark.parametrize("sql_file", PROCS)
def test_no_procedure_fans_an_frtb_signoff_out(sql_file):
    code = _scope_predicates(sql_file)
    assert code.strip(), sql_file + ": no scope predicate found to check"
    assert not re.search(r"IN \('FRTB'", code), (
        sql_file + " builds an IN-list that always includes FRTB")
    for a in FRTB_SCOPES:
        for b in FRTB_SCOPES:
            if a != b:
                assert f"'{a}', '{b}'" not in code, (
                    sql_file + f" pairs {a} with {b} in its sign-off match")


def test_the_sync_procedure_matches_each_scope_on_its_own():
    block = _scope_predicates("10_sp_signoff_sync.sql")
    for scope in FRTB_SCOPES:
        assert f"UPPER(u.PROCESS_TYPE) = '{scope}'" in block, (
            scope + " is not matched exactly by the sync procedure")


@pytest.mark.parametrize("sql_file", ["03_sp_submit_adjustment.sql",
                                      "14_sp_submit_direct_batch.sql"])
def test_the_engine_and_the_batch_procedure_match_exactly(sql_file):
    assert "UPPER(u.PROCESS_TYPE) = '{pt_esc}'" in _scope_predicates(sql_file)


# ── What the split must NOT touch ────────────────────────────────────────────

def test_the_processing_pipeline_grouping_is_deliberately_untouched():
    """A guard against 'fixing' the wrong map.

    PIPELINE_TYPES groups FRTB/FRTBDRC/FRTBRRAO because ONE task processes
    all three and they must not run concurrently over the same data. That
    is not sign-off, and splitting it would change how adjustments
    serialise. If it ever should change, it needs its own reason.
    """
    src = _source(os.path.join(SQL, "03_sp_submit_adjustment.sql"))
    block = src[src.index("PIPELINE_TYPES = {"):]
    block = block[:block.index("}") + 1]
    assert "'FRTBDRC':     ['FRTB', 'FRTBDRC', 'FRTBRRAO']" in block
    assert "used for blocking checks at submit time" in src


def test_frtbsbm_is_a_label_and_never_a_stored_code():
    """The UI spells FRTB as FRTBSBM; nothing stores or matches that word."""
    for path in (PAGE, os.path.join(SQL, "03_sp_submit_adjustment.sql"),
                 os.path.join(SQL, "10_sp_signoff_sync.sql"),
                 os.path.join(SQL, "14_sp_submit_direct_batch.sql")):
        src = _source(path)
        assert "'FRTBSBM'" not in src, (
            os.path.basename(path) + " treats FRTBSBM as a code. Every code "
            "in the database is FRTB; FRTBSBM exists only in the UI "
            "(utils/styles.scope_label).")


# ── The copy users read ──────────────────────────────────────────────────────

def test_the_scope_tooltip_no_longer_claims_frtb_covers_the_others():
    styles = _source(os.path.join(APP, "utils", "styles.py"))
    assert "FRTB covers FRTBDRC and FRTBRRAO" not in styles
    assert "own sign-off" in styles


def test_the_documentation_says_the_three_sign_off_separately():
    docs = _source(os.path.join(APP, "pages", "7_Documentation.py"))
    assert "one FRTB entry covers FRTBDRC and FRTBRRAO" not in docs
    assert "separately" in docs and "FRTBDRC" in docs
