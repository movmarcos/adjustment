"""FRTBDRC and FRTBRRAO have their own sign-off; FRTB and FRTBSBM are one.

Marcos, 2026-09-25: "there is an issue with the sign-off adjustment check.
If FRTB is signed off you are considering FRTBRRAO and FRTBDRC, now they
have their own sign-off process, they can be split. FRTB and FRTBSBM are the
same. FRTBDRC and FRTBRRAO are now separated."

The bug: the New Adjustment page matched the upstream feed with
`PROCESS_TYPE IN ('FRTB', <scope>)`, so a signed-off SBM blocked a DRC or
RRAO adjustment nobody had signed off — and it disagreed with the engine,
which already matched exactly. A user saw "signed off" on the page for a COB
the procedure would have accepted.

Two rules, and they are NOT the same rule:

  * FRTB and FRTBSBM are two SPELLINGS of one scope.
  * FRTBDRC and FRTBRRAO are scopes of their OWN.

The rule lives in utils/signoff_scopes.py, and the three stored procedures
carry a copy because they run inside Snowflake and cannot import it. The
copies are asserted equal here — that is the whole reason this file exists.
"""
import ast
import os
import re
import sys

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
APP = os.path.join(ROOT, "streamlit", "adjustment_engine")
SQL = os.path.join(APP, "sql")
sys.path.insert(0, APP)

from utils.signoff_scopes import (SCOPE_ALIASES, feed_process_types,  # noqa: E402
                                  sql_in_list)

PAGE = os.path.join(APP, "pages", "1_New_Adjustment.py")
PROCS = ("03_sp_submit_adjustment.sql", "14_sp_submit_direct_batch.sql",
         "10_sp_signoff_sync.sql")

ALL_SCOPES = ("VaR", "Stress", "Sensitivity", "FRTB", "FRTBDRC", "FRTBRRAO")


def _source(path):
    with open(path, encoding="utf-8") as fh:
        return fh.read()


# ── The rule ─────────────────────────────────────────────────────────────────

def test_frtb_and_frtbsbm_are_one_scope():
    assert set(feed_process_types("FRTB")) == {"FRTB", "FRTBSBM"}
    assert set(feed_process_types("FRTBSBM")) == {"FRTB", "FRTBSBM"}


@pytest.mark.parametrize("scope", ["FRTBDRC", "FRTBRRAO"])
def test_the_frtb_sub_scopes_stand_alone(scope):
    """The reported bug: an FRTB sign-off must not reach these."""
    assert feed_process_types(scope) == (scope,)
    assert "FRTB'" not in sql_in_list(scope), (
        scope + " still matches an upstream FRTB row — a signed-off SBM "
        "would block a " + scope + " adjustment nobody signed off")


@pytest.mark.parametrize("scope", ["VaR", "Stress", "Sensitivity"])
def test_every_other_scope_matches_only_itself(scope):
    assert feed_process_types(scope) == (scope.upper(),)


def test_no_alias_ever_joins_two_different_scopes():
    """A guard on the guard: aliases may widen spelling, never family."""
    for key, names in SCOPE_ALIASES.items():
        assert key.upper() in {n.upper() for n in names}
        for other in ("FRTBDRC", "FRTBRRAO"):
            if key.upper() != other:
                assert other not in {n.upper() for n in names}, (
                    key + " is aliased to " + other + ", which merges two "
                    "scopes that sign off separately")


def test_case_and_padding_do_not_matter():
    assert feed_process_types("  frtbdrc ") == ("FRTBDRC",)
    assert set(feed_process_types("frtb")) == {"FRTB", "FRTBSBM"}


def test_an_unknown_scope_matches_only_itself():
    """Safe default: a scope nobody aliased must not inherit anything."""
    assert feed_process_types("ES") == ("ES",)


def test_the_sql_in_list_is_quoted_and_comma_separated():
    assert sql_in_list("FRTBDRC") == "'FRTBDRC'"
    assert sql_in_list("FRTB") in ("'FRTB', 'FRTBSBM'", "'FRTBSBM', 'FRTB'")


# ── Every copy agrees ────────────────────────────────────────────────────────

def _proc_aliases(sql_file):
    """SCOPE_ALIASES as written inside a stored procedure."""
    src = _source(os.path.join(SQL, sql_file))
    m = re.search(r"SCOPE_ALIASES = (\{.*?\})", src, re.S)
    assert m, sql_file + " carries no SCOPE_ALIASES map"
    return {k.upper(): tuple(sorted(n.upper() for n in v))
            for k, v in ast.literal_eval(m.group(1)).items()}


@pytest.mark.parametrize("sql_file", PROCS)
def test_the_procedure_copies_match_the_module(sql_file):
    mine = {k.upper(): tuple(sorted(n.upper() for n in v))
            for k, v in SCOPE_ALIASES.items()}
    assert _proc_aliases(sql_file) == mine, (
        sql_file + "'s SCOPE_ALIASES has drifted from "
        "utils/signoff_scopes.py. Change one, change all — they decide the "
        "same question on different sides of the wire.")


def _scope_predicates(sql_file):
    """Just the expression that decides which feed rows count as the scope.

    Narrow on purpose. The rest of these files legitimately lists the three
    FRTB codes together — 03 carries PIPELINE_TYPES (one task processes all
    three, so they serialise) and 10 iterates every scope to build its map.
    Neither is sign-off.
    """
    src = _source(os.path.join(SQL, sql_file))
    if sql_file == "10_sp_signoff_sync.sql":
        block = src[src.index("SCOPE_MATCH = {"):]
        return block[:block.index("}") + 1]
    return "\n".join(ln for ln in src.splitlines() if "pt_match =" in ln)


@pytest.mark.parametrize("sql_file", PROCS)
def test_no_procedure_fans_an_frtb_signoff_out_to_its_sub_scopes(sql_file):
    """The shape of the old bug, in SQL: IN ('FRTB', <the scope>)."""
    code = _scope_predicates(sql_file)
    assert code.strip(), sql_file + ": no scope predicate found to check"
    assert not re.search(r"IN \('FRTB',\s*'\{", code), (
        sql_file + " builds an IN-list that always includes FRTB")
    for bad in ("'FRTB', 'FRTBDRC'", "'FRTB','FRTBDRC'",
                "'FRTB', 'FRTBRRAO'", "'FRTB','FRTBRRAO'"):
        assert bad not in code, sql_file + " pairs FRTB with " + bad


@pytest.mark.parametrize("sql_file", PROCS)
def test_every_procedure_builds_its_predicate_from_the_alias_map(sql_file):
    """Behaviour, not spelling: the predicate comes from SCOPE_ALIASES."""
    assert "_scope_in_list(" in _scope_predicates(sql_file)


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


def test_the_sync_procedure_matches_each_scope_on_its_own():
    src = _source(os.path.join(SQL, "10_sp_signoff_sync.sql"))
    block = src[src.index("SCOPE_MATCH = {"):]
    block = block[:block.index("}") + 1]
    assert "_scope_in_list(s)" in block, (
        "SCOPE_MATCH should be built from SCOPE_ALIASES so the two cannot "
        "disagree inside the file")


# ── The page, where the bug was ──────────────────────────────────────────────

@pytest.fixture(scope="module")
def page():
    return _source(PAGE)


def test_the_page_no_longer_fans_frtb_out(page):
    assert "IN ('FRTB', '{esc_scope}')" not in page, (
        "the reported bug is back: an upstream FRTB row would block FRTBDRC "
        "and FRTBRRAO")
    assert 'if esc_scope in ("FRTB", "FRTBDRC", "FRTBRRAO"):' not in page


def test_the_page_asks_the_shared_module(page):
    assert "from utils.signoff_scopes import sql_in_list as _signoff_in_list" in page
    body = page[page.index("def _signoff_state("):page.index("def _request_reopen(")]
    assert body.count("_signoff_in_list(esc_scope)") == 3, (
        "all three lookups — the app table, the upstream feed and the "
        "REOPENED override — must use the same scope rule, or a re-open "
        "recorded for FRTB would not clear an FRTBSBM feed row")


def test_the_page_and_the_engine_now_agree(page):
    """They disagreed: the page over-blocked, the procedure accepted."""
    engine = _source(os.path.join(SQL, "03_sp_submit_adjustment.sql"))
    assert "UPPER(u.PROCESS_TYPE) IN ({_scope_in_list(pt_esc)})" in engine
    assert "UPPER(u.PROCESS_TYPE) IN ({_signoff_in_list(esc_scope)})" in page


# ── The copy users read ──────────────────────────────────────────────────────

def test_the_scope_tooltip_no_longer_claims_frtb_covers_the_others():
    styles = _source(os.path.join(APP, "utils", "styles.py"))
    assert "FRTB covers FRTBDRC and FRTBRRAO" not in styles


def test_the_documentation_says_the_three_sign_off_separately():
    docs = _source(os.path.join(APP, "pages", "7_Documentation.py"))
    assert "one FRTB entry covers FRTBDRC and FRTBRRAO" not in docs
    assert "separately" in docs and "FRTBDRC" in docs
