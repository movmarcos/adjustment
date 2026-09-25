"""Direct adjustments are switched off in production until sign-off.

Marcos, 2026-09-25: "if it is running in the PROD_RAPTOR database, the
Direct adjustment should be disabled. I will wait for the user sign-off."

Two properties carry the risk here, and they pull in opposite directions
from the submitter list next door, so both are pinned hard:

- **Fail CLOSED.** An unreadable config table in production leaves Direct
  OFF. The submitter list fails OPEN on purpose (a deploy fault should not
  stop the day's work); a feature switch whose whole purpose is to stay shut
  until someone decides otherwise must not be opened by an error.
- **Reversible without a release.** The answer comes from
  ADJ_APP_CONFIG.DIRECT_ADJUSTMENT_ENABLED, so the sign-off this is waiting
  for is an admin toggle, not a redeploy.

And production is detected from the LIVE database, not only from the
environment baked in at deploy time: an app deployed by an older deploy.py
carries no deploy_target.py and would otherwise claim to be DVLP while
running in production.
"""
import os
import re
import sys

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
APP = os.path.join(ROOT, "streamlit", "adjustment_engine")
sys.path.insert(0, APP)

import streamlit as st  # noqa: E402

import utils.snowflake_conn as sc  # noqa: E402

PAGE = os.path.join(APP, "pages", "1_New_Adjustment.py")
ADMIN = os.path.join(APP, "pages", "6_Admin.py")
SQL = os.path.join(APP, "sql")


def _source(path):
    with open(path, encoding="utf-8") as fh:
        return fh.read()


# ── Fakes ────────────────────────────────────────────────────────────────────

class _Session:
    """Answers CURRENT_DATABASE() and the config lookup; can raise on either."""

    def __init__(self, database="DVLP_RAPTOR_NEWADJ_4", flag=None,
                 config_raises=False, db_raises=False):
        self.database = database
        self.flag = flag
        self.config_raises = config_raises
        self.db_raises = db_raises
        self.queries = []

    def sql(self, query, *a, **k):
        self.queries.append(query)
        if "CURRENT_DATABASE" in query:
            if self.db_raises:
                raise RuntimeError("no session")
            return _Result([{"D": self.database}])
        if "ADJ_APP_CONFIG" in query:
            if self.config_raises:
                raise RuntimeError("table does not exist")
            return _Result([[self.flag]] if self.flag is not None else [])
        return _Result([])


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def collect(self):
        return self._rows


@pytest.fixture(autouse=True)
def clean_state():
    st.session_state.clear()
    yield
    st.session_state.clear()


@pytest.fixture
def gate(monkeypatch):
    def _install(**kw):
        session = _Session(**kw)
        monkeypatch.setattr(sc, "get_session", lambda: session)
        return session
    return _install


# ── Outside production nothing changes ───────────────────────────────────────

@pytest.mark.parametrize("db", ["DVLP_RAPTOR_NEWADJ_4", "TEST_RAPTOR_NEWADJ_4",
                                "RLSE_RAPTOR_NEWADJ_4"])
def test_direct_is_available_outside_production(gate, db, monkeypatch):
    monkeypatch.setattr(sc.config, "ENV", db[:4])
    gate(database=db)
    enabled, reason = sc.direct_adjustment_gate()
    assert enabled is True and reason == ""


def test_no_config_lookup_happens_outside_production(gate, monkeypatch):
    """The switch is a production concern; elsewhere it costs nothing."""
    monkeypatch.setattr(sc.config, "ENV", "DVLP")
    session = gate(database="DVLP_RAPTOR_NEWADJ_4")
    sc.direct_adjustment_gate()
    assert not any("ADJ_APP_CONFIG" in q for q in session.queries)


# ── In production it is off until switched on ────────────────────────────────

def test_direct_is_blocked_in_production_by_default(gate, monkeypatch):
    monkeypatch.setattr(sc.config, "ENV", "PROD")
    gate(database="PROD_RAPTOR_NEWADJ_4")
    enabled, reason = sc.direct_adjustment_gate()
    assert enabled is False
    assert "sign-off" in reason and "Admin page" in reason, (
        "the refusal must say what it is waiting for and who can lift it")


def test_any_prod_database_counts(gate, monkeypatch):
    monkeypatch.setattr(sc.config, "ENV", "PROD")
    gate(database="PROD_RAPTOR_SOMETHING_ELSE")
    assert sc.direct_adjustment_gate()[0] is False


def test_an_admin_can_switch_it_on_without_a_release(gate, monkeypatch):
    monkeypatch.setattr(sc.config, "ENV", "PROD")
    gate(database="PROD_RAPTOR_NEWADJ_4", flag="true")
    assert sc.direct_adjustment_gate() == (True, "")


@pytest.mark.parametrize("flag", ["TRUE", " true ", "True"])
def test_the_switch_ignores_case_and_padding(gate, monkeypatch, flag):
    monkeypatch.setattr(sc.config, "ENV", "PROD")
    gate(database="PROD_RAPTOR_NEWADJ_4", flag=flag)
    assert sc.direct_adjustment_gate()[0] is True


@pytest.mark.parametrize("flag", ["false", "", "yes", "1", "enabled", None])
def test_anything_but_true_leaves_it_off(gate, monkeypatch, flag):
    """Only the exact word opens a production gate — no clever coercion."""
    monkeypatch.setattr(sc.config, "ENV", "PROD")
    gate(database="PROD_RAPTOR_NEWADJ_4", flag=flag)
    assert sc.direct_adjustment_gate()[0] is False


# ── Fail closed ──────────────────────────────────────────────────────────────

def test_an_unreadable_config_table_leaves_direct_off_in_production(gate, monkeypatch):
    monkeypatch.setattr(sc.config, "ENV", "PROD")
    gate(database="PROD_RAPTOR_NEWADJ_4", config_raises=True)
    assert sc.direct_adjustment_gate()[0] is False, (
        "an error must not open the gate — this is the opposite of the "
        "submitter list, and deliberately so")


def test_a_stale_baked_environment_cannot_open_the_gate(gate, monkeypatch):
    """deploy_target.py missing (older deploy.py) → config.ENV says DVLP."""
    monkeypatch.setattr(sc.config, "ENV", "DVLP")
    gate(database="PROD_RAPTOR_NEWADJ_4")
    assert sc.is_production() is True
    assert sc.direct_adjustment_gate()[0] is False


def test_an_unreadable_database_name_falls_back_to_the_baked_environment(gate, monkeypatch):
    monkeypatch.setattr(sc.config, "ENV", "PROD")
    monkeypatch.setattr(sc.config, "DATABASE", "PROD_RAPTOR_NEWADJ_4")
    gate(db_raises=True)
    assert sc.is_production() is True


# ── Caching, and forgetting after an admin change ────────────────────────────

def test_the_answer_is_cached_and_can_be_forgotten(gate, monkeypatch):
    monkeypatch.setattr(sc.config, "ENV", "PROD")
    session = gate(database="PROD_RAPTOR_NEWADJ_4")
    assert sc.direct_adjustment_gate()[0] is False
    n = len([q for q in session.queries if "ADJ_APP_CONFIG" in q])
    sc.direct_adjustment_gate()
    assert len([q for q in session.queries if "ADJ_APP_CONFIG" in q]) == n

    session.flag = "true"
    assert sc.direct_adjustment_gate()[0] is False, "still the cached answer"
    sc.forget_direct_gate()
    assert sc.direct_adjustment_gate()[0] is True


# ── The page ─────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def page():
    return _source(PAGE)


def test_the_category_is_disabled_not_hidden(page):
    """A user who expects Direct should see why it is unavailable."""
    assert 'disabled=() if _direct_ok else {"Direct Adjustment"}' in page
    assert "_direct_ok, _direct_why = direct_adjustment_gate()" in page
    assert "st.info(_direct_why" in page


def test_the_pill_row_can_disable_an_option(page):
    body = page[page.index("def _pill_row("):page.index("def _sec(")]
    assert "disabled=opt in _off" in body


def test_a_draft_started_before_the_switch_cannot_keep_a_usable_form(page):
    block = page[page.index('elif wiz["category"] == "Direct Adjustment":'):]
    block = block[:block.index('elif wiz["category"] == "VaR Upload":')]
    assert "if _direct_ok:" in block and "render_direct_form()" in block
    assert "st.error(_direct_why" in block


def test_submit_is_blocked_and_says_why(page):
    assert 'direct_blocked = (not _direct_ok) and cat == "Direct Adjustment"' in page
    assert "or submit_blocked or direct_blocked" in page
    assert "Direct adjustments are switched off in " in page


# ── The server-side gate is the one that counts ──────────────────────────────

@pytest.mark.parametrize("sql_file", ["03_sp_submit_adjustment.sql",
                                      "14_sp_submit_direct_batch.sql"])
def test_both_submit_procedures_enforce_it(sql_file):
    src = _source(os.path.join(SQL, sql_file))
    assert "def _direct_blocked_reason(session):" in src, (
        sql_file + " does not check the gate. The page gate is a convenience; "
        "this procedure is a path every submission takes.")
    assert 'SELECT CURRENT_DATABASE() AS D' in src, (
        "the procedure runs inside the database it protects — ask it, do not "
        "configure it")
    assert "DIRECT_ADJUSTMENT_ENABLED" in src


@pytest.mark.parametrize("sql_file", ["03_sp_submit_adjustment.sql",
                                      "14_sp_submit_direct_batch.sql"])
def test_the_procedures_fail_closed_too(sql_file):
    src = _source(os.path.join(SQL, sql_file))
    body = src[src.index("def _direct_blocked_reason(session):"):]
    body = body[:body.index("\ndef ", 10)]
    # the only early return that opens the gate is an explicit 'true'
    assert body.count("return None") == 2
    assert 'str(rows[0][0] or "").strip().lower() == "true"' in body
    assert "except Exception:\n        pass" in body, (
        "a config read that raises must fall through to the refusal")


def test_the_engine_checks_before_it_writes():
    """A refused Direct submission must not already be in ADJ_HEADER."""
    src = _source(os.path.join(SQL, "03_sp_submit_adjustment.sql"))
    gate = src.index('if str(adjustment_type or "").strip().upper() == "DIRECT":')
    insert = src.index("INSERT INTO ADJUSTMENT_APP.ADJ_HEADER")
    assert gate < insert


def test_the_batch_procedure_checks_before_it_reads_the_batch():
    src = _source(os.path.join(SQL, "14_sp_submit_direct_batch.sql"))
    gate = src.index("_blocked = _direct_blocked_reason(session)")
    signoff = src.index("ents = session.sql(")
    assert gate < signoff


def test_the_frtb_file_flow_reaches_the_gate(page):
    """It submits through SP_SUBMIT_ADJUSTMENT typed 'Direct', not 'Upload'."""
    body = page[page.index("def _build_payload()"):page.index("def _completion_checks()")]
    assert '"adjustment_type":       "Direct" if _is_frtb_file_direct() else "Upload"' in body


# ── The admin switch ─────────────────────────────────────────────────────────

def test_the_admin_page_can_lift_it_after_signoff():
    src = _source(ADMIN)
    sect = src[src.index('section_title("Direct adjustments in production"'):]
    sect = sect[:sect.index("# TAB 2 — APPROVERS MANAGEMENT")]
    assert "DIRECT_ADJUSTMENT_ENABLED" in sect and "MERGE INTO" in sect
    assert "forget_direct_gate()" in sect, (
        "the cached answer must be dropped or the admin sees no change")
    assert "reload" in sect, "other users' sessions hold their own cached answer"


def test_the_admin_page_says_where_it_applies():
    src = _source(ADMIN)
    sect = src[src.index('section_title("Direct adjustments in production"'):]
    sect = sect[:sect.index("# TAB 2 — APPROVERS MANAGEMENT")]
    assert "not production" in sect, (
        "in DVLP/TEST the switch does nothing — say so, or an admin will "
        "toggle it and wonder why nothing happened")


# ── Documented ───────────────────────────────────────────────────────────────

def test_the_documentation_says_direct_is_not_live_in_production():
    docs = _source(os.path.join(APP, "pages", "7_Documentation.py"))
    assert "Not yet " in docs and "available in production" in docs
    start = docs.index('_KNOWLEDGE = """')
    end = docs.index('""".strip()', start)
    assert "NOT YET LIVE IN PRODUCTION" in docs[start:end], (
        "the assistant answers 'can I use Direct?' from this text")
