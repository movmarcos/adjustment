"""The Adjustments page must actually render.

Every other test for this page reads its source. None of them ran it, so the
2026-09-24 rework (server-side count, paging, status strip, deep link) could
have shipped a NameError and no test would have noticed.

This drives the real page through AppTest against a fake session, on the
paths that matter: empty, populated, paged, and the query-failure branch
where several names are only assigned in an `except`.
"""
import os
import sys

import pandas as pd
import pytest

APP = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, APP)

from streamlit.testing.v1 import AppTest
import utils.snowflake_conn as sc

PAGE = os.path.join(APP, "pages", "2_Adjustments.py")

COLS = ["ADJ_ID", "DIMENSION_ADJ_ID", "COBID", "PROCESS_TYPE",
        "ADJUSTMENT_TYPE", "RUN_STATUS", "ENTITY_CODE", "DEPARTMENT_CODE",
        "BOOK_CODE", "SUBMITTED_BY", "SUBMITTED_AT", "IS_DELETED",
        "SCALE_FACTOR", "REASON", "ERRORMESSAGE", "RECORD_COUNT",
        "SOURCE_COBID", "ADJUSTMENT_CATEGORY", "PROCESS_DATE"]


def _rows(n, status="Processed"):
    return pd.DataFrame([{
        "ADJ_ID": f"adj-{i:04d}", "DIMENSION_ADJ_ID": 1000 + i,
        "COBID": 20260420, "PROCESS_TYPE": "VaR", "ADJUSTMENT_TYPE": "Scale",
        "RUN_STATUS": status, "ENTITY_CODE": "MUSI", "DEPARTMENT_CODE": "TRD",
        "BOOK_CODE": "BK1", "SUBMITTED_BY": "someone@mufg.com",
        "SUBMITTED_AT": pd.Timestamp("2026-04-20 10:00:00"),
        "IS_DELETED": False, "SCALE_FACTOR": 1.05, "REASON": "why",
        "ERRORMESSAGE": None, "RECORD_COUNT": 10, "SOURCE_COBID": 20260420,
        "ADJUSTMENT_CATEGORY": "Booking Error",
        "PROCESS_DATE": pd.Timestamp("2026-04-20 10:05:00"),
    } for i in range(n)], columns=COLS)


def _install(monkeypatch, total=3, rows=3, fail=False):
    """Fake every read the page performs."""
    def _df(sql, *a, **k):
        if fail:
            raise RuntimeError("warehouse unavailable")
        s = " ".join(str(sql).split()).upper()
        # Order matters: the status-mix query ALSO selects COUNT(*) AS N, so
        # the GROUP BY must be recognised first or it gets the scalar total.
        if "GROUP BY RUN_STATUS" in s:
            return pd.DataFrame([{"RUN_STATUS": "Processed", "N": total},
                                 {"RUN_STATUS": "Failed", "N": 1}])
        if "COUNT(*) AS N" in s:
            return pd.DataFrame([{"N": total}])
        if "SELECT DISTINCT COBID" in s:
            return pd.DataFrame([{"COBID": 20260420, "ENTITY_CODE": "MUSI",
                                  "DEPARTMENT_CODE": "TRD",
                                  "SUBMITTED_BY": "someone@mufg.com"}])
        if "VW_MY_WORK" in s:
            return _rows(rows)
        return pd.DataFrame()

    monkeypatch.setattr(sc, "run_query_df_cached", _df)
    monkeypatch.setattr(sc, "run_query_df", _df)
    monkeypatch.setattr(sc, "run_query", lambda *a, **k: [])
    monkeypatch.setattr(sc, "current_user_name", lambda: "someone@mufg.com")
    monkeypatch.setattr(sc, "get_session", lambda: None)


def _run(monkeypatch, **kw):
    _install(monkeypatch, **kw)
    at = AppTest.from_file(PAGE, default_timeout=120).run()
    assert not at.exception, at.exception
    return at


# ── It renders ──────────────────────────────────────────────────────────────

def test_it_renders_with_rows(monkeypatch):
    _run(monkeypatch, total=3, rows=3)


def test_it_renders_with_no_rows(monkeypatch):
    _run(monkeypatch, total=0, rows=0)


def test_it_renders_when_every_read_fails(monkeypatch):
    """match_total, page_no, page_count and include_deleted are only assigned
    in the except branch — a missing one is a NameError on a bad day."""
    _run(monkeypatch, fail=True)


# ── The new controls appear when they should ────────────────────────────────

def test_paging_controls_appear_when_there_is_more_than_one_page(monkeypatch):
    at = _run(monkeypatch, total=500, rows=200)
    keys = [b.key for b in at.button]
    assert "adj_prev" in keys and "adj_next" in keys, keys


def test_paging_controls_are_absent_for_a_single_page(monkeypatch):
    at = _run(monkeypatch, total=3, rows=3)
    keys = [b.key for b in at.button]
    assert "adj_prev" not in keys, (
        "Paging controls on a one-page result are pure noise.")


def test_the_status_strip_renders_a_chip_per_status(monkeypatch):
    at = _run(monkeypatch, total=3, rows=3)
    keys = [b.key for b in at.button]
    assert "mix_Processed" in keys and "mix_Failed" in keys, keys


def test_clicking_a_status_chip_filters_to_it(monkeypatch):
    at = _run(monkeypatch, total=3, rows=3)
    at.button(key="mix_Failed").click().run()
    assert not at.exception, at.exception
    assert at.session_state["mw_status"] == ["Failed"]


def test_clicking_the_active_status_chip_clears_it(monkeypatch):
    """The strip has to be able to undo itself."""
    at = _run(monkeypatch, total=3, rows=3)
    at.button(key="mix_Failed").click().run()
    at.button(key="mix_Failed").click().run()
    assert not at.exception, at.exception
    assert at.session_state["mw_status"] == []


def test_the_page_size_control_exists(monkeypatch):
    at = _run(monkeypatch, total=3, rows=3)
    assert any(s.key == "mw_page_size" for s in at.selectbox)


# ── The deep link ───────────────────────────────────────────────────────────

def test_the_adj_query_param_drives_find_by_id(monkeypatch):
    _install(monkeypatch, total=1, rows=1)
    at = AppTest.from_file(PAGE, default_timeout=120)
    at.query_params["adj"] = "adj-0000"
    at.run()
    assert not at.exception, at.exception
    assert at.session_state["mw_find"] == "adj-0000", (
        "?adj= must populate the Find-by-ID box so the list narrows to it.")


# ── The detail card (condensed 2026-09-24) ──────────────────────────────────
# render_adj_card was rewritten: the 10-row meta dataframe became an
# auto-fitting grid, the <br/> spacers went, and the timeline is capped. That
# is ~110 lines of new code inside a branch that only runs when a row is
# selected, so without this it would ship unrendered.

def _open_one(monkeypatch):
    """Render the page with a single matching row, which auto-opens it."""
    _install(monkeypatch, total=1, rows=1)
    at = AppTest.from_file(PAGE, default_timeout=120)
    at.query_params["adj"] = "adj-0000"
    at.run()
    assert not at.exception, at.exception
    return at


def test_the_detail_card_renders(monkeypatch):
    at = _open_one(monkeypatch)
    body = " ".join(m.value for m in at.markdown)
    assert "Adjustment Detail" in body or "ADJ" in body


def test_the_meta_is_a_grid_not_a_dataframe(monkeypatch):
    """The dataframe was ~400px of height for eleven short values."""
    at = _open_one(monkeypatch)
    body = " ".join(m.value for m in at.markdown)
    assert "grid-template-columns:repeat(auto-fit" in body, (
        "The meta panel is not the auto-fitting grid — if it went back to a "
        "dataframe, the card is tall again.")


def test_empty_meta_fields_are_omitted(monkeypatch):
    """'Omit, then omit again' — a dash per empty field is pure height."""
    at = _open_one(monkeypatch)
    body = " ".join(m.value for m in at.markdown)
    # The fixture leaves SOURCE_BOOK_CODE and START_DATE unset.
    assert "From book" not in body, (
        "An empty field is being rendered. Empty fields must be dropped, not "
        "shown as a dash.")
    assert "Target COB" in body, "populated fields must still render"


def test_the_shareable_link_is_offered(monkeypatch):
    at = _open_one(monkeypatch)
    body = " ".join(str(c.value) for c in at.code)
    assert "?adj=adj-0000" in body, (
        "The detail card should offer the link to this adjustment.")
