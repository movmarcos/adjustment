"""The Sign-Off status is a board, not a nine-column list.

Marcos, 2026-09-24: "the signoff status is also terrible. I see a big list in
the grid. I want something that I can identify the scope, cob and the entity
and status. the grid is not great visualisation."

He is right about the shape of the data. Sign-off is a matrix — COB by scope
by entity — and it was being rendered as one flat row per combination across
nine columns, so the four things anyone actually looks for were spread across
a wall of text.

The default view groups by COB, then scope, with one coloured chip per
entity. The table stays one radio click away for sorting and exporting,
because losing it would trade one complaint for another.

This file also RENDERS the page. Nothing did before, so the ~60 new lines
would have shipped unexercised — the same gap that let two real bugs into
the Adjustments rework earlier the same day.
"""
import os
import sys

import pandas as pd
import pytest

APP = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, APP)

from streamlit.testing.v1 import AppTest
import utils.snowflake_conn as sc

PAGE = os.path.join(APP, "pages", "5_Sign_Off.py")

STATUS_COLS = ["COBID", "PROCESS_TYPE", "ENTITY_CODE", "SUB_TYPE",
               "SIGN_OFF_STATUS", "SIGN_OFF_BY", "SIGN_OFF_TIMESTAMP",
               "SIGNOFF_SOURCE", "REOPEN_REASON", "REOPEN_REQUESTED_BY",
               "REOPEN_REQUESTED_AT", "REOPEN_APPROVED_BY"]


def _status_rows():
    def _r(cob, scope, ent, status):
        return {"COBID": cob, "PROCESS_TYPE": scope, "ENTITY_CODE": ent,
                "SUB_TYPE": None, "SIGN_OFF_STATUS": status,
                "SIGN_OFF_BY": "someone@mufg.com",
                "SIGN_OFF_TIMESTAMP": pd.Timestamp("2026-04-20 18:00:00"),
                "SIGNOFF_SOURCE": "FEED", "REOPEN_REASON": None,
                "REOPEN_REQUESTED_BY": None, "REOPEN_REQUESTED_AT": None,
                "REOPEN_APPROVED_BY": None}
    return pd.DataFrame([
        _r(20260420, "VaR", "MUSI", "SIGNED_OFF"),
        _r(20260420, "VaR", "MUSE", "OPEN"),
        _r(20260420, "Stress", "*", "SIGNOFF_REQUESTED"),
        _r(20260419, "VaR", "MUSI", "SIGNED_OFF"),
    ], columns=STATUS_COLS)


def _install(monkeypatch, rows=None):
    data = _status_rows() if rows is None else rows

    def _df(sql, *a, **k):
        s = " ".join(str(sql).split()).upper()
        if "SIGNOFF" in s or "SIGN_OFF" in s:
            if "HISTORY" in s or "CHANGED_AT" in s:
                return pd.DataFrame()
            return data
        return pd.DataFrame()

    monkeypatch.setattr(sc, "run_query_df", _df)
    monkeypatch.setattr(sc, "run_query_df_cached", _df)
    monkeypatch.setattr(sc, "run_query", lambda *a, **k: [])
    monkeypatch.setattr(sc, "current_user_name", lambda: "someone@mufg.com")
    monkeypatch.setattr(sc, "get_session", lambda: None)
    monkeypatch.setattr(sc, "signoff_access", lambda u: None)


def _run(monkeypatch, rows=None):
    _install(monkeypatch, rows)
    at = AppTest.from_file(PAGE, default_timeout=120).run()
    assert not at.exception, at.exception
    return at


def _body(at):
    return " ".join(m.value for m in at.markdown)


# ── It renders ──────────────────────────────────────────────────────────────

def test_the_page_renders(monkeypatch):
    _run(monkeypatch)


def test_it_renders_with_no_signoff_data(monkeypatch):
    _run(monkeypatch, rows=pd.DataFrame(columns=STATUS_COLS))


# ── The board is the default ────────────────────────────────────────────────

def test_the_board_is_the_default_view(monkeypatch):
    at = _run(monkeypatch)
    view = next((r for r in at.radio if r.key == "so_view"), None)
    assert view is not None, "no Board/Table switch"
    assert view.value == "Board", (
        "The table was the problem; the board has to be what you land on.")


def test_the_board_labels_the_cob_group(monkeypatch):
    """The COB filter follows the page's COB picker, so only that one shows."""
    at = _run(monkeypatch)
    body = _body(at)
    assert "COB 20260420" in body, (
        "Each COB must be its own labelled group, or the board is just "
        "another undifferentiated list.")
    assert "grid-template-columns:130px 1fr" in body, (
        "The scope-label / chips grid is missing, so the board collapsed "
        "back into a flat run of chips.")


def test_clearing_the_cob_filter_shows_every_cob_as_its_own_group(monkeypatch):
    # Two runs on purpose: the first lets the COB picker seed the filter
    # (the page deliberately follows it), the second clears it the way a user
    # does. Clearing before the first run is overwritten by that seeding.
    at = _run(monkeypatch)
    at.session_state["so_f_cob"] = []          # "empty = all COBs"
    at.run()
    assert not at.exception, at.exception
    body = _body(at)
    assert "COB 20260420" in body and "COB 20260419" in body, (
        "With no COB filter, every COB must appear as its own group.")


def test_the_board_shows_a_chip_per_entity(monkeypatch):
    at = _run(monkeypatch)
    body = _body(at)
    for entity in ("MUSI", "MUSE"):
        assert entity in body, "entity " + entity + " is missing from the board"


def test_the_chip_carries_the_detail_the_table_had(monkeypatch):
    """The board drops columns, so the detail has to survive somewhere."""
    at = _run(monkeypatch)
    body = _body(at)
    assert "submissions blocked" in body or "submissions allowed" in body, (
        "Chips must carry the status and whether submissions are blocked in "
        "their tooltip, or the board loses information the table had.")


def test_scope_names_use_display_labels(monkeypatch):
    at = _run(monkeypatch)
    assert "FRTBSBM" in _body(at) or "VaR" in _body(at)


# ── The table is still reachable ────────────────────────────────────────────

def test_the_table_is_still_available(monkeypatch):
    at = _run(monkeypatch)
    view = next(r for r in at.radio if r.key == "so_view")
    assert "Table" in view.options, (
        "Removing the table entirely trades one complaint for another: it is "
        "what you use to sort and export.")


def test_switching_to_the_table_works(monkeypatch):
    at = _run(monkeypatch)
    at.radio(key="so_view").set_value("Table").run()
    assert not at.exception, at.exception
