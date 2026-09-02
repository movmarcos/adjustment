"""Tests for the canonical 'dream grid' (user-specified 2026-09-02):
Logs-style .mgrid HTML, sized to its rows; up to 15 rows render whole,
beyond that 12 rows per page with a compact click-a-number pager.
Also locks the safety invariants of the HTML emitter: single-line output,
'$' neutralised (KaTeX), content HTML-escaped.
"""
import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import streamlit
from utils import styles
from utils.styles import (render_df_table, render_activity_grid,
                          SELECTION_UNSUPPORTED, GRID_PAGE_ROWS,
                          PAGINATE_OVER)


@pytest.fixture
def harness(monkeypatch):
    """Capture st.markdown + st.button; isolate session_state."""
    md, buttons = [], []

    def fake_markdown(body, *a, **k):
        md.append(str(body))

    def fake_button(label, *a, **k):
        buttons.append(str(label))
        return False

    for mod in (streamlit, styles.st):
        monkeypatch.setattr(mod, "markdown", fake_markdown, raising=False)
        monkeypatch.setattr(mod, "button", fake_button, raising=False)
    monkeypatch.setattr(styles.st, "session_state", {}, raising=False)
    return md, buttons


def _grids(md):
    return [h for h in md if "mgrid-wrap" in h]


def test_small_table_renders_whole_no_pager(harness):
    md, buttons = harness
    render_df_table(pd.DataFrame({"A": range(PAGINATE_OVER)}))  # 15 rows
    html = _grids(md)[0]
    assert html.count("<tr>") == 1 + PAGINATE_OVER   # header + all rows
    assert buttons == []                              # no pager


def test_large_table_paginates_at_12(harness):
    md, buttons = harness
    render_df_table(pd.DataFrame({"A": range(50)}))   # > 15 → paginate
    html = _grids(md)[0]
    assert html.count("<tr>") == 1 + GRID_PAGE_ROWS   # header + 12 rows
    # 50/12 → 5 pages; pager shows ‹ 1..5 › and a rows caption
    assert {"1", "2", "3", "4", "5"} <= set(buttons)
    assert any("Rows 1–12 of 50" in h for h in md)


def test_pager_second_page_slices_rows(harness):
    md, _ = harness
    styles.st.session_state["k2"] = 1                 # page 2 preselected
    render_df_table(pd.DataFrame({"A": range(50)}), key="k2")
    html = _grids(md)[0]
    assert ">12<" in html and ">23<" in html          # rows 12..23 shown
    assert ">0<" not in html


def test_html_is_single_line_escaped_and_dollar_safe(harness):
    md, _ = harness
    df = pd.DataFrame({"Comment": ["cost is $5\ntwo lines",
                                   "<script>alert(1)</script>"],
                       "Amount": [1234.5, None]})
    render_df_table(df, formats={"Amount": "{:,.2f}"})
    html = _grids(md)[0]
    assert "\n" not in html
    assert "$" not in html and "&#36;5" in html
    assert "<script>" not in html and "&lt;script&gt;" in html
    assert "1,234.50" in html and "—" in html
    assert 'class="r"' in html                        # numeric right-aligned


def test_color_and_highlight_inline_css(harness):
    md, _ = harness
    df = pd.DataFrame({"Status": ["Failed", "Processed"], "N": [1, 2]})
    render_df_table(df, color_cols={"Status": {"Failed": "#D50032"}},
                    highlight=lambda r: r["Status"] == "Failed")
    html = _grids(md)[0]
    assert "color:#D50032" in html and "background-color:" in html


def test_grid_pager_math(monkeypatch):
    from utils.styles import grid_pager
    monkeypatch.setattr(styles.st, "session_state", {}, raising=False)
    assert grid_pager(0, 12, key="t1") == (0, 1)
    assert grid_pager(120, 12, key="t2") == (0, 10)
    styles.st.session_state["t3"] = 99                 # stale page clamps
    assert grid_pager(50, 12, key="t3") == (4, 5)


def test_activity_grid_sentinel_and_status_colour(harness):
    md, _ = harness
    src = pd.DataFrame([{
        "DIMENSION_ADJ_ID": 101, "COBID": 20231231, "PROCESS_TYPE": "VaR",
        "RUN_STATUS": "Failed", "USERNAME": "alice",
    }])
    got = render_activity_grid(src, selectable=True)
    assert got is SELECTION_UNSUPPORTED
    html = _grids(md)[0]
    assert "Adj ID" in html and "Failed" in html
    assert "font-weight:600" in html                  # STATUS_STYLE inline


def test_activity_grid_empty_shows_info(harness, monkeypatch):
    infos = []
    monkeypatch.setattr(streamlit, "info", lambda *a, **k: infos.append(a))
    monkeypatch.setattr(styles.st, "info", lambda *a, **k: infos.append(a),
                        raising=False)
    assert render_activity_grid(pd.DataFrame()) is None
    assert infos
