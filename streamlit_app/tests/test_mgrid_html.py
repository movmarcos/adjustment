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


def test_large_table_paginates_css_only(harness):
    md, buttons = harness
    render_df_table(pd.DataFrame({"A": range(50)}), key="k1")  # > 15 rows
    html = _grids(md)[0]
    # ALL rows ship in the HTML, chunked into 12-row tbodys; hidden radio
    # inputs + labels switch pages purely in the browser — NO Streamlit
    # buttons (a button pager forced a slow full rerun per click on SiS).
    assert buttons == []
    assert html.count("<tr>") == 1 + 50               # header + every row
    assert html.count("<tbody>") == 5                 # 50/12 → 5 pages
    assert html.count('type="radio"') == 5 and "checked" in html
    assert '<label for="k1_4">5</label>' in html
    assert "Rows 1–12 of 50" in html and "\n" not in html


def test_long_values_truncate_with_tooltip(harness):
    md, _ = harness
    long = "MARCOS.MAGRI@MUFGSECURITIES.COM"
    render_df_table(pd.DataFrame({"User": [long], "N": [1]}))
    html = _grids(md)[0]
    assert "…" in html                                # truncated display
    assert f'title="{long}"' in html                  # full value on hover
    assert f">{long}<" not in html


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


def test_color_cols_render_logs_style_pills(harness):
    md, _ = harness
    df = pd.DataFrame({"Status": ["Failed", "Processed"], "N": [1, 2]})
    render_df_table(df, color_cols={"Status": {"Failed": "#D50032"}},
                    highlight=lambda r: r["Status"] == "Failed")
    html = _grids(md)[0]
    # oval tinted chip identical to the Logs page _pill markup
    assert "border-radius:99px" in html
    assert "background:#D5003218" in html and "color:#D50032" in html
    assert "background-color:" in html               # row highlight kept


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
    assert "border-radius:99px" in html               # status/scope pills
    assert "color:#DC2626" in html                    # STATUS_COLORS Failed


def test_activity_grid_empty_shows_info(harness, monkeypatch):
    infos = []
    monkeypatch.setattr(streamlit, "info", lambda *a, **k: infos.append(a))
    monkeypatch.setattr(styles.st, "info", lambda *a, **k: infos.append(a),
                        raising=False)
    assert render_activity_grid(pd.DataFrame()) is None
    assert infos
