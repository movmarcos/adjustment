"""Tests for the canonical server-rendered .mgrid HTML grid.

These lock in the invariants behind every historical 'white grid' bug:
- pure HTML output (no st.dataframe canvas — blank white box on SiS 1.26)
- single-line HTML (a newline ends Streamlit's raw-HTML block: c360960)
- no raw '$' (Streamlit runs KaTeX over $...$ even inside HTML: e4927e9)
- cell content is HTML-escaped
- wrapper uses max-height (no white gap under short tables)
"""
import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import streamlit
from utils import styles
from utils.styles import (render_df_table, render_activity_grid,
                          SELECTION_UNSUPPORTED)


@pytest.fixture
def captured(monkeypatch):
    """Capture everything the grid helpers pass to st.markdown."""
    out = []

    def fake_markdown(body, *a, **k):
        out.append(str(body))

    monkeypatch.setattr(streamlit, "markdown", fake_markdown)
    # styles.py binds `st` at module import; patch that reference too.
    monkeypatch.setattr(styles.st, "markdown", fake_markdown, raising=False)
    return out


def _html(out):
    grids = [h for h in out if "mgrid-wrap" in h]
    assert grids, f"no .mgrid HTML emitted; got: {out!r}"
    return grids[0]


def test_render_df_table_is_pure_single_line_html(captured):
    df = pd.DataFrame({
        "Comment": ["cost is $5\nover two lines", "<script>alert(1)</script>"],
        "Amount": [1234.5, None],
    })
    render_df_table(df, formats={"Amount": "{:,.2f}"})
    html = _html(captured)
    assert "\n" not in html                      # newline ends raw-HTML mode
    assert "$" not in html                       # KaTeX guard
    assert "&#36;5" in html
    assert "<script>" not in html                # escaped, not executed
    assert "&lt;script&gt;" in html
    assert "1,234.50" in html
    assert "—" in html                           # NaN placeholder
    assert 'class="r"' in html                   # numeric col right-aligned
    assert "max-height:440px" in html            # bounded, not fixed height


def test_render_df_table_color_and_highlight(captured):
    df = pd.DataFrame({"Status": ["Failed", "Processed"], "N": [1, 2]})
    render_df_table(
        df,
        color_cols={"Status": {"Failed": "#D50032"}},
        highlight=lambda r: r["Status"] == "Failed")
    html = _html(captured)
    assert "color:#D50032" in html
    assert "background-color:" in html


def test_render_df_table_max_rows_caption(captured, monkeypatch):
    caps = []
    monkeypatch.setattr(streamlit, "caption", lambda *a, **k: caps.append(a))
    monkeypatch.setattr(styles.st, "caption", lambda *a, **k: caps.append(a),
                        raising=False)
    df = pd.DataFrame({"A": range(50)})
    render_df_table(df, max_rows=10)
    html = _html(captured)
    assert html.count("<tr>") == 1 + 10          # header + 10 rows
    assert any("Showing the first 10" in str(c) for c in caps)


def test_activity_grid_html_and_selection_sentinel(captured):
    src = pd.DataFrame([{
        "DIMENSION_ADJ_ID": 101, "COBID": 20231231, "PROCESS_TYPE": "VaR",
        "RUN_STATUS": "Failed", "USERNAME": "alice",
    }])
    got = render_activity_grid(src, selectable=True)
    assert got is SELECTION_UNSUPPORTED
    html = _html(captured)
    assert "\n" not in html and "$" not in html
    assert "Adj ID" in html and "Failed" in html
    assert "font-weight:600" in html             # STATUS_STYLE applied inline


def test_activity_grid_empty_shows_info(monkeypatch):
    calls = []
    monkeypatch.setattr(streamlit, "info", lambda *a, **k: calls.append(a))
    monkeypatch.setattr(styles.st, "info", lambda *a, **k: calls.append(a),
                        raising=False)
    assert render_activity_grid(pd.DataFrame()) is None
    assert calls
