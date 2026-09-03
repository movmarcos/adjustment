"""Tests for the RESTORED pre-redesign grids (user request 2026-09-03:
'we had a quite decent grid except when we freeze the header').

- render_activity_grid: st.dataframe at a fixed height (Grid Lab style A).
- render_df_table: flowing .mgrid HTML, FULL values (no truncation), no
  pagination, escaped + $-guarded single-line output, no sticky header.
- render_grid: cells render AS AUTHORED (pills/entity chips preserved).
"""
import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import streamlit
from utils import styles
from utils.styles import (render_df_table, render_activity_grid, render_grid,
                          SELECTION_UNSUPPORTED)


@pytest.fixture
def harness(monkeypatch):
    """Capture st.markdown, st.dataframe and st.caption calls."""
    md, dfs, caps = [], [], []
    for mod in (streamlit, styles.st):
        monkeypatch.setattr(mod, "markdown",
                            lambda b, *a, **k: md.append(str(b)),
                            raising=False)
        monkeypatch.setattr(mod, "dataframe",
                            lambda d, *a, **k: dfs.append((d, k)),
                            raising=False)
        monkeypatch.setattr(mod, "caption",
                            lambda *a, **k: caps.append(a), raising=False)
    return md, dfs, caps


def _grids(md):
    return [h for h in md if "mgrid-wrap" in h]


def test_df_table_full_values_no_truncation_no_pagination(harness):
    md, dfs, _ = harness
    long = "MARCOS.MAGRI@MUFGSECURITIES.COM"
    render_df_table(pd.DataFrame({"User": [long] * 20, "N": range(20)}))
    html = _grids(md)[0]
    assert dfs == []                              # HTML, not canvas
    assert f">{long}<" in html                    # full value, no ellipsis
    assert "…" not in html
    assert 'type="radio"' not in html             # no pagination
    assert html.count("<tr>") == 1 + 20           # all rows in one table
    assert "position:sticky" not in html and "position: sticky" not in html


def test_df_table_escaping_and_markdown_guards(harness):
    md, _, _ = harness
    df = pd.DataFrame({"Comment": ["cost is $5\ntwo", "<script>x</script>"],
                       "Amount": [1234.5, None]})
    render_df_table(df, formats={"Amount": "{:,.2f}"})
    html = _grids(md)[0]
    assert "\n" not in html                       # raw-HTML block guard
    assert "$" not in html and "&#36;5" in html   # KaTeX guard
    assert "<script>" not in html and "&lt;script&gt;" in html
    assert "1,234.50" in html and "—" in html
    assert 'class="r"' in html                    # numeric right-aligned


def test_df_table_colour_and_highlight(harness):
    md, _, _ = harness
    df = pd.DataFrame({"Status": ["Failed", "OK"], "N": [1, 2]})
    render_df_table(df, color_cols={"Status": {"Failed": "#D50032"}},
                    highlight=lambda r: r["Status"] == "Failed")
    html = _grids(md)[0]
    assert "color:#D50032;font-weight:700" in html
    assert f"background:{styles.P['danger_lt']}" in html


def test_df_table_max_rows_caption(harness):
    md, _, caps = harness
    render_df_table(pd.DataFrame({"A": range(300)}), max_rows=200)
    assert _grids(md)[0].count("<tr>") == 1 + 200
    assert any("Showing the first 200" in str(c) for c in caps)


def test_render_grid_keeps_authored_markup_and_dividers(harness):
    md, _, _ = harness
    pill = '<span style="border-radius:99px">Stress</span>'
    render_grid(["COB", "Scope"],
                [{"divider": "Monday"}, [20260626, pill]],
                aligns=["right", "left"])
    html = _grids(md)[0]
    assert pill in html                           # pills NOT stripped
    assert 'class="mgrid-div"' in html and "Monday" in html
    assert '<td class="r">20260626</td>' in html


def test_render_grid_return_html_returns_string(harness):
    md, _, _ = harness
    out = render_grid(["A"], [["x"]], return_html=True)
    assert "mgrid-wrap" in out and md == []


def test_activity_grid_is_fixed_height_dataframe(harness):
    # USER'S EXPLICIT CHOICE (2026-09-03): Home/Adjustments list hundreds of
    # rows, so these two keep the fixed-height st.dataframe with internal
    # scroll; every other page uses the .mgrid HTML grid.
    md, dfs, _ = harness
    src = pd.DataFrame([{
        "DIMENSION_ADJ_ID": 101, "COBID": 20231231, "PROCESS_TYPE": "VaR",
        "RUN_STATUS": "Failed", "USERNAME": "alice",
    }])
    got = render_activity_grid(src, selectable=True)
    assert got is SELECTION_UNSUPPORTED
    assert _grids(md) == []                       # canvas, not HTML
    _, kwargs = dfs[0]
    assert kwargs.get("height") == 380
    assert kwargs.get("use_container_width") is True


def test_data_grid_small_uses_exact_html_no_filler(harness):
    from utils.styles import render_data_grid
    md, dfs, _ = harness
    render_data_grid(pd.DataFrame({"A": [1]}), height=380)
    assert dfs == []                              # no canvas → no filler rows
    assert _grids(md)[0].count("<tr>") == 1 + 1   # header + the single row


def test_data_grid_large_uses_capped_canvas(harness):
    from utils.styles import render_data_grid
    md, dfs, _ = harness
    render_data_grid(pd.DataFrame({"A": range(100)}), height=380)
    assert _grids(md) == []
    _, kwargs = dfs[0]
    assert kwargs.get("height") == 380            # fit (3538) capped at 380
    render_data_grid(pd.DataFrame({"A": range(16)}), height=380)
    _, kwargs = dfs[1]
    assert kwargs.get("height") == 35 * 17 + 3    # 16 rows fit exactly, no cap


def test_activity_grid_empty_shows_info(harness, monkeypatch):
    infos = []
    monkeypatch.setattr(streamlit, "info", lambda *a, **k: infos.append(a))
    monkeypatch.setattr(styles.st, "info", lambda *a, **k: infos.append(a),
                        raising=False)
    assert render_activity_grid(pd.DataFrame()) is None
    assert infos
