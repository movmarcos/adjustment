"""Tests for the canonical grid renderers.

Grid Lab verdict (2026-09-02, tested live under the users' Menlo browser
isolation): fixed-height st.dataframe (style A) is the ONLY presentation
free of white blocks. These tests lock the renderers to that shape:
- grids go through st.dataframe with a bounded height (internal scroll,
  short pages) — never through flowing HTML tables;
- the truncation caption still appears past max_rows;
- selection stays on the fallback-picker contract (runtime is 1.22 < 1.35).
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
    """Capture every st.dataframe call the grid helpers make."""
    calls = []

    def fake_dataframe(data, *a, **k):
        calls.append((data, k))

    monkeypatch.setattr(streamlit, "dataframe", fake_dataframe)
    monkeypatch.setattr(styles.st, "dataframe", fake_dataframe, raising=False)
    return calls


def test_render_df_table_small_table_fits_content(captured):
    df = pd.DataFrame({"Comment": ["cost is $5", "plain"],
                       "Amount": [1234.5, None]})
    render_df_table(df, formats={"Amount": "{:,.2f}"}, height=300)
    assert len(captured) == 1
    _, kwargs = captured[0]
    # 2 rows fit in 35*(2+1)+3 = 108px — no empty filler rows below.
    assert kwargs.get("height") == 108
    assert kwargs.get("use_container_width") is True


def test_render_df_table_large_table_capped(captured):
    render_df_table(pd.DataFrame({"A": range(200)}), height=300)
    _, kwargs = captured[0]
    assert kwargs.get("height") == 300          # cap → internal scroll


def test_render_df_table_max_rows_caption(captured, monkeypatch):
    caps = []
    monkeypatch.setattr(streamlit, "caption", lambda *a, **k: caps.append(a))
    monkeypatch.setattr(styles.st, "caption", lambda *a, **k: caps.append(a),
                        raising=False)
    render_df_table(pd.DataFrame({"A": range(50)}), max_rows=10)
    data, _ = captured[0]
    # Styler wraps the truncated frame; count rows on the underlying data.
    frame = getattr(data, "data", data)
    assert len(frame) == 10
    assert any("Showing the first 10" in str(c) for c in caps)


def test_render_df_table_empty_shows_caption(captured, monkeypatch):
    caps = []
    monkeypatch.setattr(streamlit, "caption", lambda *a, **k: caps.append(a))
    monkeypatch.setattr(styles.st, "caption", lambda *a, **k: caps.append(a),
                        raising=False)
    render_df_table(pd.DataFrame())
    assert not captured and caps


def test_activity_grid_fixed_height_and_selection_sentinel(captured):
    src = pd.DataFrame([{
        "DIMENSION_ADJ_ID": 101, "COBID": 20231231, "PROCESS_TYPE": "VaR",
        "RUN_STATUS": "Failed", "USERNAME": "alice",
    }])
    got = render_activity_grid(src, selectable=True)
    assert got is SELECTION_UNSUPPORTED
    _, kwargs = captured[0]
    # 1 row fits in 35*(1+1)+3 = 73px; the 380px style-A cap applies only
    # to tables taller than that.
    assert kwargs.get("height") == 73


def test_activity_grid_empty_shows_info(captured, monkeypatch):
    infos = []
    monkeypatch.setattr(streamlit, "info", lambda *a, **k: infos.append(a))
    monkeypatch.setattr(styles.st, "info", lambda *a, **k: infos.append(a),
                        raising=False)
    assert render_activity_grid(pd.DataFrame()) is None
    assert infos and not captured
