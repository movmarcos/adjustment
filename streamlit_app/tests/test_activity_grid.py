import os, sys
import numpy as np
import pandas as pd
import pytest

# Make the app package importable (styles.py lives in streamlit_app/utils)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from utils.styles import build_activity_grid_df, resolve_selected_adjustment

CANONICAL_COLS = [
    "Adj ID", "COB", "Source COB", "Scope", "Type", "Status", "Deleted",
    "Entity", "Dept", "Book", "Measure", "Simulation", "VaR Comp", "User",
    "Records", "Created", "Started", "Ended", "Processing Time",
]


def _row(**over):
    base = dict(
        DIMENSION_ADJ_ID=101, COBID=20231231, SOURCE_COBID=20231130,
        PROCESS_TYPE="VaR", ADJUSTMENT_TYPE="Scale", RUN_STATUS="Processed",
        IS_DELETED=False, ENTITY_CODE="E1", DEPARTMENT_CODE="D1", BOOK_CODE="B1",
        MEASURE_TYPE_CODE="M1", SIMULATION_NAME="SIM", VAR_COMPONENT_ID="VC1",
        RECORD_COUNT=12345,
        START_DATE=pd.Timestamp("2026-06-01 09:00:00"),
        PROCESS_DATE=pd.Timestamp("2026-06-01 09:01:05"),
    )
    base.update(over)
    return base


def test_columns_match_canonical_order_and_count():
    df = build_activity_grid_df(pd.DataFrame([_row(USERNAME="alice", CREATED_DATE=pd.Timestamp("2026-06-01 08:00:00"))]))
    assert list(df.columns) == CANONICAL_COLS


def test_formatting_values():
    df = build_activity_grid_df(pd.DataFrame([_row(USERNAME="alice", CREATED_DATE=pd.Timestamp("2026-06-01 08:00:00"))]))
    r = df.iloc[0]
    assert r["Records"] == "12,345"          # comma-grouped
    assert r["COB"] == "20231231"            # int, no commas
    assert r["User"] == "alice"
    assert r["Created"] == "01 Jun 2026 08:00"
    assert r["Processing Time"] == "1m 5s"   # 65 seconds
    assert r["Deleted"] == ""


def test_alias_resolution_submitted_columns():
    # VW_MY_WORK uses SUBMITTED_BY / SUBMITTED_AT instead of USERNAME / CREATED_DATE
    df = build_activity_grid_df(pd.DataFrame([_row(SUBMITTED_BY="bob", SUBMITTED_AT=pd.Timestamp("2026-06-02 10:30:00"))]))
    r = df.iloc[0]
    assert r["User"] == "bob"
    assert r["Created"] == "02 Jun 2026 10:30"


def test_deleted_flag_renders():
    df = build_activity_grid_df(pd.DataFrame([_row(IS_DELETED=True, USERNAME="x", CREATED_DATE=pd.NaT)]))
    assert df.iloc[0]["Deleted"] == "Deleted"


def test_missing_columns_become_dash():
    df = build_activity_grid_df(pd.DataFrame([{"DIMENSION_ADJ_ID": 7, "RUN_STATUS": "Pending"}]))
    r = df.iloc[0]
    assert r["Entity"] == "—"
    assert r["Records"] == "—"
    assert r["Created"] == "—"
    assert r["Processing Time"] == "—"


def test_empty_input_returns_empty_with_canonical_columns():
    df = build_activity_grid_df(pd.DataFrame())
    assert df.empty
    assert list(df.columns) == CANONICAL_COLS


def test_resolve_selected_adjustment_maps_back_to_source_row():
    src = pd.DataFrame([_row(DIMENSION_ADJ_ID=1), _row(DIMENSION_ADJ_ID=2), _row(DIMENSION_ADJ_ID=3)])
    got = resolve_selected_adjustment(src, [1])
    assert got["DIMENSION_ADJ_ID"] == 2


def test_resolve_selected_adjustment_empty_returns_none():
    src = pd.DataFrame([_row()])
    assert resolve_selected_adjustment(src, []) is None


def test_deleted_nan_is_not_deleted():
    # A NaN IS_DELETED must render as "" (not "Deleted") — matches the page
    # filter which treats null as not-deleted.
    df = build_activity_grid_df(pd.DataFrame([_row(IS_DELETED=float("nan"), USERNAME="x", CREATED_DATE=pd.NaT)]))
    assert df.iloc[0]["Deleted"] == ""

def test_deleted_none_is_not_deleted():
    df = build_activity_grid_df(pd.DataFrame([_row(IS_DELETED=None, USERNAME="x", CREATED_DATE=pd.NaT)]))
    assert df.iloc[0]["Deleted"] == ""
