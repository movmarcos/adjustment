# Unified Adjustment Grid Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the Home and Adjustments grids identical (same 19 columns + styling), replace the Adjustments status tabs with a filter, and let a row click open the detail/action card below the grid.

**Architecture:** Extract one shared grid into `utils/styles.py` (`build_activity_grid_df` — pure pandas display-frame builder; `resolve_selected_adjustment` — pure selection-index → row mapper; `render_activity_grid` — Streamlit renderer). Home calls it display-only; Adjustments calls it selectable and renders `render_adj_card()` for the clicked row. No database/DDL changes — `VW_MY_WORK` already exposes every field Home shows.

**Tech Stack:** Streamlit 1.35.0 (native `st.dataframe` row-selection landed in this version), pandas 2.2, Snowflake (Streamlit-in-Snowflake runtime), pytest 9 for the pure-function unit tests.

## Global Constraints

- Streamlit pinned at `1.35.0` (`environment.yml`) — use only APIs available at 1.35.0. `st.dataframe(on_select=..., selection_mode="single-row")` is valid at 1.35.0.
- Runtime is Streamlit-in-Snowflake (SiS). `st.column_config` is avoided historically; the grid pre-formats every cell to a display string and styles via a pandas `Styler`. Keep that pattern.
- No `st.data_editor`, no AgGrid, no new dependencies.
- No DDL / view changes — `ADJUSTMENT_APP.VW_MY_WORK` already selects all needed columns from `ADJ_HEADER`.
- The canonical 19 display columns, in this exact order:
  `Adj ID, COB, Source COB, Scope, Type, Status, Deleted, Entity, Dept, Book, Measure, Simulation, VaR Comp, User, Records, Created, Started, Ended, Processing Time`
- Existing Delete / Submit-for-Approval / Recall / Retry SQL inside `render_adj_card()` (`pages/2_Adjustments.py:134-406`) must remain byte-for-byte unchanged. Only the *entry point* to the card changes.

---

## File Structure

- `streamlit_app/utils/styles.py` — **modify**: add `STATUS_STYLE`, `_fmt_duration` (moved from `app.py`), `build_activity_grid_df`, `resolve_selected_adjustment`, `render_activity_grid`.
- `streamlit_app/app.py` — **modify**: Home "Recent Activity" query selects raw columns; grid rendered via `render_activity_grid(..., selectable=False)`; delete the now-duplicated inline formatting + `_fmt_duration`.
- `streamlit_app/pages/2_Adjustments.py` — **modify**: remove the status-tab block and the per-tab loop; add a "Show deleted" toggle; render one `render_activity_grid(..., selectable=True)` + click-to-card via `render_adj_card`.
- `streamlit_app/tests/test_activity_grid.py` — **create**: pytest unit tests for `build_activity_grid_df` and `resolve_selected_adjustment`.

---

### Task 1: Pure display-frame builder + selection resolver (TDD)

Add the two pure functions that have no Streamlit dependency, so they can be unit-tested locally without Snowflake.

**Files:**
- Modify: `streamlit_app/utils/styles.py` (append near the other helpers, after `fmt_adj_id`)
- Create: `streamlit_app/tests/test_activity_grid.py`

**Interfaces:**
- Consumes: existing `fmt_adj_id(value) -> str` (already in `styles.py`).
- Produces:
  - `build_activity_grid_df(df_source: pd.DataFrame) -> pd.DataFrame` — returns a frame with exactly the 19 canonical columns (see Global Constraints), in order, every cell a display string. Resolves source-column aliases: user column from `USERNAME` or `SUBMITTED_BY`; created column from `CREATED_DATE` or `SUBMITTED_AT`. Missing source columns render as `"—"`. Empty input → empty frame with the 19 columns.
  - `resolve_selected_adjustment(df_source: pd.DataFrame, selection_rows: list[int]) -> dict | None` — maps a positional selection (list of row indices) back to the original source row as a dict; returns `None` when `selection_rows` is empty.

- [ ] **Step 1: Write the failing tests**

Create `streamlit_app/tests/test_activity_grid.py`:

```python
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd streamlit_app && python -m pytest tests/test_activity_grid.py -v`
Expected: FAIL with `ImportError: cannot import name 'build_activity_grid_df'`.

- [ ] **Step 3: Implement the two pure functions in `styles.py`**

Append to `streamlit_app/utils/styles.py` (after `fmt_adj_id`). `P` (palette dict) is already defined in this module:

```python
# ── Activity grid (shared by Home + Adjustments) ──────────────────────────────

# Canonical display columns, in order. Both grids render exactly these.
ACTIVITY_GRID_COLS = [
    "Adj ID", "COB", "Source COB", "Scope", "Type", "Status", "Deleted",
    "Entity", "Dept", "Book", "Measure", "Simulation", "VaR Comp", "User",
    "Records", "Created", "Started", "Ended", "Processing Time",
]

STATUS_STYLE = {
    "Processed":        f"color:{P['success']};font-weight:600",
    "Failed":           f"color:{P['danger']};font-weight:600",
    "Running":          f"color:{P['info']};font-weight:600",
    "Pending":          f"color:{P['warning']};font-weight:600",
    "Approved":         "color:#00897B;font-weight:600",
    "Pending Approval": f"color:{P['info']};font-weight:600",
}


def _fmt_duration(seconds):
    """Human-readable duration from a seconds count (e.g. 65 -> '1m 5s')."""
    try:
        s = int(seconds)
    except (TypeError, ValueError):
        return "—"
    if s < 0:
        return "—"
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m {s % 60}s"
    return f"{s // 3600}h {(s % 3600) // 60}m"


def _grid_int_str(v, commas=False):
    try:
        if v is None or v != v:          # None or NaN
            return "—"
        return f"{int(v):,}" if commas else str(int(v))
    except (ValueError, TypeError):
        return "—"


def build_activity_grid_df(df_source):
    """Build the canonical 19-column display frame from a raw ADJ_HEADER-style
    (or VW_MY_WORK) frame. Every cell is a display string. Pure pandas — no
    Streamlit. Empty input returns an empty frame with the canonical columns."""
    import pandas as pd

    if df_source is None or df_source.empty:
        return pd.DataFrame(columns=ACTIVITY_GRID_COLS)

    df = df_source.reset_index(drop=True)
    n = len(df)

    def col(name, default=None):
        return df[name] if name in df.columns else pd.Series([default] * n)

    def pick(primary, alt):
        if primary in df.columns:
            return df[primary]
        if alt in df.columns:
            return df[alt]
        return pd.Series([None] * n)

    def fmt_dt(series):
        return pd.to_datetime(series, errors="coerce").dt.strftime("%d %b %Y %H:%M").fillna("—")

    start = pd.to_datetime(col("START_DATE"), errors="coerce")
    end = pd.to_datetime(col("PROCESS_DATE"), errors="coerce")
    dur_secs = (end - start).dt.total_seconds()

    out = pd.DataFrame({
        "Adj ID":          col("DIMENSION_ADJ_ID").apply(fmt_adj_id),
        "COB":             col("COBID").apply(_grid_int_str),
        "Source COB":      col("SOURCE_COBID").apply(_grid_int_str),
        "Scope":           col("PROCESS_TYPE").fillna("—").astype(str),
        "Type":            col("ADJUSTMENT_TYPE").fillna("—").astype(str),
        "Status":          col("RUN_STATUS").fillna("—").astype(str),
        "Deleted":         col("IS_DELETED").apply(lambda v: "Deleted" if bool(v) else ""),
        "Entity":          col("ENTITY_CODE").fillna("—").astype(str),
        "Dept":            col("DEPARTMENT_CODE").fillna("—").astype(str),
        "Book":            col("BOOK_CODE").fillna("—").astype(str),
        "Measure":         col("MEASURE_TYPE_CODE").fillna("—").astype(str),
        "Simulation":      col("SIMULATION_NAME").fillna("—").astype(str),
        "VaR Comp":        col("VAR_COMPONENT_ID").fillna("—").astype(str),
        "User":            pick("USERNAME", "SUBMITTED_BY").fillna("—").astype(str),
        "Records":         col("RECORD_COUNT").apply(lambda v: _grid_int_str(v, commas=True)),
        "Created":         fmt_dt(pick("CREATED_DATE", "SUBMITTED_AT")),
        "Started":         fmt_dt(col("START_DATE")),
        "Ended":           fmt_dt(col("PROCESS_DATE")),
        "Processing Time": dur_secs.apply(lambda v: _fmt_duration(v) if v == v else "—"),
    })
    return out[ACTIVITY_GRID_COLS]


def resolve_selected_adjustment(df_source, selection_rows):
    """Map a positional row selection back to the original source row dict.
    Returns None when nothing is selected."""
    if not selection_rows:
        return None
    idx = selection_rows[0]
    return df_source.reset_index(drop=True).iloc[idx].to_dict()
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd streamlit_app && python -m pytest tests/test_activity_grid.py -v`
Expected: PASS (8 passed).

- [ ] **Step 5: Commit**

```bash
git add streamlit_app/utils/styles.py streamlit_app/tests/test_activity_grid.py
git commit -m "feat(grid): pure builder + selection resolver for shared activity grid"
```

---

### Task 2: Streamlit grid renderer

Add the renderer that wraps `build_activity_grid_df` in a styled `st.dataframe`, optionally selectable, returning the clicked adjustment.

**Files:**
- Modify: `streamlit_app/utils/styles.py` (append after `resolve_selected_adjustment`)

**Interfaces:**
- Consumes: `build_activity_grid_df`, `resolve_selected_adjustment`, `STATUS_STYLE` (Task 1).
- Produces: `render_activity_grid(df_source, *, selectable=False, key=None, height=380, empty_msg="No adjustments yet.") -> dict | None` — renders the grid; when `selectable` and a row is clicked, returns that source row as a dict, else `None`.

- [ ] **Step 1: Implement the renderer**

Append to `streamlit_app/utils/styles.py`:

```python
def render_activity_grid(df_source, *, selectable=False, key=None,
                         height=380, empty_msg="No adjustments yet."):
    """Render the shared 19-column activity grid. Display-only by default;
    when selectable=True, a single-row click returns that adjustment as a dict
    (else None). Uses native st.dataframe selection (Streamlit >= 1.35)."""
    import streamlit as st

    if df_source is None or df_source.empty:
        st.info(empty_msg)
        return None

    grid_df = build_activity_grid_df(df_source)
    styler = (grid_df.style
              .map(lambda v: STATUS_STYLE.get(v, ""), subset=["Status"])
              .hide(axis="index"))

    if not selectable:
        st.dataframe(styler, use_container_width=True, height=height)
        return None

    event = st.dataframe(
        styler, use_container_width=True, height=height,
        on_select="rerun", selection_mode="single-row", key=key,
    )
    rows = []
    try:
        rows = event.selection.rows           # Streamlit >= 1.35 selection payload
    except AttributeError:
        rows = (event or {}).get("selection", {}).get("rows", [])
    return resolve_selected_adjustment(df_source, rows)
```

- [ ] **Step 2: Verify the module imports cleanly**

Run: `cd streamlit_app && python -c "import utils.styles as s; assert callable(s.render_activity_grid); print('ok')"`
Expected: prints `ok` (no import error). The function's Streamlit behavior is verified manually in Task 5.

- [ ] **Step 3: Commit**

```bash
git add streamlit_app/utils/styles.py
git commit -m "feat(grid): render_activity_grid renderer with single-row selection"
```

---

### Task 3: Home page uses the shared grid

Switch Home's "Recent Activity" to the shared helper and delete the now-duplicated inline formatting.

**Files:**
- Modify: `streamlit_app/app.py:488-559` (the `_fmt_duration` def and the Recent Activity block)

**Interfaces:**
- Consumes: `render_activity_grid` (Task 2).

- [ ] **Step 1: Update the import in `app.py`**

Find the existing import from `utils.styles` in `app.py` and add `render_activity_grid`. For example, if it reads:

```python
from utils.styles import inject_css, render_sidebar, fmt_adj_id, P
```

change it to include the new function:

```python
from utils.styles import inject_css, render_sidebar, fmt_adj_id, P, render_activity_grid
```

(Keep whatever other names are already imported; just add `render_activity_grid`.)

- [ ] **Step 2: Delete the local `_fmt_duration` in `app.py`**

Remove the function defined at `app.py:488-500` (it now lives in `styles.py`). The exact block to delete:

```python
def _fmt_duration(seconds):
    try:
        s = int(seconds)
    except (TypeError, ValueError):
        return "—"
    if s < 0:
        return "—"
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m {s % 60}s"
    return f"{s // 3600}h {(s % 3600) // 60}m"
```

(Confirm no other reference remains: `grep -n "_fmt_duration" streamlit_app/app.py` should return nothing after this and Step 3.)

- [ ] **Step 3: Replace the Recent Activity block** (`app.py:503-559`)

Replace the whole `try: ... except Exception as e: st.warning(...)` Recent Activity block with a version that selects raw columns and delegates rendering:

```python
try:
    # Query ADJ_HEADER directly — avoids VW_RECENT_ACTIVITY's cross-table JOIN
    # which can fail if ADJ_STATUS_HISTORY.ADJ_ID type differs from ADJ_HEADER.ADJ_ID.
    df_activity = run_query_df("""
        SELECT
            DIMENSION_ADJ_ID, COBID, SOURCE_COBID, PROCESS_TYPE, ADJUSTMENT_TYPE,
            RUN_STATUS, IS_DELETED, ENTITY_CODE, DEPARTMENT_CODE, BOOK_CODE,
            MEASURE_TYPE_CODE, SIMULATION_NAME, VAR_COMPONENT_ID, USERNAME,
            RECORD_COUNT, CREATED_DATE, START_DATE, PROCESS_DATE
        FROM ADJUSTMENT_APP.ADJ_HEADER
        ORDER BY CREATED_DATE DESC
        LIMIT 50
    """)
    render_activity_grid(df_activity, selectable=False,
                         empty_msg="No adjustments yet.")
except Exception as e:
    st.warning(f"Could not load recent activity: {e}")
```

- [ ] **Step 4: Smoke-check the file compiles**

Run: `python -m py_compile streamlit_app/app.py && echo ok`
Expected: prints `ok`.

- [ ] **Step 5: Commit**

```bash
git add streamlit_app/app.py
git commit -m "refactor(home): render Recent Activity via shared activity grid"
```

---

### Task 4: Adjustments page — remove tabs, add filter + click-to-card

Replace the status tabs and the selectbox-driven card with one selectable grid whose row click opens `render_adj_card`.

**Files:**
- Modify: `streamlit_app/pages/2_Adjustments.py` — remove `112-131` (tab definitions) and `408-482` (tab loop); add `render_activity_grid` import; add "Show deleted" toggle + single grid + card.

**Interfaces:**
- Consumes: `render_activity_grid` (Task 2), existing `render_adj_card(row, expanded=False)` (unchanged, `pages/2_Adjustments.py:134-406`).

- [ ] **Step 1: Add the import**

In the `from utils.styles import (...)` block at the top of `pages/2_Adjustments.py` (lines 13-18), add `render_activity_grid` to the imported names.

- [ ] **Step 2: Remove the status-tab definitions** (`pages/2_Adjustments.py:108-131`)

Delete this block entirely:

```python
# ──────────────────────────────────────────────────────────────────────────────
# STATUS TABS
# ──────────────────────────────────────────────────────────────────────────────

tab_labels = {
    "Pending":            ["Pending"],
    "Pending Approval":   ["Pending Approval"],
    "Approved":           ["Approved"],
    "Processed":          ["Processed"],
    "Errors / Rejected":  ["Failed", "Rejected", "Rejected - SignedOff"],
    "Deleted":            None,   # None = filter by IS_DELETED, not by status
}

def _tab_df(label, statuses):
    if df_adjs.empty:
        return pd.DataFrame()
    is_del = df_adjs["IS_DELETED"].fillna(False).astype(bool)
    if statuses is None:                          # Deleted tab
        return df_adjs[is_del]
    return df_adjs[df_adjs["RUN_STATUS"].isin(statuses) & ~is_del]

counts    = {lbl: len(_tab_df(lbl, st)) for lbl, st in tab_labels.items()}
tab_names = [f"{lbl} ({counts[lbl]})" for lbl in tab_labels]
tabs      = st.tabs(tab_names)
```

(Leave the `render_adj_card` function that follows it intact.)

- [ ] **Step 3: Replace the tab-render loop** (`pages/2_Adjustments.py:408-482`)

Delete the entire block from the `# ── Render tabs ──` comment through the end of the file (the `for tab, (label, statuses) in zip(...)` loop, lines 408-482) and replace it with a single grid + filter + card:

```python
# ── Browse + act ───────────────────────────────────────────────────────────────

show_deleted = st.toggle(
    "Show deleted", value=False,
    help="Include deleted adjustments. Hidden by default.")

if df_adjs.empty:
    view_df = df_adjs
else:
    is_del = df_adjs["IS_DELETED"].fillna(False).astype(bool)
    view_df = df_adjs if show_deleted else df_adjs[~is_del]

view_df = view_df.reset_index(drop=True)

total = len(df_adjs)
shown = len(view_df)
st.markdown(
    f"<span style='color:{P['grey_700']};font-size:0.82rem'>"
    f"Showing {shown} of {total} adjustments. Click a row to view details and actions."
    f"</span>",
    unsafe_allow_html=True)

selected = render_activity_grid(
    view_df, selectable=True, key="adj_grid",
    empty_msg="No adjustments match the current filter.")

if selected is not None:
    st.markdown("---")
    render_adj_card(selected, expanded=True)
```

- [ ] **Step 4: Confirm nothing still references the removed names**

Run: `grep -nE "tab_labels|_tab_df|st\.tabs|_opt_label|selectbox" streamlit_app/pages/2_Adjustments.py`
Expected: no matches (the tabs/selectbox flow is fully gone).

- [ ] **Step 5: Smoke-check the file compiles**

Run: `python -m py_compile streamlit_app/pages/2_Adjustments.py && echo ok`
Expected: prints `ok`.

- [ ] **Step 6: Run the unit tests again (no regression in shared module)**

Run: `cd streamlit_app && python -m pytest tests/test_activity_grid.py -v`
Expected: PASS (8 passed).

- [ ] **Step 7: Commit**

```bash
git add streamlit_app/pages/2_Adjustments.py
git commit -m "feat(adjustments): single filterable grid with click-to-card; remove status tabs"
```

---

### Task 5: Manual end-to-end verification (SiS) + selection fallback

The renderer and page wiring can only be fully verified in the running Streamlit-in-Snowflake app. Deploy the changed `streamlit_app/` and verify.

**Files:** none changed unless the fallback in the last step is needed.

- [ ] **Step 1: Deploy the app** using the project's normal deploy path (`deploy.py`) and open it in Snowflake.

- [ ] **Step 2: Home grid** — open Home, confirm "Recent Activity" shows the 19 columns in order with Status color-coding, and is display-only (no selection).

- [ ] **Step 3: Adjustments grid parity** — open Adjustments; confirm the grid shows the **same 19 columns**, same formatting and Status colors as Home. Confirm there are **no status tabs**.

- [ ] **Step 4: Filters** — the top Status / Scope / Type multiselects and "Only my adjustments" narrow the grid. Confirm "Show deleted" is off by default and that turning it on adds rows whose Deleted column reads "Deleted". Confirm the "Showing N of M" caption updates.

- [ ] **Step 5: Click-to-card** — click a row; confirm the correct adjustment's card renders below the grid with its actions. Change a filter, click a different row; confirm the right adjustment opens (positional mapping correct).

- [ ] **Step 6: Delete** — for a Pending/Failed/Processed adjustment, click Delete in the card; confirm it deletes, the page reruns, the selection clears, and the row leaves the default view (and appears under "Show deleted").

- [ ] **Step 7: Selection fallback (only if Step 5 fails on SiS).** If the SiS build does not surface `on_select` (grid renders but clicks never select), restore a selectbox entry point above the card without bringing back the tabs. Replace the `selected = render_activity_grid(...)` call in Task 4 Step 3 with:

```python
render_activity_grid(view_df, selectable=False, key="adj_grid",
                     empty_msg="No adjustments match the current filter.")

def _opt_label(i):
    if i is None:
        return "— select an adjustment to view detail / actions —"
    r = view_df.iloc[i]
    return (f'{fmt_adj_id(r.get("DIMENSION_ADJ_ID"))} · {r.get("PROCESS_TYPE")} · '
            f'{r.get("ADJUSTMENT_TYPE")} · {r.get("RUN_STATUS")} · '
            f'{r.get("ENTITY_CODE") or "—"}')

choice = st.selectbox(
    "Open an adjustment", options=[None] + list(range(len(view_df))),
    format_func=_opt_label, key="adj_pick", label_visibility="collapsed")
selected = view_df.iloc[choice].to_dict() if choice is not None else None

if selected is not None:
    st.markdown("---")
    render_adj_card(selected, expanded=True)
```

Then commit:

```bash
git add streamlit_app/pages/2_Adjustments.py
git commit -m "fix(adjustments): selectbox fallback for SiS builds without native row-selection"
```

---

## Self-Review

**Spec coverage:**
- Align grids → same 19 columns + styling: Task 1 (builder) + Task 2 (renderer) + Task 3 (Home) + Task 4 (Adjustments). ✓
- Remove status tabs → filter: Task 4 removes tabs; existing top Status multiselect + new "Show deleted" toggle cover filtering. ✓
- Click row → card with Delete: Task 2 returns the clicked row; Task 4 renders `render_adj_card`; Delete SQL unchanged. ✓
- Home stays display-only (non-goal honored): Task 3 uses `selectable=False`. ✓
- No DDL change (non-goal honored): confirmed `VW_MY_WORK` exposes all fields; no view edits. ✓
- Empty-filter friendly message: `render_activity_grid` `empty_msg`, exercised in Task 4. ✓
- Selection persistence / positional mapping: `resolve_selected_adjustment` + `reset_index`; rerun-on-action clears selection. ✓

**Placeholder scan:** No TBD/TODO; every code step shows full code; tests contain real assertions. ✓

**Type consistency:** `build_activity_grid_df(df) -> DataFrame`, `resolve_selected_adjustment(df, rows) -> dict|None`, `render_activity_grid(df, *, selectable, key, height, empty_msg) -> dict|None` — names/signatures used consistently across Tasks 1-4. `ACTIVITY_GRID_COLS` / `CANONICAL_COLS` match between implementation and test. ✓

**Risk note:** The one real unknown is whether the SiS build honors `st.dataframe` native selection at 1.35.0. Task 5 verifies it live and Step 7 carries a fully-specified selectbox fallback that keeps the single-grid (no-tabs) design.
