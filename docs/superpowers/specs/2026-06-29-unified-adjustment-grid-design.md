# Unified Adjustment Grid — Design

**Date:** 2026-06-29
**Status:** Approved (pending spec review)

## Problem

The Home page and the Adjustments page each render their own data grid, built
independently. They have drifted: Home shows ~19 columns with status
color-coding; Adjustments shows 11 leaner columns, split across **6 status tabs**
(`st.tabs`), with row selection done through a **`st.selectbox` dropdown** where
the user picks an adjustment to open its detail/action card.

Two problems for non-technical end users:
1. The status tabs make it hard to *find* a specific adjustment — you must guess
   which tab it lives in.
2. Selecting an adjustment to act on (e.g. delete) means using a dropdown and
   effectively hunting by ID, rather than just clicking the row you see.

## Goals

1. **Align the two grids** — both render the **same 19-column set** (Home's
   current columns) with identical formatting and status color-coding.
2. **Remove the status tabs** on the Adjustments page — replace with a **status
   filter** so all adjustments live in one grid.
3. **Click-to-act** — clicking a grid row opens the existing detail/action card
   (with Delete etc.) directly below the grid, replacing the dropdown + ID flow.

## Non-Goals

- Home stays a display-only dashboard snapshot ("Recent Activity"). No
  click-to-card on Home — only column/style alignment.
- No change to the underlying Delete / Submit / Recall / Retry SQL or business
  rules. Only the *entry point* to those actions changes.
- No new columns invented beyond Home's existing 19.

## Current State (reference)

- **Home grid:** `streamlit_app/app.py:503-555` — `st.dataframe()` with
  `STATUS_STYLE` map on the Status column, `.hide(axis="index")`, display-only,
  19 columns.
- **Adjustments tabs:** `streamlit_app/pages/2_Adjustments.py:112-131` —
  `tab_labels` dict + `st.tabs()`; `_tab_df()` filters the shared `df_adjs` by
  `RUN_STATUS` and the `IS_DELETED` flag (Deleted tab uses the flag, not a
  status).
- **Adjustments grid:** `pages/2_Adjustments.py:436-466` — `st.dataframe()`,
  11 columns, per tab.
- **Selection / card:** `pages/2_Adjustments.py:468-482` — `st.selectbox`
  "Open an adjustment" → `render_adj_card()` (lines 134-406), whose Actions
  section (lines 276-406) holds Delete (gated to Pending/Failed/Processed),
  Submit for Approval, Recall to Pending, Retry.

## Design

### 1. Shared grid helper (`streamlit_app/utils/styles.py`)

Extract the grid into one reusable place so the two pages cannot drift again:

```
build_activity_grid_df(df_adjs) -> pd.DataFrame
    # Canonical 19-column display frame + all formatting (fmt_adj_id,
    # comma'd record counts, COB ints, dd MMM YYYY HH:MM timestamps,
    # processing-time duration, etc.). Column order == Home's order.

render_activity_grid(df_adjs, *, selectable=False, key=None) -> dict | None
    # Builds the frame via build_activity_grid_df, applies STATUS_STYLE color
    # map on the Status column + .hide(axis="index").
    # selectable=False -> plain st.dataframe (Home).
    # selectable=True  -> st.dataframe(..., on_select="rerun",
    #                     selection_mode="single-row"); returns the clicked
    #                     adjustment as a dict (mapped positionally back to the
    #                     input df_adjs row), or None when nothing selected.
    # Empty df -> renders a friendly "no adjustments" message, returns None.
```

- **Home** (`app.py`) calls `render_activity_grid(df_activity, selectable=False)`.
- **Adjustments** calls `render_activity_grid(filtered_df, selectable=True,
  key="adj_grid")`.

### 2. Adjustments page restructure (`pages/2_Adjustments.py`)

Remove `tab_labels`, `st.tabs()`, `_tab_df`, and the per-tab loop. Replace with a
single filter bar above one grid:

- **Status multi-select** — options are the real statuses (Pending, Pending
  Approval, Approved, Processed, Failed, Rejected, Rejected - SignedOff).
  Default: **all non-deleted statuses selected** (grid shows everything that
  isn't deleted). User narrows to any combination.
- **"Show deleted" toggle** — off by default. When on, `IS_DELETED` rows are
  included in the result.
- A `showing N of M` caption for orientation (replaces the per-tab counts).
- One `render_activity_grid(filtered_df, selectable=True)` below the bar.

Filtering logic: start from `df_adjs`; if "Show deleted" is off, drop
`IS_DELETED` rows; then keep rows whose `RUN_STATUS` is in the selected statuses.

### 3. Row-click → action card

Replace the `st.selectbox` block (lines 468-482) entirely:

- `selected = render_activity_grid(filtered_df, selectable=True, key=...)`.
- `if selected is not None:` render `st.markdown("---")` +
  `render_adj_card(selected, expanded=True)` directly below the grid — the
  existing card, unchanged, with Delete / Submit / Recall / Retry intact.
- No dropdown, no typing an ID.

### 4. Edge cases & error handling

- **Empty result:** grid helper shows "No adjustments match the current filter"
  instead of a bare empty table; returns None.
- **Selection persistence:** after an action reruns the page, selection state is
  reset so a stale row can't re-open a card for a just-deleted adjustment.
- **Positional mapping:** the clicked index resolves against the *filtered*
  frame, so changing the filter can never open the wrong adjustment.
- **Action SQL unchanged:** Delete and the other actions keep their existing SQL
  and gating; only the entry point changes.

### 5. Testing

Streamlit-on-Snowflake UI — verification is manual via the running app:

1. Home and Adjustments grids render the identical 19 columns, formatting, and
   status colors.
2. Status multi-select narrows the grid correctly; default shows all non-deleted.
3. "Show deleted" toggle includes/excludes `IS_DELETED` rows.
4. Clicking a row opens the correct adjustment's card below the grid.
5. Delete works end-to-end from the card; after delete the page reruns,
   selection clears, and the row is gone (or moves under "Show deleted").
6. Empty-filter state shows the friendly message, no crash.

## Files Touched

- `streamlit_app/utils/styles.py` — add `build_activity_grid_df` +
  `render_activity_grid`.
- `streamlit_app/app.py` — Home grid calls the shared helper.
- `streamlit_app/pages/2_Adjustments.py` — remove tabs, add filter bar, switch
  to shared selectable grid + click-to-card.
