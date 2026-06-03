# Direct Adjustment Category — Design

**Date:** 2026-06-03
**Status:** Approved (pending spec review)
**Area:** `streamlit_app/pages/1_New_Adjustment.py`, `streamlit_app/utils/styles.py`

## Summary

Add a new adjustment category, **Direct Adjustment**, that combines:

- the **data-scope selection of Scaling** (scope cards VaR / Stress / FRTB[+sub-types] /
  Sensitivity), and
- the **value-input mechanism of VaR Upload** (paste a CSV; values are written verbatim as
  line items and applied via the existing `Direct` processing path).

It **replaces** the existing **VaR Upload** category — VaR becomes one selectable scope inside
Direct Adjustment.

Each scope will eventually have its own CSV column set. **For now every scope reuses the VaR
column definitions**; the per-scope configuration is structured as a single extensibility point
to update later.

## Goals

- One category for direct/uploaded adjustments across all four scopes.
- Scope UX identical to Scaling (shared component, including FRTB sub-types).
- No stored-procedure changes — reuse the existing `Direct` action.
- A clearly-marked place to add per-scope columns + writers later.

## Non-goals

- Per-scope column sets and per-scope line-item writers for Stress / FRTB / Sensitivity
  (scaffolded with VaR columns now; the user fills them in later).
- Any change to `SP_SUBMIT_ADJUSTMENT`, `SP_PROCESS_ADJUSTMENT`, `SP_PREVIEW_ADJUSTMENT`,
  or `SP_RUN_PIPELINE`.

## Design

### 1. Category config (`utils/styles.py`)

- Remove the `"VaR Upload"` entry from `CATEGORY_CONFIG`.
- Add:
  ```python
  "Direct Adjustment": {
      "icon": "📥", "color": "#6A1B9A", "bg": "#F3E5F5",
      "desc": "Upload exact adjustment values for a chosen scope (CSV)",
  },
  ```

### 2. Shared scope selector (`1_New_Adjustment.py`)

Extract the Scaling form's scope-card + FRTB sub-type block into a reusable helper:

```python
def _render_scope_selector(include_frtball: bool = True) -> None:
    """Render scope cards (VaR/Stress/FRTB/Sensitivity) + FRTB sub-type selector.
    Sets wiz['process_type']. Shared by Scaling and Direct Adjustment forms.
    include_frtball=False hides the FRTBALL sub-type (used by Direct Adjustment —
    a fan-out tag makes no sense when uploading explicit values)."""
```

`render_scaling_form()` calls it as `_render_scope_selector()` (FRTBALL kept);
`render_direct_form()` calls it as `_render_scope_selector(include_frtball=False)`.
When `include_frtball` is False the FRTB sub-type selector offers only
`FRTB`, `FRTBDRC`, `FRTBRRAO`. Behaviour (FRTB → sub-type selection, `safe_rerun`
on click) is otherwise unchanged.

### 3. Per-scope CSV configuration (the "update later" hook)

```python
DIRECT_SCOPE_CONFIG = {
    # scope: {expected_cols, measure_cols, writer}
    # TODO: give each non-VaR scope its real columns + writer. All point to VaR for now.
    "VaR":         {"expected": EXPECTED_VAR_COLS, "measures": VAR_MEASURE_COLS, "writer": _write_var_upload_line_items},
    "Stress":      {... same as VaR (placeholder) ...},
    "Sensitivity": {... same as VaR (placeholder) ...},
    "FRTB":        {... same as VaR (placeholder) ...},
    "FRTBDRC":     {... same as VaR (placeholder) ...},
    "FRTBRRAO":    {... same as VaR (placeholder) ...},
    # NOTE: no FRTBALL — fan-out is not applicable to direct value uploads.
}
```

A dispatcher resolves the writer:

```python
def _write_direct_line_items(scope: str, adj_id: str, df_csv) -> int:
    cfg = DIRECT_SCOPE_CONFIG.get(scope) or DIRECT_SCOPE_CONFIG["VaR"]
    return cfg["writer"](adj_id, df_csv)
```

### 4. `render_direct_form()` (replaces `render_var_upload_form`)

1. `_render_scope_selector(include_frtball=False)`; if no `process_type`, show "select a scope" and return.
2. Look up `cfg = DIRECT_SCOPE_CONFIG[scope]`. Show an info banner listing `cfg["expected"]`.
3. CSV paste `text_area` → `pd.read_csv` → `wiz["uploaded_df"]` (reuse the VaR parse: row/column
   metrics, head preview, missing/extra-column warnings vs `cfg["expected"]`).
4. Auto-detect `wiz["cobid"]` and `wiz["entity_code"]` from the CSV (`COBId`, `EntityCode`).
5. **Reference \*** (`global_reference`) and **Reason \*** (`reason`) — both required.
   **Requires Approval** checkbox. Duplicate-reference check (reuse existing query + banner).
6. Validation list (all marked `*`): Scope, CSV Data, COB Date, Entity Code, Reference, Reason →
   `_missing_info`. Continue → `wiz["step"] = 2` only when complete.

### 5. Payload + submit

- `_build_payload()`: replace the `cat == "VaR Upload"` branch with `cat == "Direct Adjustment"`:
  ```python
  return {
      "cobid": wiz["cobid"], "process_type": wiz["process_type"],
      "adjustment_type": "Direct", "username": current_user_name(),
      "source_cobid": wiz["cobid"], "reason": wiz.get("reason", ""),
      "entity_code": wiz.get("entity_code", ""),
      "requires_approval": wiz.get("requires_approval", False),
      "adjustment_occurrence": "ADHOC",
      "file_name": wiz.get("uploaded_file_name", ""),
      "global_reference": wiz.get("global_reference", ""),
  }
  ```
- `_do_submit()`: replace the `category == "VaR Upload"` special-case with
  `category == "Direct Adjustment"`; pre-generate `adj_id`, call
  `_write_direct_line_items(wiz["process_type"], adj_id, wiz["uploaded_df"])`; the "no non-zero
  values" guard is unchanged.

### 6. Step-1 routing & Step-2 preview

- Step 1 router: `elif wiz["category"] == "Direct Adjustment": render_direct_form()`
  (remove the `"VaR Upload"` branch).
- Step 2 summary banner: rename the `cat == "VaR Upload"` branch to `"Direct Adjustment"`,
  showing scope + reference + row count + COB, then the uploaded-df head preview (reuse).

### 7. Processing (unchanged)

`adjustment_type="Direct"` → `ACTION_MAP` → `Direct` action → `SP_PROCESS_ADJUSTMENT` reads
`ADJ_LINE_ITEM` for the `ADJ_ID` and maps to the scope's `FACT.*_ADJUSTMENT`. No SP changes.

## Limitations (explicit)

- Direct upload is **end-to-end correct for VaR today** (real columns + writer).
- Stress / FRTB / Sensitivity are **selectable and scaffolded** using the VaR column set as a
  placeholder. Correct processing for them requires per-scope `expected`/`measures` columns and a
  per-scope line-item writer, added later in `DIRECT_SCOPE_CONFIG` and `_write_direct_line_items`.
  These spots carry `TODO` markers.

## Testing

- Manual: Direct Adjustment → VaR scope → paste sample VaR CSV → preview → submit → run pipeline →
  verify `ADJ_LINE_ITEM` and `FACT.VAR_MEASURES_ADJUSTMENT`.
- `py_compile` the page and styles modules.
- No stored-procedure or automated UI tests required.

## Related

- Roll/preview/mandatory-field work: [[project_roll_semantics]].
- Existing Direct path: `new_adjustment_db_objects/05_sp_process_adjustment.sql` (Direct branch).
