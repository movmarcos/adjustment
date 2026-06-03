# Direct Adjustment Category — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a "Direct Adjustment" category that pairs Scaling-style scope selection with VaR Upload-style CSV input, replacing the VaR Upload category.

**Architecture:** Two files only — `utils/styles.py` (category metadata) and `pages/1_New_Adjustment.py` (forms, payload, submit, routing, preview). VaR Upload's CSV mechanism is generalised across scopes via a per-scope config dict (`DIRECT_SCOPE_CONFIG`, all scopes reuse VaR columns for now) and a writer dispatcher. No stored-procedure changes — `adjustment_type="Direct"` reuses the existing `Direct` processing action.

**Tech Stack:** Python, Streamlit-in-Snowflake, Snowpark, pandas.

**Spec:** `docs/superpowers/specs/2026-06-03-direct-adjustment-category-design.md`

**Testing note:** The Streamlit pages import `streamlit`/`snowflake`, which aren't importable outside the SiS runtime, so there is no unit-test harness. Per-task verification is `python3 -m py_compile` (catches syntax errors); functional behaviour is verified manually in the running app after Task 6. Run all commands from the repo root `/Users/marcosmagri/Documents/MUFG/adjustment`.

---

## File Structure

- **Modify** `streamlit_app/utils/styles.py` — `CATEGORY_CONFIG`: drop `VaR Upload`, add `Direct Adjustment`.
- **Modify** `streamlit_app/pages/1_New_Adjustment.py`:
  - Add `_render_scope_selector(include_frtball)` (extracted from `render_scaling_form`).
  - Add `DIRECT_SCOPE_CONFIG` + `_write_direct_line_items(scope, adj_id, df)` dispatcher.
  - Add `render_direct_form()` (rename + adapt of `render_var_upload_form`).
  - Update `_build_payload`, `_do_submit`, the Step-1 router, and the Step-2 preview branch.

---

## Task 1: Swap the category config

**Files:**
- Modify: `streamlit_app/utils/styles.py` (the `CATEGORY_CONFIG` dict)

- [ ] **Step 1: Replace the `VaR Upload` entry with `Direct Adjustment`**

Find this entry in `CATEGORY_CONFIG`:

```python
    "VaR Upload": {
        "icon": "📤", "color": "#6A1B9A", "bg": "#F3E5F5",
        "desc": "Upload a CSV with 21 VaR measure columns for direct insertion",
    },
```

Replace it with:

```python
    "Direct Adjustment": {
        "icon": "📥", "color": "#6A1B9A", "bg": "#F3E5F5",
        "desc": "Upload exact adjustment values for a chosen scope (CSV)",
    },
```

Keep the dict key order so Direct Adjustment appears where VaR Upload did.

- [ ] **Step 2: Verify it compiles**

Run: `python3 -m py_compile streamlit_app/utils/styles.py`
Expected: no output (success).

- [ ] **Step 3: Commit**

```bash
git add streamlit_app/utils/styles.py
git commit -m "feat(direct-adj): replace VaR Upload category with Direct Adjustment"
```

---

## Task 2: Extract a shared scope selector

`render_scaling_form()` currently renders the scope cards + FRTB sub-type selector inline. Move that block into a reusable helper so the Direct form can reuse it, and add a flag to hide FRTBALL.

**Files:**
- Modify: `streamlit_app/pages/1_New_Adjustment.py` (`render_scaling_form` and a new helper above it)

- [ ] **Step 1: Add the `_render_scope_selector` helper**

Add this new function immediately **above** `def render_scaling_form() -> None:`. Build it by **moving the existing block verbatim** out of `render_scaling_form` — specifically everything from the `section_title("Data Scope", "🔍")` line down to (but **not** including) the `st.divider()` that precedes the `if not wiz["process_type"]:` guard. Then apply the two marked changes (signature + FRTBALL filter):

```python
def _render_scope_selector(include_frtball: bool = True) -> None:
    """Scope cards (VaR/Stress/FRTB/Sensitivity) + FRTB sub-type selector.

    Sets wiz['process_type']. Shared by the Scaling and Direct Adjustment forms.
    include_frtball=False hides the FRTBALL sub-type (Direct Adjustment uploads
    explicit values, so the fan-out tag does not apply).
    """
    # ── Scope cards ───────────────────────────────────────────────────────
    section_title("Data Scope", "🔍")
    scope_cols = st.columns(len(SCOPE_CONFIG))
    for i, (sk, cfg) in enumerate(SCOPE_CONFIG.items()):
        with scope_cols[i]:
            is_sel = (wiz["process_type"] in FRTB_SUBTYPES) if sk == "FRTB" \
                     else (wiz["process_type"] == sk)
            st.markdown(
                f'<div style="background:{cfg["bg"] if is_sel else P["white"]};'
                f'border:2px solid {P["primary"] if is_sel else P["border"]};'
                f'border-radius:10px;padding:0.6rem 0.4rem;text-align:center">'
                f'<div style="font-size:1.5rem">{cfg["icon"]}</div>'
                f'<div style="font-weight:700;font-size:0.8rem;margin-top:0.2rem">'
                f'{cfg["label"]}</div></div>', unsafe_allow_html=True)
            if st.button(f'{"✓ " if is_sel else ""}{cfg["label"]}',
                         key=_k(f"scope_{sk}"), use_container_width=True,
                         type="primary" if is_sel else "secondary"):
                wiz["process_type"] = sk
                safe_rerun()

    # FRTB sub-type selector
    if wiz["process_type"] in FRTB_SUBTYPES:
        st.markdown(
            f'<div style="background:{P["success_lt"]};border:1px solid #A5D6A7;'
            f'border-radius:8px;padding:0.5rem 1rem;margin:0.5rem 0 0.3rem;'
            f'font-size:0.82rem;color:{P["success"]}">'
            f'🏛️ <strong>FRTB selected</strong> — choose a sub-type</div>',
            unsafe_allow_html=True)
        subtypes = {k: v for k, v in FRTB_SUBTYPE_CONFIG.items()
                    if include_frtball or k != "FRTBALL"}
        sub_cols = st.columns(len(subtypes))
        for i, (stk, stdesc) in enumerate(subtypes.items()):
            with sub_cols[i]:
                is_sub = wiz["process_type"] == stk
                st.markdown(
                    f'<div style="background:{P["success_lt"] if is_sub else P["white"]};'
                    f'border:2px solid {P["success"] if is_sub else P["border"]};'
                    f'border-radius:8px;padding:0.5rem 0.3rem;text-align:center">'
                    f'<div style="font-weight:700;font-size:0.82rem">{stk}</div>'
                    f'<div style="font-size:0.68rem;color:{P["grey_700"]};margin-top:2px">'
                    f'{stdesc}</div></div>', unsafe_allow_html=True)
                if st.button(f'{"✓ " if is_sub else ""}{stk}',
                             key=_k(f"frtb_{stk}"), use_container_width=True,
                             type="primary" if is_sub else "secondary"):
                    wiz["process_type"] = stk
                    safe_rerun()
```

(The only differences from the original inline block are the function wrapper/docstring and the `subtypes = {...}` filter replacing the direct iteration over `FRTB_SUBTYPE_CONFIG`.)

- [ ] **Step 2: Replace the inline block in `render_scaling_form` with a call**

In `render_scaling_form()`, the body now begins with the moved block removed. Replace the removed block with a single call so the function starts like this:

```python
def render_scaling_form() -> None:
    # ── Scope selection (shared with Direct Adjustment) ───────────────────
    _render_scope_selector()

    st.divider()
    if not wiz["process_type"]:
        st.info("👆 Select a data scope to continue.")
        return
```

Everything after the `return` (Adjustment Type, Date & Schedule, filters, validation) stays unchanged.

- [ ] **Step 3: Verify it compiles**

Run: `python3 -m py_compile streamlit_app/pages/1_New_Adjustment.py`
Expected: no output (success).

- [ ] **Step 4: Commit**

```bash
git add streamlit_app/pages/1_New_Adjustment.py
git commit -m "refactor(new-adj): extract _render_scope_selector shared by Scaling/Direct"
```

---

## Task 3: Add per-scope config + line-item writer dispatcher

**Files:**
- Modify: `streamlit_app/pages/1_New_Adjustment.py`

**Placement (important):** this block references `_write_var_upload_line_items`, `EXPECTED_VAR_COLS`, and `VAR_MEASURE_COLS`. All three are module-level and execute at import, and `EXPECTED_VAR_COLS`/`VAR_MEASURE_COLS` are defined *after* `_write_var_upload_line_items`. Add the block **immediately after the `EXPECTED_VAR_COLS = [ ... ]` definition and immediately before `def render_var_upload_form`** so every reference is already defined. (Putting it right after `_write_var_upload_line_items` would `NameError` on `EXPECTED_VAR_COLS` at import.)

- [ ] **Step 1: Add `DIRECT_SCOPE_CONFIG` and `_write_direct_line_items`**

Add this block at the placement described above:

```python
# ── Direct Adjustment: per-scope CSV config ──────────────────────────────────
# Each scope will eventually define its own CSV columns + line-item writer.
# TODO: replace the placeholder entries below with each scope's real columns and
#       a scope-specific writer. For now every scope reuses the VaR definitions,
#       so Direct uploads are only end-to-end correct for VaR.
_DIRECT_VAR_ENTRY = {
    "expected": EXPECTED_VAR_COLS,
    "measures": VAR_MEASURE_COLS,
    "writer":   _write_var_upload_line_items,
}
DIRECT_SCOPE_CONFIG = {
    "VaR":         _DIRECT_VAR_ENTRY,
    "Stress":      _DIRECT_VAR_ENTRY,   # TODO: Stress-specific columns + writer
    "Sensitivity": _DIRECT_VAR_ENTRY,   # TODO: Sensitivity-specific columns + writer
    "FRTB":        _DIRECT_VAR_ENTRY,   # TODO: FRTB-specific columns + writer
    "FRTBDRC":     _DIRECT_VAR_ENTRY,   # TODO: FRTBDRC-specific columns + writer
    "FRTBRRAO":    _DIRECT_VAR_ENTRY,   # TODO: FRTBRRAO-specific columns + writer
    # No FRTBALL — fan-out is not applicable to direct value uploads.
}


def _direct_cfg(scope: str) -> dict:
    """Per-scope Direct config; falls back to VaR while scopes are placeholders."""
    return DIRECT_SCOPE_CONFIG.get(scope) or _DIRECT_VAR_ENTRY


def _write_direct_line_items(scope: str, adj_id: str, df_csv) -> int:
    """Write Direct-upload line items for a scope using its configured writer."""
    return _direct_cfg(scope)["writer"](adj_id, df_csv)
```

- [ ] **Step 2: Verify it compiles**

Run: `python3 -m py_compile streamlit_app/pages/1_New_Adjustment.py`
Expected: no output (success).

- [ ] **Step 3: Commit**

```bash
git add streamlit_app/pages/1_New_Adjustment.py
git commit -m "feat(direct-adj): add DIRECT_SCOPE_CONFIG + line-item writer dispatcher"
```

---

## Task 4: Build `render_direct_form` (rename + adapt `render_var_upload_form`)

Convert the VaR-specific upload form into the scope-aware Direct form.

**Files:**
- Modify: `streamlit_app/pages/1_New_Adjustment.py` (`render_var_upload_form`)

- [ ] **Step 1: Rename the function and add the scope selector + per-scope columns**

Rename `def render_var_upload_form() -> None:` to `def render_direct_form() -> None:`. Replace its **opening** (the `section_title(...)`, the static `_info_banner(...)`, and the `csv_text = st.text_area(...)` setup) so the function starts like this, and resolve the scope's expected columns from config:

```python
def render_direct_form() -> None:
    # ── Scope selection (FRTBALL excluded — no fan-out for direct values) ──
    _render_scope_selector(include_frtball=False)

    st.divider()
    if not wiz["process_type"]:
        st.info("👆 Select a data scope to continue.")
        return

    cfg          = _direct_cfg(wiz["process_type"])
    expected_cols = cfg["expected"]

    section_title(f"Direct Adjustment — {wiz['process_type']} CSV", "📥")
    _info_banner(
        'Paste a CSV of exact adjustment values. Expected columns: '
        '<code>' + ', '.join(expected_cols) + '</code>.')

    csv_text = st.text_area(
        "Paste CSV Data Here", value="", height=180, key=_k("direct_csv"),
        help="Paste the full CSV content including the header row.")
```

- [ ] **Step 2: Make the CSV validation use the scope's expected columns**

In the CSV-parse `try` block, the lines that currently compare against `EXPECTED_VAR_COLS` must use `expected_cols` instead. Change:

```python
            missing_cols = [c for c in EXPECTED_VAR_COLS if c not in df.columns]
            extra_cols   = [c for c in df.columns   if c not in EXPECTED_VAR_COLS]
```

to:

```python
            missing_cols = [c for c in expected_cols if c not in df.columns]
            extra_cols   = [c for c in df.columns    if c not in expected_cols]
```

Leave the rest of the parse block (row/column metrics, `st.dataframe` preview, `wiz["cobid"]`/`wiz["entity_code"]` auto-detect, the `wiz["uploaded_df"]`/`wiz["uploaded_file_name"]` assignments) unchanged.

- [ ] **Step 3: Make the Reason field mandatory**

Find the reason text area in this function:

```python
    wiz["reason"] = st.text_area(
        "Reason / Business Justification", value=wiz.get("reason", ""),
        height=60, key=_k("var_reason"))
```

Replace with (add the `*`):

```python
    wiz["reason"] = st.text_area(
        "Reason / Business Justification *", value=wiz.get("reason", ""),
        height=60, key=_k("var_reason"))
```

- [ ] **Step 4: Add Scope + Reason to the validation checklist**

Find the `_checks` list near the end of this function:

```python
    _checks = [
        ("CSV Data",     wiz.get("uploaded_df") is not None),
        ("COB Date",     bool(wiz.get("cobid"))),
        ("Entity Code",  bool(wiz.get("entity_code"))),
        ("Reference",    bool(wiz.get("global_reference"))),
    ]
```

Replace with:

```python
    _checks = [
        ("Scope",        bool(wiz.get("process_type"))),
        ("CSV Data",     wiz.get("uploaded_df") is not None),
        ("COB Date",     bool(wiz.get("cobid"))),
        ("Entity Code",  bool(wiz.get("entity_code"))),
        ("Reference",    bool(wiz.get("global_reference"))),
        ("Reason",       bool((wiz.get("reason") or "").strip())),
    ]
```

- [ ] **Step 5: Verify it compiles**

Run: `python3 -m py_compile streamlit_app/pages/1_New_Adjustment.py`
Expected: no output (success). (Note: the Step-1 router still calls the old name in a now-unreachable branch — that is fixed in Task 6. `py_compile` passes because names resolve at runtime.)

- [ ] **Step 6: Commit**

```bash
git add streamlit_app/pages/1_New_Adjustment.py
git commit -m "feat(direct-adj): scope-aware render_direct_form with mandatory Reason"
```

---

## Task 5: Update payload + submit for Direct Adjustment

**Files:**
- Modify: `streamlit_app/pages/1_New_Adjustment.py` (`_build_payload`, `_do_submit`)

- [ ] **Step 1: Replace the VaR Upload branch in `_build_payload`**

Find:

```python
    if cat == "VaR Upload":
        return {
            "cobid":                 wiz["cobid"],
            "process_type":          "VaR",
            "adjustment_type":       "Upload",
            "username":              current_user_name(),
            "source_cobid":          wiz["cobid"],
            "reason":                wiz.get("reason", ""),
            "entity_code":           wiz.get("entity_code", ""),
            "requires_approval":     wiz.get("requires_approval", False),
            "adjustment_occurrence": "ADHOC",
            "file_name":             wiz.get("uploaded_file_name", ""),
            "global_reference":      wiz.get("global_reference", ""),
        }
```

Replace with:

```python
    if cat == "Direct Adjustment":
        return {
            "cobid":                 wiz["cobid"],
            "process_type":          wiz["process_type"],
            "adjustment_type":       "Direct",
            "username":              current_user_name(),
            "source_cobid":          wiz["cobid"],
            "reason":                wiz.get("reason", ""),
            "entity_code":           wiz.get("entity_code", ""),
            "requires_approval":     wiz.get("requires_approval", False),
            "adjustment_occurrence": "ADHOC",
            "file_name":             wiz.get("uploaded_file_name", ""),
            "global_reference":      wiz.get("global_reference", ""),
        }
```

- [ ] **Step 2: Replace the VaR Upload special-case in `_do_submit`**

Find:

```python
        if wiz.get("category") == "VaR Upload" and wiz.get("uploaded_df") is not None:
            adj_id = str(_uuid.uuid4())
            payload["adj_id"] = adj_id
            n = _write_var_upload_line_items(adj_id, wiz["uploaded_df"])
            if n == 0:
                return {"status": "Error",
                        "message": "No non-zero VaR values found in CSV data"}
```

Replace with:

```python
        if wiz.get("category") == "Direct Adjustment" and wiz.get("uploaded_df") is not None:
            adj_id = str(_uuid.uuid4())
            payload["adj_id"] = adj_id
            n = _write_direct_line_items(wiz["process_type"], adj_id, wiz["uploaded_df"])
            if n == 0:
                return {"status": "Error",
                        "message": "No non-zero values found in CSV data"}
```

- [ ] **Step 3: Verify it compiles**

Run: `python3 -m py_compile streamlit_app/pages/1_New_Adjustment.py`
Expected: no output (success).

- [ ] **Step 4: Commit**

```bash
git add streamlit_app/pages/1_New_Adjustment.py
git commit -m "feat(direct-adj): payload + submit use selected scope and Direct action"
```

---

## Task 6: Wire routing + preview

**Files:**
- Modify: `streamlit_app/pages/1_New_Adjustment.py` (Step-1 router, Step-2 preview branch)

- [ ] **Step 1: Update the Step-1 category router**

Find:

```python
    elif wiz["category"] == "VaR Upload":
        render_var_upload_form()
    elif wiz["category"] == "Entity Roll":
        render_entity_roll_form()
```

Replace with:

```python
    elif wiz["category"] == "Direct Adjustment":
        render_direct_form()
    elif wiz["category"] == "Entity Roll":
        render_entity_roll_form()
```

- [ ] **Step 2: Update the Step-2 preview summary branch**

Find the Step-2 branch header:

```python
    if cat == "VaR Upload":
        df_up     = wiz.get("uploaded_df")
        row_count = len(df_up) if df_up is not None else 0
```

Change the condition (the body, which shows the upload summary card + `df_up.head(50)` + the duplicate-replacement banner, is scope-agnostic and stays as-is):

```python
    if cat == "Direct Adjustment":
        df_up     = wiz.get("uploaded_df")
        row_count = len(df_up) if df_up is not None else 0
```

In that branch's summary card, replace the hard-coded `VaR Upload` heading with the scope. Find:

```python
            f'<div><div style="font-weight:700;font-size:1.1rem">VaR Upload</div>'
```

Replace with:

```python
            f'<div><div style="font-weight:700;font-size:1.1rem">'
            f'{wiz.get("process_type","")} — Direct</div>'
```

- [ ] **Step 3: Verify it compiles**

Run: `python3 -m py_compile streamlit_app/pages/1_New_Adjustment.py`
Expected: no output (success).

- [ ] **Step 4: Confirm no stale references remain**

Run: `grep -n "VaR Upload\|render_var_upload_form" streamlit_app/pages/1_New_Adjustment.py`
Expected: only the `_write_var_upload_line_items` definition/usage may match by substring of `var_upload`; there must be **no** remaining `"VaR Upload"` string literal and **no** call to `render_var_upload_form()`. If any remain, fix them.

- [ ] **Step 5: Commit**

```bash
git add streamlit_app/pages/1_New_Adjustment.py
git commit -m "feat(direct-adj): route + preview the Direct Adjustment category"
```

---

## Task 7: Manual verification

**Files:** none (runtime check in the app)

- [ ] **Step 1: Compile both modules**

Run: `python3 -m py_compile streamlit_app/utils/styles.py streamlit_app/pages/1_New_Adjustment.py`
Expected: no output (success).

- [ ] **Step 2: Walk the happy path in the running app (VaR scope)**

In the New Adjustment page:
1. Category step shows **Direct Adjustment** (📥) and **no** VaR Upload.
2. Select Direct Adjustment → scope cards appear (VaR/Stress/FRTB/Sensitivity).
3. Select **FRTB** → sub-types show **FRTB / FRTBDRC / FRTBRRAO** only (no FRTBALL).
4. Select **VaR** → expected-columns banner lists the VaR columns.
5. Paste a valid VaR CSV → preview table + row/column metrics render; COB & Entity auto-fill.
6. Leave Reason blank → "Complete required fields" lists **Reason**; fill it → Continue enables.
7. Continue → Step 2 shows the summary card titled "VaR — Direct" and the data preview.
8. Submit → status returns Pending/Processed (not Error).

- [ ] **Step 3: Verify the data landed (reuse the test harness or query directly)**

After the scope pipeline runs, confirm line items and fact rows exist:

```sql
SELECT COUNT(*) FROM ADJUSTMENT_APP.ADJ_LINE_ITEM li
JOIN ADJUSTMENT_APP.ADJ_HEADER h ON h.ADJ_ID = li.ADJ_ID
WHERE h.GLOBAL_REFERENCE = '<your reference>' AND li.IS_DELETED = FALSE;

SELECT COUNT(*), SUM(PNL_VECTOR_VALUE_IN_USD)
FROM FACT.VAR_MEASURES_ADJUSTMENT
WHERE ADJUSTMENT_ID = (SELECT DIMENSION_ADJ_ID FROM ADJUSTMENT_APP.ADJ_HEADER
                       WHERE GLOBAL_REFERENCE = '<your reference>' AND IS_DELETED = FALSE
                       ORDER BY CREATED_DATE DESC LIMIT 1);
```

Expected: line-item count > 0 and a matching adjustment-table row count.

---

## Notes / Limitations

- Direct upload is end-to-end correct for **VaR** only right now. Stress / FRTB / Sensitivity are selectable and scaffolded with the VaR column set + writer (`DIRECT_SCOPE_CONFIG` placeholders, marked `TODO`); they need real per-scope columns and writers before producing correct fact rows.
- No stored-procedure changes. The existing `Direct` action in `SP_PROCESS_ADJUSTMENT` consumes the line items.
