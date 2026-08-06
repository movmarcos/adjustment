# Direct Grid Entry + Templates + Field Coverage — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an editable-grid entry mode and per-scope CSV templates to Direct Adjustment, and grow field coverage to the agreed per-scope lists (TENOR/CURVE for Sensitivity, PCA for Sensitivity+Stress, optional per-row REASON).

**Architecture:** `DIRECT_ACCEPTED_COLUMNS` gains `DISPLAY_ORDER` (canonical rows with an order appear in grid/template; NULL-order canonicals stay CSV-only). Staging/views/engine each gain the new fields' link in the chain. The grid (`st.data_editor`) is a third input door that feeds the existing stage→validate→per-row-submit pipeline unchanged.

**Tech Stack:** Snowflake SQL, Python/Snowpark SP, Streamlit-in-Snowflake (`st.data_editor`), pandas.

**Spec:** `docs/superpowers/specs/2026-08-06-direct-grid-entry-design.md`

## Global Constraints

- Gates: `python3 -m py_compile streamlit_app/pages/1_New_Adjustment.py`; 05 handler gate `awk 'BEGIN{f=0} /^\$\$;?$/{f=!f; next} f' new_adjustment_db_objects/05_sp_process_adjustment.sql > /tmp/proc.py && python3 -m py_compile /tmp/proc.py && echo "05 OK"`. No DB/SiS runtime — static reading is the SQL gate; manual acceptance is Task 6.
- New stage column widths mirror ADJ_HEADER: TENOR_CODE VARCHAR(10), CURVE_CODE VARCHAR(50), PRODUCT_CATEGORY_ATTRIBUTES VARCHAR(255), REASON VARCHAR(1000).
- Tenor dimension convention: `DIMENSION.TENOR_CURRENCY.TENOR_CURRENCY_CODE = CONCAT(tenor, '_', COALESCE(currency, 'USD'))`. Curve: `DIMENSION.CURVE_CURRENCY.CURVE_CODE`. PCA: space-insensitive `DIMENSION.PRODUCT_CATEGORY_ATTRIBUTES.PCA_CONCAT_KEY = REPLACE(value, ' ', '')`.
- Grid/template column sets per scope = canonical config rows with `DISPLAY_ORDER NOT NULL`, ascending (exact orders in Task 1). DEPARTMENT_CODE/STRATEGY keep NULL order for Sensitivity/Stress (CSV-accepted, not displayed).
- RECORD_COUNT and SCALE_FACTOR are NOT entry fields; COBID stays batch-level; per-row REASON overrides batch reason when non-blank.
- Commit per task with the repo's Claude co-author trailer.

---

### Task 1: Config + staging DDL and seeds

**Files:** Modify `new_adjustment_db_objects/01_tables.sql`

**Interfaces (produces):** `ADJ_DIRECT_STAGE` + `TENOR_CODE, CURVE_CODE, PRODUCT_CATEGORY_ATTRIBUTES, REASON`; `DIRECT_ACCEPTED_COLUMNS.DISPLAY_ORDER NUMBER(3,0)`; seeds for the new fields + orders. Consumed by Tasks 2, 4, 5.

- [ ] **Step 1:** In `ADJ_DIRECT_STAGE`, add the new columns at the END of the column list, after `CREATED_DATE` and BEFORE the `CONSTRAINT` line (CREATE OR ALTER requires new columns appended after existing ones):

```sql
    TENOR_CODE          VARCHAR(10),
    CURVE_CODE          VARCHAR(50),
    PRODUCT_CATEGORY_ATTRIBUTES VARCHAR(255),
    REASON              VARCHAR(1000),
```

- [ ] **Step 2:** In `DIRECT_ACCEPTED_COLUMNS`, add before the CONSTRAINT line:

```sql
    DISPLAY_ORDER  NUMBER(3,0),               -- grid/template position; NULL = CSV-only
```

- [ ] **Step 3:** Replace the seed MERGE with one whose source also carries `DISPLAY_ORDER` (matched-update sets it). Canonical orders — Sensitivity: ENTITY_CODE 10, SOURCE_SYSTEM_CODE 20, BOOK_CODE 30, TENOR_CODE 40, CURRENCY_CODE 50, CURVE_CODE 60, INSTRUMENT_CODE 70, MEASURE_TYPE_CODE 80, VALUE_USD 90, REASON 100, TRADE_TYPOLOGY 110, TRADE_CODE 120, PRODUCT_CATEGORY_ATTRIBUTES 130. Stress: ENTITY_CODE 10, SOURCE_SYSTEM_CODE 20, BOOK_CODE 30, CURRENCY_CODE 40, INSTRUMENT_CODE 50, VALUE_USD 60, REASON 70, TRADE_TYPOLOGY 80, TRADE_CODE 90, SIMULATION_NAME 100, SIMULATION_SOURCE 110, PRODUCT_CATEGORY_ATTRIBUTES 120. VaR: ENTITY_CODE 10, SOURCE_SYSTEM_CODE 20, DEPARTMENT_CODE 30, BOOK_CODE 40, TRADE_CODE 50, TRADE_TYPOLOGY 60, STRATEGY 70, INSTRUMENT_CODE 80, CURRENCY_CODE 90, VALUE_USD 100, REASON 110. FRTB/FRTBDRC/FRTBRRAO: as VaR but MEASURE_TYPE_CODE 95. Aliases: DISPLAY_ORDER NULL. New accepted rows (canonical + aliases, IS_REQUIRED FALSE): Sensitivity TENOR_CODE/TENOR, CURVE_CODE/CURVE; Sensitivity+Stress PRODUCT_CATEGORY_ATTRIBUTES/PCA; ALL six scopes REASON. Keep the MERGE idempotent (`ON PROCESS_TYPE, ACCEPTED_NAME`; matched update sets STAGE_COLUMN, IS_REQUIRED, IS_ACTIVE=TRUE, DISPLAY_ORDER). Implementation freedom: restructure the source query as scope-specific UNION blocks if the CROSS JOIN + orders gets unwieldy — the net rows/orders above are the requirement.

- [ ] **Step 4:** Verify: `grep -c "DISPLAY_ORDER" new_adjustment_db_objects/01_tables.sql` ≥ 3; `grep -c "REASON" ` within the seed block ≥ 6.
- [ ] **Step 5:** Commit `feat(direct): grid/template config — DISPLAY_ORDER, tenor/curve/PCA/reason fields`.

### Task 2: Validation view additions

**Files:** Modify `new_adjustment_db_objects/13_direct_validation.sql`

- [ ] **Step 1:** In `VW_DIRECT_VALIDATE_SENSITIVITY`'s ARRAY_CONSTRUCT, after the INSTRUMENT check add:

```sql
        IFF(s.TENOR_CODE IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM DIMENSION.TENOR_CURRENCY tc
                WHERE UPPER(tc.TENOR_CURRENCY_CODE) =
                      UPPER(CONCAT(s.TENOR_CODE, '_', COALESCE(s.CURRENCY_CODE, 'USD')))),
            'Unknown TENOR_CODE (for currency): ' || s.TENOR_CODE, NULL),
        IFF(s.CURVE_CODE IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM DIMENSION.CURVE_CURRENCY cc
                WHERE UPPER(cc.CURVE_CODE) = UPPER(s.CURVE_CODE)),
            'Unknown CURVE_CODE: ' || s.CURVE_CODE, NULL),
        IFF(s.PRODUCT_CATEGORY_ATTRIBUTES IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM DIMENSION.PRODUCT_CATEGORY_ATTRIBUTES pca
                WHERE UPPER(REPLACE(pca.PCA_CONCAT_KEY, ' ', '')) =
                      UPPER(REPLACE(s.PRODUCT_CATEGORY_ATTRIBUTES, ' ', ''))),
            'Unknown PRODUCT_CATEGORY_ATTRIBUTES: ' || s.PRODUCT_CATEGORY_ATTRIBUTES, NULL),
```

- [ ] **Step 2:** In `VW_DIRECT_VALIDATE_STRESS`, add ONLY the PCA check (same block), after the pair-wise simulation check.
- [ ] **Step 3:** Comma/paren check; the other four views untouched. Verify `grep -c "PRODUCT_CATEGORY_ATTRIBUTES" new_adjustment_db_objects/13_direct_validation.sql` shows hits in exactly two views.
- [ ] **Step 4:** Commit `feat(direct): tenor/curve/PCA validation rules (Sens; PCA also Stress)`.

### Task 3: Engine key resolution

**Files:** Modify `new_adjustment_db_objects/05_sp_process_adjustment.sql` (Direct branch `_direct_expr` `fixed` dict)

- [ ] **Step 1:** Add three entries (mirroring the Scale path's dim conventions at ~:1362-1390):

```python
                    'TENOR_CURRENCY_KEY': ("COALESCE((SELECT MAX(tc.TENOR_CURRENCY_KEY) "
                                            "FROM DIMENSION.TENOR_CURRENCY tc "
                                            "WHERE UPPER(tc.TENOR_CURRENCY_CODE) = "
                                            "UPPER(CONCAT(h.TENOR_CODE, '_', COALESCE(h.CURRENCY_CODE, 'USD')))), -1)"),
                    'CURVE_CURRENCY_KEY': ("COALESCE((SELECT MAX(cc.CURVE_CURRENCY_KEY) "
                                            "FROM DIMENSION.CURVE_CURRENCY cc "
                                            "WHERE UPPER(cc.CURVE_CODE) = UPPER(h.CURVE_CODE)), -1)"),
                    'PRODUCT_CATEGORY_ATTRIBUTES_KEY': ("COALESCE((SELECT MAX(pca.PRODUCT_CATEGORY_ATTRIBUTES_KEY) "
                                            "FROM DIMENSION.PRODUCT_CATEGORY_ATTRIBUTES pca "
                                            "WHERE UPPER(REPLACE(pca.PCA_CONCAT_KEY, ' ', '')) = "
                                            "UPPER(REPLACE(h.PRODUCT_CATEGORY_ATTRIBUTES, ' ', ''))), -1)"),
```

NOTE: `CONCAT(h.TENOR_CODE, ...)` with NULL tenor yields NULL → no match → −1, same as today; keep it simple.
- [ ] **Step 2:** 05 gate → `05 OK`. Commit `feat(engine): Direct resolves tenor/curve/PCA keys`.

### Task 4: App field plumbing (CSV path picks up the new fields)

**Files:** Modify `streamlit_app/pages/1_New_Adjustment.py`

- [ ] **Step 1:** `_DIRECT_STAGE_COLS` (:1041) gains `"TENOR_CODE", "CURVE_CODE", "PRODUCT_CATEGORY_ATTRIBUTES", "REASON"` (append after "VALUE_USD" — order must match the INSERT column list, which is generated from this constant, so no other change needed there).
- [ ] **Step 2:** `_direct_row_payload` (:196): add pairs `("TENOR_CODE", "tenor_code"), ("CURVE_CODE", "curve_code"), ("PRODUCT_CATEGORY_ATTRIBUTES", "product_category_attributes")` to the loop list. REASON is NOT in the loop: after the loop add

```python
    row_reason = row.get("REASON")
    if not _direct_cell_blank(row_reason):
        p["reason"] = str(row_reason).strip()
```

(the dict already carries the batch `"reason"` — the override must come after it is set).
- [ ] **Step 3:** `_accepted_columns` query gains `DISPLAY_ORDER` and returns a third element: ordered canonical list `[(stage_col, display_order), ...]` where order is not NULL, ascending — new signature `(alias_map, required, ordered_cols)`; update its call sites.
- [ ] **Step 4:** Compile gate. Commit `feat(app): Direct CSV accepts tenor/curve/PCA + per-row reason`.

### Task 5: Grid mode + template download

**Files:** Modify `streamlit_app/pages/1_New_Adjustment.py` (render_direct_form region ~:1470+)

- [ ] **Step 1:** Input-mode selector gains "Enter in grid". In grid mode render:

```python
        ordered = _accepted_columns(wiz["process_type"])[2]     # [(stage_col, ord), ...]
        grid_cols = [c for c, _ in ordered]
        col_cfg = {}
        for c in grid_cols:
            if c == "VALUE_USD":
                col_cfg[c] = st.column_config.NumberColumn("VALUE_USD (required)", format="%.6f")
            elif c == "ENTITY_CODE":
                col_cfg[c] = st.column_config.SelectboxColumn("ENTITY_CODE (required)", options=_entity_options())
            elif c == "BOOK_CODE":
                col_cfg[c] = st.column_config.SelectboxColumn("BOOK_CODE", options=_book_options(None, None))
            elif c == "MEASURE_TYPE_CODE":
                col_cfg[c] = st.column_config.SelectboxColumn("MEASURE_TYPE_CODE", options=_measure_type_options(wiz["process_type"]))
            elif c == "SIMULATION_NAME":
                col_cfg[c] = st.column_config.SelectboxColumn("SIMULATION_NAME", options=_sim_name_options(None))
            elif c == "SIMULATION_SOURCE":
                col_cfg[c] = st.column_config.SelectboxColumn("SIMULATION_SOURCE", options=_sim_source_options())
            elif c == "REASON":
                col_cfg[c] = st.column_config.TextColumn("REASON", help="Blank = use the batch reason below")
            else:
                col_cfg[c] = st.column_config.TextColumn(c)
        gdf = st.data_editor(
            wiz.get("direct_grid_df") if wiz.get("direct_grid_df") is not None
            else pd.DataFrame(columns=grid_cols),
            num_rows="dynamic", column_config=col_cfg,
            use_container_width=True, key=_k("direct_grid"))
        wiz["direct_grid_df"] = gdf
```

Then a "Validate rows" button: drop empty rows (`gdf.dropna(how="all")`), and feed the result into the SAME staging path the CSV route uses (fresh batch id, `_delete_direct_batch` of the old, `_stage_direct_batch`, `_direct_validation`, verdict preview) — reuse the existing code by extracting the current stage-and-validate steps into a helper `_stage_and_validate(ndf)` if they are inline, or call the existing functions directly in the same order the CSV path does. The `_direct_sig` signature for grid mode = hash of the grid dataframe content (`pd.util.hash_pandas_object(gdf).sum()`), so edits invalidate verdicts. Submit path: unchanged (the staged batch + verdicts drive it, regardless of input mode).
- [ ] **Step 2:** Grid dataframe state resets where the other direct state resets (category/scope switch, after submit). Add `"direct_grid_df": None` to those wiz.update dicts.
- [ ] **Step 3:** Template download in Paste/Upload modes:

```python
        _tmpl_cols = [c for c, _ in _accepted_columns(wiz["process_type"])[2]]
        if _tmpl_cols:
            st.download_button(
                "Download CSV template", data=",".join(_tmpl_cols) + "\n",
                file_name=f"direct_{wiz['process_type'].lower()}_template.csv",
                mime="text/csv", key=_k("direct_tmpl"))
```

(headers only — an example row of fake codes would fail validation and confuse more than help; the grid mode is the guided path).
- [ ] **Step 4:** Completion checks: grid mode counts as "CSV parsed" when a validated batch exists (reuse the same wiz keys — staging already sets them).
- [ ] **Step 5:** Compile gate; self-review that Paste/Upload modes are pixel-unchanged apart from the template button. Commit `feat(app): Direct grid entry mode + per-scope CSV template download`.

### Task 6: Manual acceptance (Snowflake dev)

Deploy: `01_tables.sql` (stage/config changes + seeds) → `13_direct_validation.sql` → `05` SP → app page. Then the spec's acceptance list: Sensitivity grid 13 ordered columns with dropdowns + real TENOR_CURRENCY_KEY on the fact row; Stress grid 12 columns + PCA space-insensitive match; mixed-validity batch → ✓-only submission with per-row reasons; template round-trip; CSV route accepts new columns by name.
