# Direct Adjustment — Per-Scope Schema Framework (Spec 1 of 2)

**Date:** 2026-06-03
**Status:** Approved (pending spec review)
**Area:** `new_adjustment_db_objects/` (tables, SP), `streamlit_app/pages/1_New_Adjustment.py`
**Follows:** `2026-06-03-direct-adjustment-category-design.md`
**Followed by:** Spec 2 — declarative validation engine (separate spec)

## Summary

The Direct Adjustment category currently uses an in-code placeholder (`DIRECT_SCOPE_CONFIG`) where every scope reuses the VaR columns and the VaR writer, so only VaR is correct end-to-end. This spec replaces that with a **config-driven, JSON-backed per-scope schema framework** so each scope can declare its own CSV layout and have it processed into its own `FACT.*_ADJUSTMENT` table — with no per-scope code in the common case.

Two ideas combine:

1. **Store uploads as semi-structured JSON** — one table row per uploaded CSV line, with a `PAYLOAD VARIANT` holding that line's raw fields. This removes the need for a fixed, wide typed table and naturally absorbs each scope's different layout.
2. **Drive interpretation from a config table** — one row per scope declares how to extract / resolve / map the JSON payload into the scope's fact table. Maintained by **developers** via versioned SQL seed (`CREATE OR ALTER` + seed rows); no Admin editing UI.

## Goals

- Each scope declares its own CSV columns + mapping in config; adding/adjusting a scope is a config-seed change, not a code change (in the common case).
- Uploads stored verbatim as JSON (one row per line) for audit + reprocessing + Spec 2 validation.
- Generic, config-driven extraction engine writes to any scope's `FACT.*_ADJUSTMENT`.
- VaR (the hardest layout — 21-column wide, unpivot + name→ID resolution) migrated onto the framework declaratively, proving coverage. Result must match the current typed VaR path (parity).
- A named-writer escape hatch exists for shapes the declarative engine genuinely cannot express.

## Non-goals (deferred to Spec 2)

- Validation rules (required / type / enum / conditional "mandatory-if" / cross-field), surfacing them to the user, and row-level accept/reject with reasons. Spec 2 reads the same `EXPECTED_COLUMNS` + `PAYLOAD`.
- File-upload-to-stage ingestion for very large loads (current input remains CSV paste).
- Any change to scopes other than wiring the framework + migrating VaR; non-VaR scope column lists are filled in later by developers as config seed rows.

## Architecture

### 1. `ADJUSTMENT_APP.ADJ_LINE_ITEM_JSON` (new table)

One row per uploaded CSV data line.

```sql
CREATE OR ALTER TABLE ADJUSTMENT_APP.ADJ_LINE_ITEM_JSON (
    LINE_ID      NUMBER(38,0) NOT NULL AUTOINCREMENT,
    ADJ_ID       VARCHAR(36)  NOT NULL,            -- FK to ADJ_HEADER
    ROW_NUM      NUMBER(38,0),                     -- 1-based line order within the upload
    PAYLOAD      VARIANT,                          -- the raw CSV row as a JSON object
    IS_DELETED   BOOLEAN          DEFAULT FALSE,
    RUN_STATUS   VARCHAR(30)      DEFAULT 'Pending',
    CREATED_DATE TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_ADJ_LINE_ITEM_JSON PRIMARY KEY (LINE_ID)
)
COMMENT = 'Direct Adjustment uploads: one row per CSV line, raw fields in PAYLOAD (VARIANT).';
```

Scale note: a `VARIANT` cell is capped at 16 MB compressed, which is irrelevant here because each row holds only one line's fields. Row count is effectively unbounded; the practical ceiling is the CSV-paste UI (low tens of thousands of rows), not storage. The existing `write_pandas` → temp → `INSERT SELECT` path handles bulk writes.

The existing typed `ADJ_LINE_ITEM` table is left untouched (legacy/other use); Direct Adjustment no longer writes to it.

### 2. `ADJUSTMENT_APP.DIRECT_SCOPE_SCHEMA` (new config table)

One row per scope, dev-maintained via seed.

```sql
CREATE OR ALTER TABLE ADJUSTMENT_APP.DIRECT_SCOPE_SCHEMA (
    PROCESS_TYPE      VARCHAR(30)  NOT NULL,   -- VaR | Stress | Sensitivity | FRTB | FRTBDRC | FRTBRRAO
    EXPECTED_COLUMNS  VARIANT,                 -- [{ "name":..., "type":..., "required":bool }]
    UNPIVOT           VARIANT,                 -- nullable wide->long directive (see below)
    FACT_MAPPING      VARIANT,                 -- [{ "payload_field":..., "target_column":..., "type":... }]
    RESOLUTIONS       VARIANT,                 -- [{ "source_field":..., "dimension_table":..., "match_column":..., "key_column":..., "target_column":... }]
    METRIC_FIELD      VARCHAR(100),            -- payload field (post-unpivot) → local metric
    METRIC_USD_FIELD  VARCHAR(100),            -- payload field (post-unpivot) → USD metric
    WRITER_OVERRIDE   VARCHAR(100),            -- nullable: name of a per-scope Python extractor (escape hatch)
    IS_ACTIVE         BOOLEAN          DEFAULT TRUE,
    CREATED_DATE      TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_DIRECT_SCOPE_SCHEMA PRIMARY KEY (PROCESS_TYPE)
)
COMMENT = 'Per-scope Direct Adjustment schema: how to extract/resolve/map JSON payload into the scope fact table.';
```

Field contracts:
- **`EXPECTED_COLUMNS`** — declared CSV columns; drives the page's expected-columns banner and a basic presence check. `{name, type, required}`.
- **`UNPIVOT`** (nullable) — for wide layouts. Shape: `{ "measure_map": { "<csv_col>": "<measure_value>", ... }, "measure_name_field": "<out field>", "value_field": "<out field>" }`. Each listed CSV column becomes a separate logical row whose `measure_name_field` = the mapped measure value and `value_field` = the cell value; non-measure payload fields carry through unchanged. Null ⇒ no unpivot (long format already).
- **`FACT_MAPPING`** — maps payload fields (post-unpivot) to `FACT.*_ADJUSTMENT` columns with a target type. Fields not mapped and not resolved fall back to the existing defaults (`-1` for `*_KEY`/`*_ID`, NULL otherwise; `IS_OFFICIAL_SOURCE` defaults TRUE), preserving current behavior.
- **`RESOLUTIONS`** — declarative code→key lookups: for each, the engine joins `dimension_table` on `match_column = <source_field value>` and writes `key_column` into `target_column`.
- **`METRIC_FIELD` / `METRIC_USD_FIELD`** — which payload field supplies the adjustment value(s). If only USD exists, both point to the same field (matches current VaR writer).
- **`WRITER_OVERRIDE`** — escape hatch; when set, the engine calls the named Python function instead of the declarative path.

### 3. Write path (Streamlit) — generic, no per-scope code

In `render_direct_form` / `_do_submit`:
1. Parse the pasted CSV into a DataFrame (as today).
2. Build one JSON object per row (column → value) plus `ROW_NUM`, `ADJ_ID`.
3. Bulk-write to `ADJ_LINE_ITEM_JSON` (`write_pandas` to a temp table with a `PAYLOAD` VARIANT via `PARSE_JSON`, then `INSERT SELECT`).
4. The page reads `DIRECT_SCOPE_SCHEMA.EXPECTED_COLUMNS` for the selected scope to render the expected-columns banner and warn on missing/extra columns (replacing the in-code `DIRECT_SCOPE_CONFIG` dict).

`_write_var_upload_line_items` and the in-code `DIRECT_SCOPE_CONFIG`/`_write_direct_line_items` placeholder are removed; the generic JSON writer replaces them.

### 4. Process path (SP_PROCESS_ADJUSTMENT — Direct branch rewritten)

For a Direct adjustment:
1. Read `ADJ_LINE_ITEM_JSON` rows for the `ADJ_ID` (not deleted).
2. Load `DIRECT_SCOPE_SCHEMA` for the scope.
3. If `WRITER_OVERRIDE` set → call that function and skip the rest.
4. Else (declarative engine): optional **unpivot** → **field mapping** (payload → fact columns, typed) → **resolutions** (DIMENSION joins code→key) → **metric assignment** (`METRIC_FIELD`/`METRIC_USD_FIELD` → the scope's metric columns) → defaults for unmapped NOT-NULL columns → insert into `FACT.*_ADJUSTMENT` (reuse the existing temp-table + `INSERT SELECT` + `DIMENSION.ADJUSTMENT` id + `RECORD_COUNT` mechanics).

The existing `check_columns` mapping is replaced by the config-driven mapping. The Scale/Flatten/Roll and EntityRoll paths are unchanged.

### 5. VaR worked example (declarative `UNPIVOT`)

VaR CSV: `COBId, EntityCode, SourceSystemCode, BookCode, CurrencyCode, ScenarioDate, TradeCode, AllVaR … ParCreditSpreadVaR (21 measures), Category, Detail`.

- `UNPIVOT.measure_map` maps each of the 21 measure columns to its `VAR_SUB_COMPONENT` name (e.g. `"AllVaR" → "ALL VAR"`), producing one logical row per non-zero measure with `measure_name_field` = the sub-component name and `value_field` = the value.
- `RESOLUTIONS` resolves `VAR_SUB_COMPONENT_ID` from `DIMENSION.VAR_SUB_COMPONENT` matching the sub-component name.
- `FACT_MAPPING` maps the carried-through dimension fields (`EntityCode`→`ENTITY_CODE`, etc.) to the fact columns; `METRIC_FIELD = METRIC_USD_FIELD = value_field`.
- Acceptance: the rows written to `FACT.VAR_MEASURES_ADJUSTMENT` via this declarative path must match those produced by the current typed VaR writer for the same input (parity test).

## Error handling

- Missing `DIRECT_SCOPE_SCHEMA` row for the scope, or missing `FACT_MAPPING` → mark the adjustment `Failed` with a clear `ERRORMESSAGE` (consistent with existing SP_PROCESS failure handling).
- Unresolvable resolution (no DIMENSION match) → the key takes the default (`-1`) as today; row still written. (Spec 2 will add validation to reject such rows up front.)
- Zero usable rows → return the existing "No non-zero values found" style error.

## Testing

- Deploy both tables + the VaR `DIRECT_SCOPE_SCHEMA` seed (`CREATE OR ALTER`).
- `python3 -m py_compile` the page; extract + `py_compile` the SP Python handler.
- **Parity (manual, Snowflake):** run a VaR Direct upload through the new JSON+config path and confirm `ADJ_LINE_ITEM_JSON` is populated and `FACT.VAR_MEASURES_ADJUSTMENT` matches the current typed-path output for the same CSV.
- One non-VaR scope can be added as a config-only smoke test once its columns are known (out of scope here).

## Relationship to existing code

- Removes: in-code `DIRECT_SCOPE_CONFIG`, `_direct_cfg`, `_write_direct_line_items`, `_write_var_upload_line_items` (Direct no longer uses the typed table).
- Keeps: `render_direct_form` scope selection, mandatory fields, routing, preview (banner now sourced from `DIRECT_SCOPE_SCHEMA`).
- No change to Scale/Flatten/Roll/EntityRoll processing.

## Related
- Direct category: [[2026-06-03-direct-adjustment-category-design]]
- Roll/preview/processing background: [[project_roll_semantics]]
