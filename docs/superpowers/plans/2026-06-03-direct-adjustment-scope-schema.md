# Direct Adjustment — Per-Scope Schema Framework — Implementation Plan (Spec 1 of 2)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Direct Adjustment per-scope by storing uploads as JSON (one row per CSV line) and driving extraction into each scope's `FACT.*_ADJUSTMENT` from a config table, with VaR migrated onto it declaratively.

**Architecture:** Two new tables — `ADJ_LINE_ITEM_JSON` (one row/line, `PAYLOAD VARIANT`) and `DIRECT_SCOPE_SCHEMA` (per-scope, dev-seeded config). The Streamlit write path stores the pasted CSV as JSON rows; `SP_PROCESS_ADJUSTMENT`'s Direct branch is rewritten to read the JSON + the scope's config and build a dynamic SQL pipeline (optional unpivot → field extraction → DIMENSION resolutions → metric → insert). A named-writer escape hatch (`WRITER_OVERRIDE`) covers shapes the declarative engine can't express.

**Tech Stack:** Snowflake SQL (VARIANT/semi-structured), Python/Snowpark stored procedure, Streamlit-in-Snowflake, pandas.

**Spec:** `docs/superpowers/specs/2026-06-03-direct-adjustment-scope-schema-design.md`

**Testing note:** Streamlit/Snowflake modules can't be imported outside the SiS runtime, so there is no unit-test harness. Per-task gates: `python3 -m py_compile` (page) and extract-and-`py_compile` the SP Python handler. The acceptance gate is a **manual Snowflake parity test** (Task 5): a VaR Direct upload through the new path must reproduce the rows the current typed VaR path produces. Run commands from repo root `/Users/marcosmagri/Documents/MUFG/adjustment`.

To extract + compile the SP handler:
```bash
awk 'BEGIN{f=0} /^\$\$;?$/{f=!f; next} f' new_adjustment_db_objects/05_sp_process_adjustment.sql | sed '/^\$\$;/d' > /tmp/proc.py && python3 -m py_compile /tmp/proc.py && echo "SP HANDLER OK"
```

---

## File Structure

- **Modify** `new_adjustment_db_objects/01_tables.sql` — add `ADJ_LINE_ITEM_JSON` table, `DIRECT_SCOPE_SCHEMA` table, and the VaR seed row.
- **Modify** `new_adjustment_db_objects/05_sp_process_adjustment.sql` — rewrite the Direct branch + add config-driven engine helpers in the handler.
- **Modify** `streamlit_app/pages/1_New_Adjustment.py` — replace the in-code placeholder layer with a config-sourced banner + a generic JSON writer to `ADJ_LINE_ITEM_JSON`.

---

## Task 1: Create `ADJ_LINE_ITEM_JSON` table

**Files:** Modify `new_adjustment_db_objects/01_tables.sql`

- [ ] **Step 1: Add the table**

Add this block immediately **after** the `CREATE OR ALTER TABLE ADJUSTMENT_APP.ADJ_LINE_ITEM (...)` statement and its `COMMENT` line (so the two line-item tables sit together):

```sql

-- ═══════════════════════════════════════════════════════════════════════════
-- ADJ_LINE_ITEM_JSON — Direct Adjustment uploads (semi-structured)
-- One row per uploaded CSV line; raw fields live in PAYLOAD (VARIANT).
-- Per-scope interpretation is driven by DIRECT_SCOPE_SCHEMA at processing time.
-- ═══════════════════════════════════════════════════════════════════════════
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

- [ ] **Step 2: Verify SQL is well-formed (lightweight check — no DB)**

Run: `grep -n "ADJ_LINE_ITEM_JSON" new_adjustment_db_objects/01_tables.sql`
Expected: shows the `CREATE OR ALTER TABLE ... ADJ_LINE_ITEM_JSON` line and the PK constraint line.

- [ ] **Step 3: Commit**

```bash
git add new_adjustment_db_objects/01_tables.sql
git commit -m "feat(direct-adj): add ADJ_LINE_ITEM_JSON table for JSON uploads"
```

---

## Task 2: Create `DIRECT_SCOPE_SCHEMA` table + VaR seed

**Files:** Modify `new_adjustment_db_objects/01_tables.sql`

- [ ] **Step 1: Add the config table**

Add this block immediately **after** the `ADJ_LINE_ITEM_JSON` table you added in Task 1:

```sql

-- ═══════════════════════════════════════════════════════════════════════════
-- DIRECT_SCOPE_SCHEMA — per-scope Direct Adjustment schema (dev-maintained)
-- Declares how to extract/resolve/map a scope's JSON payload into its fact table.
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR ALTER TABLE ADJUSTMENT_APP.DIRECT_SCOPE_SCHEMA (
    PROCESS_TYPE      VARCHAR(30)  NOT NULL,
    EXPECTED_COLUMNS  VARIANT,      -- [{ "name":..., "type":..., "required":bool }]
    UNPIVOT           VARIANT,      -- nullable {measure_map:{csv_col:measure_value}, measure_name_field, value_field}
    FACT_MAPPING      VARIANT,      -- [{ "payload_field":..., "target_column":..., "type":... }]
    RESOLUTIONS       VARIANT,      -- [{ "source_field":..., "dimension_table":..., "match_column":..., "key_column":..., "target_column":... }]
    METRIC_FIELD      VARCHAR(100),
    METRIC_USD_FIELD  VARCHAR(100),
    WRITER_OVERRIDE   VARCHAR(100),
    IS_ACTIVE         BOOLEAN          DEFAULT TRUE,
    CREATED_DATE      TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_DIRECT_SCOPE_SCHEMA PRIMARY KEY (PROCESS_TYPE)
)
COMMENT = 'Per-scope Direct Adjustment schema: how to extract/resolve/map JSON payload into the scope fact table.';
```

- [ ] **Step 2: Seed the VaR scope**

Add this block immediately after the table definition. The `measure_map` keys are the 21 VaR CSV columns; the values are the matching `DIMENSION.VAR_SUB_COMPONENT.VAR_SUB_COMPONENT_NAME` values (the current writer derives these as `DB_COL.replace('_',' ')`, e.g. `AllVaR`→`ALL VAR`).

```sql
DELETE FROM ADJUSTMENT_APP.DIRECT_SCOPE_SCHEMA WHERE PROCESS_TYPE = 'VaR';
INSERT INTO ADJUSTMENT_APP.DIRECT_SCOPE_SCHEMA
    (PROCESS_TYPE, EXPECTED_COLUMNS, UNPIVOT, FACT_MAPPING, RESOLUTIONS,
     METRIC_FIELD, METRIC_USD_FIELD, WRITER_OVERRIDE, IS_ACTIVE)
SELECT
    'VaR',
    PARSE_JSON('[
        {"name":"COBId","type":"number","required":true},
        {"name":"EntityCode","type":"string","required":true},
        {"name":"SourceSystemCode","type":"string","required":false},
        {"name":"BookCode","type":"string","required":false},
        {"name":"CurrencyCode","type":"string","required":false},
        {"name":"ScenarioDate","type":"number","required":false},
        {"name":"TradeCode","type":"string","required":false},
        {"name":"AllVaR","type":"number","required":false},
        {"name":"AllVaRSkew","type":"number","required":false},
        {"name":"BasisVaR","type":"number","required":false},
        {"name":"BondAssetSpreadVaR","type":"number","required":false},
        {"name":"CrossEffects","type":"number","required":false},
        {"name":"EquityPriceVaR","type":"number","required":false},
        {"name":"EquityVegaVaR","type":"number","required":false},
        {"name":"FXRateVaR","type":"number","required":false},
        {"name":"FXVolatilityVaR","type":"number","required":false},
        {"name":"IRCapVolVaR","type":"number","required":false},
        {"name":"IRCapVolVaRSkew","type":"number","required":false},
        {"name":"IRSkewVolVaR","type":"number","required":false},
        {"name":"IRSwaptionVolVaR","type":"number","required":false},
        {"name":"IRSwaptionVolVaRSkew","type":"number","required":false},
        {"name":"InflationRateCurveVaR","type":"number","required":false},
        {"name":"InflationVolVaR","type":"number","required":false},
        {"name":"InterestRateCurveVaR","type":"number","required":false},
        {"name":"InterestRateVegaVaR","type":"number","required":false},
        {"name":"MTGSprdVaR","type":"number","required":false},
        {"name":"OASVaR","type":"number","required":false},
        {"name":"ParCreditSpreadVaR","type":"number","required":false},
        {"name":"Category","type":"string","required":false},
        {"name":"Detail","type":"string","required":false}
    ]'),
    PARSE_JSON('{
        "measure_map":{
            "AllVaR":"ALL VAR","AllVaRSkew":"ALL VAR SKEW","BasisVaR":"BASIS VAR",
            "BondAssetSpreadVaR":"BOND ASSET SPREAD VAR","CrossEffects":"CROSS EFFECTS",
            "EquityPriceVaR":"EQUITY PRICE VAR","EquityVegaVaR":"EQUITY VEGA VAR",
            "FXRateVaR":"FX RATE VAR","FXVolatilityVaR":"FX VOLATILITY VAR",
            "IRCapVolVaR":"IR CAP VOL VAR","IRCapVolVaRSkew":"IR CAP VOL VAR SKEW",
            "IRSkewVolVaR":"IR SKEW VOL VAR","IRSwaptionVolVaR":"IR SWAPTION VOL VAR",
            "IRSwaptionVolVaRSkew":"IR SWAPTION VOL VAR SKEW",
            "InflationRateCurveVaR":"INFLATION RATE CURVE VAR","InflationVolVaR":"INFLATION VOL VAR",
            "InterestRateCurveVaR":"INTEREST RATE CURVE VAR","InterestRateVegaVaR":"INTEREST RATE VEGA VAR",
            "MTGSprdVaR":"MTG SPRD VAR","OASVaR":"OAS VAR","ParCreditSpreadVaR":"PAR CREDIT SPREAD VAR"
        },
        "measure_name_field":"VAR_SUB_COMPONENT_NAME",
        "value_field":"ADJ_VALUE"
    }'),
    PARSE_JSON('[
        {"payload_field":"COBId","target_column":"COBID","type":"number"},
        {"payload_field":"EntityCode","target_column":"ENTITY_CODE","type":"string"},
        {"payload_field":"SourceSystemCode","target_column":"SOURCE_SYSTEM_CODE","type":"string"},
        {"payload_field":"CurrencyCode","target_column":"CURRENCY_CODE","type":"string"},
        {"payload_field":"ScenarioDate","target_column":"SCENARIO_DATE_ID","type":"number"}
    ]'),
    PARSE_JSON('[
        {"source_field":"VAR_SUB_COMPONENT_NAME","dimension_table":"DIMENSION.VAR_SUB_COMPONENT",
         "match_column":"VAR_SUB_COMPONENT_NAME","key_column":"VAR_SUB_COMPONENT_ID",
         "target_column":"VAR_SUBCOMPONENT_ID"}
    ]'),
    'ADJ_VALUE', 'ADJ_VALUE', NULL, TRUE;
```

(Note: COBID is also the partition column on the fact table; the engine in Task 4 sources it from `target_cobid`, so its `FACT_MAPPING` entry is harmless/ignored for the COBID write — it is listed for completeness. `FACT_MAPPING` deliberately omits `BookCode`/`TradeCode` because the VaR fact-adjustment table keys those via `BOOK_KEY`/`TRADE_KEY` which the current typed path leaves at the `-1` default for Direct uploads; matching that default preserves parity.)

- [ ] **Step 3: Verify**

Run: `grep -n "DIRECT_SCOPE_SCHEMA\|measure_name_field\|VAR_SUBCOMPONENT_ID" new_adjustment_db_objects/01_tables.sql`
Expected: shows the table, the UNPIVOT `measure_name_field`, and the resolution `target_column` `VAR_SUBCOMPONENT_ID`.

- [ ] **Step 4: Commit**

```bash
git add new_adjustment_db_objects/01_tables.sql
git commit -m "feat(direct-adj): add DIRECT_SCOPE_SCHEMA config table + VaR seed"
```

---

## Task 3: Streamlit write path — JSON writer + config banner

Replace the in-code placeholder layer (`DIRECT_SCOPE_CONFIG`, `_direct_cfg`, `_write_direct_line_items`, `_write_var_upload_line_items`) with: a helper that reads expected columns from `DIRECT_SCOPE_SCHEMA`, and a generic writer that stores the upload as JSON rows.

**Files:** Modify `streamlit_app/pages/1_New_Adjustment.py`

- [ ] **Step 1: Remove the placeholder layer**

Delete the entire `DIRECT_SCOPE_CONFIG` / `_DIRECT_VAR_ENTRY` / `_direct_cfg` / `_write_direct_line_items` block (the block added earlier, located just before `def render_var_upload_form`/`render_direct_form`). Also delete the `def _write_var_upload_line_items(...)` function (it is no longer used — Direct now writes JSON, and nothing else calls it; confirm with `grep -n "_write_var_upload_line_items" streamlit_app/pages/1_New_Adjustment.py` returning no other references before deleting).

- [ ] **Step 2: Add the config reader + JSON writer**

Add these two functions where `_write_var_upload_line_items` used to be (they need `run_query`, `get_session`, `json`, `pd`, all already imported at top of file):

```python
# ── Direct Adjustment: config-driven schema + JSON upload writer ──────────────
def _direct_expected_columns(scope: str) -> list:
    """Return the ordered expected CSV column names for a scope from DIRECT_SCOPE_SCHEMA.
    Empty list if the scope has no config row yet."""
    try:
        rows = run_query(f"""
            SELECT EXPECTED_COLUMNS
            FROM ADJUSTMENT_APP.DIRECT_SCOPE_SCHEMA
            WHERE UPPER(PROCESS_TYPE) = UPPER('{scope.replace("'", "''")}')
              AND IS_ACTIVE = TRUE
        """)
        if not rows or rows[0][0] is None:
            return []
        spec = rows[0][0]
        spec = json.loads(spec) if isinstance(spec, str) else spec
        return [c["name"] for c in spec]
    except Exception:
        return []


def _write_direct_json_rows(adj_id: str, df_csv: pd.DataFrame) -> int:
    """Store each CSV row verbatim as a JSON object in ADJ_LINE_ITEM_JSON.
    Returns the number of rows written."""
    from utils.snowflake_conn import get_session
    session = get_session()
    if df_csv is None or len(df_csv) == 0:
        return 0

    payloads = pd.DataFrame({
        "ADJ_ID":  adj_id,
        "ROW_NUM": range(1, len(df_csv) + 1),
        # one JSON string per row; NaN → None so PARSE_JSON yields null
        "PAYLOAD_TEXT": [json.dumps({k: (None if pd.isna(v) else v) for k, v in rec.items()})
                         for rec in df_csv.to_dict(orient="records")],
    })

    session.write_pandas(
        payloads, table_name="TEMP_DIRECT_JSON_ROWS", schema="ADJUSTMENT_APP",
        auto_create_table=True, overwrite=True, table_type="temporary")
    session.sql("""
        INSERT INTO ADJUSTMENT_APP.ADJ_LINE_ITEM_JSON (ADJ_ID, ROW_NUM, PAYLOAD)
        SELECT ADJ_ID, ROW_NUM, PARSE_JSON(PAYLOAD_TEXT)
        FROM ADJUSTMENT_APP.TEMP_DIRECT_JSON_ROWS
    """).collect()
    return len(payloads)
```

- [ ] **Step 3: Source the banner + validation columns from config in `render_direct_form`**

In `render_direct_form`, replace the config lookup + banner lines:

```python
    cfg          = _direct_cfg(wiz["process_type"])
    expected_cols = cfg["expected"]

    section_title(f"Direct Adjustment — {wiz['process_type']} CSV", "📥")
    _info_banner(
        'Paste a CSV of exact adjustment values. Expected columns: '
        '<code>' + ', '.join(expected_cols) + '</code>.')
```

with:

```python
    expected_cols = _direct_expected_columns(wiz["process_type"])

    section_title(f"Direct Adjustment — {wiz['process_type']} CSV", "📥")
    if expected_cols:
        _info_banner(
            'Paste a CSV of exact adjustment values. Expected columns: '
            '<code>' + ', '.join(expected_cols) + '</code>.')
    else:
        _info_banner(
            f'No upload schema is configured for the <b>{wiz["process_type"]}</b> scope yet. '
            'Paste a CSV; columns will be stored as-is.')
```

(The CSV-parse block already uses `expected_cols`; when it's empty, `missing_cols` will be empty and `extra_cols` lists all columns as "ignored" — acceptable for an unconfigured scope.)

- [ ] **Step 4: Switch the submit to the JSON writer**

In `_do_submit`, replace:

```python
        if wiz.get("category") == "Direct Adjustment" and wiz.get("uploaded_df") is not None:
            adj_id = str(_uuid.uuid4())
            payload["adj_id"] = adj_id
            n = _write_direct_line_items(wiz["process_type"], adj_id, wiz["uploaded_df"])
            if n == 0:
                return {"status": "Error",
                        "message": "No non-zero values found in CSV data"}
```

with:

```python
        if wiz.get("category") == "Direct Adjustment" and wiz.get("uploaded_df") is not None:
            adj_id = str(_uuid.uuid4())
            payload["adj_id"] = adj_id
            n = _write_direct_json_rows(adj_id, wiz["uploaded_df"])
            if n == 0:
                return {"status": "Error",
                        "message": "No rows found in CSV data"}
```

- [ ] **Step 5: Verify it compiles and the old symbols are gone**

Run: `python3 -m py_compile streamlit_app/pages/1_New_Adjustment.py && echo OK`
Expected: `OK`.
Run: `grep -n "DIRECT_SCOPE_CONFIG\|_write_direct_line_items\|_write_var_upload_line_items\|_direct_cfg" streamlit_app/pages/1_New_Adjustment.py || echo "clean"`
Expected: `clean` (no matches).

- [ ] **Step 6: Commit**

```bash
git add streamlit_app/pages/1_New_Adjustment.py
git commit -m "feat(direct-adj): store uploads as JSON; banner from DIRECT_SCOPE_SCHEMA"
```

---

## Task 4: Rewrite SP_PROCESS Direct branch as a config-driven engine

Replace the `Direct` branch (the `if adjustment_action.lower() == 'direct':` block) in `SP_PROCESS_ADJUSTMENT` so it reads `ADJ_LINE_ITEM_JSON` + `DIRECT_SCOPE_SCHEMA` and builds a dynamic SQL pipeline. Keep the surrounding scaffolding (run log, `insert_to_dimension_and_get_ids`, status updates, PowerBI trigger).

**Files:** Modify `new_adjustment_db_objects/05_sp_process_adjustment.sql`

- [ ] **Step 1: Add engine helpers in the handler**

Add these functions inside the `$$ ... $$` handler, immediately **after** the `def check_columns(...)` function definition (they are module-level helpers for the Direct engine). `check_columns` may remain for now (unused by the new path); do not delete it in this task.

```python
def _cfg_list(val):
    """Coerce a VARIANT config value (str or list/dict) to a Python list."""
    if val is None:
        return []
    if isinstance(val, str):
        val = json.loads(val)
    return val if isinstance(val, list) else [val]


def _cfg_obj(val):
    """Coerce a VARIANT config value to a Python dict (or None)."""
    if val is None:
        return None
    if isinstance(val, str):
        val = json.loads(val)
    return val if isinstance(val, dict) else None


def load_direct_schema(session, process_type):
    """Read the DIRECT_SCOPE_SCHEMA row for a scope. Returns a dict or None."""
    rows = session.sql(f"""
        SELECT EXPECTED_COLUMNS, UNPIVOT, FACT_MAPPING, RESOLUTIONS,
               METRIC_FIELD, METRIC_USD_FIELD, WRITER_OVERRIDE
        FROM ADJUSTMENT_APP.DIRECT_SCOPE_SCHEMA
        WHERE UPPER(PROCESS_TYPE) = UPPER('{process_type}') AND IS_ACTIVE = TRUE
    """).collect()
    if not rows:
        return None
    r = rows[0]
    return {
        "unpivot":      _cfg_obj(r["UNPIVOT"]),
        "fact_mapping": _cfg_list(r["FACT_MAPPING"]),
        "resolutions":  _cfg_list(r["RESOLUTIONS"]),
        "metric_field": r["METRIC_FIELD"],
        "metric_usd_field": r["METRIC_USD_FIELD"],
        "writer_override":  r["WRITER_OVERRIDE"],
    }


def _payload_expr(field, ftype):
    """SQL expression to read PAYLOAD:<field> as a typed value."""
    f = field.replace('"', '')
    if ftype == "number":
        return f'TRY_TO_NUMBER(TO_VARCHAR(j.PAYLOAD:"{f}"))'
    return f'TO_VARCHAR(j.PAYLOAD:"{f}")'


def build_direct_extract_sql(cfg, adj_ids_str):
    """Build a SELECT over ADJ_LINE_ITEM_JSON that yields one row per output line
    with: ADJ_ID, each mapped target_column, each resolution source_field, and
    METRIC_VALUE. Applies the optional unpivot (UNION ALL per measure)."""
    fm = cfg["fact_mapping"]
    unpivot = cfg["unpivot"]
    # Non-metric mapped fields carried straight from the payload
    carried = [(m["payload_field"], m["target_column"], m.get("type", "string"))
               for m in fm]

    base_from = (f"FROM ADJUSTMENT_APP.ADJ_LINE_ITEM_JSON j "
                 f"WHERE j.ADJ_ID IN ({adj_ids_str}) AND j.IS_DELETED = FALSE")

    if unpivot:
        legs = []
        name_field  = unpivot["measure_name_field"]
        value_field = unpivot["value_field"]
        for csv_col, measure_value in unpivot["measure_map"].items():
            sel = [f"j.ADJ_ID AS ADJ_ID"]
            for pf, tc, ty in carried:
                sel.append(f"{_payload_expr(pf, ty)} AS {tc}")
            mv = str(measure_value).replace("'", "''")
            sel.append(f"'{mv}' AS {name_field}")
            sel.append(f"{_payload_expr(csv_col, 'number')} AS METRIC_VALUE")
            # METRIC_VALUE = TRY_TO_NUMBER(payload:col); rows where it is 0 or NULL
            # are excluded (NULL <> 0 is NULL → filtered), matching the legacy
            # writer which skipped NaN/zero measures.
            legs.append(
                "SELECT " + ", ".join(sel) + " " + base_from +
                f" AND {_payload_expr(csv_col, 'number')} <> 0")
        return "\n  UNION ALL\n  ".join(legs)
    else:
        sel = ["j.ADJ_ID AS ADJ_ID"]
        for pf, tc, ty in carried:
            sel.append(f"{_payload_expr(pf, ty)} AS {tc}")
        metric_pf = cfg["metric_field"]
        sel.append(f"{_payload_expr(metric_pf, 'number')} AS METRIC_VALUE")
        return "SELECT " + ", ".join(sel) + " " + base_from + \
               f" AND {_payload_expr(metric_pf, 'number')} <> 0"
```

- [ ] **Step 2: Replace the Direct branch body**

Find the Direct branch. It begins:

```python
        if adjustment_action.lower() == 'direct':

            df_adj_direct = df_adj.filter(
                (col('ADJUSTMENT_ACTION') == 'Direct') &
                (col('IS_POSITIVE_ADJUSTMENT') == True)
            )
```

…and ends just before the line:

```python
        # ═════════════════════════════════════════════════════════════════
        # SCALE (Scale / Flatten / Roll) PATH
        # ═════════════════════════════════════════════════════════════════
        elif adjustment_action.lower() == 'scale':
```

Replace the **entire** Direct branch (everything from `if adjustment_action.lower() == 'direct':` down to — but not including — the `# SCALE …` comment / `elif adjustment_action.lower() == 'scale':`) with:

```python
        if adjustment_action.lower() == 'direct':

            df_adj_direct = df_adj.filter(
                (col('ADJUSTMENT_ACTION') == 'Direct') &
                (col('IS_POSITIVE_ADJUSTMENT') == True)
            )
            if df_adj_direct.count() == 0:
                result["message"] = 'No Running Direct adjustments found'
                return json.dumps(result)

            adj_ids = [row["ADJ_ID"] for row in df_adj_direct.select("ADJ_ID").collect()]
            adj_ids_str = ", ".join(f"'{a}'" for a in adj_ids)

            session.sql(f"""
                UPDATE ADJUSTMENT_APP.ADJ_HEADER SET RUN_LOG_ID = {run_log_id}
                WHERE ADJ_ID IN ({adj_ids_str})
            """).collect()

            # DIMENSION.ADJUSTMENT first → get NUMBER ADJUSTMENT_ID per adj
            dim_adj_map = {}
            try:
                dim_adj_map = insert_to_dimension_and_get_ids(session, adj_ids, adj_ids_str)
            except Exception as dim_err:
                print(f"Warning: DIMENSION.ADJUSTMENT insert failed: {dim_err}")
            dim_ids_str = ', '.join(str(v) for v in dim_adj_map.values()) if dim_adj_map else "0"

            # Load per-scope schema config
            cfg = load_direct_schema(session, process_type)
            if cfg is None:
                update_header_status(session, df_adj_direct, cobid, "Failed",
                                     f"No DIRECT_SCOPE_SCHEMA for scope {process_type}")
                log_status_history(session, adj_ids, "Running", "Failed")
                result["message"] = f"No upload schema configured for {process_type}"
                return json.dumps(result)

            # Escape hatch: named per-scope writer
            if cfg.get("writer_override"):
                fn = globals().get(cfg["writer_override"])
                if fn is None:
                    update_header_status(session, df_adj_direct, cobid, "Failed",
                                         f"WRITER_OVERRIDE {cfg['writer_override']} not found")
                    log_status_history(session, adj_ids, "Running", "Failed")
                    result["message"] = f"Writer override {cfg['writer_override']} not found"
                    return json.dumps(result)
                rows_count = fn(session, adj_ids, adj_ids_str, dim_adj_map, cobid,
                                fact_adj_tbl_name, metric_name, metric_usd_name, run_log_id)
            else:
                # ── Declarative engine ───────────────────────────────────
                extract_sql = build_direct_extract_sql(cfg, adj_ids_str)

                # Build the resolution joins + final SELECT columns
                fact_adj_cols_set = set(fact_adj_tbl.columns)
                target_cols = []      # columns we will INSERT
                select_exprs = []     # matching SELECT expressions over the extract CTE `x`
                join_sql = ""
                ri = 0
                resolved_targets = set()
                for res in cfg["resolutions"]:
                    alias = f"d{ri}"; ri += 1
                    src = res["source_field"]
                    tgt = res["target_column"]
                    join_sql += (f"\n  LEFT JOIN {res['dimension_table']} {alias} "
                                 f"ON UPPER({alias}.{res['match_column']}) = UPPER(x.{src})")
                    if tgt in fact_adj_cols_set:
                        target_cols.append(tgt)
                        select_exprs.append(f"COALESCE({alias}.{res['key_column']}, -1) AS {tgt}")
                        resolved_targets.add(tgt)

                # Mapped (carried) columns that exist in the fact adj table
                for m in cfg["fact_mapping"]:
                    tc = m["target_column"]
                    if tc in fact_adj_cols_set and tc not in resolved_targets and tc != "COBID":
                        target_cols.append(tc); select_exprs.append(f"x.{tc} AS {tc}")

                # COBID, ADJUSTMENT_ID, metric, system columns
                target_cols.append("COBID");          select_exprs.append(f"{cobid} AS COBID")
                target_cols.append("ADJUSTMENT_ID")
                select_exprs.append("h.DIMENSION_ADJ_ID AS ADJUSTMENT_ID")
                if metric_name in fact_adj_cols_set:
                    target_cols.append(metric_name);     select_exprs.append(f"x.METRIC_VALUE AS {metric_name}")
                if metric_usd_name in fact_adj_cols_set and metric_usd_name != metric_name:
                    target_cols.append(metric_usd_name); select_exprs.append(f"x.METRIC_VALUE AS {metric_usd_name}")
                elif metric_usd_name in fact_adj_cols_set:
                    if metric_usd_name not in target_cols:
                        target_cols.append(metric_usd_name); select_exprs.append(f"x.METRIC_VALUE AS {metric_usd_name}")
                if "IS_OFFICIAL_SOURCE" in fact_adj_cols_set:
                    target_cols.append("IS_OFFICIAL_SOURCE"); select_exprs.append("TRUE AS IS_OFFICIAL_SOURCE")
                if "RUN_LOG_ID" in fact_adj_cols_set:
                    target_cols.append("RUN_LOG_ID"); select_exprs.append(f"{run_log_id} AS RUN_LOG_ID")
                if "LOAD_TIMESTAMP" in fact_adj_cols_set:
                    target_cols.append("LOAD_TIMESTAMP"); select_exprs.append("CURRENT_TIMESTAMP() AS LOAD_TIMESTAMP")

                # Default every remaining surrogate-key/id fact column to -1, mirroring
                # the legacy check_columns behaviour (these are NOT NULL in the fact
                # tables and must be present even when a Direct upload doesn't supply
                # them). Non-key columns left unlisted default/NULL as before.
                _managed = set(target_cols)
                for c in fact_adj_tbl.columns:
                    if c in _managed:
                        continue
                    if c.split('_')[-1].upper() in ('KEY', 'ID'):
                        target_cols.append(c); select_exprs.append(f"-1 AS {c}")

                # Remove any existing rows for this batch's adjustments, then insert
                session.sql(f"""
                    DELETE FROM {fact_adj_tbl_name}
                    WHERE COBID = {cobid} AND ADJUSTMENT_ID IN ({dim_ids_str})
                """).collect()

                insert_sql = f"""
                    INSERT INTO {fact_adj_tbl_name} ({', '.join(target_cols)})
                    WITH x AS (
                        {extract_sql}
                    )
                    SELECT {', '.join(select_exprs)}
                    FROM x
                    INNER JOIN ADJUSTMENT_APP.ADJ_HEADER h ON h.ADJ_ID = x.ADJ_ID
                    {join_sql}
                """
                session.sql(insert_sql).collect()

                rows_count = session.sql(f"""
                    SELECT COUNT(*) AS CNT FROM {fact_adj_tbl_name}
                    WHERE COBID = {cobid} AND ADJUSTMENT_ID IN ({dim_ids_str})
                """).collect()[0]["CNT"]

            # ── Common post-processing ───────────────────────────────────
            session.sql(f"""
                UPDATE ADJUSTMENT_APP.ADJ_HEADER SET RECORD_COUNT = {rows_count}
                WHERE ADJ_ID IN ({adj_ids_str})
            """).collect()
            if dim_adj_map:
                session.sql(f"""
                    UPDATE DIMENSION.ADJUSTMENT
                    SET RECORD_COUNT = {rows_count}, RUN_STATUS = 'Processed'
                    WHERE ADJUSTMENT_ID IN ({dim_ids_str})
                """).collect()
            update_header_status(session, df_adj_direct, cobid, "Processed")
            log_status_history(session, adj_ids, "Running", "Processed")
            result["rows_inserted"] = rows_count
            result["message"] = "Direct adjustments processed successfully"
            try:
                session.sql(f"""
                    CALL BATCH.LOAD_RUN_LOG_END_WITH_DETAIL({run_log_id}, '{{"status":"Processed"}}')
                """).collect()
            except Exception as rl_err:
                print(f"Warning: Run log close failed: {rl_err}")
            trigger_powerbi_refresh(session, process_type, run_log_id)

```

- [ ] **Step 3: Verify the handler compiles**

Run:
```bash
awk 'BEGIN{f=0} /^\$\$;?$/{f=!f; next} f' new_adjustment_db_objects/05_sp_process_adjustment.sql | sed '/^\$\$;/d' > /tmp/proc.py && python3 -m py_compile /tmp/proc.py && echo "SP HANDLER OK"
```
Expected: `SP HANDLER OK`.

- [ ] **Step 4: Commit**

```bash
git add new_adjustment_db_objects/05_sp_process_adjustment.sql
git commit -m "feat(direct-adj): config-driven Direct engine over ADJ_LINE_ITEM_JSON"
```

---

## Task 5: Deploy + parity verification (manual, Snowflake)

**Files:** none (runtime).

- [ ] **Step 1: Deploy the changed objects**

In Snowflake, run (CREATE OR ALTER — safe to re-run):
- `01_tables.sql` (creates `ADJ_LINE_ITEM_JSON`, `DIRECT_SCOPE_SCHEMA`, seeds VaR)
- `05_sp_process_adjustment.sql` (rewritten Direct branch)

- [ ] **Step 2: Submit a VaR Direct upload via the app**

New Adjustment → Direct Adjustment → VaR → paste a small known VaR CSV (a few rows, a handful of non-zero measures) → fill Reference + Reason → Continue → Submit.

- [ ] **Step 3: Confirm JSON storage**

```sql
SELECT COUNT(*) FROM ADJUSTMENT_APP.ADJ_LINE_ITEM_JSON li
JOIN ADJUSTMENT_APP.ADJ_HEADER h ON h.ADJ_ID = li.ADJ_ID
WHERE h.GLOBAL_REFERENCE = '<your reference>';
```
Expected: one row per pasted CSV line.

- [ ] **Step 4: Run the pipeline and confirm fact rows**

```sql
CALL ADJUSTMENT_APP.SP_RUN_PIPELINE('VaR', '["VaR"]');

SELECT COUNT(*) AS ROWS, SUM(PNL_VECTOR_VALUE_IN_USD) AS TOTAL
FROM FACT.VAR_MEASURES_ADJUSTMENT
WHERE ADJUSTMENT_ID = (SELECT DIMENSION_ADJ_ID FROM ADJUSTMENT_APP.ADJ_HEADER
                       WHERE GLOBAL_REFERENCE = '<your reference>' AND IS_DELETED = FALSE
                       ORDER BY CREATED_DATE DESC LIMIT 1);
```
Expected: one fact row per non-zero measure cell, `VAR_SUBCOMPONENT_ID` resolved (not `-1`), totals equal the sum of the non-zero values you pasted.

- [ ] **Step 5: Parity check vs the old typed path**

For the same input CSV, the row count and per-sub-component totals must match what the previous typed VaR upload produced. If they differ, capture the diff and fix the config/engine before considering the task done. (The `test_scaling_adjustment.sql` harness can be adapted to dump the adjustment rows for comparison.)

---

## Notes / Limitations

- VaR is migrated declaratively (no `WRITER_OVERRIDE`). Non-VaR scopes need their own `DIRECT_SCOPE_SCHEMA` seed rows before they process; until then they store JSON but fail processing with "No upload schema configured" (a clear, intended failure).
- `BOOK_KEY`/`TRADE_KEY` are left at the `-1` default for VaR Direct (matching the current typed path). If business later wants those resolved, add `RESOLUTIONS` entries.
- The engine builds dynamic SQL from config — validate with the parity test before trusting at scale.
- Validation (required/conditional/cross-field, user-facing rule display, row accept/reject + reasons) is **Spec 2**, built on `EXPECTED_COLUMNS` + `PAYLOAD`.
