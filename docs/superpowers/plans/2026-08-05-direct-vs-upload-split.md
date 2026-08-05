# Direct vs VaR Upload Split — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split the conflated "Direct Adjustment" into two flows — **Direct** (paste/upload where *each row* becomes its own adjustment: one header + one dimension entry + exactly one fact row) and **VaR Upload** (today's declarative CSV engine, VaR only, one file = one entry, re-keyed `ADJUSTMENT_TYPE/ACTION='Upload'`).

**Architecture:** New staging table + per-scope accepted-columns config + per-scope validation views own Direct row validation in the DB; the app parses order-free CSV, stages it, shows the view's verdicts, and submits one `SP_SUBMIT_ADJUSTMENT` call per valid row (codes land on existing `ADJ_HEADER` columns + `ADJUSTMENT_VALUE_IN_USD`). The engine gets a lean one-row Direct branch (codes→keys, −1 defaults, value into the USD measure) while the existing declarative engine moves under the Upload action. Summary rebuild is added to both.

**Tech Stack:** Snowflake SQL, Python/Snowpark stored procedures, Streamlit-in-Snowflake, pandas.

**Spec:** `docs/superpowers/specs/2026-08-05-direct-vs-upload-design.md`

## Global Constraints

- No unit-test harness (SiS runtime): per-task gates are `python3 -m py_compile` for pages and handler-extraction + `py_compile` for SPs; acceptance is the manual Snowflake checklist in Task 9.
- SP handler compile gate (run from repo root `/Users/marcosmagri/Documents/MUFG/adjustment`):
  ```bash
  awk 'BEGIN{f=0} /^\$\$;?$/{f=!f; next} f' new_adjustment_db_objects/05_sp_process_adjustment.sql > /tmp/proc.py && python3 -m py_compile /tmp/proc.py && echo "05 OK"
  awk 'BEGIN{f=0} /^\$\$;?$/{f=!f; next} f' new_adjustment_db_objects/03_sp_submit_adjustment.sql > /tmp/submit.py && python3 -m py_compile /tmp/submit.py && echo "03 OK"
  ```
- DDL uses `CREATE OR ALTER` for tables (user preference), `CREATE OR REPLACE` for views/procedures.
- Key strings (exact): Direct flow = `ADJUSTMENT_TYPE='Direct'`, `ADJUSTMENT_ACTION='Direct'`; Upload flow = `ADJUSTMENT_TYPE='Upload'`, `ADJUSTMENT_ACTION='Upload'`; app categories = `"Direct Adjustment"`, `"VaR Upload"`.
- Legacy value defaults in fact rows: `CURRENCY_CODE/TRADE_CURRENCY → 'N/A'`, `SOURCE_SYSTEM_CODE → 'QP'`, `IS_OFFICIAL_SOURCE → TRUE`, every unresolved `*_KEY`/`*_ID` → `-1`.
- Direct is offered for Stress, Sensitivity, FRTB, FRTBDRC, FRTBRRAO. VaR is upload-only.
- Commit after every task; end commit messages with the Claude co-author trailer used in this repo.

---

## File Structure

- **Modify** `new_adjustment_db_objects/01_tables.sql` — add `ADJ_DIRECT_STAGE`, `DIRECT_ACCEPTED_COLUMNS` + seeds (Task 1).
- **Create** `new_adjustment_db_objects/13_direct_validation.sql` — 5 `VW_DIRECT_VALIDATE_*` views (Task 2).
- **Modify** `new_adjustment_db_objects/05_sp_process_adjustment.sql` — Upload gate + new Direct branch + summary rebuild (Task 3).
- **Modify** `new_adjustment_db_objects/05b_sp_run_pipeline.sql` — overlap filters (Task 4).
- **Modify** `new_adjustment_db_objects/03_sp_submit_adjustment.sql` — ACTION_MAP + replace/blocking gates (Task 5).
- **Modify** `streamlit_app/pages/1_New_Adjustment.py` — category rename + new Direct flow (Tasks 6–7).
- **Modify** `streamlit_app/utils/styles.py`, `streamlit_app/pages/2_Adjustments.py`, `streamlit_app/pages/6_Documentation.py` — label sweep (Task 7).
- **Modify** `docs/adjustment_recon_v2.sql` — Direct reconciliation rule (Task 8).

---

### Task 1: Staging table + accepted-columns config

**Files:**
- Modify: `new_adjustment_db_objects/01_tables.sql` (append after the `ADJ_LINE_ITEM_JSON` block)

**Interfaces:**
- Produces: `ADJUSTMENT_APP.ADJ_DIRECT_STAGE(BATCH_ID, ROW_NUM, ENTITY_CODE, SOURCE_SYSTEM_CODE, DEPARTMENT_CODE, BOOK_CODE, TRADE_CODE, TRADE_TYPOLOGY, STRATEGY, INSTRUMENT_CODE, SIMULATION_NAME, SIMULATION_SOURCE, MEASURE_TYPE_CODE, CURRENCY_CODE, VALUE_USD, USERNAME, CREATED_DATE)` and `ADJUSTMENT_APP.DIRECT_ACCEPTED_COLUMNS(PROCESS_TYPE, ACCEPTED_NAME, STAGE_COLUMN, IS_REQUIRED, IS_ACTIVE)` — consumed by Tasks 2 and 6.

- [ ] **Step 1: Append the DDL + seeds**

```sql
-- ═══════════════════════════════════════════════════════════════════════════
-- ADJ_DIRECT_STAGE — parse target for Direct Adjustment paste/upload.
-- The app writes one row per pasted CSV line (BATCH_ID = one paste), the
-- per-scope VW_DIRECT_VALIDATE_* views validate them, and submit turns each
-- VALID row into its own ADJ_HEADER. Rows are deleted after submit/cancel;
-- anything older than 2 days is abandoned and may be purged.
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR ALTER TABLE ADJUSTMENT_APP.ADJ_DIRECT_STAGE (
    BATCH_ID            VARCHAR(36)  NOT NULL,
    ROW_NUM             NUMBER(38,0) NOT NULL,
    ENTITY_CODE         VARCHAR(50),
    SOURCE_SYSTEM_CODE  VARCHAR(50),
    DEPARTMENT_CODE     VARCHAR(50),
    BOOK_CODE           VARCHAR(100),
    TRADE_CODE          VARCHAR(200),
    TRADE_TYPOLOGY      VARCHAR(50),
    STRATEGY            VARCHAR(100),
    INSTRUMENT_CODE     VARCHAR(200),
    SIMULATION_NAME     VARCHAR(200),
    SIMULATION_SOURCE   VARCHAR(100),
    MEASURE_TYPE_CODE   VARCHAR(30),
    CURRENCY_CODE       VARCHAR(10),
    VALUE_USD           VARCHAR(100),          -- raw text; numeric check is a validation rule
    USERNAME            VARCHAR(200),
    CREATED_DATE        TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_ADJ_DIRECT_STAGE PRIMARY KEY (BATCH_ID, ROW_NUM)
)
COMMENT = 'Direct Adjustment staging: one row per pasted/uploaded CSV line, validated by VW_DIRECT_VALIDATE_<scope>, then submitted one header per valid row.';

-- ═══════════════════════════════════════════════════════════════════════════
-- DIRECT_ACCEPTED_COLUMNS — per-scope accepted CSV header names for Direct.
-- Column order/case in the paste never matter: the app matches each CSV
-- header (upper-cased, trimmed) against ACCEPTED_NAME and writes the cell to
-- STAGE_COLUMN. IS_REQUIRED drives the validation views' required checks.
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR ALTER TABLE ADJUSTMENT_APP.DIRECT_ACCEPTED_COLUMNS (
    PROCESS_TYPE   VARCHAR(20)  NOT NULL,
    ACCEPTED_NAME  VARCHAR(100) NOT NULL,      -- stored upper-case
    STAGE_COLUMN   VARCHAR(50)  NOT NULL,      -- a column of ADJ_DIRECT_STAGE
    IS_REQUIRED    BOOLEAN      DEFAULT FALSE, -- required flags live on the canonical name row
    IS_ACTIVE      BOOLEAN      DEFAULT TRUE,
    CONSTRAINT PK_DIRECT_ACCEPTED_COLUMNS PRIMARY KEY (PROCESS_TYPE, ACCEPTED_NAME)
)
COMMENT = 'Accepted CSV header names (incl. aliases) per scope for Direct Adjustment; drives order-free parsing and required-field validation.';

-- Seeds — canonical name + aliases per scope. MERGE keeps re-runs idempotent.
MERGE INTO ADJUSTMENT_APP.DIRECT_ACCEPTED_COLUMNS t
USING (
    -- shared columns for every Direct scope
    SELECT s.PT AS PROCESS_TYPE, c.ACCEPTED_NAME, c.STAGE_COLUMN, c.IS_REQUIRED
    FROM (SELECT 'Stress' PT UNION ALL SELECT 'Sensitivity'
          UNION ALL SELECT 'FRTB' UNION ALL SELECT 'FRTBDRC'
          UNION ALL SELECT 'FRTBRRAO') s
    CROSS JOIN (
        SELECT 'ENTITY_CODE' ACCEPTED_NAME,'ENTITY_CODE' STAGE_COLUMN, TRUE  IS_REQUIRED UNION ALL
        SELECT 'ENTITY',            'ENTITY_CODE',        FALSE UNION ALL
        SELECT 'DEPARTMENT_CODE',   'DEPARTMENT_CODE',    FALSE UNION ALL
        SELECT 'DEPARTMENT',        'DEPARTMENT_CODE',    FALSE UNION ALL
        SELECT 'BOOK_CODE',         'BOOK_CODE',          FALSE UNION ALL
        SELECT 'BOOK',              'BOOK_CODE',          FALSE UNION ALL
        SELECT 'TRADE_CODE',        'TRADE_CODE',         FALSE UNION ALL
        SELECT 'TRADE',             'TRADE_CODE',         FALSE UNION ALL
        SELECT 'TRADE_TYPOLOGY',    'TRADE_TYPOLOGY',     FALSE UNION ALL
        SELECT 'STRATEGY',          'STRATEGY',           FALSE UNION ALL
        SELECT 'INSTRUMENT_CODE',   'INSTRUMENT_CODE',    FALSE UNION ALL
        SELECT 'INSTRUMENT',        'INSTRUMENT_CODE',    FALSE UNION ALL
        SELECT 'CURRENCY_CODE',     'CURRENCY_CODE',      FALSE UNION ALL
        SELECT 'CURRENCY',          'CURRENCY_CODE',      FALSE UNION ALL
        SELECT 'SOURCE_SYSTEM_CODE','SOURCE_SYSTEM_CODE', FALSE UNION ALL
        SELECT 'VALUE_USD',         'VALUE_USD',          TRUE  UNION ALL
        SELECT 'VALUE',             'VALUE_USD',          FALSE UNION ALL
        SELECT 'AMOUNT',            'VALUE_USD',          FALSE UNION ALL
        SELECT 'ADJUSTMENT_VALUE',  'VALUE_USD',          FALSE
    ) c
    UNION ALL
    -- Stress-only
    SELECT 'Stress','SIMULATION_NAME','SIMULATION_NAME', FALSE UNION ALL
    SELECT 'Stress','SIMULATION',     'SIMULATION_NAME', FALSE UNION ALL
    SELECT 'Stress','SIMULATION_SOURCE','SIMULATION_SOURCE', FALSE
    UNION ALL
    -- Sensitivity + FRTB scopes: measure type
    SELECT s2.PT,'MEASURE_TYPE_CODE','MEASURE_TYPE_CODE', FALSE
    FROM (SELECT 'Sensitivity' PT UNION ALL SELECT 'FRTB'
          UNION ALL SELECT 'FRTBDRC' UNION ALL SELECT 'FRTBRRAO') s2
    UNION ALL
    SELECT s3.PT,'MEASURE_TYPE','MEASURE_TYPE_CODE', FALSE
    FROM (SELECT 'Sensitivity' PT UNION ALL SELECT 'FRTB'
          UNION ALL SELECT 'FRTBDRC' UNION ALL SELECT 'FRTBRRAO') s3
) src
ON  t.PROCESS_TYPE = src.PROCESS_TYPE AND t.ACCEPTED_NAME = src.ACCEPTED_NAME
WHEN MATCHED THEN UPDATE SET
    t.STAGE_COLUMN = src.STAGE_COLUMN, t.IS_REQUIRED = src.IS_REQUIRED,
    t.IS_ACTIVE = TRUE
WHEN NOT MATCHED THEN INSERT
    (PROCESS_TYPE, ACCEPTED_NAME, STAGE_COLUMN, IS_REQUIRED, IS_ACTIVE)
VALUES (src.PROCESS_TYPE, src.ACCEPTED_NAME, src.STAGE_COLUMN, src.IS_REQUIRED, TRUE);
```

- [ ] **Step 2: Verify**

Run: `grep -c "ADJ_DIRECT_STAGE\|DIRECT_ACCEPTED_COLUMNS" new_adjustment_db_objects/01_tables.sql` — expect ≥ 6 mentions.

- [ ] **Step 3: Commit**

```bash
git add new_adjustment_db_objects/01_tables.sql
git commit -m "feat(direct): ADJ_DIRECT_STAGE + DIRECT_ACCEPTED_COLUMNS config with seeds"
```

---

### Task 2: Per-scope validation views

**Files:**
- Create: `new_adjustment_db_objects/13_direct_validation.sql`

**Interfaces:**
- Consumes: `ADJ_DIRECT_STAGE`, `DIRECT_ACCEPTED_COLUMNS` (Task 1).
- Produces: `ADJUSTMENT_APP.VW_DIRECT_VALIDATE_STRESS / _SENSITIVITY / _FRTB / _FRTBDRC / _FRTBRRAO`, each with columns `(BATCH_ID, ROW_NUM, IS_VALID BOOLEAN, VALIDATION_ERRORS ARRAY)` — consumed by Task 6.

- [ ] **Step 1: Create the file with all five views**

All five share one generic template; each scope has its own view so FRTB
cross-field rules can later be added by editing only the FRTB views. The
required-field checks are config-driven via `DIRECT_ACCEPTED_COLUMNS`
(`IS_REQUIRED` on the canonical name row). Full file content:

```sql
-- =============================================================================
-- 13_DIRECT_VALIDATION.SQL — per-scope validation views for Direct Adjustment
-- One view per scope over ADJ_DIRECT_STAGE. Generic rules v1:
--   • required fields present (config-driven via DIRECT_ACCEPTED_COLUMNS)
--   • VALUE_USD numeric and <> 0
--   • every supplied code exists in its dimension (case-insensitive)
-- FRTB cross-field rules will be added to the three FRTB views ONLY, when the
-- requirements arrive — that is why each scope has its own view.
-- =============================================================================
USE SCHEMA ADJUSTMENT_APP;

CREATE OR REPLACE VIEW ADJUSTMENT_APP.VW_DIRECT_VALIDATE_STRESS
    COMMENT = 'Row validation for Stress Direct Adjustment staging rows.'
AS
SELECT
    s.BATCH_ID, s.ROW_NUM,
    ARRAY_SIZE(v.ERRS) = 0 AS IS_VALID,
    v.ERRS               AS VALIDATION_ERRORS
FROM ADJUSTMENT_APP.ADJ_DIRECT_STAGE s,
LATERAL (
    SELECT ARRAY_COMPACT(ARRAY_CONSTRUCT(
        IFF(s.ENTITY_CODE IS NULL AND EXISTS (
                SELECT 1 FROM ADJUSTMENT_APP.DIRECT_ACCEPTED_COLUMNS c
                WHERE c.PROCESS_TYPE = 'Stress' AND c.IS_ACTIVE AND c.IS_REQUIRED
                  AND c.STAGE_COLUMN = 'ENTITY_CODE'),
            'ENTITY_CODE is required', NULL),
        IFF(s.VALUE_USD IS NULL AND EXISTS (
                SELECT 1 FROM ADJUSTMENT_APP.DIRECT_ACCEPTED_COLUMNS c
                WHERE c.PROCESS_TYPE = 'Stress' AND c.IS_ACTIVE AND c.IS_REQUIRED
                  AND c.STAGE_COLUMN = 'VALUE_USD'),
            'VALUE_USD is required', NULL),
        IFF(s.VALUE_USD IS NOT NULL AND TRY_TO_NUMBER(s.VALUE_USD) IS NULL,
            'VALUE_USD is not numeric: ' || s.VALUE_USD, NULL),
        IFF(TRY_TO_NUMBER(s.VALUE_USD) = 0, 'VALUE_USD must not be zero', NULL),
        IFF(s.ENTITY_CODE IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM DIMENSION.ENTITY e
                WHERE UPPER(e.ENTITY_CODE) = UPPER(s.ENTITY_CODE)),
            'Unknown ENTITY_CODE: ' || s.ENTITY_CODE, NULL),
        IFF(s.BOOK_CODE IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM DIMENSION.BOOK b
                WHERE UPPER(b.BOOK_CODE) = UPPER(s.BOOK_CODE)
                  AND b.IS_CURRENT_ROW = TRUE
                  AND (s.ENTITY_CODE IS NULL OR UPPER(b.ENTITY_CODE) = UPPER(s.ENTITY_CODE))
                  AND (s.DEPARTMENT_CODE IS NULL OR UPPER(b.DEPARTMENT_CODE) = UPPER(s.DEPARTMENT_CODE))),
            'Unknown BOOK_CODE (for entity/department): ' || s.BOOK_CODE, NULL),
        IFF(s.DEPARTMENT_CODE IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM DIMENSION.BOOK b
                WHERE UPPER(b.DEPARTMENT_CODE) = UPPER(s.DEPARTMENT_CODE)
                  AND b.IS_CURRENT_ROW = TRUE
                  AND (s.ENTITY_CODE IS NULL OR UPPER(b.ENTITY_CODE) = UPPER(s.ENTITY_CODE))),
            'Unknown DEPARTMENT_CODE (for entity): ' || s.DEPARTMENT_CODE, NULL),
        IFF(s.TRADE_CODE IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM DIMENSION.TRADE t
                WHERE UPPER(t.TRADE_CODE) = UPPER(s.TRADE_CODE)
                  AND t.IS_CURRENT_ROW = TRUE),
            'Unknown TRADE_CODE: ' || s.TRADE_CODE, NULL),
        IFF(s.INSTRUMENT_CODE IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM DIMENSION.COMMON_INSTRUMENT ci
                WHERE UPPER(ci.INSTRUMENT_CODE) = UPPER(s.INSTRUMENT_CODE)
                  AND ci.IS_CURRENT_ROW = TRUE),
            'Unknown INSTRUMENT_CODE: ' || s.INSTRUMENT_CODE, NULL),
        IFF(s.SIMULATION_NAME IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM DIMENSION.STRESS_SIMULATION ss
                WHERE UPPER(ss.STRESS_SIMULATION_NAME) = UPPER(s.SIMULATION_NAME)),
            'Unknown SIMULATION_NAME: ' || s.SIMULATION_NAME, NULL),
        IFF(s.SIMULATION_SOURCE IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM DIMENSION.STRESS_SIMULATION ss
                WHERE UPPER(ss.SIMULATION_SOURCE) = UPPER(s.SIMULATION_SOURCE)),
            'Unknown SIMULATION_SOURCE: ' || s.SIMULATION_SOURCE, NULL)
    )) AS ERRS
) v;
```

The **Sensitivity** view is identical except: view name
`VW_DIRECT_VALIDATE_SENSITIVITY`, comment says Sensitivity, every
`c.PROCESS_TYPE = 'Stress'` becomes `'Sensitivity'`, the two
SIMULATION checks are **removed**, and this measure-type check is added in
their place:

```sql
        IFF(s.MEASURE_TYPE_CODE IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM DIMENSION.MEASURE_TYPE mt
                WHERE UPPER(mt.MEASURE_TYPE_CODE) = UPPER(s.MEASURE_TYPE_CODE)),
            'Unknown MEASURE_TYPE_CODE: ' || s.MEASURE_TYPE_CODE, NULL)
```

The **FRTB / FRTBDRC / FRTBRRAO** views are identical to the Sensitivity
view except for their names (`VW_DIRECT_VALIDATE_FRTB`, `_FRTBDRC`,
`_FRTBRRAO`), comments, and `c.PROCESS_TYPE` literals (`'FRTB'`,
`'FRTBDRC'`, `'FRTBRRAO'`). Each additionally carries this comment line
above the ARRAY_CONSTRUCT so the future edit point is explicit:

```sql
        -- FRTB cross-field validation rules land HERE (requirements pending)
```

Write all five views **in full** in the file (copy the template and apply
the substitutions above — no shared macro; each view must stand alone so
FRTB edits stay local).

- [ ] **Step 2: Verify**

Run: `grep -c "CREATE OR REPLACE VIEW ADJUSTMENT_APP.VW_DIRECT_VALIDATE" new_adjustment_db_objects/13_direct_validation.sql` — expect `5`.

- [ ] **Step 3: Commit**

```bash
git add new_adjustment_db_objects/13_direct_validation.sql
git commit -m "feat(direct): per-scope VW_DIRECT_VALIDATE_* views (generic rules v1)"
```

---

### Task 3: Engine — Upload gate, new Direct branch, summary rebuild

**Files:**
- Modify: `new_adjustment_db_objects/05_sp_process_adjustment.sql`

**Interfaces:**
- Consumes: headers submitted by Task 5/6 (`ADJUSTMENT_ACTION` in ('Direct','Upload'), Direct headers carrying codes + `ADJUSTMENT_VALUE_IN_USD`).
- Produces: one `FACT.*_ADJUSTMENT` row per Direct header; unchanged many-row behaviour for Upload; `*_ADJUSTMENT_SUMMARY` rebuilt in both branches.

- [ ] **Step 1: Re-gate the declarative branch as Upload**

In the `# DIRECT (Upload) PATH` section (`if adjustment_action.lower() == 'direct':` around line 639):
- change the branch condition to `if adjustment_action.lower() == 'upload':`
- change the claim filter `(col('ADJUSTMENT_ACTION') == 'Direct')` to `(col('ADJUSTMENT_ACTION') == 'Upload')`
- update the section banner comment to `# UPLOAD PATH (one file → one adjustment entry; declarative engine)`
- rename the `_erlog` step label `"direct_insert"` to `"upload_insert"`
- update `result["message"]` strings from `Direct adjustments` to `Upload adjustments`.

- [ ] **Step 2: Add the summary rebuild to the Upload branch**

Immediately after the `rows_count` count query and before the `# ── Common post-processing` comment in that branch, insert (same pattern as the Scale path):

```python
            # ── Rebuild summary (atomic delete + insert) ─────────────────
            if fact_adj_summary_name:
                summary_non_metric = ', '.join([
                    c for c in fact_adj_summary_cols
                    if c not in {metric_name, metric_usd_name}
                ])
                upload_summary_insert = f"""
                INSERT INTO {fact_adj_summary_name}
                ({summary_non_metric}, {metric_name}{', ' + metric_usd_name if metric_usd_name != metric_name else ''})
                SELECT {summary_non_metric},
                       SUM({metric_name}){', SUM(' + metric_usd_name + ')' if metric_usd_name != metric_name else ''}
                FROM {fact_adj_tbl_name}
                WHERE COBID = {cobid}
                GROUP BY ALL
                """
                upload_summary_delete = f"""
                    DELETE FROM {fact_adj_summary_name}
                    WHERE COBID = {cobid}
                """
                session.sql("BEGIN").collect()
                try:
                    _erlog(session, _sqlog, "summary_delete", upload_summary_delete)
                    _erlog(session, _sqlog, "summary_insert", upload_summary_insert)
                    session.sql("COMMIT").collect()
                except Exception:
                    session.sql("ROLLBACK").collect()
                    raise
```

NOTE: `fact_adj_summary_cols` must be loaded in this branch the same way the
Scale path loads it; if it is only loaded inside the Scale branch, hoist the
load (`fact_adj_summary_cols = session.table(fact_adj_summary_name).columns
if fact_adj_summary_name else []`) to the shared settings section right
after `fact_adj_summary_name` is read (around line 568).

- [ ] **Step 3: Add the new Direct branch**

Insert a new `elif adjustment_action.lower() == 'direct':` branch between the
Upload branch and the Scale branch, with this body (it deliberately reuses
the existing helpers — `insert_to_dimension_and_get_ids`,
`update_header_status`, `log_status_history`, `_erlog`,
`trigger_downstream_handoff`, `notify_outcome` — all already defined):

```python
        # ═════════════════════════════════════════════════════════════════
        # DIRECT PATH — one header = one fact row, value straight in
        # Codes on ADJ_HEADER resolve to dimension keys (case-insensitive,
        # -1 when blank or unmatched); ADJUSTMENT_VALUE_IN_USD lands in the
        # USD measure column (and the native twin — no FX conversion).
        # ═════════════════════════════════════════════════════════════════
        elif adjustment_action.lower() == 'direct':

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

            # Prior-run cleanup (retry): remove fact rows keyed by the
            # PREVIOUS DIMENSION_ADJ_ID and retire the old dimension rows.
            session.sql(f"""
                DELETE FROM {fact_adj_tbl_name}
                WHERE COBID = {cobid}
                  AND ADJUSTMENT_ID IN (
                      SELECT DIMENSION_ADJ_ID FROM ADJUSTMENT_APP.ADJ_HEADER
                      WHERE ADJ_ID IN ({adj_ids_str})
                        AND DIMENSION_ADJ_ID IS NOT NULL)
            """).collect()
            session.sql(f"""
                UPDATE DIMENSION.ADJUSTMENT
                SET IS_DELETED = TRUE
                WHERE ADJUSTMENT_ID IN (
                      SELECT DIMENSION_ADJ_ID FROM ADJUSTMENT_APP.ADJ_HEADER
                      WHERE ADJ_ID IN ({adj_ids_str})
                        AND DIMENSION_ADJ_ID IS NOT NULL)
            """).collect()

            dim_adj_map = insert_to_dimension_and_get_ids(session, adj_ids, adj_ids_str)
            if not dim_adj_map:
                raise Exception("DIMENSION.ADJUSTMENT insert returned no ADJUSTMENT_IDs")
            dim_ids_str = ', '.join(str(v) for v in dim_adj_map.values())

            # ── Column expression per fact-adj column (header alias: h) ──
            def _direct_expr(c):
                fixed = {
                    'COBID':               str(cobid),
                    'ADJUSTMENT_ID':       "h.DIMENSION_ADJ_ID",
                    'ENTITY_CODE':         "COALESCE(h.ENTITY_CODE, 'N/A')",
                    'ENTITY_KEY':          ("COALESCE((SELECT MAX(e.ENTITY_KEY) FROM DIMENSION.ENTITY e "
                                            "WHERE UPPER(e.ENTITY_CODE) = UPPER(h.ENTITY_CODE)), -1)"),
                    'BOOK_KEY':            ("COALESCE((SELECT MAX(bk.BOOK_KEY) FROM DIMENSION.BOOK bk "
                                            "WHERE UPPER(bk.BOOK_CODE) = UPPER(h.BOOK_CODE) "
                                            "AND bk.IS_CURRENT_ROW = TRUE), -1)"),
                    'TRADE_KEY':           ("COALESCE((SELECT MAX(td.TRADE_KEY) FROM DIMENSION.TRADE td "
                                            "WHERE UPPER(td.TRADE_CODE) = UPPER(h.TRADE_CODE) "
                                            "AND td.IS_CURRENT_ROW = TRUE), -1)"),
                    'COMMON_INSTRUMENT_KEY': ("COALESCE((SELECT MAX(ci.COMMON_INSTRUMENT_KEY) "
                                            "FROM DIMENSION.COMMON_INSTRUMENT ci "
                                            "WHERE UPPER(ci.INSTRUMENT_CODE) = UPPER(h.INSTRUMENT_CODE) "
                                            "AND ci.IS_CURRENT_ROW = TRUE), -1)"),
                    'STRESS_SIMULATION_KEY': ("COALESCE((SELECT MAX(ss.STRESS_SIMULATION_KEY) "
                                            "FROM DIMENSION.STRESS_SIMULATION ss "
                                            "WHERE UPPER(ss.STRESS_SIMULATION_NAME) = UPPER(h.SIMULATION_NAME)), -1)"),
                    'MEASURE_TYPE_KEY':    ("COALESCE((SELECT MAX(mt.MEASURE_TYPE_KEY) "
                                            "FROM DIMENSION.MEASURE_TYPE mt "
                                            "WHERE UPPER(mt.MEASURE_TYPE_CODE) = UPPER(h.MEASURE_TYPE_CODE)), -1)"),
                    'MEASURE_TYPE_CODE':   "h.MEASURE_TYPE_CODE",
                    'INSTRUMENT_CODE':     "h.INSTRUMENT_CODE",
                    'TRADE_CURRENCY':      "COALESCE(h.CURRENCY_CODE, 'N/A')",
                    'CURRENCY_CODE':       "COALESCE(h.CURRENCY_CODE, 'N/A')",
                    'SOURCE_SYSTEM_CODE':  "COALESCE(h.SOURCE_SYSTEM_CODE, 'QP')",
                    'IS_OFFICIAL_SOURCE':  "TRUE",
                    'RUN_LOG_ID':          str(run_log_id),
                    'LOAD_TIMESTAMP':      "CURRENT_TIMESTAMP()",
                }
                if c in fixed:
                    return fixed[c]
                if c in (metric_name, metric_usd_name):
                    return "h.ADJUSTMENT_VALUE_IN_USD"
                if c.split('_')[-1].upper() in ('KEY', 'ID'):
                    return "-1"          # legacy default for unmapped keys
                return None              # column left out → its own default/NULL

            target_cols, select_exprs = [], []
            for c in fact_adj_tbl.columns:
                expr = _direct_expr(c)
                if expr is not None:
                    target_cols.append(c)
                    select_exprs.append(f"{expr} AS {c}")

            direct_insert = f"""
                INSERT INTO {fact_adj_tbl_name} ({', '.join(target_cols)})
                SELECT {', '.join(select_exprs)}
                FROM ADJUSTMENT_APP.ADJ_HEADER h
                WHERE h.ADJ_ID IN ({adj_ids_str})
                  AND h.ADJUSTMENT_VALUE_IN_USD IS NOT NULL
            """
            _erlog(session, _sqlog, "direct_row_insert", direct_insert)

            rows_count = session.sql(f"""
                SELECT COUNT(*) AS CNT FROM {fact_adj_tbl_name}
                WHERE COBID = {cobid} AND ADJUSTMENT_ID IN ({dim_ids_str})
            """).collect()[0]["CNT"]
```

Then, still inside the branch, append the same post-processing as the Upload
branch (copy it — per-adjustment `RECORD_COUNT` zero-init + grouped update on
`ADJ_HEADER` and `DIMENSION.ADJUSTMENT`, `RUN_STATUS='Processed'` on the
dimension rows), followed by the summary rebuild block from Step 2 (verbatim,
same variable names), then this zero-match warning, and finally the standard
close-out:

```python
            # Zero-match warning: a Direct header whose row was not written
            # (NULL value, or filtered) — surface it, never silently succeed.
            session.sql(f"""
                UPDATE ADJUSTMENT_APP.ADJ_HEADER
                SET ERRORMESSAGE = 'Warning: processed but no fact row was '
                    || 'written — ADJUSTMENT_VALUE_IN_USD was empty or the row '
                    || 'was filtered. Check the submitted values.'
                WHERE ADJ_ID IN ({adj_ids_str})
                  AND RECORD_COUNT = 0
                  AND ERRORMESSAGE IS NULL
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
            result["downstream_handoff"] = trigger_downstream_handoff(
                session, process_type, cobid, run_log_id, adj_ids_str)
            notify_outcome(session, adj_ids, "Processed")
```

- [ ] **Step 4: Compile gate**

Run the 05 handler-extraction command from Global Constraints. Expected: `05 OK`.

- [ ] **Step 5: Commit**

```bash
git add new_adjustment_db_objects/05_sp_process_adjustment.sql
git commit -m "feat(engine): Upload gate for declarative branch; new one-row Direct branch; summary rebuild in both"
```

---

### Task 4: Pipeline overlap filters recognise Upload

**Files:**
- Modify: `new_adjustment_db_objects/05b_sp_run_pipeline.sql:94` and `:484`

- [ ] **Step 1: Widen both action filters**

Change (line 94): `_OVERLAP_ACTION_FILTER = "AND ADJUSTMENT_ACTION NOT IN ('Direct')"` → `... NOT IN ('Direct', 'Upload')"`.
Change (line 484): `AND p.ADJUSTMENT_ACTION NOT IN ('Direct')` → `AND p.ADJUSTMENT_ACTION NOT IN ('Direct', 'Upload')`.
(The dispatch itself iterates distinct `(PROCESS_TYPE, ADJUSTMENT_ACTION, COBID)` combos generically — no other change needed.)

- [ ] **Step 2: Compile gate**

```bash
awk 'BEGIN{f=0} /^\$\$;?$/{f=!f; next} f' new_adjustment_db_objects/05b_sp_run_pipeline.sql > /tmp/pipe.py && python3 -m py_compile /tmp/pipe.py && echo "05b OK"
```

- [ ] **Step 3: Commit**

```bash
git add new_adjustment_db_objects/05b_sp_run_pipeline.sql
git commit -m "fix(pipeline): overlap serialisation skips Upload action like Direct"
```

---

### Task 5: Submit SP — Upload action + gates

**Files:**
- Modify: `new_adjustment_db_objects/03_sp_submit_adjustment.sql`

- [ ] **Step 1: ACTION_MAP** (line 46): change `"upload": "Direct",` to `"upload": "Upload",` (keep `"direct": "Direct"`).

- [ ] **Step 2: Global-reference replace gate** (line 383 + 391): the one-file-replaces-file semantics belong to the Upload flow now. Change `str(adjustment_type).lower() == 'direct'` → `str(adjustment_type).lower() == 'upload'` and `UPPER(ADJUSTMENT_TYPE) = 'DIRECT'` → `UPPER(ADJUSTMENT_TYPE) = 'UPLOAD'`. Also update the two literal comment strings mentioning "new upload" — they stay accurate.

- [ ] **Step 3: Overlap-blocking skip** (line 335): change `if initial_status == STATUS_PENDING and adj_action != "Direct":` → `if initial_status == STATUS_PENDING and adj_action not in ("Direct", "Upload"):` (both are explicit value insertions; overlap serialisation does not apply).

- [ ] **Step 4: Compile gate** — run the 03 extraction command from Global Constraints. Expected: `03 OK`.

- [ ] **Step 5: Commit**

```bash
git add new_adjustment_db_objects/03_sp_submit_adjustment.sql
git commit -m "feat(submit): Upload action mapping; replace-by-reference + blocking gates follow the split"
```

---

### Task 6: App — rename category to VaR Upload; new Direct flow

**Files:**
- Modify: `streamlit_app/pages/1_New_Adjustment.py`

**Interfaces:**
- Consumes: `DIRECT_ACCEPTED_COLUMNS`, `ADJ_DIRECT_STAGE`, `VW_DIRECT_VALIDATE_<SCOPE>` (Tasks 1–2); `SP_SUBMIT_ADJUSTMENT` payload keys (existing; Direct rows add `"adjustment_value_in_usd"`).
- Produces: category strings `"VaR Upload"` and `"Direct Adjustment"`; helper names used below: `_accepted_columns(scope)`, `_parse_direct_df(df, scope)`, `_stage_direct_batch(batch_id, ndf)`, `_direct_validation(batch_id, scope)`, `_delete_direct_batch(batch_id)`.

- [ ] **Step 1: Mechanical rename — existing flow becomes "VaR Upload"**

Replace the category string `"Direct Adjustment"` with `"VaR Upload"` at every current occurrence (lines 112, 472, 622, 774, 1502 and any others — `grep -n '"Direct Adjustment"' streamlit_app/pages/1_New_Adjustment.py` must return ZERO after this step). In `_build_payload`'s (now) `"VaR Upload"` branch change `"adjustment_type": "Direct"` → `"adjustment_type": "Upload"`. Gate the category so it is only offered when `wiz["process_type"] == "VaR"` (wherever the category options list is built, restrict membership; the scope pills inside the branch pin to VaR).

- [ ] **Step 2: Add the Direct helpers** (place after `_sim_source_options`):

```python
# ── Direct Adjustment: order-free CSV → stage → per-scope validation view ────
_DIRECT_STAGE_COLS = [
    "ENTITY_CODE", "SOURCE_SYSTEM_CODE", "DEPARTMENT_CODE", "BOOK_CODE",
    "TRADE_CODE", "TRADE_TYPOLOGY", "STRATEGY", "INSTRUMENT_CODE",
    "SIMULATION_NAME", "SIMULATION_SOURCE", "MEASURE_TYPE_CODE",
    "CURRENCY_CODE", "VALUE_USD",
]


def _accepted_columns(scope: str):
    """{ACCEPTED_NAME (upper): STAGE_COLUMN} + set of required stage columns."""
    rows = _ref_rows(
        f"SELECT ACCEPTED_NAME, STAGE_COLUMN, IS_REQUIRED "
        f"FROM ADJUSTMENT_APP.DIRECT_ACCEPTED_COLUMNS "
        f"WHERE UPPER(PROCESS_TYPE) = UPPER('{scope}') AND IS_ACTIVE = TRUE",
        f"_ref_direct_cols_{scope}")
    alias_map = {str(r[0]).strip().upper(): str(r[1]).strip().upper() for r in rows}
    required  = {str(r[1]).strip().upper() for r in rows if r[2]}
    return alias_map, required


def _parse_direct_df(df, scope: str):
    """Map pasted columns to stage columns by header name (order/case-free).
    Returns (normalized_df, unknown_cols, missing_required)."""
    alias_map, required = _accepted_columns(scope)
    out, seen = {}, set()
    unknown = []
    for c in df.columns:
        key = str(c).strip().upper()
        tgt = alias_map.get(key)
        if tgt is None:
            unknown.append(str(c))
        elif tgt not in seen:
            seen.add(tgt)
            out[tgt] = df[c]
    ndf = pd.DataFrame(out)
    missing_required = sorted(required - seen)
    return ndf, unknown, missing_required


def _stage_direct_batch(batch_id: str, ndf) -> int:
    """Write normalized rows to ADJ_DIRECT_STAGE. Returns row count."""
    user = (current_user_name() or "").replace("'", "''")
    values = []
    for i, (_, row) in enumerate(ndf.iterrows(), start=1):
        cells = []
        for col_name in _DIRECT_STAGE_COLS:
            v = row.get(col_name)
            if v is None or (isinstance(v, float) and pd.isna(v)) or str(v).strip() == "":
                cells.append("NULL")
            else:
                cells.append("'" + str(v).strip().replace("'", "''") + "'")
        values.append(f"('{batch_id}', {i}, {', '.join(cells)}, '{user}')")
    if not values:
        return 0
    run_query(
        f"INSERT INTO ADJUSTMENT_APP.ADJ_DIRECT_STAGE "
        f"(BATCH_ID, ROW_NUM, {', '.join(_DIRECT_STAGE_COLS)}, USERNAME) "
        f"VALUES {', '.join(values)}")
    return len(values)


def _direct_validation(batch_id: str, scope: str):
    """Per-row verdicts from the scope's validation view."""
    view = f"ADJUSTMENT_APP.VW_DIRECT_VALIDATE_{scope.upper()}"
    return run_query(
        f"SELECT ROW_NUM, IS_VALID, VALIDATION_ERRORS FROM {view} "
        f"WHERE BATCH_ID = '{batch_id}' ORDER BY ROW_NUM")


def _delete_direct_batch(batch_id: str) -> None:
    run_query(f"DELETE FROM ADJUSTMENT_APP.ADJ_DIRECT_STAGE "
              f"WHERE BATCH_ID = '{batch_id}'")
```

- [ ] **Step 3: New Direct render branch + payload + submit loop**

Add a `render_direct_form()` (patterned on the existing upload branch's paste/file UI — reuse `_read_csv` and the delimiter picker): scope pills (Stress/Sensitivity/FRTB/FRTBDRC/FRTBRRAO only), paste/file input, then on parse: `_parse_direct_df` → warn on `unknown` columns, error on `missing_required`; generate `batch_id = str(uuid.uuid4())`, `_delete_direct_batch(old)` if re-parsing, then age-purge abandoned batches before staging (`run_query("DELETE FROM ADJUSTMENT_APP.ADJ_DIRECT_STAGE WHERE CREATED_DATE < DATEADD('day', -2, CURRENT_TIMESTAMP())")` — best-effort, wrapped in try/except), `_stage_direct_batch`, `_direct_validation`, and render the preview with a ✓/✗ column + error messages, plus a rejects download (same pattern as `_render_upload_validation`). Store `wiz["direct_batch_id"]`, `wiz["direct_ndf"]`, `wiz["direct_verdicts"]`.

In `_build_payload`, add a `"Direct Adjustment"` branch that builds ONE ROW's payload (called per row by the submit loop):

```python
def _direct_row_payload(row: dict) -> dict:
    p = {
        "cobid":                 wiz["cobid"],
        "process_type":          wiz["process_type"],
        "adjustment_type":       "Direct",
        "username":              current_user_name(),
        "source_cobid":          wiz["cobid"],
        "reason":                wiz.get("reason", ""),
        "requires_approval":     wiz.get("requires_approval", False),
        "adjustment_occurrence": "ADHOC",
        "adjustment_category":   wiz.get("adjustment_category"),
        "adjustment_value_in_usd": float(row["VALUE_USD"]),
    }
    for stage_col, payload_key in [
            ("ENTITY_CODE", "entity_code"), ("SOURCE_SYSTEM_CODE", "source_system_code"),
            ("DEPARTMENT_CODE", "department_code"), ("BOOK_CODE", "book_code"),
            ("TRADE_CODE", "trade_code"), ("TRADE_TYPOLOGY", "trade_typology"),
            ("STRATEGY", "strategy"), ("INSTRUMENT_CODE", "instrument_code"),
            ("SIMULATION_NAME", "simulation_name"), ("SIMULATION_SOURCE", "simulation_source"),
            ("MEASURE_TYPE_CODE", "measure_type_code"), ("CURRENCY_CODE", "currency_code")]:
        v = row.get(stage_col)
        if v is not None and str(v).strip():
            p[payload_key] = str(v).strip()
    return p
```

In `_do_submit`, add the Direct branch BEFORE the generic `_submit_one` call
(modelled on the FRTBALL fan-out loop directly above it): iterate the staged
rows whose verdict `IS_VALID` is true, call `_submit_one(_direct_row_payload(row))`
per row, collect created/failed counts, then `_delete_direct_batch(...)` and
return a summary result
(`{"status": ..., "message": f"Created {n_ok} Direct adjustments" + failures}`).
Invalid rows are never submitted.

- [ ] **Step 4: Completion checks + ticket**

Extend `_completion_checks` with a `cat == "Direct Adjustment"` branch: CSV parsed, all-required present, ≥1 valid row, reason set. Extend the ticket/summary section (around line 1502) with a Direct block showing scope + row counts (valid/invalid). The old checks/ticket blocks follow the renamed `"VaR Upload"` category.

- [ ] **Step 5: Compile gate**

```bash
python3 -m py_compile streamlit_app/pages/1_New_Adjustment.py && echo "PAGE OK"
grep -c '"Direct Adjustment"' streamlit_app/pages/1_New_Adjustment.py   # only NEW-flow occurrences
```

- [ ] **Step 6: Commit**

```bash
git add streamlit_app/pages/1_New_Adjustment.py
git commit -m "feat(app): VaR Upload category (one file = one entry); new Direct flow (one header per pasted row)"
```

---

### Task 7: Label sweep (styles, grids, docs page)

**Files:**
- Modify: `streamlit_app/utils/styles.py`, `streamlit_app/pages/2_Adjustments.py`, `streamlit_app/pages/6_Documentation.py`

- [ ] **Step 1: Sweep for hardcoded type/category strings**

Run: `grep -rn "'Direct'\|\"Direct\"\|Direct Adjustment" streamlit_app/ --include="*.py" | grep -v 1_New_Adjustment` and fix each hit so:
- any ADJUSTMENT_TYPE label/filter map that lists `Direct` also lists `Upload` (e.g. clone/prefill maps in `2_Adjustments.py:222` region, grid label maps in `styles.py`);
- the icon map entry for the new `"VaR Upload"` category uses `":material/upload_file:"` and `"Direct Adjustment"` gets `":material/playlist_add:"`;
- `6_Documentation.py` flow descriptions describe both flows (short edit: Direct = each row its own adjustment; VaR Upload = one file one entry).

- [ ] **Step 2: Compile gate**

```bash
python3 -m py_compile streamlit_app/utils/styles.py streamlit_app/pages/2_Adjustments.py streamlit_app/pages/6_Documentation.py && echo "SWEEP OK"
```

- [ ] **Step 3: Commit**

```bash
git add streamlit_app/utils/styles.py streamlit_app/pages/2_Adjustments.py streamlit_app/pages/6_Documentation.py
git commit -m "chore(app): label/icon sweep for Direct vs Upload split"
```

---

### Task 8: Recon v2 — Direct rule

**Files:**
- Modify: `docs/adjustment_recon_v2.sql`

- [ ] **Step 1: Carry the header value + add the rule**

In the `adj` CTE add `h.ADJUSTMENT_VALUE_IN_USD,` after `h.SCALE_FACTOR,`. In the final SELECT’s `RECON_DIFFERENCE` CASE add, as the FIRST branch:

```sql
        WHEN a.ADJUSTMENT_TYPE = 'Direct'
             THEN av.ADJUSTMENT_VALUE - a.ADJUSTMENT_VALUE_IN_USD
```

and in `RECON_STATUS` add, as the FIRST branch:

```sql
        WHEN a.ADJUSTMENT_TYPE = 'Direct' THEN
            CASE
                WHEN av.ADJUSTMENT_VALUE IS NULL THEN 'MISSING ADJUSTMENT'
                WHEN ABS(av.ADJUSTMENT_VALUE - a.ADJUSTMENT_VALUE_IN_USD) < 0.01
                     THEN 'RECONCILED'
                ELSE 'BREAK'
            END
```

Also add `AND h.ADJUSTMENT_TYPE <> 'Upload'` next to the existing `AND h.ADJUSTMENT_TYPE <> 'EROL'` filter (Upload entries are many-row uploads with no single expected value — out of recon scope, like EROL). Add `a.ADJUSTMENT_VALUE_IN_USD` to `EXPECTED_ADJ_VALUE`'s Direct case: `WHEN a.ADJUSTMENT_TYPE = 'Direct' THEN a.ADJUSTMENT_VALUE_IN_USD` (first branch).

- [ ] **Step 2: Commit**

```bash
git add docs/adjustment_recon_v2.sql
git commit -m "feat(recon): Direct rule — fact sum must equal ADJUSTMENT_VALUE_IN_USD"
```

---

### Task 9: Manual acceptance checklist (Snowflake dev)

**Files:** none (manual gate; deploy first: `01_tables.sql` new blocks, `13_direct_validation.sql`, SPs `03`/`05`/`05b`, app files to the Streamlit stage)

- [ ] **A. VaR Upload parity:** category shows "VaR Upload" only for VaR; paste a legacy-layout file → ONE header + ONE `DIMENSION.ADJUSTMENT` row (`ADJUSTMENT_TYPE='Upload'`, `ADJUSTMENT_ACTION='Upload'`), unpivoted rows in `FACT.VAR_MEASURES_ADJUSTMENT`, summary rebuilt, PowerBI action row appears.
- [ ] **B. Direct happy path (Stress):** paste 3 rows with shuffled, mixed-case headers (`book, VALUE, Entity_Code, simulation`) → preview shows 3 verdicts from `VW_DIRECT_VALIDATE_STRESS`; all valid → 3 headers; process → exactly 3 rows in `FACT.STRESS_MEASURES_ADJUSTMENT` (one per `DIMENSION_ADJ_ID`), keys resolved, unfiltered keys = −1, values = pasted USD, summary rebuilt.
- [ ] **C. Direct invalid row:** repeat B with one bad `BOOK_CODE` → that row flagged ✗ with `Unknown BOOK_CODE`, only 2 headers created.
- [ ] **D. Zero-match warning:** submit a Direct row, NULL its `ADJUSTMENT_VALUE_IN_USD` on the header before processing → header shows the warning after processing.
- [ ] **E. Recon:** run `docs/adjustment_recon_v2.sql` → the B rows show `RECONCILED` under the Direct rule.
- [ ] **F. FRTB scopes:** one Direct row through each FRTB scope validates via its own view and lands one fact row each.
