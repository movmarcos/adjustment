-- =============================================================================
-- SP_DEBUG_ENTITY_ROLL — read-only: print the exact SQL an Entity Roll runs
-- =============================================================================
-- Mirrors the EROL branch of SP_PROCESS_ADJUSTMENT, resolving every dynamic
-- piece (config table names, column lists, entity/metric predicates, flatten
-- source) for the given scope, and RETURNS the statements as a runnable,
-- annotated script — WITHOUT executing or modifying anything.
--
-- Usage:
--   CALL ADJUSTMENT_APP.SP_DEBUG_ENTITY_ROLL('VaR', 20260506, 20260505, 'MUSI');
--   -- copy the returned text into a worksheet and run each step to time it.
--
-- Notes:
--   * NEW_ADJUSTMENT_ID and RUN_LOG_ID are generated at runtime; this debug
--     uses the literal placeholders 999999999 / 0 so the statements are
--     directly runnable for timing. Row *counts/timings* are unaffected by the
--     literal id — only which rows belong to the roll afterwards.
--   * The flag UPDATEs and the DELETE are shown but are DESTRUCTIVE — run them
--     only on a scratch COB, or just EXPLAIN/time the SELECT/INSERT scans.
-- =============================================================================
USE SCHEMA ADJUSTMENT_APP;

CREATE OR REPLACE PROCEDURE ADJUSTMENT_APP.SP_DEBUG_ENTITY_ROLL(
    PROCESS_TYPE VARCHAR, COBID NUMBER, SOURCE_COBID NUMBER, ENTITY_CODE VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
def main(session, process_type, cobid, source_cobid, entity_code):
    NEW_ID  = 999999999      # placeholder for the generated DIMENSION.ADJUSTMENT id
    RUN_LOG = 0              # placeholder for the generated RUN_LOG_ID
    esc_entity = str(entity_code).replace("'", "''")
    esc_pt     = str(process_type).replace("'", "''")
    cobid, source_cobid = int(cobid), int(source_cobid)

    # ── 1. Resolve scope config (same columns as SP_PROCESS_ADJUSTMENT) ───────
    rows = session.sql(f"""
        SELECT FACT_TABLE, FACT_ADJUSTED_TABLE, ADJUSTMENTS_TABLE,
               ADJUSTMENTS_SUMMARY_TABLE, METRIC_NAME, METRIC_USD_NAME
        FROM ADJUSTMENT_APP.ADJUSTMENTS_SETTINGS
        WHERE UPPER(PROCESS_TYPE) = UPPER('{esc_pt}')
    """).collect()
    if not rows:
        return f"No ADJUSTMENTS_SETTINGS row for PROCESS_TYPE = {process_type}"
    s = rows[0]
    fact_tbl_name          = s["FACT_TABLE"]
    fact_adjusted_tbl_name = s["FACT_ADJUSTED_TABLE"]
    fact_adj_tbl_name      = s["ADJUSTMENTS_TABLE"]
    fact_adj_summary_name  = s["ADJUSTMENTS_SUMMARY_TABLE"]
    metric_name            = s["METRIC_NAME"].upper()
    metric_usd_name        = s["METRIC_USD_NAME"].upper()

    view_cols        = set(session.table(fact_adjusted_tbl_name).columns)
    fact_adj_cols    = list(session.table(fact_adj_tbl_name).columns)
    base_cols        = set(session.table(fact_tbl_name).columns)
    summary_cols     = list(session.table(fact_adj_summary_name).columns) if fact_adj_summary_name else []

    # ── 2. Metric + non-metric column resolution (mirror of the EROL path) ────
    er_metric     = metric_name if metric_name in view_cols else metric_usd_name
    er_metric_usd = metric_usd_name
    er_metrics    = [er_metric_usd] if er_metric == er_metric_usd else [er_metric, er_metric_usd]
    er_metric_cols = ', '.join(er_metrics)
    er_metric_sums = ', '.join(f"SUM({m}) AS {m}" for m in er_metrics)
    er_exclude = {er_metric, er_metric_usd, 'COBID', 'ADJUSTMENT_ID',
                  'ADJUSTMENT_CREATED_TIMESTAMP', 'RUN_LOG_ID', 'LOAD_TIMESTAMP',
                  'RAVEN_FILENAME', 'RAVEN_FILE_ROW_NUMBER'}
    er_non_metric = [c for c in fact_adj_cols if c not in er_exclude]

    def _default(c):
        return "-1" if c.split('_')[-1].upper() in ('KEY', 'ID') else "NULL"

    er_select_non_metric = ', '.join(
        (f"fact.{c}" if c in view_cols else f"{_default(c)} AS {c}") for c in er_non_metric)
    er_neg = ', '.join(f"fact.{m} * -1 AS {m}" for m in er_metrics)
    er_pos = ', '.join(f"fact.{m} AS {m}" for m in er_metrics)

    ins_extra = [c for c in ('RUN_LOG_ID', 'LOAD_TIMESTAMP',
                             'ADJUSTMENT_CREATED_TIMESTAMP') if c in fact_adj_cols]
    extra_sel = {'RUN_LOG_ID': str(RUN_LOG), 'LOAD_TIMESTAMP': 'CURRENT_TIMESTAMP()',
                 'ADJUSTMENT_CREATED_TIMESTAMP': 'CURRENT_TIMESTAMP()'}
    ins_cols  = ', '.join(['COBID', 'ADJUSTMENT_ID'] + er_non_metric + er_metrics + ins_extra)
    sel_extra = ''.join(f", {extra_sel[c]}" for c in ins_extra)

    # ── 3. Entity keys + predicates (literals — no DIMENSION.ENTITY sub-select)
    entity_keys_csv = ", ".join(
        str(r["ENTITY_KEY"]) for r in session.sql(
            f"SELECT ENTITY_KEY FROM DIMENSION.ENTITY WHERE ENTITY_CODE = '{esc_entity}'").collect())

    # combined-view leg predicate
    if 'ENTITY_CODE' in view_cols:
        er_pred = f"fact.ENTITY_CODE = '{esc_entity}'"
    elif 'ENTITY_KEY' in view_cols:
        er_pred = f"fact.ENTITY_KEY IN ({entity_keys_csv})"
    else:
        er_pred = "/* no ENTITY column in combined view */ 1=0"

    # adjustment-table (wipe) predicate
    if 'ENTITY_CODE' in fact_adj_cols:
        er_adj_ent_pred = f"ENTITY_CODE = '{esc_entity}'"
    elif 'ENTITY_KEY' in fact_adj_cols:
        er_adj_ent_pred = f"ENTITY_KEY IN ({entity_keys_csv})"
    else:
        er_adj_ent_pred = "/* no ENTITY column */ 1=0"

    # base-table predicate
    if 'ENTITY_CODE' in base_cols:
        base_ent_pred = f"ENTITY_CODE = '{esc_entity}'"
    elif 'ENTITY_KEY' in base_cols and entity_keys_csv:
        base_ent_pred = f"ENTITY_KEY IN ({entity_keys_csv})"
    else:
        base_ent_pred = None

    er_pt_pred = f"UPPER(PROCESS_TYPE) = UPPER('{esc_pt}')"

    # ── 4. Base-empty probe → decide flatten source ──────────────────────────
    base_has = False
    base_probe = None
    if base_ent_pred:
        base_probe = (f"SELECT 1 FROM {fact_tbl_name} "
                      f"WHERE COBID = {cobid} AND {base_ent_pred} LIMIT 1")
        base_has = bool(session.sql(base_probe).collect())
    do_flatten = base_has if base_ent_pred else True

    _flat_from_base = base_ent_pred is not None and all(m in base_cols for m in er_metrics)
    if _flat_from_base:
        flat_src, flat_cols, flat_pred = fact_tbl_name, base_cols, base_ent_pred
    else:
        flat_src, flat_cols, flat_pred = fact_adjusted_tbl_name, view_cols, er_pred
    flat_non_metric = ', '.join(
        (f"fact.{c}" if c in flat_cols else f"{_default(c)} AS {c}") for c in er_non_metric)

    # ── 5. Build the statements ───────────────────────────────────────────────
    out = []
    def add(title, sql):
        out.append(f"-- ===== {title} =====\n{sql.strip()};\n")

    out.append(f"""-- ============================================================
-- ENTITY ROLL DEBUG  |  scope={process_type}  target_cob={cobid}
--   source_cob={source_cobid}  entity={entity_code}
-- Resolved tables:
--   FACT_TABLE (base)        = {fact_tbl_name}
--   FACT_ADJUSTED_TABLE      = {fact_adjusted_tbl_name}
--   ADJUSTMENTS_TABLE        = {fact_adj_tbl_name}
--   ADJUSTMENTS_SUMMARY      = {fact_adj_summary_name}
--   metric / metric_usd      = {er_metric} / {er_metric_usd}
--   target base present?     = {base_has}  -> flatten {'RUNS' if do_flatten else 'SKIPPED'}
--   flatten source           = {flat_src} {'(BASE table)' if _flat_from_base else '(adjusted view)'}
-- Placeholders: ADJUSTMENT_ID = {NEW_ID}, RUN_LOG_ID = {RUN_LOG}
-- ============================================================
""")

    if base_probe:
        add("0. BASE PRESENCE PROBE (cheap; decides flatten)", base_probe)

    add("1. RECONCILE — adjustments for this entity/COB/scope (read-only)", f"""
SELECT
  (SELECT COUNT(*) FROM ADJUSTMENT_APP.ADJ_HEADER
    WHERE COBID = {cobid} AND ENTITY_CODE = '{esc_entity}' AND {er_pt_pred} AND IS_DELETED = FALSE) AS HEADER_CNT,
  (SELECT COUNT(*) FROM DIMENSION.ADJUSTMENT
    WHERE COBID = {cobid} AND ENTITY_CODE = '{esc_entity}' AND {er_pt_pred} AND IS_DELETED = FALSE) AS DIM_CNT,
  (SELECT COUNT(DISTINCT ADJUSTMENT_ID) FROM {fact_adj_tbl_name}
    WHERE COBID = {cobid} AND {er_adj_ent_pred}) AS FACT_CNT
""")

    add("2. FLAG DIMENSION.ADJUSTMENT superseded  [DESTRUCTIVE]", f"""
UPDATE DIMENSION.ADJUSTMENT
SET IS_DELETED = TRUE, RUN_STATUS = 'Superseded'
WHERE COBID = {cobid} AND ENTITY_CODE = '{esc_entity}' AND {er_pt_pred}
  AND IS_DELETED = FALSE AND ADJUSTMENT_ID <> {NEW_ID}
""")

    add("3. FLAG ADJ_HEADER superseded  [DESTRUCTIVE]", f"""
UPDATE ADJUSTMENT_APP.ADJ_HEADER
SET IS_DELETED = TRUE, RUN_STATUS = 'Superseded', DELETED_BY = 'ENTITY_ROLL',
    DELETED_DATE = CURRENT_TIMESTAMP(), ERRORMESSAGE = 'Superseded by Entity Roll <debug>'
WHERE COBID = {cobid} AND ENTITY_CODE = '{esc_entity}' AND {er_pt_pred} AND IS_DELETED = FALSE
""")

    add("4. WIPE entity rows at the target COB  [DESTRUCTIVE]", f"""
DELETE FROM {fact_adj_tbl_name}
WHERE COBID = {cobid} AND {er_adj_ent_pred}
""")

    if do_flatten:
        add("5. FLATTEN insert (leg ① negate base)  [writes]", f"""
INSERT INTO {fact_adj_tbl_name} ({ins_cols})
SELECT {cobid} AS COBID, {NEW_ID} AS ADJUSTMENT_ID, {flat_non_metric}, {er_neg}{sel_extra}
FROM {flat_src} fact
WHERE fact.COBID = {cobid} AND {flat_pred} AND fact.{er_metric_usd} IS NOT NULL
""")
    else:
        out.append("-- ===== 5. FLATTEN insert SKIPPED (target base empty) =====\n")

    add("6. ROLL insert (leg ② copy source)  [writes]  <-- this is the ~1-min combined-view read", f"""
INSERT INTO {fact_adj_tbl_name} ({ins_cols})
SELECT {cobid} AS COBID, {NEW_ID} AS ADJUSTMENT_ID, {er_select_non_metric}, {er_pos}{sel_extra}
FROM {fact_adjusted_tbl_name} fact
WHERE fact.COBID = {source_cobid} AND {er_pred} AND fact.{er_metric_usd} IS NOT NULL
""")

    if fact_adj_summary_name:
        # summary scoped per-table by its own entity column
        if 'ENTITY_CODE' in summary_cols:
            sum_pred = f"ENTITY_CODE = '{esc_entity}'"
        elif 'ENTITY_KEY' in summary_cols and entity_keys_csv:
            sum_pred = f"ENTITY_KEY IN ({entity_keys_csv})"
        else:
            sum_pred = None
        summary_non_metric = ', '.join(c for c in summary_cols if c not in {er_metric, er_metric_usd})
        sum_where = f" AND {sum_pred}" if sum_pred else ""
        adj_where = f" AND {er_adj_ent_pred}" if sum_pred else ""
        add("7. SUMMARY rebuild — delete entity slice  [DESTRUCTIVE]",
            f"DELETE FROM {fact_adj_summary_name} WHERE COBID = {cobid}{sum_where}")
        add("8. SUMMARY rebuild — reinsert entity slice  [writes]", f"""
INSERT INTO {fact_adj_summary_name} ({summary_non_metric}, {er_metric_cols})
SELECT {summary_non_metric}, {er_metric_sums}
FROM {fact_adj_tbl_name}
WHERE COBID = {cobid}{adj_where}
GROUP BY ALL
""")

    return "\n".join(out)
$$;
