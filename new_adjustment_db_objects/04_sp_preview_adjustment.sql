-- =============================================================================
-- 04_SP_PREVIEW_ADJUSTMENT.SQL
-- Preview the impact of an adjustment BEFORE applying it.
--
-- Returns: table of rows that would be affected, with current values and
-- projected (adjusted) values side-by-side. No data is modified.
--
-- Streamlit calls:
--   CALL ADJUSTMENT_APP.SP_PREVIEW_ADJUSTMENT('{ ... JSON ... }');
-- =============================================================================

USE SCHEMA ADJUSTMENT_APP;

CREATE OR ALTER PROCEDURE ADJUSTMENT_APP.SP_PREVIEW_ADJUSTMENT(p_adjustment VARCHAR)
RETURNS TABLE()
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
COMMENT = 'Preview the impact of a Scale/Flatten/Roll adjustment. Returns current vs projected values. Read-only — nothing is modified.'
EXECUTE AS CALLER
AS
$$
import json
from snowflake.snowpark.functions import col, lit, upper
from snowflake.snowpark import DataFrame


def _esc(v):
    """Escape a value for inlining as a Snowflake single-quoted string literal.
    Snowflake escapes a quote by doubling it ('' ), NOT with a backslash."""
    return str(v).replace("'", "''")


def get_table_columns(session, table_name):
    """Return the set of column names (UPPER) for a given table."""
    parts = table_name.upper().split('.')
    if len(parts) == 2:
        schema, tbl = parts
    else:
        tbl = parts[0]
        schema = None
    try:
        rows = session.sql(f"SHOW COLUMNS IN TABLE {table_name}").collect()
        return {row["column_name"].upper() for row in rows}
    except Exception:
        return set()


def main(session, p_adjustment):
    """
    Preview adjustment impact without modifying any data.

    Expected JSON (same structure as SP_SUBMIT_ADJUSTMENT):
      cobid, process_type, adjustment_type, source_cobid, scale_factor,
      entity_code, book_code, department_code, strategy, trade_typology, ...

    Returns a result set with:
      Current values, adjustment delta, and projected values.
    """
    adj = json.loads(p_adjustment) if isinstance(p_adjustment, str) else p_adjustment

    cobid          = int(adj["cobid"])
    process_type   = adj["process_type"]
    adjustment_type = adj["adjustment_type"].lower()
    source_cobid   = int(adj.get("source_cobid", cobid))
    scale_factor   = float(adj.get("scale_factor", 1.0))

    # ── Get settings ─────────────────────────────────────────────────────
    settings_row = session.table("ADJUSTMENT_APP.ADJUSTMENTS_SETTINGS").filter(
        upper(col("PROCESS_TYPE")) == process_type.upper()
    ).collect()

    if not settings_row:
        return session.sql("SELECT 'Error: scope not found' AS MESSAGE")

    s = settings_row[0]
    fact_tbl     = s["FACT_TABLE"]
    fact_adj_tbl = s["FACT_ADJUSTED_TABLE"] if "FACT_ADJUSTED_TABLE" in s else None
    metric_name  = s["METRIC_NAME"].upper()
    metric_usd   = s["METRIC_USD_NAME"].upper()

    # A cross-COB Roll carries the source COB's *adjusted* value forward,
    # mirroring SP_PROCESS_ADJUSTMENT leg ② (which reads FACT_ADJUSTED_TABLE —
    # the combined view, same schema as the fact). Preview: current = target
    # original, projected = factor × source adjusted.
    is_roll = (adjustment_type == "roll" and int(source_cobid) != int(cobid))

    # ── Discover which columns actually exist in the fact table ──────────
    fact_columns = get_table_columns(session, fact_tbl)

    has_metric_local = metric_name in fact_columns and metric_name != metric_usd
    has_metric_usd   = metric_usd in fact_columns
    has_entity_key   = "ENTITY_KEY" in fact_columns
    has_entity_code  = "ENTITY_CODE" in fact_columns
    has_book_key     = "BOOK_KEY" in fact_columns
    has_trade_key    = "TRADE_KEY" in fact_columns
    has_currency     = "CURRENCY_CODE" in fact_columns
    has_source_sys   = "SOURCE_SYSTEM_CODE" in fact_columns

    # If neither metric column exists, we can't preview
    if not has_metric_usd and not has_metric_local:
        return session.sql(
            f"SELECT 'Error: metric columns {metric_usd} / {metric_name} not found in {fact_tbl}' AS MESSAGE"
        )

    # Use whichever metric column exists (prefer USD)
    primary_metric = metric_usd if has_metric_usd else metric_name

    # ── Compute effective scale factor ───────────────────────────────────
    if adjustment_type == "flatten":
        sf_adjusted = -1.0
    elif adjustment_type in ("scale", "roll"):
        if int(source_cobid) != int(cobid):
            sf_adjusted = scale_factor        # cross-COB
        else:
            sf_adjusted = scale_factor - 1.0  # same-COB
    else:
        # Direct / Upload — preview shows the uploaded values themselves
        return session.sql(f"""
            SELECT li.*, 'Uploaded value' AS PREVIEW_NOTE
            FROM ADJUSTMENT_APP.ADJ_LINE_ITEM li
            WHERE li.ADJ_ID = '{_esc(adj.get("adj_id", -1))}'
              AND li.IS_DELETED = FALSE
            ORDER BY li.LINE_ID
        """)

    # ── Preview mode ─────────────────────────────────────────────────────
    # summary   : ONE aggregated row (count + sums). No row transfer — safe
    #             at any scale. This is the default and what the metric cards use.
    # breakdown : server-side GROUP BY entity/book/department (small result).
    # sample    : up to 1,000 row-level rows (LIMIT enforced server-side).
    # Older callers that omit "mode" get the summary, which never crashes.
    mode = str(adj.get("mode", "summary")).lower()

    # Metric expressions reused by every mode
    m_cur  = f"fact.{primary_metric}"
    m_del  = f"fact.{primary_metric} * {sf_adjusted}"
    m_proj = f"fact.{primary_metric} + (fact.{primary_metric} * {sf_adjusted})"

    # ── Build WHERE filters ──────────────────────────────────────────────
    fact_source = fact_tbl
    cob_filter  = source_cobid

    where_clauses = [f"fact.COBID = {int(cob_filter)}"]

    # Dimension filters (only apply if the join column exists).
    # All user-supplied values are escaped via _esc — these are EXECUTE AS CALLER
    # procs, so an unescaped value is both an injection vector and breaks on any
    # apostrophe in a code/name.
    if has_entity_key:
        val = adj.get("entity_code")
        if val:
            where_clauses.append(
                f"EXISTS (SELECT 1 FROM DIMENSION.ENTITY d "
                f"WHERE d.ENTITY_KEY = fact.ENTITY_KEY AND d.ENTITY_CODE = '{_esc(val)}')"
            )
    elif has_entity_code:
        # Facts keyed by text ENTITY_CODE (e.g. VaR) — filter directly. Without
        # this an invalid/non-existent entity code matched every row.
        val = adj.get("entity_code")
        if val:
            where_clauses.append(f"fact.ENTITY_CODE = '{_esc(val)}'")

    if has_book_key:
        # BOOK attributes map to MANY books, so each is a semi-join on DIMENSION.BOOK
        # (mirrors SP_PROCESS_ADJUSTMENT's BOOK EXISTS): book_code, department_code,
        # trader_code, guaranteed_entity, region_key.
        bk_conds = []
        if adj.get("book_code"):
            bk_conds.append(f"bk.BOOK_CODE = '{_esc(adj['book_code'])}'")
        if adj.get("department_code"):
            bk_conds.append(f"bk.DEPARTMENT_CODE = '{_esc(adj['department_code'])}'")
        if adj.get("trader_code"):
            bk_conds.append(f"bk.PRIMARY_TRADER_CODE = '{_esc(adj['trader_code'])}'")
        if adj.get("guaranteed_entity"):
            bk_conds.append(f"bk.GUARANTEED_ENTITY = '{_esc(adj['guaranteed_entity'])}'")
        if adj.get("region_key") not in (None, ""):
            try:
                bk_conds.append(f"bk.REGION_KEY = {int(adj['region_key'])}")
            except (TypeError, ValueError):
                pass
        if bk_conds:
            where_clauses.append(
                f"EXISTS (SELECT 1 FROM DIMENSION.BOOK bk "
                f"WHERE bk.BOOK_KEY = fact.BOOK_KEY AND {' AND '.join(bk_conds)})"
            )

    if has_currency:
        val = adj.get("currency_code")
        if val:
            where_clauses.append(f"fact.CURRENCY_CODE = '{_esc(val)}'")

    if has_source_sys:
        val = adj.get("source_system_code")
        if val:
            where_clauses.append(f"fact.SOURCE_SYSTEM_CODE = '{_esc(val)}'")

    if has_trade_key:
        # TRADE attributes map to MANY trades (mirrors SP_PROCESS_ADJUSTMENT's
        # TRADE EXISTS): trade_code, strategy, trade_typology.
        td_conds = []
        if adj.get("trade_code"):
            td_conds.append(f"td.TRADE_CODE = '{_esc(adj['trade_code'])}'")
        if adj.get("strategy"):
            td_conds.append(f"td.STRATEGY = '{_esc(adj['strategy'])}'")
        if adj.get("trade_typology"):
            td_conds.append(f"td.TRADE_TYPOLOGY = '{_esc(adj['trade_typology'])}'")
        if td_conds:
            where_clauses.append(
                f"EXISTS (SELECT 1 FROM DIMENSION.TRADE td "
                f"WHERE td.TRADE_KEY = fact.TRADE_KEY AND {' AND '.join(td_conds)})"
            )

    # ── 1:1 code→key dimensions ──────────────────────────────────────────
    # Mirror the resolution SP_PROCESS_ADJUSTMENT applies (and the dbt base
    # model) so the preview reflects the same affected rows. Each only applies
    # when the fact keys that dimension by surrogate key.
    if "MEASURE_TYPE_KEY" in fact_columns:
        val = adj.get("measure_type_code")
        if val:
            where_clauses.append(
                f"EXISTS (SELECT 1 FROM DIMENSION.MEASURE_TYPE mt "
                f"WHERE mt.MEASURE_TYPE_KEY = fact.MEASURE_TYPE_KEY AND mt.MEASURE_TYPE_CODE = '{_esc(val)}')"
            )

    if "TENOR_CURRENCY_KEY" in fact_columns:
        val = adj.get("tenor_code")
        if val:
            cur = adj.get("currency_code") or "USD"
            where_clauses.append(
                f"EXISTS (SELECT 1 FROM DIMENSION.TENOR_CURRENCY tc "
                f"WHERE tc.TENOR_CURRENCY_KEY = fact.TENOR_CURRENCY_KEY "
                f"AND tc.TENOR_CURRENCY_CODE = '{_esc(val)}_{_esc(cur)}')"
            )

    if "CURVE_CURRENCY_KEY" in fact_columns:
        val = adj.get("curve_code")
        if val:
            where_clauses.append(
                f"EXISTS (SELECT 1 FROM DIMENSION.CURVE_CURRENCY cc "
                f"WHERE cc.CURVE_CURRENCY_KEY = fact.CURVE_CURRENCY_KEY AND cc.CURVE_CODE = '{_esc(val)}')"
            )

    if "UNDERLYING_TENOR_CURRENCY_KEY" in fact_columns:
        val = adj.get("underlying_tenor_code")
        if val:
            where_clauses.append(
                f"EXISTS (SELECT 1 FROM DIMENSION.UNDERLYING_TENOR_CURRENCY ut "
                f"WHERE ut.UNDERLYING_TENOR_CURRENCY_KEY = fact.UNDERLYING_TENOR_CURRENCY_KEY "
                f"AND ut.UNDERYLING_TENOR_CODE = '{_esc(val)}')"
            )

    if "PRODUCT_CATEGORY_ATTRIBUTES_KEY" in fact_columns:
        val = adj.get("product_category_attributes")
        if val:
            where_clauses.append(
                f"EXISTS (SELECT 1 FROM DIMENSION.PRODUCT_CATEGORY_ATTRIBUTES pca "
                f"WHERE pca.PRODUCT_CATEGORY_ATTRIBUTES_KEY = fact.PRODUCT_CATEGORY_ATTRIBUTES_KEY "
                f"AND REPLACE(pca.PCA_CONCAT_KEY, ' ', '') = REPLACE('{_esc(val)}', ' ', ''))"
            )

    # Instrument (COMMON_INSTRUMENT_KEY → INSTRUMENT_CODE)
    if "COMMON_INSTRUMENT_KEY" in fact_columns:
        val = adj.get("instrument_code")
        if val:
            where_clauses.append(
                f"EXISTS (SELECT 1 FROM DIMENSION.COMMON_INSTRUMENT ci "
                f"WHERE ci.COMMON_INSTRUMENT_KEY = fact.COMMON_INSTRUMENT_KEY "
                f"AND ci.INSTRUMENT_CODE = '{_esc(val)}')"
            )

    # Stress simulation (STRESS_SIMULATION_KEY → SIMULATION_NAME + SOURCE)
    if "STRESS_SIMULATION_KEY" in fact_columns:
        ss_conds = []
        if adj.get("simulation_name"):
            ss_conds.append(f"s.STRESS_SIMULATION_NAME = '{_esc(adj['simulation_name'])}'")
        if adj.get("simulation_source"):
            ss_conds.append(f"s.SIMULATION_SOURCE = '{_esc(adj['simulation_source'])}'")
        if ss_conds:
            where_clauses.append(
                f"EXISTS (SELECT 1 FROM DIMENSION.STRESS_SIMULATION s "
                f"WHERE s.STRESS_SIMULATION_KEY = fact.STRESS_SIMULATION_KEY AND {' AND '.join(ss_conds)})"
            )

    # VaR sub-component (VAR_SUBCOMPONENT_ID → VAR_COMPONENT_ID + VAR_SUB_COMPONENT_ID + DAY_TYPE)
    if "VAR_SUBCOMPONENT_ID" in fact_columns:
        vsc_conds = []
        for fld, dim_col in (("var_component_id", "VAR_COMPONENT_ID"),
                             ("var_sub_component_id", "VAR_SUB_COMPONENT_ID"),
                             ("day_type", "VAR_SUB_COMPONENT_DAY_TYPE")):
            if adj.get(fld) not in (None, ""):
                try:
                    vsc_conds.append(f"v.{dim_col} = {int(adj[fld])}")
                except (TypeError, ValueError):
                    pass
        if vsc_conds:
            where_clauses.append(
                f"EXISTS (SELECT 1 FROM DIMENSION.VAR_SUB_COMPONENT v "
                f"WHERE v.VAR_SUB_COMPONENT_ID = fact.VAR_SUBCOMPONENT_ID AND {' AND '.join(vsc_conds)})"
            )

    # Scenario date (direct column match)
    if "SCENARIO_DATE_ID" in fact_columns and adj.get("scenario_date_id") not in (None, ""):
        try:
            where_clauses.append(f"fact.SCENARIO_DATE_ID = {int(adj['scenario_date_id'])}")
        except (TypeError, ValueError):
            pass

    where_sql = "\n      AND ".join(where_clauses)
    base_where = f"WHERE {where_sql}\n      AND fact.{primary_metric} IS NOT NULL"

    # ═════════════════════════════════════════════════════════════════════
    # CROSS-COB ROLL — preview mirrors SP_PROCESS_ADJUSTMENT:
    #   current   = SUM(original) at the TARGET cob   (what leg ③ flattens)
    #   projected = factor × SUM(adjusted) at the SOURCE cob   (leg ②, reads
    #               FACT_ADJUSTED_TABLE — the combined view incl. existing adj)
    #   delta     = projected − current
    # where_clauses[0] is the COBID predicate; the rest are dimension filters
    # that apply (alias `fact`) to both the original fact and the combined
    # adjusted view (the adjusted view has the same schema as the fact table).
    # ═════════════════════════════════════════════════════════════════════
    if is_roll and fact_adj_tbl and fact_adj_tbl != fact_tbl:
        dim_filters = where_clauses[1:]
        dim_sql     = ("\n      AND " + "\n      AND ".join(dim_filters)) if dim_filters else ""
        src_where   = f"WHERE fact.COBID = {int(source_cobid)}{dim_sql}\n      AND fact.{primary_metric} IS NOT NULL"
        tgt_where   = f"WHERE fact.COBID = {int(cobid)}{dim_sql}\n      AND fact.{primary_metric} IS NOT NULL"

        roll_summary = f"""
        SELECT
            ROWS_AFFECTED,
            NONZERO_ROWS,
            TOTAL_CURRENT_VALUE,
            TOTAL_PROJECTED_VALUE - TOTAL_CURRENT_VALUE AS TOTAL_ADJUSTMENT_DELTA,
            TOTAL_PROJECTED_VALUE
        FROM (
            SELECT
                (SELECT COUNT(*)
                   FROM {fact_adj_tbl} fact {src_where})                    AS ROWS_AFFECTED,
                (SELECT COUNT_IF(fact.{primary_metric} != 0)
                   FROM {fact_adj_tbl} fact {src_where})                    AS NONZERO_ROWS,
                (SELECT COALESCE(SUM(fact.{primary_metric}), 0)
                   FROM {fact_tbl} fact {tgt_where})                        AS TOTAL_CURRENT_VALUE,
                {scale_factor} * (SELECT COALESCE(SUM(fact.{primary_metric}), 0)
                   FROM {fact_adj_tbl} fact {src_where})                    AS TOTAL_PROJECTED_VALUE
        ) q
        """
        return session.sql(roll_summary)

    # ── MODE: summary — single aggregated row, NO row transfer ───────────
    # The dimension filters above use EXISTS sub-queries, so no joins are
    # needed here. This runs as a server-side aggregate and returns one row
    # regardless of how many fact rows match — the fix for large-scope
    # (e.g. entity + department) adjustments that previously OOM'd the app.
    if mode == "summary":
        summary_sql = f"""
        SELECT
            COUNT(*)                        AS ROWS_AFFECTED,
            COUNT_IF({m_cur} != 0)          AS NONZERO_ROWS,
            COALESCE(SUM({m_cur}),  0)      AS TOTAL_CURRENT_VALUE,
            COALESCE(SUM({m_del}),  0)      AS TOTAL_ADJUSTMENT_DELTA,
            COALESCE(SUM({m_proj}), 0)      AS TOTAL_PROJECTED_VALUE
        FROM {fact_source} fact
        {base_where}
        """
        return session.sql(summary_sql)

    # ── MODE: breakdown — server-side GROUP BY dimensions ────────────────
    if mode == "breakdown":
        dim_parts  = []
        join_parts = []
        if has_entity_key:
            dim_parts.append("e.ENTITY_CODE")
            join_parts.append("LEFT JOIN DIMENSION.ENTITY e ON e.ENTITY_KEY = fact.ENTITY_KEY")
        if has_book_key:
            dim_parts.append("b.BOOK_CODE")
            dim_parts.append("b.DEPARTMENT_CODE")
            join_parts.append("LEFT JOIN DIMENSION.BOOK b ON b.BOOK_KEY = fact.BOOK_KEY")
        if not dim_parts:
            dim_parts = ["'ALL' AS SCOPE"]

        dim_sql  = ",\n            ".join(dim_parts)
        join_sql = "\n    ".join(join_parts)
        group_by = ", ".join(str(i + 1) for i in range(len(dim_parts)))

        breakdown_sql = f"""
        SELECT
            {dim_sql},
            COUNT(*)                        AS ROWS_AFFECTED,
            COALESCE(SUM({m_cur}),  0)      AS CURRENT_VALUE,
            COALESCE(SUM({m_del}),  0)      AS ADJUSTMENT_DELTA,
            COALESCE(SUM({m_proj}), 0)      AS PROJECTED_VALUE
        FROM {fact_source} fact
        {join_sql}
        {base_where}
        GROUP BY {group_by}
        ORDER BY {group_by}
        """
        return session.sql(breakdown_sql)

    # ── MODE: sample — bounded row-level rows (default cap 1,000) ─────────
    select_parts = []
    join_parts   = []

    if has_entity_key:
        select_parts.append("e.ENTITY_CODE")
        join_parts.append("LEFT JOIN DIMENSION.ENTITY e ON e.ENTITY_KEY = fact.ENTITY_KEY")
    if has_book_key:
        select_parts.append("b.BOOK_CODE")
        select_parts.append("b.DEPARTMENT_CODE")
        join_parts.append("LEFT JOIN DIMENSION.BOOK b ON b.BOOK_KEY = fact.BOOK_KEY")
    if has_currency:
        select_parts.append("fact.CURRENCY_CODE")
    if has_source_sys:
        select_parts.append("fact.SOURCE_SYSTEM_CODE")

    select_parts.append(f"{m_cur} AS CURRENT_VALUE")
    select_parts.append(f"{m_del} AS ADJUSTMENT_DELTA")
    select_parts.append(f"{m_proj} AS PROJECTED_VALUE")

    if has_metric_local:
        select_parts.append(f"fact.{metric_name} AS CURRENT_VALUE_LOCAL")
        select_parts.append(f"fact.{metric_name} * {sf_adjusted} AS ADJUSTMENT_LOCAL")
        select_parts.append(f"fact.{metric_name} + (fact.{metric_name} * {sf_adjusted}) AS PROJECTED_VALUE_LOCAL")

    select_sql = ",\n        ".join(select_parts)
    join_sql   = "\n    ".join(join_parts)

    try:
        sample_limit = int(adj.get("sample_limit", 1000))
    except (TypeError, ValueError):
        sample_limit = 1000
    sample_limit = max(1, min(sample_limit, 10000))

    preview_sql = f"""
    SELECT
        {select_sql}
    FROM {fact_source} fact
    {join_sql}
    {base_where}
    ORDER BY ABS(fact.{primary_metric}) DESC
    LIMIT {sample_limit}
    """

    return session.sql(preview_sql)
$$;
