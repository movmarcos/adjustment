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

USE DATABASE DVLP_RAPTOR_NEWADJ;
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

    if has_book_key:
        val = adj.get("book_code")
        if val:
            where_clauses.append(
                f"EXISTS (SELECT 1 FROM DIMENSION.BOOK d "
                f"WHERE d.BOOK_KEY = fact.BOOK_KEY AND d.BOOK_CODE = '{_esc(val)}')"
            )
        dept = adj.get("department_code")
        if dept:
            where_clauses.append(
                f"EXISTS (SELECT 1 FROM DIMENSION.BOOK bk "
                f"WHERE bk.BOOK_KEY = fact.BOOK_KEY AND bk.DEPARTMENT_CODE = '{_esc(dept)}')"
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
        strategy = adj.get("strategy")
        typology = adj.get("trade_typology")
        if strategy or typology:
            trade_cond = "1=1"
            if strategy:
                trade_cond += f" AND td.STRATEGY = '{_esc(strategy)}'"
            if typology:
                trade_cond += f" AND td.TRADE_TYPOLOGY = '{_esc(typology)}'"
            where_clauses.append(
                f"EXISTS (SELECT 1 FROM DIMENSION.TRADE td "
                f"WHERE td.TRADE_KEY = fact.TRADE_KEY AND {trade_cond})"
            )

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
