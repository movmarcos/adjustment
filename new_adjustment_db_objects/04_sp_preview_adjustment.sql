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

    cobid          = adj["cobid"]
    process_type   = adj["process_type"]
    adjustment_type = adj["adjustment_type"].lower()
    source_cobid   = adj.get("source_cobid", cobid)
    scale_factor   = float(adj.get("scale_factor", 1.0))

    # ── Get settings ─────────────────────────────────────────────────────
    settings_row = session.table("ADJUSTMENT_APP.ADJUSTMENTS_SETTINGS").filter(
        upper(col("PROCESS_TYPE")) == process_type.upper()
    ).collect()

    if not settings_row:
        return session.sql("SELECT 'Error: scope not found' AS MESSAGE")

    s = settings_row[0]
    fact_tbl     = s["FACT_TABLE"]
    metric_name  = s["METRIC_NAME"].upper()
    metric_usd   = s["METRIC_USD_NAME"].upper()

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
            WHERE li.ADJ_ID = {adj.get("adj_id", -1)}
              AND li.IS_DELETED = FALSE
            ORDER BY li.LINE_ID
        """)

    # ── Build SELECT columns dynamically ─────────────────────────────────
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

    # Primary metric (USD or whichever exists)
    select_parts.append(f"fact.{primary_metric} AS CURRENT_VALUE")
    select_parts.append(f"fact.{primary_metric} * {sf_adjusted} AS ADJUSTMENT_DELTA")
    select_parts.append(f"fact.{primary_metric} + (fact.{primary_metric} * {sf_adjusted}) AS PROJECTED_VALUE")

    # Local currency metric (only if it exists AND is different from USD)
    if has_metric_local:
        select_parts.append(f"fact.{metric_name} AS CURRENT_VALUE_LOCAL")
        select_parts.append(f"fact.{metric_name} * {sf_adjusted} AS ADJUSTMENT_LOCAL")
        select_parts.append(f"fact.{metric_name} + (fact.{metric_name} * {sf_adjusted}) AS PROJECTED_VALUE_LOCAL")

    select_sql = ",\n        ".join(select_parts)
    join_sql   = "\n    ".join(join_parts)

    # ── Build WHERE filters ──────────────────────────────────────────────
    fact_source = fact_tbl
    cob_filter  = source_cobid

    where_clauses = [f"fact.COBID = {cob_filter}"]

    # Dimension filters (only apply if the join column exists)
    if has_entity_key:
        val = adj.get("entity_code")
        if val:
            where_clauses.append(
                f"EXISTS (SELECT 1 FROM DIMENSION.ENTITY d "
                f"WHERE d.ENTITY_KEY = fact.ENTITY_KEY AND d.ENTITY_CODE = '{val}')"
            )

    if has_book_key:
        val = adj.get("book_code")
        if val:
            where_clauses.append(
                f"EXISTS (SELECT 1 FROM DIMENSION.BOOK d "
                f"WHERE d.BOOK_KEY = fact.BOOK_KEY AND d.BOOK_CODE = '{val}')"
            )
        dept = adj.get("department_code")
        if dept:
            where_clauses.append(
                f"EXISTS (SELECT 1 FROM DIMENSION.BOOK bk "
                f"WHERE bk.BOOK_KEY = fact.BOOK_KEY AND bk.DEPARTMENT_CODE = '{dept}')"
            )

    if has_currency:
        val = adj.get("currency_code")
        if val:
            where_clauses.append(f"fact.CURRENCY_CODE = '{val}'")

    if has_source_sys:
        val = adj.get("source_system_code")
        if val:
            where_clauses.append(f"fact.SOURCE_SYSTEM_CODE = '{val}'")

    if has_trade_key:
        strategy = adj.get("strategy")
        typology = adj.get("trade_typology")
        if strategy or typology:
            trade_cond = "1=1"
            if strategy:
                trade_cond += f" AND td.STRATEGY = '{strategy}'"
            if typology:
                trade_cond += f" AND td.TRADE_TYPOLOGY = '{typology}'"
            where_clauses.append(
                f"EXISTS (SELECT 1 FROM DIMENSION.TRADE td "
                f"WHERE td.TRADE_KEY = fact.TRADE_KEY AND {trade_cond})"
            )

    where_sql = "\n      AND ".join(where_clauses)

    preview_sql = f"""
    SELECT
        {select_sql}
    FROM {fact_source} fact
    {join_sql}
    WHERE {where_sql}
      AND fact.{primary_metric} IS NOT NULL
    ORDER BY ABS(fact.{primary_metric}) DESC
    """

    return session.sql(preview_sql)
$$;
