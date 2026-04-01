-- =============================================================================
-- 05_SP_PROCESS_ADJUSTMENT.SQL
-- Core processing engine.
--
-- Adapted from the existing ADJUSTMENT_APP.PROCESS_ADJUSTMENT procedure.
-- Key changes:
--   ① Reads from ADJUSTMENT_APP.ADJ_HEADER (via config ADJUSTMENT_BASE_TABLE)
--   ② For Direct: reads values from ADJUSTMENT_APP.ADJ_LINE_ITEM
--   ③ Updates ADJ_HEADER status (not DIMENSION.ADJUSTMENT)
--   ④ Records status transitions in ADJ_STATUS_HISTORY
--   ⑤ Logs to BATCH.RUN_LOG
--
-- Called by: SP_RUN_PIPELINE (one call per adjustment, after claim step)
--
-- Signature matches the existing procedure for backward compatibility:
--   CALL ADJUSTMENT_APP.SP_PROCESS_ADJUSTMENT('VaR', 'Scale', 20250328);
-- =============================================================================

USE DATABASE DVLP_RAPTOR_NEWADJ;
USE SCHEMA ADJUSTMENT_APP;

CREATE OR REPLACE PROCEDURE ADJUSTMENT_APP.SP_PROCESS_ADJUSTMENT(
    process_type STRING,
    adjustment_action STRING,
    cobid INT
)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
COMMENT = 'Core processing engine. Reads Running adjustments (claimed by SP_RUN_PIPELINE), applies Scale/Direct logic, writes to FACT.*_ADJUSTMENT tables, marks records Processed or Failed.'
EXECUTE AS CALLER
AS
$$
from snowflake.snowpark.functions import col, lit, upper
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField, TimestampType
from datetime import datetime
import pytz
import json
import pandas as pd
import numpy as np


def check_columns(df_sf, column_list, metric_name, metric_usd_name):
    """Map line-item columns to fact adjustment table columns."""
    df = df_sf.to_pandas()
    column_data = {}
    for c in column_list:
        if c in df.columns:
            column_data[c] = df[c]
        else:
            column_data[c] = -1 if c.split('_')[-1].upper() == 'KEY' else np.nan

    column_data[metric_name]     = df["ADJUSTMENT_VALUE"]
    column_data[metric_usd_name] = df["ADJUSTMENT_VALUE_IN_USD"]
    column_data["IS_DELETED"]    = df["IS_DELETED"]
    return pd.DataFrame(column_data)


def update_header_status(session, df_adjustments, cobid, new_status, error_msg=None):
    """Update ADJ_HEADER.RUN_STATUS for the processed adjustments."""
    london_now = session.sql(
        "SELECT CONVERT_TIMEZONE('Europe/London', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9) AS T"
    ).collect()[0]["T"]

    adj_header = session.table('ADJUSTMENT_APP.ADJ_HEADER')
    update_cols = {
        "RUN_STATUS":  new_status,
        "PROCESS_DATE": london_now,
    }
    if error_msg:
        update_cols["ERRORMESSAGE"] = error_msg

    adj_header.update(
        update_cols,
        (
            (adj_header["ADJ_ID"] == df_adjustments["ADJ_ID"]) &
            (adj_header["COBID"] == cobid) &
            (adj_header["RUN_STATUS"] == "Running")
        ),
        df_adjustments
    )


def log_status_history(session, adj_ids, old_status, new_status, changed_by="SYSTEM"):
    """Record each status transition in ADJ_STATUS_HISTORY."""
    if not adj_ids:
        return
    values = ", ".join([
        f"('{aid}', '{old_status}', '{new_status}', '{changed_by}', CURRENT_TIMESTAMP(), 'Processed by SP_PROCESS_ADJUSTMENT')"
        for aid in adj_ids
    ])
    session.sql(f"""
        INSERT INTO ADJUSTMENT_APP.ADJ_STATUS_HISTORY
            (ADJ_ID, OLD_STATUS, NEW_STATUS, CHANGED_BY, CHANGED_AT, COMMENT)
        VALUES {values}
    """).collect()


def insert_to_dimension_and_get_ids(session, adj_ids, adj_ids_str):
    """
    1. Insert one row per ADJ_ID into DIMENSION.ADJUSTMENT (RECORD_COUNT = NULL,
       updated later once fact-table row counts are known).
    2. Read back the generated DIMENSION.ADJUSTMENT.ADJUSTMENT_ID for each adj.
    3. Store it in ADJ_HEADER.DIMENSION_ADJ_ID.
    4. Return {adj_uuid: dimension_adjustment_id} so callers can use the NUMBER
       key when writing to FACT.*_ADJUSTMENT tables.

    Match uses COBID + PROCESS_TYPE + USERNAME + CREATED_DATE — the four columns
    that are always present and inserted verbatim from ADJ_HEADER.
    """
    session.sql(f"""
        INSERT INTO DIMENSION.ADJUSTMENT (
            COBID, PROCESS_TYPE, ADJUSTMENT_TYPE, SOURCE_COBID,
            ENTITY_CODE, SOURCE_SYSTEM_CODE, DEPARTMENT_CODE, BOOK_CODE,
            TENOR_CODE, CURRENCY_CODE, CURVE_CODE, INSTRUMENT_CODE,
            MEASURE_TYPE_CODE, ADJUSTMENT_VALUE_IN_USD,
            CREATED_DATE, PROCESS_DATE, USERNAME, RUN_STATUS, REASON,
            MUREX_FAMILY, MUREX_GROUP, TRADE_TYPOLOGY, TRADE_CODE,
            SCALE_FACTOR, BATCH_REGION_AREA, SIMULATION_NAME, TRADER_CODE,
            VAR_COMPONENT_ID, VAR_SUB_COMPONENT_ID, SCENARIO_DATE_ID,
            GUARANTEED_ENTITY, STRATEGY, REGION_KEY,
            UNDERLYING_TENOR_CODE, PRODUCT_CATEGORY_ATTRIBUTES,
            ADJUSTMENT_OCCURRENCE, GLOBAL_REFERENCE, FILE_NAME,
            SIMULATION_SOURCE, DAY_TYPE
        )
        SELECT
            COBID, PROCESS_TYPE, ADJUSTMENT_TYPE, SOURCE_COBID,
            ENTITY_CODE, SOURCE_SYSTEM_CODE, DEPARTMENT_CODE, BOOK_CODE,
            TENOR_CODE, CURRENCY_CODE, CURVE_CODE, INSTRUMENT_CODE,
            MEASURE_TYPE_CODE, ADJUSTMENT_VALUE_IN_USD,
            CREATED_DATE, CURRENT_TIMESTAMP(), USERNAME, 'Processed', REASON,
            MUREX_FAMILY, MUREX_GROUP, TRADE_TYPOLOGY, TRADE_CODE,
            SCALE_FACTOR, BATCH_REGION_AREA, SIMULATION_NAME, TRADER_CODE,
            VAR_COMPONENT_ID, VAR_SUB_COMPONENT_ID, SCENARIO_DATE_ID,
            GUARANTEED_ENTITY, STRATEGY, REGION_KEY,
            UNDERLYING_TENOR_CODE, PRODUCT_CATEGORY_ATTRIBUTES,
            ADJUSTMENT_OCCURRENCE, GLOBAL_REFERENCE, FILE_NAME,
            SIMULATION_SOURCE, DAY_TYPE
        FROM ADJUSTMENT_APP.ADJ_HEADER
        WHERE ADJ_ID IN ({adj_ids_str})
    """).collect()

    dim_adj_map = {}
    for aid in adj_ids:
        try:
            row = session.sql(f"""
                SELECT da.ADJUSTMENT_ID
                FROM DIMENSION.ADJUSTMENT da
                INNER JOIN ADJUSTMENT_APP.ADJ_HEADER ah
                    ON  da.COBID        = ah.COBID
                    AND da.PROCESS_TYPE = ah.PROCESS_TYPE
                    AND da.USERNAME     = ah.USERNAME
                    AND da.CREATED_DATE = ah.CREATED_DATE
                WHERE ah.ADJ_ID = '{aid}'
                ORDER BY da.ADJUSTMENT_ID DESC
                LIMIT 1
            """).collect()
            if row:
                dim_adj_id = row[0]['ADJUSTMENT_ID']
                dim_adj_map[aid] = dim_adj_id
                session.sql(f"""
                    UPDATE ADJUSTMENT_APP.ADJ_HEADER
                    SET DIMENSION_ADJ_ID = {dim_adj_id}
                    WHERE ADJ_ID = '{aid}'
                """).collect()
        except Exception as e:
            print(f"Warning: could not retrieve DIMENSION_ADJ_ID for {aid}: {e}")
    return dim_adj_map


def surrogate_key(list_key, key_name):
    """Build a surrogate key expression from multiple columns."""
    parts = [f"COALESCE(CAST({k} AS TEXT), '_dbt_utils_surrogate_key_null_')" for k in list_key]
    joined = " || '-' || ".join(parts)
    return f"MD5(CAST({joined} AS TEXT)) AS {key_name}"


def main(session, process_type, adjustment_action, cobid):
    """
    Process Running adjustments (already claimed by SP_RUN_PIPELINE).

    Flow:
      1. Read config from ADJUSTMENTS_SETTINGS
      2. Filter Running adjustments in ADJ_HEADER
      3. INSERT into DIMENSION.ADJUSTMENT; retrieve generated ADJUSTMENT_ID (NUMBER)
         → store in ADJ_HEADER.DIMENSION_ADJ_ID
      4. For Direct  → read ADJ_LINE_ITEM, set ADJUSTMENT_ID = DIMENSION_ADJ_ID,
                        delete old FACT rows, insert
      5. For Scale   → 3-way UNION ALL using DIMENSION_ADJ_ID as ADJUSTMENT_ID,
                        DENSE_RANK overlap resolution, supersede delete,
                        SCD2 key fix, summary rebuild
      6. Update RECORD_COUNT in ADJ_HEADER + DIMENSION.ADJUSTMENT
      7. Update ADJ_HEADER status; log to ADJ_STATUS_HISTORY
    """
    adj_ids = []
    try:
        result = {
            "process_type": process_type,
            "adjustment_action": adjustment_action,
            "cobid": cobid,
        }

        # ── 1. CONFIG ────────────────────────────────────────────────────
        df_settings = session.table('ADJUSTMENT_APP.ADJUSTMENTS_SETTINGS').filter(
            upper(col('PROCESS_TYPE')) == process_type.upper()
        )
        settings = df_settings.to_pandas().to_dict(orient='records')
        if not settings:
            raise Exception(f'No settings found for process type: {process_type}')

        s = settings[0]
        fact_tbl_name           = s["FACT_TABLE"]
        fact_adjusted_tbl_name  = s.get("FACT_ADJUSTED_TABLE") or s.get("FACT_AJUSTED_TABLE")
        fact_tbl_pk             = s["FACT_TABLE_PK"]
        fact_adj_tbl_name       = s["ADJUSTMENTS_TABLE"]
        fact_adj_summary_name   = s.get("ADJUSTMENTS_SUMMARY_TABLE")
        adj_base_tbl_name       = s["ADJUSTMENT_BASE_TABLE"]   # → ADJUSTMENT_APP.ADJ_HEADER
        metric_name             = s["METRIC_NAME"].upper()
        metric_usd_name         = s["METRIC_USD_NAME"].upper()

        # Parse composite PK
        pk_parts = fact_tbl_pk.split(';')
        if len(pk_parts) > 1:
            key_name   = fact_tbl_name.split('.')[1].split('MEASURES')[0].strip('_') + "_KEY"
            pk_expr    = surrogate_key(pk_parts, key_name)
        else:
            key_name   = pk_parts[0]
            pk_expr    = pk_parts[0]

        # Run log ID
        run_log_id = session.sql("SELECT BATCH.SEQ_RUN_LOG.NEXTVAL AS X").collect()[0]["X"]
        result["run_log_id"] = run_log_id

        # ── 2. READ PENDING ADJUSTMENTS ──────────────────────────────────
        df_adj = session.table(adj_base_tbl_name).filter(
            (col('COBID') == cobid) &
            ((upper(col('PROCESS_TYPE')) == process_type.upper()) |
             (upper(col('PROCESS_TYPE')) == 'FRTBALL')) &
            (col('RUN_STATUS') == 'Running')
        )

        # Column lists for joining
        adj_columns  = df_adj.columns
        fact_adj_tbl = session.table(fact_adj_tbl_name)
        fact_adj_cols = fact_adj_tbl.columns
        fact_tbl     = session.table(fact_tbl_name)
        fact_cols    = fact_tbl.columns

        if fact_adj_summary_name:
            fact_adj_summary_tbl  = session.table(fact_adj_summary_name)
            fact_adj_summary_cols = fact_adj_summary_tbl.columns

        # ═════════════════════════════════════════════════════════════════
        # DIRECT (Upload) PATH
        # ═════════════════════════════════════════════════════════════════
        if adjustment_action.lower() == 'direct':

            df_adj_direct = df_adj.filter(
                (col('ADJUSTMENT_ACTION') == 'Direct') &
                (col('IS_POSITIVE_ADJUSTMENT') == True)
            )

            if df_adj_direct.count() == 0:
                result["message"] = f'No Running Direct adjustments found'
                return json.dumps(result)

            # Collect ADJ_IDs for line-item lookup
            adj_ids = [row["ADJ_ID"] for row in df_adj_direct.select("ADJ_ID").collect()]
            adj_ids_str = ", ".join(f"'{a}'" for a in adj_ids)

            # ── Insert into DIMENSION.ADJUSTMENT FIRST ───────────────────
            # We need DIMENSION.ADJUSTMENT.ADJUSTMENT_ID before writing to FACT,
            # so that FACT.ADJUSTMENT_ID = DIMENSION.ADJUSTMENT.ADJUSTMENT_ID (NUMBER).
            dim_adj_map = {}
            try:
                dim_adj_map = insert_to_dimension_and_get_ids(session, adj_ids, adj_ids_str)
            except Exception as dim_err:
                print(f"Warning: could not insert into DIMENSION.ADJUSTMENT: {dim_err}")

            # Read line items for these adjustments
            df_line_items = session.table("ADJUSTMENT_APP.ADJ_LINE_ITEM").filter(
                (col("ADJ_ID").isin(adj_ids)) &
                (col("IS_DELETED") == False)
            )

            if df_line_items.count() == 0:
                result["message"] = "No line items found for Direct adjustments"
                return json.dumps(result)

            # Convert line items to pandas (preserve ADJ_ID for the ADJUSTMENT_ID mapping)
            _raw_pd = df_line_items.to_pandas()
            df_pd = check_columns(df_line_items, fact_adj_cols, metric_name, metric_usd_name)
            # Set ADJUSTMENT_ID = DIMENSION.ADJUSTMENT.ADJUSTMENT_ID (NUMBER, not our UUID)
            if "ADJUSTMENT_ID" in df_pd.columns and dim_adj_map:
                df_pd["ADJUSTMENT_ID"] = _raw_pd["ADJ_ID"].map(dim_adj_map)

            # Delete existing fact rows for these DIMENSION ADJUSTMENT_IDs
            dim_ids_str = ', '.join(str(v) for v in dim_adj_map.values()) if dim_adj_map else "''"
            session.sql(f"""
                DELETE FROM {fact_adj_tbl_name}
                WHERE COBID = {cobid}
                  AND ADJUSTMENT_ID IN ({dim_ids_str})
            """).collect()

            # Exclude soft-deleted line items
            df_pd_valid = df_pd[df_pd["IS_DELETED"] == False].drop(columns=["IS_DELETED"])
            df_pd_valid["RUN_LOG_ID"] = run_log_id

            # Insert into fact adjustment table
            session.write_pandas(
                df_pd_valid,
                auto_create_table=False,
                table_name=fact_adj_tbl_name.split('.')[-1].upper(),
                schema=fact_adj_tbl_name.split('.')[0].upper()
            )

            # Update status
            update_header_status(session, df_adj_direct, cobid, "Processed")
            log_status_history(session, adj_ids, "Running", "Processed")

            rows_count = len(df_pd_valid)
            session.sql(f"""
                UPDATE ADJUSTMENT_APP.ADJ_HEADER
                SET RECORD_COUNT = {rows_count}
                WHERE ADJ_ID IN ({adj_ids_str})
            """).collect()
            # Update RECORD_COUNT in DIMENSION.ADJUSTMENT now that count is known
            if dim_adj_map:
                session.sql(f"""
                    UPDATE DIMENSION.ADJUSTMENT
                    SET RECORD_COUNT = {rows_count}
                    WHERE ADJUSTMENT_ID IN ({dim_ids_str})
                """).collect()

            result["rows_inserted"] = rows_count
            result["message"] = "Direct adjustments processed successfully"

        # ═════════════════════════════════════════════════════════════════
        # SCALE (Scale / Flatten / Roll) PATH
        # ═════════════════════════════════════════════════════════════════
        elif adjustment_action.lower() == 'scale':

            df_adj_scale = df_adj.filter(
                (col('ADJUSTMENT_ACTION') != 'Direct') &
                (col('IS_POSITIVE_ADJUSTMENT') == True)
            )

            if df_adj_scale.count() == 0:
                result["message"] = f'No Running Scale adjustments found'
                return json.dumps(result)

            adj_ids     = [row["ADJ_ID"] for row in df_adj_scale.select("ADJ_ID").collect()]
            adj_ids_str = ", ".join(f"'{a}'" for a in adj_ids)

            # ── Insert into DIMENSION.ADJUSTMENT FIRST ───────────────────
            # Get DIMENSION.ADJUSTMENT.ADJUSTMENT_ID (NUMBER) before building
            # the TEMP table so FACT.ADJUSTMENT_ID = DIMENSION.ADJUSTMENT.ADJUSTMENT_ID.
            dim_adj_map = {}
            try:
                dim_adj_map = insert_to_dimension_and_get_ids(session, adj_ids, adj_ids_str)
            except Exception as dim_err:
                print(f"Warning: could not insert into DIMENSION.ADJUSTMENT: {dim_err}")
            dim_ids_str = ', '.join(str(v) for v in dim_adj_map.values()) if dim_adj_map else "0"

            # ── Join columns (fact ∩ adj, minus exclusions) ──────────────
            exclude_join = ['COBID', 'IS_OFFICIAL_SOURCE', 'STRATEGY',
                            'LOAD_TIMESTAMP', 'REGION_AREA_KEY']
            join_cols = [c for c in fact_cols if c in adj_columns and c not in exclude_join]

            # ── Select columns (fact minus metrics/system cols) ──────────
            exclude_select = [metric_name, metric_usd_name, 'COBID',
                              'RAVEN_FILENAME', 'RAVEN_FILE_ROW_NUMBER',
                              'LOAD_TIMESTAMP', 'RUN_LOG_ID']
            fact_non_metric = [c for c in fact_cols if c not in exclude_select]
            fact_non_metric_matched = [c for c in fact_non_metric if c in fact_adj_cols]

            select_non_metric = ', '.join([f'fact.{c}' for c in fact_non_metric_matched])
            insert_non_metric = ', '.join(fact_non_metric_matched)

            # Detect if local-currency metric exists in the fact table
            # (e.g. FACT.VAR_MEASURES only has PNL_VECTOR_VALUE_IN_USD, not PNL_VECTOR_VALUE)
            if metric_name not in fact_cols:
                metric_name = metric_usd_name  # collapse: treat USD column as the only metric

            if metric_name == metric_usd_name:
                # Single-metric fact table
                select_measure  = (f"fact.{metric_usd_name} * adjust.SCALE_FACTOR_ADJUSTED "
                                   f"AS {metric_usd_name}")
                metric_col_list = metric_usd_name
                metric_sum_list = f"SUM({metric_usd_name}) AS {metric_usd_name}"
            else:
                # Dual-metric fact table (local + USD)
                select_measure  = (
                    f"fact.{metric_name} * adjust.SCALE_FACTOR_ADJUSTED AS {metric_name}, "
                    f"fact.{metric_usd_name} * adjust.SCALE_FACTOR_ADJUSTED AS {metric_usd_name}"
                )
                metric_col_list = f"{metric_name}, {metric_usd_name}"
                metric_sum_list = (f"SUM({metric_name}) AS {metric_name}, "
                                   f"SUM({metric_usd_name}) AS {metric_usd_name}")

            # ── Build perm INSERT column list (only cols in target adj table) ───
            _adj_set = set(fact_adj_cols)
            _perm = (['COBID', 'ADJUSTMENT_ID']
                     + fact_non_metric_matched
                     + [mc.strip() for mc in metric_col_list.split(',')
                        if mc.strip() in _adj_set]
                     + [c for c in ['RUN_LOG_ID', 'LOAD_TIMESTAMP',
                                    'ADJUSTMENT_CREATED_TIMESTAMP']
                        if c in _adj_set])
            perm_col_list = ', '.join(_perm)

            # ── Base FROM/WHERE ──────────────────────────────────────────
            from_where = f"""
                FROM {fact_tbl_name} fact
                INNER JOIN {adj_base_tbl_name} adjust
                WHERE adjust.COBID = {cobid}
                  AND adjust.IS_DELETED = FALSE
                  AND adjust.RUN_STATUS = 'Running'
                  AND (adjust.PROCESS_TYPE = '{process_type}' OR adjust.PROCESS_TYPE = 'FRTBALL')
                  AND {metric_usd_name} IS NOT NULL
            """

            # Special dimension filters: key-mapped columns (BOOK_KEY, TRADE_KEY)
            # ADJ_HEADER stores codes (BOOK_CODE, TRADE_CODE) but FACT stores keys.
            # We join through DIMENSION tables to apply these filters.
            if "BOOK_KEY" in fact_non_metric_matched:
                from_where += (
                    "\n AND EXISTS (SELECT 1 FROM DIMENSION.BOOK bk "
                    "WHERE bk.BOOK_KEY = COALESCE(fact.BOOK_KEY, -1) "
                    "AND (bk.BOOK_CODE = adjust.BOOK_CODE OR adjust.BOOK_CODE IS NULL) "
                    "AND (bk.DEPARTMENT_CODE = adjust.DEPARTMENT_CODE OR adjust.DEPARTMENT_CODE IS NULL))"
                )
            if "TRADE_KEY" in fact_non_metric_matched:
                from_where += (
                    "\n AND EXISTS (SELECT 1 FROM DIMENSION.TRADE td "
                    "WHERE td.TRADE_KEY = COALESCE(fact.TRADE_KEY, -1) "
                    "AND (td.TRADE_CODE = adjust.TRADE_CODE OR adjust.TRADE_CODE IS NULL) "
                    "AND (td.STRATEGY = adjust.STRATEGY OR adjust.STRATEGY IS NULL) "
                    "AND (td.TRADE_TYPOLOGY = adjust.TRADE_TYPOLOGY OR adjust.TRADE_TYPOLOGY IS NULL))"
                )

            join_cond = '\n'.join([
                f"AND (adjust.{c} = fact.{c} OR adjust.{c} IS NULL)"
                for c in join_cols
            ])

            # ── 3-way UNION ALL ──────────────────────────────────────────
            # ADJUSTMENT_ID = DIMENSION.ADJUSTMENT.ADJUSTMENT_ID (NUMBER), not our UUID.
            # ADJ_HEADER.DIMENSION_ADJ_ID was populated above by insert_to_dimension_and_get_ids.
            select_scale = (
                f"SELECT DISTINCT adjust.COBID, adjust.DIMENSION_ADJ_ID AS ADJUSTMENT_ID, "
                f"adjust.CREATED_DATE AS ADJUSTMENT_CREATED_TIMESTAMP, "
                f"{select_non_metric}, {select_measure}"
            )
            select_flatten = select_scale.replace(
                'adjust.SCALE_FACTOR_ADJUSTED', '-1'
            )

            # Build the key expression
            select_with_keys = "*" if key_name == pk_expr else f"{pk_expr}, *"
            exclude_keys = "*" if key_name == pk_expr else f"* EXCLUDE ({key_name})"

            insert_sql = f"""
            CREATE OR REPLACE TABLE {fact_adj_tbl_name}_TEMP
            (
                COBID, ADJUSTMENT_ID, ADJUSTMENT_CREATED_TIMESTAMP,
                {insert_non_metric}, {metric_col_list},
                RUN_LOG_ID, LOAD_TIMESTAMP
            ) AS
            WITH cte AS (
                -- ① Scale/Flatten same COB (COBID = SOURCE_COBID)
                {select_scale} {from_where}
                AND fact.COBID = adjust.SOURCE_COBID
                AND adjust.COBID = adjust.SOURCE_COBID
                {join_cond}
                UNION ALL
                -- ② Roll/Scale cross-COB: always reads from original fact table at SOURCE_COBID
                {select_scale} {from_where}
                AND fact.COBID = adjust.SOURCE_COBID
                AND adjust.COBID <> adjust.SOURCE_COBID
                {join_cond}
                UNION ALL
                -- ③ Flatten current COB (for cross-COB: zero out existing values at target COB)
                {select_flatten} {from_where}
                AND fact.COBID = adjust.COBID
                AND adjust.COBID <> adjust.SOURCE_COBID
                {join_cond}
            ),
            fact_key AS (
                SELECT {select_with_keys}
                FROM cte
            ),
            ranked AS (
                SELECT
                    {exclude_keys},
                    DENSE_RANK() OVER (
                        PARTITION BY {key_name}
                        ORDER BY ADJUSTMENT_CREATED_TIMESTAMP DESC, ADJUSTMENT_ID DESC
                    ) AS ROW_NUM
                FROM fact_key
            )
            SELECT
                ranked.* EXCLUDE (ROW_NUM),
                {run_log_id} AS RUN_LOG_ID,
                CURRENT_TIMESTAMP() AS LOAD_TIMESTAMP
            FROM ranked
            WHERE ROW_NUM = 1
              AND {metric_usd_name} <> 0
            """

            result["insert_cmd"] = insert_sql
            session.sql(insert_sql).collect()

            # ── Supersede: delete older adjustments for positions in TEMP ──
            # The DENSE_RANK in the CTE only sees the *current* Running batch.
            # Adjustments processed in earlier SP calls (e.g. a Flatten with
            # narrower filters) are already in the fact table and won't appear
            # in the CTE, so DENSE_RANK never evicts them.
            # Fix: for every position the TEMP table covers, delete any row in
            # the fact table that belongs to a *different* (older) adjustment.
            # Blocking guarantees the current batch is always the newest for
            # any position it touches, so deleting older rows is always correct.
            _supersede_dims = [k for k in pk_parts if k.upper() != 'COBID']
            if _supersede_dims:
                # dim_ids_str contains DIMENSION.ADJUSTMENT NUMBERs — no quoting needed
                _pos_join = " AND ".join(
                    [f"fa.{k} = tmp.{k}" for k in _supersede_dims]
                )
                session.sql(f"""
                    DELETE FROM {fact_adj_tbl_name} fa
                    WHERE fa.COBID = {cobid}
                      AND fa.ADJUSTMENT_ID NOT IN ({dim_ids_str})
                      AND EXISTS (
                          SELECT 1 FROM {fact_adj_tbl_name}_TEMP tmp
                          WHERE {_pos_join}
                      )
                """).collect()

            # ── Delete current batch's own previous rows (re-run scenario) ─
            session.sql(f"""
                DELETE FROM {fact_adj_tbl_name}
                WHERE COBID = {cobid}
                  AND ADJUSTMENT_ID IN ({dim_ids_str})
            """).collect()

            # Delete from summary table
            if fact_adj_summary_name:
                fact_adj_summary_tbl.delete(
                    fact_adj_summary_tbl["COBID"] == df_adj_scale["COBID"],
                    df_adj_scale
                )

            # ── Insert from temp table ───────────────────────────────────
            perm_insert = f"""
            INSERT INTO {fact_adj_tbl_name}
            ({perm_col_list})
            SELECT {perm_col_list}
            FROM {fact_adj_tbl_name}_TEMP
            """
            session.sql(perm_insert).collect()
            session.sql(f"DROP TABLE IF EXISTS {fact_adj_tbl_name}_TEMP").collect()

            # ── SCD2 key fix for cross-COB (Roll) adjustments ────────────
            scd2_update = f"""
            UPDATE {fact_adj_tbl_name} tgt
            SET tgt.TRADE_KEY = src.TRADE_KEY_ADJ,
                tgt.COMMON_INSTRUMENT_KEY = src.COMMON_INSTRUMENT_KEY_ADJ,
                tgt.COMMON_INSTRUMENT_FCD_KEY = src.COMMON_INSTRUMENT_FCD_KEY_ADJ
            FROM (
                WITH adj_cte AS (
                    SELECT DISTINCT
                        f.ADJUSTMENT_ID, f.COBID, f.BOOK_KEY, f.TRADE_KEY,
                        f.COMMON_INSTRUMENT_KEY, f.COMMON_INSTRUMENT_FCD_KEY,
                        ad.SOURCE_COBID
                    FROM {fact_adj_tbl_name} f
                    INNER JOIN {adj_base_tbl_name} ad
                        ON f.ADJUSTMENT_ID = ad.DIMENSION_ADJ_ID AND f.COBID = ad.COBID
                    WHERE f.COBID = {cobid}
                      AND ad.COBID <> ad.SOURCE_COBID
                )
                SELECT DISTINCT
                    f.*,
                    td2.TRADE_KEY          AS TRADE_KEY_ADJ,
                    ci2.COMMON_INSTRUMENT_KEY AS COMMON_INSTRUMENT_KEY_ADJ,
                    cif.COMMON_INSTRUMENT_FCD_KEY AS COMMON_INSTRUMENT_FCD_KEY_ADJ
                FROM adj_cte f
                INNER JOIN DIMENSION.BOOK b   ON f.BOOK_KEY = b.BOOK_KEY
                INNER JOIN DIMENSION.TRADE td ON td.TRADE_KEY = f.TRADE_KEY
                INNER JOIN DIMENSION.TRADE td2
                    ON  td.TRADE_CODE = td2.TRADE_CODE
                    AND b.BOOK_CODE  = td2.BOOK_CODE
                    AND TO_DATE(f.SOURCE_COBID::STRING, 'YYYYMMDD')
                        BETWEEN td2.EFFECTIVE_START_DATE AND td2.EFFECTIVE_END_DATE
                INNER JOIN DIMENSION.COMMON_INSTRUMENT ci
                    ON ci.COMMON_INSTRUMENT_KEY = f.COMMON_INSTRUMENT_KEY
                INNER JOIN DIMENSION.COMMON_INSTRUMENT ci2
                    ON  ci.INSTRUMENT_CODE = ci2.INSTRUMENT_CODE
                    AND TO_DATE(f.SOURCE_COBID::STRING, 'YYYYMMDD')
                        BETWEEN ci2.EFFECTIVE_START_DATE AND ci2.EFFECTIVE_END_DATE
                INNER JOIN DIMENSION.COMMON_INSTRUMENT_FCD cif
                    ON  ci2.INSTRUMENT_KEY = cif.INSTRUMENT_KEY
                    AND TO_DATE(f.SOURCE_COBID::STRING, 'YYYYMMDD')
                        BETWEEN cif.EFFECTIVE_START_DATE AND cif.EFFECTIVE_END_DATE
                WHERE f.TRADE_KEY <> TRADE_KEY_ADJ
                   OR f.COMMON_INSTRUMENT_KEY <> COMMON_INSTRUMENT_KEY_ADJ
                   OR f.COMMON_INSTRUMENT_FCD_KEY <> COMMON_INSTRUMENT_FCD_KEY_ADJ
            ) src
            WHERE tgt.TRADE_KEY = src.TRADE_KEY
              AND tgt.COMMON_INSTRUMENT_KEY = src.COMMON_INSTRUMENT_KEY
              AND tgt.COMMON_INSTRUMENT_FCD_KEY = src.COMMON_INSTRUMENT_FCD_KEY
              AND tgt.COBID = src.COBID
            """
            session.sql(scd2_update).collect()

            # ── Rebuild summary ──────────────────────────────────────────
            if fact_adj_summary_name:
                summary_non_metric = ', '.join([
                    c for c in fact_adj_summary_cols
                    if c not in {metric_name, metric_usd_name}
                ])
                summary_insert = f"""
                INSERT INTO {fact_adj_summary_name}
                ({summary_non_metric}, {metric_col_list})
                SELECT {summary_non_metric},
                       {metric_sum_list}
                FROM {fact_adj_tbl_name}
                WHERE COBID = {cobid}
                GROUP BY ALL
                """
                session.sql(summary_insert).collect()

            # ── Count rows inserted and update RECORD_COUNT ──────────────
            rows_count_row = session.sql(f"""
                SELECT COUNT(*) AS CNT
                FROM {fact_adj_tbl_name}
                WHERE COBID = {cobid}
                  AND ADJUSTMENT_ID IN ({dim_ids_str})
            """).collect()
            rows_count = rows_count_row[0]["CNT"] if rows_count_row else 0
            session.sql(f"""
                UPDATE ADJUSTMENT_APP.ADJ_HEADER
                SET RECORD_COUNT = {rows_count}
                WHERE ADJ_ID IN ({adj_ids_str})
            """).collect()
            # Update RECORD_COUNT in DIMENSION.ADJUSTMENT now that count is known
            if dim_adj_map:
                session.sql(f"""
                    UPDATE DIMENSION.ADJUSTMENT
                    SET RECORD_COUNT = {rows_count}
                    WHERE ADJUSTMENT_ID IN ({dim_ids_str})
                """).collect()
            result["rows_inserted"] = rows_count

            # ── Update status ────────────────────────────────────────────
            update_header_status(session, df_adj_scale, cobid, "Processed")
            log_status_history(session, adj_ids, "Running", "Processed")

            result["message"] = "Scale adjustments processed successfully"

        else:
            result["message"] = f"Invalid adjustment_action: {adjustment_action}"

    except Exception as e:
        error_msg = str(e).replace("'", "")
        print(f"Error: {error_msg}")
        result["message"] = f"Error: {error_msg}"

        # Try to mark as Failed
        try:
            df_adj_err = session.table(adj_base_tbl_name).filter(
                (col('COBID') == cobid) &
                (upper(col('PROCESS_TYPE')) == process_type.upper()) &
                (col('RUN_STATUS') == 'Running')
            )
            update_header_status(session, df_adj_err, cobid, "Failed", error_msg)
            if adj_ids:
                log_status_history(session, adj_ids, "Running", "Failed")
        except:
            pass

    return json.dumps(result)
$$;
