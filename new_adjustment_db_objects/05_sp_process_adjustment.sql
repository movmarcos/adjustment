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

CREATE OR ALTER PROCEDURE ADJUSTMENT_APP.SP_PROCESS_ADJUSTMENT(
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
    carried = [(m["payload_field"], m["target_column"], m.get("type", "string"))
               for m in fm]

    base_from = (f"FROM ADJUSTMENT_APP.ADJ_LINE_ITEM_JSON j "
                 f"WHERE j.ADJ_ID IN ({adj_ids_str}) AND j.IS_DELETED = FALSE")

    if unpivot:
        legs = []
        name_field  = unpivot["measure_name_field"]
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


# ── INSERT_SOURCE mapping for PowerBI ────────────────────────────────────
PBI_INSERT_SOURCE = {
    'VAR':         'LOAD_VAR_ADJUSTMENT',
    'STRESS':      'LOAD_STRESS_ADJUSTMENT',
    'SENSITIVITY': 'LOAD_SENSITIVITY_ADJUSTMENT',
    'FRTB':        'LOAD_FRTB_ADJUSTMENT',
    'FRTBDRC':     'LOAD_FRTB_ADJUSTMENT',
    'FRTBRRAO':    'LOAD_FRTB_ADJUSTMENT',
    'FRTBALL':     'LOAD_FRTB_ADJUSTMENT',
}

def trigger_powerbi_refresh(session, process_type, run_log_id):
    """Call FACT.UPDATE_POWERBI_FOR_ADJUSTMENTS to queue a PowerBI refresh."""
    insert_source = PBI_INSERT_SOURCE.get(process_type.upper(),
                                          f'LOAD_{process_type.upper()}_ADJUSTMENT')
    try:
        session.sql(f"""
            CALL FACT.UPDATE_POWERBI_FOR_ADJUSTMENTS(
                '{process_type}',
                'RaptorReporting',
                '{insert_source}',
                '{run_log_id}',
                '0'
            )
        """).collect()
    except Exception as pbi_err:
        print(f"Warning: PowerBI refresh trigger failed: {pbi_err}")


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
            CREATED_DATE, CURRENT_TIMESTAMP(), USERNAME, 'Running', REASON,
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
      5. For Scale   → 3-way UNION ALL (same-COB scale/flatten + cross-COB roll legs),
                        netted into one delta row per position (cross-COB pairs collapsed),
                        DENSE_RANK overlap resolution, supersede delete,
                        SCD2 key fix, summary rebuild
      6. Update RECORD_COUNT in ADJ_HEADER + DIMENSION.ADJUSTMENT
      7. Update ADJ_HEADER status; log to ADJ_STATUS_HISTORY
    """
    adj_ids = []
    dim_adj_map = {}
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

        # Run log ID — insert into BATCH.RUN_LOG so UPDATE_POWERBI_FOR_ADJUSTMENTS can find it
        run_log_id = session.sql("SELECT BATCH.SEQ_RUN_LOG.NEXTVAL AS X").collect()[0]["X"]
        result["run_log_id"] = run_log_id
        session.sql(f"""
            CALL BATCH.LOAD_RUN_LOG(
                {run_log_id},
                {cobid},
                'FACT.SP_PROCESS_ADJUSTMENT',
                '{process_type}',
                0, 0, 'false', ''
            )
        """).collect()

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
                # the legacy key-default behaviour (these are NOT NULL in the fact
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

            # Does this batch contain a cross-COB Roll? Only then do we read the
            # adjusted view (FACT_ADJUSTED_TABLE). Keeping the adjusted-view leg
            # out of same-COB Scale/Flatten batches means those never depend on a
            # combined view existing/being correctly shaped — so a misconfigured
            # adjusted view can only ever affect Rolls, never all scaling.
            has_cross_cob = df_adj_scale.filter(
                col('COBID') != col('SOURCE_COBID')
            ).count() > 0

            # Store RUN_LOG_ID in ADJ_HEADER for traceability
            session.sql(f"""
                UPDATE ADJUSTMENT_APP.ADJ_HEADER
                SET RUN_LOG_ID = {run_log_id}
                WHERE ADJ_ID IN ({adj_ids_str})
            """).collect()

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
                            'LOAD_TIMESTAMP', 'REGION_AREA_KEY', 'RUN_LOG_ID']
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

            # ── Netting: collapse cross-COB pairs into one delta row per position ──
            # A cross-COB roll produces 2 CTE rows per position (leg ② scale +
            # leg ③ flatten). Grouping by the surrogate/PK key and summing the
            # metric collapses them into a single net-delta row (e.g. -1 + 1.5 → 0.5).
            # Same-COB rows are already one row per position; grouping is a no-op.
            # HAVING <> 0 drops positions whose net change is exactly zero.
            non_metric_any = ', '.join([f"ANY_VALUE({c}) AS {c}" for c in fact_non_metric_matched])
            if metric_name == metric_usd_name:
                metric_sums = f"SUM({metric_usd_name}) AS {metric_usd_name}"
            else:
                metric_sums = (f"SUM({metric_name}) AS {metric_name}, "
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

            # ── Detect which filter fields actually have values ─────────
            # When an adjustment leaves a filter NULL it means "match all" —
            # the OR adjust.X IS NULL clause always returns TRUE, so the
            # entire EXISTS / direct-join condition is a no-op that still
            # costs a dimension table scan.  By checking up front which
            # fields have at least one non-NULL value in the current batch
            # we can skip unused lookups entirely.
            _dim_fields = [
                'BOOK_CODE', 'DEPARTMENT_CODE', 'TRADER_CODE',
                'GUARANTEED_ENTITY', 'REGION_KEY',
                'TRADE_CODE', 'STRATEGY', 'TRADE_TYPOLOGY',
                'ENTITY_CODE', 'INSTRUMENT_CODE',
                'SIMULATION_NAME', 'SIMULATION_SOURCE',
                'VAR_COMPONENT_ID', 'VAR_SUB_COMPONENT_ID', 'DAY_TYPE',
            ]
            _check_fields = list(set(join_cols + _dim_fields))
            _check_in_adj = [f for f in _check_fields if f in adj_columns]
            if _check_in_adj:
                _adj_sample = df_adj_scale.select(_check_in_adj).to_pandas()
                _has = {f: _adj_sample[f].notna().any() for f in _check_in_adj}
            else:
                _has = {}

            def _any_has(*fields):
                """True if at least one adjustment has a value for any of the listed fields."""
                return any(_has.get(f, False) for f in fields)

            # ── Dimension lookup EXISTS filters ─────────────────────────────
            # Only added when (a) the KEY column exists in the fact table AND
            # (b) the current batch actually filters on the mapped fields.

            # 1. BOOK_KEY → BOOK_CODE, DEPARTMENT_CODE, TRADER_CODE,
            #               GUARANTEED_ENTITY, REGION_KEY
            if "BOOK_KEY" in fact_cols and _any_has(
                    'BOOK_CODE', 'DEPARTMENT_CODE', 'TRADER_CODE',
                    'GUARANTEED_ENTITY', 'REGION_KEY'):
                from_where += (
                    "\n AND EXISTS (SELECT 1 FROM DIMENSION.BOOK bk "
                    "WHERE bk.BOOK_KEY = COALESCE(fact.BOOK_KEY, -1) "
                    "AND (bk.BOOK_CODE = adjust.BOOK_CODE OR adjust.BOOK_CODE IS NULL) "
                    "AND (bk.DEPARTMENT_CODE = adjust.DEPARTMENT_CODE OR adjust.DEPARTMENT_CODE IS NULL) "
                    "AND (bk.PRIMARY_TRADER_CODE = adjust.TRADER_CODE OR adjust.TRADER_CODE IS NULL) "
                    "AND (bk.GUARANTEED_ENTITY = adjust.GUARANTEED_ENTITY OR adjust.GUARANTEED_ENTITY IS NULL) "
                    "AND (bk.REGION_KEY = adjust.REGION_KEY OR adjust.REGION_KEY IS NULL))"
                )

            # 2. TRADE_KEY → TRADE_CODE, STRATEGY, TRADE_TYPOLOGY
            if "TRADE_KEY" in fact_cols and _any_has(
                    'TRADE_CODE', 'STRATEGY', 'TRADE_TYPOLOGY'):
                from_where += (
                    "\n AND EXISTS (SELECT 1 FROM DIMENSION.TRADE td "
                    "WHERE td.TRADE_KEY = COALESCE(fact.TRADE_KEY, -1) "
                    "AND (td.TRADE_CODE = adjust.TRADE_CODE OR adjust.TRADE_CODE IS NULL) "
                    "AND (td.STRATEGY = adjust.STRATEGY OR adjust.STRATEGY IS NULL) "
                    "AND (td.TRADE_TYPOLOGY = adjust.TRADE_TYPOLOGY OR adjust.TRADE_TYPOLOGY IS NULL))"
                )

            # 3. ENTITY_KEY → ENTITY_CODE (SENSITIVITY_MEASURES, STRESS_MEASURES)
            if ("ENTITY_KEY" in fact_cols and "ENTITY_CODE" not in fact_cols
                    and _any_has('ENTITY_CODE')):
                from_where += (
                    "\n AND EXISTS (SELECT 1 FROM DIMENSION.ENTITY ent "
                    "WHERE ent.ENTITY_KEY = COALESCE(fact.ENTITY_KEY, -1) "
                    "AND (ent.ENTITY_CODE = adjust.ENTITY_CODE OR adjust.ENTITY_CODE IS NULL))"
                )

            # 4. COMMON_INSTRUMENT_KEY → INSTRUMENT_CODE
            if ("COMMON_INSTRUMENT_KEY" in fact_cols and "INSTRUMENT_CODE" not in fact_cols
                    and _any_has('INSTRUMENT_CODE')):
                from_where += (
                    "\n AND EXISTS (SELECT 1 FROM DIMENSION.COMMON_INSTRUMENT ci "
                    "WHERE ci.COMMON_INSTRUMENT_KEY = COALESCE(fact.COMMON_INSTRUMENT_KEY, -1) "
                    "AND (ci.INSTRUMENT_CODE = adjust.INSTRUMENT_CODE OR adjust.INSTRUMENT_CODE IS NULL))"
                )

            # 5. STRESS_SIMULATION_KEY → SIMULATION_NAME, SIMULATION_SOURCE
            if ("STRESS_SIMULATION_KEY" in fact_cols
                    and _any_has('SIMULATION_NAME', 'SIMULATION_SOURCE')):
                from_where += (
                    "\n AND EXISTS (SELECT 1 FROM DIMENSION.STRESS_SIMULATION ss "
                    "WHERE ss.STRESS_SIMULATION_KEY = COALESCE(fact.STRESS_SIMULATION_KEY, -1) "
                    "AND (ss.STRESS_SIMULATION_NAME = adjust.SIMULATION_NAME OR adjust.SIMULATION_NAME IS NULL) "
                    "AND (ss.SIMULATION_SOURCE = adjust.SIMULATION_SOURCE OR adjust.SIMULATION_SOURCE IS NULL))"
                )

            # 6. VAR_SUBCOMPONENT_ID → VAR_COMPONENT_ID, VAR_SUB_COMPONENT_ID, DAY_TYPE
            if ("VAR_SUBCOMPONENT_ID" in fact_cols
                    and _any_has('VAR_COMPONENT_ID', 'VAR_SUB_COMPONENT_ID', 'DAY_TYPE')):
                from_where += (
                    "\n AND EXISTS (SELECT 1 FROM DIMENSION.VAR_SUB_COMPONENT vsc "
                    "WHERE vsc.VAR_SUB_COMPONENT_ID = COALESCE(fact.VAR_SUBCOMPONENT_ID, -1) "
                    "AND (vsc.VAR_COMPONENT_ID = adjust.VAR_COMPONENT_ID OR adjust.VAR_COMPONENT_ID IS NULL) "
                    "AND (vsc.VAR_SUB_COMPONENT_ID = adjust.VAR_SUB_COMPONENT_ID OR adjust.VAR_SUB_COMPONENT_ID IS NULL) "
                    "AND (vsc.VAR_SUB_COMPONENT_DAY_TYPE = adjust.DAY_TYPE OR adjust.DAY_TYPE IS NULL))"
                )

            # ── Direct join conditions (auto-detected column matches) ───────
            # Also skip conditions where no adjustment in the batch has a value.
            join_cond = '\n'.join([
                f"AND (adjust.{c} = fact.{c} OR adjust.{c} IS NULL)"
                for c in join_cols
                if _has.get(c, True)
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

            # ── Cross-COB (Roll) reads the ADJUSTED view ─────────────────────
            # A cross-COB Roll must carry the source COB's *adjusted* value
            # (original + existing adjustments) forward, not the raw original.
            # So leg ② reads from FACT_ADJUSTED_TABLE — a combined view with the
            # SAME schema as the fact table (e.g. FACT.VAR_MEASURES_COMBINED,
            # FACT.SENSITIVITY_MEASURES_ADJUSTED). Mirrors the reference proc's
            # `replace(fact_tbl_name, fact_ajusted_tbl_name)`. Falls back to the
            # original fact table when no distinct adjusted view is configured.
            from_where_adj = (
                from_where.replace(fact_tbl_name, fact_adjusted_tbl_name)
                if fact_adjusted_tbl_name and fact_adjusted_tbl_name != fact_tbl_name
                else from_where
            )

            # Cross-COB Roll leg ② — ONLY included when the batch has a cross-COB
            # roll AND a distinct adjusted view is configured. It reads the source
            # COB's adjusted value (× factor) from FACT_ADJUSTED_TABLE; netted with
            # leg ③ (flatten of the target original) →
            #   adjusted(target) = factor × adjusted(source).
            # Omitting it for same-COB batches means Scale/Flatten never reference
            # the adjusted view, so a misconfigured combined view can only break
            # Roll — never all scaling.
            roll_leg = ""
            if has_cross_cob and fact_adjusted_tbl_name and fact_adjusted_tbl_name != fact_tbl_name:
                roll_leg = f"""
                UNION ALL
                -- ② Roll cross-COB: source COB's ADJUSTED value from FACT_ADJUSTED_TABLE
                {select_scale} {from_where_adj}
                AND fact.COBID = adjust.SOURCE_COBID
                AND adjust.COBID <> adjust.SOURCE_COBID
                {join_cond}"""

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
                {join_cond}{roll_leg}
                UNION ALL
                -- ③ Flatten current COB (offsets existing values at target COB for cross-COB roll)
                {select_flatten} {from_where}
                AND fact.COBID = adjust.COBID
                AND adjust.COBID <> adjust.SOURCE_COBID
                {join_cond}
            ),
            fact_key AS (
                SELECT {select_with_keys}
                FROM cte
            ),
            netted AS (
                -- Collapse cross-COB roll pairs (legs ② + ③) into one net-delta row per
                -- position: e.g. -1.0 (flatten) + 1.5 (scale) → 0.5.
                -- Same-COB rows (leg ①) are already one row per position; GROUP BY is a no-op.
                -- ANY_VALUE is safe for non-metric dimension columns because legs ② and ③
                -- represent the same position; SCD2 key discrepancies are fixed post-insert.
                -- HAVING <> 0 drops positions whose net change is exactly zero.
                SELECT
                    {key_name},
                    ANY_VALUE(COBID)                          AS COBID,
                    ANY_VALUE(ADJUSTMENT_ID)                  AS ADJUSTMENT_ID,
                    ANY_VALUE(ADJUSTMENT_CREATED_TIMESTAMP)   AS ADJUSTMENT_CREATED_TIMESTAMP,
                    {non_metric_any},
                    {metric_sums}
                FROM fact_key
                GROUP BY {key_name}
                HAVING SUM({metric_usd_name}) <> 0
            ),
            ranked AS (
                SELECT
                    {exclude_keys},
                    DENSE_RANK() OVER (
                        PARTITION BY {key_name}
                        ORDER BY ADJUSTMENT_CREATED_TIMESTAMP DESC, ADJUSTMENT_ID DESC
                    ) AS ROW_NUM
                FROM netted
            )
            SELECT
                ranked.* EXCLUDE (ROW_NUM),
                {run_log_id} AS RUN_LOG_ID,
                CURRENT_TIMESTAMP() AS LOAD_TIMESTAMP
            FROM ranked
            WHERE ROW_NUM = 1
            """

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
            # Update RECORD_COUNT + RUN_STATUS in DIMENSION.ADJUSTMENT
            if dim_adj_map:
                session.sql(f"""
                    UPDATE DIMENSION.ADJUSTMENT
                    SET RECORD_COUNT = {rows_count},
                        RUN_STATUS   = 'Processed'
                    WHERE ADJUSTMENT_ID IN ({dim_ids_str})
                """).collect()
            result["rows_inserted"] = rows_count

            # ── Update status ────────────────────────────────────────────
            update_header_status(session, df_adj_scale, cobid, "Processed")
            log_status_history(session, adj_ids, "Running", "Processed")

            result["message"] = "Scale adjustments processed successfully"

            # ── Close run log and trigger PowerBI refresh ──────────────
            try:
                session.sql(f"""
                    CALL BATCH.LOAD_RUN_LOG_END_WITH_DETAIL({run_log_id}, '{{"status":"Processed"}}')
                """).collect()
            except Exception as rl_err:
                print(f"Warning: Run log close failed: {rl_err}")
            trigger_powerbi_refresh(session, process_type, run_log_id)

        # ═════════════════════════════════════════════════════════════════
        # ENTITY ROLL PATH
        # Full delete + copy: no delta calculation.
        # 1. Delete target COB+entity from FACT_TABLE and FACT_ADJUSTED_TABLE
        # 2. Copy source COB+entity into FACT_TABLE (replace COBID)
        # 3. Copy source COB+entity adjustments into FACT_ADJUSTED_TABLE
        #    (replace COBID, consolidate all source ADJUSTMENT_IDs → single new ID)
        # ═════════════════════════════════════════════════════════════════
        elif adjustment_action.lower() == 'entityroll':

            df_adj_er = df_adj.filter(
                (col('ADJUSTMENT_ACTION') == 'EntityRoll') &
                (col('IS_POSITIVE_ADJUSTMENT') == True)
            )

            if df_adj_er.count() == 0:
                result["message"] = 'No Running EntityRoll adjustments found'
                return json.dumps(result)

            adj_ids = [row["ADJ_ID"] for row in df_adj_er.select("ADJ_ID").collect()]
            adj_ids_str = ", ".join(f"'{a}'" for a in adj_ids)

            # Store RUN_LOG_ID in ADJ_HEADER for traceability
            session.sql(f"""
                UPDATE ADJUSTMENT_APP.ADJ_HEADER
                SET RUN_LOG_ID = {run_log_id}
                WHERE ADJ_ID IN ({adj_ids_str})
            """).collect()

            # EntityRoll processes one adjustment at a time (entity-level operation)
            er_row = df_adj_er.collect()[0]
            source_cobid = er_row["SOURCE_COBID"]
            entity_code  = er_row["ENTITY_CODE"]

            if not entity_code:
                raise Exception("EntityRoll requires ENTITY_CODE")
            if not source_cobid or int(source_cobid) == int(cobid):
                raise Exception("EntityRoll requires a SOURCE_COBID different from target COBID")

            esc_entity = str(entity_code).replace("'", "''")

            # ── Insert into DIMENSION.ADJUSTMENT ────────────────────────
            dim_adj_map = {}
            try:
                dim_adj_map = insert_to_dimension_and_get_ids(session, adj_ids, adj_ids_str)
            except Exception as dim_err:
                print(f"Warning: could not insert into DIMENSION.ADJUSTMENT: {dim_err}")

            # The single DIMENSION.ADJUSTMENT.ADJUSTMENT_ID for the new roll
            new_dim_adj_id = list(dim_adj_map.values())[0] if dim_adj_map else None

            # ── Detect entity column name ───────────────────────────────
            # FACT tables use ENTITY_CODE or ENTITY_KEY — detect which exists
            entity_col = None
            for candidate in ['ENTITY_CODE', 'ENTITY_KEY']:
                if candidate in fact_cols:
                    entity_col = candidate
                    break
            if not entity_col:
                raise Exception(f"Cannot find entity column (ENTITY_CODE or ENTITY_KEY) in {fact_tbl_name}")

            entity_adj_col = None
            for candidate in ['ENTITY_CODE', 'ENTITY_KEY']:
                if candidate in fact_adj_cols:
                    entity_adj_col = candidate
                    break
            if not entity_adj_col:
                raise Exception(f"Cannot find entity column in {fact_adj_tbl_name}")

            # ── STEP 1: Delete target COB+entity from FACT_TABLE ────────
            del_fact_sql = f"""
                DELETE FROM {fact_tbl_name}
                WHERE COBID = {int(cobid)}
                  AND {entity_col} = '{esc_entity}'
            """
            session.sql(del_fact_sql).collect()
            print(f"EntityRoll: deleted from {fact_tbl_name} WHERE COBID={cobid}, {entity_col}={entity_code}")

            # ── STEP 2: Delete target COB+entity from FACT_ADJUSTED_TABLE
            del_adj_sql = f"""
                DELETE FROM {fact_adjusted_tbl_name}
                WHERE COBID = {int(cobid)}
                  AND {entity_adj_col} = '{esc_entity}'
            """
            session.sql(del_adj_sql).collect()
            print(f"EntityRoll: deleted from {fact_adjusted_tbl_name} WHERE COBID={cobid}, {entity_adj_col}={entity_code}")

            # ── STEP 3: Copy source → target in FACT_TABLE ──────────────
            # All columns except COBID are copied as-is; COBID is replaced
            fact_copy_cols = [c for c in fact_cols if c != 'COBID']
            fact_select_cols = ', '.join(fact_copy_cols)
            insert_fact_sql = f"""
                INSERT INTO {fact_tbl_name} (COBID, {fact_select_cols})
                SELECT {int(cobid)}, {fact_select_cols}
                FROM {fact_tbl_name}
                WHERE COBID = {int(source_cobid)}
                  AND {entity_col} = '{esc_entity}'
            """
            session.sql(insert_fact_sql).collect()
            fact_count = session.sql(f"""
                SELECT COUNT(*) AS CNT FROM {fact_tbl_name}
                WHERE COBID = {int(cobid)} AND {entity_col} = '{esc_entity}'
            """).collect()[0]["CNT"]
            print(f"EntityRoll: copied {fact_count} rows into {fact_tbl_name}")

            # ── STEP 4: Copy source → target in FACT_ADJUSTED_TABLE ─────
            # Replace COBID and consolidate all ADJUSTMENT_IDs → single new ID
            adj_copy_cols = [c for c in fact_adj_cols if c not in ('COBID', 'ADJUSTMENT_ID')]
            adj_select_cols = ', '.join(adj_copy_cols)

            adj_id_expr = str(new_dim_adj_id) if new_dim_adj_id else 'NULL'
            insert_adj_sql = f"""
                INSERT INTO {fact_adjusted_tbl_name} (COBID, ADJUSTMENT_ID, {adj_select_cols})
                SELECT {int(cobid)}, {adj_id_expr}, {adj_select_cols}
                FROM {fact_adjusted_tbl_name}
                WHERE COBID = {int(source_cobid)}
                  AND {entity_adj_col} = '{esc_entity}'
            """
            session.sql(insert_adj_sql).collect()
            adj_count = session.sql(f"""
                SELECT COUNT(*) AS CNT FROM {fact_adjusted_tbl_name}
                WHERE COBID = {int(cobid)} AND {entity_adj_col} = '{esc_entity}'
            """).collect()[0]["CNT"]
            print(f"EntityRoll: copied {adj_count} rows into {fact_adjusted_tbl_name}")

            # ── Update RECORD_COUNT in DIMENSION.ADJUSTMENT ─────────────
            total_count = fact_count + adj_count
            if new_dim_adj_id:
                session.sql(f"""
                    UPDATE DIMENSION.ADJUSTMENT
                    SET RECORD_COUNT = {total_count},
                        RUN_STATUS   = 'Processed',
                        PROCESS_DATE = CURRENT_TIMESTAMP()
                    WHERE ADJUSTMENT_ID = {new_dim_adj_id}
                """).collect()

            # ── Update ADJ_HEADER ───────────────────────────────────────
            session.sql(f"""
                UPDATE ADJUSTMENT_APP.ADJ_HEADER
                SET RECORD_COUNT = {total_count},
                    ADJUSTMENT_VALUE_IN_USD = NULL
                WHERE ADJ_ID IN ({adj_ids_str})
            """).collect()

            update_header_status(session, df_adj_er, cobid, "Processed")
            log_status_history(session, adj_ids, "Running", "Processed")

            result["fact_rows_copied"]     = fact_count
            result["adj_rows_copied"]      = adj_count
            result["message"] = f"Entity Roll processed: {fact_count} fact rows + {adj_count} adjustment rows copied from COB {source_cobid} to {cobid}"

            # ── Close run log and trigger PowerBI refresh ──────────────
            try:
                session.sql(f"""
                    CALL BATCH.LOAD_RUN_LOG_END_WITH_DETAIL({run_log_id}, '{{"status":"Processed"}}')
                """).collect()
            except Exception as rl_err:
                print(f"Warning: Run log close failed: {rl_err}")
            trigger_powerbi_refresh(session, process_type, run_log_id)

        else:
            result["message"] = f"Invalid adjustment_action: {adjustment_action}"

    except Exception as e:
        error_msg = str(e).replace("'", "''")
        print(f"Error: {error_msg}")
        result["message"] = f"Error: {error_msg}"

        # Try to mark as Failed
        try:
            if 'adj_base_tbl_name' in dir():
                df_adj_err = session.table(adj_base_tbl_name).filter(
                    (col('COBID') == cobid) &
                    (upper(col('PROCESS_TYPE')) == process_type.upper()) &
                    (col('RUN_STATUS') == 'Running')
                )
                update_header_status(session, df_adj_err, cobid, "Failed", error_msg)
            if adj_ids:
                log_status_history(session, adj_ids, "Running", "Failed")
            if dim_adj_map:
                _fail_ids = ', '.join(str(v) for v in dim_adj_map.values())
                session.sql(f"""
                    UPDATE DIMENSION.ADJUSTMENT
                    SET RUN_STATUS = 'Failed'
                    WHERE ADJUSTMENT_ID IN ({_fail_ids})
                """).collect()
        except Exception as cleanup_err:
            print(f"Cleanup failed: {cleanup_err}")

        # Close run log with failure status (do NOT trigger PowerBI refresh)
        try:
            if 'run_log_id' in dir() and run_log_id:
                session.sql(f"""
                    CALL BATCH.LOAD_RUN_LOG_END_WITH_DETAIL(
                        {run_log_id}, '{{"status":"Failed","error":"{error_msg[:200]}"}}'
                    )
                """).collect()
        except Exception:
            pass

    return json.dumps(result)
$$;
