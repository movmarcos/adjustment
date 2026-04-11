-- =============================================================================
-- 05_STORED_PROCEDURES.SQL
-- Core adjustment logic: preview, apply, reverse adjustments
-- Written in Snowpark Python for maximum flexibility.
-- =============================================================================

USE DATABASE ADJUSTMENT_DB;
USE SCHEMA CORE;
USE WAREHOUSE ADJUSTMENT_WH;

-- ═════════════════════════════════════════════════════════════════════════════
-- 1. SP_PREVIEW_ADJUSTMENT
--    Shows what rows would be affected and the computed deltas WITHOUT applying.
--    Called from Streamlit for the "Before/After" preview.
-- ═════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE PROCEDURE CORE.SP_PREVIEW_ADJUSTMENT(
    P_ADJ_TYPE      VARCHAR,       -- 'FLATTEN', 'SCALE', 'ROLL'
    P_FILTER_JSON   VARIANT,       -- {"entity_key": "X", "product_key": "Y", ...}
    P_TARGET_DATE   DATE,          -- Business date to adjust
    P_SCALE_FACTOR  FLOAT,         -- Scale factor (used by SCALE and ROLL)
    P_ROLL_SOURCE   DATE           -- Roll source date (ROLL only, NULL otherwise)
)
RETURNS TABLE()
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'preview_adjustment'
COMMENT = 'Previews adjustment impact without applying. Returns current values, deltas, and projected values.'
AS
$$
import json
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit, when, sum as sf_sum

def preview_adjustment(session: Session, p_adj_type: str, p_filter_json: dict,
                       p_target_date, p_scale_factor: float, p_roll_source):
    """
    Preview the adjustment: return current rows, calculated deltas, and projected values.
    """
    # Parse filter criteria
    filters = p_filter_json if isinstance(p_filter_json, dict) else json.loads(str(p_filter_json))

    # Build base query on fact table for target date
    fact_df = session.table("FACT.FACT_TABLE").filter(col("BUSINESS_DATE") == p_target_date)

    # Apply dynamic filters
    for col_name, value in filters.items():
        col_upper = col_name.upper()
        if isinstance(value, list):
            fact_df = fact_df.filter(col(col_upper).isin(value))
        elif value is not None and str(value).strip() != '':
            fact_df = fact_df.filter(col(col_upper) == value)

    if p_adj_type == 'FLATTEN':
        result_df = fact_df.select(
            col("FACT_ID"),
            col("BUSINESS_DATE"),
            col("ENTITY_KEY"), col("PRODUCT_KEY"), col("ACCOUNT_KEY"),
            col("CURRENCY_KEY"), col("COUNTERPARTY_KEY"), col("COST_CENTER_KEY"),
            col("AMOUNT").alias("AMOUNT_ORIGINAL"),
            col("QUANTITY").alias("QUANTITY_ORIGINAL"),
            col("NOTIONAL").alias("NOTIONAL_ORIGINAL"),
            (lit(0) - col("AMOUNT")).alias("AMOUNT_DELTA"),
            (lit(0) - col("QUANTITY")).alias("QUANTITY_DELTA"),
            (lit(0) - col("NOTIONAL")).alias("NOTIONAL_DELTA"),
            lit(0).alias("AMOUNT_PROJECTED"),
            lit(0).alias("QUANTITY_PROJECTED"),
            lit(0).alias("NOTIONAL_PROJECTED"),
            lit('FLATTEN').alias("ADJ_TYPE")
        )

    elif p_adj_type == 'SCALE':
        factor = float(p_scale_factor) if p_scale_factor else 1.0
        result_df = fact_df.select(
            col("FACT_ID"),
            col("BUSINESS_DATE"),
            col("ENTITY_KEY"), col("PRODUCT_KEY"), col("ACCOUNT_KEY"),
            col("CURRENCY_KEY"), col("COUNTERPARTY_KEY"), col("COST_CENTER_KEY"),
            col("AMOUNT").alias("AMOUNT_ORIGINAL"),
            col("QUANTITY").alias("QUANTITY_ORIGINAL"),
            col("NOTIONAL").alias("NOTIONAL_ORIGINAL"),
            (col("AMOUNT") * lit(factor - 1)).alias("AMOUNT_DELTA"),
            (col("QUANTITY") * lit(factor - 1)).alias("QUANTITY_DELTA"),
            (col("NOTIONAL") * lit(factor - 1)).alias("NOTIONAL_DELTA"),
            (col("AMOUNT") * lit(factor)).alias("AMOUNT_PROJECTED"),
            (col("QUANTITY") * lit(factor)).alias("QUANTITY_PROJECTED"),
            (col("NOTIONAL") * lit(factor)).alias("NOTIONAL_PROJECTED"),
            lit('SCALE').alias("ADJ_TYPE")
        )

    elif p_adj_type == 'ROLL':
        scale = float(p_scale_factor) if p_scale_factor else 1.0

        # Get source-date data
        source_df = session.table("FACT.FACT_TABLE").filter(
            col("BUSINESS_DATE") == p_roll_source
        )
        for col_name, value in filters.items():
            col_upper = col_name.upper()
            if isinstance(value, list):
                source_df = source_df.filter(col(col_upper).isin(value))
            elif value is not None and str(value).strip() != '':
                source_df = source_df.filter(col(col_upper) == value)

        # Join target with source on dimension keys
        join_keys = ["ENTITY_KEY", "PRODUCT_KEY", "ACCOUNT_KEY",
                     "CURRENCY_KEY", "COUNTERPARTY_KEY", "COST_CENTER_KEY"]

        target_alias = fact_df.alias("t")
        source_alias = source_df.alias("s")

        join_cond = None
        for k in join_keys:
            c = (col(f"t.{k}") == col(f"s.{k}"))
            join_cond = c if join_cond is None else (join_cond & c)

        joined = target_alias.join(source_alias, join_cond, "left")

        result_df = joined.select(
            col("t.FACT_ID").alias("FACT_ID"),
            col("t.BUSINESS_DATE").alias("BUSINESS_DATE"),
            col("t.ENTITY_KEY").alias("ENTITY_KEY"),
            col("t.PRODUCT_KEY").alias("PRODUCT_KEY"),
            col("t.ACCOUNT_KEY").alias("ACCOUNT_KEY"),
            col("t.CURRENCY_KEY").alias("CURRENCY_KEY"),
            col("t.COUNTERPARTY_KEY").alias("COUNTERPARTY_KEY"),
            col("t.COST_CENTER_KEY").alias("COST_CENTER_KEY"),
            col("t.AMOUNT").alias("AMOUNT_ORIGINAL"),
            col("t.QUANTITY").alias("QUANTITY_ORIGINAL"),
            col("t.NOTIONAL").alias("NOTIONAL_ORIGINAL"),
            (col("s.AMOUNT") * lit(scale) - col("t.AMOUNT")).alias("AMOUNT_DELTA"),
            (col("s.QUANTITY") * lit(scale) - col("t.QUANTITY")).alias("QUANTITY_DELTA"),
            (col("s.NOTIONAL") * lit(scale) - col("t.NOTIONAL")).alias("NOTIONAL_DELTA"),
            (col("s.AMOUNT") * lit(scale)).alias("AMOUNT_PROJECTED"),
            (col("s.QUANTITY") * lit(scale)).alias("QUANTITY_PROJECTED"),
            (col("s.NOTIONAL") * lit(scale)).alias("NOTIONAL_PROJECTED"),
            lit('ROLL').alias("ADJ_TYPE")
        )

    else:
        raise ValueError(f"Invalid adjustment type: {p_adj_type}")

    return result_df
$$;


-- ═════════════════════════════════════════════════════════════════════════════
-- 2. SP_CREATE_ADJUSTMENT
--    Creates adjustment header + computes and inserts line items.
--    Status is set to DRAFT (not yet applied).
-- ═════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE PROCEDURE CORE.SP_CREATE_ADJUSTMENT(
    P_ADJ_TYPE        VARCHAR,
    P_FILTER_JSON     VARIANT,
    P_TARGET_DATE     DATE,
    P_SCALE_FACTOR    FLOAT,
    P_ROLL_SOURCE     DATE,
    P_BUSINESS_REASON VARCHAR,
    P_TICKET_REF      VARCHAR
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'create_adjustment'
COMMENT = 'Creates a new adjustment in DRAFT status with computed delta line items.'
AS
$$
import json
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit

def create_adjustment(session: Session, p_adj_type: str, p_filter_json: dict,
                      p_target_date, p_scale_factor: float, p_roll_source,
                      p_business_reason: str, p_ticket_ref: str):

    filters = p_filter_json if isinstance(p_filter_json, dict) else json.loads(str(p_filter_json))
    filter_json_str = json.dumps(filters)

    # Insert header
    roll_source_val = f"'{p_roll_source}'" if p_roll_source else "NULL"
    ticket_val = f"'{p_ticket_ref}'" if p_ticket_ref else "NULL"

    session.sql(f"""
        INSERT INTO CORE.ADJ_HEADER
            (ADJ_TYPE, ADJ_STATUS, SCALE_FACTOR, ROLL_SOURCE_DATE, TARGET_DATE,
             FILTER_CRITERIA, BUSINESS_REASON, TICKET_REFERENCE, CREATED_BY, CREATED_AT)
        VALUES
            ('{p_adj_type}', 'DRAFT', {p_scale_factor or 1}, {roll_source_val},
             '{p_target_date}', PARSE_JSON('{filter_json_str}'),
             '{p_business_reason.replace("'", "''")}', {ticket_val},
             CURRENT_USER(), CURRENT_TIMESTAMP())
    """).collect()

    # Get the new ADJ_ID
    adj_id_row = session.sql("SELECT MAX(ADJ_ID) AS ADJ_ID FROM CORE.ADJ_HEADER").collect()
    adj_id = adj_id_row[0]['ADJ_ID']

    # Build fact query for target date
    fact_df = session.table("FACT.FACT_TABLE").filter(col("BUSINESS_DATE") == p_target_date)
    for col_name, value in filters.items():
        col_upper = col_name.upper()
        if isinstance(value, list):
            fact_df = fact_df.filter(col(col_upper).isin(value))
        elif value is not None and str(value).strip() != '':
            fact_df = fact_df.filter(col(col_upper) == value)

    row_count = fact_df.count()

    # Compute deltas and insert line items
    if p_adj_type == 'FLATTEN':
        insert_df = fact_df.select(
            lit(adj_id).alias("ADJ_ID"),
            col("BUSINESS_DATE"), col("ENTITY_KEY"), col("PRODUCT_KEY"),
            col("ACCOUNT_KEY"), col("CURRENCY_KEY"),
            col("COUNTERPARTY_KEY"), col("COST_CENTER_KEY"),
            (lit(0) - col("AMOUNT")).alias("AMOUNT_DELTA"),
            (lit(0) - col("QUANTITY")).alias("QUANTITY_DELTA"),
            (lit(0) - col("NOTIONAL")).alias("NOTIONAL_DELTA"),
            col("AMOUNT").alias("AMOUNT_ORIGINAL"),
            col("QUANTITY").alias("QUANTITY_ORIGINAL"),
            col("NOTIONAL").alias("NOTIONAL_ORIGINAL"),
            lit(None).cast("FLOAT").alias("AMOUNT_SOURCE"),
            lit(None).cast("FLOAT").alias("QUANTITY_SOURCE"),
            lit(None).cast("FLOAT").alias("NOTIONAL_SOURCE"),
            col("FACT_ID")
        )

    elif p_adj_type == 'SCALE':
        factor = float(p_scale_factor) if p_scale_factor else 1.0
        insert_df = fact_df.select(
            lit(adj_id).alias("ADJ_ID"),
            col("BUSINESS_DATE"), col("ENTITY_KEY"), col("PRODUCT_KEY"),
            col("ACCOUNT_KEY"), col("CURRENCY_KEY"),
            col("COUNTERPARTY_KEY"), col("COST_CENTER_KEY"),
            (col("AMOUNT") * lit(factor - 1)).alias("AMOUNT_DELTA"),
            (col("QUANTITY") * lit(factor - 1)).alias("QUANTITY_DELTA"),
            (col("NOTIONAL") * lit(factor - 1)).alias("NOTIONAL_DELTA"),
            col("AMOUNT").alias("AMOUNT_ORIGINAL"),
            col("QUANTITY").alias("QUANTITY_ORIGINAL"),
            col("NOTIONAL").alias("NOTIONAL_ORIGINAL"),
            lit(None).cast("FLOAT").alias("AMOUNT_SOURCE"),
            lit(None).cast("FLOAT").alias("QUANTITY_SOURCE"),
            lit(None).cast("FLOAT").alias("NOTIONAL_SOURCE"),
            col("FACT_ID")
        )

    elif p_adj_type == 'ROLL':
        scale = float(p_scale_factor) if p_scale_factor else 1.0
        source_df = session.table("FACT.FACT_TABLE").filter(
            col("BUSINESS_DATE") == p_roll_source
        )
        for col_name, value in filters.items():
            col_upper = col_name.upper()
            if isinstance(value, list):
                source_df = source_df.filter(col(col_upper).isin(value))
            elif value is not None and str(value).strip() != '':
                source_df = source_df.filter(col(col_upper) == value)

        join_keys = ["ENTITY_KEY", "PRODUCT_KEY", "ACCOUNT_KEY",
                     "CURRENCY_KEY", "COUNTERPARTY_KEY", "COST_CENTER_KEY"]

        target_alias = fact_df.alias("t")
        source_alias = source_df.alias("s")

        join_cond = None
        for k in join_keys:
            c = (col(f"t.{k}") == col(f"s.{k}"))
            join_cond = c if join_cond is None else (join_cond & c)

        joined = target_alias.join(source_alias, join_cond, "left")

        insert_df = joined.select(
            lit(adj_id).alias("ADJ_ID"),
            col("t.BUSINESS_DATE").alias("BUSINESS_DATE"),
            col("t.ENTITY_KEY").alias("ENTITY_KEY"),
            col("t.PRODUCT_KEY").alias("PRODUCT_KEY"),
            col("t.ACCOUNT_KEY").alias("ACCOUNT_KEY"),
            col("t.CURRENCY_KEY").alias("CURRENCY_KEY"),
            col("t.COUNTERPARTY_KEY").alias("COUNTERPARTY_KEY"),
            col("t.COST_CENTER_KEY").alias("COST_CENTER_KEY"),
            (col("s.AMOUNT") * lit(scale) - col("t.AMOUNT")).alias("AMOUNT_DELTA"),
            (col("s.QUANTITY") * lit(scale) - col("t.QUANTITY")).alias("QUANTITY_DELTA"),
            (col("s.NOTIONAL") * lit(scale) - col("t.NOTIONAL")).alias("NOTIONAL_DELTA"),
            col("t.AMOUNT").alias("AMOUNT_ORIGINAL"),
            col("t.QUANTITY").alias("QUANTITY_ORIGINAL"),
            col("t.NOTIONAL").alias("NOTIONAL_ORIGINAL"),
            col("s.AMOUNT").alias("AMOUNT_SOURCE"),
            col("s.QUANTITY").alias("QUANTITY_SOURCE"),
            col("s.NOTIONAL").alias("NOTIONAL_SOURCE"),
            col("t.FACT_ID").alias("FACT_ID")
        )

    # Write line items
    insert_df.write.mode("append").save_as_table(
        "CORE.ADJ_LINE_ITEM",
        column_order="name"
    )

    # Update affected rows count on header
    session.sql(f"""
        UPDATE CORE.ADJ_HEADER
        SET AFFECTED_ROWS = {row_count}
        WHERE ADJ_ID = {adj_id}
    """).collect()

    return {"adj_id": adj_id, "rows_affected": row_count, "status": "DRAFT"}
$$;


-- ═════════════════════════════════════════════════════════════════════════════
-- 3. SP_UPDATE_ADJUSTMENT_STATUS
--    Manages the state machine transitions with validation.
-- ═════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE PROCEDURE CORE.SP_UPDATE_ADJUSTMENT_STATUS(
    P_ADJ_ID      NUMBER,
    P_NEW_STATUS  VARCHAR,
    P_COMMENT     VARCHAR
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'update_status'
COMMENT = 'Validates and applies status transitions for an adjustment.'
AS
$$
import json
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

# Valid state transitions
VALID_TRANSITIONS = {
    'DRAFT':            ['PENDING_APPROVAL'],
    'PENDING_APPROVAL': ['APPROVED', 'REJECTED'],
    'APPROVED':         ['APPLIED'],
    'APPLIED':          ['REVERSED'],
    'REJECTED':         [],
    'REVERSED':         []
}

def update_status(session: Session, p_adj_id: int, p_new_status: str, p_comment: str):
    # Get current status
    row = session.sql(f"""
        SELECT ADJ_STATUS, CREATED_BY FROM CORE.ADJ_HEADER WHERE ADJ_ID = {p_adj_id}
    """).collect()

    if not row:
        return {"success": False, "error": f"Adjustment {p_adj_id} not found"}

    current_status = row[0]['ADJ_STATUS']
    created_by = row[0]['CREATED_BY']
    current_user_row = session.sql("SELECT CURRENT_USER() AS CU").collect()
    current_user = current_user_row[0]['CU']

    # Validate transition
    if p_new_status not in VALID_TRANSITIONS.get(current_status, []):
        return {
            "success": False,
            "error": f"Invalid transition: {current_status} → {p_new_status}. "
                     f"Allowed: {VALID_TRANSITIONS.get(current_status, [])}"
        }

    # Self-approval guard: approver != creator
    if p_new_status == 'APPROVED' and current_user == created_by:
        return {
            "success": False,
            "error": "Self-approval not allowed. A different user must approve."
        }

    # Update header
    extra_cols = ""
    if p_new_status == 'APPROVED':
        extra_cols = f", APPROVED_BY = CURRENT_USER(), APPROVED_AT = CURRENT_TIMESTAMP()"
    elif p_new_status == 'APPLIED':
        extra_cols = f", APPLIED_BY = CURRENT_USER(), APPLIED_AT = CURRENT_TIMESTAMP()"
    elif p_new_status == 'REVERSED':
        extra_cols = f", REVERSED_BY = CURRENT_USER(), REVERSED_AT = CURRENT_TIMESTAMP()"

    session.sql(f"""
        UPDATE CORE.ADJ_HEADER
        SET ADJ_STATUS = '{p_new_status}' {extra_cols}
        WHERE ADJ_ID = {p_adj_id}
    """).collect()

    # Log status history
    comment_safe = p_comment.replace("'", "''") if p_comment else ''
    session.sql(f"""
        INSERT INTO CORE.ADJ_STATUS_HISTORY
            (ADJ_ID, OLD_STATUS, NEW_STATUS, CHANGED_BY, CHANGED_AT, COMMENT)
        VALUES
            ({p_adj_id}, '{current_status}', '{p_new_status}',
             CURRENT_USER(), CURRENT_TIMESTAMP(), '{comment_safe}')
    """).collect()

    return {"success": True, "adj_id": p_adj_id,
            "transition": f"{current_status} → {p_new_status}"}
$$;


-- ═════════════════════════════════════════════════════════════════════════════
-- 4. SP_REVERSE_ADJUSTMENT
--    Reverses an applied adjustment by inserting counter-deltas.
-- ═════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE PROCEDURE CORE.SP_REVERSE_ADJUSTMENT(
    P_ADJ_ID  NUMBER,
    P_REASON  VARCHAR
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'reverse_adjustment'
COMMENT = 'Reverses a previously applied adjustment by inserting negated deltas as a new adjustment.'
AS
$$
import json
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit

def reverse_adjustment(session: Session, p_adj_id: int, p_reason: str):
    # Validate status is APPLIED
    row = session.sql(f"""
        SELECT ADJ_STATUS, ADJ_TYPE, TARGET_DATE, FILTER_CRITERIA, SCALE_FACTOR
        FROM CORE.ADJ_HEADER WHERE ADJ_ID = {p_adj_id}
    """).collect()

    if not row:
        return {"success": False, "error": f"Adjustment {p_adj_id} not found"}

    if row[0]['ADJ_STATUS'] != 'APPLIED':
        return {"success": False, "error": "Only APPLIED adjustments can be reversed"}

    # Create a new reversal header
    reason_safe = p_reason.replace("'", "''")
    session.sql(f"""
        INSERT INTO CORE.ADJ_HEADER
            (ADJ_TYPE, ADJ_STATUS, SCALE_FACTOR, ROLL_SOURCE_DATE, TARGET_DATE,
             FILTER_CRITERIA, BUSINESS_REASON, TICKET_REFERENCE, CREATED_BY, CREATED_AT)
        SELECT
            ADJ_TYPE, 'APPLIED', SCALE_FACTOR, ROLL_SOURCE_DATE, TARGET_DATE,
            FILTER_CRITERIA, 'REVERSAL of ADJ #{p_adj_id}: {reason_safe}',
            TICKET_REFERENCE, CURRENT_USER(), CURRENT_TIMESTAMP()
        FROM CORE.ADJ_HEADER WHERE ADJ_ID = {p_adj_id}
    """).collect()

    new_id_row = session.sql("SELECT MAX(ADJ_ID) AS ADJ_ID FROM CORE.ADJ_HEADER").collect()
    new_adj_id = new_id_row[0]['ADJ_ID']

    # Insert negated line items
    session.sql(f"""
        INSERT INTO CORE.ADJ_LINE_ITEM
            (ADJ_ID, BUSINESS_DATE, ENTITY_KEY, PRODUCT_KEY, ACCOUNT_KEY,
             CURRENCY_KEY, COUNTERPARTY_KEY, COST_CENTER_KEY,
             AMOUNT_DELTA, QUANTITY_DELTA, NOTIONAL_DELTA,
             AMOUNT_ORIGINAL, QUANTITY_ORIGINAL, NOTIONAL_ORIGINAL,
             FACT_ID, CREATED_AT)
        SELECT
            {new_adj_id}, BUSINESS_DATE, ENTITY_KEY, PRODUCT_KEY, ACCOUNT_KEY,
            CURRENCY_KEY, COUNTERPARTY_KEY, COST_CENTER_KEY,
            -1 * AMOUNT_DELTA, -1 * QUANTITY_DELTA, -1 * NOTIONAL_DELTA,
            AMOUNT_ORIGINAL, QUANTITY_ORIGINAL, NOTIONAL_ORIGINAL,
            FACT_ID, CURRENT_TIMESTAMP()
        FROM CORE.ADJ_LINE_ITEM
        WHERE ADJ_ID = {p_adj_id}
    """).collect()

    # Update row count
    line_count = session.sql(f"""
        SELECT COUNT(*) AS CNT FROM CORE.ADJ_LINE_ITEM WHERE ADJ_ID = {new_adj_id}
    """).collect()[0]['CNT']

    session.sql(f"""
        UPDATE CORE.ADJ_HEADER SET AFFECTED_ROWS = {line_count},
               APPLIED_BY = CURRENT_USER(), APPLIED_AT = CURRENT_TIMESTAMP()
        WHERE ADJ_ID = {new_adj_id}
    """).collect()

    # Mark original as REVERSED
    session.sql(f"""
        UPDATE CORE.ADJ_HEADER
        SET ADJ_STATUS = 'REVERSED', REVERSED_BY = CURRENT_USER(), REVERSED_AT = CURRENT_TIMESTAMP()
        WHERE ADJ_ID = {p_adj_id}
    """).collect()

    # Log history
    session.sql(f"""
        INSERT INTO CORE.ADJ_STATUS_HISTORY
            (ADJ_ID, OLD_STATUS, NEW_STATUS, CHANGED_BY, CHANGED_AT, COMMENT)
        VALUES
            ({p_adj_id}, 'APPLIED', 'REVERSED', CURRENT_USER(), CURRENT_TIMESTAMP(),
             'Reversed by ADJ #{new_adj_id}: {reason_safe}')
    """).collect()

    return {"success": True, "original_adj_id": p_adj_id,
            "reversal_adj_id": new_adj_id, "rows_reversed": line_count}
$$;


-- ═════════════════════════════════════════════════════════════════════════════
-- 5. SP_GENERATE_AI_SUMMARY
--    Uses Cortex COMPLETE to generate a human-readable impact summary.
-- ═════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE PROCEDURE CORE.SP_GENERATE_AI_SUMMARY()
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Generates AI summaries for adjustments missing one using Cortex COMPLETE.'
AS
$$
BEGIN
    -- Update adjustments that don't have an AI summary yet
    UPDATE CORE.ADJ_HEADER h
    SET AI_IMPACT_SUMMARY = (
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            'mistral-large2',
            'You are a financial data analyst. Summarize this adjustment concisely in 2-3 sentences. '
            || 'Include the type, scope, and business impact. '
            || 'Adjustment Type: ' || h.ADJ_TYPE
            || ', Target Date: ' || h.TARGET_DATE::VARCHAR
            || ', Scale Factor: ' || COALESCE(h.SCALE_FACTOR::VARCHAR, 'N/A')
            || ', Business Reason: ' || h.BUSINESS_REASON
            || ', Rows Affected: ' || h.AFFECTED_ROWS::VARCHAR
            || ', Filter: ' || h.FILTER_CRITERIA::VARCHAR
            || ', Total Amount Delta: ' || COALESCE(
                (SELECT SUM(AMOUNT_DELTA)::VARCHAR FROM CORE.ADJ_LINE_ITEM WHERE ADJ_ID = h.ADJ_ID), '0')
        )
    )
    WHERE h.ADJ_STATUS IN ('APPLIED', 'APPROVED')
      AND h.AI_IMPACT_SUMMARY IS NULL
      AND h.AFFECTED_ROWS > 0;

    RETURN 'AI summaries generated successfully';
END;
$$;
