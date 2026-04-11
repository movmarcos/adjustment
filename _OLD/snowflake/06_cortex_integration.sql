-- =============================================================================
-- 06_CORTEX_INTEGRATION.SQL
-- Cortex AI features: LLM functions, anomaly detection, semantic model
-- =============================================================================

USE DATABASE ADJUSTMENT_DB;
USE SCHEMA AI;
USE WAREHOUSE ADJUSTMENT_WH;

-- ═════════════════════════════════════════════════════════════════════════════
-- 1. CORTEX LLM HELPER UDFs
-- ═════════════════════════════════════════════════════════════════════════════

-- Natural language query translator: converts user question → SQL
CREATE OR REPLACE FUNCTION AI.NL_TO_SQL(user_question VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Uses Cortex COMPLETE to translate natural language to SQL against adjustment tables.'
AS
$$
    SELECT SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large2',
        'You are a SQL expert for Snowflake. Convert the following natural language question ' ||
        'into a valid Snowflake SQL query. ' ||
        'Available tables: ' ||
        'CORE.ADJ_HEADER (ADJ_ID, ADJ_TYPE [FLATTEN/SCALE/ROLL], ADJ_STATUS [DRAFT/PENDING_APPROVAL/APPROVED/REJECTED/APPLIED/REVERSED], ' ||
        'SCALE_FACTOR, ROLL_SOURCE_DATE, TARGET_DATE, FILTER_CRITERIA VARIANT, AFFECTED_ROWS, ' ||
        'BUSINESS_REASON, TICKET_REFERENCE, CREATED_BY, CREATED_AT, APPROVED_BY, APPLIED_AT, REVERSED_BY, REVERSED_AT), ' ||
        'CORE.ADJ_LINE_ITEM (LINE_ID, ADJ_ID, BUSINESS_DATE, ENTITY_KEY, PRODUCT_KEY, ACCOUNT_KEY, ' ||
        'CURRENCY_KEY, COUNTERPARTY_KEY, COST_CENTER_KEY, AMOUNT_DELTA, QUANTITY_DELTA, NOTIONAL_DELTA, ' ||
        'AMOUNT_ORIGINAL, QUANTITY_ORIGINAL, NOTIONAL_ORIGINAL), ' ||
        'FACT.FACT_TABLE (same dimension keys + AMOUNT, QUANTITY, NOTIONAL), ' ||
        'MART.FACT_ADJUSTED (same as fact + IS_ADJUSTED boolean). ' ||
        'Return ONLY the SQL query, no explanation. ' ||
        'Question: ' || user_question
    )
$$;


-- Adjustment impact explainer: generates business-friendly explanation
CREATE OR REPLACE FUNCTION AI.EXPLAIN_ADJUSTMENT(adj_details VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Uses Cortex COMPLETE to generate a business-friendly explanation of an adjustment.'
AS
$$
    SELECT SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large2',
        'You are a financial reporting analyst. Explain the following adjustment in clear, ' ||
        'non-technical business language suitable for a management report. ' ||
        'Include: what changed, why, how many rows were affected, and the financial impact. ' ||
        'Keep it under 4 sentences. ' ||
        'Adjustment details: ' || adj_details
    )
$$;


-- Sentiment/risk classifier for adjustment reasons
CREATE OR REPLACE FUNCTION AI.CLASSIFY_ADJUSTMENT_RISK(business_reason VARCHAR, amount_impact FLOAT)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Uses Cortex to classify the risk level of an adjustment based on reason and impact.'
AS
$$
    SELECT SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large2',
        'Classify the risk level of this financial adjustment as LOW, MEDIUM, or HIGH. ' ||
        'Consider the business justification and dollar impact. ' ||
        'Return ONLY one word: LOW, MEDIUM, or HIGH. ' ||
        'Business reason: ' || business_reason ||
        '. Dollar impact: ' || amount_impact::VARCHAR
    )
$$;


-- ═════════════════════════════════════════════════════════════════════════════
-- 2. CORTEX ANOMALY DETECTION
--    Detects unusual adjustment patterns (unusually large adjustments,
--    unusual frequency, etc.)
-- ═════════════════════════════════════════════════════════════════════════════

-- Create a view for anomaly detection input (daily adjustment metrics)
CREATE OR REPLACE VIEW AI.V_DAILY_ADJ_METRICS AS
SELECT
    DATE_TRUNC('DAY', h.CREATED_AT)::TIMESTAMP_NTZ AS TS,
    COUNT(DISTINCT h.ADJ_ID)                        AS ADJ_COUNT,
    SUM(ABS(li.AMOUNT_DELTA))                       AS TOTAL_ABS_AMOUNT_DELTA,
    AVG(ABS(li.AMOUNT_DELTA))                       AS AVG_ABS_AMOUNT_DELTA,
    MAX(ABS(li.AMOUNT_DELTA))                       AS MAX_ABS_AMOUNT_DELTA,
    SUM(h.AFFECTED_ROWS)                            AS TOTAL_ROWS_AFFECTED
FROM CORE.ADJ_HEADER h
LEFT JOIN CORE.ADJ_LINE_ITEM li ON h.ADJ_ID = li.ADJ_ID
WHERE h.ADJ_STATUS IN ('APPLIED', 'APPROVED', 'REVERSED')
GROUP BY 1;


-- Stored procedure to run anomaly detection and flag results
CREATE OR REPLACE PROCEDURE AI.SP_DETECT_ADJUSTMENT_ANOMALIES()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'snowflake-ml-python')
HANDLER = 'detect_anomalies'
COMMENT = 'Runs Cortex anomaly detection on adjustment patterns and flags outliers.'
AS
$$
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

def detect_anomalies(session: Session):
    try:
        # Use Cortex COMPLETE for anomaly narrative (simpler approach that works today)
        # Get recent adjustment stats
        stats = session.sql("""
            SELECT
                COUNT(*) AS total_adj,
                SUM(AFFECTED_ROWS) AS total_rows,
                SUM(ABS(li.AMOUNT_DELTA)) AS total_impact
            FROM CORE.ADJ_HEADER h
            LEFT JOIN CORE.ADJ_LINE_ITEM li ON h.ADJ_ID = li.ADJ_ID
            WHERE h.CREATED_AT >= DATEADD('DAY', -7, CURRENT_TIMESTAMP())
              AND h.ADJ_STATUS IN ('APPLIED', 'APPROVED')
        """).collect()

        if not stats or stats[0]['TOTAL_ADJ'] == 0:
            return "No recent adjustments to analyze."

        # Get the top adjustments by impact
        top_adj = session.sql("""
            SELECT
                h.ADJ_ID, h.ADJ_TYPE, h.TARGET_DATE, h.CREATED_BY,
                h.AFFECTED_ROWS, h.BUSINESS_REASON,
                SUM(ABS(li.AMOUNT_DELTA)) AS ABS_IMPACT
            FROM CORE.ADJ_HEADER h
            LEFT JOIN CORE.ADJ_LINE_ITEM li ON h.ADJ_ID = li.ADJ_ID
            WHERE h.CREATED_AT >= DATEADD('DAY', -7, CURRENT_TIMESTAMP())
              AND h.ADJ_STATUS IN ('APPLIED', 'APPROVED')
            GROUP BY 1,2,3,4,5,6
            ORDER BY ABS_IMPACT DESC
            LIMIT 10
        """).collect()

        details = "; ".join([
            f"ADJ#{r['ADJ_ID']} type={r['ADJ_TYPE']} impact={r['ABS_IMPACT']} rows={r['AFFECTED_ROWS']} by={r['CREATED_BY']}"
            for r in top_adj
        ])

        # Ask Cortex to identify anomalies
        result = session.sql(f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                'mistral-large2',
                'Analyze these recent financial adjustments for anomalies. '
                || 'Flag anything unusual: very large impacts, high frequency by a single user, '
                || 'or unusual patterns. Return a JSON array of flagged items with adj_id and reason. '
                || 'If nothing is anomalous, return an empty array []. '
                || 'Adjustments: {details}'
            ) AS ANALYSIS
        """).collect()

        return result[0]['ANALYSIS'] if result else "Analysis complete, no anomalies detected."

    except Exception as e:
        return f"Anomaly detection error: {str(e)}"
$$;


-- ═════════════════════════════════════════════════════════════════════════════
-- 3. CORTEX SEARCH SERVICE (for semantic search over audit trail)
-- ═════════════════════════════════════════════════════════════════════════════

-- Flattened view of adjustments with text description for search indexing
CREATE OR REPLACE VIEW AI.V_ADJUSTMENT_SEARCH_CORPUS AS
SELECT
    h.ADJ_ID,
    h.ADJ_TYPE || ' adjustment on ' || h.TARGET_DATE::VARCHAR
        || ' by ' || h.CREATED_BY
        || '. Status: ' || h.ADJ_STATUS
        || '. Reason: ' || h.BUSINESS_REASON
        || '. Rows affected: ' || h.AFFECTED_ROWS::VARCHAR
        || '. Filter: ' || h.FILTER_CRITERIA::VARCHAR
        || COALESCE('. AI Summary: ' || h.AI_IMPACT_SUMMARY, '')
    AS SEARCH_TEXT,
    h.CREATED_AT,
    h.ADJ_TYPE,
    h.ADJ_STATUS,
    h.CREATED_BY,
    h.TARGET_DATE
FROM CORE.ADJ_HEADER h;

-- Create Cortex Search Service for semantic search over adjustments
-- NOTE: Cortex Search Service requires Enterprise Edition or higher.
-- Uncomment when available in your account:
--
-- CREATE OR REPLACE CORTEX SEARCH SERVICE AI.ADJUSTMENT_SEARCH_SERVICE
--   ON SEARCH_TEXT
--   ATTRIBUTES ADJ_TYPE, ADJ_STATUS, CREATED_BY
--   WAREHOUSE = ADJUSTMENT_TASK_WH
--   TARGET_LAG = '1 hour'
--   COMMENT = 'Semantic search over adjustment audit trail'
-- AS (
--   SELECT * FROM AI.V_ADJUSTMENT_SEARCH_CORPUS
-- );


-- ═════════════════════════════════════════════════════════════════════════════
-- 4. CORTEX ANALYST SEMANTIC MODEL (YAML)
--    Enables natural language BI queries over the adjustment data.
--    Upload this YAML to the AI_STAGE.
-- ═════════════════════════════════════════════════════════════════════════════

-- The semantic model YAML will be created as a file and uploaded to the stage.
-- See: streamlit/utils/cortex_semantic_model.yaml

-- To use Cortex Analyst, after uploading the YAML:
-- SELECT SNOWFLAKE.CORTEX.ANALYST(
--     '@ADJUSTMENT_DB.APP.AI_STAGE/adjustment_semantic_model.yaml',
--     'How many adjustments were applied last week?'
-- );
