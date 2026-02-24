-- =============================================================================
-- 03_DYNAMIC_TABLES.SQL
-- Dynamic Tables replace manual UNION ALL + GROUP BY views and materialized tables
-- They auto-refresh incrementally when upstream data changes.
-- =============================================================================

USE DATABASE ADJUSTMENT_DB;
USE WAREHOUSE ADJUSTMENT_WH;

-- ═════════════════════════════════════════════════════════════════════════════
-- 1. DYNAMIC TABLE: FACT_ADJUSTED
--    The single source of truth for "adjusted fact" — replaces all manual
--    UNION ALL + GROUP BY views and their materialized counterparts.
--    Refreshes automatically with configurable lag.
-- ═════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE DYNAMIC TABLE MART.FACT_ADJUSTED
  TARGET_LAG = '1 minute'        -- Adjust: '1 minute' for near-real-time, 'DOWNSTREAM' for chaining
  WAREHOUSE  = ADJUSTMENT_TASK_WH
  COMMENT    = 'Auto-materialized adjusted fact: original fact + applied adjustment deltas'
AS
SELECT
    -- Dimension keys
    f.BUSINESS_DATE,
    f.ENTITY_KEY,
    f.PRODUCT_KEY,
    f.ACCOUNT_KEY,
    f.CURRENCY_KEY,
    f.COUNTERPARTY_KEY,
    f.COST_CENTER_KEY,

    -- Adjusted measures = SUM(original + deltas)
    SUM(f.AMOUNT)       AS AMOUNT,
    SUM(f.QUANTITY)      AS QUANTITY,
    SUM(f.NOTIONAL)      AS NOTIONAL,

    -- Metadata: was this grain adjusted?
    CASE WHEN SUM(ABS(a.AMOUNT_DELTA)) > 0
              OR SUM(ABS(a.QUANTITY_DELTA)) > 0
              OR SUM(ABS(a.NOTIONAL_DELTA)) > 0
         THEN TRUE ELSE FALSE
    END AS IS_ADJUSTED,

    -- Source system (for lineage)
    'ADJUSTED' AS SOURCE_LAYER

FROM (
    -- Original fact values
    SELECT
        BUSINESS_DATE, ENTITY_KEY, PRODUCT_KEY, ACCOUNT_KEY,
        CURRENCY_KEY, COUNTERPARTY_KEY, COST_CENTER_KEY,
        AMOUNT, QUANTITY, NOTIONAL
    FROM FACT.FACT_TABLE

    UNION ALL

    -- Applied adjustment deltas only
    SELECT
        li.BUSINESS_DATE, li.ENTITY_KEY, li.PRODUCT_KEY, li.ACCOUNT_KEY,
        li.CURRENCY_KEY, li.COUNTERPARTY_KEY, li.COST_CENTER_KEY,
        li.AMOUNT_DELTA   AS AMOUNT,
        li.QUANTITY_DELTA  AS QUANTITY,
        li.NOTIONAL_DELTA  AS NOTIONAL
    FROM CORE.ADJ_LINE_ITEM li
    INNER JOIN CORE.ADJ_HEADER h
        ON li.ADJ_ID = h.ADJ_ID
    WHERE h.ADJ_STATUS = 'APPLIED'
) f

LEFT JOIN (
    -- Aggregate deltas for the IS_ADJUSTED flag
    SELECT
        li.BUSINESS_DATE, li.ENTITY_KEY, li.PRODUCT_KEY, li.ACCOUNT_KEY,
        li.CURRENCY_KEY, li.COUNTERPARTY_KEY, li.COST_CENTER_KEY,
        SUM(li.AMOUNT_DELTA)   AS AMOUNT_DELTA,
        SUM(li.QUANTITY_DELTA)  AS QUANTITY_DELTA,
        SUM(li.NOTIONAL_DELTA)  AS NOTIONAL_DELTA
    FROM CORE.ADJ_LINE_ITEM li
    INNER JOIN CORE.ADJ_HEADER h
        ON li.ADJ_ID = h.ADJ_ID
    WHERE h.ADJ_STATUS = 'APPLIED'
    GROUP BY 1,2,3,4,5,6,7
) a
    ON  f.BUSINESS_DATE    = a.BUSINESS_DATE
    AND f.ENTITY_KEY       = a.ENTITY_KEY
    AND f.PRODUCT_KEY      = a.PRODUCT_KEY
    AND f.ACCOUNT_KEY      = a.ACCOUNT_KEY
    AND f.CURRENCY_KEY     = a.CURRENCY_KEY
    AND COALESCE(f.COUNTERPARTY_KEY, '<<NULL>>') = COALESCE(a.COUNTERPARTY_KEY, '<<NULL>>')
    AND COALESCE(f.COST_CENTER_KEY, '<<NULL>>') = COALESCE(a.COST_CENTER_KEY, '<<NULL>>')

GROUP BY
    f.BUSINESS_DATE,
    f.ENTITY_KEY,
    f.PRODUCT_KEY,
    f.ACCOUNT_KEY,
    f.CURRENCY_KEY,
    f.COUNTERPARTY_KEY,
    f.COST_CENTER_KEY,
    a.AMOUNT_DELTA,
    a.QUANTITY_DELTA,
    a.NOTIONAL_DELTA
;


-- ═════════════════════════════════════════════════════════════════════════════
-- 2. DYNAMIC TABLE: ADJUSTMENT_IMPACT_SUMMARY
--    Pre-aggregated adjustment impact for the dashboard.
--    Chains off FACT_ADJUSTED via DOWNSTREAM lag.
-- ═════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE DYNAMIC TABLE MART.ADJUSTMENT_IMPACT_SUMMARY
  TARGET_LAG = 'DOWNSTREAM'
  WAREHOUSE  = ADJUSTMENT_TASK_WH
  COMMENT    = 'Pre-aggregated adjustment impact metrics for dashboard'
AS
SELECT
    h.ADJ_ID,
    h.ADJ_TYPE,
    h.ADJ_STATUS,
    h.TARGET_DATE,
    h.BUSINESS_REASON,
    h.CREATED_BY,
    h.CREATED_AT,
    h.APPROVED_BY,
    h.APPLIED_AT,

    -- Impact metrics
    COUNT(li.LINE_ID)                       AS ROWS_AFFECTED,
    SUM(ABS(li.AMOUNT_DELTA))               AS TOTAL_AMOUNT_IMPACT,
    SUM(ABS(li.QUANTITY_DELTA))              AS TOTAL_QUANTITY_IMPACT,
    SUM(ABS(li.NOTIONAL_DELTA))             AS TOTAL_NOTIONAL_IMPACT,
    SUM(li.AMOUNT_DELTA)                    AS NET_AMOUNT_IMPACT,
    SUM(li.QUANTITY_DELTA)                  AS NET_QUANTITY_IMPACT,
    SUM(li.NOTIONAL_DELTA)                  AS NET_NOTIONAL_IMPACT,

    -- Original totals
    SUM(li.AMOUNT_ORIGINAL)                  AS ORIGINAL_AMOUNT_TOTAL,
    SUM(li.QUANTITY_ORIGINAL)                AS ORIGINAL_QUANTITY_TOTAL,
    SUM(li.NOTIONAL_ORIGINAL)                AS ORIGINAL_NOTIONAL_TOTAL,

    -- Percentage impact
    CASE WHEN SUM(li.AMOUNT_ORIGINAL) != 0
         THEN ROUND(SUM(li.AMOUNT_DELTA) / SUM(li.AMOUNT_ORIGINAL) * 100, 4)
         ELSE NULL
    END AS AMOUNT_IMPACT_PCT,

    h.SCALE_FACTOR,
    h.ROLL_SOURCE_DATE,
    h.FILTER_CRITERIA,
    h.TICKET_REFERENCE,
    h.AI_IMPACT_SUMMARY

FROM CORE.ADJ_HEADER h
LEFT JOIN CORE.ADJ_LINE_ITEM li
    ON h.ADJ_ID = li.ADJ_ID
GROUP BY
    h.ADJ_ID, h.ADJ_TYPE, h.ADJ_STATUS, h.TARGET_DATE,
    h.BUSINESS_REASON, h.CREATED_BY, h.CREATED_AT,
    h.APPROVED_BY, h.APPLIED_AT, h.SCALE_FACTOR,
    h.ROLL_SOURCE_DATE, h.FILTER_CRITERIA, h.TICKET_REFERENCE,
    h.AI_IMPACT_SUMMARY
;


-- ═════════════════════════════════════════════════════════════════════════════
-- 3. DYNAMIC TABLE: DAILY_ADJUSTMENT_ACTIVITY
--    Daily activity metrics for the audit/visibility dashboard.
-- ═════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE DYNAMIC TABLE MART.DAILY_ADJUSTMENT_ACTIVITY
  TARGET_LAG = '5 minutes'
  WAREHOUSE  = ADJUSTMENT_TASK_WH
  COMMENT    = 'Daily aggregated adjustment activity for audit dashboard'
AS
SELECT
    DATE_TRUNC('DAY', h.CREATED_AT)::DATE   AS ACTIVITY_DATE,
    h.ADJ_TYPE,
    h.ADJ_STATUS,
    h.CREATED_BY,
    COUNT(*)                                 AS ADJUSTMENT_COUNT,
    SUM(h.AFFECTED_ROWS)                     AS TOTAL_ROWS_AFFECTED,
    MIN(h.CREATED_AT)                        AS FIRST_CREATED,
    MAX(h.CREATED_AT)                        AS LAST_CREATED
FROM CORE.ADJ_HEADER h
GROUP BY 1, 2, 3, 4
;


-- ═════════════════════════════════════════════════════════════════════════════
-- 4. VERIFY DYNAMIC TABLES
-- ═════════════════════════════════════════════════════════════════════════════

SHOW DYNAMIC TABLES IN SCHEMA MART;
