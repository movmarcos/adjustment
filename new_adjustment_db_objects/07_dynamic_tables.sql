-- =============================================================================
-- 07_DYNAMIC_TABLES.SQL
-- Auto-refreshing dynamic tables for dashboard monitoring and overlap alerts.
--
-- Dynamic tables are the right choice here because:
--   • They auto-refresh on a schedule (no manual refresh needed)
--   • They materialise aggregations from ADJ_HEADER (fast reads for dashboard)
--   • Overlap detection is a complex join — better pre-computed than ad-hoc
-- =============================================================================

USE DATABASE DVLP_RAPTOR_NEWADJ;
USE SCHEMA ADJUSTMENT_APP;

-- ═══════════════════════════════════════════════════════════════════════════
-- 1. DT_DASHBOARD — Adjustment status summary for the dashboard
--
-- Aggregates ADJ_HEADER by COBID, PROCESS_TYPE, ADJUSTMENT_TYPE, RUN_STATUS.
-- Joined with dimension tables to resolve entity/book names.
-- Refreshes every 1 minute (near real-time for ad-hoc visibility).
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE DYNAMIC TABLE ADJUSTMENT_APP.DT_DASHBOARD
    TARGET_LAG = '1 MINUTE'
    WAREHOUSE  = DVLP_RAPTOR_WH_XS
    COMMENT    = 'Pre-aggregated dashboard view of all adjustments. Auto-refreshes every minute.'
AS
SELECT
    h.COBID,
    h.PROCESS_TYPE,
    h.ADJUSTMENT_TYPE,
    h.ADJUSTMENT_ACTION,
    h.RUN_STATUS,
    h.ADJUSTMENT_OCCURRENCE,
    h.ENTITY_CODE,
    h.DEPARTMENT_CODE,
    h.BOOK_CODE,
    h.USERNAME,
    COUNT(*)                                     AS ADJUSTMENT_COUNT,
    SUM(CASE WHEN h.IS_DELETED THEN 0 ELSE 1 END)   AS ACTIVE_COUNT,
    SUM(h.ADJUSTMENT_VALUE_IN_USD)               AS TOTAL_VALUE_USD,
    MIN(h.CREATED_DATE)                          AS EARLIEST_CREATED,
    MAX(h.CREATED_DATE)                          AS LATEST_CREATED,
    MAX(h.PROCESS_DATE)                          AS LAST_PROCESSED,
    SUM(h.RECORD_COUNT)                          AS TOTAL_RECORDS
FROM ADJUSTMENT_APP.ADJ_HEADER h
WHERE h.IS_DELETED = FALSE
GROUP BY
    h.COBID,
    h.PROCESS_TYPE,
    h.ADJUSTMENT_TYPE,
    h.ADJUSTMENT_ACTION,
    h.RUN_STATUS,
    h.ADJUSTMENT_OCCURRENCE,
    h.ENTITY_CODE,
    h.DEPARTMENT_CODE,
    h.BOOK_CODE,
    h.USERNAME;


-- ═══════════════════════════════════════════════════════════════════════════
-- 2. DT_OVERLAP_ALERTS — Detects overlapping adjustments
--
-- Two adjustments "overlap" when they target the same scope + COB + entity +
-- book (or broader dimension combination). The later one supersedes via
-- DENSE_RANK in the processing engine, but users should be WARNED before
-- submitting.
--
-- This dynamic table pre-computes overlaps so the Streamlit UI can query it
-- quickly when the user is filling the form.
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE DYNAMIC TABLE ADJUSTMENT_APP.DT_OVERLAP_ALERTS
    TARGET_LAG = '1 MINUTE'
    WAREHOUSE  = DVLP_RAPTOR_WH_XS
    COMMENT    = 'Detects overlapping adjustments. Streamlit queries this to warn users before submission.'
AS
WITH active_adjustments AS (
    SELECT
        ADJ_ID,
        COBID,
        PROCESS_TYPE,
        ADJUSTMENT_TYPE,
        ADJUSTMENT_ACTION,
        -- Build a normalised "filter fingerprint" for overlap detection
        -- NULL = no filter on this dimension = matches everything
        COALESCE(ENTITY_CODE,       '*') AS ENTITY_CODE,
        COALESCE(SOURCE_SYSTEM_CODE, '*') AS SOURCE_SYSTEM_CODE,
        COALESCE(DEPARTMENT_CODE,   '*') AS DEPARTMENT_CODE,
        COALESCE(BOOK_CODE,         '*') AS BOOK_CODE,
        COALESCE(CURRENCY_CODE,     '*') AS CURRENCY_CODE,
        COALESCE(TRADE_TYPOLOGY,    '*') AS TRADE_TYPOLOGY,
        COALESCE(STRATEGY,          '*') AS STRATEGY,
        SCALE_FACTOR,
        USERNAME,
        CREATED_DATE,
        RUN_STATUS
    FROM ADJUSTMENT_APP.ADJ_HEADER
    WHERE IS_DELETED = FALSE
      AND RUN_STATUS IN ('Pending', 'Pending Approval', 'Approved', 'Processed')
),
overlaps AS (
    SELECT
        a.ADJ_ID          AS ADJ_ID_A,
        b.ADJ_ID          AS ADJ_ID_B,
        a.COBID,
        a.PROCESS_TYPE,
        a.ENTITY_CODE     AS ENTITY_A,
        b.ENTITY_CODE     AS ENTITY_B,
        a.BOOK_CODE       AS BOOK_A,
        b.BOOK_CODE       AS BOOK_B,
        a.DEPARTMENT_CODE  AS DEPT_A,
        b.DEPARTMENT_CODE  AS DEPT_B,
        a.USERNAME         AS USER_A,
        b.USERNAME         AS USER_B,
        a.CREATED_DATE     AS CREATED_A,
        b.CREATED_DATE     AS CREATED_B,
        a.RUN_STATUS       AS STATUS_A,
        b.RUN_STATUS       AS STATUS_B,
        CASE
            WHEN a.CREATED_DATE > b.CREATED_DATE THEN a.ADJ_ID
            ELSE b.ADJ_ID
        END AS SUPERSEDING_ADJ_ID
    FROM active_adjustments a
    INNER JOIN active_adjustments b
        ON  a.COBID        = b.COBID
        AND a.PROCESS_TYPE = b.PROCESS_TYPE
        AND a.ADJ_ID       < b.ADJ_ID     -- avoid self-join duplicates
        -- Overlap condition: dimensions must match OR one side is wildcard
        AND (a.ENTITY_CODE       = b.ENTITY_CODE       OR a.ENTITY_CODE       = '*' OR b.ENTITY_CODE       = '*')
        AND (a.SOURCE_SYSTEM_CODE = b.SOURCE_SYSTEM_CODE OR a.SOURCE_SYSTEM_CODE = '*' OR b.SOURCE_SYSTEM_CODE = '*')
        AND (a.DEPARTMENT_CODE   = b.DEPARTMENT_CODE   OR a.DEPARTMENT_CODE   = '*' OR b.DEPARTMENT_CODE   = '*')
        AND (a.BOOK_CODE         = b.BOOK_CODE         OR a.BOOK_CODE         = '*' OR b.BOOK_CODE         = '*')
        AND (a.CURRENCY_CODE     = b.CURRENCY_CODE     OR a.CURRENCY_CODE     = '*' OR b.CURRENCY_CODE     = '*')
)
SELECT
    ADJ_ID_A,
    ADJ_ID_B,
    COBID,
    PROCESS_TYPE,
    ENTITY_A,
    ENTITY_B,
    BOOK_A,
    BOOK_B,
    DEPT_A,
    DEPT_B,
    USER_A,
    USER_B,
    CREATED_A,
    CREATED_B,
    STATUS_A,
    STATUS_B,
    SUPERSEDING_ADJ_ID,
    'Overlap detected: ADJ ' || ADJ_ID_A || ' and ADJ ' || ADJ_ID_B
        || ' target the same scope/entity/book on COB ' || COBID AS ALERT_MESSAGE
FROM overlaps;
