-- =============================================================================
-- 06_TASKS.SQL
-- Stream-triggered task that processes recurring (and any leftover) pending
-- adjustments.
--
-- Ad-hoc adjustments are processed immediately by SP_SUBMIT_ADJUSTMENT.
-- This task handles:
--   • Recurring adjustments (created from templates or external triggers)
--   • Retry of previously failed adjustments (if manually re-set to Pending)
-- =============================================================================

USE DATABASE DVLP_RAPTOR_NEWADJ;
USE SCHEMA ADJUSTMENT_APP;

-- ═══════════════════════════════════════════════════════════════════════════
-- 1. PROCESS_PENDING_TASK — Main processing task
--
-- Checks every minute if the ADJ_HEADER_STREAM has data.
-- For each distinct (PROCESS_TYPE, ADJUSTMENT_ACTION, COBID) combination
-- with status = 'Pending', calls SP_PROCESS_ADJUSTMENT.
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE TASK ADJUSTMENT_APP.PROCESS_PENDING_TASK
    WAREHOUSE = DVLP_RAPTOR_WH_XS
    SCHEDULE  = '1 MINUTE'
    COMMENT   = 'Processes pending adjustments detected via ADJ_HEADER_STREAM. Handles recurring adjustments.'
    WHEN SYSTEM$STREAM_HAS_DATA('ADJUSTMENT_APP.ADJ_HEADER_STREAM')
AS
DECLARE
    c1 CURSOR FOR
        SELECT DISTINCT
            h.PROCESS_TYPE,
            h.ADJUSTMENT_ACTION,
            h.COBID
        FROM ADJUSTMENT_APP.ADJ_HEADER h
        WHERE h.RUN_STATUS IN ('Pending', 'Approved')
          AND h.IS_DELETED = FALSE
          AND h.ADJUSTMENT_OCCURRENCE = 'RECURRING'   -- Only recurring; ad-hoc is processed immediately
        ORDER BY h.COBID, h.PROCESS_TYPE;
    v_process_type     VARCHAR;
    v_adjustment_action VARCHAR;
    v_cobid            NUMBER;
    v_result           VARCHAR;
BEGIN
    -- Consume the stream so the WHEN guard resets
    -- (The actual processing reads from ADJ_HEADER, not the stream)
    CREATE OR REPLACE TEMPORARY TABLE ADJUSTMENT_APP._STREAM_CONSUME AS
        SELECT * FROM ADJUSTMENT_APP.ADJ_HEADER_STREAM;

    FOR record IN c1 DO
        v_process_type      := record.PROCESS_TYPE;
        v_adjustment_action := record.ADJUSTMENT_ACTION;
        v_cobid             := record.COBID;

        -- Call the processing engine
        CALL ADJUSTMENT_APP.SP_PROCESS_ADJUSTMENT(
            :v_process_type,
            :v_adjustment_action,
            :v_cobid
        );
    END FOR;

    DROP TABLE IF EXISTS ADJUSTMENT_APP._STREAM_CONSUME;

    RETURN 'PROCESS_PENDING_TASK completed';
END;


-- ═══════════════════════════════════════════════════════════════════════════
-- 2. INSTANTIATE_RECURRING_TASK — Creates ADJ_HEADER rows from templates
--
-- Checks for recurring templates whose dependencies are met.
-- Creates new ADJ_HEADER entries with status = 'Pending'.
-- Runs on a schedule (e.g., every 5 minutes after COB data lands).
--
-- NOTE: This is a placeholder. The dependency-check logic will depend on
-- the external scheduling system or BATCH.RUN_LOG completion signals.
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE TASK ADJUSTMENT_APP.INSTANTIATE_RECURRING_TASK
    WAREHOUSE = DVLP_RAPTOR_WH_XS
    SCHEDULE  = '5 MINUTE'
    COMMENT   = 'Creates ADJ_HEADER entries from ADJ_RECURRING_TEMPLATE when dependencies are met.'
    WHEN SYSTEM$STREAM_HAS_DATA('ADJUSTMENT_APP.ADJ_HEADER_STREAM') = FALSE
    -- Only runs when the processing task is idle (no pending work)
AS
BEGIN
    -- For each active template, check if the dependency is met
    -- and if an adjustment for today's COB doesn't already exist
    INSERT INTO ADJUSTMENT_APP.ADJ_HEADER (
        COBID, PROCESS_TYPE, ADJUSTMENT_TYPE, ADJUSTMENT_ACTION,
        SCALE_FACTOR, SCALE_FACTOR_ADJUSTED,
        ENTITY_CODE, SOURCE_SYSTEM_CODE, DEPARTMENT_CODE, BOOK_CODE,
        CURRENCY_CODE, TRADE_TYPOLOGY, TRADE_CODE, STRATEGY,
        INSTRUMENT_CODE, SIMULATION_NAME, SIMULATION_SOURCE,
        MEASURE_TYPE_CODE, TRADER_CODE, GUARANTEED_ENTITY,
        REASON, RUN_STATUS, IS_POSITIVE_ADJUSTMENT,
        USERNAME, ADJUSTMENT_OCCURRENCE
    )
    SELECT
        TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')) AS COBID,
        t.PROCESS_TYPE,
        t.ADJUSTMENT_TYPE,
        t.ADJUSTMENT_ACTION,
        t.SCALE_FACTOR,
        CASE
            WHEN LOWER(t.ADJUSTMENT_TYPE) = 'flatten' THEN -1
            ELSE t.SCALE_FACTOR - 1
        END AS SCALE_FACTOR_ADJUSTED,
        t.ENTITY_CODE, t.SOURCE_SYSTEM_CODE, t.DEPARTMENT_CODE, t.BOOK_CODE,
        t.CURRENCY_CODE, t.TRADE_TYPOLOGY, t.TRADE_CODE, t.STRATEGY,
        t.INSTRUMENT_CODE, t.SIMULATION_NAME, t.SIMULATION_SOURCE,
        t.MEASURE_TYPE_CODE, t.TRADER_CODE, t.GUARANTEED_ENTITY,
        CONCAT('Recurring: ', t.TEMPLATE_NAME) AS REASON,
        'Pending' AS RUN_STATUS,
        TRUE AS IS_POSITIVE_ADJUSTMENT,
        'SYSTEM' AS USERNAME,
        'RECURRING' AS ADJUSTMENT_OCCURRENCE
    FROM ADJUSTMENT_APP.ADJ_RECURRING_TEMPLATE t
    WHERE t.IS_ACTIVE = TRUE
      -- Don't duplicate: skip if an adjustment for this template + COB already exists
      AND NOT EXISTS (
          SELECT 1
          FROM ADJUSTMENT_APP.ADJ_HEADER h
          WHERE h.COBID = TO_NUMBER(TO_CHAR(CURRENT_DATE(), 'YYYYMMDD'))
            AND h.PROCESS_TYPE = t.PROCESS_TYPE
            AND h.ADJUSTMENT_TYPE = t.ADJUSTMENT_TYPE
            AND h.REASON = CONCAT('Recurring: ', t.TEMPLATE_NAME)
            AND h.IS_DELETED = FALSE
      )
      -- TODO: Add dependency check here
      -- e.g., EXISTS (SELECT 1 FROM BATCH.RUN_LOG WHERE ...)
    ;

    RETURN 'INSTANTIATE_RECURRING_TASK completed';
END;


-- ═══════════════════════════════════════════════════════════════════════════
-- TASK STATE — Both tasks start SUSPENDED (enable when ready)
-- ═══════════════════════════════════════════════════════════════════════════

ALTER TASK ADJUSTMENT_APP.PROCESS_PENDING_TASK       SUSPEND;
ALTER TASK ADJUSTMENT_APP.INSTANTIATE_RECURRING_TASK SUSPEND;

-- To enable:
-- ALTER TASK ADJUSTMENT_APP.PROCESS_PENDING_TASK       RESUME;
-- ALTER TASK ADJUSTMENT_APP.INSTANTIATE_RECURRING_TASK RESUME;

SHOW TASKS LIKE 'PROCESS_%' IN SCHEMA ADJUSTMENT_APP;
SHOW TASKS LIKE 'INSTANTIATE_%' IN SCHEMA ADJUSTMENT_APP;
