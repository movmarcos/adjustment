-- =============================================================================
-- 04_STREAMS_TASKS.SQL
-- CDC Streams on adjustment tables + Task chains for automated processing
-- =============================================================================

USE DATABASE ADJUSTMENT_DB;
USE WAREHOUSE ADJUSTMENT_WH;

-- ═════════════════════════════════════════════════════════════════════════════
-- 1. STREAMS — Change Data Capture on adjustment tables
-- ═════════════════════════════════════════════════════════════════════════════

-- Stream on ADJ_HEADER to detect new/updated adjustments
CREATE OR REPLACE STREAM CORE.ADJ_HEADER_STREAM
  ON TABLE CORE.ADJ_HEADER
  APPEND_ONLY = FALSE
  COMMENT = 'Captures all changes to adjustment headers for downstream processing';

-- Stream on ADJ_LINE_ITEM for monitoring
CREATE OR REPLACE STREAM CORE.ADJ_LINE_ITEM_STREAM
  ON TABLE CORE.ADJ_LINE_ITEM
  APPEND_ONLY = TRUE
  COMMENT = 'Captures new adjustment line items for audit logging';


-- ═════════════════════════════════════════════════════════════════════════════
-- 2. TASK: Log status transitions automatically
--    Runs every minute, picks up status changes from the header stream
-- ═════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE TASK CORE.TASK_LOG_STATUS_CHANGES
  WAREHOUSE = ADJUSTMENT_TASK_WH
  SCHEDULE  = '1 MINUTE'
  COMMENT   = 'Automatically logs ADJ_HEADER status changes to ADJ_STATUS_HISTORY'
  WHEN SYSTEM$STREAM_HAS_DATA('CORE.ADJ_HEADER_STREAM')
AS
INSERT INTO CORE.ADJ_STATUS_HISTORY (ADJ_ID, OLD_STATUS, NEW_STATUS, CHANGED_BY, CHANGED_AT, COMMENT)
SELECT
    s.ADJ_ID,
    NULL,                               -- old status not available from append-only; use metadata action
    s.ADJ_STATUS,
    s.CREATED_BY,                       -- fallback; ideally set by the procedure
    CURRENT_TIMESTAMP(),
    'Auto-logged by TASK_LOG_STATUS_CHANGES from stream'
FROM CORE.ADJ_HEADER_STREAM s
WHERE s.METADATA$ACTION = 'INSERT'
   OR s.METADATA$ISUPDATE = TRUE
;


-- ═════════════════════════════════════════════════════════════════════════════
-- 3. TASK: Generate AI impact summary for newly applied adjustments
--    Chains after status log task. Uses Cortex COMPLETE to summarize.
-- ═════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE TASK CORE.TASK_AI_SUMMARIZE
  WAREHOUSE = ADJUSTMENT_TASK_WH
  AFTER CORE.TASK_LOG_STATUS_CHANGES
  COMMENT = 'Generates Cortex AI summary for newly applied adjustments'
AS
CALL CORE.SP_GENERATE_AI_SUMMARY();


-- ═════════════════════════════════════════════════════════════════════════════
-- 4. TASK: Anomaly detection on adjustment patterns (daily)
-- ═════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE TASK CORE.TASK_ANOMALY_CHECK
  WAREHOUSE = ADJUSTMENT_TASK_WH
  SCHEDULE  = 'USING CRON 0 8 * * * America/New_York'  -- Daily at 8 AM ET
  COMMENT   = 'Daily anomaly detection on adjustment patterns using Cortex'
AS
CALL AI.SP_DETECT_ADJUSTMENT_ANOMALIES();


-- ═════════════════════════════════════════════════════════════════════════════
-- 5. TASK: Send email notification on applied adjustments
--    Uncomment when notification integration is configured.
-- ═════════════════════════════════════════════════════════════════════════════

-- CREATE OR REPLACE TASK CORE.TASK_NOTIFY_ON_APPLY
--   WAREHOUSE = ADJUSTMENT_TASK_WH
--   AFTER CORE.TASK_AI_SUMMARIZE
--   COMMENT = 'Sends notification when adjustments are applied'
-- AS
-- CALL SYSTEM$SEND_EMAIL(
--   'ADJ_EMAIL_NOTIFICATION',
--   'team@company.com',
--   'Adjustment Applied',
--   (SELECT LISTAGG(
--       'ADJ #' || ADJ_ID || ' (' || ADJ_TYPE || ') applied by ' || APPLIED_BY || ' on ' || TARGET_DATE,
--       '\n'
--   ) FROM CORE.ADJ_HEADER
--    WHERE ADJ_STATUS = 'APPLIED'
--      AND APPLIED_AT >= DATEADD('MINUTE', -5, CURRENT_TIMESTAMP()))
-- );


-- ═════════════════════════════════════════════════════════════════════════════
-- 6. RESUME TASKS (tasks are created in suspended state)
-- ═════════════════════════════════════════════════════════════════════════════

ALTER TASK CORE.TASK_AI_SUMMARIZE    RESUME;
ALTER TASK CORE.TASK_LOG_STATUS_CHANGES RESUME;
ALTER TASK CORE.TASK_ANOMALY_CHECK   RESUME;

-- Note: child tasks (AFTER) must be resumed before parent tasks.
-- Correct resume order:
--   1. TASK_AI_SUMMARIZE     (child)
--   2. TASK_LOG_STATUS_CHANGES (parent)
--   3. TASK_ANOMALY_CHECK    (independent)

-- ═════════════════════════════════════════════════════════════════════════════
-- 7. VERIFY
-- ═════════════════════════════════════════════════════════════════════════════

SHOW STREAMS IN SCHEMA CORE;
SHOW TASKS IN SCHEMA CORE;
