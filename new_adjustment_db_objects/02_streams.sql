-- =============================================================================
-- 02_STREAMS.SQL
-- Queue views (one per scope) used for monitoring.
--
-- STREAMS REMOVED — see explanation below.
--
-- WHY STREAMS WERE REMOVED:
--   The original design used APPEND_ONLY streams on these views as task guards
--   (WHEN SYSTEM$STREAM_HAS_DATA(...)). This had two fatal flaws:
--
--   1. APPEND_ONLY only captures INSERTs. When a blocked adjustment is unblocked
--      (_unblock_resolved sets BLOCKED_BY_ADJ_ID = NULL), that is an UPDATE —
--      invisible to APPEND_ONLY streams. The task would never fire for newly
--      unblocked adjustments, leaving them stuck in the queue forever.
--
--   2. SP_RUN_PIPELINE reads from ADJ_HEADER directly, never from the stream.
--      Snowflake only advances a stream's offset when you consume it inside a
--      DML transaction. Since the stream was never consumed, SYSTEM$STREAM_HAS_DATA
--      remained TRUE permanently, making the guard useless after the first run.
--
--   The tasks now use pure time-based polling (every 1 minute) with no WHEN
--   clause. SP_RUN_PIPELINE exits cleanly when there is nothing to process.
--
-- QUEUE VIEWS are kept — they are still useful for the Processing Queue
-- monitoring page (VW_PROCESSING_QUEUE in 08_views.sql uses them conceptually,
-- and they make it easy to inspect the eligible queue directly in Snowflake).
--
-- PREREQUISITE: 01_tables.sql must run first (BLOCKED_BY_ADJ_ID column).
-- =============================================================================

USE DATABASE DVLP_RAPTOR_NEWADJ;
USE SCHEMA ADJUSTMENT_APP;

-- ═══════════════════════════════════════════════════════════════════════════
-- DROP STALE STREAMS (idempotent — safe to re-run after migration)
-- ═══════════════════════════════════════════════════════════════════════════

DROP STREAM IF EXISTS ADJUSTMENT_APP.STREAM_QUEUE_VAR;
DROP STREAM IF EXISTS ADJUSTMENT_APP.STREAM_QUEUE_STRESS;
DROP STREAM IF EXISTS ADJUSTMENT_APP.STREAM_QUEUE_FRTB;
DROP STREAM IF EXISTS ADJUSTMENT_APP.STREAM_QUEUE_SENSITIVITY;


-- ═══════════════════════════════════════════════════════════════════════════
-- QUEUE VIEWS
--
-- Show only adjustments that are eligible to be picked up by a pipeline task:
--   • Pending (not yet claimed)
--   • Not blocked (BLOCKED_BY_ADJ_ID IS NULL)
--   • Not soft-deleted
--
-- Used for: monitoring in Snowflake, and as the base for SP_RUN_PIPELINE's
-- eligibility check (which queries ADJ_HEADER directly with the same filters).
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW ADJUSTMENT_APP.VW_QUEUE_VAR
    COMMENT = 'Eligible VaR adjustments: Pending/Approved + unblocked. Monitoring view for TASK_PROCESS_VAR.'
AS
SELECT * FROM ADJUSTMENT_APP.ADJ_HEADER
WHERE PROCESS_TYPE = 'VaR'
  AND RUN_STATUS IN ('Pending', 'Approved')
  AND BLOCKED_BY_ADJ_ID IS NULL
  AND IS_DELETED = FALSE;

CREATE OR REPLACE VIEW ADJUSTMENT_APP.VW_QUEUE_STRESS
    COMMENT = 'Eligible Stress adjustments: Pending/Approved + unblocked. Monitoring view for TASK_PROCESS_STRESS.'
AS
SELECT * FROM ADJUSTMENT_APP.ADJ_HEADER
WHERE PROCESS_TYPE = 'Stress'
  AND RUN_STATUS IN ('Pending', 'Approved')
  AND BLOCKED_BY_ADJ_ID IS NULL
  AND IS_DELETED = FALSE;

CREATE OR REPLACE VIEW ADJUSTMENT_APP.VW_QUEUE_FRTB
    COMMENT = 'Eligible FRTB-pipeline adjustments (FRTB + FRTBDRC + FRTBRRAO + FRTBALL): Pending/Approved + unblocked.'
AS
SELECT * FROM ADJUSTMENT_APP.ADJ_HEADER
WHERE PROCESS_TYPE IN ('FRTB', 'FRTBDRC', 'FRTBRRAO', 'FRTBALL')
  AND RUN_STATUS IN ('Pending', 'Approved')
  AND BLOCKED_BY_ADJ_ID IS NULL
  AND IS_DELETED = FALSE;

CREATE OR REPLACE VIEW ADJUSTMENT_APP.VW_QUEUE_SENSITIVITY
    COMMENT = 'Eligible Sensitivity adjustments: Pending/Approved + unblocked. Monitoring view for TASK_PROCESS_SENSITIVITY.'
AS
SELECT * FROM ADJUSTMENT_APP.ADJ_HEADER
WHERE PROCESS_TYPE = 'Sensitivity'
  AND RUN_STATUS IN ('Pending', 'Approved')
  AND BLOCKED_BY_ADJ_ID IS NULL
  AND IS_DELETED = FALSE;


-- ═══════════════════════════════════════════════════════════════════════════
-- VERIFY
-- ═══════════════════════════════════════════════════════════════════════════
SHOW VIEWS LIKE 'VW_QUEUE_%' IN SCHEMA ADJUSTMENT_APP;
