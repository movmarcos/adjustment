-- =============================================================================
-- 02_STREAMS.SQL
-- Queue views (one per scope). The eligible set for each pipeline.
--
-- The pipeline POLLS these (via SP_RUN_PIPELINE reading ADJ_HEADER every minute)
-- — it no longer uses streams. The earlier stream-gated design stranded Pending
-- rows that arrived while a run was in flight (the drain consumed their event),
-- which got worse with concurrent users. Polling can't lose work.
--
-- The old per-scope STREAM_QUEUE_* streams are DROPped below so they don't keep
-- pinning change data on ADJ_HEADER. The views stay (eligible-set + monitoring).
--
-- PREREQUISITE: 01_tables.sql first (ADJ_HEADER + BLOCKED_BY_ADJ_ID).
-- =============================================================================

USE SCHEMA ADJUSTMENT_APP;

-- Change tracking stays on (cheap, and dynamic tables on ADJ_HEADER rely on it).
ALTER TABLE ADJUSTMENT_APP.ADJ_HEADER SET CHANGE_TRACKING = TRUE;

-- Drop the old per-scope streams FIRST — the pipeline polls now and nothing
-- consumes them; left in place they pin change data on ADJ_HEADER, and they must
-- be gone before the queue views they sit on can be replaced below.
DROP STREAM IF EXISTS ADJUSTMENT_APP.STREAM_QUEUE_VAR;
DROP STREAM IF EXISTS ADJUSTMENT_APP.STREAM_QUEUE_STRESS;
DROP STREAM IF EXISTS ADJUSTMENT_APP.STREAM_QUEUE_FRTB;
DROP STREAM IF EXISTS ADJUSTMENT_APP.STREAM_QUEUE_SENSITIVITY;


-- ═══════════════════════════════════════════════════════════════════════════
-- QUEUE VIEWS
--
-- Show only adjustments that are eligible to be picked up by a pipeline task:
--   • Pending or Approved (not yet claimed)
--   • Not blocked (BLOCKED_BY_ADJ_ID IS NULL)
--   • Not soft-deleted
--
-- Used for monitoring and as the documented "eligible set" definition; the
-- pipeline applies the same filter directly against ADJ_HEADER when it polls.
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW ADJUSTMENT_APP.VW_QUEUE_VAR
    COMMENT = 'Eligible VaR adjustments: Pending/Approved + unblocked.'
AS
SELECT * FROM ADJUSTMENT_APP.ADJ_HEADER
WHERE PROCESS_TYPE = 'VaR'
  AND RUN_STATUS IN ('Pending', 'Approved')
  AND BLOCKED_BY_ADJ_ID IS NULL
  AND IS_DELETED = FALSE;

CREATE OR REPLACE VIEW ADJUSTMENT_APP.VW_QUEUE_STRESS
    COMMENT = 'Eligible Stress adjustments: Pending/Approved + unblocked.'
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
    COMMENT = 'Eligible Sensitivity adjustments: Pending/Approved + unblocked.'
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
