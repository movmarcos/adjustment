-- =============================================================================
-- 02_STREAMS.SQL
-- Standard (non-APPEND_ONLY) streams on ADJ_HEADER, one per scope pipeline.
-- Queue views (one per scope) used for monitoring.
--
-- WHY STANDARD STREAMS (NOT APPEND_ONLY):
--   APPEND_ONLY streams only capture INSERT operations. When a blocked
--   adjustment is unblocked (_unblock_resolved sets BLOCKED_BY_ADJ_ID = NULL),
--   that is an UPDATE — invisible to APPEND_ONLY streams. Standard streams
--   capture INSERTs, UPDATEs, and DELETEs, so newly unblocked adjustments
--   will be visible and the tasks will fire correctly.
--
-- WHY STREAMS ON ADJ_HEADER (NOT ON VIEWS):
--   Snowflake streams require a table or materialized view as source.
--   The queue views (VW_QUEUE_*) are plain views and cannot have streams.
--   ADJ_HEADER is the correct source — all changes flow through it.
--
-- PREREQUISITE: 01_tables.sql must run first (ADJ_HEADER + BLOCKED_BY_ADJ_ID).
-- =============================================================================

USE DATABASE DVLP_RAPTOR_NEWADJ;
USE SCHEMA ADJUSTMENT_APP;

-- ═══════════════════════════════════════════════════════════════════════════
-- STREAMS — standard (captures INSERT + UPDATE + DELETE)
-- One stream per scope pipeline on ADJ_HEADER.
-- APPEND_ONLY is intentionally omitted.
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE STREAM ADJUSTMENT_APP.STREAM_QUEUE_VAR
    ON TABLE ADJUSTMENT_APP.ADJ_HEADER
    COMMENT = 'Standard stream on ADJ_HEADER for VaR pipeline. Captures INSERTs and UPDATEs (e.g. unblocking via BLOCKED_BY_ADJ_ID = NULL).';

CREATE OR REPLACE STREAM ADJUSTMENT_APP.STREAM_QUEUE_STRESS
    ON TABLE ADJUSTMENT_APP.ADJ_HEADER
    COMMENT = 'Standard stream on ADJ_HEADER for Stress pipeline. Captures INSERTs and UPDATEs.';

CREATE OR REPLACE STREAM ADJUSTMENT_APP.STREAM_QUEUE_FRTB
    ON TABLE ADJUSTMENT_APP.ADJ_HEADER
    COMMENT = 'Standard stream on ADJ_HEADER for FRTB pipeline (FRTB, FRTBDRC, FRTBRRAO, FRTBALL). Captures INSERTs and UPDATEs.';

CREATE OR REPLACE STREAM ADJUSTMENT_APP.STREAM_QUEUE_SENSITIVITY
    ON TABLE ADJUSTMENT_APP.ADJ_HEADER
    COMMENT = 'Standard stream on ADJ_HEADER for Sensitivity pipeline. Captures INSERTs and UPDATEs.';


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
SHOW STREAMS LIKE 'STREAM_QUEUE_%' IN SCHEMA ADJUSTMENT_APP;
SHOW VIEWS  LIKE 'VW_QUEUE_%'     IN SCHEMA ADJUSTMENT_APP;
