-- =============================================================================
-- 02_STREAMS.SQL
-- Queue views (one per scope) + standard streams on those views.
--
-- WHY STREAMS ON VIEWS (NOT ON ADJ_HEADER DIRECTLY):
--   Each queue view filters ADJ_HEADER by PROCESS_TYPE, RUN_STATUS,
--   BLOCKED_BY_ADJ_ID, and IS_DELETED. A stream on the view only fires
--   when rows ENTER or LEAVE that view's result set for that specific scope.
--   If the streams were on ADJ_HEADER, every change to ANY scope would
--   trigger ALL 4 streams — defeating the purpose of per-scope isolation.
--
-- WHY STANDARD STREAMS (NOT APPEND_ONLY):
--   APPEND_ONLY streams only capture INSERTs. When a blocked adjustment is
--   unblocked (_unblock_resolved sets BLOCKED_BY_ADJ_ID = NULL), that is an
--   UPDATE which makes the row RE-ENTER the view. Standard streams capture
--   this as an INSERT event in the stream, so the task fires correctly.
--
-- HOW VIEW-BASED STREAMS WORK:
--   • Row enters view (new Pending INSERT, or unblock UPDATE) → stream INSERT
--   • Row leaves view (promoted to Running, or blocked)        → stream DELETE
--   • SYSTEM$STREAM_HAS_DATA is TRUE only when NET changes exist for THIS scope
--   • The stream exposes all columns from the view (SELECT * FROM ADJ_HEADER)
--     plus METADATA$ACTION, METADATA$ISUPDATE, METADATA$ROW_ID
--
-- PREREQUISITE:
--   01_tables.sql must run first (ADJ_HEADER + BLOCKED_BY_ADJ_ID).
--   Change tracking must be enabled on ADJ_HEADER (required for streams on
--   views). The ALTER TABLE below is idempotent.
-- =============================================================================

USE DATABASE DVLP_RAPTOR_NEWADJ;
USE SCHEMA ADJUSTMENT_APP;

-- ═══════════════════════════════════════════════════════════════════════════
-- ENABLE CHANGE TRACKING ON ADJ_HEADER
-- Required for streams on views that reference this table.
-- ═══════════════════════════════════════════════════════════════════════════

ALTER TABLE ADJUSTMENT_APP.ADJ_HEADER SET CHANGE_TRACKING = TRUE;


-- ═══════════════════════════════════════════════════════════════════════════
-- QUEUE VIEWS
--
-- Show only adjustments that are eligible to be picked up by a pipeline task:
--   • Pending or Approved (not yet claimed)
--   • Not blocked (BLOCKED_BY_ADJ_ID IS NULL)
--   • Not soft-deleted
--
-- These views serve two purposes:
--   1. Source for the per-scope streams (stream tracks rows entering/leaving)
--   2. Monitoring — easy to query directly in Snowflake or from Streamlit
--
-- IMPORTANT: Views must be created BEFORE the streams that reference them.
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW ADJUSTMENT_APP.VW_QUEUE_VAR
    COMMENT = 'Eligible VaR adjustments: Pending/Approved + unblocked. Source for STREAM_QUEUE_VAR.'
AS
SELECT * FROM ADJUSTMENT_APP.ADJ_HEADER
WHERE PROCESS_TYPE = 'VaR'
  AND RUN_STATUS IN ('Pending', 'Approved')
  AND BLOCKED_BY_ADJ_ID IS NULL
  AND IS_DELETED = FALSE;

CREATE OR REPLACE VIEW ADJUSTMENT_APP.VW_QUEUE_STRESS
    COMMENT = 'Eligible Stress adjustments: Pending/Approved + unblocked. Source for STREAM_QUEUE_STRESS.'
AS
SELECT * FROM ADJUSTMENT_APP.ADJ_HEADER
WHERE PROCESS_TYPE = 'Stress'
  AND RUN_STATUS IN ('Pending', 'Approved')
  AND BLOCKED_BY_ADJ_ID IS NULL
  AND IS_DELETED = FALSE;

CREATE OR REPLACE VIEW ADJUSTMENT_APP.VW_QUEUE_FRTB
    COMMENT = 'Eligible FRTB-pipeline adjustments (FRTB + FRTBDRC + FRTBRRAO + FRTBALL): Pending/Approved + unblocked. Source for STREAM_QUEUE_FRTB.'
AS
SELECT * FROM ADJUSTMENT_APP.ADJ_HEADER
WHERE PROCESS_TYPE IN ('FRTB', 'FRTBDRC', 'FRTBRRAO', 'FRTBALL')
  AND RUN_STATUS IN ('Pending', 'Approved')
  AND BLOCKED_BY_ADJ_ID IS NULL
  AND IS_DELETED = FALSE;

CREATE OR REPLACE VIEW ADJUSTMENT_APP.VW_QUEUE_SENSITIVITY
    COMMENT = 'Eligible Sensitivity adjustments: Pending/Approved + unblocked. Source for STREAM_QUEUE_SENSITIVITY.'
AS
SELECT * FROM ADJUSTMENT_APP.ADJ_HEADER
WHERE PROCESS_TYPE = 'Sensitivity'
  AND RUN_STATUS IN ('Pending', 'Approved')
  AND BLOCKED_BY_ADJ_ID IS NULL
  AND IS_DELETED = FALSE;


-- ═══════════════════════════════════════════════════════════════════════════
-- STREAMS — standard (on views, NOT APPEND_ONLY)
--
-- Each stream tracks rows entering/leaving its queue view.
-- SYSTEM$STREAM_HAS_DATA fires the corresponding task only when this
-- specific scope has new eligible work.
--
-- IMPORTANT: Views must exist before these statements run.
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE STREAM ADJUSTMENT_APP.STREAM_QUEUE_VAR
    ON VIEW ADJUSTMENT_APP.VW_QUEUE_VAR
    COMMENT = 'Tracks rows entering/leaving VW_QUEUE_VAR. INSERT = new eligible VaR adjustment (submitted or unblocked). DELETE = claimed (Running) or blocked.';

CREATE OR REPLACE STREAM ADJUSTMENT_APP.STREAM_QUEUE_STRESS
    ON VIEW ADJUSTMENT_APP.VW_QUEUE_STRESS
    COMMENT = 'Tracks rows entering/leaving VW_QUEUE_STRESS. INSERT = new eligible Stress adjustment. DELETE = claimed or blocked.';

CREATE OR REPLACE STREAM ADJUSTMENT_APP.STREAM_QUEUE_FRTB
    ON VIEW ADJUSTMENT_APP.VW_QUEUE_FRTB
    COMMENT = 'Tracks rows entering/leaving VW_QUEUE_FRTB. INSERT = new eligible FRTB-pipeline adjustment. DELETE = claimed or blocked.';

CREATE OR REPLACE STREAM ADJUSTMENT_APP.STREAM_QUEUE_SENSITIVITY
    ON VIEW ADJUSTMENT_APP.VW_QUEUE_SENSITIVITY
    COMMENT = 'Tracks rows entering/leaving VW_QUEUE_SENSITIVITY. INSERT = new eligible Sensitivity adjustment. DELETE = claimed or blocked.';


-- ═══════════════════════════════════════════════════════════════════════════
-- VERIFY
-- ═══════════════════════════════════════════════════════════════════════════
SHOW VIEWS   LIKE 'VW_QUEUE_%'     IN SCHEMA ADJUSTMENT_APP;
SHOW STREAMS LIKE 'STREAM_QUEUE_%' IN SCHEMA ADJUSTMENT_APP;
