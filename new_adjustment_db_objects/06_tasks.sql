-- =============================================================================
-- 06_TASKS.SQL
-- Four independent scope-pipeline tasks, stream-guarded + time-based schedule.
-- Each task:
--   • Runs every 1 minute on schedule
--   • Only executes the body WHEN the corresponding stream has data
--     (captures INSERT + UPDATE + DELETE — standard streams, not APPEND_ONLY)
--   • Calls SP_RUN_PIPELINE with its scope name and pipeline type list
--
-- WHY STANDARD STREAMS (NOT APPEND_ONLY):
--   Standard streams capture INSERTs and UPDATEs. This means that when a
--   blocked adjustment is unblocked (_unblock_resolved sets BLOCKED_BY_ADJ_ID
--   = NULL), that UPDATE is visible to SYSTEM$STREAM_HAS_DATA and the task
--   fires correctly. APPEND_ONLY would miss those updates entirely.
--
-- NOTE: SP_RUN_PIPELINE reads from ADJ_HEADER directly (not from the stream).
--   The stream is consumed inside the task body via the SP call's implicit
--   DML context — Snowflake advances the stream offset when the task runs.
--   If SP_RUN_PIPELINE does not consume the stream explicitly, the offset will
--   not advance and SYSTEM$STREAM_HAS_DATA will remain TRUE after the first run.
--   Ensure SP_RUN_PIPELINE (or a wrapper SELECT) consumes the stream.
--
-- FRTB pipeline covers: FRTB, FRTBDRC, FRTBRRAO, FRTBALL
-- FRTBALL is a fan-out tag applied within each real FRTB sub-type call.
-- =============================================================================

USE DATABASE DVLP_RAPTOR_NEWADJ;
USE SCHEMA ADJUSTMENT_APP;

-- ─── VaR ───────────────────────────────────────────────────────────────────

CREATE OR REPLACE TASK ADJUSTMENT_APP.TASK_PROCESS_VAR
    WAREHOUSE = DVLP_RAVEN_WH_M
    SCHEDULE  = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('ADJUSTMENT_APP.STREAM_QUEUE_VAR')
    COMMENT   = 'Fires when STREAM_QUEUE_VAR has data (INSERT or UPDATE on ADJ_HEADER). Processes eligible VaR adjustments via SP_RUN_PIPELINE.'
AS
    CALL ADJUSTMENT_APP.SP_RUN_PIPELINE('VaR', '["VaR"]');


-- ─── Stress ────────────────────────────────────────────────────────────────

CREATE OR REPLACE TASK ADJUSTMENT_APP.TASK_PROCESS_STRESS
    WAREHOUSE = DVLP_RAVEN_WH_M
    SCHEDULE  = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('ADJUSTMENT_APP.STREAM_QUEUE_STRESS')
    COMMENT   = 'Fires when STREAM_QUEUE_STRESS has data (INSERT or UPDATE on ADJ_HEADER). Processes eligible Stress adjustments via SP_RUN_PIPELINE.'
AS
    CALL ADJUSTMENT_APP.SP_RUN_PIPELINE('Stress', '["Stress"]');


-- ─── FRTB (all sub-types) ───────────────────────────────────────────────────

CREATE OR REPLACE TASK ADJUSTMENT_APP.TASK_PROCESS_FRTB
    WAREHOUSE = DVLP_RAVEN_WH_M
    SCHEDULE  = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('ADJUSTMENT_APP.STREAM_QUEUE_FRTB')
    COMMENT   = 'Fires when STREAM_QUEUE_FRTB has data (INSERT or UPDATE on ADJ_HEADER). Processes eligible FRTB-pipeline adjustments (FRTB, FRTBDRC, FRTBRRAO, FRTBALL).'
AS
    CALL ADJUSTMENT_APP.SP_RUN_PIPELINE('FRTB', '["FRTB","FRTBDRC","FRTBRRAO","FRTBALL"]');


-- ─── Sensitivity ───────────────────────────────────────────────────────────

CREATE OR REPLACE TASK ADJUSTMENT_APP.TASK_PROCESS_SENSITIVITY
    WAREHOUSE = DVLP_RAVEN_WH_M
    SCHEDULE  = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('ADJUSTMENT_APP.STREAM_QUEUE_SENSITIVITY')
    COMMENT   = 'Fires when STREAM_QUEUE_SENSITIVITY has data (INSERT or UPDATE on ADJ_HEADER). Processes eligible Sensitivity adjustments via SP_RUN_PIPELINE.'
AS
    CALL ADJUSTMENT_APP.SP_RUN_PIPELINE('Sensitivity', '["Sensitivity"]');


-- NOTE: Tasks are resumed by deploy.py (resume_pipeline_tasks) as a
-- dedicated step after all DB objects are deployed.

-- ═══════════════════════════════════════════════════════════════════════════
-- VERIFY
-- ═══════════════════════════════════════════════════════════════════════════
SHOW TASKS LIKE 'TASK_PROCESS_%' IN SCHEMA ADJUSTMENT_APP;
