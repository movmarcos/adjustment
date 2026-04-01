-- =============================================================================
-- 06_TASKS.SQL
-- Four independent scope-pipeline tasks (time-based polling, no stream guard).
-- Each task:
--   • Runs every 1 minute unconditionally (SCHEDULE only, no WHEN clause)
--   • Calls SP_RUN_PIPELINE with its scope name and pipeline type list
--   • Exits in milliseconds when there is nothing to process
--
-- WHY NO STREAM GUARD:
--   APPEND_ONLY streams on views only capture INSERT operations. When a blocked
--   adjustment is unblocked (_unblock_resolved sets BLOCKED_BY_ADJ_ID = NULL),
--   that is an UPDATE — invisible to APPEND_ONLY streams. The task would never
--   fire for those adjustments, leaving them stuck in the queue forever.
--
--   Additionally, SP_RUN_PIPELINE reads directly from ADJ_HEADER (not from
--   the stream), so the stream offset never advances. SYSTEM$STREAM_HAS_DATA
--   remains TRUE permanently after the first adjustment, making the guard
--   a no-op rather than an efficiency measure.
--
--   Pure time-based polling (1 minute) is the correct pattern. SP_RUN_PIPELINE
--   exits cleanly when nothing is eligible (0-row UPDATE → empty Running set).
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
    COMMENT   = 'Polls for eligible VaR adjustments every minute and processes them via SP_RUN_PIPELINE.'
AS
    CALL ADJUSTMENT_APP.SP_RUN_PIPELINE('VaR', '["VaR"]');


-- ─── Stress ────────────────────────────────────────────────────────────────

CREATE OR REPLACE TASK ADJUSTMENT_APP.TASK_PROCESS_STRESS
    WAREHOUSE = DVLP_RAVEN_WH_M
    SCHEDULE  = '1 MINUTE'
    COMMENT   = 'Polls for eligible Stress adjustments every minute and processes them via SP_RUN_PIPELINE.'
AS
    CALL ADJUSTMENT_APP.SP_RUN_PIPELINE('Stress', '["Stress"]');


-- ─── FRTB (all sub-types) ───────────────────────────────────────────────────

CREATE OR REPLACE TASK ADJUSTMENT_APP.TASK_PROCESS_FRTB
    WAREHOUSE = DVLP_RAVEN_WH_M
    SCHEDULE  = '1 MINUTE'
    COMMENT   = 'Polls for eligible FRTB-pipeline adjustments (FRTB, FRTBDRC, FRTBRRAO, FRTBALL) every minute.'
AS
    CALL ADJUSTMENT_APP.SP_RUN_PIPELINE('FRTB', '["FRTB","FRTBDRC","FRTBRRAO","FRTBALL"]');


-- ─── Sensitivity ───────────────────────────────────────────────────────────

CREATE OR REPLACE TASK ADJUSTMENT_APP.TASK_PROCESS_SENSITIVITY
    WAREHOUSE = DVLP_RAVEN_WH_M
    SCHEDULE  = '1 MINUTE'
    COMMENT   = 'Polls for eligible Sensitivity adjustments every minute and processes them via SP_RUN_PIPELINE.'
AS
    CALL ADJUSTMENT_APP.SP_RUN_PIPELINE('Sensitivity', '["Sensitivity"]');


-- NOTE: Tasks are resumed by deploy.py (resume_pipeline_tasks) as a
-- dedicated step after all DB objects are deployed.

-- ═══════════════════════════════════════════════════════════════════════════
-- VERIFY
-- ═══════════════════════════════════════════════════════════════════════════
SHOW TASKS LIKE 'TASK_PROCESS_%' IN SCHEMA ADJUSTMENT_APP;
