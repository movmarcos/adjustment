-- =============================================================================
-- 06_TASKS.SQL
-- Four scope-pipeline tasks. Each runs every 1 minute and calls SP_RUN_PIPELINE,
-- which POLLS the queue (reads ADJ_HEADER) and processes whatever is eligible.
--
-- No stream guard (no WHEN clause): the task fires every minute unconditionally.
-- SP_RUN_PIPELINE exits in milliseconds when nothing is eligible. Polling — not
-- stream-gating — is what guarantees an adjustment is never stranded in Pending,
-- including under concurrent submissions. (A stream guard + drain raced with
-- rows that arrived mid-run and silently dropped them.)
--
-- FRTB pipeline covers: FRTB, FRTBDRC, FRTBRRAO, FRTBALL.
-- FRTBALL is a fan-out tag applied within each real FRTB sub-type call.
-- =============================================================================

USE DATABASE DVLP_RAPTOR_NEWADJ;
USE SCHEMA ADJUSTMENT_APP;

-- ─── VaR ───────────────────────────────────────────────────────────────────

CREATE OR REPLACE TASK ADJUSTMENT_APP.TASK_PROCESS_VAR
    WAREHOUSE = DVLP_RAVEN_WH_M
    SCHEDULE  = '1 MINUTE'
    COMMENT   = 'Every 1 min: SP_RUN_PIPELINE polls and processes eligible VaR adjustments.'
AS
    CALL ADJUSTMENT_APP.SP_RUN_PIPELINE('VaR', '["VaR"]');


-- ─── Stress ────────────────────────────────────────────────────────────────

CREATE OR REPLACE TASK ADJUSTMENT_APP.TASK_PROCESS_STRESS
    WAREHOUSE = DVLP_RAVEN_WH_M
    SCHEDULE  = '1 MINUTE'
    COMMENT   = 'Every 1 min: SP_RUN_PIPELINE polls and processes eligible Stress adjustments.'
AS
    CALL ADJUSTMENT_APP.SP_RUN_PIPELINE('Stress', '["Stress"]');


-- ─── FRTB (all sub-types) ───────────────────────────────────────────────────

CREATE OR REPLACE TASK ADJUSTMENT_APP.TASK_PROCESS_FRTB
    WAREHOUSE = DVLP_RAVEN_WH_M
    SCHEDULE  = '1 MINUTE'
    COMMENT   = 'Every 1 min: SP_RUN_PIPELINE polls and processes eligible FRTB-pipeline adjustments (FRTB, FRTBDRC, FRTBRRAO, FRTBALL).'
AS
    CALL ADJUSTMENT_APP.SP_RUN_PIPELINE('FRTB', '["FRTB","FRTBDRC","FRTBRRAO","FRTBALL"]');


-- ─── Sensitivity ───────────────────────────────────────────────────────────

CREATE OR REPLACE TASK ADJUSTMENT_APP.TASK_PROCESS_SENSITIVITY
    WAREHOUSE = DVLP_RAVEN_WH_M
    SCHEDULE  = '1 MINUTE'
    COMMENT   = 'Every 1 min: SP_RUN_PIPELINE polls and processes eligible Sensitivity adjustments.'
AS
    CALL ADJUSTMENT_APP.SP_RUN_PIPELINE('Sensitivity', '["Sensitivity"]');


-- NOTE: Tasks are resumed by deploy.py (resume_pipeline_tasks) as a
-- dedicated step after all DB objects are deployed.

-- ═══════════════════════════════════════════════════════════════════════════
-- VERIFY
-- ═══════════════════════════════════════════════════════════════════════════
SHOW TASKS LIKE 'TASK_PROCESS_%' IN SCHEMA ADJUSTMENT_APP;
