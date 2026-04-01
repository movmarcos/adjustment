-- =============================================================================
-- 06_TASKS.SQL
-- Four independent scope-pipeline tasks.
-- Each task:
--   • Is triggered by its scope's stream (stream-guarded, not time-only)
--   • Calls SP_RUN_PIPELINE with its scope name and pipeline type list
--   • Runs on DVLP_RAPTOR_WH_XS (can be changed per scope independently)
--
-- FRTB pipeline covers: FRTB, FRTBDRC, FRTBRRAO, FRTBALL
-- FRTBALL is a fan-out tag applied within each real FRTB sub-type call.
-- =============================================================================

USE DATABASE DVLP_RAPTOR_NEWADJ;
USE SCHEMA ADJUSTMENT_APP;

-- ─── VaR ───────────────────────────────────────────────────────────────────

CREATE OR REPLACE TASK ADJUSTMENT_APP.TASK_PROCESS_VAR
    WAREHOUSE = DVLP_RAPTOR_WH_XS
    SCHEDULE  = '1 MINUTE'
    COMMENT   = 'Processes eligible VaR adjustments. Stream-triggered via STREAM_QUEUE_VAR.'
    WHEN SYSTEM$STREAM_HAS_DATA('ADJUSTMENT_APP.STREAM_QUEUE_VAR')
AS
    CALL ADJUSTMENT_APP.SP_RUN_PIPELINE('VaR', '["VaR"]');


-- ─── Stress ────────────────────────────────────────────────────────────────

CREATE OR REPLACE TASK ADJUSTMENT_APP.TASK_PROCESS_STRESS
    WAREHOUSE = DVLP_RAPTOR_WH_XS
    SCHEDULE  = '1 MINUTE'
    COMMENT   = 'Processes eligible Stress adjustments. Stream-triggered via STREAM_QUEUE_STRESS.'
    WHEN SYSTEM$STREAM_HAS_DATA('ADJUSTMENT_APP.STREAM_QUEUE_STRESS')
AS
    CALL ADJUSTMENT_APP.SP_RUN_PIPELINE('Stress', '["Stress"]');


-- ─── FRTB (all sub-types) ───────────────────────────────────────────────────

CREATE OR REPLACE TASK ADJUSTMENT_APP.TASK_PROCESS_FRTB
    WAREHOUSE = DVLP_RAPTOR_WH_XS
    SCHEDULE  = '1 MINUTE'
    COMMENT   = 'Processes eligible FRTB-pipeline adjustments (FRTB, FRTBDRC, FRTBRRAO, FRTBALL). Stream-triggered via STREAM_QUEUE_FRTB.'
    WHEN SYSTEM$STREAM_HAS_DATA('ADJUSTMENT_APP.STREAM_QUEUE_FRTB')
AS
    CALL ADJUSTMENT_APP.SP_RUN_PIPELINE('FRTB', '["FRTB","FRTBDRC","FRTBRRAO","FRTBALL"]');


-- ─── Sensitivity ───────────────────────────────────────────────────────────

CREATE OR REPLACE TASK ADJUSTMENT_APP.TASK_PROCESS_SENSITIVITY
    WAREHOUSE = DVLP_RAPTOR_WH_XS
    SCHEDULE  = '1 MINUTE'
    COMMENT   = 'Processes eligible Sensitivity adjustments. Stream-triggered via STREAM_QUEUE_SENSITIVITY.'
    WHEN SYSTEM$STREAM_HAS_DATA('ADJUSTMENT_APP.STREAM_QUEUE_SENSITIVITY')
AS
    CALL ADJUSTMENT_APP.SP_RUN_PIPELINE('Sensitivity', '["Sensitivity"]');


-- ═══════════════════════════════════════════════════════════════════════════
-- RESUME ALL
-- CREATE OR REPLACE leaves tasks in Suspended state — must explicitly resume.
-- ═══════════════════════════════════════════════════════════════════════════

ALTER TASK ADJUSTMENT_APP.TASK_PROCESS_VAR         RESUME;
ALTER TASK ADJUSTMENT_APP.TASK_PROCESS_STRESS       RESUME;
ALTER TASK ADJUSTMENT_APP.TASK_PROCESS_FRTB         RESUME;
ALTER TASK ADJUSTMENT_APP.TASK_PROCESS_SENSITIVITY  RESUME;

-- ═══════════════════════════════════════════════════════════════════════════
-- VERIFY
-- ═══════════════════════════════════════════════════════════════════════════
SHOW TASKS LIKE 'TASK_PROCESS_%' IN SCHEMA ADJUSTMENT_APP;
