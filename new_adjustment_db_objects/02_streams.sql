-- =============================================================================
-- 02_STREAMS.SQL
-- One stream per scope pipeline, each on its queue view.
--
-- APPEND_ONLY = TRUE: we only care about rows becoming eligible (appearing
-- in the view). Rows leaving the view (claimed as Running) are not tracked.
--
-- PREREQUISITE: Task 1 must run first (CHANGE_TRACKING = TRUE on ADJ_HEADER).
-- PREREQUISITE: Task 2 must run first (VW_QUEUE_* views must exist).
-- =============================================================================

USE DATABASE DVLP_RAPTOR_NEWADJ;
USE SCHEMA ADJUSTMENT_APP;

CREATE OR REPLACE STREAM ADJUSTMENT_APP.STREAM_QUEUE_VAR
    ON VIEW ADJUSTMENT_APP.VW_QUEUE_VAR
    APPEND_ONLY = TRUE
    COMMENT = 'Fires when new eligible VaR adjustments appear (Pending + unblocked). Triggers TASK_PROCESS_VAR.';

CREATE OR REPLACE STREAM ADJUSTMENT_APP.STREAM_QUEUE_STRESS
    ON VIEW ADJUSTMENT_APP.VW_QUEUE_STRESS
    APPEND_ONLY = TRUE
    COMMENT = 'Fires when new eligible Stress adjustments appear. Triggers TASK_PROCESS_STRESS.';

CREATE OR REPLACE STREAM ADJUSTMENT_APP.STREAM_QUEUE_FRTB
    ON VIEW ADJUSTMENT_APP.VW_QUEUE_FRTB
    APPEND_ONLY = TRUE
    COMMENT = 'Fires when new eligible FRTB-pipeline adjustments appear (all sub-types). Triggers TASK_PROCESS_FRTB.';

CREATE OR REPLACE STREAM ADJUSTMENT_APP.STREAM_QUEUE_SENSITIVITY
    ON VIEW ADJUSTMENT_APP.VW_QUEUE_SENSITIVITY
    APPEND_ONLY = TRUE
    COMMENT = 'Fires when new eligible Sensitivity adjustments appear. Triggers TASK_PROCESS_SENSITIVITY.';

-- ═══════════════════════════════════════════════════════════════════════════
-- VERIFY
-- ═══════════════════════════════════════════════════════════════════════════
SHOW STREAMS LIKE 'STREAM_QUEUE_%' IN SCHEMA ADJUSTMENT_APP;
