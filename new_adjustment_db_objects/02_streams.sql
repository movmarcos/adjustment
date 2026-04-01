-- =============================================================================
-- 02_STREAMS.SQL
-- Queue views (one per scope) + streams on those views.
--
-- Views must be created before streams — both live here to enforce that order.
--
-- APPEND_ONLY = TRUE: we only care about rows becoming eligible (appearing
-- in the view). Rows leaving the view (claimed as Running) are not tracked.
--
-- PREREQUISITE: 01_tables.sql must run first (BLOCKED_BY_ADJ_ID column +
--               CHANGE_TRACKING = TRUE on ADJ_HEADER).
-- =============================================================================

USE DATABASE DVLP_RAPTOR_NEWADJ;
USE SCHEMA ADJUSTMENT_APP;

-- ═══════════════════════════════════════════════════════════════════════════
-- PART 1 — QUEUE VIEWS
--
-- Show only adjustments that are eligible to be picked up by a pipeline task:
--   • Pending (not yet claimed)
--   • Not blocked (BLOCKED_BY_ADJ_ID IS NULL)
--   • Not soft-deleted
--
-- CHANGE_TRACKING on ADJ_HEADER (01_tables.sql) enables APPEND_ONLY streams
-- on these simple single-table filtered views.
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW ADJUSTMENT_APP.VW_QUEUE_VAR
    COMMENT = 'Eligible VaR adjustments: Pending + unblocked. Stream source for TASK_PROCESS_VAR.'
AS
SELECT * FROM ADJUSTMENT_APP.ADJ_HEADER
WHERE PROCESS_TYPE = 'VaR'
  AND RUN_STATUS = 'Pending'
  AND BLOCKED_BY_ADJ_ID IS NULL
  AND IS_DELETED = FALSE;

CREATE OR REPLACE VIEW ADJUSTMENT_APP.VW_QUEUE_STRESS
    COMMENT = 'Eligible Stress adjustments: Pending + unblocked. Stream source for TASK_PROCESS_STRESS.'
AS
SELECT * FROM ADJUSTMENT_APP.ADJ_HEADER
WHERE PROCESS_TYPE = 'Stress'
  AND RUN_STATUS = 'Pending'
  AND BLOCKED_BY_ADJ_ID IS NULL
  AND IS_DELETED = FALSE;

CREATE OR REPLACE VIEW ADJUSTMENT_APP.VW_QUEUE_FRTB
    COMMENT = 'Eligible FRTB-pipeline adjustments (FRTB + FRTBDRC + FRTBRRAO + FRTBALL): Pending + unblocked.'
AS
SELECT * FROM ADJUSTMENT_APP.ADJ_HEADER
WHERE PROCESS_TYPE IN ('FRTB', 'FRTBDRC', 'FRTBRRAO', 'FRTBALL')
  AND RUN_STATUS = 'Pending'
  AND BLOCKED_BY_ADJ_ID IS NULL
  AND IS_DELETED = FALSE;

CREATE OR REPLACE VIEW ADJUSTMENT_APP.VW_QUEUE_SENSITIVITY
    COMMENT = 'Eligible Sensitivity adjustments: Pending + unblocked. Stream source for TASK_PROCESS_SENSITIVITY.'
AS
SELECT * FROM ADJUSTMENT_APP.ADJ_HEADER
WHERE PROCESS_TYPE = 'Sensitivity'
  AND RUN_STATUS = 'Pending'
  AND BLOCKED_BY_ADJ_ID IS NULL
  AND IS_DELETED = FALSE;


-- ═══════════════════════════════════════════════════════════════════════════
-- PART 2 — STREAMS (one per queue view)
-- ═══════════════════════════════════════════════════════════════════════════

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
