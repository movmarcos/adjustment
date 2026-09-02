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
-- Compute: serverless (USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE). Snowflake
-- bills the time each run actually executes. An empty poll is NOT free by
-- itself: SP_RUN_PIPELINE is a Python proc with ~7-16s sandbox cold-start,
-- billed at task size every minute. Each pipeline task body therefore gates
-- the CALL behind a sub-second Snowflake-Scripting EXISTS probe (2026-09) —
-- Python only starts when there is work. Do NOT replace the gate with a
-- stream WHEN clause: tried before, adjustments did not start (Marcos).
-- VaR and Sensitivity are LARGE: a VaR Entity Roll (e.g. entity MUSI) moves
-- ~900M rows per leg — on MEDIUM it overran the task timeout and was killed
-- mid-run, leaving the adjustment stuck in Running.
--
-- USER_TASK_TIMEOUT_MS is set explicitly to 3h (default is 1h): a long Entity
-- Roll must finish rather than be killed. Safe with the 1-minute schedule —
-- tasks never overlap themselves (ALLOW_OVERLAPPING_EXECUTION defaults FALSE),
-- so a long run just makes the next poll wait. Keep this BELOW
-- SP_RUN_PIPELINE's STALE_RUNNING_MINUTES (240) so the reaper can never fire
-- on a run the task might still legitimately be executing.
--
-- SUSPEND_TASK_AFTER_NUM_FAILURES = 0 disables Snowflake's auto-suspend
-- (default: 10 consecutive failures silently halt the task — the pipeline
-- would stop with zero signal and Pending work would pile up forever).
-- Failures stay visible on the Admin page's System Health tab instead.
--
-- FRTB pipeline covers: FRTB, FRTBDRC, FRTBRRAO. (FRTBALL is retired — the app
-- fans an "All FRTB" request out into three sibling adjustments at submit time.)
-- =============================================================================

USE SCHEMA ADJUSTMENT_APP;

-- ─── Cost-attribution tag ───────────────────────────────────────────────────
-- SOLUTION tag on every task: slices serverless-task spend per solution in
-- ACCOUNT_USAGE (join SERVERLESS_TASK_HISTORY to TAG_REFERENCES — see VERIFY).
-- Mechanism (settled by trial on this environment, 2026-09): the tag clause
-- is REJECTED combined with CREATE OR REPLACE, and ALTER TASK ... SET TAG is
-- refused too — so each task is DROP IF EXISTS + CREATE ... WITH TAG (clause
-- directly after the task name). Same end state as OR REPLACE; acceptable
-- because a replaced task loses no state either way. Requires Enterprise
-- Edition; owner needs CREATE TAG once.
-- Values are display-ready ("Adjustment App") — they surface as-is in cost
-- dashboards — so the tag is deliberately free-text (no ALLOWED_VALUES).
CREATE TAG IF NOT EXISTS ADJUSTMENT_APP.SOLUTION
    COMMENT = 'Cost/governance attribution: display name of the solution that owns this object.';
-- Normalize environments deployed while the tag briefly had ALLOWED_VALUES.
ALTER TAG ADJUSTMENT_APP.SOLUTION UNSET ALLOWED_VALUES;

-- ─── VaR ───────────────────────────────────────────────────────────────────

DROP TASK IF EXISTS ADJUSTMENT_APP.TASK_PROCESS_VAR;
CREATE TASK ADJUSTMENT_APP.TASK_PROCESS_VAR
    WITH TAG (ADJUSTMENT_APP.SOLUTION = 'Adjustment App')
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'LARGE'
    USER_TASK_TIMEOUT_MS = 10800000
    SUSPEND_TASK_AFTER_NUM_FAILURES = 0
    SCHEDULE  = '1 MINUTE'
    COMMENT   = 'Every 1 min: SP_RUN_PIPELINE polls and processes eligible VaR adjustments. LARGE: VaR Entity Rolls move ~900M rows/leg.'
AS
BEGIN
    -- COST GATE: the Python proc pays ~7-16s of sandbox cold-start per call,
    -- billed at this task's compute size — even when the queue is empty.
    -- This Snowflake-Scripting probe (same predicate as the proc's own fast
    -- exit, incl. 'Running' so the stale-run reaper still fires) costs
    -- sub-second; Python only runs when there is actual work.
    IF (EXISTS (SELECT 1 FROM ADJUSTMENT_APP.ADJ_HEADER
                WHERE PROCESS_TYPE IN ('VaR')
                  AND RUN_STATUS IN ('Pending', 'Approved', 'Running')
                  AND IS_DELETED = FALSE)) THEN
        CALL ADJUSTMENT_APP.SP_RUN_PIPELINE('VaR', '["VaR"]');
    END IF;
END;


-- ─── Stress ────────────────────────────────────────────────────────────────

DROP TASK IF EXISTS ADJUSTMENT_APP.TASK_PROCESS_STRESS;
CREATE TASK ADJUSTMENT_APP.TASK_PROCESS_STRESS
    WITH TAG (ADJUSTMENT_APP.SOLUTION = 'Adjustment App')
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'MEDIUM'
    USER_TASK_TIMEOUT_MS = 10800000
    SUSPEND_TASK_AFTER_NUM_FAILURES = 0
    SCHEDULE  = '1 MINUTE'
    COMMENT   = 'Every 1 min: SP_RUN_PIPELINE polls and processes eligible Stress adjustments.'
AS
BEGIN
    -- COST GATE: the Python proc pays ~7-16s of sandbox cold-start per call,
    -- billed at this task's compute size — even when the queue is empty.
    -- This Snowflake-Scripting probe (same predicate as the proc's own fast
    -- exit, incl. 'Running' so the stale-run reaper still fires) costs
    -- sub-second; Python only runs when there is actual work.
    IF (EXISTS (SELECT 1 FROM ADJUSTMENT_APP.ADJ_HEADER
                WHERE PROCESS_TYPE IN ('Stress')
                  AND RUN_STATUS IN ('Pending', 'Approved', 'Running')
                  AND IS_DELETED = FALSE)) THEN
        CALL ADJUSTMENT_APP.SP_RUN_PIPELINE('Stress', '["Stress"]');
    END IF;
END;


-- ─── FRTB (all sub-types) ───────────────────────────────────────────────────

DROP TASK IF EXISTS ADJUSTMENT_APP.TASK_PROCESS_FRTB;
CREATE TASK ADJUSTMENT_APP.TASK_PROCESS_FRTB
    WITH TAG (ADJUSTMENT_APP.SOLUTION = 'Adjustment App')
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'MEDIUM'
    USER_TASK_TIMEOUT_MS = 10800000
    SUSPEND_TASK_AFTER_NUM_FAILURES = 0
    SCHEDULE  = '1 MINUTE'
    COMMENT   = 'Every 1 min: SP_RUN_PIPELINE polls and processes eligible FRTB-pipeline adjustments (FRTB, FRTBDRC, FRTBRRAO).'
AS
BEGIN
    -- COST GATE: the Python proc pays ~7-16s of sandbox cold-start per call,
    -- billed at this task's compute size — even when the queue is empty.
    -- This Snowflake-Scripting probe (same predicate as the proc's own fast
    -- exit, incl. 'Running' so the stale-run reaper still fires) costs
    -- sub-second; Python only runs when there is actual work.
    IF (EXISTS (SELECT 1 FROM ADJUSTMENT_APP.ADJ_HEADER
                WHERE PROCESS_TYPE IN ('FRTB', 'FRTBDRC', 'FRTBRRAO')
                  AND RUN_STATUS IN ('Pending', 'Approved', 'Running')
                  AND IS_DELETED = FALSE)) THEN
        CALL ADJUSTMENT_APP.SP_RUN_PIPELINE('FRTB', '["FRTB","FRTBDRC","FRTBRRAO"]');
    END IF;
END;


-- ─── Sensitivity ───────────────────────────────────────────────────────────

DROP TASK IF EXISTS ADJUSTMENT_APP.TASK_PROCESS_SENSITIVITY;
CREATE TASK ADJUSTMENT_APP.TASK_PROCESS_SENSITIVITY
    WITH TAG (ADJUSTMENT_APP.SOLUTION = 'Adjustment App')
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'LARGE'
    USER_TASK_TIMEOUT_MS = 10800000
    SUSPEND_TASK_AFTER_NUM_FAILURES = 0
    SCHEDULE  = '1 MINUTE'
    COMMENT   = 'Every 1 min: SP_RUN_PIPELINE polls and processes eligible Sensitivity adjustments.'
AS
BEGIN
    -- COST GATE: the Python proc pays ~7-16s of sandbox cold-start per call,
    -- billed at this task's compute size — even when the queue is empty.
    -- This Snowflake-Scripting probe (same predicate as the proc's own fast
    -- exit, incl. 'Running' so the stale-run reaper still fires) costs
    -- sub-second; Python only runs when there is actual work.
    IF (EXISTS (SELECT 1 FROM ADJUSTMENT_APP.ADJ_HEADER
                WHERE PROCESS_TYPE IN ('Sensitivity')
                  AND RUN_STATUS IN ('Pending', 'Approved', 'Running')
                  AND IS_DELETED = FALSE)) THEN
        CALL ADJUSTMENT_APP.SP_RUN_PIPELINE('Sensitivity', '["Sensitivity"]');
    END IF;
END;


-- ─── Sign-off sync — every 30 minutes ───────────────────────────────────────
-- Materialises upstream publish sign-offs (BATCH.PUBLISH_VAR_SIGNOFF_STATUS /
-- PUBLISH_SIGNOFF_STATUS_EXCEPTION) into ADJ_SIGNOFF_STATUS so the app can
-- drive the re-open / re-sign-off lifecycle. Proc: 10_sp_signoff_sync.sql.
-- The task resolves its CALL target at execution time, so 06 deploying before
-- 10 is fine — deploy.py resumes all tasks after every object is deployed.

DROP TASK IF EXISTS ADJUSTMENT_APP.TASK_SYNC_SIGNOFF;
CREATE TASK ADJUSTMENT_APP.TASK_SYNC_SIGNOFF
    WITH TAG (ADJUSTMENT_APP.SOLUTION = 'Adjustment App')
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    SUSPEND_TASK_AFTER_NUM_FAILURES = 0
    SCHEDULE  = '30 MINUTE'
    COMMENT   = 'Every 30 min: sync upstream publish sign-offs into ADJ_SIGNOFF_STATUS.'
AS
    CALL ADJUSTMENT_APP.SP_SYNC_SIGNOFF_STATUS();


-- Self-resume: a freshly created task is always SUSPENDED — running
-- this file standalone (worksheet hotfix, partial deploy) used to silently
-- halt every pipeline until someone remembered the separate resume step.
-- deploy.py's resume_pipeline_tasks step is now a harmless double-resume.
ALTER TASK ADJUSTMENT_APP.TASK_PROCESS_VAR         RESUME;
ALTER TASK ADJUSTMENT_APP.TASK_PROCESS_STRESS      RESUME;
ALTER TASK ADJUSTMENT_APP.TASK_PROCESS_FRTB        RESUME;
ALTER TASK ADJUSTMENT_APP.TASK_PROCESS_SENSITIVITY RESUME;
ALTER TASK ADJUSTMENT_APP.TASK_SYNC_SIGNOFF        RESUME;

-- ═══════════════════════════════════════════════════════════════════════════
-- VERIFY
-- ═══════════════════════════════════════════════════════════════════════════
SHOW TASKS LIKE 'TASK_%' IN SCHEMA ADJUSTMENT_APP;

-- Tags applied (live, no lag):
SELECT TAG_NAME, TAG_VALUE
FROM TABLE(INFORMATION_SCHEMA.TAG_REFERENCES(
    'ADJUSTMENT_APP.TASK_SYNC_SIGNOFF', 'TASK'));

-- Cost attribution per solution (ACCOUNT_USAGE, up to ~2h behind):
-- serverless task credits for the last 30 days, grouped by SOLUTION tag.
-- SELECT tr.TAG_VALUE AS SOLUTION,
--        SUM(sth.CREDITS_USED) AS CREDITS_30D
-- FROM SNOWFLAKE.ACCOUNT_USAGE.SERVERLESS_TASK_HISTORY sth
-- JOIN SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES tr
--   ON tr.OBJECT_ID = sth.TASK_ID AND tr.DOMAIN = 'TASK'
--  AND tr.TAG_NAME = 'SOLUTION'
-- WHERE sth.START_TIME >= DATEADD(day, -30, CURRENT_TIMESTAMP())
-- GROUP BY tr.TAG_VALUE;
