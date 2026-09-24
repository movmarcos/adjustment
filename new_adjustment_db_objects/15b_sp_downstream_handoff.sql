-- =============================================================================
-- 15B_SP_DOWNSTREAM_HANDOFF.SQL
-- Tell the reporting side that a COB's adjustment data changed.
--
-- Processing an adjustment already does this, inside SP_PROCESS_ADJUSTMENT:
--   • VaR / Stress        → CALL FACT.UPDATE_POWERBI_FOR_ADJUSTMENTS, which
--                           queues a row in METADATA.POWERBI_ACTION
--   • Sensitivity / FRTB* → a DUMMY_* row in RAVEN.LOG_STAGE_ME_STATUS that
--                           Control-M polls to start the dbt rebuild
--
-- DELETING an adjustment did neither (reported 2026-09-24). The rows left the
-- warehouse immediately but nothing downstream was told, so reports kept
-- showing the deleted numbers until some unrelated adjustment happened to
-- trigger a rebuild. This procedure exists so the Adjustments page can make
-- the same hand-off the engine makes, without reimplementing it.
--
-- WHY A RUN LOG. FACT.UPDATE_POWERBI_FOR_ADJUSTMENTS keys its lookup on a
-- BATCH.RUN_LOG row whose proc_parameters equals the data group name. A
-- delete is not a pipeline run and has no run log of its own, so one is
-- minted here exactly as the engine mints it. Reusing the deleted
-- adjustment's original RUN_LOG_ID was the obvious shortcut and is wrong:
-- that run is closed and describes the insert, not this change.
--
-- Deliberately NOT called by SP_PROCESS_ADJUSTMENT. The engine's own
-- hand-off is entangled with its run log, its step logging and its
-- stamping of failures onto ADJ_HEADER, and rewiring that on a live engine
-- to save a duplicated dictionary is a bad trade. The two scope maps are
-- kept in step by a test instead (test_delete_handoff.py).
-- =============================================================================

USE SCHEMA ADJUSTMENT_APP;

CREATE OR ALTER PROCEDURE ADJUSTMENT_APP.SP_DOWNSTREAM_HANDOFF(
    p_process_type VARCHAR,
    p_cobid        NUMBER,
    p_reason       VARCHAR
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
COMMENT = 'Queue the reporting hand-off for a scope + COB: a PowerBI refresh action for VaR/Stress, a DUMMY_* trigger row in RAVEN.LOG_STAGE_ME_STATUS for Sensitivity/FRTB. Used by the Adjustments page after a delete, so removing data refreshes reports the same way adding it does.'
EXECUTE AS CALLER
AS
$$

# Must match SP_PROCESS_ADJUSTMENT's maps of the same names. A test asserts
# they agree; if you change one, change both.
PBI_INSERT_SOURCE = {
    'VAR':    'LOAD_VAR_ADJUSTMENT',
    'STRESS': 'LOAD_STRESS_ADJUSTMENT',
}
PBI_DATA_GROUP = {
    'VAR':    'VAR',
    'STRESS': 'STRESS',
}
DBT_DUMMY_DATASET = {
    'SENSITIVITY': 'DUMMY_Sensitivity_Adjustment',
    'FRTB':        'DUMMY_FRTB_Adjustment',
    'FRTBDRC':     'DUMMY_FRTB_Adjustment',
    'FRTBRRAO':    'DUMMY_FRTB_Adjustment',
}


def _esc(value):
    """Backslashes doubled BEFORE quotes — the reverse order re-escapes the
    backslashes this function just inserted."""
    if value is None:
        return ""
    return str(value).replace("\\", "\\\\").replace("'", "''")


def main(session, p_process_type, p_cobid, p_reason):
    pt = str(p_process_type or "").strip().upper()
    cobid = int(p_cobid)
    reason = _esc(str(p_reason or "")[:200])

    if pt not in PBI_INSERT_SOURCE and pt not in DBT_DUMMY_DATASET:
        return f"skipped (no downstream hand-off configured for '{p_process_type}')"

    data_group = PBI_DATA_GROUP.get(pt, pt)

    # Mint and open a run log. The PowerBI proc looks its run up by the data
    # group in proc_parameters, so that column has to carry the same value
    # the proc is called with.
    # BYTE-FOR-BYTE the call SP_PROCESS_ADJUSTMENT makes, including the
    # process name and the empty trailing argument. FACT.UPDATE_POWERBI_FOR_
    # ADJUSTMENTS lives outside this repo and cannot be inspected; the only
    # thing known about how it finds a run log is that this exact shape
    # works. An earlier revision put its own process name here and passed a
    # reason string, for tidiness — deviating from a known-working call into
    # a black box bought nothing and risked the hand-off finding no run.
    # The reason lives in the return value instead.
    try:
        run_log_id = session.sql(
            "SELECT BATCH.SEQ_RUN_LOG.NEXTVAL AS X").collect()[0]["X"]
        session.sql(f"""
            CALL BATCH.LOAD_RUN_LOG(
                {run_log_id},
                {cobid},
                'FACT.SP_PROCESS_ADJUSTMENT',
                '{_esc(data_group)}',
                0, 0, 'false', ''
            )
        """).collect()
    except Exception as rl_err:
        return f"failed (could not open a run log: {rl_err})"

    outcome = ""
    _why = f" [{reason}]" if reason else ""
    try:
        if pt in PBI_INSERT_SOURCE:
            res = session.sql(f"""
                CALL FACT.UPDATE_POWERBI_FOR_ADJUSTMENTS(
                    '{_esc(data_group)}',
                    'RaptorReporting',
                    '{_esc(PBI_INSERT_SOURCE[pt])}',
                    '{run_log_id}',
                    '0'
                )
            """).collect()
            # The proc reports problems as a return STRING, not an exception.
            ret = str(res[0][0]) if res and res[0] is not None else ""
            if ret.strip().lower() != "success":
                raise Exception(f"UPDATE_POWERBI_FOR_ADJUSTMENTS returned: {ret}")
            outcome = f"powerbi: queued (run_log={run_log_id}){_why}"
        else:
            dataset = DBT_DUMMY_DATASET[pt]
            session.sql(f"""
                INSERT INTO RAVEN.LOG_STAGE_ME_STATUS
                    (ID, RAVEN_COBID, DATASET_NAME, PROCESS_STATUS,
                     START_TIMESTAMP, END_TIMESTAMP)
                SELECT
                    HASH(CURRENT_TIMESTAMP(), {cobid}, '{_esc(dataset)}', RANDOM()),
                    {cobid},
                    '{_esc(dataset)}',
                    'SUCCESS',
                    CURRENT_TIMESTAMP(),
                    CURRENT_TIMESTAMP()
            """).collect()
            outcome = f"dbt: trigger written ({dataset}){_why}"
    except Exception as hand_err:
        outcome = f"failed ({hand_err})"

    try:
        session.sql(f"""
            CALL BATCH.LOAD_RUN_LOG_END_WITH_DETAIL(
                {run_log_id}, '{{"status":"Processed"}}')
        """).collect()
    except Exception as close_err:
        print(f"Warning: run log close failed: {close_err}")

    return outcome
$$;
