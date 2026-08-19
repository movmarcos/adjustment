-- =============================================================================
-- 10_SP_SIGNOFF_SYNC.SQL
-- Syncs the FIRST sign-off from the upstream publish system into the app.
--
-- The COB sign-off is owned by another system shared across many consumers.
-- The unified feed table (ADJ_APP_CONFIG.SIGNOFF_FEED_TABLE, default
-- BATCH.PUBLISH_SIGNOFF_STATUS) carries one row per COBID + ENTITY_CODE +
-- PROCESS_TYPE (+ SUB_TYPE):
--
--   COBID, ENTITY_CODE, PROCESS_TYPE, SUB_TYPE, PUBLISH_STATUS, SIGNOFF_UPDATE_TIME
--   20260608, MUSI,  FRTB, NonCVA, InProgress, ...
--   20260608, MUSUA, VaR,  NULL,   SignedOff,  2026-06-09 17:09:11
--
-- Sync rules (deliberately one-way and non-destructive):
--   • A 'SignedOff' feed row → SIGNED_OFF app row per COBID + scope +
--     SUB_TYPE + ENTITY, SIGNOFF_SOURCE = 'EXTERNAL'. SUB_TYPE is an extra
--     granularity dimension carried verbatim (NULL/'' = none) — no rows are
--     filtered out.
--   • EXACT process-type match: the feed's 'FRTB' row is SBM only;
--     FRTBDRC/FRTBRRAO sync in only when the feed carries those process
--     types itself (no fan-out).
--   • Rows already in the app lifecycle (SIGNED_OFF, REOPEN_REQUESTED,
--     REOPENED) are NEVER touched — an approved in-app re-open must not be
--     clobbered by the next sync run.
--   • Only COBs within LOOKBACK_DAYS are synced.
--
-- Note: SP_SUBMIT_ADJUSTMENT ALSO checks the feed live, so the gate holds
-- even between sync runs — this sync exists so users can SEE the sign-off
-- state in the app and request a re-open against a concrete row. While the
-- feed table is being migrated, set ADJ_APP_CONFIG.SIGNOFF_FEED_ENABLED =
-- 'false' and both the sync and the live check stand down.
--
--   CALL ADJUSTMENT_APP.SP_SYNC_SIGNOFF_STATUS();
-- =============================================================================

USE SCHEMA ADJUSTMENT_APP;

CREATE OR ALTER PROCEDURE ADJUSTMENT_APP.SP_SYNC_SIGNOFF_STATUS()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
COMMENT = 'Materialises the unified upstream publish sign-off feed into ADJ_SIGNOFF_STATUS as SIGNED_OFF/EXTERNAL rows per COB + scope + entity (EXACT process-type match — no FRTB fan-out). Never overwrites the in-app re-open lifecycle.'
EXECUTE AS CALLER
AS
$$
import json

LOOKBACK_DAYS = 180

# app scope → predicate on the feed's PROCESS_TYPE.
# EXACT match per scope (Marcos, 2026-08): the feed's 'FRTB' row means SBM
# only — FRTBDRC/FRTBRRAO appear in the app ONLY when the publish table
# carries those process types itself. No fan-out.
SCOPE_MATCH = {
    'VaR':         "UPPER(u.PROCESS_TYPE) = 'VAR'",
    'Stress':      "UPPER(u.PROCESS_TYPE) = 'STRESS'",
    'Sensitivity': "UPPER(u.PROCESS_TYPE) = 'SENSITIVITY'",
    'FRTB':        "UPPER(u.PROCESS_TYPE) = 'FRTB'",
    'FRTBDRC':     "UPPER(u.PROCESS_TYPE) = 'FRTBDRC'",
    'FRTBRRAO':    "UPPER(u.PROCESS_TYPE) = 'FRTBRRAO'",
}


def _cfg(session, key, default=""):
    rows = session.sql(f"""
        SELECT CONFIG_VALUE FROM ADJUSTMENT_APP.ADJ_APP_CONFIG
        WHERE CONFIG_KEY = '{key}'
    """).collect()
    v = rows[0]["CONFIG_VALUE"] if rows else None
    return str(v) if v is not None else default


def main(session):
    if _cfg(session, "SIGNOFF_FEED_ENABLED", "true").strip().lower() != "true":
        return json.dumps({"status": "skipped",
                           "message": "SIGNOFF_FEED_ENABLED is false — feed "
                                      "sync is paused (migration in progress)."})
    feed = _cfg(session, "SIGNOFF_FEED_TABLE",
                "BATCH.PUBLISH_SIGNOFF_STATUS").strip()

    min_cobid = session.sql(
        f"SELECT TO_NUMBER(TO_CHAR(DATEADD(day, -{LOOKBACK_DAYS}, CURRENT_DATE()), 'YYYYMMDD')) AS C"
    ).collect()[0]["C"]

    summary = {"feed": feed, "lookback_min_cobid": int(min_cobid), "synced": {}}

    for scope, pt_match in SCOPE_MATCH.items():
        # ── Open COBs: every feed row NOT signed off materialises as an OPEN
        # app row (insert-only — an existing row of ANY status is never
        # downgraded). This is what makes "which COBs are open" visible in
        # the app; sign-off requests are raised against these rows.
        open_src = f"""
            SELECT DISTINCT u.COBID, UPPER(TRIM(u.ENTITY_CODE)) AS ENTITY_CODE,
                   NULLIF(TRIM(u.SUB_TYPE), '') AS SUB_TYPE
            FROM {feed} u
            WHERE {pt_match}
              AND UPPER(u.PUBLISH_STATUS) <> 'SIGNEDOFF'
              AND u.ENTITY_CODE IS NOT NULL AND TRIM(u.ENTITY_CODE) <> ''
              AND u.COBID >= {int(min_cobid)}
        """
        opened = session.sql(f"""
            INSERT INTO ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS
                (COBID, PROCESS_TYPE, ENTITY_CODE, SUB_TYPE,
                 SIGN_OFF_STATUS, SIGNOFF_SOURCE)
            SELECT s.COBID, '{scope}', s.ENTITY_CODE, s.SUB_TYPE,
                   'OPEN', 'EXTERNAL'
            FROM ({open_src}) s
            WHERE NOT EXISTS (
                SELECT 1 FROM ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS a
                WHERE a.COBID = s.COBID
                  AND UPPER(a.PROCESS_TYPE) = '{scope.upper()}'
                  AND UPPER(a.ENTITY_CODE) = s.ENTITY_CODE
                  AND COALESCE(UPPER(a.SUB_TYPE), '') = COALESCE(UPPER(s.SUB_TYPE), '')
            )
        """).collect()
        try:
            summary.setdefault("opened", {})[scope] = int(opened[0][0]) if opened else 0
        except (TypeError, ValueError, IndexError):
            summary.setdefault("opened", {})[scope] = 0

        src_sql = f"""
            SELECT DISTINCT u.COBID, UPPER(TRIM(u.ENTITY_CODE)) AS ENTITY_CODE,
                   NULLIF(TRIM(u.SUB_TYPE), '') AS SUB_TYPE
            FROM {feed} u
            WHERE {pt_match}
              AND UPPER(u.PUBLISH_STATUS) = 'SIGNEDOFF'
              AND u.ENTITY_CODE IS NOT NULL AND TRIM(u.ENTITY_CODE) <> ''
              AND u.COBID >= {int(min_cobid)}
        """

        # Rows this sync will create or flip (same predicate as the MERGE) —
        # captured first so the history insert records exactly what changed.
        # LEFT JOIN, not a correlated scalar subquery: Snowflake rejects
        # correlated subqueries in select-list expressions ("Unsupported
        # subquery type cannot be evaluated"). (COBID, PROCESS_TYPE,
        # ENTITY_CODE) is the table's key, so the join cannot fan out.
        to_change = session.sql(f"""
            SELECT s.COBID, s.ENTITY_CODE, s.SUB_TYPE,
                   a.SIGN_OFF_STATUS AS OLD_STATUS
            FROM ({src_sql}) s
            LEFT JOIN ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS a
              ON a.COBID = s.COBID
             AND UPPER(a.PROCESS_TYPE) = '{scope.upper()}'
             AND UPPER(a.ENTITY_CODE) = s.ENTITY_CODE
             AND COALESCE(UPPER(a.SUB_TYPE), '') = COALESCE(UPPER(s.SUB_TYPE), '')
            WHERE a.COBID IS NULL
               OR UPPER(a.SIGN_OFF_STATUS) = 'OPEN'
        """).collect()

        if not to_change:
            summary["synced"][scope] = 0
            continue

        session.sql(f"""
            MERGE INTO ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS t
            USING ({src_sql}) s
            ON t.COBID = s.COBID
               AND UPPER(t.PROCESS_TYPE) = '{scope.upper()}'
               AND UPPER(t.ENTITY_CODE) = s.ENTITY_CODE
               AND COALESCE(UPPER(t.SUB_TYPE), '') = COALESCE(UPPER(s.SUB_TYPE), '')
            WHEN MATCHED AND UPPER(t.SIGN_OFF_STATUS) = 'OPEN' THEN UPDATE SET
                t.SIGN_OFF_STATUS    = 'SIGNED_OFF',
                t.SIGN_OFF_BY        = 'EXTERNAL FEED',
                t.SIGN_OFF_TIMESTAMP = CURRENT_TIMESTAMP(),
                t.SIGNOFF_SOURCE     = 'EXTERNAL',
                t.UPDATED_DATE       = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT
                (COBID, PROCESS_TYPE, ENTITY_CODE, SUB_TYPE, SIGN_OFF_STATUS,
                 SIGN_OFF_BY, SIGN_OFF_TIMESTAMP, SIGNOFF_SOURCE)
            VALUES
                (s.COBID, '{scope}', s.ENTITY_CODE, s.SUB_TYPE, 'SIGNED_OFF',
                 'EXTERNAL FEED', CURRENT_TIMESTAMP(), 'EXTERNAL')
        """).collect()

        def _hv(v):
            return ("NULL" if v is None else
                    "'" + str(v).replace(chr(92), chr(92) * 2)
                                .replace(chr(39), chr(39) * 2) + "'")
        hist_values = ", ".join(
            f"({int(r['COBID'])}, '{scope}', {_hv(r['ENTITY_CODE'])}, "
            f"{_hv(r['SUB_TYPE'])}, {_hv(r['OLD_STATUS'])}, "
            "'SIGNED_OFF', 'EXTERNAL FEED', "
            "'Signed off by the upstream publish system (synced)')"
            for r in to_change)
        session.sql(f"""
            INSERT INTO ADJUSTMENT_APP.ADJ_SIGNOFF_HISTORY
                (COBID, PROCESS_TYPE, ENTITY_CODE, SUB_TYPE, OLD_STATUS,
                 NEW_STATUS, ACTION_BY, COMMENT)
            VALUES {hist_values}
        """).collect()

        summary["synced"][scope] = len(to_change)

    return json.dumps(summary)
$$;

-- NOTE: the 30-minute TASK_SYNC_SIGNOFF that drives this procedure is defined
-- with the other tasks in 06_tasks.sql.

-- ═══════════════════════════════════════════════════════════════════════════
-- VERIFY
-- ═══════════════════════════════════════════════════════════════════════════
DESCRIBE PROCEDURE ADJUSTMENT_APP.SP_SYNC_SIGNOFF_STATUS();
