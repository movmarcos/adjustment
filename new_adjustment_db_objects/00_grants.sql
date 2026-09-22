-- =============================================================================
-- 00_GRANTS.SQL
-- Establishes the privileges the engine and the read-only app role need.
-- Before this file, `grep GRANT new_adjustment_db_objects/*.sql` returned only
-- two comment lines (01:15, 01:936) and deploy.py granted USAGE ON STREAMLIT
-- and nothing else — a fresh environment failed at the first write because
-- nothing had ever established the underlying table/procedure privileges.
--
-- deploy.py runs files in filename-sorted order (glob.glob + sorted()), so
-- this file — 00 — runs BEFORE 01_tables.sql creates every other object in
-- ADJUSTMENT_APP. Two consequences of that ordering, both handled below:
--
--   1. Every statement in the AUTO-EXECUTED section only touches the
--      ADJUSTMENT_APP schema itself, which THIS file creates defensively
--      (CREATE SCHEMA IF NOT EXISTS, same idempotent form 01_tables.sql also
--      uses) before granting on it. That makes it safe to run first even
--      against a brand-new, never-deployed database: there is no ordering
--      dependency on 01 having run yet, and re-running this file on every
--      later deploy is a no-op (GRANT is idempotent; CREATE SCHEMA IF NOT
--      EXISTS is a no-op once 01 has created it for real).
--   2. The engine and the workflow procedures (03, 04, 05, 12) ALSO read and
--      write tables/sequences/procedures in the DIMENSION, FACT, BATCH,
--      RAVEN and METADATA schemas (see the table in the audit this file
--      implements — C-db-objects.md I13). Those schemas are NOT created by
--      this repo (grep CREATE SCHEMA new_adjustment_db_objects/*.sql — only
--      ADJUSTMENT_APP appears) and {{ROLE_OWNER}} does not own them, so the
--      deploy role most likely lacks GRANT authority there. Rather than emit
--      statements that fail on every deploy (harmless but noisy — deploy.py
--      tolerates per-statement failures and keeps going), those grants are
--      documented below as a PREREQUISITE to be run ONCE by whoever owns
--      those schemas (or ACCOUNTADMIN) — the same pattern already used for
--      the READ SESSION prerequisite in 01_tables.sql:13-18.
-- =============================================================================

-- ═══════════════════════════════════════════════════════════════════════════
-- PREREQUISITE (run ONCE, by the owner of DIMENSION / FACT / BATCH / RAVEN /
-- METADATA, or ACCOUNTADMIN — {{ROLE_OWNER}} does not own those schemas and
-- this deploy role cannot be relied on to have GRANT authority over them):
--
--   -- DIMENSION.ADJUSTMENT is written by SP_SUBMIT_ADJUSTMENT (soft-delete)
--   -- and by the engine when it registers a processed adjustment.
--   GRANT SELECT, INSERT, UPDATE ON TABLE {{DATABASE}}.DIMENSION.ADJUSTMENT TO ROLE {{ROLE_OWNER}};
--   -- Every other DIMENSION table (ENTITY, BOOK, TRADE, ...) the engine and
--   -- the Direct/FRTB validation views resolve codes against — read-only.
--   -- FUTURE TABLES covers a dimension added later with no repo DDL change.
--   GRANT SELECT ON ALL TABLES IN SCHEMA {{DATABASE}}.DIMENSION TO ROLE {{ROLE_OWNER}};
--   GRANT SELECT ON FUTURE TABLES IN SCHEMA {{DATABASE}}.DIMENSION TO ROLE {{ROLE_OWNER}};
--
--   -- The engine's write targets: one *_ADJUSTMENT (+ *_ADJUSTMENT_SUMMARY
--   -- where the scope has one) table per ADJUSTMENTS_SETTINGS row. Table-
--   -- level, not schema-wide — FACT also holds the official *_MEASURES /
--   -- *_COMBINED / *_ADJUSTED tables the engine must only ever SELECT from,
--   -- never write to. Adding a new scope's ADJUSTMENTS_SETTINGS row (the
--   -- "no code changes" promise at 01_tables.sql's SEED DATA section) does
--   -- need one GRANT added here alongside it.
--   GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {{DATABASE}}.FACT.VAR_MEASURES_ADJUSTMENT            TO ROLE {{ROLE_OWNER}};
--   GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {{DATABASE}}.FACT.VAR_MEASURES_ADJUSTMENT_SUMMARY    TO ROLE {{ROLE_OWNER}};
--   GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {{DATABASE}}.FACT.STRESS_MEASURES_ADJUSTMENT         TO ROLE {{ROLE_OWNER}};
--   GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {{DATABASE}}.FACT.STRESS_MEASURES_ADJUSTMENT_SUMMARY TO ROLE {{ROLE_OWNER}};
--   GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {{DATABASE}}.FACT.SENSITIVITY_MEASURES_ADJUSTMENT         TO ROLE {{ROLE_OWNER}};
--   GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {{DATABASE}}.FACT.SENSITIVITY_MEASURES_ADJUSTMENT_SUMMARY TO ROLE {{ROLE_OWNER}};
--   GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {{DATABASE}}.FACT.FRTBSA_SENSITIVITY_MEASURES_ADJUSTMENT  TO ROLE {{ROLE_OWNER}};
--   GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {{DATABASE}}.FACT.FRTBSA_DRC_MEASURES_ADJUSTMENT           TO ROLE {{ROLE_OWNER}};
--   GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {{DATABASE}}.FACT.FRTBSA_RRAO_MEASURES_ADJUSTMENT          TO ROLE {{ROLE_OWNER}};
--   -- Read-only access to the official/combined/adjusted fact tables the
--   -- engine and the Preview procedure join against (FUTURE covers a new
--   -- scope's base table with no repo DDL change).
--   GRANT SELECT ON ALL TABLES IN SCHEMA {{DATABASE}}.FACT TO ROLE {{ROLE_OWNER}};
--   GRANT SELECT ON FUTURE TABLES IN SCHEMA {{DATABASE}}.FACT TO ROLE {{ROLE_OWNER}};
--   -- The Scale leg stages its delta in a transient table inside FACT
--   -- (05_sp_process_adjustment.sql _scale_temp — see audit item I17; still
--   -- FACT-schema as of this batch, I17 is tracked separately).
--   GRANT CREATE TABLE ON SCHEMA {{DATABASE}}.FACT TO ROLE {{ROLE_OWNER}};
--
--   -- Run-log plumbing the engine calls into on every processing run.
--   GRANT USAGE ON SEQUENCE {{DATABASE}}.BATCH.SEQ_RUN_LOG TO ROLE {{ROLE_OWNER}};
--   -- Exact signatures are owned by the BATCH team and not visible from this
--   -- repo — confirm with them before running (a wrong signature just fails
--   -- the GRANT harmlessly; it does not grant the wrong thing).
--   GRANT USAGE ON PROCEDURE {{DATABASE}}.BATCH.LOAD_RUN_LOG(<signature>) TO ROLE {{ROLE_OWNER}};
--   GRANT USAGE ON PROCEDURE {{DATABASE}}.BATCH.LOAD_RUN_LOG_END_WITH_DETAIL(<signature>) TO ROLE {{ROLE_OWNER}};
--
--   -- Sensitivity/FRTB dbt hand-off trigger row (05:335 INSERT; also read
--   -- back by VW_REPORT_REFRESH_STATUS / VW_ADJUSTMENT_TRACK in 08_views.sql,
--   -- which is why SELECT is included here too, alongside the INSERT I13
--   -- itself calls out).
--   GRANT SELECT, INSERT ON TABLE {{DATABASE}}.RAVEN.LOG_STAGE_ME_STATUS TO ROLE {{ROLE_OWNER}};
--
--   -- VaR/Stress PowerBI hand-off: the trigger procedure, and the action
--   -- table VW_REPORT_REFRESH_STATUS reads back to report refresh status.
--   GRANT USAGE ON PROCEDURE {{DATABASE}}.FACT.UPDATE_POWERBI_FOR_ADJUSTMENTS(<signature>) TO ROLE {{ROLE_OWNER}};
--   GRANT SELECT ON TABLE {{DATABASE}}.METADATA.POWERBI_ACTION TO ROLE {{ROLE_OWNER}};
--
--   -- Sign-off feed (12_sp_workflow.sql SP_REQUEST_SIGNOFF_CHANGE /
--   -- SP_DECIDE_SIGNOFF_CHANGE / SP_PROPAGATE_SIGNOFF_FEED; 10_sp_signoff_
--   -- sync.sql SP_SYNC_SIGNOFF_STATUS). Table name is configurable via
--   -- ADJ_APP_CONFIG.SIGNOFF_FEED_TABLE — this is the default.
--   GRANT SELECT, INSERT, UPDATE ON TABLE {{DATABASE}}.BATCH.PUBLISH_SIGNOFF_STATUS TO ROLE {{ROLE_OWNER}};
-- ═══════════════════════════════════════════════════════════════════════════

-- ═══════════════════════════════════════════════════════════════════════════
-- AUTO-EXECUTED — runs on every deploy, as {{ROLE_OWNER}} (deploy.py's
-- session role). Touches ONLY the ADJUSTMENT_APP schema, which {{ROLE_OWNER}}
-- owns (it is the role that creates every object in 01-15), so no cross-
-- schema GRANT authority is required and there is no ordering hazard even
-- on a brand-new database.
-- ═══════════════════════════════════════════════════════════════════════════

-- Defensive: makes every grant below valid even if this file runs before
-- 01_tables.sql has created the schema (00 sorts first). Same idempotent
-- form 01_tables.sql itself uses — a no-op once 01 has run for real.
CREATE SCHEMA IF NOT EXISTS {{DATABASE}}.ADJUSTMENT_APP;

-- {{ROLE_RO}} — read-only app viewers (granted USAGE ON STREAMLIT by
-- deploy.py's deploy_streamlit_app()). Streamlit pages query several
-- ADJUSTMENT_APP tables directly as well as the VW_* views (e.g. ADJ_HEADER,
-- ADJ_SIGNOFF_STATUS, ADJ_APP_CONFIG, ADJ_ADMINS, DIRECT_SCOPE_SCHEMA), and
-- those queries run under the viewer's own role — not EXECUTE AS CALLER
-- procedure context — so SELECT is needed on both tables and views.
GRANT USAGE ON DATABASE {{DATABASE}} TO ROLE {{ROLE_RO}};
GRANT USAGE ON SCHEMA {{DATABASE}}.ADJUSTMENT_APP TO ROLE {{ROLE_RO}};
GRANT SELECT ON ALL TABLES IN SCHEMA {{DATABASE}}.ADJUSTMENT_APP TO ROLE {{ROLE_RO}};
GRANT SELECT ON FUTURE TABLES IN SCHEMA {{DATABASE}}.ADJUSTMENT_APP TO ROLE {{ROLE_RO}};
GRANT SELECT ON ALL VIEWS IN SCHEMA {{DATABASE}}.ADJUSTMENT_APP TO ROLE {{ROLE_RO}};
GRANT SELECT ON FUTURE VIEWS IN SCHEMA {{DATABASE}}.ADJUSTMENT_APP TO ROLE {{ROLE_RO}};

-- ═══════════════════════════════════════════════════════════════════════════
-- VERIFY
-- ═══════════════════════════════════════════════════════════════════════════
SHOW GRANTS TO ROLE {{ROLE_RO}};
