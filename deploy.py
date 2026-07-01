"""
deploy.py — Deploy all Snowflake objects + Streamlit app (target set in config.py)
================================================================================
Usage:
    python deploy.py                      # Deploy everything (DB objects + Streamlit app)
    python deploy.py --db-only            # Deploy DB objects only
    python deploy.py --streamlit-only     # Deploy Streamlit app only

Prerequisites:
    pip install mufg_snowflakeconn snowflake-snowpark-python
"""
import os
import re
import sys
import glob
import argparse
from pathlib import Path

import config

# ─── Connection ──────────────────────────────────────────────────────────────

def get_session():
    """Create a Snowpark session using the MUFG connector."""
    from mufg_snowflakeconn import sfconnection as m_sf
    mufgconn = m_sf.MufgSnowflakeConn('dvlp', 'apd_raptor_sfk_depl@mufgsecurities.com')
    session = mufgconn.get_snowflake_session()
    session.use_role(config.ROLE_OWNER)
    session.use_warehouse(config.WAREHOUSE)
    session.use_database(config.DATABASE)
    # NB: do not USE SCHEMA here — on a fresh database the schema does not exist
    # yet. 01_tables.sql runs `CREATE SCHEMA IF NOT EXISTS` then `USE SCHEMA`
    # before anything needs it.
    return session


# ─── SQL Parsing ─────────────────────────────────────────────────────────────

def split_sql_statements(sql_text: str) -> list[str]:
    """
    Split a SQL file into individual statements, correctly handling:
      - $$ ... $$ procedure/function bodies
      - DECLARE ... BEGIN ... END scripting blocks (tasks)
      - Standard ; delimiters
      - -- comments
    """
    statements = []
    current = []
    in_dollar_block = False
    in_scripting_block = 0   # nesting depth for BEGIN/END
    in_declare_block = False  # entered DECLARE before BEGIN
    lines = sql_text.split('\n')

    for line in lines:
        stripped = line.strip()

        # Skip pure comment lines and empty lines when not in a block
        if not in_dollar_block and in_scripting_block == 0 and not in_declare_block:
            if stripped == '' or stripped.startswith('--'):
                current.append(line)
                continue

        # Check for $$ toggling
        dollar_count = line.count('$$')
        if dollar_count % 2 == 1:
            in_dollar_block = not in_dollar_block

        current.append(line)

        # If we're inside a $$ block, keep accumulating
        if in_dollar_block:
            continue

        # Track DECLARE ... BEGIN ... END scripting blocks
        # (used by Snowflake tasks with procedural bodies)
        upper_stripped = stripped.upper().rstrip(';')

        if in_scripting_block == 0 and not in_declare_block:
            if upper_stripped == 'DECLARE':
                in_declare_block = True
                continue
            if upper_stripped == 'BEGIN':
                in_scripting_block = 1
                continue

        if in_declare_block:
            if upper_stripped == 'BEGIN':
                in_declare_block = False
                in_scripting_block = 1
            continue

        if in_scripting_block > 0:
            # Nested BEGIN
            if upper_stripped == 'BEGIN':
                in_scripting_block += 1
            # END or END; closes a block
            elif upper_stripped in ('END', 'END;') or re.match(r'^END\s*;', stripped, re.IGNORECASE):
                in_scripting_block -= 1
                if in_scripting_block == 0:
                    # Block fully closed — emit the whole statement
                    stmt_text = '\n'.join(current).strip()
                    statements.append(stmt_text)
                    current = []
            continue

        # Outside $$ and scripting blocks: check if line ends with ;
        if stripped.endswith(';'):
            stmt_text = '\n'.join(current).strip()
            # Filter out pure comment/empty blocks
            non_comment = '\n'.join(
                l for l in current
                if l.strip() and not l.strip().startswith('--')
            ).strip()
            if non_comment:
                statements.append(stmt_text)
            current = []

    # Any remaining text
    if current:
        stmt_text = '\n'.join(current).strip()
        non_comment = '\n'.join(
            l for l in current
            if l.strip() and not l.strip().startswith('--')
        ).strip()
        if non_comment:
            statements.append(stmt_text)

    return statements


def is_executable_statement(stmt: str) -> bool:
    """Check if a statement is something we should actually execute."""
    # Strip comments to find the first real SQL keyword
    lines = stmt.strip().split('\n')
    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith('--'):
            keyword = stripped.split()[0].upper() if stripped.split() else ''
            return keyword in (
                'CREATE', 'ALTER', 'DROP', 'INSERT', 'DELETE', 'UPDATE',
                'SELECT', 'USE', 'SHOW', 'GRANT', 'REVOKE', 'MERGE',
                'WITH', 'CALL', 'BEGIN', 'DECLARE',
            )
    return False


# ─── Deploy DB Objects ───────────────────────────────────────────────────────

def deploy_db_objects(session):
    """Deploy all SQL files from new_adjustment_db_objects/ in order."""
    base_dir = Path(__file__).parent / 'new_adjustment_db_objects'
    sql_files = sorted(glob.glob(str(base_dir / '*.sql')))

    if not sql_files:
        print("  ❌ No SQL files found in new_adjustment_db_objects/")
        return False

    total_stmts = 0
    total_errors = 0

    for sql_file in sql_files:
        file_name = os.path.basename(sql_file)
        print(f"\n  📄 {file_name}")
        print(f"     {'─' * 50}")

        with open(sql_file, 'r', encoding='utf-8') as f:
            sql_text = config.render(f.read())   # substitute {{TOKEN}} placeholders

        statements = split_sql_statements(sql_text)
        file_stmts = 0
        file_errors = 0

        for i, stmt in enumerate(statements, 1):
            if not is_executable_statement(stmt):
                continue

            # Get a short description of the statement
            first_line = next(
                (l.strip() for l in stmt.split('\n') if l.strip() and not l.strip().startswith('--')),
                stmt[:60]
            )
            short_desc = first_line[:80] + ('...' if len(first_line) > 80 else '')

            try:
                session.sql(stmt).collect()
                print(f"     ✅ [{i}] {short_desc}")
                file_stmts += 1
            except Exception as e:
                err_msg = str(e).split('\n')[0][:120]
                print(f"     ❌ [{i}] {short_desc}")
                print(f"           Error: {err_msg}")
                file_errors += 1

        total_stmts += file_stmts
        total_errors += file_errors
        status = "✅" if file_errors == 0 else "⚠️"
        print(f"     {status} {file_name}: {file_stmts} succeeded, {file_errors} failed")

    print(f"\n  {'═' * 60}")
    print(f"  DB Objects: {total_stmts} statements succeeded, {total_errors} failed")
    return total_errors == 0


# ─── Deploy Streamlit App ────────────────────────────────────────────────────

def deploy_streamlit_app(session):
    """
    Deploy the Streamlit app to Snowflake (Streamlit in Snowflake).

    Steps:
      1. Create an internal stage for the app files
      2. Upload all Python files (app.py, pages/*, utils/*)
      3. Create the STREAMLIT object
    """
    app_dir = Path(__file__).parent / 'streamlit_app'
    stage_name = 'ADJUSTMENT_APP.STREAMLIT_ADJUSTMENT_STAGE'
    streamlit_name = 'ADJUSTMENT_APP.ADJUSTMENT_ENGINE'

    print(f"\n  📦 Creating stage {stage_name}...")
    try:
        session.sql(f"CREATE STAGE IF NOT EXISTS {stage_name} ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')").collect()
        print(f"     ✅ Stage ready")
    except Exception as e:
        print(f"     ❌ Stage creation failed: {e}")
        return False

    # ── Upload files ─────────────────────────────────────────────────────
    files_to_upload = []

    # Root files: app.py, requirements.txt, environment.yml (SiS package declaration)
    for fname in ['app.py', 'requirements.txt', 'environment.yml']:
        fpath = app_dir / fname
        if fpath.exists():
            files_to_upload.append((fpath, ''))

    # config.py — single source of truth, shipped to the stage root so the app
    # can `import config` (stage root is on sys.path in SiS).
    cfg_path = Path(__file__).parent / 'config.py'
    if cfg_path.exists():
        files_to_upload.append((cfg_path, ''))

    # utils/ directory
    utils_dir = app_dir / 'utils'
    if utils_dir.exists():
        for fpath in utils_dir.glob('*.py'):
            files_to_upload.append((fpath, 'utils'))

    # pages/ directory
    pages_dir = app_dir / 'pages'
    if pages_dir.exists():
        for fpath in pages_dir.glob('*.py'):
            files_to_upload.append((fpath, 'pages'))

    # ── Remove stale files from stage (files deleted locally) ────────────
    print(f"\n  🧹 Checking for stale files on stage...")
    try:
        staged = session.sql(f"LIST @{stage_name}").collect()
        # Build expected set of relative paths (lowercased, as Snowflake normalises them)
        expected_rel_paths = set()
        for fpath, subdir in files_to_upload:
            rel = f"{subdir}/{fpath.name}".lower() if subdir else fpath.name.lower()
            expected_rel_paths.add(rel)

        removed = 0
        for row in staged:
            # LIST name format: "stage_unqualified_name/path/to/file[.gz]"
            raw = row['name']
            slash_idx = raw.find('/')
            rel_path = raw[slash_idx + 1:] if slash_idx != -1 else raw  # "pages/5_documentation.py"
            # Strip .gz suffix if auto_compress produced it
            rel_cmp = rel_path[:-3].lower() if rel_path.lower().endswith('.gz') else rel_path.lower()
            if rel_cmp not in expected_rel_paths:
                try:
                    session.sql(f"REMOVE @{stage_name}/{rel_path}").collect()
                    print(f"     🗑️  Removed stale: {rel_path}")
                    removed += 1
                except Exception as rm_err:
                    print(f"     ⚠️  Could not remove {rel_path}: {rm_err}")
        if removed == 0:
            print(f"     ✅ No stale files found")
    except Exception as e:
        print(f"     ⚠️  Stage cleanup warning: {e}")

    print(f"\n  📤 Uploading {len(files_to_upload)} files...")
    upload_errors = 0

    for fpath, subdir in files_to_upload:
        stage_path = f'@{stage_name}/{subdir}' if subdir else f'@{stage_name}'
        local_path = str(fpath).replace('\\', '/')
        display_name = f"{subdir + '/' if subdir else ''}{fpath.name}"
        try:
            session.file.put(
                local_path,
                stage_path,
                auto_compress=False,
                overwrite=True,
            )
            print(f"     ✅ {display_name}")
        except Exception as e:
            print(f"     ❌ {display_name}: {e}")
            upload_errors += 1

    if upload_errors > 0:
        print(f"  ⚠️ {upload_errors} files failed to upload")
        return False

    # ── Verify uploads ───────────────────────────────────────────────────
    print(f"\n  🔍 Verifying staged files...")
    try:
        staged = session.sql(f"LIST @{stage_name}").collect()
        for row in staged:
            print(f"     📁 {row['name']}")
    except Exception as e:
        print(f"     ⚠️ Could not list stage: {e}")

    # ── Create the Streamlit app ─────────────────────────────────────────
    print(f"\n  🚀 Creating Streamlit app {streamlit_name}...")
    create_sql = f"""
    CREATE OR REPLACE STREAMLIT {streamlit_name}
        ROOT_LOCATION   = '@{stage_name}'
        MAIN_FILE       = 'app.py'
        QUERY_WAREHOUSE = '{config.WAREHOUSE}'
        COMMENT         = 'Adjustment Engine — MUFG. Unified adjustment management for VaR, Stress, FRTB, Sensitivity.'
    """
    try:
        session.sql(create_sql).collect()
        print(f"     ✅ Streamlit app created successfully!")
    except Exception as e:
        print(f"     ❌ Streamlit creation failed: {e}")
        return False

    # ── Grant access ─────────────────────────────────────────────────────
    print(f"\n  🔐 Granting USAGE on Streamlit app...")
    try:
        session.sql(f"GRANT USAGE ON STREAMLIT {streamlit_name} TO ROLE {config.ROLE_OWNER}").collect()
        print(f"     ✅ USAGE granted to {config.ROLE_OWNER}")
        session.sql(f"GRANT USAGE ON STREAMLIT {streamlit_name} TO ROLE {config.ROLE_RO}").collect()
        print(f"     ✅ USAGE granted to {config.ROLE_RO}")
    except Exception as e:
        # May fail if role already owns it — that's fine
        print(f"     ℹ️  Grant note: {str(e)[:100]}")

    # ── Show the URL ─────────────────────────────────────────────────────
    try:
        info = session.sql(f"SHOW STREAMLITS LIKE 'ADJUSTMENT_ENGINE' IN SCHEMA ADJUSTMENT_APP").collect()
        if info:
            print(f"\n  🌐 Streamlit app details:")
            for row in info:
                for key in row.asDict():
                    if key.upper() in ('NAME', 'DATABASE_NAME', 'SCHEMA_NAME', 'URL_ID', 'QUERY_WAREHOUSE'):
                        print(f"     {key}: {row[key]}")
    except Exception:
        pass

    print(f"\n  ✅ Streamlit app deployed successfully!")
    return True


# ─── Resume Tasks ────────────────────────────────────────────────────────────

def resume_pipeline_tasks(session):
    """
    Explicitly resume the four scope-pipeline tasks.

    CREATE OR REPLACE TASK always leaves tasks in Suspended state. This runs
    as a dedicated step after all DB objects are deployed so it is guaranteed
    to execute regardless of SQL parsing order within the task file.
    """
    tasks = [
        'ADJUSTMENT_APP.TASK_PROCESS_VAR',
        'ADJUSTMENT_APP.TASK_PROCESS_STRESS',
        'ADJUSTMENT_APP.TASK_PROCESS_FRTB',
        'ADJUSTMENT_APP.TASK_PROCESS_SENSITIVITY',
    ]
    for task in tasks:
        try:
            session.sql(f"ALTER TASK {task} RESUME").collect()
            print(f"     ✅ Resumed {task}")
        except Exception as e:
            err_msg = str(e).split('\n')[0][:120]
            print(f"     ❌ Could not resume {task}: {err_msg}")


# ─── Submit Test Adjustment ─────────────────────────────────────────────────

def submit_test_adjustment(session):
    """
    Submit a test Flatten adjustment after every deploy to verify the
    end-to-end pipeline without manual UI interaction.
    """
    import json as _json

    adj_payload = _json.dumps({
        "cobid":              20260410,
        "process_type":       "VaR",
        "adjustment_type":    "Flatten",
        "source_cobid":       20260410,
        "scale_factor":       1.0,
        "entity_code":        "MUSI",
        "source_system_code": "QS",
        "book_code":          "Book-CCC",
        "adjustment_occurrence": "ADHOC",
        "reason":             "Post-deploy smoke test",
    })

    try:
        rows = session.sql(f"""
            CALL ADJUSTMENT_APP.SP_SUBMIT_ADJUSTMENT('{adj_payload}')
        """).collect()
        result = rows[0][0] if rows else "No result"
        # Parse if JSON string
        try:
            parsed = _json.loads(result) if isinstance(result, str) else result
            adj_id = parsed.get("adj_id", "?")
            status = parsed.get("status", "?")
            msg    = parsed.get("message", "")
            print(f"     ✅ Adjustment submitted — ADJ_ID: {adj_id}, Status: {status}")
            if msg:
                print(f"        {msg}")
        except Exception:
            print(f"     ✅ Result: {str(result)[:200]}")
        return True
    except Exception as e:
        err = str(e).split('\n')[0][:200]
        print(f"     ❌ Submit failed: {err}")
        return False


# ─── Clean (rebuild from scratch) ─────────────────────────────────────────────

# External base tables the repo does NOT recreate — must be preserved on rebuild.
_PRESERVE_TABLES = {'ADJUSTMENTS_BASE_SENSITIVITY', 'ADJUSTMENTS_BASE_FRTB'}

def clean_schema(session):
    """DROP all repo-managed objects in ADJUSTMENT_APP for a from-scratch rebuild.

    DESTRUCTIVE: removes ADJ_HEADER (incl. any corrupted/quoted version),
    ADJ_LINE_ITEM(_JSON), status history, settings, approvers, sign-off, etc.
    PRESERVES the external base tables in _PRESERVE_TABLES (ADJUSTMENTS_BASE_*),
    which the deploy does not recreate. Drops in dependency order; each DROP is
    IF EXISTS and isolated so one failure doesn't halt the teardown.
    """
    print("\n" + "─" * 64)
    print("  PHASE 0: Clean ADJUSTMENT_APP (DROP repo-managed objects)")
    print("─" * 64)
    print(f"  Preserving external base tables: {', '.join(sorted(_PRESERVE_TABLES))}")

    drops = []
    # 1. Tasks (consume streams + call procedures)
    for t in ('TASK_PROCESS_VAR', 'TASK_PROCESS_STRESS',
              'TASK_PROCESS_FRTB', 'TASK_PROCESS_SENSITIVITY'):
        drops.append(f"DROP TASK IF EXISTS ADJUSTMENT_APP.{t}")
    # 2. Streams (sit on the queue views)
    for s in ('STREAM_QUEUE_VAR', 'STREAM_QUEUE_STRESS',
              'STREAM_QUEUE_FRTB', 'STREAM_QUEUE_SENSITIVITY'):
        drops.append(f"DROP STREAM IF EXISTS ADJUSTMENT_APP.{s}")
    # 3. Dynamic tables
    for d in ('DT_DASHBOARD', 'DT_OVERLAP_ALERTS'):
        drops.append(f"DROP DYNAMIC TABLE IF EXISTS ADJUSTMENT_APP.{d}")
    # 4. Views
    for v in ('VW_QUEUE_VAR', 'VW_QUEUE_STRESS', 'VW_QUEUE_FRTB', 'VW_QUEUE_SENSITIVITY',
              'VW_DASHBOARD_KPI', 'VW_SIGNOFF_STATUS', 'VW_RECENT_ACTIVITY', 'VW_ERRORS',
              'VW_MY_WORK', 'VW_PROCESSING_QUEUE', 'VW_APPROVAL_QUEUE',
              'VW_REPORT_REFRESH_STATUS', 'VW_ADJUSTMENT_TRACK'):
        drops.append(f"DROP VIEW IF EXISTS ADJUSTMENT_APP.{v}")
    # 5. Procedures (exact signatures)
    drops += [
        "DROP PROCEDURE IF EXISTS ADJUSTMENT_APP.SP_SUBMIT_ADJUSTMENT(VARCHAR)",
        "DROP PROCEDURE IF EXISTS ADJUSTMENT_APP.SP_PREVIEW_ADJUSTMENT(VARCHAR)",
        "DROP PROCEDURE IF EXISTS ADJUSTMENT_APP.SP_PROCESS_ADJUSTMENT(VARCHAR, VARCHAR, NUMBER)",
        "DROP PROCEDURE IF EXISTS ADJUSTMENT_APP.SP_RUN_PIPELINE(VARCHAR, VARCHAR)",
    ]
    # 6. Tables (repo-managed only — NEVER the preserved base tables)
    for tbl in ('ADJ_HEADER', 'ADJ_LINE_ITEM', 'ADJ_LINE_ITEM_JSON', 'ADJ_STATUS_HISTORY',
                'ADJUSTMENTS_SETTINGS', 'ADJ_RECURRING_TEMPLATE', 'DIRECT_SCOPE_SCHEMA',
                'ADJ_APPROVERS', 'ADJ_SIGNOFF_STATUS', 'ADJ_CATEGORY'):
        assert tbl not in _PRESERVE_TABLES
        drops.append(f"DROP TABLE IF EXISTS ADJUSTMENT_APP.{tbl}")

    ok = 0
    for stmt in drops:
        obj = stmt.replace('DROP ', '').replace('IF EXISTS ', '')
        try:
            session.sql(stmt).collect()
            print(f"     ✅ {obj}")
            ok += 1
        except Exception as e:
            print(f"     ⚠️  {obj}: {str(e).splitlines()[0][:100]}")
    print(f"  Teardown: {ok}/{len(drops)} drops succeeded")
    return True


# ─── Schema validation ────────────────────────────────────────────────────────

# Columns the Scale/Roll proc never copies from the adjusted view into the
# _ADJUSTMENT table (they're excluded from the SELECT or generated by the proc),
# so they don't need to exist in the combined view.
_VALIDATE_IGNORE_COLS = {
    "COBID", "LOAD_TIMESTAMP", "RUN_LOG_ID", "RAVEN_FILENAME",
    "RAVEN_FILE_ROW_NUMBER", "ADJUSTMENT_ID", "ADJUSTMENT_CREATED_TIMESTAMP",
}


def _object_columns(session, fqname):
    """Return the column names of a table/view exactly as Snowflake reports them
    (case-sensitive — a quoted lowercase column comes back lowercase), or None if
    the object does not exist / is not accessible."""
    try:
        rows = session.sql(f"SHOW COLUMNS IN {fqname}").collect()
        return [r["column_name"] for r in rows]
    except Exception:
        return None


def validate_schema(session):
    """Validate the objects ADJUSTMENTS_SETTINGS points at, before adjustments run.

    Per active scope:
      1. FACT_TABLE (official data), ADJUSTMENTS_TABLE (the _ADJUSTMENT delta /
         write target), and FACT_ADJUSTED_TABLE (the combined / _ADJUSTED /
         _COMBINED view) all exist.
      2. Roll invariant (informational, never fatal): the combined view may lack
         some columns the _ADJUSTMENT table has. A cross-COB Roll discovers the
         view's columns at runtime and defaults any it lacks to -1 (KEY/ID) or
         NULL — by design — so this is reported as a warning, not a failure.
      3. METRIC_USD_NAME exists in FACT_TABLE and in the combined view.
      4. Every FACT_TABLE_PK column exists (same name + case) in both FACT_TABLE
         and ADJUSTMENTS_TABLE — i.e. it survives into the processing CTE the
         surrogate dedup/overlap key is built over. A phantom PK column compiles
         to an invalid identifier at runtime, so this is a hard failure.
      5. ADJUSTMENTS_SUMMARY_TABLE (if set) exists and its non-metric columns all
         exist in the _ADJUSTMENT table (the summary rebuild SELECTs them there).

    METRIC_NAME is intentionally NOT validated: single-metric (USD-only) scopes
    legitimately have no local METRIC_NAME and the engine collapses to USD.

    Prints a per-scope report. Returns True if every scope passes.
    """
    print("\n" + "─" * 64)
    print("  PHASE: Validate schema (combined view vs _ADJUSTMENT table)")
    print("─" * 64)
    try:
        rows = session.sql("""
            SELECT PROCESS_TYPE, FACT_TABLE, FACT_ADJUSTED_TABLE,
                   ADJUSTMENTS_TABLE, ADJUSTMENTS_SUMMARY_TABLE,
                   METRIC_NAME, METRIC_USD_NAME, FACT_TABLE_PK
            FROM ADJUSTMENT_APP.ADJUSTMENTS_SETTINGS
            WHERE IS_ACTIVE = TRUE
            ORDER BY PROCESS_TYPE
        """).collect()
    except Exception as e:
        print(f"     ❌ Could not read ADJUSTMENTS_SETTINGS: {str(e).splitlines()[0][:120]}")
        return False

    if not rows:
        print("     ⚠️  No active rows in ADJUSTMENTS_SETTINGS — nothing to validate.")
        return True

    all_ok = True
    had_warnings = False
    for r in rows:
        pt         = r["PROCESS_TYPE"]
        fact       = r["FACT_TABLE"]
        adjusted   = r["FACT_ADJUSTED_TABLE"]      # combined / _adjusted / _combined view
        adj_tbl    = r["ADJUSTMENTS_TABLE"]        # _adjustment delta (write target)
        summary_tbl = r["ADJUSTMENTS_SUMMARY_TABLE"]   # optional rollup (write target)
        metric_name = (r["METRIC_NAME"] or "").upper()
        metric_usd = (r["METRIC_USD_NAME"] or "").upper()
        problems   = []   # hard failures (block)
        warnings   = []   # informational (Roll will default these to -1/NULL)

        fact_cols = _object_columns(session, fact) if fact else None
        view_cols = _object_columns(session, adjusted) if adjusted else None
        adj_cols  = _object_columns(session, adj_tbl) if adj_tbl else None
        summary_cols = _object_columns(session, summary_tbl) if summary_tbl else None

        # 1. Existence
        if not fact or fact_cols is None:
            problems.append(f"FACT_TABLE '{fact}' does not exist / not accessible")
        if not adj_tbl or adj_cols is None:
            problems.append(f"ADJUSTMENTS_TABLE '{adj_tbl}' does not exist / not accessible")
        if not adjusted or view_cols is None:
            problems.append(f"FACT_ADJUSTED_TABLE '{adjusted}' does not exist / not accessible")

        # 2. Roll invariant — combined view must expose the columns the proc copies
        #    into the _ADJUSTMENT table (= cols common to FACT_TABLE and the delta).
        if fact_cols is not None and adj_cols is not None and view_cols is not None:
            adj_set, view_set = set(adj_cols), set(view_cols)
            selected = [c for c in fact_cols
                        if c in adj_set and c.upper() not in _VALIDATE_IGNORE_COLS]
            missing = [c for c in selected if c not in view_set]
            if missing:
                shown = ', '.join(missing[:12]) + (' …' if len(missing) > 12 else '')
                # EXPECTED, not an error. A cross-COB Roll discovers the combined
                # view's columns at runtime (SP_PROCESS_ADJUSTMENT) and defaults any
                # it lacks to -1 (KEY/ID) or NULL. Informational only — never a
                # failure; the deploy still PASSES.
                warnings.append(
                    f"FACT_ADJUSTED_TABLE '{adjusted}' lacks {len(missing)} column(s) "
                    f"present in '{adj_tbl}'; a Roll defaults them to -1/NULL: {shown}")

        # 3. Metric column present in the fact and the combined view
        if fact_cols is not None and metric_usd and metric_usd not in {c.upper() for c in fact_cols}:
            problems.append(f"METRIC_USD_NAME '{metric_usd}' not found in FACT_TABLE '{fact}'")
        if view_cols is not None and metric_usd and metric_usd not in {c.upper() for c in view_cols}:
            problems.append(f"METRIC_USD_NAME '{metric_usd}' not found in FACT_ADJUSTED_TABLE '{adjusted}'")

        # 4. FACT_TABLE_PK columns must be buildable into the dedup/overlap key.
        #    SP_PROCESS_ADJUSTMENT builds the surrogate key over the processing
        #    CTE, which carries COBID plus the columns common to FACT_TABLE and
        #    ADJUSTMENTS_TABLE. A PK column outside that set makes the generated
        #    key SQL reference a non-existent identifier and fail to compile at
        #    runtime — the exact Stress failure (config said CURRENCY_CODE, but
        #    FACT.STRESS_MEASURES only has TRADE_CURRENCY). Hard failure: a wrong
        #    key column silently degraded would corrupt netting/supersede.
        pk_raw = (r["FACT_TABLE_PK"] or "").strip()
        if pk_raw and fact_cols is not None and adj_cols is not None:
            usable  = (set(fact_cols) & set(adj_cols)) | {"COBID"}   # what the CTE carries
            fact_ci = {c.upper() for c in fact_cols}
            adj_ci  = {c.upper() for c in adj_cols}
            for k in [p.strip() for p in pk_raw.split(';') if p.strip()]:
                if k in usable:
                    continue
                in_fact, in_adj = k.upper() in fact_ci, k.upper() in adj_ci
                if in_fact and in_adj:
                    problems.append(
                        f"FACT_TABLE_PK column '{k}' matches FACT_TABLE/ADJUSTMENTS_TABLE "
                        f"only under a different case — the key build is case-sensitive; "
                        f"use the exact column name")
                else:
                    where = [t for t, present in
                             ((f"FACT_TABLE '{fact}'", in_fact),
                              (f"ADJUSTMENTS_TABLE '{adj_tbl}'", in_adj)) if not present]
                    problems.append(
                        f"FACT_TABLE_PK column '{k}' is missing from {' and '.join(where)} "
                        f"— the dedup/overlap key cannot be built; processing will fail to compile")

        # 5. ADJUSTMENTS_SUMMARY_TABLE (optional). The summary rebuild SELECTs the
        #    summary table's non-metric columns FROM the _ADJUSTMENT table, so each
        #    such column must exist there or the INSERT fails to compile.
        if summary_tbl:
            if summary_cols is None:
                problems.append(f"ADJUSTMENTS_SUMMARY_TABLE '{summary_tbl}' does not exist / not accessible")
            elif adj_cols is not None:
                metrics = {metric_name, metric_usd}
                adj_set = set(adj_cols)
                miss = [c for c in summary_cols
                        if c.upper() not in metrics and c not in adj_set]
                if miss:
                    shown = ', '.join(miss[:12]) + (' …' if len(miss) > 12 else '')
                    problems.append(
                        f"ADJUSTMENTS_SUMMARY_TABLE '{summary_tbl}' has column(s) not in "
                        f"'{adj_tbl}' — the summary rebuild SELECTs them from the delta "
                        f"table and will fail to compile: {shown}")

        if warnings:
            had_warnings = True
        if problems:
            all_ok = False
            print(f"     ❌ {pt}")
            for p in problems:
                print(f"          • {p}")
            for w in warnings:
                print(f"          • (warn) {w}")
        elif warnings:
            print(f"     ⚠️  {pt}  (adjusted={adjusted} vs delta={adj_tbl})")
            for w in warnings:
                print(f"          • {w}")
        else:
            print(f"     ✅ {pt}  (adjusted={adjusted} vs delta={adj_tbl})")

    if all_ok:
        summary = "PASS"
        if had_warnings:
            summary += " (with warnings — Roll defaults some keys to -1/NULL)"
    else:
        summary = "FAILED — fix the items above before running adjustments"
    print(f"\n  Schema validation: {summary}")
    return all_ok


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Deploy Adjustment Engine to Snowflake')
    parser.add_argument('--db-only', action='store_true', help='Deploy DB objects only')
    parser.add_argument('--streamlit-only', action='store_true', help='Deploy Streamlit app only')
    parser.add_argument('--test-adj', action='store_true', help='Submit a test VaR Flatten adjustment after deploy')
    parser.add_argument('--rebuild', action='store_true',
                        help='DESTRUCTIVE: DROP all repo-managed ADJUSTMENT_APP objects '
                             '(incl. ADJ_HEADER and all adjustment/approver/sign-off data) '
                             'before deploying from scratch. Preserves external base tables '
                             '(ADJUSTMENTS_BASE_*).')
    parser.add_argument('--validate-only', action='store_true',
                        help='Only run the schema validation (ADJUSTMENTS_SETTINGS objects + '
                             'Roll invariant) and exit non-zero on failure. No deploy.')
    args = parser.parse_args()

    deploy_db = not args.streamlit_only
    deploy_st = not args.db_only
    if args.rebuild:
        deploy_db = True   # rebuild always reapplies DB objects after teardown
    if args.validate_only:
        deploy_db = deploy_st = False

    print("=" * 64)
    print("  Adjustment Engine — Snowflake Deployment")
    print(f"  Database: {config.DATABASE}")
    print(f"  Schema:   {config.SCHEMA}")
    print("=" * 64)

    # ── Connect ──────────────────────────────────────────────────────────
    print("\n  🔌 Connecting to Snowflake...")
    try:
        session = get_session()
        ctx = session.sql("SELECT CURRENT_ROLE() AS R, CURRENT_WAREHOUSE() AS W, CURRENT_DATABASE() AS D").collect()[0]
        print(f"     ✅ Connected — Role: {ctx['R']}, Warehouse: {ctx['W']}, Database: {ctx['D']}")
    except Exception as e:
        print(f"     ❌ Connection failed: {e}")
        sys.exit(1)

    success = True

    # ── Validate-only mode ───────────────────────────────────────────────
    if args.validate_only:
        ok = validate_schema(session)
        session.close()
        return 0 if ok else 1

    # ── Clean (rebuild only) ─────────────────────────────────────────────
    if args.rebuild:
        clean_schema(session)

    # ── Deploy DB objects ────────────────────────────────────────────────
    if deploy_db:
        print("\n" + "─" * 64)
        print("  PHASE 1: Database Objects (tables, streams, SPs, tasks, views)")
        print("─" * 64)
        if not deploy_db_objects(session):
            print("\n  ⚠️  Some DB objects had errors — review above.")
            success = False

        print("\n" + "─" * 64)
        print("  PHASE 1b: Resume pipeline tasks")
        print("─" * 64)
        resume_pipeline_tasks(session)

        print("\n" + "─" * 64)
        print("  PHASE 1c: Validate schema")
        print("─" * 64)
        if not validate_schema(session):
            print("\n  ⚠️  Schema validation FAILED — adjustments may error until fixed.")
            success = False

    # ── Deploy Streamlit app ─────────────────────────────────────────────
    if deploy_st:
        print("\n" + "─" * 64)
        print("  PHASE 2: Streamlit Application")
        print("─" * 64)
        if not deploy_streamlit_app(session):
            print("\n  ⚠️  Streamlit deployment had errors — review above.")
            success = False

    # ── Submit test adjustment ──────────────────────────────────────────
    if args.test_adj:
        print("\n" + "─" * 64)
        print("  PHASE 3: Submit Test Adjustment (VaR Flatten)")
        print("─" * 64)
        if not submit_test_adjustment(session):
            print("\n  ⚠️  Test adjustment submission failed — review above.")

    # ── Summary ──────────────────────────────────────────────────────────
    print("\n" + "=" * 64)
    if success:
        print("  ✅ DEPLOYMENT COMPLETE — All objects deployed successfully!")
    else:
        print("  ⚠️  DEPLOYMENT COMPLETE WITH WARNINGS — Review errors above.")
    print("=" * 64)

    session.close()
    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())
