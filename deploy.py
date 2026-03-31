"""
deploy.py — Deploy all Snowflake objects + Streamlit app to DVLP_RAPTOR_NEWADJ
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

# ─── Connection ──────────────────────────────────────────────────────────────

def get_session():
    """Create a Snowpark session using the MUFG connector."""
    from mufg_snowflakeconn import sfconnection as m_sf
    mufgconn = m_sf.MufgSnowflakeConn('dvlp', 'apd_raptor_sfk_depl@mufgsecurities.com')
    session = mufgconn.get_snowflake_session()
    session.use_role("DVLP_RAPTOR_OWNER")
    session.use_warehouse("DVLP_RAPTOR_WH_XS")
    session.use_database("DVLP_RAPTOR_NEWADJ")
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
            sql_text = f.read()

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

    # Root: app.py, requirements.txt, environment.yml
    for fname in ['app.py', 'requirements.txt']:
        fpath = app_dir / fname
        if fpath.exists():
            files_to_upload.append((fpath, ''))

    # environment.yml lives at the project root (one level above streamlit_app/)
    env_yml = Path(__file__).parent / 'environment.yml'
    if env_yml.exists():
        files_to_upload.append((env_yml, ''))

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
        ROOT_LOCATION    = '@{stage_name}'
        MAIN_FILE        = 'app.py'
        ENVIRONMENT_FILE = 'environment.yml'
        QUERY_WAREHOUSE  = 'DVLP_RAPTOR_WH_XS'
        COMMENT          = 'Adjustment Engine — MUFG. Unified adjustment management for VaR, Stress, FRTB, Sensitivity.'
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
        session.sql(f"GRANT USAGE ON STREAMLIT {streamlit_name} TO ROLE DVLP_RAPTOR_OWNER").collect()
        print(f"     ✅ USAGE granted to DVLP_RAPTOR_OWNER")
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


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Deploy Adjustment Engine to Snowflake')
    parser.add_argument('--db-only', action='store_true', help='Deploy DB objects only')
    parser.add_argument('--streamlit-only', action='store_true', help='Deploy Streamlit app only')
    args = parser.parse_args()

    deploy_db = not args.streamlit_only
    deploy_st = not args.db_only

    print("=" * 64)
    print("  Adjustment Engine — Snowflake Deployment")
    print("  Database: DVLP_RAPTOR_NEWADJ")
    print("  Schema:   ADJUSTMENT_APP")
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

    # ── Deploy DB objects ────────────────────────────────────────────────
    if deploy_db:
        print("\n" + "─" * 64)
        print("  PHASE 1: Database Objects (tables, streams, SPs, tasks, views)")
        print("─" * 64)
        if not deploy_db_objects(session):
            print("\n  ⚠️  Some DB objects had errors — review above.")
            success = False

    # ── Deploy Streamlit app ─────────────────────────────────────────────
    if deploy_st:
        print("\n" + "─" * 64)
        print("  PHASE 2: Streamlit Application")
        print("─" * 64)
        if not deploy_streamlit_app(session):
            print("\n  ⚠️  Streamlit deployment had errors — review above.")
            success = False

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
