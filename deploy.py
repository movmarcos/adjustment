"""
deploy.py — Deploy all Snowflake objects + Streamlit app (target set in config.py)
================================================================================
Usage:
    python deploy.py                      # Deploy everything (DB objects + Streamlit app + Notebooks)
    python deploy.py --db-only            # Deploy DB objects only
    python deploy.py --streamlit-only     # Deploy Streamlit app only
    python deploy.py --notebooks-only     # Deploy Snowflake Notebooks only

Prerequisites:
    pip install mufg_snowflakeconn snowflake-snowpark-python
"""
import os
import re
import sys
import glob
import time
import hashlib
import argparse
from pathlib import Path

import config

# ─── Timing ──────────────────────────────────────────────────────────────────
# A deploy is well over a hundred Snowflake round trips. When it feels slow the
# only useful question is WHICH part is slow, so every phase is timed and the
# breakdown is printed at the end, slowest first.

_PHASE_TIMES = []


class phase_timer:
    """Context manager recording how long a named deploy phase took."""

    def __init__(self, label):
        self.label = label
        self.t0 = 0.0

    def __enter__(self):
        self.t0 = time.time()
        return self

    def __exit__(self, *exc):
        _PHASE_TIMES.append((self.label, time.time() - self.t0))
        return False


def print_timing_breakdown():
    if not _PHASE_TIMES:
        return
    total = sum(sec for _, sec in _PHASE_TIMES)
    print("\n  ⏱  Where the time went:")
    for label, sec in sorted(_PHASE_TIMES, key=lambda r: -r[1]):
        share = (sec / total * 100) if total else 0
        bar = "█" * max(1, int(share / 4))
        print(f"     {label:<34} {sec:7.1f}s  {share:5.1f}%  {bar}")
    print(f"     {'TOTAL':<34} {total:7.1f}s")


# ─── Stage upload helpers ────────────────────────────────────────────────────

def file_md5(path):
    """MD5 of a local file, to compare against what LIST reports on a stage."""
    digest = hashlib.md5()
    with open(path, 'rb') as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


def staged_md5_map(session, stage_name):
    """{relative stage path (lowercased): md5} for everything on a stage.

    Lets an unchanged file skip its PUT. Uploads are one network round trip
    each and dominate a Streamlit-only deploy, where usually one or two of the
    seventeen files actually changed. Returns {} if LIST fails, which just
    means every file uploads as before.
    """
    out = {}
    try:
        for row in session.sql(f"LIST @{stage_name}").collect():
            data = row.asDict()
            raw = data.get('name') or ''
            slash = raw.find('/')
            rel = raw[slash + 1:] if slash != -1 else raw
            md5 = data.get('md5') or data.get('MD5') or ''
            if rel and md5:
                out[rel.lower()] = str(md5).lower()
    except Exception:
        pass
    return out

# ─── Connection ──────────────────────────────────────────────────────────────

def get_session():
    """Create a Snowpark session using the MUFG connector."""
    from mufg_snowflakeconn import sfconnection as m_sf
    mufgconn = m_sf.MufgSnowflakeConn(config.SF_CONN_ENV, config.DEPLOY_USER)
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
        file_t0 = time.time()
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
                _t0 = time.time()
                session.sql(stmt).collect()
                _took = time.time() - _t0
                # Most DDL is metadata-only and returns in well under a
                # second. Anything that does not is what makes a deploy feel
                # slow (an initial dynamic-table refresh, a table rewrite),
                # so name it rather than hiding it in the total.
                _slow = f"   ⏱ {_took:.1f}s" if _took >= 2.0 else ""
                print(f"     ✅ [{i}] {short_desc}{_slow}")
                file_stmts += 1
            except Exception as e:
                # Full message, whitespace-normalized — Snowflake often puts
                # the real error on line 2+, so first-line-only hid it.
                err_msg = " ".join(str(e).split())[:600]
                print(f"     ❌ [{i}] {short_desc}")
                print(f"           Error: {err_msg}")
                file_errors += 1

        total_stmts += file_stmts
        total_errors += file_errors
        status = "✅" if file_errors == 0 else "⚠️"
        file_secs = time.time() - file_t0
        _PHASE_TIMES.append((f"sql: {file_name}", file_secs))
        print(f"     {status} {file_name}: {file_stmts} succeeded, "
              f"{file_errors} failed, {file_secs:.1f}s")

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

    # .streamlit/ directory (theme config — pins the light theme so the app
    # stays readable when the viewer's browser/Snowsight is in dark mode)
    st_cfg_dir = app_dir / '.streamlit'
    if st_cfg_dir.exists():
        for fpath in st_cfg_dir.glob('*.toml'):
            files_to_upload.append((fpath, '.streamlit'))

    # utils/ directory
    utils_dir = app_dir / 'utils'

    # Build stamp — regenerated on every deploy so the running app can PROVE
    # which commit it is (shown in the sidebar footer). Ends the "is the fix
    # actually deployed?" ambiguity for good.
    try:
        import subprocess
        from datetime import datetime, timezone
        _sha = subprocess.run(
            ['git', 'rev-parse', '--short', 'HEAD'],
            capture_output=True, text=True,
            cwd=Path(__file__).parent).stdout.strip() or 'unknown'
        _when = datetime.now(timezone.utc).strftime('%d %b %Y %H:%M UTC')
        (utils_dir / 'build_info.py').write_text(
            f'BUILD = "{_sha} · {_when}"\n', encoding='utf-8')
        print(f"     🏷️  Build stamp: {_sha} · {_when}")
    except Exception as e:
        print(f"     ⚠️ Build stamp skipped: {e}")

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

    # Every PUT is a network round trip, and on a normal deploy almost every
    # file is byte-identical to what is already staged. Compare MD5s once and
    # upload only what actually changed.
    staged_md5 = staged_md5_map(session, stage_name)
    print(f"\n  📤 Uploading {len(files_to_upload)} files...")
    upload_errors = 0
    uploaded = 0
    skipped = 0

    for fpath, subdir in files_to_upload:
        stage_path = f'@{stage_name}/{subdir}' if subdir else f'@{stage_name}'
        local_path = str(fpath).replace('\\', '/')
        display_name = f"{subdir + '/' if subdir else ''}{fpath.name}"
        try:
            if staged_md5.get(display_name.lower()) == file_md5(fpath):
                skipped += 1
                continue
        except Exception:
            pass    # unreadable hash — fall through and upload it
        try:
            session.file.put(
                local_path,
                stage_path,
                auto_compress=False,
                overwrite=True,
            )
            print(f"     ✅ {display_name}")
            uploaded += 1
        except Exception as e:
            print(f"     ❌ {display_name}: {e}")
            upload_errors += 1

    if skipped:
        print(f"     ⏭️  {skipped} unchanged, {uploaded} uploaded")

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


# ─── Deploy the line-probe control app ───────────────────────────────────────

def _deploy_probe(session, name, files, comment):
    """One control app: `files` is a list of (local Path, stage subdir)."""
    stage_name = f'ADJUSTMENT_APP.{name}_STAGE'
    streamlit_name = f'ADJUSTMENT_APP.{name}'
    print(f"\n  📦 {streamlit_name}")
    try:
        session.sql(f"CREATE STAGE IF NOT EXISTS {stage_name} ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')").collect()
    except Exception as e:
        print(f"     ❌ Stage creation failed: {e}")
        return False
    for fpath, subdir in files:
        stage_path = f'@{stage_name}/{subdir}' if subdir else f'@{stage_name}'
        try:
            session.file.put(str(fpath).replace('\\', '/'), stage_path,
                             auto_compress=False, overwrite=True)
            print(f"     ✅ {subdir + '/' if subdir else ''}{fpath.name}")
        except Exception as e:
            print(f"     ❌ {fpath.name}: {e}")
            return False
    try:
        session.sql(f"""
        CREATE OR REPLACE STREAMLIT {streamlit_name}
            ROOT_LOCATION   = '@{stage_name}'
            MAIN_FILE       = 'app.py'
            QUERY_WAREHOUSE = '{config.WAREHOUSE}'
            COMMENT         = '{comment}'
        """).collect()
    except Exception as e:
        print(f"     ❌ Streamlit creation failed: {e}")
        return False
    for role in (config.ROLE_OWNER, config.ROLE_RO):
        try:
            session.sql(f"GRANT USAGE ON STREAMLIT {streamlit_name} TO ROLE {role}").collect()
        except Exception as e:
            print(f"     ℹ️  Grant note ({role}): {str(e)[:100]}")
    return True


# Line-probe variants. Each is a sibling control app whose ONLY difference
# from the bare control is the .streamlit/config.toml body given here.
#
# Round 3 (2026-09-24): one app per engine theme setting → ALL FIVE showed
# the line. So no single colour is the cause; the presence of a theme (or of
# the file) is. Those five are retired below.
#
# Round 4: split "file present" from "theme present", and try two candidate
# fixes alongside, so one deploy can give both the cause and the cure.
ENGINE_THEME = (
    '[theme]\n'
    'base = "light"\n'
    'primaryColor = "#D50032"\n'
    'backgroundColor = "#F6F7F9"\n'
    'secondaryBackgroundColor = "#FFFFFF"\n'
    'textColor = "#0F172A"\n'
)
# Round 4 result: T6 (file, no sections) and T8 ([client] only) → NO line.
# T7 (EMPTY [theme] section) → line. In Streamlit 1.50 an empty theme
# section sends nothing to the front-end (app_session._populate_theme_msg
# returns early), so Streamlit ran T7 exactly like the bare control. The only
# thing that reacts to the [theme] header itself is Snowsight, which reads
# the file to pick the app's colour scheme. The line is Snowsight's, outside
# the iframe — which is why it is fixed to the window, crosses the sidebar,
# shows while the app is still loading and is invisible to any in-app probe.
# Locally on 1.50.0 with the same theme file there is no line at all.
#
# Round 5: the same five settings written as TOML dotted keys, with no
# [theme] header. Streamlit's parser produces the identical config; if
# Snowsight keys on the header text it will not react.
ENGINE_THEME_DOTTED = (
    'theme.base = "light"\n'
    'theme.primaryColor = "#D50032"\n'
    'theme.backgroundColor = "#F6F7F9"\n'
    'theme.secondaryBackgroundColor = "#FFFFFF"\n'
    'theme.textColor = "#0F172A"\n'
)
PROBE_VARIANTS = [
    ('LINE_PROBE_T11_DOTTED', ENGINE_THEME_DOTTED),
]
# Settled variants from earlier rounds — dropped on the next --sample-app run.
RETIRED_PROBES = [
    'LINE_PROBE_T1_BASE', 'LINE_PROBE_T2_PRIMARY', 'LINE_PROBE_T3_BG',
    'LINE_PROBE_T4_SECONDARY', 'LINE_PROBE_T5_TEXT',
    'LINE_PROBE_T6_FILE_ONLY', 'LINE_PROBE_T7_THEME_EMPTY',
    'LINE_PROBE_T8_CLIENT_ONLY', 'LINE_PROBE_T9_HIDETOPBAR',
    'LINE_PROBE_T10_MINIMAL',
]


def deploy_sample_app(session):
    """
    Deploy sample_app/ — a bare Streamlit app with NO code from this repo —
    as ADJUSTMENT_APP.LINE_PROBE, next to the engine, plus one sibling per
    theme setting (PROBE_THEME_VARIANTS).

    It exists to settle where the horizontal line comes from (see
    sample_app/app.py). It ships only app.py, pages/ and the runtime pin:
    no config.py, no utils/. Round 1 also shipped no theme; round 2 added the
    engine's .streamlit/config.toml unchanged and the line appeared; round 3
    split that file one setting per app (all showed it); round 4 is
    PROBE_VARIANTS.
    """
    import tempfile
    app_dir = Path(__file__).parent / 'sample_app'
    base_files = [(app_dir / 'app.py', ''), (app_dir / 'environment.yml', '')]
    base_files += [(f, 'pages') for f in sorted((app_dir / 'pages').glob('*.py'))]

    # The reference: whole theme, byte-identical to the engine's.
    full = base_files + [(f, '.streamlit') for f in sorted((app_dir / '.streamlit').glob('*.toml'))]
    ok = _deploy_probe(session, 'LINE_PROBE', full,
                       'Line probe — bare control app + the engine theme file. Delete when settled.')
    if not ok:
        return False

    # Retire settled variants so the Snowsight list only shows live ones.
    for name in RETIRED_PROBES:
        for stmt in (f"DROP STREAMLIT IF EXISTS ADJUSTMENT_APP.{name}",
                     f"DROP STAGE IF EXISTS ADJUSTMENT_APP.{name}_STAGE"):
            try:
                session.sql(stmt).collect()
            except Exception as e:
                print(f"     ℹ️  {stmt}: {str(e)[:80]}")

    tmp = Path(tempfile.mkdtemp(prefix='line_probe_'))
    for name, body in PROBE_VARIANTS:
        vdir = tmp / name / '.streamlit'
        vdir.mkdir(parents=True)
        (vdir / 'config.toml').write_text(
            f"# Line-probe variant {name}\n" + body, encoding='utf-8')
        files = base_files + [(vdir / 'config.toml', '.streamlit')]
        if not _deploy_probe(session, name, files,
                             f'Line probe variant {name}. Delete when settled.'):
            return False

    print("\n  ✅ Line probes deployed. In Snowsight → Streamlit open each of:")
    print("     LINE_PROBE                 whole engine theme — the reference, shows the line")
    for name, body in PROBE_VARIANTS:
        print(f"     {name:<27}{body.strip().replace(chr(10), ' | ')}")
    return True


# ─── Deploy Notebooks ────────────────────────────────────────────────────────

def deploy_notebooks(session):
    """
    Deploy the Snowflake Notebook test harness (notebooks/) to Snowflake.

    Steps:
      1. Create an internal stage for the notebook files
      2. Upload the kit module (adjustment_test_kit.py) and the notebook
         (adjustment_test_harness.ipynb) onto the stage root
      3. Create the NOTEBOOK object and activate a live version

    config.py is deliberately NOT uploaded here: adjustment_test_kit.py takes
    an explicit `session` and reads everything scope-specific from
    ADJUSTMENT_APP.ADJUSTMENTS_SETTINGS at runtime — it imports nothing from
    this repo's config module (see notebooks/adjustment_test_kit.py docstring
    and docs/superpowers/specs/2026-09-23-adjustment-test-harness-design.md).
    """
    nb_dir = Path(__file__).parent / 'notebooks'
    stage_name = 'ADJUSTMENT_APP.NOTEBOOK_STAGE'
    notebook_name = 'ADJUSTMENT_APP.ADJUSTMENT_TEST_HARNESS'
    main_file = 'adjustment_test_harness.ipynb'

    print(f"\n  📦 Creating stage {stage_name}...")
    try:
        session.sql(f"CREATE STAGE IF NOT EXISTS {stage_name} ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')").collect()
        print(f"     ✅ Stage ready")
    except Exception as e:
        print(f"     ❌ Stage creation failed: {e}")
        return False

    # ── Upload files ─────────────────────────────────────────────────────
    # Stage root only, no subdirectories — the notebook's first code cell is
    # `import adjustment_test_kit as kit`, and Snowflake puts the notebook's
    # own stage directory on sys.path, so the kit module must sit right next
    # to the .ipynb. Top-level notebooks/*.py and notebooks/*.ipynb only —
    # notebooks/tests/ is a local pytest suite, not shipped to the stage.
    files_to_upload = []
    for pattern in ('*.py', '*.ipynb'):
        for fpath in sorted(nb_dir.glob(pattern)):
            files_to_upload.append((fpath, ''))

    if not files_to_upload:
        print(f"     ❌ No files found in {nb_dir} — nothing to deploy")
        return False

    main_file_present = (nb_dir / main_file).exists()
    if not main_file_present:
        print(f"     ⚠️  {main_file} not found in {nb_dir} yet — uploading what "
              f"exists; CREATE NOTEBOOK below will be skipped until it lands")

    # ── Remove stale files from stage (files deleted/renamed locally) ────
    print(f"\n  🧹 Checking for stale files on stage...")
    try:
        staged = session.sql(f"LIST @{stage_name}").collect()
        expected_rel_paths = {fpath.name.lower() for fpath, _ in files_to_upload}

        removed = 0
        for row in staged:
            # LIST name format: "stage_unqualified_name/path/to/file[.gz]"
            raw = row['name']
            slash_idx = raw.find('/')
            rel_path = raw[slash_idx + 1:] if slash_idx != -1 else raw
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

    # Every PUT is a network round trip, and on a normal deploy almost every
    # file is byte-identical to what is already staged. Compare MD5s once and
    # upload only what actually changed.
    staged_md5 = staged_md5_map(session, stage_name)
    print(f"\n  📤 Uploading {len(files_to_upload)} files...")
    upload_errors = 0
    uploaded = 0
    skipped = 0

    for fpath, subdir in files_to_upload:
        stage_path = f'@{stage_name}/{subdir}' if subdir else f'@{stage_name}'
        local_path = str(fpath).replace('\\', '/')
        display_name = f"{subdir + '/' if subdir else ''}{fpath.name}"
        try:
            if staged_md5.get(display_name.lower()) == file_md5(fpath):
                skipped += 1
                continue
        except Exception:
            pass    # unreadable hash — fall through and upload it
        try:
            session.file.put(
                local_path,
                stage_path,
                auto_compress=False,
                overwrite=True,
            )
            print(f"     ✅ {display_name}")
            uploaded += 1
        except Exception as e:
            print(f"     ❌ {display_name}: {e}")
            upload_errors += 1

    if skipped:
        print(f"     ⏭️  {skipped} unchanged, {uploaded} uploaded")

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

    if not main_file_present:
        print(f"\n  ⚠️  Skipping CREATE NOTEBOOK — {main_file} is not present "
              f"locally yet. Re-run this deploy once it exists.")
        return False

    live_version_ok = True

    # ── Create the Notebook object ───────────────────────────────────────
    print(f"\n  🚀 Creating Notebook {notebook_name}...")
    create_sql = f"""
    CREATE OR REPLACE NOTEBOOK {notebook_name}
        FROM '@{stage_name}'
        MAIN_FILE       = '{main_file}'
        QUERY_WAREHOUSE = '{config.WAREHOUSE}'
        COMMENT         = 'Adjustment test harness — end-to-end scenario runner (seed/preview/submit/approve/wait/verify/cleanup).'
    """
    try:
        session.sql(create_sql).collect()
        print(f"     ✅ Notebook created successfully!")
    except Exception as e:
        print(f"     ❌ Notebook creation failed: {e}")
        return False

    # ── Activate a live version ──────────────────────────────────────────
    # CREATE NOTEBOOK ... FROM stage registers the object, but it is the
    # "ADD LIVE VERSION FROM LAST" step that makes the staged files runnable
    # as the notebook's live version in Snowsight. This syntax is
    # comparatively new — if this account's Snowflake release rejects it,
    # print an actionable message rather than letting the exception kill the
    # rest of the deploy; the notebook object still exists and can be
    # activated by hand.
    print(f"\n  🔄 Activating live version...")
    try:
        session.sql(f"ALTER NOTEBOOK {notebook_name} ADD LIVE VERSION FROM LAST").collect()
        print(f"     ✅ Live version active")
    except Exception as e:
        err_msg = " ".join(str(e).split())[:300]
        print(f"     ⚠️  Could not activate live version automatically: {err_msg}")
        print(f"     ⚠️  ACTION: open the notebook in Snowsight and activate it "
              f"manually (notebook page -> \"•••\" menu -> \"Add live version\"), "
              f"or confirm this Snowflake account's release supports "
              f"'ALTER NOTEBOOK ... ADD LIVE VERSION FROM LAST'.")
        # Deliberately NOT a deploy failure. The notebook object exists and is
        # usable; only the convenience of a pre-activated version is missing.
        # Failing the deploy over an optional statement is the mistake
        # 16_grants.sql was renamed to avoid.
        live_version_ok = False

    # ── Grant access ─────────────────────────────────────────────────────
    print(f"\n  🔐 Granting USAGE on Notebook...")
    try:
        for _role in (config.ROLE_OWNER, config.ROLE_RO):
            try:
                session.sql(f"GRANT USAGE ON NOTEBOOK {notebook_name} "
                            f"TO ROLE {_role}").collect()
                print(f"     ✅ USAGE granted to {_role}")
            except Exception as e:
                # ROLE_OWNER already owns the notebook, so its grant is a
                # no-op that some releases reject. Neither grant is required
                # for the notebook to run.
                print(f"     ℹ️  {_role}: {str(e)[:100]}")
    except Exception as e:
        print(f"     ℹ️  Grant note: {str(e)[:100]}")

    # ── Show details ─────────────────────────────────────────────────────
    try:
        info = session.sql(f"SHOW NOTEBOOKS LIKE 'ADJUSTMENT_TEST_HARNESS' IN SCHEMA ADJUSTMENT_APP").collect()
        if info:
            print(f"\n  🌐 Notebook details:")
            for row in info:
                for key in row.asDict():
                    if key.upper() in ('NAME', 'DATABASE_NAME', 'SCHEMA_NAME', 'QUERY_WAREHOUSE'):
                        print(f"     {key}: {row[key]}")
    except Exception:
        pass

    if live_version_ok:
        print(f"\n  ✅ Notebook deployed successfully!")
    else:
        print(f"\n  ✅ Notebook deployed — live-version activation needs a "
              f"one-off manual step, see ACTION above. Not a deploy failure.")
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
        'ADJUSTMENT_APP.TASK_SYNC_SIGNOFF',
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
    parser.add_argument('--notebooks-only', action='store_true', help='Deploy Snowflake Notebooks only')
    parser.add_argument('--sample-app', action='store_true',
                        help='Deploy ONLY sample_app/ (the bare line-probe control app) as ADJUSTMENT_APP.LINE_PROBE. Touches nothing else.')
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

    deploy_db = not (args.streamlit_only or args.notebooks_only)
    deploy_st = not (args.db_only or args.notebooks_only)
    deploy_nb = not (args.db_only or args.streamlit_only)
    if args.rebuild:
        deploy_db = True   # rebuild always reapplies DB objects after teardown
    if args.validate_only:
        deploy_db = deploy_st = deploy_nb = False
    if args.sample_app:
        deploy_db = deploy_st = deploy_nb = False

    print("=" * 64)
    print("  Adjustment Engine — Snowflake Deployment")
    print(f"  Database: {config.DATABASE}")
    print(f"  Schema:   {config.SCHEMA}")
    print("=" * 64)

    # ── Connect ──────────────────────────────────────────────────────────
    print("\n  🔌 Connecting to Snowflake...")
    _connect_t0 = time.time()
    try:
        session = get_session()
        ctx = session.sql("SELECT CURRENT_ROLE() AS R, CURRENT_WAREHOUSE() AS W, CURRENT_DATABASE() AS D").collect()[0]
        _connect_secs = time.time() - _connect_t0
        _PHASE_TIMES.append(("connect + warehouse resume", _connect_secs))
        print(f"     ✅ Connected — Role: {ctx['R']}, Warehouse: {ctx['W']}, "
              f"Database: {ctx['D']} ({_connect_secs:.1f}s)")
    except Exception as e:
        print(f"     ❌ Connection failed: {e}")
        sys.exit(1)

    success = True

    # ── Validate-only mode ───────────────────────────────────────────────
    if args.validate_only:
        ok = validate_schema(session)
        session.close()
        return 0 if ok else 1

    # ── Line-probe control app only ──────────────────────────────────────
    if args.sample_app:
        ok = deploy_sample_app(session)
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
        with phase_timer("resume pipeline tasks"):
            resume_pipeline_tasks(session)

        print("\n" + "─" * 64)
        print("  PHASE 1c: Validate schema")
        print("─" * 64)
        with phase_timer("validate schema"):
            _schema_ok = validate_schema(session)
        if not _schema_ok:
            print("\n  ⚠️  Schema validation FAILED — adjustments may error until fixed.")
            success = False

    # ── Deploy Streamlit app ─────────────────────────────────────────────
    if deploy_st:
        print("\n" + "─" * 64)
        print("  PHASE 2: Streamlit Application")
        print("─" * 64)
        with phase_timer("streamlit app"):
            _st_ok = deploy_streamlit_app(session)
        if not _st_ok:
            print("\n  ⚠️  Streamlit deployment had errors — review above.")
            success = False

    # ── Deploy Notebooks ─────────────────────────────────────────────────
    if deploy_nb:
        print("\n" + "─" * 64)
        print("  PHASE 2b: Snowflake Notebooks")
        print("─" * 64)
        with phase_timer("notebooks"):
            _nb_ok = deploy_notebooks(session)
        if not _nb_ok:
            print("\n  ⚠️  Notebook deployment had errors — review above.")
            success = False

    # ── Submit test adjustment ──────────────────────────────────────────
    if args.test_adj:
        print("\n" + "─" * 64)
        print("  PHASE 3: Submit Test Adjustment (VaR Flatten)")
        print("─" * 64)
        with phase_timer("test adjustment"):
            _adj_ok = submit_test_adjustment(session)
        if not _adj_ok:
            print("\n  ⚠️  Test adjustment submission failed — review above.")

    # ── Summary ──────────────────────────────────────────────────────────
    print_timing_breakdown()

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
