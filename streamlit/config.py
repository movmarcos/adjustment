"""
config.py — single source of truth for environment-specific Snowflake names.
================================================================================
Every name derives from ONE decision: which database this deployment targets.
Nothing else in the repo hardcodes an environment prefix.

The target is resolved in this order, first hit wins:

  1. deploy_target.py next to this file — written by deploy.py and uploaded
     with the app, so a deployed Streamlit app knows the environment it was
     deployed INTO. (Streamlit in Snowflake sets no environment variables,
     so without this an app deployed to TEST would read the default below
     and query the DVLP database by name — pages/10_Tasks_Cost.py does
     exactly that.)
  2. ADJ_DB — the full database name, e.g. TEST_RAPTOR_NEWADJ_4. Set by
     deploy.ps1 -DbTarget. The ENVIRONMENT IS THE FIRST FOUR LETTERS.
  3. ADJ_ENV — the environment on its own (DVLP | TEST | RLSE | PROD); the
     database name is then derived from it.
  4. The DEFAULT_ENV below.

To deploy:  .\\deploy.ps1 -DbTarget TEST_RAPTOR_NEWADJ_4
        or: python deploy.py            (targets the default / ADJ_* vars)
"""
import os

# ─── Environments ───────────────────────────────────────────────────────────
VALID_ENVS = ("DVLP", "TEST", "RLSE", "PROD")
DEFAULT_ENV = "DVLP"

#: How a database name is built from an environment when only the environment
#: is known. A database passed explicitly is used verbatim — it does NOT have
#: to match this pattern.
DATABASE_PATTERN = "{env}_RAPTOR_NEWADJ_4"


def env_from_database(database: str) -> str:
    """The environment a database belongs to: its first four letters.

    'TEST_RAPTOR_NEWADJ_4' → 'TEST'. Raises on anything outside VALID_ENVS,
    so a typo in -DbTarget stops the deploy at the first line rather than
    creating objects in a database nobody meant to touch.
    """
    name = str(database or "").strip().upper()
    prefix = name[:4]
    if prefix not in VALID_ENVS:
        raise ValueError(
            f"Database '{database}' does not name an environment: its first "
            f"four letters are '{prefix}', expected one of {list(VALID_ENVS)}.")
    return prefix


def _resolve():
    """(env, database) from the deploy target, the environment, or the default."""
    # 1. Baked in at deploy time (see deploy.py: _write_deploy_target).
    try:
        import deploy_target                      # noqa: F401
        db = str(getattr(deploy_target, "DATABASE", "") or "").strip().upper()
        if db:
            return env_from_database(db), db
    except ImportError:
        pass

    # 2. A database named explicitly (deploy.ps1 -DbTarget).
    db = str(os.environ.get("ADJ_DB", "") or "").strip().upper()
    if db:
        return env_from_database(db), db

    # 3. An environment named explicitly.
    env = str(os.environ.get("ADJ_ENV", "") or "").strip().upper()
    if env:
        if env not in VALID_ENVS:
            raise ValueError(f"ADJ_ENV '{env}' is not one of {list(VALID_ENVS)}")
        return env, DATABASE_PATTERN.format(env=env)

    # 4. Default.
    return DEFAULT_ENV, DATABASE_PATTERN.format(env=DEFAULT_ENV)


ENV, DATABASE = _resolve()

# ─── Derived names ──────────────────────────────────────────────────────────
SCHEMA     = "ADJUSTMENT_APP"         # app schema (same name in every environment)
WAREHOUSE  = f"{ENV}_RAPTOR_WH"       # THE warehouse: deploy session, Streamlit
                                      # QUERY_WAREHOUSE and dynamic tables all use
                                      # this one (RAVEN_WH_M / RAPTOR_WH_XS retired)
DT_WH      = WAREHOUSE                # kept as a separate token for the SQL files
ROLE_OWNER = f"{ENV}_RAPTOR_OWNER"    # owning role (deploy + grants)
ROLE_RO    = f"{ENV}_RAPTOR_RO"       # read-only role (grants)

# ─── Connection (deploy.py, tests/live/, scratch scripts) ───────────────────
SF_CONN_ENV = ENV.lower()             # MufgSnowflakeConn environment name
# Service-account prefix is per environment: apd/apt/apr/app.
_USER_PREFIX = {"DVLP": "apd", "TEST": "apt", "RLSE": "apr", "PROD": "app"}[ENV]
DEPLOY_USER = f"{_USER_PREFIX}_raptor_sfk_depl@mufgsecurities.com"


# ─── SQL placeholder substitution ───────────────────────────────────────────
# deploy.py calls render() on each .sql file before executing it, replacing
# {{TOKEN}} markers with the values above. SQL files with no markers pass
# through unchanged.
_TOKENS = {
    "DATABASE":   DATABASE,
    "SCHEMA":     SCHEMA,
    "WAREHOUSE":  WAREHOUSE,
    "DT_WH":      DT_WH,
    "ROLE_OWNER": ROLE_OWNER,
    "ROLE_RO":    ROLE_RO,
}


def render(sql_text: str) -> str:
    """Replace {{TOKEN}} placeholders in a SQL string with config values."""
    for token, value in _TOKENS.items():
        sql_text = sql_text.replace("{{" + token + "}}", value)
    return sql_text
