"""
config.py — single source of truth for environment-specific Snowflake names.
================================================================================
Every name derives from ENV — nothing else in the repo hardcodes an
environment prefix. Environments: DVLP | TEST | RLSE | PROD.

To target another environment either:
    set ADJ_ENV=TEST            # env var, wins when present
or edit the ENV default below. Every consumer (deploy.py, tests/, the SQL
files via {{TOKEN}} rendering, and the Streamlit app) reads from here.
In Streamlit-in-Snowflake ADJ_ENV is not set, so the deployed app uses the
ENV default — deploy each environment with its own value.

To deploy: python deploy.py
"""
import os

# ─── Environment ────────────────────────────────────────────────────────────
ENV = os.environ.get("ADJ_ENV", "DVLP").upper()   # DVLP | TEST | RLSE | PROD

_VALID_ENVS = {"DVLP", "TEST", "RLSE", "PROD"}
if ENV not in _VALID_ENVS:
    raise ValueError(f"ADJ_ENV '{ENV}' is not one of {sorted(_VALID_ENVS)}")

# ─── Derived names ──────────────────────────────────────────────────────────
DATABASE   = f"{ENV}_RAPTOR_NEWADJ"   # app database (deploy session + app + tests)
SCHEMA     = "ADJUSTMENT_APP"         # app schema (same name in every environment)
WAREHOUSE  = f"{ENV}_RAPTOR_WH"       # THE warehouse: deploy session, Streamlit
                                      # QUERY_WAREHOUSE and dynamic tables all use
                                      # this one (RAVEN_WH_M / RAPTOR_WH_XS retired)
DT_WH      = WAREHOUSE                # kept as a separate token for the SQL files
ROLE_OWNER = f"{ENV}_RAPTOR_OWNER"    # owning role (deploy + grants)
ROLE_RO    = f"{ENV}_RAPTOR_RO"       # read-only role (grants)
PROD_DB    = "PROD_RAPTOR"            # cross-DB validation compare target

# ─── Connection (deploy.py, tests/, scratch scripts) ────────────────────────
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
    "PROD_DB":    PROD_DB,
}


def render(sql_text: str) -> str:
    """Replace {{TOKEN}} placeholders in a SQL string with config values."""
    for token, value in _TOKENS.items():
        sql_text = sql_text.replace("{{" + token + "}}", value)
    return sql_text
