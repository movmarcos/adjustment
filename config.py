"""
config.py — single source of truth for environment-specific Snowflake names.
================================================================================
Change these values once to retarget the whole solution at a different database,
warehouse, or set of roles. Every consumer (deploy.py, test_process.py, the SQL
files, and the Streamlit app) reads from here — there are no DB names hardcoded
anywhere else outside the _OLD/ archive.

To deploy to a new environment: edit the constants below, then run
    python deploy.py
"""

# ─── Environment identifiers ────────────────────────────────────────────────
DATABASE   = "DVLP_RAPTOR_NEWADJ"   # app database (deploy session + app + tests)
SCHEMA     = "ADJUSTMENT_APP"       # app schema (same name in every environment)
WAREHOUSE  = "DVLP_RAVEN_WH_M"      # deploy session + Streamlit QUERY_WAREHOUSE
DT_WH      = "DVLP_RAPTOR_WH_XS"    # dynamic-table refresh warehouse
PROCESS_WH = "DVLP_RAVEN_WH_M"      # heavy-roll processing task (provisioned, not serverless)
ROLE_OWNER = "DVLP_RAPTOR_OWNER"    # owning role (deploy + grants)
ROLE_RO    = "DVLP_RAPTOR_RO"       # read-only role (grants)
PROD_DB    = "PROD_RAPTOR"          # cross-DB validation compare target


# ─── SQL placeholder substitution ───────────────────────────────────────────
# deploy.py calls render() on each .sql file before executing it, replacing
# {{TOKEN}} markers with the values above. SQL files with no markers pass
# through unchanged.
_TOKENS = {
    "DATABASE":   DATABASE,
    "SCHEMA":     SCHEMA,
    "WAREHOUSE":  WAREHOUSE,
    "DT_WH":      DT_WH,
    "PROCESS_WH": PROCESS_WH,
    "ROLE_OWNER": ROLE_OWNER,
    "ROLE_RO":    ROLE_RO,
    "PROD_DB":    PROD_DB,
}


def render(sql_text: str) -> str:
    """Replace {{TOKEN}} placeholders in a SQL string with config values."""
    for token, value in _TOKENS.items():
        sql_text = sql_text.replace("{{" + token + "}}", value)
    return sql_text
