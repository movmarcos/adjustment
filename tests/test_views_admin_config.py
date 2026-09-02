"""
Read-only checks: view compilation (the class of bug that broke
VW_ADJUSTMENT_TRACK post-deploy), admin seeds, and role-membership
resolution. Safe to run any time — they write nothing.
"""
import pytest

from conftest import FAKE_COB

# Every app-facing view: a SELECT ... LIMIT 1 forces full compilation, which
# is exactly where "Unsupported subquery type" and column drift surface.
VIEWS = [
    ("REG: VW_ADJUSTMENT_TRACK compiles", "ADJUSTMENT_APP.VW_ADJUSTMENT_TRACK"),
    ("VW_SIGNOFF_STATUS compiles", "ADJUSTMENT_APP.VW_SIGNOFF_STATUS"),
    ("FRT-01: VW_DIRECT_FRTB_ENRICHED compiles", "ADJUSTMENT_APP.VW_DIRECT_FRTB_ENRICHED"),
    ("FRT-02: VW_DIRECT_FRTBDRC_ENRICHED compiles", "ADJUSTMENT_APP.VW_DIRECT_FRTBDRC_ENRICHED"),
    ("FRT-03: VW_DIRECT_FRTBRRAO_ENRICHED compiles", "ADJUSTMENT_APP.VW_DIRECT_FRTBRRAO_ENRICHED"),
]


@pytest.mark.uat("VIEW-01", title="All app views compile (subquery/column drift)", priority="P1")
def test_views_compile(session, ev):
    failures = []
    for label, view in VIEWS:
        try:
            ev.sql(label, f"SELECT * FROM {view} LIMIT 1", max_rows=0)
        except Exception as ex:
            failures.append(f"{view}: {str(ex).splitlines()[0][:150]}")
    ev.check("every view compiles and is selectable: "
             + ("; ".join(failures) if failures else "all OK"),
             not failures)


@pytest.mark.uat("ADM-00", title="Admin seeds present (BI_DEVELOPER + named admin)", priority="P1")
def test_admin_seeds(session, ev):
    r = ev.sql("Seeded admin rows",
               """SELECT USERNAME, COALESCE(ADMIN_TYPE, 'USER') AS ADMIN_TYPE, IS_ACTIVE
                  FROM ADJUSTMENT_APP.ADJ_ADMINS
                  WHERE UPPER(USERNAME) IN
                        ('BI_DEVELOPER', 'MICHELANGELO.ALIBERTI@MUFGSECURITIES.COM')""")
    names = {(x["USERNAME"].upper(), x["ADMIN_TYPE"].upper()) for x in r
             if x["IS_ACTIVE"]}
    ev.check("BI_DEVELOPER seeded as ROLE admin",
             ("BI_DEVELOPER", "ROLE") in names)
    ev.check("Michelangelo Aliberti seeded as USER admin",
             ("MICHELANGELO.ALIBERTI@MUFGSECURITIES.COM", "USER") in names)


@pytest.mark.uat("ADM-02", title="BI_DEVELOPER membership resolvable (incl. nested)", priority="P1")
def test_role_membership_resolution(session, ev):
    """Runs BOTH resolution paths the Admin page uses. At least one must
    work in this environment, or the in-app role check cannot either."""
    resolved = {}
    try:
        rows_ = ev.sql("SHOW GRANTS OF ROLE (direct grants)",
                       'SHOW GRANTS OF ROLE "BI_DEVELOPER"', max_rows=5)
        resolved["SHOW GRANTS"] = len(rows_)
    except Exception as ex:
        ev.note("SHOW GRANTS failed", str(ex).splitlines()[0][:200])
    try:
        rows_ = ev.sql(
            "ACCOUNT_USAGE nested membership",
            """WITH RECURSIVE holders AS (
                   SELECT 'BI_DEVELOPER' AS ROLE_NAME
                   UNION ALL
                   SELECT g.GRANTEE_NAME
                   FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES g
                   JOIN holders h ON UPPER(g.NAME) = h.ROLE_NAME
                   WHERE g.GRANTED_ON = 'ROLE' AND g.GRANTED_TO = 'ROLE'
                     AND g.PRIVILEGE = 'USAGE' AND g.DELETED_ON IS NULL)
               SELECT COUNT(DISTINCT u.GRANTEE_NAME) AS MEMBERS
               FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_USERS u
               JOIN holders h ON UPPER(u.ROLE) = h.ROLE_NAME
               WHERE u.DELETED_ON IS NULL""")
        resolved["ACCOUNT_USAGE"] = int(rows_[0]["MEMBERS"]) if rows_ else 0
    except Exception as ex:
        ev.note("ACCOUNT_USAGE failed", str(ex).splitlines()[0][:200])
    ev.note("Resolution summary", str(resolved))
    ev.check("at least one membership source works from this environment "
             "(if none do, the Admin page role check needs a grant: "
             "MANAGE GRANTS or IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE)",
             bool(resolved))


@pytest.mark.uat("CFG-01", title="Scope + feed configuration sane", priority="P2")
def test_config_sanity(session, ev):
    r = ev.sql("Active scopes",
               """SELECT PROCESS_TYPE FROM ADJUSTMENT_APP.ADJUSTMENTS_SETTINGS
                  WHERE IS_ACTIVE = TRUE""")
    scopes = {str(x["PROCESS_TYPE"]).upper() for x in r}
    ev.check("core scopes active (VaR, Stress, Sensitivity, FRTB)",
             {"VAR", "STRESS", "SENSITIVITY", "FRTB"} <= scopes)
    f = ev.sql("Feed reachable",
               """SELECT COUNT(*) AS N FROM ADJUSTMENT_APP.ADJ_APP_CONFIG
                  WHERE CONFIG_KEY IN ('SIGNOFF_FEED_TABLE', 'SIGNOFF_FEED_ENABLED')""")
    ev.note("Feed config keys present", str(f[0]["N"]) if f else "0")
