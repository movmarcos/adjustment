"""
Admin — Metadata & Configuration
===================================
View and manage ADJUSTMENTS_SETTINGS, recurring templates, and reference.
Reads from: ADJUSTMENTS_SETTINGS, ADJ_RECURRING_TEMPLATE, ADJ_HEADER (stats).
"""
import streamlit as st
import pandas as pd

st.set_page_config(page_title="Admin · MUFG", page_icon="⚙️", layout="wide", initial_sidebar_state="expanded")

from utils.styles import (inject_css, render_sidebar, section_title, P,
                          SCOPE_CONFIG, icon, render_df_table)
from utils.snowflake_conn import run_query, run_query_df, current_user_name, safe_rerun

def _esc(val):
    """Escape single quotes for safe SQL interpolation."""
    return str(val).replace("\\", "\\\\").replace("'", "''") if val is not None else ""

inject_css()
render_sidebar()

user = current_user_name()

# ──────────────────────────────────────────────────────────────────────────────
# AUTHORIZATION GATE
# This page manages approvers and scope config — the controls the
# 4-eyes workflow depends on — so access must itself be controlled. While
# ADJ_ADMINS is empty the page runs in bootstrap mode (open, with a warning)
# so the first admin can be registered in the Approvers tab.
# ──────────────────────────────────────────────────────────────────────────────
def _role_members(role: str):
    """Usernames holding a Snowflake role — INCLUDING through nested role
    grants (role granted to role granted to user), which is how AD/SCIM group
    membership usually lands. Two sources, tried in order:
      1. SHOW GRANTS OF ROLE, walked recursively — live, but the app's
         owner's-rights session may not be allowed to run SHOW at all;
      2. SNOWFLAKE.ACCOUNT_USAGE grant views with a recursive CTE — needs
         IMPORTED PRIVILEGES on the SNOWFLAKE database and lags up to ~2h,
         which is fine for admin membership.
    Cached per session. Returns None only when BOTH sources fail; each
    source's error is kept in _admin_role_diag for the diagnostics expander."""
    cache = st.session_state.setdefault("_admin_role_members", {})
    diag = st.session_state.setdefault("_admin_role_diag", {})
    if role in cache:
        return cache[role]
    safe = str(role).replace('"', "").strip().upper()
    errors = {}

    # Source 1 — SHOW GRANTS OF ROLE, expanded through the role hierarchy:
    # users of the role itself, plus users of every role this role has been
    # granted TO (those users inherit it). Depth-capped for safety.
    try:
        members, to_visit, visited = set(), [safe], set()
        while to_visit and len(visited) < 25:
            r = to_visit.pop()
            if r in visited:
                continue
            visited.add(r)
            for row in run_query(f'SHOW GRANTS OF ROLE "{r}"') or []:
                d = row.as_dict() if hasattr(row, "as_dict") else dict(row)
                granted_to = str(d.get("granted_to") or d.get("GRANTED_TO") or "").upper()
                grantee = str(d.get("grantee_name") or d.get("GRANTEE_NAME") or "").strip()
                if not grantee:
                    continue
                if granted_to == "USER":
                    members.add(grantee.upper())
                elif granted_to == "ROLE":
                    to_visit.append(grantee.replace('"', "").upper())
        cache[role] = members
        diag[role] = {"source": "SHOW GRANTS OF ROLE (nested roles expanded)",
                      "count": len(members), "errors": errors}
        return members
    except Exception as ex:
        errors["SHOW GRANTS OF ROLE"] = str(ex).split("\n")[0][:200]

    # Source 2 — ACCOUNT_USAGE grant views: same expansion as a recursive
    # CTE (roles the target was granted to, then every user holding any
    # role in that set).
    try:
        _lit = safe.replace("'", "''")
        rows = run_query(f"""
            WITH RECURSIVE holders AS (
                SELECT '{_lit}' AS ROLE_NAME
                UNION ALL
                SELECT g.GRANTEE_NAME
                FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES g
                JOIN holders h ON UPPER(g.NAME) = h.ROLE_NAME
                WHERE g.GRANTED_ON = 'ROLE' AND g.GRANTED_TO = 'ROLE'
                  AND g.PRIVILEGE = 'USAGE' AND g.DELETED_ON IS NULL
            )
            SELECT DISTINCT UPPER(u.GRANTEE_NAME) AS USERNAME
            FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_USERS u
            JOIN holders h ON UPPER(u.ROLE) = h.ROLE_NAME
            WHERE u.DELETED_ON IS NULL
        """)
        members = {str(r["USERNAME"]).strip().upper()
                   for r in rows or [] if r["USERNAME"]}
        cache[role] = members
        diag[role] = {"source": "ACCOUNT_USAGE grant views (nested roles, ≤2h behind)",
                      "count": len(members), "errors": errors}
        return members
    except Exception as ex:
        errors["ACCOUNT_USAGE"] = str(ex).split("\n")[0][:200]

    cache[role] = None
    diag[role] = {"source": None, "count": None, "errors": errors}
    return None


_admin_bootstrap = False
try:
    _admin_rows = run_query(
        "SELECT USERNAME, COALESCE(ADMIN_TYPE, 'USER') AS ADMIN_TYPE "
        "FROM ADJUSTMENT_APP.ADJ_ADMINS WHERE IS_ACTIVE = TRUE")
except Exception:
    try:  # pre-ADMIN_TYPE deployment
        _admin_rows = run_query(
            "SELECT USERNAME, 'USER' AS ADMIN_TYPE "
            "FROM ADJUSTMENT_APP.ADJ_ADMINS WHERE IS_ACTIVE = TRUE")
    except Exception:
        _admin_rows = []
if not _admin_rows:
    _admin_bootstrap = True
    st.warning(
        "**No page administrators are configured yet** — this Admin page is "
        "currently open to every user. Add the first administrator in the "
        "*Approvers* tab to lock it down.")
else:
    def _identity_keys(name: str) -> set:
        """Comparable forms of an identity. The app-resolved viewer name is
        often the EMAIL (st.user, when READ SESSION is absent) while SHOW
        GRANTS OF ROLE returns Snowflake USERNAMES — frequently the same
        person as 'MARCOS.MAGRI@BANK.COM' vs 'MARCOS.MAGRI'. Compare on the
        full string AND the local part (before '@'), both upper-cased."""
        n = str(name or "").strip().upper()
        if not n:
            return set()
        keys = {n}
        if "@" in n:
            keys.add(n.split("@", 1)[0])
        return keys

    _me_keys = _identity_keys(user)
    _admin_users = {str(r["USERNAME"]).strip().upper() for r in _admin_rows
                    if str(r["ADMIN_TYPE"]).upper() == "USER"}
    _admin_roles = [str(r["USERNAME"]).strip().upper() for r in _admin_rows
                    if str(r["ADMIN_TYPE"]).upper() == "ROLE"]
    _is_admin = any(_identity_keys(u) & _me_keys for u in _admin_users)
    _roles_unresolved = []
    _role_report = {}
    if not _is_admin:
        for _role in _admin_roles:
            _members = _role_members(_role)
            if _members is None:
                _roles_unresolved.append(_role)
                _role_report[_role] = None
            else:
                _role_report[_role] = len(_members)
                _member_keys = set()
                for m in _members:
                    _member_keys |= _identity_keys(m)
                if _me_keys & _member_keys:
                    _is_admin = True
                    break
    if not _is_admin and _roles_unresolved and not _admin_users:
        # Admin roles exist but none could be resolved AND no user entries to
        # fall back on — locking everyone out here would be unrecoverable
        # from inside the app, so stay open with a loud warning instead.
        _admin_bootstrap = True
        st.warning(
            "**Admin-role membership could not be verified** ("
            + ", ".join(_roles_unresolved) +
            ") — the app's owner role cannot run SHOW GRANTS OF ROLE. The "
            "page stays open so this can be fixed: either grant the "
            "privilege, or add named user administrators in the Approvers "
            "tab.")
        _is_admin = True
    if not _is_admin:
        st.markdown("## Admin — Configuration")
        st.error(
            f"You ({user}) are not authorized to use the Admin page. "
            f"Access is granted to listed administrators"
            + (f" and members of: {', '.join(_admin_roles)}" if _admin_roles else "")
            + ". Ask an existing administrator to add you in the Approvers tab.")
        # Diagnostics so a wrong denial is debuggable on the spot instead of
        # a guessing game (identity-format mismatch, unresolvable role, or a
        # nested grant SHOW GRANTS OF ROLE cannot see).
        with st.expander("Why am I not authorized? (diagnostics)"):
            st.markdown(
                f"- Your resolved identity: **{user}** "
                f"(compared as: {', '.join(sorted(_me_keys)) or '—'})")
            _diag_all = st.session_state.get("_admin_role_diag", {})
            for _role in _admin_roles:
                _n = _role_report.get(_role, "not checked")
                _d = _diag_all.get(_role) or {}
                _errs = _d.get("errors") or {}
                if _n is None:
                    _why = "; ".join(f"`{k}` failed: {v}"
                                     for k, v in _errs.items()) or "no detail"
                    st.markdown(
                        f"- Role **{_role}**: membership could **not** be "
                        f"verified by any method ({_why}). Fix: grant the "
                        f"app owner role MANAGE GRANTS, or IMPORTED "
                        f"PRIVILEGES on the SNOWFLAKE database (for the "
                        f"ACCOUNT_USAGE fallback) — or add yourself as a "
                        f"USER administrator.")
                else:
                    _src = _d.get("source") or "SHOW GRANTS OF ROLE"
                    st.markdown(
                        f"- Role **{_role}**: {_n} user(s) resolved via "
                        f"{_src}, nested role grants included — your "
                        f"identity did not match any of them. If you were "
                        f"granted the role in the last ~2 hours and the "
                        f"source above is ACCOUNT_USAGE, the lag may "
                        f"explain it.")
        st.stop()

st.markdown("## Admin — Configuration")
st.markdown(
    f"<span style='color:{P['grey_700']};font-size:0.9rem'>"
    "Manage scope configurations, recurring templates, and view system reference. "
    "In production, changes here write to <code>ADJUSTMENTS_SETTINGS</code> and "
    "<code>ADJ_RECURRING_TEMPLATE</code> in the ADJUSTMENT schema."
    "</span>", unsafe_allow_html=True)
st.markdown("<br/>", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────

# Sign-off management moved to its own page (5_Sign_Off.py) so every user
# can see the COB sign-off status; its mutating controls stay admin-gated there.
(tab_health, tab_scopes, tab_approvers, tab_recurring,
 tab_notify, tab_schema, tab_sql) = st.tabs([
    "System Health",
    "Scope Configuration",
    "Approvers",
    "Recurring Templates",
    "Notifications",
    "Schema Reference",
    "SQL Reference",
])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 0 — SYSTEM HEALTH
# ══════════════════════════════════════════════════════════════════════════════

with tab_health:
    section_title("System Health", "activity")
    st.caption("Live health of the processing pipeline. Anything red needs a "
               "human; amber is worth a look.")

    def _health_card(col, label, value, state, sub=""):
        color = {"ok": P["success"], "warn": "#B45309",
                 "bad": P["danger"]}.get(state, P["grey_700"])
        col.markdown(
            f'<div style="background:{P["white"]};border:1px solid {P["border"]};'
            f'border-top:3px solid {color};border-radius:8px;padding:0.75rem;'
            f'text-align:center;min-height:92px">'
            f'<div style="font-size:1.25rem;font-weight:800;color:{color}">{value}</div>'
            f'<div style="font-size:0.72rem;text-transform:uppercase;'
            f'letter-spacing:.05em;color:{P["grey_700"]};margin-top:2px">{label}</div>'
            + (f'<div style="font-size:0.68rem;color:{P["grey_700"]};'
               f'margin-top:2px">{sub}</div>' if sub else "")
            + '</div>', unsafe_allow_html=True)

    # ── Tasks ────────────────────────────────────────────────────────────────
    st.markdown("<div style='height:0.4rem'></div>", unsafe_allow_html=True)
    section_title("Tasks", "settings")
    try:
        _tasks = run_query("SHOW TASKS LIKE 'TASK_%' IN SCHEMA ADJUSTMENT_APP")
        if _tasks:
            tcols = st.columns(min(len(_tasks), 5))
            for i, t in enumerate(_tasks):
                name  = str(t["name"])
                state = str(t["state"]).lower()
                _health_card(
                    tcols[i % len(tcols)],
                    name.replace("TASK_", "").replace("_", " ").title(),
                    state.title(),
                    "ok" if state == "started" else "bad",
                    "polling" if state == "started" else "NOT RUNNING — resume it")
            if any(str(t["state"]).lower() != "started" for t in _tasks):
                st.error("One or more tasks are suspended — the pipeline is NOT "
                         "processing for those scopes. Ask the deploy owner to "
                         "run the resume step (ALTER TASK … RESUME).")
        else:
            st.warning("No tasks found in ADJUSTMENT_APP — has the deploy run?")
    except Exception as ex:
        st.info(f"Task state not available: {ex}")

    # ── Queue & processing ───────────────────────────────────────────────────
    section_title("Queue & Processing", "zap")
    try:
        _q = run_query("""
            SELECT
                COUNT(CASE WHEN RUN_STATUS IN ('Pending','Approved')
                           AND BLOCKED_BY_ADJ_ID IS NULL THEN 1 END) AS QUEUED,
                COUNT(CASE WHEN RUN_STATUS IN ('Pending','Approved')
                           AND BLOCKED_BY_ADJ_ID IS NOT NULL THEN 1 END) AS BLOCKED,
                COUNT(CASE WHEN RUN_STATUS = 'Running' THEN 1 END) AS RUNNING,
                COUNT(CASE WHEN RUN_STATUS = 'Failed'
                           AND PROCESS_DATE >= DATEADD(hour, -24,
                               CONVERT_TIMEZONE('Europe/London', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9))
                           THEN 1 END) AS FAILED_24H,
                DATEDIFF('minute',
                    MIN(CASE WHEN RUN_STATUS IN ('Pending','Approved')
                             AND BLOCKED_BY_ADJ_ID IS NULL THEN CREATED_DATE END),
                    CONVERT_TIMEZONE('Europe/London', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9)
                ) AS OLDEST_QUEUED_MIN,
                DATEDIFF('minute',
                    MIN(CASE WHEN RUN_STATUS = 'Running' THEN START_DATE END),
                    CONVERT_TIMEZONE('Europe/London', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9)
                ) AS OLDEST_RUNNING_MIN
            FROM ADJUSTMENT_APP.ADJ_HEADER
            WHERE IS_DELETED = FALSE
        """)
        q = _q[0] if _q else None
        if q is not None:
            oq  = q["OLDEST_QUEUED_MIN"]
            orn = q["OLDEST_RUNNING_MIN"]
            h1, h2, h3, h4, h5 = st.columns(5)
            _health_card(h1, "Queued", int(q["QUEUED"] or 0),
                         "warn" if oq is not None and int(oq) > 5 else "ok",
                         f"oldest {int(oq)} min" if oq is not None else "")
            _health_card(h2, "Blocked", int(q["BLOCKED"] or 0),
                         "warn" if int(q["BLOCKED"] or 0) else "ok",
                         "waiting behind overlaps")
            _health_card(h3, "Running", int(q["RUNNING"] or 0),
                         "bad" if orn is not None and int(orn) > 240
                         else ("warn" if orn is not None and int(orn) > 180 else "ok"),
                         f"oldest {int(orn)} min" if orn is not None else "")
            _health_card(h4, "Failed (24h)", int(q["FAILED_24H"] or 0),
                         "bad" if int(q["FAILED_24H"] or 0) else "ok",
                         "Retry from the Adjustments page")
            # PBI backlog (external table — best effort)
            try:
                _pbi = run_query("""
                    SELECT COUNT(*) AS C FROM METADATA.POWERBI_ACTION
                    WHERE START_TIME IS NULL
                      AND REQUEST_TIME >= DATEADD(day, -3, CURRENT_TIMESTAMP())
                """)
                pbi_waiting = int(_pbi[0]["C"]) if _pbi else 0
                _health_card(h5, "PBI refreshes waiting", pbi_waiting,
                             "warn" if pbi_waiting > 5 else "ok",
                             "picked up ~every 5 min")
            except Exception:
                _health_card(h5, "PBI refreshes waiting", "n/a", "warn",
                             "METADATA.POWERBI_ACTION not readable")
    except Exception as ex:
        st.info(f"Queue stats not available: {ex}")

    # ── Notifications ────────────────────────────────────────────────────────
    section_title("Notifications (24h)", "mail")
    try:
        _n = run_query("""
            SELECT
                COUNT(CASE WHEN STATUS = 'SENT' THEN 1 END)   AS SENT,
                COUNT(CASE WHEN STATUS = 'FAILED' THEN 1 END) AS FAILED,
                COUNT(CASE WHEN STATUS = 'SKIPPED_DISABLED' THEN 1 END) AS SKIPPED
            FROM ADJUSTMENT_APP.ADJ_NOTIFICATION_LOG
            WHERE CREATED_AT >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
        """)
        n = _n[0] if _n else None
        if n is not None:
            n1, n2, n3 = st.columns(3)
            _health_card(n1, "Sent", int(n["SENT"] or 0), "ok")
            _health_card(n2, "Failed", int(n["FAILED"] or 0),
                         "bad" if int(n["FAILED"] or 0) else "ok",
                         "see the Notifications tab log")
            _health_card(n3, "Skipped (disabled)", int(n["SKIPPED"] or 0),
                         "warn" if int(n["SKIPPED"] or 0) else "ok",
                         "master switch is off")
    except Exception as ex:
        st.info(f"Notification stats not available: {ex}")

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — SCOPE CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

with tab_scopes:
    section_title("Configured Data Sources (ADJUSTMENTS_SETTINGS)", "database")
    st.markdown(
        f'<div style="background:{P["info_lt"]};border:1px solid #90CAF9;border-radius:8px;'
        f'padding:0.7rem 1rem;margin-bottom:1rem;font-size:0.85rem">'
        f'This metadata drives the New Adjustment wizard. Each row represents a scope '
        f'(VaR, Stress, ES, etc.) with its fact table, primary key columns, and metrics. '
        f'Add a row to onboard a new data source without any code changes.'
        f'</div>',
        unsafe_allow_html=True)

    try:
        df_settings = run_query_df("""
            SELECT PROCESS_TYPE, FACT_TABLE, FACT_TABLE_PK,
                   METRIC_NAME, METRIC_USD_NAME,
                   CREATED_DATE
            FROM ADJUSTMENT_APP.ADJUSTMENTS_SETTINGS
            ORDER BY PROCESS_TYPE
        """)

        if not df_settings.empty:
            for _, row in df_settings.iterrows():
                scope = str(row["PROCESS_TYPE"])
                cfg = SCOPE_CONFIG.get(scope, {})
                with st.expander(f'{scope} — {row["FACT_TABLE"]}'):
                    c1, c2 = st.columns(2)
                    with c1:
                        section_title("Configuration")
                        st.markdown(
                            f'<table style="font-size:0.85rem;border-collapse:collapse;width:100%">'
                            f'<tr><td style="color:{P["grey_700"]};padding:4px 12px 4px 0;width:40%">Fact Table</td>'
                            f'<td><code>{row["FACT_TABLE"]}</code></td></tr>'
                            f'<tr><td style="color:{P["grey_700"]};padding:4px 12px 4px 0">Primary Key</td>'
                            f'<td><code>{row["FACT_TABLE_PK"]}</code></td></tr>'
                            f'<tr><td style="color:{P["grey_700"]};padding:4px 12px 4px 0">Metric (Local)</td>'
                            f'<td><code>{row["METRIC_NAME"]}</code></td></tr>'
                            f'<tr><td style="color:{P["grey_700"]};padding:4px 12px 4px 0">Metric (USD)</td>'
                            f'<td><code>{row["METRIC_USD_NAME"]}</code></td></tr>'
                            f'</table>',
                            unsafe_allow_html=True)
                    with c2:
                        section_title("Timestamps")
                        created = row.get("CREATED_DATE", "")
                        if hasattr(created, "strftime"):
                            created = created.strftime("%d %b %Y %H:%M")
                        st.markdown(
                            f'<div style="font-size:0.85rem">'
                            f'<strong>Created:</strong> {created}</div>',
                            unsafe_allow_html=True)
        else:
            st.info("No scope configurations found. Seed data may not be loaded yet.")
    except Exception as e:
        st.warning(f"Could not load settings: {e}")

    st.markdown("<br/>", unsafe_allow_html=True)
    section_title("Add / Edit Scope", "settings")
    st.markdown(
        f'<div style="background:{P["grey_100"]};border-radius:8px;padding:1rem;'
        f'font-size:0.85rem;color:{P["grey_700"]}">'
        f'To add a new data source:<br/>'
        f'1. Ensure the fact table exists and is accessible.<br/>'
        f'2. Insert a row into <code>ADJUSTMENT_APP.ADJUSTMENTS_SETTINGS</code> with the new PROCESS_TYPE.<br/>'
        f'3. The New Adjustment wizard will automatically include the new scope on next load.<br/>'
        f'No Streamlit code changes required.'
        f'</div>',
        unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — APPROVERS MANAGEMENT
# ══════════════════════════════════════════════════════════════════════════════

with tab_approvers:
    section_title("Authorized Approvers", "user")
    st.markdown(
        f'<span style="font-size:0.85rem;color:{P["grey_700"]}">'
        f'Users listed here can approve or reject adjustments in the Approval Queue. '
        f'A user can <strong>never</strong> approve their own adjustment regardless of this list. '
        f'Set <code>PROCESS_TYPE</code> to limit an approver to a specific scope, or leave blank for all scopes.'
        f'</span>',
        unsafe_allow_html=True)

    try:
        df_approvers = run_query_df("""
            SELECT APPROVER_ID, USERNAME, PROCESS_TYPE, IS_ACTIVE,
                   ADDED_BY, ADDED_DATE
            FROM ADJUSTMENT_APP.ADJ_APPROVERS
            ORDER BY IS_ACTIVE DESC, USERNAME
        """)

        if not df_approvers.empty:
            active_ct  = int(df_approvers[df_approvers["IS_ACTIVE"] == True].shape[0])
            inactive_ct = len(df_approvers) - active_ct
            st.markdown(
                f'<span style="font-size:0.85rem">'
                f'<strong style="color:{P["success"]}">{active_ct} active</strong> · '
                f'<strong style="color:{P["grey_700"]}">{inactive_ct} inactive</strong>'
                f'</span>',
                unsafe_allow_html=True)

            render_df_table(df_approvers, max_rows=200, height=300)

            # Deactivate / reactivate
            st.markdown("<br/>", unsafe_allow_html=True)
            section_title("Toggle Approver Status", "refresh-cw")
            toggle_cols = st.columns([2, 1, 1])
            with toggle_cols[0]:
                approver_options = [
                    f"{r['USERNAME']} (ID {r['APPROVER_ID']}) — {'Active' if r['IS_ACTIVE'] else 'Inactive'}"
                    for _, r in df_approvers.iterrows()
                ]
                sel_approver = st.selectbox("Select approver", approver_options, key="toggle_approver")
            with toggle_cols[1]:
                if st.button("Activate", key="activate_approver_btn"):
                    approver_id = int(sel_approver.split("ID ")[1].split(")")[0])
                    try:
                        run_query(f"""
                            UPDATE ADJUSTMENT_APP.ADJ_APPROVERS
                            SET IS_ACTIVE = TRUE
                            WHERE APPROVER_ID = {approver_id}
                        """)
                        st.success("Approver activated.")
                        safe_rerun()
                    except Exception as ex:
                        st.error(str(ex))
            with toggle_cols[2]:
                if st.button("Deactivate", key="deactivate_approver_btn"):
                    approver_id = int(sel_approver.split("ID ")[1].split(")")[0])
                    try:
                        run_query(f"""
                            UPDATE ADJUSTMENT_APP.ADJ_APPROVERS
                            SET IS_ACTIVE = FALSE
                            WHERE APPROVER_ID = {approver_id}
                        """)
                        st.success("Approver deactivated.")
                        safe_rerun()
                    except Exception as ex:
                        st.error(str(ex))
        else:
            st.info("No approvers configured yet. Add one below.")
    except Exception as e:
        st.info(f"Approvers table not available: {e}")

    st.markdown("<br/>", unsafe_allow_html=True)
    section_title("Add New Approver", "user")

    with st.form("new_approver_form"):
        ac1, ac2 = st.columns(2)
        with ac1:
            a_username = st.text_input("Username", placeholder="e.g. JSMITH", key="approver_user")
        with ac2:
            scope_options = ["All Scopes"] + list(SCOPE_CONFIG.keys())
            a_scope = st.selectbox("Scope (optional)", scope_options, key="approver_scope")

        a_submit = st.form_submit_button("Add Approver", type="primary")
        if a_submit:
            if not a_username.strip():
                st.error("Username is required.")
            else:
                try:
                    scope_val = "NULL" if a_scope == "All Scopes" else f"'{_esc(a_scope)}'"
                    run_query(f"""
                        INSERT INTO ADJUSTMENT_APP.ADJ_APPROVERS
                            (USERNAME, PROCESS_TYPE, IS_ACTIVE, ADDED_BY)
                        VALUES (UPPER('{_esc(a_username.strip())}'), {scope_val}, TRUE, '{_esc(user)}')
                    """)
                    st.success(f"Approver {a_username.strip().upper()} added successfully!")
                    safe_rerun()
                except Exception as ex:
                    st.error(f"Failed to add approver: {ex}")

    # ── Page administrators (gate for THIS Admin page) ────────────────────────
    st.markdown("<br/>", unsafe_allow_html=True)
    section_title("Page Administrators", "lock")
    st.caption(
        "Users listed here (active) — and every member of the listed Snowflake "
        "ROLES (e.g. BI_DEVELOPER) — can open the Admin page. While the list "
        "is empty the page is open to everyone — add the first administrator to "
        "lock it down. You cannot deactivate yourself.")
    try:
        df_admins = run_query_df("""
            SELECT ADMIN_ID, USERNAME, COALESCE(ADMIN_TYPE, 'USER') AS ADMIN_TYPE,
                   IS_ACTIVE, ADDED_BY, ADDED_DATE
            FROM ADJUSTMENT_APP.ADJ_ADMINS
            ORDER BY ADMIN_TYPE, USERNAME
        """)
        if not df_admins.empty:
            render_df_table(df_admins, max_rows=100, height=300)
            adm_cols = st.columns([2, 1, 1])
            with adm_cols[0]:
                admin_options = [
                    f"{r['USERNAME']} [{r['ADMIN_TYPE']}] (ID {r['ADMIN_ID']}) — {'Active' if r['IS_ACTIVE'] else 'Inactive'}"
                    for _, r in df_admins.iterrows()
                ]
                sel_admin = st.selectbox("Select administrator", admin_options,
                                         key="toggle_admin")
            _sel_admin_id   = int(sel_admin.split("ID ")[1].split(")")[0])
            _sel_admin_user = sel_admin.split(" [")[0].strip().upper()
            with adm_cols[1]:
                if st.button("Activate", key="activate_admin_btn"):
                    try:
                        run_query(f"""
                            UPDATE ADJUSTMENT_APP.ADJ_ADMINS
                            SET IS_ACTIVE = TRUE
                            WHERE ADMIN_ID = {_sel_admin_id}
                        """)
                        st.success("Administrator activated.")
                        safe_rerun()
                    except Exception as ex:
                        st.error(f"Failed to activate administrator: {ex}")
            with adm_cols[2]:
                _is_self = _sel_admin_user == str(user).strip().upper()
                if st.button("Deactivate", key="deactivate_admin_btn",
                             disabled=_is_self,
                             help="You cannot deactivate yourself." if _is_self else None):
                    try:
                        run_query(f"""
                            UPDATE ADJUSTMENT_APP.ADJ_ADMINS
                            SET IS_ACTIVE = FALSE
                            WHERE ADMIN_ID = {_sel_admin_id}
                        """)
                        st.success("Administrator deactivated.")
                        safe_rerun()
                    except Exception as ex:
                        st.error(f"Failed to deactivate administrator: {ex}")
        else:
            st.info("No page administrators yet — the Admin page is open to "
                    "everyone until the first one is added.")
    except Exception as e:
        st.info(f"Administrators table not available: {e}")

    with st.form("new_admin_form"):
        fa1, fa2 = st.columns([1, 2])
        with fa1:
            adm_type = st.selectbox(
                "Type", ["User", "Snowflake Role"], key="admin_type",
                help="User: one Snowflake username. Snowflake Role: every "
                     "user directly granted that role becomes an admin.")
        with fa2:
            adm_username = st.text_input(
                "Name", placeholder="e.g. JSMITH or BI_DEVELOPER",
                key="admin_user",
                help="Username as returned by CURRENT_USER(), or the role "
                     "name exactly as it exists in Snowflake.")
        adm_submit = st.form_submit_button("Add Administrator", type="primary")
        if adm_submit:
            if not adm_username.strip():
                st.error("Name is required.")
            else:
                try:
                    _t = "ROLE" if adm_type == "Snowflake Role" else "USER"
                    run_query(f"""
                        INSERT INTO ADJUSTMENT_APP.ADJ_ADMINS
                            (USERNAME, ADMIN_TYPE, IS_ACTIVE, ADDED_BY)
                        VALUES (UPPER('{_esc(adm_username.strip())}'), '{_t}',
                                TRUE, '{_esc(user)}')
                    """)
                    if _t == "ROLE":
                        _m = _role_members(adm_username.strip().upper())
                        if _m is None:
                            st.warning(
                                "Role added, but its membership could NOT be "
                                "verified (the app cannot run SHOW GRANTS OF "
                                "ROLE) — role-based access will not work "
                                "until that privilege is granted.")
                        else:
                            st.success(f"Role {adm_username.strip().upper()} "
                                       f"added — {len(_m)} member(s) resolve "
                                       f"as administrators.")
                    else:
                        st.success(f"Administrator {adm_username.strip().upper()} added.")
                    safe_rerun()
                except Exception as ex:
                    st.error(f"Failed to add administrator: {ex}")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — RECURRING TEMPLATES
# ══════════════════════════════════════════════════════════════════════════════

with tab_recurring:
    section_title("Recurring Adjustment Templates", "refresh-cw")
    st.markdown(
        f'<div style="background:{P["info_lt"]};border:1px solid #90CAF9;border-radius:8px;'
        f'padding:0.7rem 1rem;margin-bottom:1rem;font-size:0.85rem">'
        f'{icon("alert-triangle", size=13, color="#B45309")} <strong>Recurring '
        f'processing is not live yet.</strong> Templates saved here are stored '
        f'but nothing instantiates them automatically — the instantiation task '
        f'is planned for a future release. Until then, submit each COB\'s '
        f'adjustment manually from the New Adjustment page.'
        f'</div>',
        unsafe_allow_html=True)

    try:
        df_templates = run_query_df("""
            SELECT TEMPLATE_ID, PROCESS_TYPE, ADJUSTMENT_TYPE, ENTITY_CODE,
                   BOOK_CODE, DEPARTMENT_CODE, SCALE_FACTOR,
                   START_COBID, END_COBID, CRON_EXPRESSION,
                   IS_ACTIVE, CREATED_BY, CREATED_DATE
            FROM ADJUSTMENT_APP.ADJ_RECURRING_TEMPLATE
            ORDER BY IS_ACTIVE DESC, CREATED_DATE DESC
        """)

        if not df_templates.empty:
            active_ct   = int(df_templates[df_templates["IS_ACTIVE"] == True].shape[0])
            inactive_ct = len(df_templates) - active_ct
            st.markdown(
                f'<span style="font-size:0.85rem">'
                f'<strong style="color:{P["success"]}">{active_ct} active</strong> · '
                f'<strong style="color:{P["grey_700"]}">{inactive_ct} inactive</strong>'
                f'</span>',
                unsafe_allow_html=True)

            render_df_table(df_templates, max_rows=200, height=300)
        else:
            st.info("No recurring templates configured yet.")
    except Exception as e:
        st.info(f"Recurring templates table not available: {e}")

    st.markdown("<br/>", unsafe_allow_html=True)
    section_title("Create New Template", "calendar")

    with st.form("new_template_form"):
        tc1, tc2, tc3 = st.columns(3)
        with tc1:
            t_scope = st.selectbox("Scope (Process Type)", list(SCOPE_CONFIG.keys()), key="tmpl_scope")
            t_type  = st.selectbox("Adjustment Type", ["Flatten", "Scale", "Roll"], key="tmpl_type")
            t_scale = st.number_input("Scale Factor", value=1.0, min_value=-10.0, max_value=100.0,
                                      step=0.01, format="%.4f", key="tmpl_scale")
        with tc2:
            t_entity = st.text_input("Entity Code", placeholder="e.g. MUSE", key="tmpl_entity")
            t_book   = st.text_input("Book Code (optional)", key="tmpl_book")
            t_dept   = st.text_input("Department Code (optional)", key="tmpl_dept")
        with tc3:
            t_start = st.text_input("Start COBID", placeholder="e.g. 20260101", key="tmpl_start")
            t_end   = st.text_input("End COBID", placeholder="e.g. 20261231", key="tmpl_end")
            t_cron  = st.text_input("CRON Expression (optional)", placeholder="0 8 * * MON-FRI",
                                    key="tmpl_cron")

        submitted = st.form_submit_button("Create Template", type="primary")
        if submitted:
            if not t_start.strip() or not t_end.strip():
                st.error("Start and End COBID are required.")
            else:
                try:
                    book_val = f"'{_esc(t_book.strip())}'" if t_book.strip() else "NULL"
                    dept_val = f"'{_esc(t_dept.strip())}'" if t_dept.strip() else "NULL"
                    entity_val = f"'{_esc(t_entity.strip())}'" if t_entity.strip() else "NULL"
                    cron_val = f"'{_esc(t_cron.strip())}'" if t_cron.strip() else "NULL"

                    run_query(f"""
                        INSERT INTO ADJUSTMENT_APP.ADJ_RECURRING_TEMPLATE
                            (PROCESS_TYPE, ADJUSTMENT_TYPE, ENTITY_CODE, BOOK_CODE,
                             DEPARTMENT_CODE, SCALE_FACTOR, START_COBID, END_COBID,
                             CRON_EXPRESSION, IS_ACTIVE, CREATED_BY)
                        VALUES ('{_esc(t_scope)}', '{_esc(t_type)}', {entity_val}, {book_val},
                                {dept_val}, {float(t_scale)}, {int(t_start.strip())}, {int(t_end.strip())},
                                {cron_val}, TRUE, '{_esc(user)}')
                    """)
                    st.success("Template created successfully!")
                    safe_rerun()
                except Exception as ex:
                    st.error(f"Failed to create template: {ex}")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — NOTIFICATIONS
# ══════════════════════════════════════════════════════════════════════════════

with tab_notify:
    section_title("Email Notifications", "mail")
    st.markdown(
        f'<div style="background:{P["info_lt"]};border:1px solid #90CAF9;border-radius:8px;'
        f'padding:0.7rem 1rem;margin-bottom:1rem;font-size:0.85rem">'
        f'Emails are sent through a Snowflake <strong>email notification '
        f'integration</strong> created by the DBA team (see '
        f'<code>docs/TICKET_email_notification_integration.md</code>). '
        f'Everything here can be configured before that exists — nothing is '
        f'sent while the master switch is off (attempts are logged as '
        f'<code>SKIPPED_DISABLED</code>). Recipients must be Snowflake users '
        f'of this account with a <strong>verified</strong> profile email.'
        f'</div>',
        unsafe_allow_html=True)

    # ── Master switch + integration name ─────────────────────────────────────
    try:
        _cfg_rows = run_query("""
            SELECT CONFIG_KEY, CONFIG_VALUE FROM ADJUSTMENT_APP.ADJ_APP_CONFIG
            WHERE CONFIG_KEY IN ('NOTIFICATIONS_ENABLED', 'EMAIL_INTEGRATION')
        """)
        _cfg = {str(r["CONFIG_KEY"]): str(r["CONFIG_VALUE"] or "") for r in (_cfg_rows or [])}
    except Exception as ex:
        _cfg = {}
        st.warning(f"Could not read notification config: {ex}")

    _enabled_now = _cfg.get("NOTIFICATIONS_ENABLED", "false").strip().lower() == "true"
    _integ_now   = _cfg.get("EMAIL_INTEGRATION", "ADJ_EMAIL_INT")

    nc1, nc2, nc3 = st.columns([1, 2, 1])
    with nc1:
        n_enabled = st.toggle("Notifications enabled", value=_enabled_now,
                              key="ntf_enabled")
    with nc2:
        n_integ = st.text_input("Email integration name", value=_integ_now,
                                key="ntf_integration",
                                help="Exact name of the DBA-created notification "
                                     "integration (e.g. ADJ_EMAIL_INT).")
    with nc3:
        st.markdown("<br/>", unsafe_allow_html=True)
        if st.button("Save settings", key="ntf_save", type="primary",
                     use_container_width=True):
            try:
                for key, val in (("NOTIFICATIONS_ENABLED",
                                  "true" if n_enabled else "false"),
                                 ("EMAIL_INTEGRATION", n_integ.strip())):
                    run_query(f"""
                        UPDATE ADJUSTMENT_APP.ADJ_APP_CONFIG
                        SET CONFIG_VALUE = '{_esc(val)}',
                            UPDATED_BY = '{_esc(user)}',
                            UPDATED_AT = CURRENT_TIMESTAMP()
                        WHERE CONFIG_KEY = '{_esc(key)}'
                    """)
                st.success("Notification settings saved.")
                safe_rerun()
            except Exception as ex:
                st.error(f"Failed to save settings: {ex}")

    if not _enabled_now:
        st.caption("Master switch is OFF — events are logged but no email is "
                   "sent. Flip it on once the DBA ticket is done and the test "
                   "below succeeds.")

    # ── Test send (bypasses the master switch on purpose) ────────────────────
    section_title("Send Test Email", "send")
    t1, t2 = st.columns([2, 1])
    with t1:
        test_email = st.text_input(
            "Recipient (verified Snowflake-user email)", key="ntf_test_email",
            placeholder="e.g. your.name@mufg.com")
    with t2:
        st.markdown("<br/>", unsafe_allow_html=True)
        if st.button("Send test", key="ntf_test_btn", use_container_width=True,
                     disabled=not test_email.strip()):
            try:
                import json as _json
                _tp = _json.dumps({"email": test_email.strip()}).replace("\\", "\\\\").replace("'", "''")
                res = run_query(f"CALL ADJUSTMENT_APP.SP_NOTIFY('test', '{_tp}')")
                try:
                    out = _json.loads(str(res[0][0])) if res else {}
                except (ValueError, TypeError, IndexError):
                    out = {}
                if out.get("sent"):
                    st.success("Test email sent — check the inbox (and that "
                               "no-reply@snowflake.net is not filtered).")
                else:
                    st.warning(f"Test did not send: "
                               f"{out.get('errors') or out.get('status') or res[0][0]}")
            except Exception as ex:
                st.error(f"Test failed: {ex}")

    # ── Recipients / preferences ─────────────────────────────────────────────
    st.markdown("<br/>", unsafe_allow_html=True)
    section_title("Recipients", "user")
    st.caption(
        "Who gets notified of what. “My outcomes” = the user's own adjustments "
        "Processed/Failed. “Approvals” = new items in the Approval Queue and "
        "COB re-open requests (only useful for registered approvers).")
    try:
        df_prefs = run_query_df("""
            SELECT PREF_ID, USERNAME, EMAIL, NOTIFY_MY_OUTCOMES,
                   NOTIFY_APPROVALS, IS_ACTIVE, ADDED_BY, ADDED_DATE
            FROM ADJUSTMENT_APP.ADJ_NOTIFICATION_PREFS
            ORDER BY USERNAME
        """)
        if not df_prefs.empty:
            render_df_table(df_prefs, max_rows=100, height=300)
            pcols = st.columns([2, 1, 1])
            with pcols[0]:
                pref_options = [
                    f"{r['USERNAME']} (ID {r['PREF_ID']}) — "
                    f"{'Active' if r['IS_ACTIVE'] else 'Inactive'}"
                    for _, r in df_prefs.iterrows()
                ]
                sel_pref = st.selectbox("Select recipient", pref_options,
                                        key="ntf_pref_pick")
            _sel_pref_id = int(sel_pref.split("ID ")[1].split(")")[0])
            with pcols[1]:
                if st.button("Activate", key="ntf_pref_on"):
                    try:
                        run_query(f"""
                            UPDATE ADJUSTMENT_APP.ADJ_NOTIFICATION_PREFS
                            SET IS_ACTIVE = TRUE WHERE PREF_ID = {_sel_pref_id}
                        """)
                        st.success("Recipient activated.")
                        safe_rerun()
                    except Exception as ex:
                        st.error(f"Failed: {ex}")
            with pcols[2]:
                if st.button("Deactivate", key="ntf_pref_off"):
                    try:
                        run_query(f"""
                            UPDATE ADJUSTMENT_APP.ADJ_NOTIFICATION_PREFS
                            SET IS_ACTIVE = FALSE WHERE PREF_ID = {_sel_pref_id}
                        """)
                        st.success("Recipient deactivated.")
                        safe_rerun()
                    except Exception as ex:
                        st.error(f"Failed: {ex}")
        else:
            st.info("No recipients configured yet — add the first one below.")
    except Exception as ex:
        st.info(f"Preferences table not available: {ex}")

    with st.form("ntf_new_pref_form"):
        np1, np2 = st.columns(2)
        with np1:
            np_user = st.text_input("Snowflake username", placeholder="e.g. JSMITH",
                                    key="ntf_new_user")
        with np2:
            np_email = st.text_input("Email (verified on their Snowflake user)",
                                     placeholder="e.g. j.smith@mufg.com",
                                     key="ntf_new_email")
        np3, np4 = st.columns(2)
        with np3:
            np_outcomes = st.checkbox("Notify their adjustment outcomes",
                                      value=True, key="ntf_new_outcomes")
        with np4:
            np_approvals = st.checkbox("Notify approver events",
                                       value=False, key="ntf_new_approvals")
        np_submit = st.form_submit_button("Add Recipient", type="primary")
        if np_submit:
            if not np_user.strip() or not np_email.strip() or "@" not in np_email:
                st.error("Username and a valid email are required.")
            else:
                try:
                    run_query(f"""
                        INSERT INTO ADJUSTMENT_APP.ADJ_NOTIFICATION_PREFS
                            (USERNAME, EMAIL, NOTIFY_MY_OUTCOMES,
                             NOTIFY_APPROVALS, IS_ACTIVE, ADDED_BY)
                        VALUES (UPPER('{_esc(np_user.strip())}'),
                                '{_esc(np_email.strip())}',
                                {'TRUE' if np_outcomes else 'FALSE'},
                                {'TRUE' if np_approvals else 'FALSE'},
                                TRUE, '{_esc(user)}')
                    """)
                    st.success(f"Recipient {np_user.strip().upper()} added.")
                    safe_rerun()
                except Exception as ex:
                    st.error(f"Failed to add recipient: {ex}")

    # ── Recent notification log ──────────────────────────────────────────────
    st.markdown("<br/>", unsafe_allow_html=True)
    section_title("Recent Notifications", "clock")
    try:
        df_nlog = run_query_df("""
            SELECT CREATED_AT, EVENT_TYPE, STATUS, RECIPIENTS, SUBJECT, ERROR
            FROM ADJUSTMENT_APP.ADJ_NOTIFICATION_LOG
            ORDER BY CREATED_AT DESC
            LIMIT 50
        """)
        if df_nlog.empty:
            st.caption("No notification attempts yet.")
        else:
            render_df_table(df_nlog, max_rows=50, height=320)
    except Exception as ex:
        st.info(f"Notification log not available: {ex}")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 5 — SCHEMA REFERENCE
# ══════════════════════════════════════════════════════════════════════════════

with tab_schema:
    section_title("Database Schema Overview", "database")

    schema_items = [
        ("ADJUSTMENT_APP.ADJ_HEADER",            "TABLE",         "One row per adjustment — lifecycle, metadata, all dimension filters"),
        ("ADJUSTMENT_APP.ADJ_LINE_ITEM",         "TABLE",         "Explicit row-level values for Upload/Direct adjustments"),
        ("ADJUSTMENT_APP.ADJ_STATUS_HISTORY",    "TABLE",         "Append-only audit log of every status change"),
        ("ADJUSTMENT_APP.ADJUSTMENTS_SETTINGS",  "TABLE",         "Config: scope → fact table mapping, PK columns, metrics"),
        ("ADJUSTMENT_APP.ADJ_RECURRING_TEMPLATE","TABLE",         "Templates for automatically recurring adjustments"),
        # (Streams retired — the pipeline POLLS; tasks fire every minute
        #  unconditionally and SP_RUN_PIPELINE exits fast when idle.)
        ("ADJUSTMENT_APP.DT_DASHBOARD",          "DYNAMIC TABLE", "Aggregated metrics by scope, status, entity, user"),
        ("ADJUSTMENT_APP.DT_OVERLAP_ALERTS",     "DYNAMIC TABLE", "Self-join detecting overlapping adjustments"),
        ("ADJUSTMENT_APP.VW_DASHBOARD_KPI",      "VIEW",          "Pre-aggregated KPIs for the dashboard"),
        ("ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS",    "TABLE",         "COB sign-off status per scope+entity. Managed via the Sign-Off page (approval-gated requests)"),
        ("ADJUSTMENT_APP.ADJ_APPROVERS",         "TABLE",         "Authorized approvers with optional scope restriction. Managed via Admin page"),
        ("ADJUSTMENT_APP.VW_SIGNOFF_STATUS",     "VIEW",          "COB sign-off status (reads from ADJ_SIGNOFF_STATUS)"),
        ("ADJUSTMENT_APP.VW_RECENT_ACTIVITY",    "VIEW",          "UNION of submissions + status changes"),
        ("ADJUSTMENT_APP.VW_ERRORS",             "VIEW",          "Adjustments with Error status"),

        ("ADJUSTMENT_APP.VW_MY_WORK",            "VIEW",          "All adjustments — filtered by user in Streamlit"),
        ("ADJUSTMENT_APP.VW_ADJUSTMENT_TRACK",   "VIEW",          "Full lifecycle per adjustment — drives the Adjustment Pipeline page"),
        ("ADJUSTMENT_APP.VW_APPROVAL_QUEUE",     "VIEW",          "Adjustments awaiting approval"),
    ]

    df_schema = pd.DataFrame(schema_items, columns=["Object", "Type", "Description"])
    render_df_table(df_schema, max_rows=200)

    section_title("Key Design Principles", "info")
    st.markdown("""
    **1. Streamlit-First** — Streamlit is the single entry point for all adjustments.
    No file-based staging tables. Users create, preview, and submit through the UI.

    **2. Config-Driven Scopes** — `ADJUSTMENTS_SETTINGS` drives which data sources
    are available. New scope = new config row, zero code changes.

    **3. Async Processing** — Adjustments are applied by Snowflake Tasks triggered
    by Streams, not by the Streamlit session. Users are never blocked.

    **4. Full Audit Trail** — Every status change is logged to `ADJ_STATUS_HISTORY`.
    Adjustments are soft-deleted (IS_DELETED flag), never physically removed.

    **5. Overlap Detection** — `DT_OVERLAP_ALERTS` (Dynamic Table) automatically
    detects overlapping adjustments via self-join with wildcard matching.

    **6. Sign-Off Guard** — When a COB is already signed off in `ADJ_SIGNOFF_STATUS`,
    the submit procedure rejects with "Rejected - SignedOff" status. Manage sign-off
    status from the **Sign-Off page** in the left menu (requests are decided on the Approval Queue page).
    """)

    section_title("System Statistics", "line-chart")
    try:
        df_sys = run_query_df("""
            SELECT
                (SELECT COUNT(*) FROM ADJUSTMENT_APP.ADJ_HEADER WHERE IS_DELETED = FALSE) AS TOTAL_ADJUSTMENTS,
                (SELECT COUNT(*) FROM ADJUSTMENT_APP.ADJ_STATUS_HISTORY) AS TOTAL_HISTORY_ENTRIES,
                (SELECT COUNT(*) FROM ADJUSTMENT_APP.ADJUSTMENTS_SETTINGS) AS SCOPE_COUNT,
                (SELECT COUNT(*) FROM ADJUSTMENT_APP.ADJ_RECURRING_TEMPLATE WHERE IS_ACTIVE = TRUE) AS ACTIVE_TEMPLATES
        """)
        if not df_sys.empty:
            s = df_sys.iloc[0]
            sc1, sc2, sc3, sc4 = st.columns(4)
            sc1.metric("Total Adjustments", int(s.get("TOTAL_ADJUSTMENTS", 0)))
            sc2.metric("Audit Trail Entries", int(s.get("TOTAL_HISTORY_ENTRIES", 0)))
            sc3.metric("Configured Scopes", int(s.get("SCOPE_COUNT", 0)))
            sc4.metric("Active Templates", int(s.get("ACTIVE_TEMPLATES", 0)))
    except Exception:
        st.info("System statistics will be available after deployment.")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 6 — SQL REFERENCE
# ══════════════════════════════════════════════════════════════════════════════

with tab_sql:
    section_title("Key Stored Procedures", "settings")

    procs = {
        "SP_SUBMIT_ADJUSTMENT": (
            "Validates and inserts a new adjustment from Streamlit. Accepts JSON payload.",
            """CALL ADJUSTMENT_APP.SP_SUBMIT_ADJUSTMENT('{
    "cobid": 20260328,
    "process_type": "VaR",
    "adjustment_type": "Scale",
    "username": "jsmith",
    "scale_factor": 1.05,
    "entity_code": "MUSE",
    "reason": "Q1 close EUR reallocation"
}');"""),
        "SP_PREVIEW_ADJUSTMENT": (
            "Returns a read-only preview of what the adjustment would affect.",
            """CALL ADJUSTMENT_APP.SP_PREVIEW_ADJUSTMENT('{
    "cobid": 20260328,
    "process_type": "VaR",
    "adjustment_type": "Scale",
    "scale_factor": 1.05,
    "entity_code": "MUSE"
}');"""),
        "SP_RUN_PIPELINE": (
            "Polling orchestrator (1-minute task) — blocks overlaps, promotes to Running, processes in parallel, unblocks resolved.",
            """-- Called automatically by scope tasks (TASK_PROCESS_VAR, etc.)
-- Can also be called manually:
CALL ADJUSTMENT_APP.SP_RUN_PIPELINE('VaR', '["VaR"]');"""),
        "SP_PROCESS_ADJUSTMENT": (
            "Core processing engine — writes deltas to fact tables. Called by SP_RUN_PIPELINE.",
            """-- Called by SP_RUN_PIPELINE for each (process_type, action, cobid) group:
CALL ADJUSTMENT_APP.SP_PROCESS_ADJUSTMENT('VaR', 'Scale', 20260328);"""),
    }

    for proc_name, (desc, code) in procs.items():
        with st.expander(f"`{proc_name}` — {desc}"):
            st.code(code, language="sql")

    section_title("Snowflake Tasks Configuration", "timer")
    st.markdown("""
    Four independent scope tasks, each guarded by a standard stream on its queue view.
    Tasks fire every 1 minute **only when** the stream has data (INSERT or UPDATE on ADJ_HEADER
    that matches the queue view filters).
    """)
    st.markdown("""
    There are four scope tasks, all using **serverless compute** and the same pattern:

    - **`TASK_PROCESS_VAR`** — every minute; polls and runs the **VaR** pipeline.
    - **`TASK_PROCESS_STRESS`** — every minute; polls and runs the **Stress** pipeline.
    - **`TASK_PROCESS_FRTB`** — every minute; polls and runs the **FRTB** pipeline
      (sub-types FRTB, FRTBDRC, FRTBRRAO — "All FRTB" in the app submits one adjustment per sub-type).
    - **`TASK_PROCESS_SENSITIVITY`** — every minute; polls and runs the **Sensitivity** pipeline.

    Each task is scheduled every **1 minute** but only does work when its stream has new data,
    at which point it calls `SP_RUN_PIPELINE` for that process type. The task definitions are
    deployed from `06_tasks.sql` — they are not created from this app.
    """)
