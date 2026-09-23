"""
Admin — Metadata & Configuration
===================================
View and manage ADJUSTMENTS_SETTINGS, recurring templates, and reference.
Reads from: ADJUSTMENTS_SETTINGS, ADJ_RECURRING_TEMPLATE, ADJ_HEADER (stats).
"""
import streamlit as st
import pandas as pd

st.set_page_config(page_title="Admin · MUFG", page_icon="⚙️", layout="wide", initial_sidebar_state="expanded")

from utils.styles import (scope_label, scope_meta, wide_kwargs, inject_css, render_sidebar, section_title, P,
                          SCOPE_CONFIG, SCOPE_LABEL_HELP, icon, render_df_table,
                          kpi_card, fmt_user_dt, set_flash, render_flash,
                          confirm_gate)
from utils.snowflake_conn import (run_query, run_query_df, current_user_name,
                                  safe_rerun, sql_escape)

# The one escape for SQL literals lives in utils.snowflake_conn (this page used
# to carry its own copy — and a second, subtly broken one in _role_members).
_esc = sql_escape


def _identity_keys(name: str) -> set:
    """Comparable forms of an identity. The app-resolved viewer name is
    often the EMAIL (st.user, when READ SESSION is absent) while SHOW
    GRANTS OF ROLE returns Snowflake USERNAMES — frequently the same
    person as 'MARCOS.MAGRI@BANK.COM' vs 'MARCOS.MAGRI'. Compare on the
    full string AND the local part (before '@'), both upper-cased.
    Module-level so the Approvers tab can use it even in bootstrap mode."""
    n = str(name or "").strip().upper()
    if not n:
        return set()
    keys = {n}
    if "@" in n:
        keys.add(n.split("@", 1)[0])
    return keys


def _cob_input(label, key, placeholder="e.g. 20260101"):
    """Compact YYYYMMDD input returning int, or None when empty/invalid —
    same strict validation as New Adjustment's _int_input (8 digits AND a
    real calendar date), with an inline error."""
    from datetime import datetime as _dt
    raw = st.text_input(label, key=key, placeholder=placeholder).strip()
    if not raw:
        return None
    if not raw.isdigit() or len(raw) != 8:
        st.error(f"“{raw}” is not a valid COB — use exactly 8 digits, "
                 f"YYYYMMDD (e.g. 20260101).")
        return None
    try:
        _dt.strptime(raw, "%Y%m%d")
    except ValueError:
        st.error(f"“{raw}” is not a real calendar date — check the month/day.")
        return None
    return int(raw)

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
        _lit = sql_escape(safe)
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
# Outcome of the last save/deactivate — stored before safe_rerun so the
# message survives the rerun (st.success right before a rerun never paints).
render_flash("admin")
st.markdown(
    f"<span style='color:{P['grey_700']};font-size:0.9rem'>"
    "Manage scope configurations, recurring templates, and view system reference. "
    "In production, changes here write to <code>ADJUSTMENTS_SETTINGS</code> and "
    "<code>ADJ_RECURRING_TEMPLATE</code> in the ADJUSTMENT_APP schema."
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

    # Standard KPI card; the state is spelled out in `sub` (threshold rule,
    # "OK", or an "Action needed" prefix) so it is never colour-only.
    _HEALTH_VARIANT = {"ok": "success", "warn": "warning", "bad": "danger"}

    def _health_card(col, label, value, state, sub=""):
        col.markdown(kpi_card(label, value, sub,
                              variant=_HEALTH_VARIANT.get(state, "primary")),
                     unsafe_allow_html=True)

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
                    "OK — polling every minute" if state == "started"
                    else "Action needed — NOT RUNNING, resume it")
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
            _blocked = int(q["BLOCKED"] or 0)
            _failed  = int(q["FAILED_24H"] or 0)
            _oq_txt  = f"oldest {int(oq)} min (amber > 5)" if oq is not None else "queue empty"
            _health_card(h1, "Queued", int(q["QUEUED"] or 0),
                         "warn" if oq is not None and int(oq) > 5 else "ok",
                         _oq_txt if (oq is not None and int(oq) > 5) else f"OK — {_oq_txt}")
            _health_card(h2, "Blocked", _blocked,
                         "warn" if _blocked else "ok",
                         "waiting behind overlaps (amber > 0)" if _blocked else "OK — none")
            if orn is not None and int(orn) > 240:
                _run_state, _run_sub = "bad", f"Action needed — oldest {int(orn)} min (red > 240)"
            elif orn is not None and int(orn) > 180:
                _run_state, _run_sub = "warn", f"oldest {int(orn)} min (amber > 180)"
            elif orn is not None:
                _run_state, _run_sub = "ok", f"OK — oldest {int(orn)} min (amber > 180)"
            else:
                _run_state, _run_sub = "ok", "OK — nothing running"
            _health_card(h3, "Running", int(q["RUNNING"] or 0), _run_state, _run_sub)
            _health_card(h4, "Failed (24h)", _failed,
                         "bad" if _failed else "ok",
                         "Action needed — retry from the Adjustments page (red > 0)"
                         if _failed else "OK — none")
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
                             "picked up ~every 5 min (amber > 5)" if pbi_waiting > 5
                             else "OK — picked up ~every 5 min")
            except Exception:
                _health_card(h5, "PBI refreshes waiting", "n/a", "warn",
                             "Not readable — METADATA.POWERBI_ACTION")
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
            _nf = int(n["FAILED"] or 0)
            _ns = int(n["SKIPPED"] or 0)
            _health_card(n1, "Sent", int(n["SENT"] or 0), "ok", "OK")
            _health_card(n2, "Failed", _nf,
                         "bad" if _nf else "ok",
                         "Action needed — see the Notifications tab log (red > 0)"
                         if _nf else "OK — none")
            _health_card(n3, "Skipped (disabled)", _ns,
                         "warn" if _ns else "ok",
                         "master switch is off (amber > 0)" if _ns else "OK — none")
    except Exception as ex:
        st.info(f"Notification stats not available: {ex}")

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — SCOPE CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

with tab_scopes:
    section_title("Scopes", "database")
    st.caption("Source table: ADJUSTMENT_APP.ADJUSTMENTS_SETTINGS")
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
                cfg = scope_meta(scope)
                with st.expander(f'{scope_label(scope)} — {row["FACT_TABLE"]}'):
                    c1, c2 = st.columns(2)
                    with c1:
                        section_title("Configuration")
                        df_cfg = pd.DataFrame(
                            [("Fact Table",     str(row["FACT_TABLE"] or "")),
                             ("Primary Key",    str(row["FACT_TABLE_PK"] or "")),
                             ("Metric (Local)", str(row["METRIC_NAME"] or "")),
                             ("Metric (USD)",   str(row["METRIC_USD_NAME"] or ""))],
                            columns=["Setting", "Value"])
                        render_df_table(df_cfg, max_rows=4, key=f"scope_cfg_{scope}")
                    with c2:
                        section_title("Timestamps")
                        created = fmt_user_dt(row.get("CREATED_DATE", ""))
                        st.markdown(
                            f'<div style="font-size:0.85rem">'
                            f'<strong>Created:</strong> {created}</div>',
                            unsafe_allow_html=True)
        else:
            st.info("No scope configurations found. Seed data may not be loaded yet.")
    except Exception as e:
        st.warning(f"Could not load settings: {e}")

    st.markdown("<br/>", unsafe_allow_html=True)
    section_title("How to onboard a scope (DBA step)", "settings")
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
                # label → (id, name): the row's identity is carried in a dict,
                # never parsed back out of the label the user sees (a username
                # containing "ID " used to break the lookup).
                _approver_by_label = {
                    f"{r['USERNAME']} (ID {r['APPROVER_ID']}) — {'Active' if r['IS_ACTIVE'] else 'Inactive'}":
                        (int(r["APPROVER_ID"]), str(r["USERNAME"]).strip())
                    for _, r in df_approvers.iterrows()
                }
                approver_options = list(_approver_by_label)
                sel_approver = st.selectbox("Select approver", approver_options, key="toggle_approver")
            approver_id, approver_name = _approver_by_label[sel_approver]
            with toggle_cols[1]:
                if st.button("Activate", key="activate_approver_btn"):
                    try:
                        run_query(f"""
                            UPDATE ADJUSTMENT_APP.ADJ_APPROVERS
                            SET IS_ACTIVE = TRUE
                            WHERE APPROVER_ID = {approver_id}
                        """)
                        set_flash("admin", "success", f"Approver {approver_name} activated.")
                        safe_rerun()
                    except Exception as ex:
                        set_flash("admin", "error", f"Failed to activate approver: {ex}")
                        safe_rerun()
            with toggle_cols[2]:
                # Deactivating removes a 4-eyes control — two clicks, not one.
                _cfm_appr = confirm_gate(f"Confirm deactivating {approver_name}",
                                         key=f"cfm_deact_approver_{approver_id}")
                if st.button("Deactivate", key="deactivate_approver_btn",
                             disabled=not _cfm_appr):
                    try:
                        run_query(f"""
                            UPDATE ADJUSTMENT_APP.ADJ_APPROVERS
                            SET IS_ACTIVE = FALSE
                            WHERE APPROVER_ID = {approver_id}
                        """)
                        set_flash("admin", "success", f"Approver {approver_name} deactivated.")
                        safe_rerun()
                    except Exception as ex:
                        set_flash("admin", "error", f"Failed to deactivate approver: {ex}")
                        safe_rerun()
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
            a_scope = st.selectbox("Scope (optional)", scope_options, key="approver_scope",
                                   help=SCOPE_LABEL_HELP)

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
                    set_flash("admin", "success",
                              f"Approver {a_username.strip().upper()} added.")
                    safe_rerun()
                except Exception as ex:
                    st.error(f"Failed to add approver: {ex}")

    # ── Authorized sign-off users (Sign-Off page + quick actions) ────────────
    st.markdown("<br/>", unsafe_allow_html=True)
    section_title("Authorized Sign-Off Users", "check-circle")
    st.markdown(
        f'<span style="font-size:0.85rem;color:{P["grey_700"]}">'
        f'Users listed here can <strong>sign off</strong> a COB and <strong>request a '
        f're-open</strong> (Sign-Off page and the quick actions on New Adjustment). '
        f'Approval of those requests stays with the Approvers list above. '
        f'Set <code>PROCESS_TYPE</code> to limit a user to one scope, or leave blank for all scopes.'
        f'</span>',
        unsafe_allow_html=True)

    try:
        df_signers = run_query_df("""
            SELECT SIGNER_ID, USERNAME, PROCESS_TYPE, IS_ACTIVE,
                   ADDED_BY, ADDED_DATE
            FROM ADJUSTMENT_APP.ADJ_SIGNOFF_USERS
            ORDER BY IS_ACTIVE DESC, USERNAME
        """)
        _n_active_signers = (int(df_signers[df_signers["IS_ACTIVE"] == True].shape[0])
                             if not df_signers.empty else 0)
        if _n_active_signers == 0:
            st.warning("The sign-off user list has no active user, so **everyone** "
                       "can sign off and request re-opens (bootstrap). Add the "
                       "first user below to restrict it.")

        if not df_signers.empty:
            _inactive_signers = len(df_signers) - _n_active_signers
            st.markdown(
                f'<span style="font-size:0.85rem">'
                f'<strong style="color:{P["success"]}">{_n_active_signers} active</strong> · '
                f'<strong style="color:{P["grey_700"]}">{_inactive_signers} inactive</strong>'
                f'</span>',
                unsafe_allow_html=True)
            render_df_table(df_signers, max_rows=200, height=260)

            st.markdown("<br/>", unsafe_allow_html=True)
            section_title("Toggle Sign-Off User Status", "refresh-cw")
            sg_cols = st.columns([2, 1, 1])
            with sg_cols[0]:
                _signer_by_label = {
                    f"{r['USERNAME']} (ID {r['SIGNER_ID']}) — {'Active' if r['IS_ACTIVE'] else 'Inactive'}":
                        (int(r["SIGNER_ID"]), str(r["USERNAME"]).strip())
                    for _, r in df_signers.iterrows()
                }
                signer_options = list(_signer_by_label)
                sel_signer = st.selectbox("Select sign-off user", signer_options,
                                          key="toggle_signer")
            signer_id, signer_name = _signer_by_label[sel_signer]
            with sg_cols[1]:
                if st.button("Activate", key="activate_signer_btn"):
                    try:
                        run_query(f"""
                            UPDATE ADJUSTMENT_APP.ADJ_SIGNOFF_USERS
                            SET IS_ACTIVE = TRUE
                            WHERE SIGNER_ID = {signer_id}
                        """)
                        set_flash("admin", "success", f"Sign-off user {signer_name} activated.")
                        safe_rerun()
                    except Exception as ex:
                        set_flash("admin", "error", f"Failed to activate sign-off user: {ex}")
                        safe_rerun()
            with sg_cols[2]:
                # Deactivating the LAST active user re-opens sign-off to
                # everyone (bootstrap rule) — two clicks, not one.
                _cfm_sg = confirm_gate(f"Confirm deactivating {signer_name}",
                                       key=f"cfm_deact_signer_{signer_id}")
                if st.button("Deactivate", key="deactivate_signer_btn",
                             disabled=not _cfm_sg):
                    try:
                        run_query(f"""
                            UPDATE ADJUSTMENT_APP.ADJ_SIGNOFF_USERS
                            SET IS_ACTIVE = FALSE
                            WHERE SIGNER_ID = {signer_id}
                        """)
                        set_flash("admin", "success", f"Sign-off user {signer_name} deactivated.")
                        safe_rerun()
                    except Exception as ex:
                        set_flash("admin", "error", f"Failed to deactivate sign-off user: {ex}")
                        safe_rerun()
        else:
            st.info("No sign-off users configured yet. Add one below.")
    except Exception as e:
        st.info(f"Sign-off users table not available: {e}")

    st.markdown("<br/>", unsafe_allow_html=True)
    section_title("Add New Sign-Off User", "check-circle")

    with st.form("new_signer_form"):
        sg1, sg2 = st.columns(2)
        with sg1:
            s_username = st.text_input("Username", placeholder="e.g. JSMITH", key="signer_user")
        with sg2:
            s_scope = st.selectbox("Scope (optional)", ["All Scopes"] + list(SCOPE_CONFIG.keys()),
                                   key="signer_scope", help=SCOPE_LABEL_HELP)

        s_submit = st.form_submit_button("Add Sign-Off User", type="primary")
        if s_submit:
            if not s_username.strip():
                st.error("Username is required.")
            else:
                try:
                    s_scope_val = "NULL" if s_scope == "All Scopes" else f"'{_esc(s_scope)}'"
                    run_query(f"""
                        INSERT INTO ADJUSTMENT_APP.ADJ_SIGNOFF_USERS
                            (USERNAME, PROCESS_TYPE, IS_ACTIVE, ADDED_BY)
                        VALUES (UPPER('{_esc(s_username.strip())}'), {s_scope_val}, TRUE, '{_esc(user)}')
                    """)
                    set_flash("admin", "success",
                              f"Sign-off user {s_username.strip().upper()} added.")
                    safe_rerun()
                except Exception as ex:
                    st.error(f"Failed to add sign-off user: {ex}")

    # ── Authorized submitters (New Adjustment page — Submit action) ──────────
    st.markdown("<br/>", unsafe_allow_html=True)
    section_title("Authorized Submitters", "send")
    st.markdown(
        f'<span style="font-size:0.85rem;color:{P["grey_700"]}">'
        f'Users listed here can <strong>submit</strong> an adjustment (New '
        f'Adjustment page). Anyone can still open the page, build a draft and '
        f'run the impact preview — only pressing Submit is gated. '
        f'Set <code>PROCESS_TYPE</code> to limit a user to one scope, or leave blank for all scopes.'
        f'</span>',
        unsafe_allow_html=True)

    try:
        df_submitters = run_query_df("""
            SELECT SUBMITTER_ID, USERNAME, PROCESS_TYPE, IS_ACTIVE,
                   ADDED_BY, ADDED_DATE
            FROM ADJUSTMENT_APP.ADJ_SUBMITTERS
            ORDER BY IS_ACTIVE DESC, USERNAME
        """)
        _n_active_submitters = (int(df_submitters[df_submitters["IS_ACTIVE"] == True].shape[0])
                                if not df_submitters.empty else 0)
        if _n_active_submitters == 0:
            st.warning("The submitters list has no active user, so **everyone** "
                       "can submit adjustments (bootstrap). Add the "
                       "first user below to restrict it.")

        if not df_submitters.empty:
            _inactive_submitters = len(df_submitters) - _n_active_submitters
            st.markdown(
                f'<span style="font-size:0.85rem">'
                f'<strong style="color:{P["success"]}">{_n_active_submitters} active</strong> · '
                f'<strong style="color:{P["grey_700"]}">{_inactive_submitters} inactive</strong>'
                f'</span>',
                unsafe_allow_html=True)
            render_df_table(df_submitters, max_rows=200, height=260)

            st.markdown("<br/>", unsafe_allow_html=True)
            section_title("Toggle Submitter Status", "refresh-cw")
            sb_cols = st.columns([2, 1, 1])
            with sb_cols[0]:
                _submitter_by_label = {
                    f"{r['USERNAME']} (ID {r['SUBMITTER_ID']}) — {'Active' if r['IS_ACTIVE'] else 'Inactive'}":
                        (int(r["SUBMITTER_ID"]), str(r["USERNAME"]).strip())
                    for _, r in df_submitters.iterrows()
                }
                submitter_options = list(_submitter_by_label)
                sel_submitter = st.selectbox("Select submitter", submitter_options,
                                             key="toggle_submitter")
            submitter_id, submitter_name = _submitter_by_label[sel_submitter]
            with sb_cols[1]:
                if st.button("Activate", key="activate_submitter_btn"):
                    try:
                        run_query(f"""
                            UPDATE ADJUSTMENT_APP.ADJ_SUBMITTERS
                            SET IS_ACTIVE = TRUE
                            WHERE SUBMITTER_ID = {submitter_id}
                        """)
                        set_flash("admin", "success", f"Submitter {submitter_name} activated.")
                        safe_rerun()
                    except Exception as ex:
                        set_flash("admin", "error", f"Failed to activate submitter: {ex}")
                        safe_rerun()
            with sb_cols[2]:
                # Deactivating the LAST active user re-opens submission to
                # everyone (bootstrap rule) — two clicks, not one.
                _cfm_sb = confirm_gate(f"Confirm deactivating {submitter_name}",
                                       key=f"cfm_deact_submitter_{submitter_id}")
                if st.button("Deactivate", key="deactivate_submitter_btn",
                             disabled=not _cfm_sb):
                    try:
                        run_query(f"""
                            UPDATE ADJUSTMENT_APP.ADJ_SUBMITTERS
                            SET IS_ACTIVE = FALSE
                            WHERE SUBMITTER_ID = {submitter_id}
                        """)
                        set_flash("admin", "success", f"Submitter {submitter_name} deactivated.")
                        safe_rerun()
                    except Exception as ex:
                        set_flash("admin", "error", f"Failed to deactivate submitter: {ex}")
                        safe_rerun()
        else:
            st.info("No submitters configured yet. Add one below.")
    except Exception as e:
        st.info(f"Submitters table not available: {e}")

    st.markdown("<br/>", unsafe_allow_html=True)
    section_title("Add New Submitter", "send")

    with st.form("new_submitter_form"):
        sb1, sb2 = st.columns(2)
        with sb1:
            b_username = st.text_input("Username", placeholder="e.g. JSMITH", key="submitter_user")
        with sb2:
            b_scope = st.selectbox("Scope (optional)", ["All Scopes"] + list(SCOPE_CONFIG.keys()),
                                   key="submitter_scope", help=SCOPE_LABEL_HELP)

        b_submit = st.form_submit_button("Add Submitter", type="primary")
        if b_submit:
            if not b_username.strip():
                st.error("Username is required.")
            else:
                try:
                    b_scope_val = "NULL" if b_scope == "All Scopes" else f"'{_esc(b_scope)}'"
                    run_query(f"""
                        INSERT INTO ADJUSTMENT_APP.ADJ_SUBMITTERS
                            (USERNAME, PROCESS_TYPE, IS_ACTIVE, ADDED_BY)
                        VALUES (UPPER('{_esc(b_username.strip())}'), {b_scope_val}, TRUE, '{_esc(user)}')
                    """)
                    set_flash("admin", "success",
                              f"Submitter {b_username.strip().upper()} added.")
                    safe_rerun()
                except Exception as ex:
                    st.error(f"Failed to add submitter: {ex}")

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
                _admin_by_label = {
                    f"{r['USERNAME']} [{r['ADMIN_TYPE']}] (ID {r['ADMIN_ID']}) — {'Active' if r['IS_ACTIVE'] else 'Inactive'}":
                        (int(r["ADMIN_ID"]), str(r["USERNAME"]).strip().upper())
                    for _, r in df_admins.iterrows()
                }
                admin_options = list(_admin_by_label)
                sel_admin = st.selectbox("Select administrator", admin_options,
                                         key="toggle_admin")
            _sel_admin_id, _sel_admin_user = _admin_by_label[sel_admin]
            with adm_cols[1]:
                if st.button("Activate", key="activate_admin_btn"):
                    try:
                        run_query(f"""
                            UPDATE ADJUSTMENT_APP.ADJ_ADMINS
                            SET IS_ACTIVE = TRUE
                            WHERE ADMIN_ID = {_sel_admin_id}
                        """)
                        set_flash("admin", "success",
                                  f"Administrator {_sel_admin_user} activated.")
                        safe_rerun()
                    except Exception as ex:
                        set_flash("admin", "error",
                                  f"Failed to activate administrator: {ex}")
                        safe_rerun()
            with adm_cols[2]:
                # Same identity comparison as the auth gate (email vs username).
                _is_self = bool(_identity_keys(_sel_admin_user) & _identity_keys(user))
                _cfm_adm = confirm_gate(f"Confirm deactivating {_sel_admin_user}",
                                        key=f"cfm_deact_admin_{_sel_admin_id}",
                                        help="You cannot deactivate yourself." if _is_self else None)
                if st.button("Deactivate", key="deactivate_admin_btn",
                             disabled=_is_self or not _cfm_adm,
                             help="You cannot deactivate yourself." if _is_self else None):
                    try:
                        run_query(f"""
                            UPDATE ADJUSTMENT_APP.ADJ_ADMINS
                            SET IS_ACTIVE = FALSE
                            WHERE ADMIN_ID = {_sel_admin_id}
                        """)
                        set_flash("admin", "success",
                                  f"Administrator {_sel_admin_user} deactivated.")
                        safe_rerun()
                    except Exception as ex:
                        set_flash("admin", "error",
                                  f"Failed to deactivate administrator: {ex}")
                        safe_rerun()
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
                            set_flash("admin", "warning",
                                      "Role added, but its membership could NOT be "
                                      "verified (the app cannot run SHOW GRANTS OF "
                                      "ROLE) — role-based access will not work "
                                      "until that privilege is granted.")
                        else:
                            set_flash("admin", "success",
                                      f"Role {adm_username.strip().upper()} "
                                      f"added — {len(_m)} member(s) resolve "
                                      f"as administrators.")
                    else:
                        set_flash("admin", "success",
                                  f"Administrator {adm_username.strip().upper()} added.")
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
        f'{icon("alert-triangle", size=13, color=P["warning"])} <strong>Recurring '
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

    # Plain widgets rather than st.form: the Scale Factor field must react to
    # the Adjustment Type (hidden for Flatten), and forms only rerun on submit.
    tc1, tc2, tc3 = st.columns(3)
    with tc1:
        t_scope = st.selectbox("Scope (Process Type)", list(SCOPE_CONFIG.keys()), key="tmpl_scope",
                               help=SCOPE_LABEL_HELP)
        t_type  = st.selectbox("Adjustment Type", ["Flatten", "Scale", "Roll"], key="tmpl_type")
        if t_type == "Flatten":
            t_scale = 1.0
            st.caption("Flatten has no scale factor.")
        else:
            t_scale = st.number_input("Scale Factor", value=1.0, min_value=-10.0, max_value=100.0,
                                      step=0.01, format="%.4f", key="tmpl_scale")
    with tc2:
        t_entity = st.text_input("Entity Code", placeholder="e.g. MUSE", key="tmpl_entity")
        t_book   = st.text_input("Book Code (optional)", key="tmpl_book")
        t_dept   = st.text_input("Department Code (optional)", key="tmpl_dept")
    with tc3:
        t_start = _cob_input("Start COBID", key="tmpl_start", placeholder="e.g. 20260101")
        t_end   = _cob_input("End COBID", key="tmpl_end", placeholder="e.g. 20261231")
        t_cron  = st.text_input("CRON Expression (optional)", placeholder="0 8 * * MON-FRI",
                                key="tmpl_cron",
                                help="5 space-separated fields: minute hour day-of-month "
                                     "month day-of-week (e.g. 0 8 * * MON-FRI).")
        _cron_ok = True
        if t_cron.strip() and len(t_cron.split()) != 5:
            _cron_ok = False
            st.error(f"“{t_cron.strip()}” is not a valid CRON expression — it needs "
                     f"exactly 5 space-separated fields (minute hour day month weekday), "
                     f"e.g. 0 8 * * MON-FRI.")

    _tmpl_ready = (t_start is not None and t_end is not None and _cron_ok
                   and t_start <= t_end)
    if t_start is not None and t_end is not None and t_start > t_end:
        st.error("End COBID must be on or after Start COBID.")

    if st.button("Save template (not scheduled yet)", key="tmpl_save",
                 type="secondary", disabled=not _tmpl_ready,
                 help="Stores the template only — nothing instantiates it until "
                      "recurring processing goes live."):
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
                        {dept_val}, {float(t_scale)}, {int(t_start)}, {int(t_end)},
                        {cron_val}, TRUE, '{_esc(user)}')
            """)
            set_flash("admin", "success",
                      f"Template saved ({t_scope} · {t_type} · {t_start}–{t_end}). "
                      "It is stored only — recurring processing is not live yet.")
            safe_rerun()
        except Exception as ex:
            st.error(f"Failed to save template: {ex}")


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
        # st.toggle (Streamlit ≥1.23) — environment.yml pins 1.50.0, so the
        # old "the SiS runtime is 1.22, st.toggle kills the page" workaround
        # that forced a checkbox here no longer applies. A master switch
        # reads as on/off, which is what a toggle shows.
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
                     **wide_kwargs()):
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
                set_flash("admin", "success", "Notification settings saved.")
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
        if st.button("Send test", key="ntf_test_btn", **wide_kwargs(),
                     disabled=not test_email.strip(),
                     help="Enter a recipient first — the test bypasses the master switch"):
            try:
                import json as _json
                _tp = sql_escape(_json.dumps({"email": test_email.strip()}))
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
                _pref_by_label = {
                    f"{r['USERNAME']} (ID {r['PREF_ID']}) — "
                    f"{'Active' if r['IS_ACTIVE'] else 'Inactive'}":
                        (int(r["PREF_ID"]), str(r["USERNAME"]).strip())
                    for _, r in df_prefs.iterrows()
                }
                pref_options = list(_pref_by_label)
                sel_pref = st.selectbox("Select recipient", pref_options,
                                        key="ntf_pref_pick")
            _sel_pref_id, _sel_pref_user = _pref_by_label[sel_pref]
            with pcols[1]:
                if st.button("Activate", key="ntf_pref_on"):
                    try:
                        run_query(f"""
                            UPDATE ADJUSTMENT_APP.ADJ_NOTIFICATION_PREFS
                            SET IS_ACTIVE = TRUE WHERE PREF_ID = {_sel_pref_id}
                        """)
                        set_flash("admin", "success",
                                  f"Recipient {_sel_pref_user} activated.")
                        safe_rerun()
                    except Exception as ex:
                        set_flash("admin", "error", f"Failed to activate recipient: {ex}")
                        safe_rerun()
            with pcols[2]:
                _cfm_pref = confirm_gate(f"Confirm deactivating {_sel_pref_user}",
                                         key=f"cfm_deact_pref_{_sel_pref_id}")
                if st.button("Deactivate", key="ntf_pref_off", disabled=not _cfm_pref):
                    try:
                        run_query(f"""
                            UPDATE ADJUSTMENT_APP.ADJ_NOTIFICATION_PREFS
                            SET IS_ACTIVE = FALSE WHERE PREF_ID = {_sel_pref_id}
                        """)
                        set_flash("admin", "success",
                                  f"Recipient {_sel_pref_user} deactivated.")
                        safe_rerun()
                    except Exception as ex:
                        set_flash("admin", "error", f"Failed to deactivate recipient: {ex}")
                        safe_rerun()
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
                    set_flash("admin", "success",
                              f"Recipient {np_user.strip().upper()} added.")
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


    # ── AI Assistant (Documentation page) ────────────────────────────────────
    section_title("AI Assistant (Documentation page)", "zap")
    st.markdown(
        f'<div style="background:{P["info_lt"]};border:1px solid #90CAF9;border-radius:8px;'
        f'padding:0.7rem 1rem;margin-bottom:1rem;font-size:0.85rem">'
        f'The assistant runs on <strong>Snowflake Cortex</strong> inside this '
        f'account. It uses the <strong>quick model</strong> by default and the '
        f'<strong>larger model</strong> when a user ticks "Think harder". Model '
        f'availability depends on the account region — use <em>Test models</em> '
        f'after changing a name. If a model is not served in this region, ask the '
        f'DBA to enable cross-region inference '
        f'(<code>CORTEX_ENABLED_CROSS_REGION</code>) or pick one that is.'
        f'</div>',
        unsafe_allow_html=True)
    try:
        _ai_rows = run_query("""
            SELECT CONFIG_KEY, CONFIG_VALUE FROM ADJUSTMENT_APP.ADJ_APP_CONFIG
            WHERE CONFIG_KEY IN ('AI_ASSISTANT_ENABLED', 'AI_ASSISTANT_MODEL',
                                 'AI_ASSISTANT_SMART_MODEL')
        """)
        _ai = {str(r["CONFIG_KEY"]): str(r["CONFIG_VALUE"] or "")
               for r in (_ai_rows or [])}
    except Exception as ex:
        _ai = {}
        st.warning(f"Could not read AI assistant config: {ex}")

    ai1, ai2, ai3 = st.columns([1, 2, 2])
    with ai1:
        # Same as the notifications master switch above: a feature switch
        # reads as on/off, and st.toggle is available on the pinned 1.50.
        ai_enabled = st.toggle(
            "Assistant enabled", key="ai_cfg_enabled",
            value=_ai.get("AI_ASSISTANT_ENABLED", "true").strip().lower() == "true")
    with ai2:
        ai_quick = st.text_input(
            "Quick model (default)", key="ai_cfg_quick",
            value=_ai.get("AI_ASSISTANT_MODEL", "llama3.1-70b"),
            help="Cortex model for everyday questions, e.g. llama3.1-70b, "
                 "llama3.3-70b, mistral-large2, claude-haiku-4-5.")
    with ai3:
        ai_smart = st.text_input(
            "Larger model (\"Think harder\")", key="ai_cfg_smart",
            value=_ai.get("AI_ASSISTANT_SMART_MODEL", "claude-sonnet-4-6"),
            help="Cortex model for tricky questions, e.g. claude-sonnet-4-6, "
                 "claude-opus-4-7, openai-gpt-5.")
    ab1, ab2, _ = st.columns([1, 1, 3])
    with ab1:
        if st.button("Save AI settings", key="ai_cfg_save", type="primary",
                     **wide_kwargs()):
            try:
                for key, val in (("AI_ASSISTANT_ENABLED",
                                  "true" if ai_enabled else "false"),
                                 ("AI_ASSISTANT_MODEL", ai_quick.strip()),
                                 ("AI_ASSISTANT_SMART_MODEL", ai_smart.strip())):
                    run_query(f"""
                        MERGE INTO ADJUSTMENT_APP.ADJ_APP_CONFIG t
                        USING (SELECT '{_esc(key)}' AS CONFIG_KEY,
                                      '{_esc(val)}' AS CONFIG_VALUE) s
                        ON t.CONFIG_KEY = s.CONFIG_KEY
                        WHEN MATCHED THEN UPDATE SET
                            CONFIG_VALUE = s.CONFIG_VALUE,
                            UPDATED_BY = '{_esc(user)}',
                            UPDATED_AT = CURRENT_TIMESTAMP()
                        WHEN NOT MATCHED THEN INSERT (CONFIG_KEY, CONFIG_VALUE,
                            DESCRIPTION, UPDATED_BY, UPDATED_AT)
                        VALUES (s.CONFIG_KEY, s.CONFIG_VALUE,
                            'Set from Admin (AI Assistant settings).',
                            '{_esc(user)}', CURRENT_TIMESTAMP())
                    """)
                set_flash("admin", "success", "AI assistant settings saved.")
                safe_rerun()
            except Exception as ex:
                st.error(f"Failed to save AI settings: {ex}")
    with ab2:
        if st.button("Test models", key="ai_cfg_test", **wide_kwargs(),
                     help="Sends a one-word prompt to each model and reports "
                          "whether Cortex served it from this account."):
            for label, mdl in (("Quick", ai_quick.strip()),
                               ("Larger", ai_smart.strip())):
                if not mdl:
                    st.warning(f"{label} model: no name set.")
                    continue
                try:
                    with st.spinner(f"Testing {mdl}…"):
                        _r = run_query(
                            f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{_esc(mdl)}', "
                            f"'Reply with the single word OK.') AS A")
                    _a = str(_r[0][0]).strip() if _r and _r[0][0] is not None else ""
                    st.success(f"{label} model {mdl}: available "
                               f"(replied: {_a[:40] or '—'})")
                except Exception as ex:
                    st.error(f"{label} model {mdl}: NOT available — {ex}")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 5 — SCHEMA REFERENCE
# ══════════════════════════════════════════════════════════════════════════════

with tab_schema:
    section_title("Database Schema Overview", "database")

    schema_items = [
        ("ADJUSTMENT_APP.ADJ_HEADER",            "TABLE",         "One row per adjustment — lifecycle, metadata, all dimension filters"),
        ("ADJUSTMENT_APP.ADJ_LINE_ITEM",         "TABLE",         "Explicit row-level values for Upload/Direct adjustments"),
        ("ADJUSTMENT_APP.ADJ_LINE_ITEM_JSON",    "TABLE",         "Direct Adjustment uploads — one row per CSV line, raw fields in PAYLOAD (VARIANT)"),
        ("ADJUSTMENT_APP.ADJ_STATUS_HISTORY",    "TABLE",         "Append-only audit log of every status change"),
        ("ADJUSTMENT_APP.ADJUSTMENTS_SETTINGS",  "TABLE",         "Config: scope → fact table mapping, PK columns, metrics"),
        ("ADJUSTMENT_APP.ADJ_RECURRING_TEMPLATE","TABLE",         "Templates for automatically recurring adjustments (not scheduled yet)"),
        # (Streams retired — the pipeline POLLS; tasks fire every minute
        #  unconditionally and SP_RUN_PIPELINE exits fast when idle.)
        ("ADJUSTMENT_APP.DT_DASHBOARD",          "DYNAMIC TABLE", "Aggregated metrics by scope, status, entity, user"),
        ("ADJUSTMENT_APP.DT_OVERLAP_ALERTS",     "DYNAMIC TABLE", "Self-join detecting overlapping adjustments"),
        ("ADJUSTMENT_APP.VW_DASHBOARD_KPI",      "VIEW",          "Pre-aggregated KPIs for the dashboard"),
        ("ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS",    "TABLE",         "COB sign-off status per scope+entity. Managed via the Sign-Off page (approval-gated requests)"),
        ("ADJUSTMENT_APP.ADJ_SIGNOFF_HISTORY",   "TABLE",         "Append-only audit of sign-off / re-open transitions"),
        ("ADJUSTMENT_APP.ADJ_APPROVERS",         "TABLE",         "Authorized approvers with optional scope restriction. Managed via Admin page"),
        ("ADJUSTMENT_APP.ADJ_SIGNOFF_USERS",     "TABLE",         "Users allowed to sign off / request re-open, optional scope. Empty = everyone (bootstrap). Managed via Admin page"),
        ("ADJUSTMENT_APP.ADJ_SUBMITTERS",        "TABLE",         "Users allowed to submit an adjustment, optional scope. Empty = everyone (bootstrap). Managed via Admin page"),
        ("ADJUSTMENT_APP.ADJ_ADMINS",            "TABLE",         "Users and Snowflake roles allowed to open this Admin page. Managed via Admin page"),
        ("ADJUSTMENT_APP.ADJ_APP_CONFIG",        "TABLE",         "App-level key/value config (notification master switch, email integration name)"),
        ("ADJUSTMENT_APP.ADJ_NOTIFICATION_PREFS","TABLE",         "Per-user email notification opt-ins (recipients). Managed via Admin page"),
        ("ADJUSTMENT_APP.ADJ_NOTIFICATION_LOG",  "TABLE",         "Every notification send attempt, including SKIPPED_DISABLED while the switch is off"),
        ("ADJUSTMENT_APP.ADJ_USER_PREFS",        "TABLE",         "Per-user display preferences (timezone), written by the app"),
        ("ADJUSTMENT_APP.EROL_PROCESS_LOG",      "TABLE",         "Append-only Entity Roll diagnostics per run/step (timings, rows, spill). Report view VW_EROL_PROCESS_LOG"),
        ("ADJUSTMENT_APP.VW_SIGNOFF_STATUS",     "VIEW",          "COB sign-off status (reads from ADJ_SIGNOFF_STATUS)"),
        ("ADJUSTMENT_APP.VW_RECENT_ACTIVITY",    "VIEW",          "UNION of submissions + status changes"),
        ("ADJUSTMENT_APP.VW_ERRORS",             "VIEW",          "Adjustments with Error status"),

        ("ADJUSTMENT_APP.VW_MY_WORK",            "VIEW",          "All adjustments — filtered by user in Streamlit"),
        ("ADJUSTMENT_APP.VW_ADJUSTMENT_TRACK",   "VIEW",          "Full lifecycle per adjustment — drives the Adjustments page pipeline overview"),
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

    **3. Async Processing** — Snowflake tasks poll the queue every minute (streams
    were retired) and process each (scope, action, COB) batch — never the Streamlit
    session. Users are never blocked.

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
    Async processing — Snowflake tasks poll the queue every minute (streams were retired)
    and process each (scope, action, COB) batch.
    """)
    st.markdown("""
    There are four scope tasks, all using **serverless compute** and the same pattern:

    - **`TASK_PROCESS_VAR`** — every minute; polls and runs the **VaR** pipeline.
    - **`TASK_PROCESS_STRESS`** — every minute; polls and runs the **Stress** pipeline.
    - **`TASK_PROCESS_FRTB`** — every minute; polls and runs the **FRTB** pipeline
      (sub-types FRTB, FRTBDRC, FRTBRRAO — selecting several data scopes on the New Adjustment
      page submits one adjustment per scope).
    - **`TASK_PROCESS_SENSITIVITY`** — every minute; polls and runs the **Sensitivity** pipeline.

    Each task fires every **1 minute** unconditionally and calls `SP_RUN_PIPELINE` for its
    process type, which exits fast when the queue is empty. The task definitions are
    deployed from `06_tasks.sql` — they are not created from this app.
    """)
