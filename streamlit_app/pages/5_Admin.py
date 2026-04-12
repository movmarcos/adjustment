"""
Admin — Metadata & Configuration
===================================
View and manage ADJUSTMENTS_SETTINGS, recurring templates, and reference.
Reads from: ADJUSTMENTS_SETTINGS, ADJ_RECURRING_TEMPLATE, ADJ_HEADER (stats).
"""
import streamlit as st
import pandas as pd

st.set_page_config(page_title="Admin · MUFG", page_icon="⚙️", layout="wide", initial_sidebar_state="expanded")

from utils.styles import inject_css, render_sidebar, section_title, P, SCOPE_CONFIG
from utils.snowflake_conn import run_query, run_query_df, current_user_name, safe_rerun

def _esc(val):
    """Escape single quotes for safe SQL interpolation."""
    return str(val).replace("'", "''") if val is not None else ""

inject_css()
render_sidebar()

user = current_user_name()

st.markdown("## ⚙️ Admin — Configuration")
st.markdown(
    f"<span style='color:{P['grey_700']};font-size:0.9rem'>"
    "Manage scope configurations, recurring templates, and view system reference. "
    "In production, changes here write to <code>ADJUSTMENTS_SETTINGS</code> and "
    "<code>ADJ_RECURRING_TEMPLATE</code> in the ADJUSTMENT schema."
    "</span>", unsafe_allow_html=True)
st.markdown("<br/>", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────

tab_scopes, tab_signoff, tab_approvers, tab_recurring, tab_schema, tab_sql = st.tabs([
    "📊 Scope Configuration",
    "🔒 Sign-Off Management",
    "👤 Approvers",
    "🔁 Recurring Templates",
    "🗂️ Schema Reference",
    "🔧 SQL Reference",
])

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — SCOPE CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

with tab_scopes:
    section_title("Configured Data Sources (ADJUSTMENTS_SETTINGS)", "📊")
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
                with st.expander(f'{cfg.get("icon", "📊")} {scope} — {row["FACT_TABLE"]}'):
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
    section_title("Add / Edit Scope", "➕")
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
# TAB 2 — SIGN-OFF MANAGEMENT
# ══════════════════════════════════════════════════════════════════════════════

with tab_signoff:
    section_title("COB Sign-Off Status", "🔒")
    st.markdown(
        f'<div style="background:{P["info_lt"]};border:1px solid #90CAF9;border-radius:8px;'
        f'padding:0.7rem 1rem;margin-bottom:1rem;font-size:0.85rem">'
        f'When a COB is <strong>signed off</strong> for a scope, no new adjustments can be '
        f'submitted for that COB/process type combination. The submit procedure will reject '
        f'the request with status "Rejected - SignedOff". '
        f'Use this panel to manage sign-off status.'
        f'</div>',
        unsafe_allow_html=True)

    # --- Current sign-off status ---
    try:
        df_signoff = run_query_df("""
            SELECT COBID, PROCESS_TYPE, SIGN_OFF_STATUS, SIGN_OFF_BY,
                   SIGN_OFF_TIMESTAMP, UPDATED_DATE
            FROM ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS
            ORDER BY COBID DESC, PROCESS_TYPE
        """)

        if not df_signoff.empty:
            signed_ct = int(df_signoff[df_signoff["SIGN_OFF_STATUS"] == "SIGNED_OFF"].shape[0])
            open_ct   = len(df_signoff) - signed_ct
            sm1, sm2, sm3 = st.columns(3)
            sm1.metric("Total Entries", len(df_signoff))
            sm2.metric("Signed Off", signed_ct)
            sm3.metric("Open", open_ct)

            st.dataframe(df_signoff, use_container_width=True, height=300)

            # --- Toggle sign-off ---
            st.markdown("<br/>", unsafe_allow_html=True)
            section_title("Toggle Sign-Off Status", "🔄")
            toggle_cols = st.columns(3)
            with toggle_cols[0]:
                cob_options = sorted(df_signoff["COBID"].unique(), reverse=True)
                sel_cob = st.selectbox("COBID", cob_options, key="signoff_toggle_cob")
            with toggle_cols[1]:
                scope_options = sorted(df_signoff[df_signoff["COBID"] == sel_cob]["PROCESS_TYPE"].unique())
                sel_scope = st.selectbox("Process Type", scope_options, key="signoff_toggle_scope")
            with toggle_cols[2]:
                current_row = df_signoff[
                    (df_signoff["COBID"] == sel_cob) & (df_signoff["PROCESS_TYPE"] == sel_scope)
                ]
                current_status = current_row["SIGN_OFF_STATUS"].values[0] if not current_row.empty else "OPEN"
                new_status = "OPEN" if current_status == "SIGNED_OFF" else "SIGNED_OFF"
                st.markdown(f"<br/>", unsafe_allow_html=True)
                if st.button(
                    f"{'🔓 Reopen' if current_status == 'SIGNED_OFF' else '🔒 Sign Off'}",
                    key="toggle_signoff_btn", type="primary"
                ):
                    try:
                        ts = "CURRENT_TIMESTAMP()" if new_status == "SIGNED_OFF" else "NULL"
                        by = f"'{_esc(user)}'" if new_status == "SIGNED_OFF" else "NULL"
                        run_query(f"""
                            UPDATE ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS
                            SET SIGN_OFF_STATUS    = '{_esc(new_status)}',
                                SIGN_OFF_BY        = {by},
                                SIGN_OFF_TIMESTAMP = {ts},
                                UPDATED_DATE       = CURRENT_TIMESTAMP()
                            WHERE COBID = {int(sel_cob)}
                              AND PROCESS_TYPE = '{_esc(sel_scope)}'
                        """)
                        st.success(f"COB {sel_cob} / {sel_scope} → {new_status}")
                        safe_rerun()
                    except Exception as ex:
                        st.error(f"Failed to update sign-off: {ex}")
        else:
            st.info("No sign-off entries yet. Use the form below to create one.")
    except Exception as e:
        st.info(f"Sign-off table not available: {e}")

    # --- Add new sign-off entry ---
    st.markdown("<br/>", unsafe_allow_html=True)
    section_title("Add Sign-Off Entry", "➕")
    with st.form("new_signoff_form"):
        sc1, sc2, sc3 = st.columns(3)
        with sc1:
            so_cobid = st.text_input("COBID", placeholder="e.g. 20260328", key="so_cobid")
        with sc2:
            so_scope = st.selectbox("Process Type", list(SCOPE_CONFIG.keys()), key="so_scope")
        with sc3:
            so_status = st.selectbox("Initial Status", ["OPEN", "SIGNED_OFF"], key="so_status")

        so_submit = st.form_submit_button("Add Entry", type="primary")
        if so_submit:
            if not so_cobid.strip():
                st.error("COBID is required.")
            else:
                try:
                    ts = "CURRENT_TIMESTAMP()" if so_status == "SIGNED_OFF" else "NULL"
                    by = f"'{_esc(user)}'" if so_status == "SIGNED_OFF" else "NULL"
                    cobid_int = int(so_cobid.strip())
                    run_query(f"""
                        MERGE INTO ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS tgt
                        USING (SELECT {cobid_int} AS COBID, '{_esc(so_scope)}' AS PROCESS_TYPE) src
                        ON tgt.COBID = src.COBID AND tgt.PROCESS_TYPE = src.PROCESS_TYPE
                        WHEN MATCHED THEN UPDATE SET
                            SIGN_OFF_STATUS    = '{_esc(so_status)}',
                            SIGN_OFF_BY        = {by},
                            SIGN_OFF_TIMESTAMP = {ts},
                            UPDATED_DATE       = CURRENT_TIMESTAMP()
                        WHEN NOT MATCHED THEN INSERT
                            (COBID, PROCESS_TYPE, SIGN_OFF_STATUS, SIGN_OFF_BY, SIGN_OFF_TIMESTAMP)
                        VALUES ({cobid_int}, '{_esc(so_scope)}', '{_esc(so_status)}', {by}, {ts})
                    """)
                    st.success(f"Sign-off entry created: COB {so_cobid.strip()} / {so_scope} = {so_status}")
                    safe_rerun()
                except Exception as ex:
                    st.error(f"Failed to add entry: {ex}")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — APPROVERS MANAGEMENT
# ══════════════════════════════════════════════════════════════════════════════

with tab_approvers:
    section_title("Authorized Approvers", "👤")
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

            st.dataframe(df_approvers, use_container_width=True, height=300)

            # Deactivate / reactivate
            st.markdown("<br/>", unsafe_allow_html=True)
            section_title("Toggle Approver Status", "🔄")
            toggle_cols = st.columns([2, 1, 1])
            with toggle_cols[0]:
                approver_options = [
                    f"{r['USERNAME']} (ID {r['APPROVER_ID']}) — {'Active' if r['IS_ACTIVE'] else 'Inactive'}"
                    for _, r in df_approvers.iterrows()
                ]
                sel_approver = st.selectbox("Select approver", approver_options, key="toggle_approver")
            with toggle_cols[1]:
                if st.button("✅ Activate", key="activate_approver_btn"):
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
                if st.button("🚫 Deactivate", key="deactivate_approver_btn"):
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
    section_title("Add New Approver", "➕")

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


# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — RECURRING TEMPLATES
# ══════════════════════════════════════════════════════════════════════════════

with tab_recurring:
    section_title("Recurring Adjustment Templates", "🔁")
    st.markdown(
        f'<div style="background:{P["info_lt"]};border:1px solid #90CAF9;border-radius:8px;'
        f'padding:0.7rem 1rem;margin-bottom:1rem;font-size:0.85rem">'
        f'Recurring templates are automatically instantiated by the '
        f'<code>INSTANTIATE_RECURRING_TASK</code> (every 5 min). When a new business date '
        f'falls within the template\'s START_COBID → END_COBID range, a new ADJ_HEADER '
        f'row is created and enters the normal processing pipeline.'
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

            st.dataframe(df_templates, use_container_width=True, height=300)
        else:
            st.info("No recurring templates configured yet.")
    except Exception as e:
        st.info(f"Recurring templates table not available: {e}")

    st.markdown("<br/>", unsafe_allow_html=True)
    section_title("Create New Template", "➕")

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
# TAB 3 — SCHEMA REFERENCE
# ══════════════════════════════════════════════════════════════════════════════

with tab_schema:
    section_title("Database Schema Overview", "🗂️")

    schema_items = [
        ("ADJUSTMENT_APP.ADJ_HEADER",            "TABLE",         "One row per adjustment — lifecycle, metadata, all dimension filters"),
        ("ADJUSTMENT_APP.ADJ_LINE_ITEM",         "TABLE",         "Explicit row-level values for Upload/Direct adjustments"),
        ("ADJUSTMENT_APP.ADJ_STATUS_HISTORY",    "TABLE",         "Append-only audit log of every status change"),
        ("ADJUSTMENT_APP.ADJUSTMENTS_SETTINGS",  "TABLE",         "Config: scope → fact table mapping, PK columns, metrics"),
        ("ADJUSTMENT_APP.ADJ_RECURRING_TEMPLATE","TABLE",         "Templates for automatically recurring adjustments"),
        ("ADJUSTMENT_APP.STREAM_QUEUE_VAR",       "STREAM",        "Standard stream on VW_QUEUE_VAR — fires TASK_PROCESS_VAR"),
        ("ADJUSTMENT_APP.STREAM_QUEUE_STRESS",    "STREAM",        "Standard stream on VW_QUEUE_STRESS — fires TASK_PROCESS_STRESS"),
        ("ADJUSTMENT_APP.STREAM_QUEUE_FRTB",      "STREAM",        "Standard stream on VW_QUEUE_FRTB — fires TASK_PROCESS_FRTB"),
        ("ADJUSTMENT_APP.STREAM_QUEUE_SENSITIVITY","STREAM",       "Standard stream on VW_QUEUE_SENSITIVITY — fires TASK_PROCESS_SENSITIVITY"),
        ("ADJUSTMENT_APP.DT_DASHBOARD",          "DYNAMIC TABLE", "Aggregated metrics by scope, status, entity, user"),
        ("ADJUSTMENT_APP.DT_OVERLAP_ALERTS",     "DYNAMIC TABLE", "Self-join detecting overlapping adjustments"),
        ("ADJUSTMENT_APP.VW_DASHBOARD_KPI",      "VIEW",          "Pre-aggregated KPIs for the dashboard"),
        ("ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS",    "TABLE",         "COB sign-off status per scope. Managed via Admin page"),
        ("ADJUSTMENT_APP.ADJ_APPROVERS",         "TABLE",         "Authorized approvers with optional scope restriction. Managed via Admin page"),
        ("ADJUSTMENT_APP.VW_SIGNOFF_STATUS",     "VIEW",          "COB sign-off status (reads from ADJ_SIGNOFF_STATUS)"),
        ("ADJUSTMENT_APP.VW_RECENT_ACTIVITY",    "VIEW",          "UNION of submissions + status changes"),
        ("ADJUSTMENT_APP.VW_ERRORS",             "VIEW",          "Adjustments with Error status"),

        ("ADJUSTMENT_APP.VW_MY_WORK",            "VIEW",          "All adjustments — filtered by user in Streamlit"),
        ("ADJUSTMENT_APP.VW_PROCESSING_QUEUE",   "VIEW",          "Active pipeline items with queue position"),
        ("ADJUSTMENT_APP.VW_APPROVAL_QUEUE",     "VIEW",          "Adjustments awaiting approval"),
    ]

    df_schema = pd.DataFrame(schema_items, columns=["Object", "Type", "Description"])
    st.dataframe(df_schema, use_container_width=True)

    section_title("Key Design Principles", "💡")
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
    status from the **🔒 Sign-Off Management** tab above.
    """)

    section_title("System Statistics", "📈")
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
# TAB 4 — SQL REFERENCE
# ══════════════════════════════════════════════════════════════════════════════

with tab_sql:
    section_title("Key Stored Procedures", "🔧")

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
            "Stream-driven orchestrator — blocks overlaps, promotes to Running, processes in parallel, unblocks resolved.",
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

    section_title("Snowflake Tasks Configuration", "⚙️")
    st.markdown("""
    Four independent scope tasks, each guarded by a standard stream on its queue view.
    Tasks fire every 1 minute **only when** the stream has data (INSERT or UPDATE on ADJ_HEADER
    that matches the queue view filters).
    """)
    st.code("""
-- VaR pipeline
CREATE OR REPLACE TASK ADJUSTMENT_APP.TASK_PROCESS_VAR
    WAREHOUSE = DVLP_RAVEN_WH_M
    SCHEDULE  = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('ADJUSTMENT_APP.STREAM_QUEUE_VAR')
AS
    CALL ADJUSTMENT_APP.SP_RUN_PIPELINE('VaR', '["VaR"]');

-- Stress pipeline
CREATE OR REPLACE TASK ADJUSTMENT_APP.TASK_PROCESS_STRESS
    WAREHOUSE = DVLP_RAVEN_WH_M
    SCHEDULE  = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('ADJUSTMENT_APP.STREAM_QUEUE_STRESS')
AS
    CALL ADJUSTMENT_APP.SP_RUN_PIPELINE('Stress', '["Stress"]');

-- FRTB pipeline (all sub-types: FRTB, FRTBDRC, FRTBRRAO, FRTBALL)
CREATE OR REPLACE TASK ADJUSTMENT_APP.TASK_PROCESS_FRTB
    WAREHOUSE = DVLP_RAVEN_WH_M
    SCHEDULE  = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('ADJUSTMENT_APP.STREAM_QUEUE_FRTB')
AS
    CALL ADJUSTMENT_APP.SP_RUN_PIPELINE('FRTB', '["FRTB","FRTBDRC","FRTBRRAO","FRTBALL"]');

-- Sensitivity pipeline
CREATE OR REPLACE TASK ADJUSTMENT_APP.TASK_PROCESS_SENSITIVITY
    WAREHOUSE = DVLP_RAVEN_WH_M
    SCHEDULE  = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('ADJUSTMENT_APP.STREAM_QUEUE_SENSITIVITY')
AS
    CALL ADJUSTMENT_APP.SP_RUN_PIPELINE('Sensitivity', '["Sensitivity"]');
    """, language="sql")
