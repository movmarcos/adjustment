"""
Admin — Metadata Management
=============================
Configure scopes, dimension/measure visibility, and user access.
Only accessible to ADJ_ADMIN role.
"""
import streamlit as st
import pandas as pd

st.set_page_config(
    page_title="Admin · MUFG",
    page_icon="⚙️",
    layout="wide",
    initial_sidebar_state="expanded",
)

from _OLD.prototype_v2.data.state_manager import init_state, current_user, can_admin
from _OLD.prototype_v2.data.mock_data import SCOPES, USERS, ROLE_LABELS
from _OLD.prototype_v2.data.styles import inject_css, render_sidebar, section_title, P

init_state()
inject_css()
render_sidebar()

user = current_user()

st.markdown("## ⚙️ Admin — Metadata Configuration")

if not can_admin():
    st.error("🔒 Access denied. This page requires the ADJ_ADMIN role.")
    st.markdown(f"Your current role is **{user['role']}**. Contact your administrator to request access.")
    st.stop()

st.markdown(
    f"<span style='color:{P['grey_700']};font-size:0.9rem'>"
    "Manage source configurations, dimension/measure metadata, and user access. "
    "In production, changes here write to <code>CORE.ADJ_DIMENSION_CONFIG</code> and "
    "<code>CORE.ADJ_MEASURE_CONFIG</code> in Snowflake."
    "</span>", unsafe_allow_html=True)
st.markdown("<br/>", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
tab_scopes, tab_users, tab_schema, tab_sql = st.tabs([
    "📊 Source Configuration",
    "👥 User Access",
    "🗂️ Schema Reference",
    "🔧 Snowflake SQL Reference",
])

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — SCOPE CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════
with tab_scopes:
    section_title("Configured Data Sources", "📊")
    st.markdown(
        f'<div style="background:{P["info_lt"]};border:1px solid #90CAF9;border-radius:8px;'
        f'padding:0.7rem 1rem;margin-bottom:1rem;font-size:0.85rem">'
        f'In production, this metadata is stored in <code>CORE.ADJ_DIMENSION_CONFIG</code> and '
        f'<code>CORE.ADJ_MEASURE_CONFIG</code>. The UI reads these tables dynamically — '
        f'add a new row to onboard a new data source without any code changes.'
        f'</div>',
        unsafe_allow_html=True)

    for sk, scope in SCOPES.items():
        with st.expander(f'{scope["icon"]} {scope["full_label"]} ({sk}) — {scope["source_system"]}'):
            c1, c2 = st.columns(2)

            with c1:
                section_title("Dimensions")
                dim_rows = []
                for d in scope["dimensions"]:
                    dim_rows.append({
                        "Column":      d["column"],
                        "Label":       d["label"],
                        "Required":    "✓" if d["required"] else "",
                        "Multi-select":"✓" if d["multi"] else "",
                        "Values":      ", ".join(d["values"][:4]) + (f" +{len(d['values'])-4} more" if len(d["values"]) > 4 else ""),
                        "Tooltip":     d.get("tooltip",""),
                    })
                st.dataframe(pd.DataFrame(dim_rows), use_container_width=True,
                             hide_index=True)

                st.markdown(
                    f'<div style="font-size:0.78rem;color:{P["grey_700"]};margin-top:0.5rem">'
                    f'To add/remove dimensions, update <code>CORE.ADJ_DIMENSION_CONFIG</code> '
                    f'with SCOPE_KEY = \'{sk}\'</div>', unsafe_allow_html=True)

            with c2:
                section_title("Measures")
                m_rows = []
                for m in scope["measures"]:
                    m_rows.append({
                        "Column":    m["column"],
                        "Label":     m["label"],
                        "Format":    m["fmt"],
                        "Primary":   "✓" if m["primary"] else "",
                    })
                st.dataframe(pd.DataFrame(m_rows), use_container_width=True, hide_index=True)

                st.markdown(
                    f'<div style="font-size:0.78rem;color:{P["grey_700"]};margin-top:0.5rem">'
                    f'To change measures, update <code>CORE.ADJ_MEASURE_CONFIG</code> '
                    f'with SCOPE_KEY = \'{sk}\'</div>', unsafe_allow_html=True)

            # Source config summary
            section_title("Source Details")
            st.markdown(
                f'<table style="font-size:0.83rem;border-collapse:collapse">'
                f'<tr><td style="color:{P["grey_700"]};padding:3px 12px 3px 0;width:140px">Source System</td>'
                f'<td><strong>{scope["source_system"]}</strong></td></tr>'
                f'<tr><td style="color:{P["grey_700"]};padding:3px 12px 3px 0">Fact Table</td>'
                f'<td><code>FACT.{sk}_FACT</code></td></tr>'
                f'<tr><td style="color:{P["grey_700"]};padding:3px 12px 3px 0">Dimensions</td>'
                f'<td>{len(scope["dimensions"])}</td></tr>'
                f'<tr><td style="color:{P["grey_700"]};padding:3px 12px 3px 0">Measures</td>'
                f'<td>{len(scope["measures"])}</td></tr>'
                f'</table>',
                unsafe_allow_html=True)

    st.markdown("<br/>", unsafe_allow_html=True)
    section_title("Add New Source", "➕")
    st.markdown(
        f'<div style="background:{P["grey_100"]};border-radius:8px;padding:1rem;'
        f'font-size:0.85rem;color:{P["grey_700"]}">'
        f'To add a new data source in production:<br/>'
        f'1. Ensure the fact table exists in the <code>FACT</code> schema.<br/>'
        f'2. Insert rows into <code>CORE.ADJ_DIMENSION_CONFIG</code> with the new SCOPE_KEY.<br/>'
        f'3. Insert rows into <code>CORE.ADJ_MEASURE_CONFIG</code> with the new SCOPE_KEY.<br/>'
        f'4. The UI will automatically include the new source on next load.<br/>'
        f'No Streamlit code changes required.'
        f'</div>',
        unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — USER ACCESS
# ══════════════════════════════════════════════════════════════════════════════
with tab_users:
    section_title("User Roles & Entity Access", "👥")
    st.markdown(
        f'<div style="background:{P["info_lt"]};border:1px solid #90CAF9;border-radius:8px;'
        f'padding:0.7rem 1rem;margin-bottom:1rem;font-size:0.85rem">'
        f'In production, role assignments are managed via Snowflake RBAC (<code>GRANT ROLE</code>). '
        f'Entity-level row access is controlled by <code>CORE.USER_ENTITY_ACCESS</code> '
        f'and enforced by a Row Access Policy on all fact and adjustment tables.'
        f'</div>',
        unsafe_allow_html=True)

    user_rows = []
    for u in USERS:
        role_lbl, role_color = ROLE_LABELS.get(u["role"], ("Operator","#78909C"))
        user_rows.append({
            "User ID":      u["id"],
            "Name":         u["name"],
            "Role":         u["role"],
            "Entities":     ", ".join(u["entity_access"]),
            "Can Create":   "✓" if u["role"] in ("ADJ_OPERATOR","ADJ_APPROVER","ADJ_ADMIN") else "",
            "Can Approve":  "✓" if u["role"] in ("ADJ_APPROVER","ADJ_ADMIN") else "",
            "Can Apply":    "✓" if u["role"] in ("ADJ_OPERATOR","ADJ_APPROVER","ADJ_ADMIN") else "",
            "Can Admin":    "✓" if u["role"] == "ADJ_ADMIN" else "",
        })
    df_users = pd.DataFrame(user_rows)
    st.dataframe(df_users, use_container_width=True, hide_index=True)

    st.markdown("<br/>", unsafe_allow_html=True)
    section_title("Role Hierarchy", "🏛️")

    role_html = ""
    for role, desc, perms in [
        ("ADJ_VIEWER",   "Read-only access",
         "SELECT on all tables and dynamic tables"),
        ("ADJ_OPERATOR", "Create and apply adjustments",
         "All VIEWER permissions + INSERT on ADJ_HEADER, ADJ_LINE_ITEM + SP_PREVIEW + SP_CREATE"),
        ("ADJ_APPROVER", "Approve and manage adjustments",
         "All OPERATOR permissions + UPDATE ADJ_HEADER status + SP_REVERSE + cannot self-approve"),
        ("ADJ_ADMIN",    "Full control",
         "All APPROVER permissions + DDL on all objects + metadata management + bypass self-approval guard"),
    ]:
        _, color = ROLE_LABELS.get(role, ("","#9E9E9E"))
        role_html += (
            f'<div style="border-left:4px solid {color};padding:0.6rem 1rem;margin-bottom:0.5rem;'
            f'background:{color}11;border-radius:0 6px 6px 0">'
            f'<span style="font-weight:700;color:{color}">{role}</span>'
            f'<span style="color:{P["grey_700"]};font-size:0.82rem;margin-left:8px">{desc}</span>'
            f'<br/><span style="font-size:0.78rem;color:{P["grey_700"]}">{perms}</span>'
            f'</div>')
    st.markdown(role_html, unsafe_allow_html=True)

    section_title("Row Access Policy", "🔒")
    st.code("""
-- CORE.RAP_ENTITY_ACCESS  (applied to FACT_TABLE, ADJ_LINE_ITEM, FACT_ADJUSTED)
CREATE OR REPLACE ROW ACCESS POLICY CORE.RAP_ENTITY_ACCESS
  AS (entity_key VARCHAR) RETURNS BOOLEAN ->
  CASE
    WHEN CURRENT_ROLE() = 'ADJ_ADMIN' THEN TRUE
    ELSE entity_key IN (
        SELECT entity_key FROM CORE.USER_ENTITY_ACCESS
        WHERE user_name = CURRENT_USER()
    )
  END;
    """, language="sql")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — SCHEMA REFERENCE
# ══════════════════════════════════════════════════════════════════════════════
with tab_schema:
    section_title("Database Schema Overview", "🗂️")

    schema_items = [
        ("CORE.ADJ_HEADER",            "HYBRID TABLE", "One row per adjustment — lifecycle, metadata, AI summary"),
        ("CORE.ADJ_LINE_ITEM",         "HYBRID TABLE", "Delta rows — one row per affected fact record"),
        ("CORE.ADJ_STATUS_HISTORY",    "TABLE",        "Append-only audit log of every status change"),
        ("CORE.ADJ_DIMENSION_CONFIG",  "TABLE",        "Metadata: which dimensions each source exposes in the UI"),
        ("CORE.ADJ_MEASURE_CONFIG",    "TABLE",        "Metadata: which measures each source exposes"),
        ("CORE.USER_ENTITY_ACCESS",    "TABLE",        "Row-level security: user → entity mappings"),
        ("FACT.FACT_TABLE",            "TABLE",        "Immutable source-of-truth fact data (never modified)"),
        ("MART.FACT_ADJUSTED",         "DYNAMIC TABLE","UNION ALL of fact + applied deltas, refreshed every 1 min"),
        ("MART.ADJUSTMENT_IMPACT_SUMMARY","DYNAMIC TABLE","Pre-aggregated impact metrics per adjustment"),
        ("MART.DAILY_ADJUSTMENT_ACTIVITY","DYNAMIC TABLE","Daily counts by type, status, user (5 min lag)"),
        ("AI.V_ADJUSTMENT_SEARCH_CORPUS","VIEW",       "Concatenated text for Cortex Search indexing"),
        ("AI.V_DAILY_ADJ_METRICS",     "VIEW",         "Daily metrics for anomaly detection charts"),
    ]

    df_schema = pd.DataFrame(schema_items, columns=["Object", "Type", "Description"])
    st.dataframe(df_schema, use_container_width=True, hide_index=True)

    section_title("Key Design Principles", "💡")
    st.markdown("""
    **1. Delta Pattern** — `FACT_TABLE` is **never modified**. Adjustments store only the offset
    (`new - original`). The adjusted view materialises as `SUM(fact) + SUM(deltas)`.

    **2. Hybrid Tables** — `ADJ_HEADER` and `ADJ_LINE_ITEM` use Snowflake Hybrid Tables for
    row-level ACID transactions, enabling safe concurrent writes from multiple Streamlit users.

    **3. Dynamic Tables** — `FACT_ADJUSTED` refreshes every minute automatically via a chained
    Dynamic Table graph. No ETL jobs needed.

    **4. Metadata-Driven UI** — `ADJ_DIMENSION_CONFIG` and `ADJ_MEASURE_CONFIG` drive what
    filters and fields appear in Streamlit. New source = new config rows, no code changes.

    **5. Async Processing** — Adjustments are applied by Snowflake Tasks triggered by Streams,
    not by the Streamlit session. Users are never blocked waiting for large operations.

    **6. Full Audit Trail** — Every status change is logged to `ADJ_STATUS_HISTORY`.
    Reversals create new negating adjustments, never deletes.
    """)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — SQL REFERENCE
# ══════════════════════════════════════════════════════════════════════════════
with tab_sql:
    section_title("Key Stored Procedures", "🔧")

    procs = {
        "SP_PREVIEW_ADJUSTMENT": (
            "Returns a preview of what the adjustment will change — no writes.",
            """CALL CORE.SP_PREVIEW_ADJUSTMENT(
    adj_type       => 'SCALE',
    filter_json    => '{"entity_key":["US_HQ"],"currency_key":["EUR"]}',
    target_date    => '2026-03-25',
    scale_factor   => 1.05,
    scope_key      => 'PNL'
);"""
        ),
        "SP_CREATE_ADJUSTMENT": (
            "Creates the adjustment header and writes delta line items.",
            """CALL CORE.SP_CREATE_ADJUSTMENT(
    scope_key      => 'PNL',
    adj_type       => 'SCALE',
    filter_json    => '{"entity_key":["US_HQ"],"currency_key":["EUR"]}',
    target_date    => '2026-03-25',
    scale_factor   => 1.05,
    business_reason=> 'Q1 close EUR reallocation',
    ticket_ref     => 'FIN-2847'
);"""
        ),
        "SP_UPDATE_ADJUSTMENT_STATUS": (
            "Transitions adjustment through the lifecycle state machine.",
            """CALL CORE.SP_UPDATE_ADJUSTMENT_STATUS(
    adj_id     => 42,
    new_status => 'APPROVED',
    comment    => 'Confirmed with desk head.'
);"""
        ),
        "SP_REVERSE_ADJUSTMENT": (
            "Creates a negating adjustment to undo an applied one.",
            """CALL CORE.SP_REVERSE_ADJUSTMENT(
    adj_id => 42,
    reason => 'Booking error identified post-close.'
);"""
        ),
        "AI.SP_DETECT_ADJUSTMENT_ANOMALIES": (
            "Calls Cortex to analyse recent adjustments for unusual patterns.",
            """CALL AI.SP_DETECT_ADJUSTMENT_ANOMALIES();
-- Returns JSON array:
-- [{"adj_id":6,"reason":"Scale factor 1.20 is unusually high","severity":"HIGH"}]"""
        ),
    }

    for proc_name, (desc, code) in procs.items():
        with st.expander(f"`{proc_name}` — {desc}"):
            st.code(code, language="sql")

    section_title("Snowflake Tasks Configuration", "⚙️")
    st.code("""
-- Main processing task (fires when stream has data)
CREATE OR REPLACE TASK TASK_ADJ_PROCESSOR
    WAREHOUSE = ADJUSTMENT_TASK_WH
    SCHEDULE  = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('CORE.ADJ_HEADER_STREAM')
AS
    CALL CORE.SP_PROCESS_APPROVED_QUEUE();

-- AI summarisation (child of processor)
CREATE OR REPLACE TASK TASK_AI_SUMMARIZE
    WAREHOUSE = ADJUSTMENT_TASK_WH
    AFTER     TASK_ADJ_PROCESSOR
AS
    CALL AI.SP_GENERATE_AI_SUMMARY();

-- Daily anomaly detection
CREATE OR REPLACE TASK TASK_ANOMALY_CHECK
    WAREHOUSE = ADJUSTMENT_TASK_WH
    SCHEDULE  = 'USING CRON 0 8 * * MON-FRI America/New_York'
AS
    CALL AI.SP_DETECT_ADJUSTMENT_ANOMALIES();

-- Recurring adjustment trigger (called after COB file load by external scheduler)
CREATE OR REPLACE TASK TASK_RECURRING_TRIGGER
    WAREHOUSE = ADJUSTMENT_TASK_WH
    -- Triggered externally via EXECUTE TASK after COB completion
AS
    CALL CORE.SP_APPLY_RECURRING_ADJUSTMENTS(CURRENT_DATE());
    """, language="sql")
