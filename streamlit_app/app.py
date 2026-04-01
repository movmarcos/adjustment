"""
Adjustment Engine — Command Center (Dashboard)
===============================================
Main entry point. Live overview of all adjustments, queue, and pending actions.
Reads from: VW_DASHBOARD_KPI, DT_DASHBOARD, DT_OVERLAP_ALERTS, VW_RECENT_ACTIVITY.
"""
import streamlit as st
import plotly.graph_objects as go

st.set_page_config(
    page_title="Adjustment Engine · MUFG",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

from utils.styles import inject_css, render_sidebar, status_badge, kpi_card, section_title, P, SCOPE_CONFIG
from utils.snowflake_conn import run_query_df, current_user_name

inject_css()
render_sidebar()

user = current_user_name()

st.markdown("## 🏠 Command Center")
st.markdown(
    f"<span style='color:{P['grey_700']};font-size:0.9rem'>"
    f"Welcome, {user}. Here's your adjustment landscape.</span>",
    unsafe_allow_html=True)
st.markdown("<br/>", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# KPI ROW — from VW_DASHBOARD_KPI
# ──────────────────────────────────────────────────────────────────────────────

try:
    df_kpi = run_query_df("""
        SELECT
            COALESCE(SUM(TOTAL_ADJUSTMENTS), 0)       AS TOTAL,
            COALESCE(SUM(PENDING_COUNT), 0)            AS PENDING,
            COALESCE(SUM(PENDING_APPROVAL_COUNT), 0)   AS PENDING_APPROVAL,
            COALESCE(SUM(APPROVED_COUNT), 0)           AS APPROVED,
            COALESCE(SUM(RUNNING_COUNT), 0)            AS RUNNING,
            COALESCE(SUM(PROCESSED_COUNT), 0)          AS PROCESSED,
            COALESCE(SUM(FAILED_COUNT), 0)             AS FAILED,
            COALESCE(SUM(OVERLAP_ALERTS), 0)           AS OVERLAPS
        FROM ADJUSTMENT_APP.VW_DASHBOARD_KPI
    """)
    kpis = df_kpi.iloc[0].to_dict() if not df_kpi.empty else {}
except Exception as e:
    kpis = {}
    st.warning(f"Could not load KPIs: {e}")

cols = st.columns(8)
kpi_data = [
    ("Total",            kpis.get("TOTAL", 0),            "All adjustments",  "primary"),
    ("Pending",          kpis.get("PENDING", 0),           "Awaiting queue",   "warning"),
    ("Awaiting Approval",kpis.get("PENDING_APPROVAL", 0),  "Need approval",    "info"),
    ("Approved",         kpis.get("APPROVED", 0),          "Ready to process", "success"),
    ("Running",          kpis.get("RUNNING", 0),           "Processing now",   "info"),
    ("Processed",        kpis.get("PROCESSED", 0),         "In the data",      "success"),
    ("Failed",           kpis.get("FAILED", 0),            "Need attention",   "danger"),
    ("Overlaps",         kpis.get("OVERLAPS", 0),          "Overlap alerts",   "purple"),
]
for col, (lbl, val, sub, variant) in zip(cols, kpi_data):
    col.markdown(kpi_card(lbl, int(val), sub, variant), unsafe_allow_html=True)

st.markdown("<br/>", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# MAIN CONTENT: 3 columns
# ──────────────────────────────────────────────────────────────────────────────

col_left, col_mid, col_right = st.columns([2, 1.4, 1.4])

# ── Status by Scope (from DT_DASHBOARD) ─────────────────────────────────────
with col_left:
    section_title("Adjustments by Scope & Status", "📊")

    try:
        df_dash = run_query_df("""
            SELECT PROCESS_TYPE, RUN_STATUS,
                   SUM(ADJUSTMENT_COUNT) AS CNT
            FROM ADJUSTMENT_APP.DT_DASHBOARD
            GROUP BY PROCESS_TYPE, RUN_STATUS
            ORDER BY PROCESS_TYPE, RUN_STATUS
        """)
        if not df_dash.empty:
            scopes = df_dash["PROCESS_TYPE"].unique()
            statuses = ["Pending", "Pending Approval", "Approved", "Running", "Processed", "Failed"]
            color_map = {
                "Pending":          P["warning"],
                "Pending Approval": P["info"],
                "Approved":         "#00897B",
                "Running":          "#1565C0",
                "Processed":        P["success"],
                "Failed":           P["danger"],
            }

            fig = go.Figure()
            for status in statuses:
                vals = []
                for scope in scopes:
                    mask = (df_dash["PROCESS_TYPE"] == scope) & (df_dash["RUN_STATUS"] == status)
                    v = df_dash.loc[mask, "CNT"].sum() if mask.any() else 0
                    vals.append(int(v))
                if any(v > 0 for v in vals):
                    fig.add_trace(go.Bar(
                        x=[str(s) for s in scopes], y=vals,
                        name=status, marker_color=color_map.get(status, P["grey_400"]),
                    ))

            fig.update_layout(
                barmode="stack", plot_bgcolor="white", paper_bgcolor="white",
                margin=dict(l=0, r=0, t=10, b=0), height=250,
                legend=dict(orientation="h", yanchor="bottom", y=1, xanchor="left", x=0, font_size=11),
                xaxis=dict(showgrid=False, tickfont_size=11),
                yaxis=dict(showgrid=True, gridcolor="#F0F0F0", tickfont_size=10),
                font_family="Inter",
            )
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
        else:
            st.info("No adjustment data yet.")
    except Exception as e:
        st.info(f"No data available: {e}")

    # Top users
    section_title("Top Submitters", "👤")
    try:
        df_users = run_query_df("""
            SELECT USERNAME, COUNT(*) AS CNT
            FROM ADJUSTMENT_APP.ADJ_HEADER
            WHERE IS_DELETED = FALSE
            GROUP BY USERNAME
            ORDER BY CNT DESC
            LIMIT 5
        """)
        if not df_users.empty:
            for _, row in df_users.iterrows():
                st.markdown(
                    f'<div style="display:flex;justify-content:space-between;padding:0.3rem 0;'
                    f'font-size:0.85rem">'
                    f'<span>{row["USERNAME"]}</span>'
                    f'<span style="font-weight:700;color:{P["accent"]}">{row["CNT"]}</span></div>',
                    unsafe_allow_html=True)
    except Exception:
        pass

# ── Overlap Alerts ──────────────────────────────────────────────────────────
with col_mid:
    section_title("Overlap Alerts", "⚠️")
    try:
        df_overlaps = run_query_df("""
            SELECT ADJ_ID_A, ADJ_ID_B, PROCESS_TYPE, ENTITY_A, ENTITY_B,
                   BOOK_A, BOOK_B, COBID, ALERT_MESSAGE
            FROM ADJUSTMENT_APP.DT_OVERLAP_ALERTS
            ORDER BY COBID DESC
            LIMIT 10
        """)
        if not df_overlaps.empty:
            for _, row in df_overlaps.iterrows():
                st.markdown(
                    f'<div class="overlap-box" style="padding:0.7rem 0.9rem;margin-bottom:0.4rem">'
                    f'<div style="font-size:0.82rem;font-weight:600;color:{P["warning"]}">'
                    f'⚠️ ADJ #{row["ADJ_ID_A"]} ↔ #{row["ADJ_ID_B"]}</div>'
                    f'<div style="font-size:0.75rem;color:{P["grey_700"]};margin-top:3px">'
                    f'{row["PROCESS_TYPE"]} · COB {row["COBID"]}</div>'
                    f'</div>',
                    unsafe_allow_html=True)
        else:
            st.markdown(
                f'<div class="mcard" style="text-align:center;color:#9E9E9E;font-size:0.85rem;padding:2rem">'
                f'✅ No overlaps detected</div>',
                unsafe_allow_html=True)
    except Exception:
        st.markdown(
            f'<div class="mcard" style="text-align:center;color:#9E9E9E;font-size:0.85rem;padding:2rem">'
            f'✅ No overlaps detected</div>',
            unsafe_allow_html=True)

    # Errors panel
    section_title("Current Errors", "❌")
    try:
        df_errors = run_query_df("""
            SELECT ADJ_ID, PROCESS_TYPE, ENTITY_CODE, ERRORMESSAGE, USERNAME
            FROM ADJUSTMENT_APP.VW_ERRORS
            ORDER BY ERROR_TIME DESC LIMIT 5
        """)
        if not df_errors.empty:
            for _, row in df_errors.iterrows():
                msg = str(row.get("ERRORMESSAGE", ""))[:80]
                st.markdown(
                    f'<div class="queue-item failed">'
                    f'<span style="font-weight:700;font-size:0.82rem">ADJ #{row["ADJ_ID"]}</span>'
                    f'<span style="float:right;font-size:0.72rem;color:{P["danger"]};font-weight:600">'
                    f'{row["PROCESS_TYPE"]}</span>'
                    f'<div style="font-size:0.75rem;color:{P["grey_700"]};margin-top:3px">{msg}</div>'
                    f'</div>',
                    unsafe_allow_html=True)
        else:
            st.markdown(
                f'<div class="mcard" style="text-align:center;color:#9E9E9E;font-size:0.85rem;padding:2rem">'
                f'✅ No errors</div>',
                unsafe_allow_html=True)
    except Exception:
        st.markdown(
            f'<div class="mcard" style="text-align:center;color:#9E9E9E;font-size:0.85rem;padding:2rem">'
            f'✅ No errors</div>',
            unsafe_allow_html=True)

# ── Recent Activity ─────────────────────────────────────────────────────────
with col_right:
    pass  # Recent Activity moved to full-width grid at the bottom

# ──────────────────────────────────────────────────────────────────────────────
# LAST 5 COBs — TREND CHARTS
# ──────────────────────────────────────────────────────────────────────────────

st.markdown("<br/>", unsafe_allow_html=True)
section_title("Last 5 COBs — Activity Trend", "📈")

try:
    df_cob = run_query_df("""
        SELECT
            COBID,
            PROCESS_TYPE,
            COUNT(*)                        AS ADJ_COUNT,
            COALESCE(SUM(RECORD_COUNT), 0)  AS ROW_COUNT
        FROM ADJUSTMENT_APP.ADJ_HEADER
        WHERE IS_DELETED = FALSE
          AND COBID IN (
              SELECT DISTINCT COBID
              FROM ADJUSTMENT_APP.ADJ_HEADER
              WHERE IS_DELETED = FALSE
              ORDER BY COBID DESC
              LIMIT 5
          )
        GROUP BY COBID, PROCESS_TYPE
        ORDER BY COBID, PROCESS_TYPE
    """)

    if not df_cob.empty:
        cobs = sorted(df_cob["COBID"].unique())
        cob_labels = [str(c) for c in cobs]

        ch1, ch2 = st.columns(2)

        # ── Chart 1: Adjustments submitted per COB, stacked by scope ──────
        with ch1:
            st.markdown(
                f"<div style='font-size:0.82rem;font-weight:600;color:{P['grey_700']};"
                f"margin-bottom:0.3rem'>Adjustments Submitted per COB</div>",
                unsafe_allow_html=True)
            fig1 = go.Figure()
            scopes = df_cob["PROCESS_TYPE"].unique()
            scope_colors = [
                SCOPE_CONFIG.get(s, {}).get("color", P["grey_400"]) for s in scopes
            ]
            for scope, color in zip(scopes, scope_colors):
                vals = []
                for cob in cobs:
                    mask = (df_cob["COBID"] == cob) & (df_cob["PROCESS_TYPE"] == scope)
                    vals.append(int(df_cob.loc[mask, "ADJ_COUNT"].sum()) if mask.any() else 0)
                fig1.add_trace(go.Bar(
                    x=cob_labels, y=vals, name=scope, marker_color=color,
                ))
            fig1.update_layout(
                barmode="stack", plot_bgcolor="white", paper_bgcolor="white",
                margin=dict(l=0, r=0, t=10, b=0), height=240,
                legend=dict(orientation="h", yanchor="bottom", y=1, xanchor="left", x=0, font_size=11),
                xaxis=dict(showgrid=False, tickfont_size=11, title="COB"),
                yaxis=dict(showgrid=True, gridcolor="#F0F0F0", tickfont_size=10, title="Adjustments"),
                font_family="Inter",
            )
            st.plotly_chart(fig1, use_container_width=True, config={"displayModeBar": False})

        # ── Chart 2: Total rows adjusted per COB, line by scope ───────────
        with ch2:
            st.markdown(
                f"<div style='font-size:0.82rem;font-weight:600;color:{P['grey_700']};"
                f"margin-bottom:0.3rem'>Rows Adjusted per COB</div>",
                unsafe_allow_html=True)
            fig2 = go.Figure()
            for scope, color in zip(scopes, scope_colors):
                vals = []
                for cob in cobs:
                    mask = (df_cob["COBID"] == cob) & (df_cob["PROCESS_TYPE"] == scope)
                    vals.append(int(df_cob.loc[mask, "ROW_COUNT"].sum()) if mask.any() else 0)
                fig2.add_trace(go.Scatter(
                    x=cob_labels, y=vals, name=scope,
                    mode="lines+markers",
                    line=dict(color=color, width=2),
                    marker=dict(color=color, size=7),
                ))
            fig2.update_layout(
                plot_bgcolor="white", paper_bgcolor="white",
                margin=dict(l=0, r=0, t=10, b=0), height=240,
                legend=dict(orientation="h", yanchor="bottom", y=1, xanchor="left", x=0, font_size=11),
                xaxis=dict(showgrid=False, tickfont_size=11, title="COB"),
                yaxis=dict(showgrid=True, gridcolor="#F0F0F0", tickfont_size=10, title="Rows"),
                font_family="Inter",
            )
            st.plotly_chart(fig2, use_container_width=True, config={"displayModeBar": False})

    else:
        st.info("No COB data available yet.")
except Exception as e:
    st.info(f"COB trend data not available: {e}")

# ──────────────────────────────────────────────────────────────────────────────
# RECENT ACTIVITY — full-width grid
# ──────────────────────────────────────────────────────────────────────────────

st.markdown("<br/>", unsafe_allow_html=True)
section_title("Recent Activity", "🕐")

try:
    df_activity = run_query_df("""
        SELECT
            ADJ_ID          AS "Adj #",
            EVENT_TIME      AS "Time",
            EVENT_TYPE      AS "Event",
            CURRENT_STATUS  AS "Status",
            PROCESS_TYPE    AS "Scope",
            ADJUSTMENT_TYPE AS "Type",
            ACTOR           AS "User",
            EVENT_DETAIL    AS "Detail"
        FROM ADJUSTMENT_APP.VW_RECENT_ACTIVITY
        ORDER BY EVENT_TIME DESC
        LIMIT 50
    """)

    if not df_activity.empty:
        def _colour_status(val):
            colours = {
                "Processed":        f"color:{P['success']};font-weight:600",
                "Failed":           f"color:{P['danger']};font-weight:600",
                "Running":          f"color:{P['info']};font-weight:600",
                "Pending":          f"color:{P['warning']};font-weight:600",
                "Pending Approval": f"color:{P['info']};font-weight:600",
                "Approved":         "color:#00897B;font-weight:600",
            }
            return colours.get(val, "")

        st.dataframe(
            df_activity.style.map(_colour_status, subset=["Status"]),
            use_container_width=True,
            height=400,
            hide_index=True,
        )
    else:
        st.info("No activity yet.")
except Exception:
    st.info("No activity data available.")
