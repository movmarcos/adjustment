"""
Adjustment Engine — Command Center (Dashboard)
===============================================
Main entry point. Live overview of all adjustments, queue, and pending actions.
Reads from: VW_DASHBOARD_KPI, DT_DASHBOARD, DT_OVERLAP_ALERTS, VW_RECENT_ACTIVITY.
"""
import streamlit as st
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime
import pytz

st.set_page_config(
    page_title="Adjustment Engine · MUFG",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

from utils.styles import inject_css, render_sidebar, section_title, P, SCOPE_CONFIG
from utils.snowflake_conn import run_query_df, current_user_name

inject_css()
render_sidebar()

user = current_user_name()

# ──────────────────────────────────────────────────────────────────────────────
# KPIs — load once, used in header + cards
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

failed  = int(kpis.get("FAILED", 0))
running = int(kpis.get("RUNNING", 0))
pending = int(kpis.get("PENDING", 0)) + int(kpis.get("APPROVED", 0))

if failed > 0:
    health_color, health_label, health_dot = "#D32F2F", "CRITICAL", "🔴"
elif running > 0:
    health_color, health_label, health_dot = "#1565C0", "PROCESSING", "🔵"
elif pending > 0:
    health_color, health_label, health_dot = "#E65100", "QUEUED", "🟡"
else:
    health_color, health_label, health_dot = "#2E7D32", "HEALTHY", "🟢"

# ──────────────────────────────────────────────────────────────────────────────
# HEADER BANNER
# ──────────────────────────────────────────────────────────────────────────────

london_now = datetime.now(pytz.timezone("Europe/London")).strftime("%d %b %Y  %H:%M")

st.markdown(f"""
<div style="background:linear-gradient(135deg,{P['accent']} 0%,#2A2A48 100%);
  border-radius:14px;padding:1.4rem 2rem;margin-bottom:1.2rem;
  display:flex;justify-content:space-between;align-items:center;
  box-shadow:0 4px 24px rgba(0,0,0,.18)">
  <div>
    <div style="font-size:1.5rem;font-weight:800;color:white;letter-spacing:.01em;line-height:1">
      📊 Adjustment Engine
    </div>
    <div style="font-size:0.87rem;color:rgba(255,255,255,.55);margin-top:6px">
      Command Center &nbsp;·&nbsp; Welcome, <strong style="color:white">{user}</strong>
    </div>
  </div>
  <div style="display:flex;align-items:center;gap:2rem">
    <div style="text-align:center">
      <div style="font-size:0.65rem;color:rgba(255,255,255,.4);text-transform:uppercase;
        letter-spacing:.1em;margin-bottom:3px">System Status</div>
      <div style="background:rgba(255,255,255,.1);border:1px solid {health_color}44;
        border-radius:99px;padding:4px 14px;font-size:0.78rem;font-weight:700;
        color:{health_color};backdrop-filter:blur(4px)">
        {health_dot} {health_label}
      </div>
    </div>
    <div style="text-align:right">
      <div style="font-size:0.65rem;color:rgba(255,255,255,.4);text-transform:uppercase;
        letter-spacing:.1em;margin-bottom:3px">London Time</div>
      <div style="font-size:1.05rem;font-weight:700;color:white;letter-spacing:.03em">
        {london_now}
      </div>
    </div>
  </div>
</div>
""", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# KPI STRIP
# ──────────────────────────────────────────────────────────────────────────────

queued = int(kpis.get("PENDING", 0)) + int(kpis.get("APPROVED", 0))

kpi_items = [
    ("Total",             int(kpis.get("TOTAL", 0)),           "All adjustments",      P["primary"],   "📋"),
    ("Queued",            queued,                               "Pending + Approved",   P["warning"],   "⏸"),
    ("Awaiting Approval", int(kpis.get("PENDING_APPROVAL", 0)), "Need approval",        P["info"],      "📝"),
    ("Running",           int(kpis.get("RUNNING", 0)),          "Processing now",       P["info"],      "⚡"),
    ("Processed",         int(kpis.get("PROCESSED", 0)),        "In the data",          P["success"],   "✔"),
    ("Failed",            int(kpis.get("FAILED", 0)),           "Need attention",       P["danger"],    "✗"),
    ("Overlaps",          int(kpis.get("OVERLAPS", 0)),         "Overlap alerts",       P["purple"],    "⚠️"),
]

cards_html = '<div style="display:grid;grid-template-columns:repeat(7,1fr);gap:10px;margin-bottom:1.4rem">'
for label, val, sub, color, icon in kpi_items:
    alert_style = f"box-shadow:0 0 0 2px {color}44;" if (label == "Failed" and val > 0) or (label == "Overlaps" and val > 0) else ""
    cards_html += f"""
    <div style="background:white;border:1px solid {P['border']};border-top:3px solid {color};
      border-radius:10px;padding:0.9rem 0.8rem;{alert_style}
      box-shadow:0 2px 8px rgba(0,0,0,.05)">
      <div style="font-size:0.62rem;font-weight:700;text-transform:uppercase;
        letter-spacing:.08em;color:{P['grey_700']};margin-bottom:0.3rem">{label}</div>
      <div style="font-size:1.75rem;font-weight:800;color:{color if val > 0 else P['grey_400']};
        line-height:1">{icon} {val}</div>
      <div style="font-size:0.68rem;color:{P['grey_700']};margin-top:4px">{sub}</div>
    </div>"""
cards_html += '</div>'
st.markdown(cards_html, unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# MAIN CONTENT: charts left | alerts right
# ──────────────────────────────────────────────────────────────────────────────

col_charts, col_alerts = st.columns([2.2, 1.8])

# ── LEFT: charts ──────────────────────────────────────────────────────────────
with col_charts:

    # ── Scope & Status bar chart ─────────────────────────────────────────────
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
            scopes   = df_dash["PROCESS_TYPE"].unique()
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
                vals = [
                    int(df_dash.loc[(df_dash["PROCESS_TYPE"] == s) & (df_dash["RUN_STATUS"] == status), "CNT"].sum())
                    for s in scopes
                ]
                if any(v > 0 for v in vals):
                    fig.add_trace(go.Bar(
                        x=[str(s) for s in scopes], y=vals,
                        name=status, marker_color=color_map.get(status, P["grey_400"]),
                        marker_line_width=0,
                    ))
            fig.update_layout(
                barmode="stack", plot_bgcolor="white", paper_bgcolor="white",
                margin=dict(l=0, r=0, t=30, b=0), height=240,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left",
                            x=0, font_size=11, bgcolor="rgba(0,0,0,0)"),
                xaxis=dict(type="category", showgrid=False, tickfont_size=12,
                           tickfont_color=P["grey_700"]),
                yaxis=dict(showgrid=True, gridcolor="#F0F0F0", tickfont_size=10,
                           tickformat=",.0f", zeroline=False),
                font_family="Inter",
                hoverlabel=dict(bgcolor="white", font_size=12),
            )
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
        else:
            st.info("No adjustment data yet.")
    except Exception as e:
        st.info(f"No data available: {e}")

    # ── COB Trend Charts ─────────────────────────────────────────────────────
    section_title("Last 5 COBs — Activity Trend", "📈")
    try:
        df_cob = run_query_df("""
            SELECT
                COBID, PROCESS_TYPE,
                COUNT(*)                        AS ADJ_COUNT,
                COALESCE(SUM(RECORD_COUNT), 0)  AS ROW_COUNT
            FROM ADJUSTMENT_APP.ADJ_HEADER
            WHERE IS_DELETED = FALSE
              AND COBID IN (
                  SELECT DISTINCT COBID FROM ADJUSTMENT_APP.ADJ_HEADER
                  WHERE IS_DELETED = FALSE ORDER BY COBID DESC LIMIT 5
              )
            GROUP BY COBID, PROCESS_TYPE
            ORDER BY COBID, PROCESS_TYPE
        """)
        if not df_cob.empty:
            cobs        = sorted(df_cob["COBID"].unique())
            cob_labels  = [str(c) for c in cobs]
            scopes      = df_cob["PROCESS_TYPE"].unique()
            scope_colors = [SCOPE_CONFIG.get(s, {}).get("color", P["grey_400"]) for s in scopes]

            ch1, ch2 = st.columns(2)

            with ch1:
                st.markdown(
                    f"<div style='font-size:0.8rem;font-weight:600;color:{P['grey_700']};"
                    f"margin-bottom:0.2rem'>Adjustments Submitted per COB</div>",
                    unsafe_allow_html=True)
                fig1 = go.Figure()
                for scope, color in zip(scopes, scope_colors):
                    vals = [
                        int(df_cob.loc[(df_cob["COBID"] == c) & (df_cob["PROCESS_TYPE"] == scope),
                                       "ADJ_COUNT"].sum())
                        if ((df_cob["COBID"] == c) & (df_cob["PROCESS_TYPE"] == scope)).any() else 0
                        for c in cobs
                    ]
                    fig1.add_trace(go.Bar(x=cob_labels, y=vals, name=scope,
                                          marker_color=color, marker_line_width=0))
                fig1.update_layout(
                    barmode="stack", plot_bgcolor="white", paper_bgcolor="white",
                    margin=dict(l=0, r=0, t=8, b=0), height=210,
                    showlegend=False,
                    xaxis=dict(type="category", showgrid=False, tickfont_size=11,
                               tickfont_color=P["grey_700"]),
                    yaxis=dict(showgrid=True, gridcolor="#F5F5F5", tickfont_size=10,
                               tickformat="d", zeroline=False),
                    font_family="Inter",
                    hoverlabel=dict(bgcolor="white", font_size=12),
                )
                st.plotly_chart(fig1, use_container_width=True, config={"displayModeBar": False})

            with ch2:
                st.markdown(
                    f"<div style='font-size:0.8rem;font-weight:600;color:{P['grey_700']};"
                    f"margin-bottom:0.2rem'>Rows Adjusted per COB</div>",
                    unsafe_allow_html=True)
                fig2 = go.Figure()
                for scope, color in zip(scopes, scope_colors):
                    vals = [
                        int(df_cob.loc[(df_cob["COBID"] == c) & (df_cob["PROCESS_TYPE"] == scope),
                                       "ROW_COUNT"].sum())
                        if ((df_cob["COBID"] == c) & (df_cob["PROCESS_TYPE"] == scope)).any() else 0
                        for c in cobs
                    ]
                    fig2.add_trace(go.Scatter(
                        x=cob_labels, y=vals, name=scope,
                        mode="lines+markers",
                        line=dict(color=color, width=2.5),
                        marker=dict(color=color, size=7, line=dict(color="white", width=1.5)),
                    ))
                fig2.update_layout(
                    plot_bgcolor="white", paper_bgcolor="white",
                    margin=dict(l=0, r=0, t=8, b=0), height=210,
                    showlegend=False,
                    xaxis=dict(type="category", showgrid=False, tickfont_size=11,
                               tickfont_color=P["grey_700"]),
                    yaxis=dict(showgrid=True, gridcolor="#F5F5F5", tickfont_size=10,
                               tickformat=",.0f", zeroline=False),
                    font_family="Inter",
                    hoverlabel=dict(bgcolor="white", font_size=12),
                )
                st.plotly_chart(fig2, use_container_width=True, config={"displayModeBar": False})
        else:
            st.info("No COB data available yet.")
    except Exception as e:
        st.info(f"COB trend data not available: {e}")


# ── RIGHT: overlap + error panels ─────────────────────────────────────────────
with col_alerts:

    # ── Overlap Alerts ───────────────────────────────────────────────────────
    section_title("Overlap Alerts", "⚠️")
    try:
        df_overlaps = run_query_df("""
            SELECT ADJ_ID_A, ADJ_ID_B, PROCESS_TYPE, ENTITY_A, ENTITY_B,
                   BOOK_A, BOOK_B, COBID, ALERT_MESSAGE
            FROM ADJUSTMENT_APP.DT_OVERLAP_ALERTS
            ORDER BY COBID DESC
            LIMIT 100
        """)
    except Exception:
        df_overlaps = __import__("pandas").DataFrame()

    if df_overlaps.empty:
        st.markdown(
            f'<div style="background:#F1F8F1;border:1px solid #C8E6C9;border-radius:10px;'
            f'padding:1.5rem;text-align:center;margin-bottom:0.8rem">'
            f'<div style="font-size:1.6rem">✅</div>'
            f'<div style="font-size:0.85rem;font-weight:600;color:#2E7D32;margin-top:6px">'
            f'No overlap alerts</div>'
            f'<div style="font-size:0.73rem;color:{P["grey_700"]};margin-top:4px">'
            f'All adjustments operate on distinct data sets</div>'
            f'</div>',
            unsafe_allow_html=True)
    else:
        count = len(df_overlaps)
        rows_html = ""
        for _, r in df_overlaps.iterrows():
            msg = str(r.get("ALERT_MESSAGE", "") or "").strip()[:55]
            rows_html += (
                f'<tr style="border-bottom:1px solid #FFF8E1">'
                f'<td style="padding:7px 8px;font-size:0.75rem;font-weight:700;'
                f'color:{P["warning"]};white-space:nowrap">'
                f'#{str(r["ADJ_ID_A"])[:8]}…<br/>↔ #{str(r["ADJ_ID_B"])[:8]}…</td>'
                f'<td style="padding:7px 6px;font-size:0.73rem;color:{P["grey_700"]}">'
                f'{r.get("PROCESS_TYPE","")}</td>'
                f'<td style="padding:7px 6px;font-size:0.73rem;color:{P["grey_700"]}">'
                f'{r.get("COBID","")}</td>'
                f'<td style="padding:7px 6px;font-size:0.72rem;color:{P["grey_700"]};'
                f'max-width:130px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">'
                f'{msg or "Overlapping filters"}</td>'
                f'</tr>'
            )
        st.markdown(
            f'<div style="background:white;border:1px solid {P["border"]};border-radius:10px;'
            f'overflow:hidden;margin-bottom:0.8rem">'
            f'<div style="background:#FFF8E1;border-bottom:2px solid #FFD54F;padding:0.55rem 0.8rem;'
            f'display:flex;justify-content:space-between;align-items:center">'
            f'<span style="font-size:0.78rem;font-weight:700;color:#E65100">⚠️ Overlapping adjustments</span>'
            f'<span style="background:#E65100;color:white;border-radius:99px;'
            f'padding:1px 9px;font-size:0.7rem;font-weight:700">{count}</span>'
            f'</div>'
            f'<div style="max-height:220px;overflow-y:auto">'
            f'<table style="width:100%;border-collapse:collapse">'
            f'<thead><tr style="background:#FAFAFA;position:sticky;top:0">'
            f'<th style="padding:6px 8px;font-size:0.65rem;font-weight:700;text-transform:uppercase;'
            f'letter-spacing:.06em;color:{P["grey_700"]};text-align:left;white-space:nowrap">ADJ IDs</th>'
            f'<th style="padding:6px 6px;font-size:0.65rem;font-weight:700;text-transform:uppercase;'
            f'letter-spacing:.06em;color:{P["grey_700"]};text-align:left">Scope</th>'
            f'<th style="padding:6px 6px;font-size:0.65rem;font-weight:700;text-transform:uppercase;'
            f'letter-spacing:.06em;color:{P["grey_700"]};text-align:left">COB</th>'
            f'<th style="padding:6px 6px;font-size:0.65rem;font-weight:700;text-transform:uppercase;'
            f'letter-spacing:.06em;color:{P["grey_700"]};text-align:left">Message</th>'
            f'</tr></thead>'
            f'<tbody>{rows_html}</tbody>'
            f'</table></div></div>',
            unsafe_allow_html=True)

    # ── Errors ───────────────────────────────────────────────────────────────
    section_title("Current Errors", "❌")
    try:
        df_errors = run_query_df("""
            SELECT ADJ_ID, PROCESS_TYPE, ENTITY_CODE, ERRORMESSAGE, USERNAME, ERROR_TIME
            FROM ADJUSTMENT_APP.VW_ERRORS
            ORDER BY ERROR_TIME DESC
            LIMIT 100
        """)
    except Exception:
        df_errors = __import__("pandas").DataFrame()

    if df_errors.empty:
        st.markdown(
            f'<div style="background:#F1F8F1;border:1px solid #C8E6C9;border-radius:10px;'
            f'padding:1.5rem;text-align:center">'
            f'<div style="font-size:1.6rem">✅</div>'
            f'<div style="font-size:0.85rem;font-weight:600;color:#2E7D32;margin-top:6px">'
            f'No errors</div>'
            f'<div style="font-size:0.73rem;color:{P["grey_700"]};margin-top:4px">'
            f'All adjustments processed successfully</div>'
            f'</div>',
            unsafe_allow_html=True)
    else:
        count = len(df_errors)
        rows_html = ""
        for _, r in df_errors.iterrows():
            msg = str(r.get("ERRORMESSAGE", "") or "").strip()[:70]
            adj_id = str(r.get("ADJ_ID", ""))
            rows_html += (
                f'<tr style="border-bottom:1px solid #FFEBEE">'
                f'<td style="padding:7px 8px;font-size:0.75rem;font-weight:700;'
                f'color:{P["danger"]};white-space:nowrap">#{adj_id[:8]}…</td>'
                f'<td style="padding:7px 6px;font-size:0.73rem;color:{P["grey_700"]}">'
                f'{r.get("PROCESS_TYPE","")}</td>'
                f'<td style="padding:7px 6px;font-size:0.72rem;color:{P["grey_700"]};'
                f'max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">'
                f'{msg or "Unknown error"}</td>'
                f'</tr>'
            )
        st.markdown(
            f'<div style="background:white;border:1px solid {P["border"]};border-radius:10px;'
            f'overflow:hidden">'
            f'<div style="background:#FFEBEE;border-bottom:2px solid #FFCDD2;padding:0.55rem 0.8rem;'
            f'display:flex;justify-content:space-between;align-items:center">'
            f'<span style="font-size:0.78rem;font-weight:700;color:{P["danger"]}">❌ Failed adjustments</span>'
            f'<span style="background:{P["danger"]};color:white;border-radius:99px;'
            f'padding:1px 9px;font-size:0.7rem;font-weight:700">{count}</span>'
            f'</div>'
            f'<div style="max-height:220px;overflow-y:auto">'
            f'<table style="width:100%;border-collapse:collapse">'
            f'<thead><tr style="background:#FAFAFA">'
            f'<th style="padding:6px 8px;font-size:0.65rem;font-weight:700;text-transform:uppercase;'
            f'letter-spacing:.06em;color:{P["grey_700"]};text-align:left;white-space:nowrap">ADJ ID</th>'
            f'<th style="padding:6px 6px;font-size:0.65rem;font-weight:700;text-transform:uppercase;'
            f'letter-spacing:.06em;color:{P["grey_700"]};text-align:left">Scope</th>'
            f'<th style="padding:6px 6px;font-size:0.65rem;font-weight:700;text-transform:uppercase;'
            f'letter-spacing:.06em;color:{P["grey_700"]};text-align:left">Error</th>'
            f'</tr></thead>'
            f'<tbody>{rows_html}</tbody>'
            f'</table></div></div>',
            unsafe_allow_html=True)

    # ── Top Submitters ───────────────────────────────────────────────────────
    st.markdown("<br/>", unsafe_allow_html=True)
    section_title("Top Submitters", "👤")
    try:
        df_users = run_query_df("""
            SELECT USERNAME, COUNT(*) AS CNT
            FROM ADJUSTMENT_APP.ADJ_HEADER
            WHERE IS_DELETED = FALSE
            GROUP BY USERNAME ORDER BY CNT DESC LIMIT 5
        """)
        if not df_users.empty:
            max_cnt = int(df_users["CNT"].max()) or 1
            rows_html = ""
            for _, row in df_users.iterrows():
                pct = int(row["CNT"]) / max_cnt * 100
                rows_html += (
                    f'<div style="margin-bottom:0.5rem">'
                    f'<div style="display:flex;justify-content:space-between;'
                    f'font-size:0.8rem;margin-bottom:3px">'
                    f'<span style="color:{P["grey_900"]};font-weight:500">{row["USERNAME"]}</span>'
                    f'<span style="font-weight:700;color:{P["accent"]}">{int(row["CNT"])}</span></div>'
                    f'<div style="background:{P["grey_100"]};border-radius:99px;height:4px">'
                    f'<div style="background:{P["primary"]};border-radius:99px;height:4px;'
                    f'width:{pct:.0f}%"></div></div></div>'
                )
            st.markdown(
                f'<div style="background:white;border:1px solid {P["border"]};'
                f'border-radius:10px;padding:0.9rem 1rem">{rows_html}</div>',
                unsafe_allow_html=True)
    except Exception:
        pass


# ──────────────────────────────────────────────────────────────────────────────
# RECENT ACTIVITY — full-width
# ──────────────────────────────────────────────────────────────────────────────

st.markdown("<br/>", unsafe_allow_html=True)
section_title("Recent Activity", "🕐")


def _fmt_duration(seconds):
    """Format a number of seconds as a human-readable string."""
    try:
        s = int(seconds)
    except (TypeError, ValueError):
        return "—"
    if s < 0:
        return "—"
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m {s % 60}s"
    return f"{s // 3600}h {(s % 3600) // 60}m"


try:
    # Query ADJ_HEADER directly — avoids VW_RECENT_ACTIVITY's cross-table JOIN
    # which can fail if ADJ_STATUS_HISTORY.ADJ_ID type differs from ADJ_HEADER.ADJ_ID.
    df_activity = run_query_df("""
        SELECT
            ADJ_ID                                                      AS "Adj ID",
            COBID                                                       AS "COB",
            PROCESS_TYPE                                                AS "Scope",
            ADJUSTMENT_TYPE                                             AS "Type",
            RUN_STATUS                                                  AS "Status",
            ENTITY_CODE                                                 AS "Entity",
            USERNAME                                                    AS "User",
            RECORD_COUNT                                                AS "Records",
            CREATED_DATE                                                AS "Submitted",
            PROCESS_DATE                                                AS "Processed",
            DATEDIFF('second', CREATED_DATE, PROCESS_DATE)              AS DURATION_SECONDS
        FROM ADJUSTMENT_APP.ADJ_HEADER
        WHERE IS_DELETED = FALSE
        ORDER BY CREATED_DATE DESC
        LIMIT 50
    """)

    if not df_activity.empty:
        df_activity["Duration"] = df_activity["DURATION_SECONDS"].apply(
            lambda v: _fmt_duration(v) if pd.notna(v) else "—"
        )
        df_activity = df_activity.drop(columns=["DURATION_SECONDS"])

        STATUS_STYLE = {
            "Processed":        f"color:{P['success']};font-weight:600",
            "Failed":           f"color:{P['danger']};font-weight:600",
            "Running":          f"color:{P['info']};font-weight:600",
            "Pending":          f"color:{P['warning']};font-weight:600",
            "Approved":         "color:#00897B;font-weight:600",
            "Pending Approval": f"color:{P['info']};font-weight:600",
        }
        st.dataframe(
            df_activity.style.map(lambda v: STATUS_STYLE.get(v, ""), subset=["Status"]),
            use_container_width=True,
            height=380,
            hide_index=True,
        )
    else:
        st.info("No adjustments yet.")
except Exception as e:
    st.warning(f"Could not load recent activity: {e}")
