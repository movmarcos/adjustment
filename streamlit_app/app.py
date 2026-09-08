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

from utils.styles import (inject_css, render_sidebar, section_title, P, SCOPE_CONFIG,
                          STATUS_COLORS, fmt_adj_id, icon, render_activity_grid,
                          render_df_table, set_flash, render_flash)
from utils.snowflake_conn import run_query_df, run_query, current_user_name, safe_rerun

inject_css()
render_sidebar()

user = current_user_name()

# ──────────────────────────────────────────────────────────────────────────────
# Downstream report hand-off — two paths, mirroring the engine:
#   • VaR / Stress   → PowerBI refresh action (METADATA.POWERBI_ACTION)
#   • Sens / FRTB    → dbt rebuild trigger row (RAVEN.LOG_STAGE_ME_STATUS
#                      DUMMY_* dataset) that a Control-M job polls to start dbt
# ──────────────────────────────────────────────────────────────────────────────

try:
    df_pbi_kpi = run_query_df("""
        SELECT
            COALESCE(SUM(CASE WHEN START_TIME IS NULL THEN 1 ELSE 0 END), 0)     AS PBI_QUEUED,
            COALESCE(SUM(CASE WHEN START_TIME IS NOT NULL THEN 1 ELSE 0 END), 0) AS PBI_RUNNING
        FROM METADATA.POWERBI_ACTION
        WHERE INSERT_SOURCE IN ('LOAD_VAR_ADJUSTMENT','LOAD_STRESS_ADJUSTMENT')
          AND COMPLETE_TIME IS NULL
    """)
    pbi_pending = int(df_pbi_kpi.iloc[0]["PBI_QUEUED"]) + int(df_pbi_kpi.iloc[0]["PBI_RUNNING"]) if not df_pbi_kpi.empty else 0
except Exception as e:
    pbi_pending = None          # rendered as "n/a" — never a misleading 0
    st.warning(f"Could not load Power BI hand-off status: {e}")

try:
    df_dbt_kpi = run_query_df("""
        SELECT COALESCE(COUNT(*), 0) AS DBT_TRIGGERS
        FROM RAVEN.LOG_STAGE_ME_STATUS
        WHERE DATASET_NAME IN ('DUMMY_Sensitivity_Adjustment',
                               'DUMMY_FRTB_Adjustment')
          AND START_TIMESTAMP >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
    """)
    dbt_triggers = int(df_dbt_kpi.iloc[0]["DBT_TRIGGERS"]) if not df_dbt_kpi.empty else 0
except Exception as e:
    dbt_triggers = None         # rendered as "n/a" — never a misleading 0
    st.warning(f"Could not load dbt rebuild-trigger status: {e}")

# ──────────────────────────────────────────────────────────────────────────────
# KPIs — loaded ONCE, scoped to the selected COB range (below, after the range
# picker). They drive the banner health, the KPI strip and the cards.
# ──────────────────────────────────────────────────────────────────────────────


def _health(k):
    """System Status from the KPI dict of the SELECTED COB range. Failed
    adjustments whose error was acknowledged (Current Errors panel) do not
    count — the status goes back to HEALTHY while the failure stays listed."""
    failed  = int(k.get("FAILED", 0)) - int(k.get("ACKED", 0))
    running = int(k.get("RUNNING", 0))
    pending = int(k.get("PENDING", 0)) + int(k.get("APPROVED", 0))
    if failed > 0:
        return "#F87171", "CRITICAL"
    if running > 0:
        return "#60A5FA", "PROCESSING"
    if pending > 0:
        return "#FBBF24", "QUEUED"
    return "#4ADE80", "HEALTHY"

# ──────────────────────────────────────────────────────────────────────────────
# HEADER BANNER
# ──────────────────────────────────────────────────────────────────────────────

from utils.styles import get_user_tz, USER_TZ_OPTIONS
_tz = get_user_tz()
_tz_label = next((l for l, z in USER_TZ_OPTIONS.items() if z == _tz), "London")
london_now = datetime.now(pytz.timezone(_tz)).strftime("%d %b %Y  %H:%M")

_banner = st.empty()   # filled once the COB range (below) is known
render_flash("home")   # one-shot message from the previous action (ack / re-open)


def _render_banner(health_color, health_label, health_title=""):
    health_dot = (f'<span style="display:inline-block;width:8px;height:8px;'
                  f'border-radius:50%;background:{health_color};'
                  f'box-shadow:0 0 6px {health_color}AA;vertical-align:1px"></span>')
    _banner.markdown(f"""
<div style="background:linear-gradient(135deg,{P['accent']} 0%,#2A2A48 100%);
  border-radius:14px;padding:1.4rem 2rem;margin-bottom:1.2rem;
  display:flex;justify-content:space-between;align-items:center;
  box-shadow:0 4px 24px rgba(0,0,0,.18)">
  <div>
    <div style="font-size:1.5rem;font-weight:800;color:white;letter-spacing:.01em;line-height:1;
      display:flex;align-items:center;gap:10px">
      {icon("bar-chart", size=22, color="white", valign="0")} Adjustment Engine
    </div>
    <div style="font-size:0.87rem;color:rgba(255,255,255,.55);margin-top:6px">
      Command Center &nbsp;·&nbsp; Welcome, <strong style="color:white">{user}</strong>
    </div>
  </div>
  <div style="display:flex;align-items:center;gap:2rem">
    <div style="text-align:center">
      <div style="font-size:0.72rem;color:rgba(255,255,255,.65);text-transform:uppercase;
        letter-spacing:.1em;margin-bottom:3px">System Status</div>
      <div title="{health_title}" style="background:rgba(255,255,255,.1);border:1px solid {health_color}44;
        border-radius:99px;padding:4px 14px;font-size:0.78rem;font-weight:700;
        color:{health_color};backdrop-filter:blur(4px)">
        {health_dot} {health_label}
      </div>
    </div>
    <div style="text-align:right">
      <div style="font-size:0.72rem;color:rgba(255,255,255,.65);text-transform:uppercase;
        letter-spacing:.1em;margin-bottom:3px">As of</div>
      <div style="font-size:1.05rem;font-weight:700;color:white;letter-spacing:.03em">
        {london_now}
      </div>
    </div>
  </div>
</div>
""", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# COB SCOPE — every panel on this page respects this selection
# ──────────────────────────────────────────────────────────────────────────────

try:
    _cob_rows = run_query_df("""
        SELECT DISTINCT COBID FROM ADJUSTMENT_APP.ADJ_HEADER
        WHERE IS_DELETED = FALSE ORDER BY COBID DESC LIMIT 60
    """)
    _all_cobs = [int(c) for c in _cob_rows["COBID"]] if not _cob_rows.empty else []
except Exception:
    _all_cobs = []

_rc1, _rc2 = st.columns([1, 3.4])
with _rc1:
    cob_range = st.selectbox(
        "COB range", ["Last 5 COBs", "Latest COB", "Last 10 COBs",
                      "Last 30 COBs", "All COBs"],
        index=0, key="home_cob_range",
        help="Every number and panel on this page covers only this COB range.")
_take = {"Latest COB": 1, "Last 5 COBs": 5, "Last 10 COBs": 10,
         "Last 30 COBs": 30}.get(cob_range)
sel_cobs = _all_cobs if _take is None else _all_cobs[:_take]
if _take is None:
    cob_where = "1=1"
    _range_txt = (f"all COBs (latest: {_all_cobs[0]})" if _all_cobs
                  else "all COBs")
elif sel_cobs:
    cob_where = "COBID IN (" + ",".join(str(c) for c in sel_cobs) + ")"
    _range_txt = (f"COB {sel_cobs[0]}" if len(sel_cobs) == 1 else
                  f"COBs {min(sel_cobs)} – {max(sel_cobs)} "
                  f"({len(sel_cobs)} dates)")
else:
    cob_where = "1=1"
    _range_txt = "no COBs yet"
with _rc2:
    st.markdown(
        f'<div style="margin-top:1.85rem">'
        f'<span style="background:{P["info_lt"]};border:1px solid #BFDBFE;'
        f'border-radius:99px;padding:4px 14px;font-size:0.78rem;font-weight:600;'
        f'color:{P["grey_900"]}">'
        f'{icon("clock", size=12, color=P["info"])} Showing {_range_txt}'
        f'</span></div>',
        unsafe_allow_html=True)

# KPI totals scoped to the selection. System Status is derived from THIS load —
# a failure outside the selected COB range must not turn the page CRITICAL.
_kpi_sql = """
        SELECT
            COALESCE(SUM(TOTAL_ADJUSTMENTS), 0)       AS TOTAL,
            COALESCE(SUM(PENDING_COUNT), 0)            AS PENDING,
            COALESCE(SUM(PENDING_APPROVAL_COUNT), 0)   AS PENDING_APPROVAL,
            COALESCE(SUM(APPROVED_COUNT), 0)           AS APPROVED,
            COALESCE(SUM(RUNNING_COUNT), 0)            AS RUNNING,
            COALESCE(SUM(PROCESSED_COUNT), 0)          AS PROCESSED,
            COALESCE(SUM(FAILED_COUNT), 0)             AS FAILED,
            {acked}
            COALESCE(SUM(OVERLAP_ALERTS), 0)           AS OVERLAPS
        FROM ADJUSTMENT_APP.VW_DASHBOARD_KPI
        WHERE {cob_where}"""
try:
    try:
        df_kpi = run_query_df(_kpi_sql.format(
            acked="COALESCE(SUM(ACKNOWLEDGED_FAILED_COUNT), 0) AS ACKED,",
            cob_where=cob_where))
    except Exception:
        # View not yet redeployed with the acknowledgement column
        df_kpi = run_query_df(_kpi_sql.format(acked="0 AS ACKED,",
                                              cob_where=cob_where))
    kpis = df_kpi.iloc[0].to_dict() if not df_kpi.empty else {}
except Exception as e:
    kpis = {}
    st.warning(f"KPIs could not be scoped to the selected COB range: {e}")

_hc, _hl = _health(kpis)
_acked_n = int(kpis.get("ACKED", 0))
_render_banner(_hc, _hl,
               (f"{_acked_n} acknowledged failure(s) in {_range_txt} — "
                f"see Current Errors" if _acked_n else f"Status for {_range_txt}"))

# ──────────────────────────────────────────────────────────────────────────────
# KPI STRIP — each card LINKS to the Adjustments page with the matching
# status pre-filtered (?status=...; the page reads the query param). Plain
# <a> links because st.switch_page needs Streamlit 1.30 and SiS runs 1.26.
# ──────────────────────────────────────────────────────────────────────────────

queued = int(kpis.get("PENDING", 0)) + int(kpis.get("APPROVED", 0))

# NOTE: cards were briefly <a> links to deep-link the Adjustments page, but
# relative navigation renders a blank page inside the Snowsight iframe —
# removed until a reliable navigation mechanism exists (st.switch_page needs
# Streamlit 1.30; SiS runs 1.26). Page 2 still honours ?status=... params.
kpi_items = [
    ("Total",             int(kpis.get("TOTAL", 0)),           "All adjustments",      P["primary"], "list"),
    ("Awaiting Approval", int(kpis.get("PENDING_APPROVAL", 0)), "Need approval",        P["info"],    "clipboard"),
    ("Queued",            queued,                               "Pending + Approved",   P["warning"], "clock"),
    ("Running",           int(kpis.get("RUNNING", 0)),          "Processing now",       P["info"],    "zap"),
    ("Processed",         int(kpis.get("PROCESSED", 0)),        "In the data",          P["success"], "check-circle"),
    ("Power BI",          pbi_pending,                           "VaR/Stress refreshes pending", P["info"], "line-chart"),
    ("dbt Rebuild",       dbt_triggers,                          "Sens/FRTB triggers (24h)", P["purple"], "refresh-cw"),
    ("Overlaps",          int(kpis.get("OVERLAPS", 0)),         "Overlap alerts",       P["purple"],  "alert-triangle"),
]

# NOTE: cards_html must START with the grid <div> — markdown keeps everything
# inside one raw-HTML block only while the first line opens a block-level tag
# (a leading <style> terminated the block and turned the cards into an
# indented code block, showing raw HTML). The hover CSS lives in inject_css.
cards_html = ('<div style="display:grid;grid-template-columns:repeat(8,1fr);'
              'gap:10px;margin-bottom:0.5rem">')
for label, val, sub, color, icon_name in kpi_items:
    # val is None when the hand-off source could not be queried — show "n/a"
    # in grey rather than a reassuring 0 (the warning above says why).
    _na = val is None
    _n = 0 if _na else int(val)
    alert_style = f"box-shadow:0 0 0 2px {color}44;" if (label in ("Power BI", "dbt Rebuild", "Overlaps") and _n > 0) else ""
    val_color = P["grey_500"] if _na else (color if _n > 0 else P["grey_400"])
    val_shown = "n/a" if _na else str(_n)
    val_title = ' title="Status could not be loaded — see the warning above"' if _na else ""
    cards_html += f"""
    <div style="position:relative;background:white;border:1px solid {P['border']};
      border-radius:10px;padding:0.9rem 0.8rem 0.9rem 1rem;{alert_style}
      box-shadow:0 1px 2px rgba(15,23,42,.05);overflow:hidden"{val_title}>
      <div style="position:absolute;left:0;top:0;bottom:0;width:3px;background:{color}"></div>
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.3rem">
        <span style="font-size:0.72rem;font-weight:700;text-transform:uppercase;
          letter-spacing:.08em;color:{P['grey_700']}">{label}</span>
        {icon(icon_name, size=13, color=val_color, valign="0")}
      </div>
      <div style="font-size:1.75rem;font-weight:800;color:{val_color};
        line-height:1;font-variant-numeric:tabular-nums">{val_shown}</div>
      <div style="font-size:0.72rem;color:{P['grey_700']};margin-top:4px">{sub}</div>
    </div>"""
cards_html += '</div>'
# Plain-language explainer for the two report hand-off paths (non-technical
# users need to know WHERE their processed numbers go next).
cards_html += (
    f'<div style="font-size:0.72rem;color:{P["grey_700"]};margin-bottom:1.4rem">'
    f'{icon("info", size=12, color=P["grey_700"])} '
    f'<strong>Reports:</strong> VaR &amp; Stress adjustments queue a '
    f'<strong>Power BI refresh</strong> (picked up ~every 5 min). Sensitivity '
    f'&amp; FRTB write a <strong>rebuild trigger</strong> that Control-M '
    f'detects and runs the dbt job to rebuild the reporting model.</div>')
st.markdown(cards_html, unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# MAIN CONTENT: charts left | alerts right
# ──────────────────────────────────────────────────────────────────────────────

col_charts, col_alerts = st.columns([2.2, 1.8])

# ── LEFT: charts ──────────────────────────────────────────────────────────────
with col_charts:

    # ── Scope & Status bar chart ─────────────────────────────────────────────
    section_title("Adjustments by Scope & Status", "bar-chart")
    try:
        df_dash = run_query_df(f"""
            SELECT PROCESS_TYPE, RUN_STATUS,
                   SUM(ADJUSTMENT_COUNT) AS CNT
            FROM ADJUSTMENT_APP.DT_DASHBOARD
            WHERE {cob_where}
            GROUP BY PROCESS_TYPE, RUN_STATUS
            ORDER BY PROCESS_TYPE, RUN_STATUS
        """)
        if not df_dash.empty:
            scopes   = df_dash["PROCESS_TYPE"].unique()
            statuses = ["Pending", "Pending Approval", "Approved", "Running", "Processed", "Failed"]
            color_map = {
                "Pending":          P["warning"],
                "Pending Approval": P["info"],
                "Approved":         STATUS_COLORS["Approved"],
                "Running":          STATUS_COLORS["Running"],
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
                font_family="-apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif",
                hoverlabel=dict(bgcolor="white", font_size=12),
            )
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
        else:
            st.info("No adjustment data yet.")
    except Exception as e:
        st.info(f"No data available: {e}")

    # ── COB Trend Charts (selected range, capped at the 10 most recent) ─────
    _trend_src  = sel_cobs if sel_cobs else _all_cobs
    _trend_cobs = _trend_src[:10]
    if _trend_cobs and len(_trend_cobs) < len(_trend_src):
        _trend_title = (f"Activity Trend — latest {len(_trend_cobs)} of "
                        f"{len(_trend_src)} COBs")
    elif _trend_cobs:
        _trend_title = f"Activity Trend — {len(_trend_cobs)} COB(s)"
    else:
        _trend_title = "Activity Trend"
    section_title(_trend_title, "line-chart")
    try:
        _trend_where = ("COBID IN (" + ",".join(str(c) for c in _trend_cobs) + ")"
                        if _trend_cobs else "1=0")
        df_cob = run_query_df(f"""
            SELECT
                COBID, PROCESS_TYPE,
                COUNT(*)                        AS ADJ_COUNT,
                COALESCE(SUM(RECORD_COUNT), 0)  AS ROW_COUNT
            FROM ADJUSTMENT_APP.ADJ_HEADER
            WHERE IS_DELETED = FALSE
              AND {_trend_where}
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
                    font_family="-apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif",
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
                    font_family="-apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif",
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
    section_title("Overlap Alerts", "alert-triangle")
    # Business-facing ids: DT_OVERLAP_ALERTS only carries the internal ADJ_ID
    # hashes, so join ADJ_HEADER twice for the DIMENSION_ADJ_IDs (NULL until
    # processed — fmt_adj_id then falls back to the short hash). The COB filter
    # is applied inside the sub-select so `COBID` stays unambiguous.
    _overlaps_failed = False
    try:
        df_overlaps = run_query_df(f"""
            SELECT o.ADJ_ID_A, o.ADJ_ID_B,
                   ha.DIMENSION_ADJ_ID AS DIM_A, hb.DIMENSION_ADJ_ID AS DIM_B,
                   o.PROCESS_TYPE, o.ENTITY_A, o.ENTITY_B,
                   o.BOOK_A, o.BOOK_B, o.COBID, o.ALERT_MESSAGE
            FROM (SELECT ADJ_ID_A, ADJ_ID_B, PROCESS_TYPE, ENTITY_A, ENTITY_B,
                         BOOK_A, BOOK_B, COBID, ALERT_MESSAGE
                  FROM ADJUSTMENT_APP.DT_OVERLAP_ALERTS
                  WHERE {cob_where}) o
            LEFT JOIN ADJUSTMENT_APP.ADJ_HEADER ha ON ha.ADJ_ID = o.ADJ_ID_A
            LEFT JOIN ADJUSTMENT_APP.ADJ_HEADER hb ON hb.ADJ_ID = o.ADJ_ID_B
            ORDER BY o.COBID DESC
            LIMIT 100
        """)
    except Exception as e:
        _overlaps_failed = True
        df_overlaps = pd.DataFrame()
        st.warning(f"Could not load overlap alerts: {e}")

    if _overlaps_failed:
        pass                                    # warning shown above — no green card
    elif df_overlaps.empty:
        st.markdown(
            f'<div style="background:#F1F8F1;border:1px solid #C8E6C9;border-radius:10px;'
            f'padding:1.5rem;text-align:center;margin-bottom:0.8rem">'
            f'<div>{icon("check-circle", size=26, color=P["success"], valign="0")}</div>'
            f'<div style="font-size:0.85rem;font-weight:600;color:#2E7D32;margin-top:6px">'
            f'No overlap alerts</div>'
            f'<div style="font-size:0.73rem;color:{P["grey_700"]};margin-top:4px">'
            f'All adjustments operate on distinct data sets</div>'
            f'</div>',
            unsafe_allow_html=True)
    else:
        count = len(df_overlaps)
        _cell = lambda v: str(v if v is not None and not pd.isna(v) else "") or "—"
        df_ov = pd.DataFrame({
            "Adj ID A": [fmt_adj_id(r.get("DIM_A"), adj_id=r.get("ADJ_ID_A"))
                         for _, r in df_overlaps.iterrows()],
            "Adj ID B": [fmt_adj_id(r.get("DIM_B"), adj_id=r.get("ADJ_ID_B"))
                         for _, r in df_overlaps.iterrows()],
            "Scope":    [_cell(r.get("PROCESS_TYPE")) for _, r in df_overlaps.iterrows()],
            "COB":      [_cell(r.get("COBID")) for _, r in df_overlaps.iterrows()],
            "Entity / Book A": [f'{_cell(r.get("ENTITY_A"))} / {_cell(r.get("BOOK_A"))}'
                                for _, r in df_overlaps.iterrows()],
            "Entity / Book B": [f'{_cell(r.get("ENTITY_B"))} / {_cell(r.get("BOOK_B"))}'
                                for _, r in df_overlaps.iterrows()],
            "Alert":    [str(r.get("ALERT_MESSAGE", "") or "").strip()
                         for _, r in df_overlaps.iterrows()],
        })
        st.markdown(
            f'<div style="background:#FFF8E1;border:1px solid #FFD54F;border-radius:10px;'
            f'padding:0.55rem 0.8rem;margin-bottom:0.4rem;'
            f'display:flex;justify-content:space-between;align-items:center">'
            f'<span style="font-size:0.78rem;font-weight:700;color:#E65100">{icon("alert-triangle", size=13, color=P["warning"])} Overlapping adjustments</span>'
            f'<span style="background:#E65100;color:white;border-radius:99px;'
            f'padding:1px 9px;font-size:0.7rem;font-weight:700">{count}</span>'
            f'</div>',
            unsafe_allow_html=True)
        _warn_col = lambda v: P["warning"]
        render_df_table(df_ov, max_rows=100, height=220,
                        color_cols={"Adj ID A": _warn_col, "Adj ID B": _warn_col},
                        key="home_overlaps")

    # ── Errors ───────────────────────────────────────────────────────────────
    section_title("Current Errors", "x-circle")
    _errors_failed = False
    try:
        try:
            df_errors = run_query_df(f"""
                SELECT ADJ_ID, DIMENSION_ADJ_ID, PROCESS_TYPE, ENTITY_CODE, ERRORMESSAGE,
                       USERNAME, ERROR_TIME, IS_ACKNOWLEDGED, ERROR_ACK_BY, ERROR_ACK_AT,
                       ERROR_ACK_NOTE
                FROM ADJUSTMENT_APP.VW_ERRORS
                WHERE {cob_where}
                ORDER BY IS_ACKNOWLEDGED, ERROR_TIME DESC
                LIMIT 100
            """)
        except Exception:
            # View not yet redeployed with the acknowledgement columns
            df_errors = run_query_df(f"""
                SELECT ADJ_ID, DIMENSION_ADJ_ID, PROCESS_TYPE, ENTITY_CODE, ERRORMESSAGE,
                       USERNAME, ERROR_TIME, FALSE AS IS_ACKNOWLEDGED,
                       NULL AS ERROR_ACK_BY, NULL AS ERROR_ACK_AT, NULL AS ERROR_ACK_NOTE
                FROM ADJUSTMENT_APP.VW_ERRORS
                WHERE {cob_where}
                ORDER BY ERROR_TIME DESC
                LIMIT 100
            """)
    except Exception as e:
        _errors_failed = True
        df_errors = pd.DataFrame()
        st.warning(f"Could not load current errors: {e}")

    if _errors_failed:
        pass                                    # warning shown above — no green card
    elif df_errors.empty:
        st.markdown(
            f'<div style="background:#F1F8F1;border:1px solid #C8E6C9;border-radius:10px;'
            f'padding:1.5rem;text-align:center">'
            f'<div>{icon("check-circle", size=26, color=P["success"], valign="0")}</div>'
            f'<div style="font-size:0.85rem;font-weight:600;color:#2E7D32;margin-top:6px">'
            f'No errors</div>'
            f'<div style="font-size:0.73rem;color:{P["grey_700"]};margin-top:4px">'
            f'All adjustments processed successfully</div>'
            f'</div>',
            unsafe_allow_html=True)
    else:
        _ack_mask = df_errors["IS_ACKNOWLEDGED"].fillna(False).astype(bool)
        count = int((~_ack_mask).sum())          # OPEN failures drive the badge
        acked_count = int(_ack_mask.sum())
        _err_rows = []
        _adj_col = {}          # Adj ID label → text colour (red open / grey acknowledged)
        for _, r in df_errors.iterrows():
            full_msg = str(r.get("ERRORMESSAGE", "") or "").strip() or "Unknown error"
            adj_label = fmt_adj_id(r.get("DIMENSION_ADJ_ID"), adj_id=r.get("ADJ_ID"))
            _is_ack = bool(r.get("IS_ACKNOWLEDGED") or False)
            _adj_col[adj_label] = P["grey_400"] if _is_ack else P["danger"]
            _err_rows.append({
                "Adj ID":          adj_label,
                "Scope":           str(r.get("PROCESS_TYPE") or ""),
                "Entity":          str(r.get("ENTITY_CODE") or ""),
                "Error":           full_msg,
                "Ack":             "Yes" if _is_ack else "",
                "Acknowledged by": str(r.get("ERROR_ACK_BY") or "") if _is_ack else "",
            })
        df_err_grid = pd.DataFrame(_err_rows)
        st.markdown(
            f'<div style="background:#FFEBEE;border:1px solid #FFCDD2;border-radius:10px;'
            f'padding:0.55rem 0.8rem;margin-bottom:0.4rem;'
            f'display:flex;justify-content:space-between;align-items:center">'
            f'<span style="font-size:0.78rem;font-weight:700;color:{P["danger"]}">{icon("x-circle", size=13, color=P["danger"])} Failed adjustments</span>'
            f'<span style="background:{P["danger"]};color:white;border-radius:99px;'
            f'padding:1px 9px;font-size:0.7rem;font-weight:700">{count} open'
            + (f' · {acked_count} acknowledged' if acked_count else "") + '</span>'
            f'</div>',
            unsafe_allow_html=True)
        render_df_table(df_err_grid, max_rows=100, height=220,
                        highlight=lambda d: d.get("Ack") != "Yes",   # open failures tinted
                        color_cols={"Adj ID": lambda v: _adj_col.get(v, ""),
                                    "Ack": {"Yes": P["grey_700"]}},
                        key="home_errors")
        st.caption("Full messages and Retry: Logs › Errors, and the Adjustments page.")

        # ── Acknowledge / re-open ─────────────────────────────────────────
        # Acknowledging records who/when/why on the header and takes the
        # failure out of System Status (it stays listed, greyed, with ACK).
        # A retry that fails again re-arms it (ack older than the failure).
        _open_rows = df_errors[~_ack_mask]
        _ack_rows  = df_errors[_ack_mask]
        with st.expander("Acknowledge a failure (System Status back to HEALTHY)",
                         expanded=False):
            _me = current_user_name() or "UNKNOWN"
            _esc = lambda v: str(v).replace("\\", "\\\\").replace("'", "''")
            if not _open_rows.empty:
                _opts = {f"{fmt_adj_id(r.DIMENSION_ADJ_ID)} · {r.PROCESS_TYPE} · "
                         f"{str(r.ERRORMESSAGE or '')[:50]}": str(r.ADJ_ID)
                         for r in _open_rows.itertuples()}
                _pick = st.selectbox("Failed adjustment", list(_opts.keys()),
                                     key="home_ack_pick")
                _note = st.text_input("Note (why it is OK to acknowledge)",
                                      key="home_ack_note", max_chars=500,
                                      placeholder="e.g. resubmitted as ADJ-1234",
                                      help="A note is required for audit")
                _n = (_note or "").strip()
                if st.button("Acknowledge", key="home_ack_btn", type="primary",
                             disabled=not _n,
                             help="A note is required for audit"):
                    _label = _pick.split(" · ")[0]       # the fmt_adj_id part
                    try:
                        run_query(f"""
                            UPDATE ADJUSTMENT_APP.ADJ_HEADER
                            SET ERROR_ACK_BY = '{_esc(_me)}',
                                ERROR_ACK_AT = CONVERT_TIMEZONE('Europe/London',
                                                   CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9),
                                ERROR_ACK_NOTE = '{_esc(_n)}'
                            WHERE ADJ_ID = '{_esc(_opts[_pick])}' AND RUN_STATUS = 'Failed'
                        """)
                        set_flash("home", "success", f"Acknowledged {_label}")
                        safe_rerun()
                    except Exception as ex:
                        st.error(f"Could not acknowledge: {ex}")
            else:
                st.caption("No open failures in this COB range.")
            if not _ack_rows.empty:
                _ropts = {f"{fmt_adj_id(r.DIMENSION_ADJ_ID)} · {r.PROCESS_TYPE} · "
                          f"ack by {r.ERROR_ACK_BY or '?'}": str(r.ADJ_ID)
                          for r in _ack_rows.itertuples()}
                _rpick = st.selectbox("Acknowledged failure", list(_ropts.keys()),
                                      key="home_unack_pick")
                if st.button("Re-open (count it again)", key="home_unack_btn"):
                    _rlabel = _rpick.split(" · ")[0]
                    try:
                        run_query(f"""
                            UPDATE ADJUSTMENT_APP.ADJ_HEADER
                            SET ERROR_ACK_BY = NULL, ERROR_ACK_AT = NULL,
                                ERROR_ACK_NOTE = NULL
                            WHERE ADJ_ID = '{_esc(_ropts[_rpick])}'
                        """)
                        set_flash("home", "success",
                                  f"Re-opened {_rlabel} — it counts in System Status again")
                        safe_rerun()
                    except Exception as ex:
                        st.error(f"Could not re-open: {ex}")

    # ── Top Submitters ───────────────────────────────────────────────────────
    st.markdown("<br/>", unsafe_allow_html=True)
    section_title("Top Submitters", "user")
    try:
        df_users = run_query_df(f"""
            SELECT USERNAME, COUNT(*) AS CNT
            FROM ADJUSTMENT_APP.ADJ_HEADER
            WHERE IS_DELETED = FALSE AND {cob_where}
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
    except Exception as e:
        st.warning(f"Could not load top submitters: {e}")


# ──────────────────────────────────────────────────────────────────────────────
# RECENT ACTIVITY — full-width
# ──────────────────────────────────────────────────────────────────────────────

st.markdown("<br/>", unsafe_allow_html=True)
section_title("Recent Activity", "clock")


try:
    # Query ADJ_HEADER directly — avoids VW_RECENT_ACTIVITY's cross-table JOIN
    # which can fail if ADJ_STATUS_HISTORY.ADJ_ID type differs from ADJ_HEADER.ADJ_ID.
    df_activity = run_query_df(f"""
        SELECT
            DIMENSION_ADJ_ID, COBID, SOURCE_COBID, PROCESS_TYPE, ADJUSTMENT_TYPE,
            RUN_STATUS, IS_DELETED, ENTITY_CODE, DEPARTMENT_CODE, BOOK_CODE,
            MEASURE_TYPE_CODE, SIMULATION_NAME, VAR_COMPONENT_ID,
            VAR_COMPONENT_NAME, USERNAME,
            RECORD_COUNT, CREATED_DATE, START_DATE, PROCESS_DATE
        FROM ADJUSTMENT_APP.ADJ_HEADER
        WHERE {cob_where}
        ORDER BY CREATED_DATE DESC
        LIMIT 50
    """)
    render_activity_grid(df_activity, selectable=False,
                         empty_msg="No adjustments yet.")
except Exception as e:
    st.warning(f"Could not load recent activity: {e}")
