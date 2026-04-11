"""
Command Center — Home Dashboard
================================
Live overview of all adjustments, queue, and pending actions.
"""
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from datetime import date, timedelta, datetime
from collections import Counter

st.set_page_config(
    page_title="Adjustment Engine · MUFG",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

from _OLD.prototype_v2.data.state_manager import init_state, dashboard_kpis, get_all_adjustments, get_pending_approvals, get_queue_stats, tick_queue, current_user
from _OLD.prototype_v2.data.styles import inject_css, render_sidebar, status_badge, kpi_card, section_title, P

init_state()
inject_css()
render_sidebar()

# ──────────────────────────────────────────────────────────────────────────────
kpis = dashboard_kpis()
q    = get_queue_stats()
adjs = get_all_adjustments()
user = current_user()

st.markdown(f"## 🏠 Command Center")
st.markdown(f"<span style='color:{P['grey_700']};font-size:0.9rem'>Good morning, {user['name'].split()[0]}. Here's your adjustment landscape for today.</span>", unsafe_allow_html=True)

st.markdown("<br/>", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# KPI ROW
# ──────────────────────────────────────────────────────────────────────────────

cols = st.columns(7)
kpi_data = [
    ("Total",           kpis["total"],            "All adjustments",     "primary"),
    ("Pending Approval",kpis["pending_approval"],  "Awaiting review",     "warning"),
    ("Approved",        kpis["approved"],          "Ready to apply",      "info"),
    ("Applied",         kpis["applied"],           "In the data",         "success"),
    ("Rejected",        kpis["rejected"],          "Declined",            "primary"),
    ("Queue Active",    q["pending"]+q["running"], "Processing now",      "info"),
    ("Recurring",       sum(1 for a in adjs if a.get("frequency")=="RECURRING"),
                                                   "Auto-scheduled",      "purple"),
]
for col, (lbl, val, sub, variant) in zip(cols, kpi_data):
    col.markdown(kpi_card(lbl, val, sub, variant), unsafe_allow_html=True)

st.markdown("<br/>", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# MAIN CONTENT: Activity + Queue + Pending Approvals
# ──────────────────────────────────────────────────────────────────────────────

col_left, col_mid, col_right = st.columns([2, 1.4, 1.4])

# ── Activity chart ──────────────────────────────────────────────────────────
with col_left:
    section_title("Adjustment Activity (Last 14 Days)", "📅")

    # Build time series
    date_range = [date.today() - timedelta(days=i) for i in range(13, -1, -1)]
    status_groups = ["APPLIED", "PENDING_APPROVAL", "DRAFT", "REJECTED"]
    color_map = {
        "APPLIED":          P["success"],
        "PENDING_APPROVAL": P["warning"],
        "DRAFT":            P["grey_400"],
        "REJECTED":         P["danger"],
    }

    chart_data = {s: [0] * 14 for s in status_groups}
    for adj in adjs:
        created = adj.get("created_at")
        if not created:
            continue
        cdate = created.date() if hasattr(created, "date") else created
        if cdate in date_range:
            idx = date_range.index(cdate)
            adj_status = adj["adj_status"]
            if adj_status in chart_data:
                chart_data[adj_status][idx] += 1

    fig = go.Figure()
    for status, counts in chart_data.items():
        if any(c > 0 for c in counts):
            fig.add_trace(go.Bar(
                x=[d.strftime("%d %b") for d in date_range],
                y=counts,
                name=status.replace("_", " ").title(),
                marker_color=color_map[status],
                hovertemplate="%{y} adjustments<extra></extra>",
            ))

    fig.update_layout(
        barmode="stack",
        plot_bgcolor="white",
        paper_bgcolor="white",
        margin=dict(l=0, r=0, t=10, b=0),
        height=220,
        legend=dict(orientation="h", yanchor="bottom", y=1, xanchor="left", x=0,
                    font_size=11),
        xaxis=dict(showgrid=False, tickfont_size=10),
        yaxis=dict(showgrid=True, gridcolor="#F0F0F0", tickfont_size=10),
        font_family="Inter",
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    # Breakdown by scope + type
    section_title("By Source & Type", "📊")
    scope_counts = Counter(a["scope_key"] for a in adjs)
    type_counts  = Counter(a["adj_type"]  for a in adjs)

    c1, c2 = st.columns(2)
    with c1:
        if scope_counts:
            fig2 = go.Figure(go.Pie(
                labels=list(scope_counts.keys()),
                values=list(scope_counts.values()),
                hole=0.55,
                marker_colors=[P["primary"], P["info"], P["success"]],
                textfont_size=11,
            ))
            fig2.update_layout(
                showlegend=True, height=160,
                margin=dict(l=0, r=0, t=0, b=0),
                legend=dict(font_size=10, orientation="v"),
                paper_bgcolor="white", plot_bgcolor="white",
                font_family="Inter",
            )
            fig2.update_traces(textinfo="label+value")
            st.plotly_chart(fig2, use_container_width=True, config={"displayModeBar": False})
    with c2:
        if type_counts:
            fig3 = go.Figure(go.Pie(
                labels=list(type_counts.keys()),
                values=list(type_counts.values()),
                hole=0.55,
                marker_colors=[P["warning"], "#8BC34A", "#00BCD4", "#9C27B0"],
                textfont_size=11,
            ))
            fig3.update_layout(
                showlegend=True, height=160,
                margin=dict(l=0, r=0, t=0, b=0),
                legend=dict(font_size=10, orientation="v"),
                paper_bgcolor="white", plot_bgcolor="white",
                font_family="Inter",
            )
            fig3.update_traces(textinfo="label+value")
            st.plotly_chart(fig3, use_container_width=True, config={"displayModeBar": False})


# ── Processing queue widget ─────────────────────────────────────────────────
with col_mid:
    section_title("Processing Queue", "⏳")
    tick_queue()
    queue = st.session_state.get("queue", [])
    active_q = [qi for qi in queue if qi["status"] in ("PENDING", "RUNNING")]
    recent_done = [qi for qi in queue if qi["status"] == "COMPLETED"][-3:]

    if not active_q and not recent_done:
        st.markdown('<div class="mcard" style="text-align:center;color:#9E9E9E;font-size:0.85rem;padding:2rem">'
                    '✅ Queue is empty</div>', unsafe_allow_html=True)
    else:
        for qi in active_q:
            adj = st.session_state["adjustments"].get(qi["adj_id"], {})
            status_cls = qi["status"].lower()
            pct = qi["progress"]
            pct_color = P["info"] if qi["status"] == "RUNNING" else P["warning"]
            st.markdown(
                f'<div class="queue-item {status_cls}">'
                f'<div style="display:flex;justify-content:space-between;align-items:center">'
                f'<span style="font-weight:700;font-size:0.85rem">ADJ #{qi["adj_id"]}</span>'
                f'<span style="font-size:0.72rem;font-weight:600;color:{pct_color}">'
                f'{"⚡ RUNNING" if qi["status"]=="RUNNING" else "• PENDING"}</span>'
                f'</div>'
                f'<div style="font-size:0.75rem;color:{P["grey_700"]};margin:3px 0">'
                f'{adj.get("scope_key","?")} · {qi["estimated_rows"]} rows</div>'
                f'<div style="background:#F0F0F0;border-radius:4px;height:5px;margin-top:6px">'
                f'<div style="background:{pct_color};width:{pct}%;height:5px;border-radius:4px;'
                f'transition:width 0.5s"></div></div>'
                f'<div style="font-size:0.68rem;color:{P["grey_700"]};margin-top:3px">'
                f'{qi["processed_rows"]}/{qi["estimated_rows"]} rows · {pct:.0f}%</div>'
                f'</div>',
                unsafe_allow_html=True)

        if recent_done:
            st.markdown(f'<div style="font-size:0.72rem;color:{P["grey_700"]};margin:0.5rem 0 0.3rem;font-weight:600">RECENTLY COMPLETED</div>', unsafe_allow_html=True)
            for qi in reversed(recent_done):
                adj = st.session_state["adjustments"].get(qi["adj_id"], {})
                ts = qi["completed_at"].strftime("%H:%M") if qi.get("completed_at") else ""
                st.markdown(
                    f'<div class="queue-item completed">'
                    f'<span style="font-size:0.82rem;font-weight:700">ADJ #{qi["adj_id"]}</span>'
                    f'<span style="font-size:0.72rem;color:{P["success"]};float:right;font-weight:600">✔ {ts}</span>'
                    f'<div style="font-size:0.73rem;color:{P["grey_700"]}">{qi["processed_rows"]} rows</div>'
                    f'</div>',
                    unsafe_allow_html=True)

    # Queue stats summary
    st.markdown(
        f'<div style="display:flex;gap:8px;margin-top:0.5rem;">'
        + "".join([
            f'<div style="flex:1;text-align:center;background:{P["grey_100"]};border-radius:6px;padding:6px">'
            f'<div style="font-size:1.1rem;font-weight:700">{v}</div>'
            f'<div style="font-size:0.65rem;color:{P["grey_700"]};text-transform:uppercase;letter-spacing:.05em">{k}</div>'
            f'</div>'
            for k, v in [("Pending", q["pending"]), ("Running", q["running"]), ("Done", q["completed"])]
        ])
        + '</div>',
        unsafe_allow_html=True)


# ── Pending approvals ───────────────────────────────────────────────────────
with col_right:
    section_title("Needs Approval", "✅")
    pending = get_pending_approvals()
    if not pending:
        st.markdown('<div class="mcard" style="text-align:center;color:#9E9E9E;font-size:0.85rem;padding:2rem">'
                    '✅ Nothing pending</div>', unsafe_allow_html=True)
    else:
        for adj in pending[:5]:
            from _OLD.prototype_v2.data.mock_data import SCOPES
            scope = SCOPES.get(adj["scope_key"], {})
            created_ts = adj["created_at"].strftime("%d %b %H:%M") if adj.get("created_at") else ""
            recur_tag = '&nbsp;<span class="tag recurring">RECURRING</span>' if adj.get("frequency") == "RECURRING" else ""
            reason_truncated = adj["business_reason"][:60] + ("..." if len(adj["business_reason"]) > 60 else "")
            cob_str = adj["target_date"].strftime("%d %b %Y") if adj.get("target_date") else "?"
            st.markdown(
                f'<div class="adj-card">'
                f'<div class="adj-card-header">'
                f'<span class="adj-id">ADJ #{adj["adj_id"]}</span>'
                f'{status_badge(adj["adj_status"])}'
                f'</div>'
                f'<div style="font-size:0.82rem;font-weight:600;margin-bottom:3px">'
                f'{scope.get("icon","📊")} {scope.get("label","?")} · {adj["adj_type"]}'
                f'{recur_tag}</div>'
                f'<div class="adj-meta">by {adj["created_by"]} · {created_ts}</div>'
                f'<div class="adj-meta">{adj["affected_rows"]} rows · COB {cob_str}</div>'
                f'<div style="font-size:0.78rem;color:{P["grey_700"]};margin-top:4px;'
                f'font-style:italic;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">'
                f'"{reason_truncated}"</div>'
                f'</div>',
                unsafe_allow_html=True)

    if len(pending) > 5:
        st.caption(f"+ {len(pending)-5} more — see Approval Queue")

# ──────────────────────────────────────────────────────────────────────────────
# RECENT ACTIVITY FEED
# ──────────────────────────────────────────────────────────────────────────────

section_title("Recent Activity", "🕐")
history = st.session_state.get("status_history", [])
recent = sorted(history, key=lambda h: h["changed_at"], reverse=True)[:8]

if recent:
    from _OLD.prototype_v2.data.mock_data import SCOPES as SC
    cols_h = st.columns([1, 1.2, 1.5, 1.5, 3])
    cols_h[0].markdown('<span style="font-size:0.7rem;font-weight:700;color:#9E9E9E">ADJ</span>', unsafe_allow_html=True)
    cols_h[1].markdown('<span style="font-size:0.7rem;font-weight:700;color:#9E9E9E">STATUS</span>', unsafe_allow_html=True)
    cols_h[2].markdown('<span style="font-size:0.7rem;font-weight:700;color:#9E9E9E">BY</span>', unsafe_allow_html=True)
    cols_h[3].markdown('<span style="font-size:0.7rem;font-weight:700;color:#9E9E9E">WHEN</span>', unsafe_allow_html=True)
    cols_h[4].markdown('<span style="font-size:0.7rem;font-weight:700;color:#9E9E9E">COMMENT</span>', unsafe_allow_html=True)
    st.markdown('<hr style="margin:0.3rem 0;border-color:#F0F0F0">', unsafe_allow_html=True)

    for h in recent:
        adj = st.session_state["adjustments"].get(h["adj_id"], {})
        scope = SC.get(adj.get("scope_key", ""), {})
        ts = h["changed_at"].strftime("%d %b %H:%M") if hasattr(h["changed_at"], "strftime") else ""
        c0, c1, c2, c3, c4 = st.columns([1, 1.2, 1.5, 1.5, 3])
        c0.markdown(f'<span style="font-size:0.82rem;font-weight:700">#{h["adj_id"]}</span> '
                    f'<span style="font-size:0.72rem;color:#9E9E9E">{scope.get("icon","")}</span>',
                    unsafe_allow_html=True)
        c1.markdown(status_badge(h["new_status"]), unsafe_allow_html=True)
        c2.markdown(f'<span style="font-size:0.8rem">{h["changed_by"]}</span>', unsafe_allow_html=True)
        c3.markdown(f'<span style="font-size:0.78rem;color:{P["grey_700"]}">{ts}</span>', unsafe_allow_html=True)
        c4.markdown(f'<span style="font-size:0.78rem;font-style:italic;color:{P["grey_700"]}">'
                    f'"{h["comment"][:60]}{"..." if len(h.get("comment",""))>60 else ""}"</span>',
                    unsafe_allow_html=True)

# Auto-refresh every 15s if queue is active
if q["pending"] + q["running"] > 0:
    import time
    st.markdown('<div style="font-size:0.7rem;color:#9E9E9E;text-align:right">🔄 Auto-refreshes while queue is active</div>',
                unsafe_allow_html=True)
