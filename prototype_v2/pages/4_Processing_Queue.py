"""
Processing Queue — Snowflake Async Pipeline
============================================
Shows the Snowflake Streams → Tasks → Dynamic Table pipeline
and the live status of each enqueued adjustment.
"""
import streamlit as st
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime, timedelta
import time

st.set_page_config(
    page_title="Processing Queue · MUFG",
    page_icon="⏳",
    layout="wide",
    initial_sidebar_state="expanded",
)

from data.state_manager import (
    init_state, tick_queue, get_queue_stats, current_user,
)
from data.mock_data import SCOPES
from data.styles import (
    inject_css, render_sidebar, render_pipeline_diagram,
    section_title, P, STATUS_COLORS,
)

init_state()
inject_css()
render_sidebar()

# Advance simulated queue progress on each render
tick_queue()

# ──────────────────────────────────────────────────────────────────────────────
st.markdown("## ⏳ Processing Queue")
st.markdown(
    f"<span style='color:{P['grey_700']};font-size:0.9rem'>"
    "Adjustments are applied asynchronously via Snowflake Streams &amp; Tasks. "
    "Monitor the pipeline here — no page refresh required while items are running."
    "</span>", unsafe_allow_html=True)
st.markdown("<br/>", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# SNOWFLAKE PIPELINE DIAGRAM
# ──────────────────────────────────────────────────────────────────────────────

section_title("Snowflake Processing Pipeline", "🔧")
q_stats = get_queue_stats()
queue   = st.session_state.get("queue", [])
active  = [qi for qi in queue if qi["status"] in ("PENDING","RUNNING")]

# Determine pipeline stage
if q_stats["running"] > 0:
    stage = 4   # SP executing
elif q_stats["pending"] > 0:
    stage = 2   # Stream captured, task about to wake
else:
    stage = 5   # All done / idle

render_pipeline_diagram(current_stage=stage)

st.markdown(
    f'<div style="display:grid;grid-template-columns:repeat(5,1fr);gap:10px;'
    f'font-size:0.78rem;color:{P["grey_700"]};text-align:center;margin-top:0.3rem">'
    f'<div>Adjustment saved to<br/><strong>ADJ_HEADER</strong> (Hybrid Table)</div>'
    f'<div>CDC captured by<br/><strong>ADJ_HEADER_STREAM</strong></div>'
    f'<div>Scheduled <strong>TASK_ADJ_PROCESSOR</strong><br/>wakes every 60s</div>'
    f'<div><strong>SP_CREATE_ADJUSTMENT</strong><br/>computes &amp; writes deltas</div>'
    f'<div><strong>FACT_ADJUSTED</strong><br/>Dynamic Table refreshes (1 min lag)</div>'
    f'</div>',
    unsafe_allow_html=True)

st.markdown("<br/>", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# LIVE QUEUE STATS
# ──────────────────────────────────────────────────────────────────────────────

c1, c2, c3, c4, c5 = st.columns(5)
stat_items = [
    ("Pending",   q_stats["pending"],   P["warning"], "⏸"),
    ("Running",   q_stats["running"],   P["info"],    "⚡"),
    ("Completed", q_stats["completed"], P["success"], "✔"),
    ("Failed",    q_stats["failed"],    P["danger"],  "✗"),
    ("Total",     q_stats["total"],     P["grey_700"],"#"),
]
for col, (label, val, color, icon) in zip([c1,c2,c3,c4,c5], stat_items):
    col.markdown(
        f'<div style="background:{P["white"]};border:1px solid {P["border"]};'
        f'border-top:3px solid {color};border-radius:8px;padding:0.8rem;text-align:center">'
        f'<div style="font-size:1.6rem;font-weight:800;color:{color}">{icon} {val}</div>'
        f'<div style="font-size:0.72rem;text-transform:uppercase;letter-spacing:.06em;'
        f'color:{P["grey_700"]};margin-top:3px">{label}</div>'
        f'</div>',
        unsafe_allow_html=True)

st.markdown("<br/>", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# ACTIVE ITEMS
# ──────────────────────────────────────────────────────────────────────────────

section_title("Active Items", "⚡")
active_items = [qi for qi in queue if qi["status"] in ("PENDING","RUNNING")]

if not active_items:
    st.markdown(
        f'<div class="mcard" style="text-align:center;padding:2rem;color:{P["grey_700"]}">'
        f'✅ No items currently in the queue.</div>',
        unsafe_allow_html=True)
else:
    for qi in active_items:
        adj  = st.session_state["adjustments"].get(qi["adj_id"], {})
        scope = SCOPES.get(adj.get("scope_key",""), {})
        pct  = qi["progress"]
        elapsed = (datetime.now() - qi["queued_at"]).total_seconds()
        status_color = P["info"] if qi["status"] == "RUNNING" else P["warning"]
        status_icon  = "⚡ RUNNING" if qi["status"] == "RUNNING" else "⏸ PENDING"

        # ETA estimate
        if qi["status"] == "RUNNING" and pct > 0:
            remaining_s = (elapsed - 4) * (100 - pct) / pct if pct > 0 else 30
            eta = f"~{int(remaining_s)}s remaining"
        elif qi["status"] == "PENDING":
            eta = f"Starts in ~{max(0, int(4 - elapsed))}s"
        else:
            eta = ""

        st.markdown(
            f'<div class="queue-item {qi["status"].lower()}">'
            f'<div style="display:flex;justify-content:space-between;align-items:flex-start">'
            f'  <div>'
            f'    <span style="font-weight:700;font-size:0.92rem">ADJ #{qi["adj_id"]}</span>'
            f'    &nbsp;<span style="font-size:0.75rem;color:{P["grey_700"]}">'
            f'    {scope.get("icon","")} {scope.get("label","?")} · {adj.get("adj_type","")} · '
            f'    {qi["estimated_rows"]} rows</span>'
            f'  </div>'
            f'  <span style="font-size:0.75rem;font-weight:700;color:{status_color}">'
            f'  {status_icon}</span>'
            f'</div>'
            f'<div style="display:flex;align-items:center;gap:8px;margin-top:8px">'
            f'  <div style="flex:1;background:#F0F0F0;border-radius:4px;height:6px">'
            f'    <div style="background:{status_color};width:{pct}%;height:6px;'
            f'         border-radius:4px;transition:width 0.5s"></div>'
            f'  </div>'
            f'  <span style="font-size:0.75rem;color:{P["grey_700"]};min-width:36px">{pct:.0f}%</span>'
            f'</div>'
            f'<div style="display:flex;justify-content:space-between;margin-top:4px">'
            f'  <span style="font-size:0.72rem;color:{P["grey_700"]}">'
            f'  Worker: <code>{qi["worker"]}</code></span>'
            f'  <span style="font-size:0.72rem;color:{P["grey_700"]}">{eta}</span>'
            f'</div>'
            f'<div style="font-size:0.72rem;color:{P["grey_700"]};margin-top:2px">'
            f'Queued at: {qi["queued_at"].strftime("%H:%M:%S")} · '
            f'{qi["processed_rows"]}/{qi["estimated_rows"]} rows processed</div>'
            f'</div>',
            unsafe_allow_html=True)

    st.markdown("<br/>", unsafe_allow_html=True)
    if st.button("🔄 Refresh Status", type="primary"):
        tick_queue()
        st.rerun()

# ──────────────────────────────────────────────────────────────────────────────
# QUEUE HISTORY
# ──────────────────────────────────────────────────────────────────────────────

st.markdown("<br/>", unsafe_allow_html=True)
section_title("Queue History", "📜")

completed = [qi for qi in queue if qi["status"] in ("COMPLETED","FAILED")]
completed_sorted = sorted(completed, key=lambda qi: qi.get("completed_at") or qi["queued_at"], reverse=True)

if not completed_sorted:
    st.info("No completed items in queue history.")
else:
    rows = []
    for qi in completed_sorted:
        adj   = st.session_state["adjustments"].get(qi["adj_id"], {})
        scope = SCOPES.get(adj.get("scope_key",""), {})
        dur   = ""
        if qi.get("started_at") and qi.get("completed_at"):
            s = (qi["completed_at"] - qi["started_at"]).total_seconds()
            dur = f"{s:.1f}s"
        rows.append({
            "Queue ID":    qi["queue_id"],
            "ADJ #":       qi["adj_id"],
            "Source":      f'{scope.get("icon","")} {scope.get("label","?")}',
            "Type":        adj.get("adj_type",""),
            "Rows":        qi["processed_rows"],
            "Status":      qi["status"],
            "Worker":      qi["worker"],
            "Queued at":   qi["queued_at"].strftime("%d %b %H:%M:%S") if qi.get("queued_at") else "",
            "Duration":    dur,
            "Error":       qi.get("error_message") or "",
        })
    df = pd.DataFrame(rows)

    def color_status(val):
        if val == "COMPLETED": return f"color:{P['success']};font-weight:600"
        if val == "FAILED":    return f"color:{P['danger']};font-weight:600"
        return ""

    st.dataframe(
        df.style.map(color_status, subset=["Status"]),
        use_container_width=True, hide_index=True, height=300,
    )

# ──────────────────────────────────────────────────────────────────────────────
# SNOWFLAKE TASK SCHEDULE INFO
# ──────────────────────────────────────────────────────────────────────────────

st.markdown("<br/>", unsafe_allow_html=True)
section_title("Snowflake Task Schedule (Reference)", "⚙️")

with st.expander("View Snowflake Tasks configuration", expanded=False):
    st.markdown("""
    | Task | Schedule | Trigger | Action |
    |------|----------|---------|--------|
    | `TASK_LOG_STATUS_CHANGES` | Every 1 min | When `ADJ_HEADER_STREAM` has data | Writes to `ADJ_STATUS_HISTORY` |
    | `TASK_AI_SUMMARIZE` | After `TASK_LOG_STATUS_CHANGES` | Child task | Calls `SP_GENERATE_AI_SUMMARY()` for new APPLIED/APPROVED rows |
    | `TASK_ADJ_PROCESSOR` | Every 1 min | When `ADJ_HEADER_STREAM` has APPROVED records | Calls `SP_CREATE_ADJUSTMENT()` to write deltas |
    | `TASK_ANOMALY_CHECK` | Daily 8 AM ET (CRON) | Time-based | Calls `AI.SP_DETECT_ADJUSTMENT_ANOMALIES()` |
    | `TASK_RECURRING_TRIGGER` | After COB file load | Event-based (external) | Finds and applies RECURRING templates in valid date window |

    **Dynamic Table Refresh:**
    - `MART.FACT_ADJUSTED` refreshes with **1-minute lag** — always near real-time
    - `MART.ADJUSTMENT_IMPACT_SUMMARY` refreshes **DOWNSTREAM** from FACT_ADJUSTED
    - `MART.DAILY_ADJUSTMENT_ACTIVITY` refreshes every **5 minutes**

    **Recurring Adjustment Flow:**
    An external process (post-COB scheduler) queries `ADJ_HEADER` for RECURRING adjustments
    where `CURRENT_DATE BETWEEN START_COB AND END_COB` and status is APPROVED.
    It calls `SP_CREATE_ADJUSTMENT()` with today's COB date for each matching template.
    This process is **not part of this UI** but the templates are stored and managed here.
    """)
