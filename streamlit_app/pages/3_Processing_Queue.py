"""
Processing Queue — Snowflake Async Pipeline
============================================
Shows the Snowflake Streams → Tasks → Dynamic Table pipeline
and the live status of each enqueued adjustment.
Reads from: VW_PROCESSING_QUEUE, ADJ_HEADER.
"""
import streamlit as st
import pandas as pd

st.set_page_config(page_title="Processing Queue · MUFG", page_icon="⏳", layout="wide", initial_sidebar_state="expanded")

from utils.styles import (
    inject_css, render_sidebar, render_pipeline_diagram,
    section_title, P, SCOPE_CONFIG, STATUS_COLORS,
)
from utils.snowflake_conn import run_query_df, safe_rerun

inject_css()
render_sidebar()

# ──────────────────────────────────────────────────────────────────────────────

st.markdown("## ⏳ Processing Queue")
st.markdown(
    f"<span style='color:{P['grey_700']};font-size:0.9rem'>"
    "Adjustments are applied asynchronously via Snowflake Streams &amp; Tasks. "
    "Monitor the pipeline here."
    "</span>", unsafe_allow_html=True)
st.markdown("<br/>", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# SNOWFLAKE PIPELINE DIAGRAM
# ──────────────────────────────────────────────────────────────────────────────

section_title("Snowflake Processing Pipeline", "🔧")

try:
    df_q = run_query_df("""
        SELECT * FROM ADJUSTMENT_APP.VW_PROCESSING_QUEUE
        ORDER BY QUEUE_POSITION
    """)
except Exception as e:
    df_q = pd.DataFrame()
    st.warning(f"Could not load queue: {e}")

pending_count  = int(df_q[df_q["RUN_STATUS"] == "Pending"].shape[0]) if not df_q.empty else 0
processing_count = int(df_q[df_q["RUN_STATUS"] == "Processing"].shape[0]) if not df_q.empty else 0

# Determine pipeline stage
if processing_count > 0:
    stage = 4   # SP executing
elif pending_count > 0:
    stage = 2   # Stream captured, task about to wake
else:
    stage = 5   # All done / idle

render_pipeline_diagram(current_stage=stage)

st.markdown(
    f'<div style="display:grid;grid-template-columns:repeat(5,1fr);gap:10px;'
    f'font-size:0.78rem;color:{P["grey_700"]};text-align:center;margin-top:0.3rem">'
    f'<div>Adjustment saved to<br/><strong>ADJ_HEADER</strong></div>'
    f'<div>CDC captured by<br/><strong>ADJ_HEADER_STREAM</strong></div>'
    f'<div>Scheduled <strong>PROCESS_PENDING_TASK</strong><br/>wakes every 60s</div>'
    f'<div><strong>SP_PROCESS_ADJUSTMENT</strong><br/>computes &amp; writes deltas</div>'
    f'<div><strong>Dynamic Tables</strong><br/>auto-refresh (1 min lag)</div>'
    f'</div>',
    unsafe_allow_html=True)

st.markdown("<br/>", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# LIVE QUEUE STATS
# ──────────────────────────────────────────────────────────────────────────────

try:
    df_stats = run_query_df("""
        SELECT
            COUNT(*)                                                  AS TOTAL,
            SUM(CASE WHEN RUN_STATUS = 'Pending' THEN 1 ELSE 0 END)    AS PENDING,
            SUM(CASE WHEN RUN_STATUS = 'Processing' THEN 1 ELSE 0 END) AS PROCESSING,
            SUM(CASE WHEN RUN_STATUS = 'Processed' THEN 1 ELSE 0 END)  AS PROCESSED,
            SUM(CASE WHEN RUN_STATUS = 'Error' THEN 1 ELSE 0 END)      AS ERRORS
        FROM ADJUSTMENT_APP.ADJ_HEADER
        WHERE IS_DELETED = FALSE
    """)
    qs = df_stats.iloc[0].to_dict() if not df_stats.empty else {}
except Exception:
    qs = {}

c1, c2, c3, c4 = st.columns(4)
stat_items = [
    ("Pending",    qs.get("PENDING", 0),    P["warning"], "⏸"),
    ("Processing", qs.get("PROCESSING", 0), "#1565C0",    "⚡"),
    ("Processed",  qs.get("PROCESSED", 0),  P["success"], "✔"),
    ("Errors",     qs.get("ERRORS", 0),     P["danger"],  "✗"),
]
for col, (label, val, color, icon) in zip([c1, c2, c3, c4], stat_items):
    col.markdown(
        f'<div style="background:{P["white"]};border:1px solid {P["border"]};'
        f'border-top:3px solid {color};border-radius:8px;padding:0.8rem;text-align:center">'
        f'<div style="font-size:1.6rem;font-weight:800;color:{color}">{icon} {int(val)}</div>'
        f'<div style="font-size:0.72rem;text-transform:uppercase;letter-spacing:.06em;'
        f'color:{P["grey_700"]};margin-top:3px">{label}</div>'
        f'</div>',
        unsafe_allow_html=True)

st.markdown("<br/>", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# ACTIVE QUEUE ITEMS
# ──────────────────────────────────────────────────────────────────────────────

section_title("Active Items", "⚡")

if df_q.empty:
    st.markdown(
        f'<div class="mcard" style="text-align:center;padding:2rem;color:{P["grey_700"]}">'
        f'✅ No items currently in the queue.</div>',
        unsafe_allow_html=True)
else:
    for _, qi in df_q.iterrows():
        adj_id      = qi["ADJ_ID"]
        scope       = str(qi.get("PROCESS_TYPE", ""))
        adj_type    = str(qi.get("ADJUSTMENT_TYPE", ""))
        run_status  = str(qi.get("RUN_STATUS", ""))
        queue_pos   = qi.get("QUEUE_POSITION", "?")
        scope_cfg   = SCOPE_CONFIG.get(scope, {})
        submitted_at = qi.get("SUBMITTED_AT", "")
        entity      = str(qi.get("ENTITY_CODE", "")) or "—"
        occurrence  = str(qi.get("ADJUSTMENT_OCCURRENCE", ""))

        status_color = P["info"] if run_status == "Processing" else P["warning"]
        status_icon  = "⚡" if run_status == "Processing" else "⏸"

        st.markdown(
            f'<div class="queue-item {run_status.lower()}">'
            f'<div style="display:flex;justify-content:space-between;align-items:flex-start">'
            f'  <div>'
            f'    <span style="font-weight:700;font-size:0.92rem">#{queue_pos} — ADJ #{adj_id}</span>'
            f'    &nbsp;<span style="font-size:0.75rem;color:{P["grey_700"]}">'
            f'    {scope_cfg.get("icon","")} {scope} · {adj_type} · Entity: {entity}</span>'
            f'  </div>'
            f'  <span style="font-size:0.75rem;font-weight:700;color:{status_color}">'
            f'  {status_icon} {run_status}</span>'
            f'</div>'
            f'<div style="display:flex;justify-content:space-between;margin-top:4px;font-size:0.72rem;color:{P["grey_700"]}">'
            f'  <span>COB: {qi.get("COBID","?")} · {occurrence} · Submitted by: {qi.get("SUBMITTED_BY","?")}</span>'
            f'  <span>Submitted: {submitted_at.strftime("%d %b %H:%M") if hasattr(submitted_at, "strftime") else str(submitted_at)}</span>'
            f'</div>'
            f'</div>',
            unsafe_allow_html=True)

    st.markdown("<br/>", unsafe_allow_html=True)
    if st.button("🔄 Refresh Queue", type="primary"):
        safe_rerun()

# ──────────────────────────────────────────────────────────────────────────────
# RECENTLY PROCESSED
# ──────────────────────────────────────────────────────────────────────────────

st.markdown("<br/>", unsafe_allow_html=True)
section_title("Recently Processed", "📜")

try:
    df_recent = run_query_df("""
        SELECT ADJ_ID, COBID, PROCESS_TYPE, ADJUSTMENT_TYPE, ENTITY_CODE,
               BOOK_CODE, RUN_STATUS, USERNAME, RECORD_COUNT,
               CREATED_DATE, PROCESS_DATE, ERRORMESSAGE
        FROM ADJUSTMENT_APP.ADJ_HEADER
        WHERE RUN_STATUS IN ('Processed', 'Error')
          AND IS_DELETED = FALSE
        ORDER BY PROCESS_DATE DESC
        LIMIT 30
    """)

    if not df_recent.empty:
        def color_status(val):
            if val == "Processed":
                return f"color:{P['success']};font-weight:600"
            if val == "Error":
                return f"color:{P['danger']};font-weight:600"
            return ""

        display_cols = ["ADJ_ID", "PROCESS_TYPE", "ADJUSTMENT_TYPE", "RUN_STATUS",
                        "ENTITY_CODE", "RECORD_COUNT", "USERNAME", "PROCESS_DATE", "ERRORMESSAGE"]
        existing_cols = [c for c in display_cols if c in df_recent.columns]
        st.dataframe(
            df_recent[existing_cols].style.map(color_status, subset=["RUN_STATUS"]),
            use_container_width=True, height=300,
        )
    else:
        st.info("No recently processed items.")
except Exception:
    st.info("No processing history available yet.")


# ──────────────────────────────────────────────────────────────────────────────
# SNOWFLAKE TASK SCHEDULE (Reference)
# ──────────────────────────────────────────────────────────────────────────────

st.markdown("<br/>", unsafe_allow_html=True)
section_title("Snowflake Task Schedule (Reference)", "⚙️")

with st.expander("View Snowflake Tasks configuration", expanded=False):
    st.markdown("""
    | Task | Schedule | Trigger | Action |
    |------|----------|---------|--------|
    | `PROCESS_PENDING_TASK` | Every 1 min | When `ADJ_HEADER_STREAM` has data | Calls `SP_PROCESS_ADJUSTMENT()` for each pending row |
    | `INSTANTIATE_RECURRING_TASK` | Every 5 min | Time-based | Creates new ADJ_HEADER rows from recurring templates |

    **Dynamic Table Refresh:**
    - `DT_DASHBOARD` refreshes with **1-minute lag** — near real-time metrics
    - `DT_OVERLAP_ALERTS` refreshes with **1-minute lag** — detects overlapping adjustments

    **Processing Flow:**
    1. User creates adjustment via Streamlit (insert into `ADJ_HEADER`)
    2. `ADJ_HEADER_STREAM` captures the CDC event
    3. `PROCESS_PENDING_TASK` wakes up (every 60 seconds, stream-guarded)
    4. Task calls `SP_PROCESS_ADJUSTMENT()` for each row with status = 'Pending'
    5. SP writes deltas to the appropriate FACT table
    6. Status updated to 'Processed' (or 'Error' on failure)
    7. Dynamic tables auto-refresh with updated aggregations
    """)
