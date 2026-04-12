"""
Processing Queue — Snowflake Async Pipeline
============================================
Shows the Snowflake Tasks → Pipeline → Dynamic Table processing
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
    "Adjustments are processed asynchronously via Snowflake Tasks polling every minute. "
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

pending_count = int(df_q[df_q["RUN_STATUS"].isin(["Pending", "Approved"])].shape[0]) if not df_q.empty else 0
running_count = int(df_q[df_q["RUN_STATUS"] == "Running"].shape[0]) if not df_q.empty else 0

# Determine pipeline stage
if running_count > 0:
    stage = 3   # SP executing
elif pending_count > 0:
    stage = 2   # Adjustment queued, task will poll within 1 minute
else:
    stage = 5   # All done / idle

render_pipeline_diagram(current_stage=stage)

st.markdown(
    f'<div style="display:grid;grid-template-columns:repeat(5,1fr);gap:10px;'
    f'font-size:0.78rem;color:{P["grey_700"]};text-align:center;margin-top:0.3rem">'
    f'<div>Adjustment saved to<br/><strong>ADJ_HEADER</strong></div>'
    f'<div><strong>Scope task</strong><br/>polls every 1 min<br/>exits fast when idle</div>'
    f'<div><strong>SP_RUN_PIPELINE</strong><br/>claim → block → process → unblock</div>'
    f'<div><strong>Dynamic Tables</strong><br/>auto-refresh (1 min lag)</div>'
    f'<div><strong>PowerBI Refresh</strong><br/>ControlM every ~5 min</div>'
    f'</div>',
    unsafe_allow_html=True)

st.markdown("<br/>", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# LIVE QUEUE STATS
# ──────────────────────────────────────────────────────────────────────────────

try:
    df_stats = run_query_df("""
        SELECT
            COUNT(*)                                                                       AS TOTAL,
            SUM(CASE WHEN RUN_STATUS IN ('Pending', 'Approved') THEN 1 ELSE 0 END)        AS PENDING,
            SUM(CASE WHEN RUN_STATUS = 'Running'                THEN 1 ELSE 0 END)        AS RUNNING,
            SUM(CASE WHEN RUN_STATUS = 'Processed'              THEN 1 ELSE 0 END)        AS PROCESSED,
            SUM(CASE WHEN RUN_STATUS = 'Failed'                 THEN 1 ELSE 0 END)        AS FAILED
        FROM ADJUSTMENT_APP.ADJ_HEADER
        WHERE IS_DELETED = FALSE
    """)
    qs = df_stats.iloc[0].to_dict() if not df_stats.empty else {}
except Exception:
    qs = {}

c1, c2, c3, c4 = st.columns(4)
stat_items = [
    ("Pending / Approved", qs.get("PENDING", 0), P["warning"], "⏸"),
    ("Running",    qs.get("RUNNING", 0),    "#1565C0",    "⚡"),
    ("Processed",  qs.get("PROCESSED", 0),  P["success"], "✔"),
    ("Failed",     qs.get("FAILED", 0),     P["danger"],  "✗"),
]
for col, (label, val, color, icon) in zip([c1, c2, c3, c4], stat_items):
    col.markdown(
        f'<div style="background:{P["white"]};border:1px solid {P["border"]};'
        f'border-top:3px solid {color};border-radius:8px;padding:0.8rem;text-align:center">'
        f'<div style="font-size:1.6rem;font-weight:800;color:{color}">{icon} {int(val) if val == val else 0}</div>'
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

        status_color = "#1565C0" if run_status == "Running" else ("#00897B" if run_status == "Approved" else P["warning"])
        status_icon  = "⚡" if run_status == "Running" else ("✅" if run_status == "Approved" else "⏸")

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
            f'  <span>Submitted: {submitted_at.strftime("%d %b %H:%M") if hasattr(submitted_at, "strftime") and str(submitted_at) != "NaT" else str(submitted_at)}</span>'
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
        SELECT
            ADJ_ID, COBID, PROCESS_TYPE, ADJUSTMENT_TYPE, ENTITY_CODE,
            RUN_STATUS, USERNAME, RECORD_COUNT,
            CREATED_DATE, START_DATE, PROCESS_DATE,
            DATEDIFF('second', START_DATE, PROCESS_DATE) AS DURATION_SECONDS,
            ERRORMESSAGE
        FROM ADJUSTMENT_APP.ADJ_HEADER
        WHERE RUN_STATUS IN ('Processed', 'Failed')
          AND IS_DELETED = FALSE
        ORDER BY PROCESS_DATE DESC
        LIMIT 30
    """)

    if not df_recent.empty:
        import pandas as pd

        def _fmt_dur(s):
            try:
                s = int(s)
            except (TypeError, ValueError):
                return "—"
            if s < 0:   return "—"
            if s < 60:  return f"{s}s"
            if s < 3600: return f"{s//60}m {s%60}s"
            return f"{s//3600}h {(s%3600)//60}m"

        df_recent["Duration"] = df_recent["DURATION_SECONDS"].apply(
            lambda v: _fmt_dur(v) if pd.notna(v) else "—"
        )
        df_recent = df_recent.drop(columns=["DURATION_SECONDS"])

        def color_status(val):
            if val == "Processed": return f"color:{P['success']};font-weight:600"
            if val == "Failed":    return f"color:{P['danger']};font-weight:600"
            return ""

        display_cols = ["ADJ_ID", "PROCESS_TYPE", "ADJUSTMENT_TYPE", "RUN_STATUS",
                        "ENTITY_CODE", "RECORD_COUNT", "USERNAME",
                        "CREATED_DATE", "START_DATE", "PROCESS_DATE", "Duration", "ERRORMESSAGE"]
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
| `TASK_PROCESS_VAR` | Every 1 min | Time-based polling | Calls `SP_RUN_PIPELINE('VaR', ...)` |
| `TASK_PROCESS_STRESS` | Every 1 min | Time-based polling | Calls `SP_RUN_PIPELINE('Stress', ...)` |
| `TASK_PROCESS_FRTB` | Every 1 min | Time-based polling | Calls `SP_RUN_PIPELINE('FRTB', ...)` — covers FRTB, FRTBDRC, FRTBRRAO, FRTBALL |
| `TASK_PROCESS_SENSITIVITY` | Every 1 min | Time-based polling | Calls `SP_RUN_PIPELINE('Sensitivity', ...)` |

**Why time-based polling (no stream guard):**
Streams only capture INSERT events. When a blocked adjustment is unblocked (`BLOCKED_BY_ADJ_ID` set to NULL by `_unblock_resolved`), that is an UPDATE — invisible to APPEND_ONLY streams. With a stream guard, the task would sleep after processing the first adjustment and the unblocked second adjustment would be stuck forever. Pure polling at 1-minute intervals is correct here. `SP_RUN_PIPELINE` exits in milliseconds when nothing is eligible.

**Processing Flow:**
1. User submits adjustment → `SP_SUBMIT_ADJUSTMENT` inserts into `ADJ_HEADER` (checks for Running overlaps → sets `BLOCKED_BY_ADJ_ID` if blocked)
2. Within ≤1 minute: scope task fires and calls `SP_RUN_PIPELINE`
3. `SP_RUN_PIPELINE`: atomically claims eligible Pending/Approved → Running, blocks overlapping adjustments, calls `SP_PROCESS_ADJUSTMENT` per adjustment, unblocks waiting adjustments
4. `SP_PROCESS_ADJUSTMENT` Scale path: 3-way UNION ALL (same-COB scale/flatten + cross-COB roll legs) → netted into **one delta row per position** (cross-COB roll pairs collapsed: e.g. −1 + 1.5 → 0.5) → DENSE_RANK overlap resolution → supersede → SCD2 key fix → summary rebuild
5. Status updated to `Processed` (or `Failed` on error)
6. If an adjustment was blocked, `BLOCKED_BY_ADJ_ID` is cleared → next task poll (within 1 min) picks it up
7. `SP_PROCESS_ADJUSTMENT` calls `FACT.UPDATE_POWERBI_FOR_ADJUSTMENTS` → queues PowerBI refresh in `METADATA.POWERBI_ACTION`
8. ControlM job (every ~5 min) picks up queued actions and triggers the PowerBI dataset refresh
9. Once the refresh completes, reports reflect the adjustment data — status shows in **Report Refresh Status** below

**Dynamic Table Refresh:**
- `DT_DASHBOARD` refreshes with **1-minute lag**
- `DT_OVERLAP_ALERTS` refreshes with **1-minute lag** — detects overlapping adjustments (includes Running)
    """)

# ──────────────────────────────────────────────────────────────────────────────
# REPORT REFRESH STATUS
# ──────────────────────────────────────────────────────────────────────────────

st.markdown("<br/>", unsafe_allow_html=True)
section_title("Report Refresh Status", "📈")

try:
    df_pbi_status = run_query_df("""
        SELECT
            CASE
                WHEN START_TIME IS NULL THEN 'Queued'
                WHEN COMPLETE_TIME IS NULL THEN 'Running'
                ELSE 'Completed'
            END AS STATUS,
            CASE
                WHEN OBJECT_NAME = 'RaptorReporting' AND INSERT_SOURCE LIKE '%VAR%' THEN 'VaR'
                WHEN OBJECT_NAME = 'RaptorReporting' AND INSERT_SOURCE LIKE '%STRESS%' THEN 'Stress'
                WHEN OBJECT_NAME = 'RaptorReporting' AND INSERT_SOURCE LIKE '%SENSITIVITY%' THEN 'Sensitivity'
                WHEN OBJECT_NAME = 'RaptorReporting' AND INSERT_SOURCE LIKE '%FRTB%' THEN 'FRTB'
                WHEN OBJECT_NAME = 'VaR Adjustment Summary Import' THEN 'VaR'
                WHEN OBJECT_NAME = 'Stress Measures Adjustment Import' THEN 'Stress'
                WHEN OBJECT_NAME = 'Sensitivity Summary Adjustment Import' THEN 'Sensitivity'
                ELSE OBJECT_NAME
            END AS SCOPE,
            COBID,
            OBJECT_NAME,
            INSERT_SOURCE,
            CONVERT_TIMEZONE('UTC', 'Europe/London', REQUEST_TIME::TIMESTAMP_NTZ) AS REQUEST_TIME,
            CONVERT_TIMEZONE('UTC', 'Europe/London', START_TIME::TIMESTAMP_NTZ) AS START_TIME,
            CONVERT_TIMEZONE('UTC', 'Europe/London', COMPLETE_TIME::TIMESTAMP_NTZ) AS COMPLETE_TIME
        FROM METADATA.POWERBI_ACTION
        WHERE INSERT_SOURCE LIKE 'LOAD_%_ADJUSTMENT'
        ORDER BY REQUEST_TIME DESC
        LIMIT 30
    """)

    if not df_pbi_status.empty:
        pbi_queued    = int(df_pbi_status[df_pbi_status["STATUS"] == "Queued"].shape[0])
        pbi_running   = int(df_pbi_status[df_pbi_status["STATUS"] == "Running"].shape[0])
        pbi_completed = int(df_pbi_status[df_pbi_status["STATUS"] == "Completed"].shape[0])

        pc1, pc2, pc3 = st.columns(3)
        pbi_stats = [
            ("Queued",    pbi_queued,    P["warning"]),
            ("Running",   pbi_running,   "#1565C0"),
            ("Completed", pbi_completed, P["success"]),
        ]
        for col, (lbl, val, color) in zip([pc1, pc2, pc3], pbi_stats):
            col.markdown(
                f'<div style="background:{P["white"]};border:1px solid {P["border"]};'
                f'border-top:3px solid {color};border-radius:8px;padding:0.6rem;text-align:center">'
                f'<div style="font-size:1.3rem;font-weight:800;color:{color}">{val}</div>'
                f'<div style="font-size:0.7rem;text-transform:uppercase;letter-spacing:.06em;'
                f'color:{P["grey_700"]};margin-top:2px">{lbl}</div>'
                f'</div>',
                unsafe_allow_html=True)

        st.markdown("<br/>", unsafe_allow_html=True)

        def color_pbi_status(val):
            if val == "Completed": return f"color:{P['success']};font-weight:600"
            if val == "Running":   return f"color:#1565C0;font-weight:600"
            if val == "Queued":    return f"color:{P['warning']};font-weight:600"
            return ""

        display = ["STATUS", "SCOPE", "COBID", "OBJECT_NAME",
                    "REQUEST_TIME", "START_TIME", "COMPLETE_TIME"]
        existing = [c for c in display if c in df_pbi_status.columns]
        st.dataframe(
            df_pbi_status[existing].style.map(color_pbi_status, subset=["STATUS"]),
            use_container_width=True, height=250,
        )
    else:
        st.info("No adjustment-related PowerBI refresh actions found.")
except Exception as pbi_ex:
    st.info(f"PowerBI refresh status not available: {pbi_ex}")
