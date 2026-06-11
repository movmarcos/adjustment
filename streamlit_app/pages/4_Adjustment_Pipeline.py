"""
Adjustment Pipeline — lifecycle + live processing
=================================================
Merged view of the old Processing Queue and Adjustment Tracker:
  • Live stats + Snowflake pipeline diagram
  • Stage board (Submitted → Approved → Processing → Reports Ready)
  • Running / waiting adjustments, with Force-process
  • Per-adjustment lifecycle deep-dive (processing + PowerBI + status history)

Reads from: VW_ADJUSTMENT_TRACK (everything) and calls
SP_FORCE_PROCESS_ADJUSTMENT for the force action.
"""
import streamlit as st
import pandas as pd

st.set_page_config(
    page_title="Pipeline · MUFG", page_icon="⏳",
    layout="wide", initial_sidebar_state="expanded",
)

from utils.styles import (
    inject_css, render_sidebar, render_lifecycle_bar, render_pipeline_diagram,
    section_title, render_status_timeline, fmt_adj_id,
    P, SCOPE_CONFIG, STAGE_CONFIG, icon,
)
from utils.snowflake_conn import run_query, run_query_df, current_user_name, safe_rerun

# A Pending/Approved item older than this (minutes) is flagged as possibly stuck —
# the scope tasks poll every minute, so anything beyond a couple of cycles is
# suspicious and the "Force process" action is emphasised.
STUCK_AFTER_MIN = 3

inject_css()
render_sidebar()

user = current_user_name()

st.markdown("## ⏳ Adjustment Pipeline")
st.markdown(
    f"<span style='color:{P['grey_700']};font-size:0.9rem'>"
    "From submission to report refresh — live status, the processing pipeline, and "
    "a deep-dive per adjustment. Adjustments are processed asynchronously by Snowflake "
    "Tasks that poll every minute.</span>",
    unsafe_allow_html=True)
st.markdown("<br/>", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# FILTERS
# ──────────────────────────────────────────────────────────────────────────────

f1, f2, f3, f4 = st.columns(4)
with f1:
    try:
        cob_rows = run_query("""
            SELECT DISTINCT COBID FROM ADJUSTMENT_APP.ADJ_HEADER
            WHERE IS_DELETED = FALSE ORDER BY COBID DESC LIMIT 30
        """)
        cob_options = [int(r["COBID"]) for r in cob_rows] if cob_rows else []
    except Exception:
        cob_options = []
    filter_cob = st.selectbox("COB Date", options=["All"] + cob_options, index=0, key="pl_cob")
with f2:
    filter_scope = st.multiselect(
        "Scope", list(SCOPE_CONFIG.keys()), default=[], key="pl_scope")
with f3:
    mine_only = st.checkbox("Only my adjustments", value=False,
                            help="Show only adjustments you submitted", key="pl_mine")
with f4:
    show_deleted = st.checkbox("Include deleted", value=False, key="pl_del")

st.markdown("<br/>", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# LOAD DATA
# ──────────────────────────────────────────────────────────────────────────────

try:
    where = ["1=1"]
    if not show_deleted:
        where.append("IS_DELETED = FALSE")
    if mine_only:
        where.append(f"SUBMITTED_BY = '{user}'")
    if filter_cob and filter_cob != "All":
        where.append(f"COBID = {filter_cob}")
    if filter_scope:
        in_list = ",".join(f"'{s}'" for s in filter_scope)
        where.append(f"PROCESS_TYPE IN ({in_list})")
    where_sql = " AND ".join(where)

    df_track = run_query_df(f"""
        SELECT *,
               DATEDIFF('minute', SUBMITTED_AT,
                        CONVERT_TIMEZONE('Europe/London', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9)) AS WAIT_MIN
        FROM ADJUSTMENT_APP.VW_ADJUSTMENT_TRACK
        WHERE {where_sql}
        ORDER BY SUBMITTED_AT DESC
        LIMIT 500
    """)
except Exception as e:
    df_track = pd.DataFrame()
    st.warning(f"Could not load pipeline data: {e}")

if df_track.empty:
    st.markdown(
        f'<div class="mcard" style="text-align:center;padding:2.5rem;color:{P["grey_700"]}">'
        f'<div>{icon("inbox", size=28, color=P["grey_400"], valign="0")}</div>'
        f'<div style="font-size:0.9rem;margin-top:0.5rem">No adjustments found</div>'
        f'</div>',
        unsafe_allow_html=True)
    st.stop()

# ──────────────────────────────────────────────────────────────────────────────
# LIVE STATS + PIPELINE DIAGRAM
# ──────────────────────────────────────────────────────────────────────────────

_status = df_track["RUN_STATUS"].fillna("")
pending_count   = int(_status.isin(["Pending", "Approved"]).sum())
running_count   = int((_status == "Running").sum())
processed_count = int((_status == "Processed").sum())
failed_count    = int((_status == "Failed").sum())

c1, c2, c3, c4 = st.columns(4)
stat_items = [
    ("Pending / Approved", pending_count,   P["warning"], "clock"),
    ("Running",            running_count,   P["info"],    "zap"),
    ("Processed",          processed_count, P["success"], "check-circle"),
    ("Failed",             failed_count,    P["danger"],  "x-circle"),
]
for col, (label, val, color, icon_name) in zip([c1, c2, c3, c4], stat_items):
    col.markdown(
        f'<div style="background:{P["white"]};border:1px solid {P["border"]};'
        f'border-top:3px solid {color};border-radius:8px;padding:0.8rem;text-align:center">'
        f'<div style="font-size:1.6rem;font-weight:800;color:{color};font-variant-numeric:tabular-nums">{icon(icon_name, size=15, color=color)} {val}</div>'
        f'<div style="font-size:0.72rem;text-transform:uppercase;letter-spacing:.06em;'
        f'color:{P["grey_700"]};margin-top:3px">{label}</div>'
        f'</div>',
        unsafe_allow_html=True)

st.markdown("<br/>", unsafe_allow_html=True)

section_title("Snowflake Processing Pipeline", "sliders")
stage = 3 if running_count > 0 else (2 if pending_count > 0 else 5)
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
# PIPELINE BOARD — overview by stage
# ──────────────────────────────────────────────────────────────────────────────

section_title("Pipeline Board", "table")

BOARD_STAGES = [
    "Submitted", "Pending Approval", "Approved",
    "Processing", "PBI Queued", "PBI Refreshing", "Reports Ready",
]
board_html = '<div class="tracker-board">'
for stg in BOARD_STAGES:
    cfg = STAGE_CONFIG.get(stg, {"color": "#9E9E9E", "icon": "", "bg": "#F5F5F5"})
    items_df = df_track[df_track["CURRENT_STAGE"] == stg]
    count = int(items_df.shape[0])
    items_html = ""
    for _, row in items_df.head(10).iterrows():
        scope = str(row.get("PROCESS_TYPE", ""))
        scope_cfg = SCOPE_CONFIG.get(scope, {})
        detail_parts = [x for x in [str(row.get("ENTITY_CODE", "")) or "",
                                    str(row.get("BOOK_CODE", "")) or ""] if x]
        detail = " · ".join(detail_parts) if detail_parts else "All"
        items_html += (
            f'<div class="board-item">'
            f'<div class="bi-scope">{scope_cfg.get("icon", "")} {scope}</div>'
            f'<div class="bi-detail">{detail}</div>'
            f'</div>')
    if count > 10:
        items_html += (
            f'<div style="font-size:0.65rem;color:{P["grey_700"]};text-align:center;padding:4px">'
            f'+ {count - 10} more</div>')
    board_html += (
        f'<div class="board-col" style="border-top-color:{cfg["color"]}">'
        f'<div class="board-col-header" style="color:{cfg["color"]}">'
        f'{cfg["icon"]} {stg}'
        f'<span class="board-col-count" style="color:{cfg["color"]}">{count}</span>'
        f'</div>{items_html}</div>')
board_html += '</div>'
st.markdown(board_html, unsafe_allow_html=True)

failed_df = df_track[df_track["CURRENT_STAGE"].isin(["Failed", "Rejected"])]
if not failed_df.empty:
    st.markdown(
        f'<div style="background:{P["danger_lt"]};border-left:4px solid {P["danger"]};'
        f'border-radius:8px;padding:0.6rem 1rem;margin:0.6rem 0;font-size:0.82rem">'
        f'{icon("x-circle", size=13, color=P["danger"])} <strong>{len(failed_df)}</strong> adjustment(s) in Failed/Rejected status</div>',
        unsafe_allow_html=True)

st.markdown("<br/>", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# RUNNING / WAITING — live items with Force-process
# ──────────────────────────────────────────────────────────────────────────────

section_title("Running & Waiting", "zap")

live_df = df_track[df_track["RUN_STATUS"].isin(["Pending", "Approved", "Running"])]
if live_df.empty:
    st.markdown(
        f'<div class="mcard" style="text-align:center;padding:1.5rem;color:{P["grey_700"]}">'
        f'{icon("check-circle", size=13, color=P["success"])} Nothing in flight — all adjustments are processed.</div>',
        unsafe_allow_html=True)
else:
    for _, qi in live_df.iterrows():
        adj_id      = qi["ADJ_ID"]
        scope       = str(qi.get("PROCESS_TYPE", ""))
        adj_type    = str(qi.get("ADJUSTMENT_TYPE", ""))
        run_status  = str(qi.get("RUN_STATUS", ""))
        scope_cfg   = SCOPE_CONFIG.get(scope, {})
        submitted_at = qi.get("SUBMITTED_AT", "")
        entity      = str(qi.get("ENTITY_CODE", "")) or "—"

        status_color = P["info"] if run_status == "Running" else ("#0F766E" if run_status == "Approved" else P["warning"])
        status_icon  = icon("zap" if run_status == "Running"
                            else ("check-circle" if run_status == "Approved" else "clock"),
                            size=12, color=status_color)
        # No report number yet (assigned at processing) — short id keeps rows distinct.
        sub_txt = (submitted_at.strftime("%d %b %H:%M")
                   if hasattr(submitted_at, "strftime") and str(submitted_at) != "NaT"
                   else str(submitted_at))

        st.markdown(
            f'<div class="queue-item {run_status.lower()}">'
            f'<div style="display:flex;justify-content:space-between;align-items:flex-start">'
            f'  <div>'
            f'    <span style="font-weight:700;font-size:0.92rem">ADJ #{str(adj_id)[:8]}…</span>'
            f'    &nbsp;<span style="font-size:0.75rem;color:{P["grey_700"]}">'
            f'    {scope} · {adj_type} · Entity: {entity}</span>'
            f'  </div>'
            f'  <span style="font-size:0.75rem;font-weight:700;color:{status_color}">'
            f'  {status_icon} {run_status}</span>'
            f'</div>'
            f'<div style="display:flex;justify-content:space-between;margin-top:4px;font-size:0.72rem;color:{P["grey_700"]}">'
            f'  <span>COB: {qi.get("COBID","?")} · Submitted by: {qi.get("SUBMITTED_BY","?")}</span>'
            f'  <span>Submitted: {sub_txt}</span>'
            f'</div>'
            f'</div>',
            unsafe_allow_html=True)

        # Force-process (Pending/Approved only) — the tasks poll every minute; if a
        # row is stranded the user can push it through immediately.
        if run_status in ("Pending", "Approved"):
            try:
                wait_min = int(qi.get("WAIT_MIN")) if pd.notna(qi.get("WAIT_MIN")) else None
            except (TypeError, ValueError):
                wait_min = None
            stuck = wait_min is not None and wait_min >= STUCK_AFTER_MIN

            wcol, bcol = st.columns([3, 1])
            with wcol:
                if wait_min is not None:
                    if stuck:
                        st.markdown(
                            f"<span style='color:{P['danger']};font-size:0.76rem;font-weight:600'>"
                            f"{icon('alert-triangle', size=12, color='#B45309')} Waiting {wait_min} min — the task may have missed it; force it through.</span>",
                            unsafe_allow_html=True)
                    else:
                        st.markdown(
                            f"<span style='color:{P['grey_700']};font-size:0.76rem'>"
                            f"Waiting {wait_min} min — the task polls every minute.</span>",
                            unsafe_allow_html=True)
            with bcol:
                if st.button("⏵ Force process", key=f"force_{adj_id}",
                             type="primary" if stuck else "secondary",
                             use_container_width=True,
                             help="Bypass the task and process this adjustment now."):
                    with st.spinner("Forcing through the pipeline…"):
                        try:
                            res = run_query(
                                f"CALL ADJUSTMENT_APP.SP_FORCE_PROCESS_ADJUSTMENT('{adj_id}')")
                            st.success(f"ADJ #{str(adj_id)[:8]}… forced. {res[0][0] if res else ''}")
                        except Exception as fe:
                            st.error(f"Force failed: {fe}")
                    safe_rerun()

    st.markdown("<br/>", unsafe_allow_html=True)
    if st.button("Refresh", type="primary"):
        safe_rerun()

st.markdown("<br/>", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# DETAIL TABLE
# ──────────────────────────────────────────────────────────────────────────────

section_title("Adjustment Details", "file-text")

display_cols = [
    "DIMENSION_ADJ_ID", "PROCESS_TYPE", "ADJUSTMENT_TYPE", "ENTITY_CODE",
    "CURRENT_STAGE", "SUBMITTED_BY", "SUBMITTED_AT",
]
available_cols = [c for c in display_cols if c in df_track.columns]
df_display = df_track[available_cols].copy()

if "DIMENSION_ADJ_ID" in df_display.columns:
    df_display["DIMENSION_ADJ_ID"] = df_display["DIMENSION_ADJ_ID"].apply(fmt_adj_id)

if "SUBMITTED_AT" in df_display.columns:
    df_display["SUBMITTED_AT"] = (
        pd.to_datetime(df_display["SUBMITTED_AT"], errors="coerce")
          .dt.strftime("%d %b %Y %H:%M").fillna("—")
    )

# Total duration = submitted → reports ready; falls back to submitted → processed.
def _fmt_duration(sec):
    if sec is None or pd.isna(sec):
        return "—"
    sec = int(sec)
    if sec < 60:
        return f"{sec}s"
    if sec < 3600:
        return f"{sec // 60}m {sec % 60}s"
    return f"{sec // 3600}h {(sec % 3600) // 60}m"

def _row_total_sec(r):
    start = r.get("SUBMITTED_AT")
    end = r.get("PBI_COMPLETED_AT")
    if end is None or pd.isna(end):
        end = r.get("PROCESSING_ENDED_AT")
    if start is None or pd.isna(start) or end is None or pd.isna(end):
        return None
    try:
        return (pd.Timestamp(end) - pd.Timestamp(start)).total_seconds()
    except (TypeError, ValueError):
        return None

df_display["TOTAL_DURATION"] = df_track.apply(_row_total_sec, axis=1).apply(_fmt_duration)

df_display = df_display.rename(columns={
    "DIMENSION_ADJ_ID": "Adj ID",
    "PROCESS_TYPE":     "Scope",
    "ADJUSTMENT_TYPE":  "Type",
    "ENTITY_CODE":      "Entity",
    "CURRENT_STAGE":    "Stage",
    "SUBMITTED_BY":     "Submitted By",
    "SUBMITTED_AT":     "Submitted",
    "TOTAL_DURATION":   "Total Duration",
})

st.dataframe(df_display.style.hide(axis='index'), use_container_width=True, height=300)

# ──────────────────────────────────────────────────────────────────────────────
# DEEP DIVE
# ──────────────────────────────────────────────────────────────────────────────

section_title("Deep Dive", "search")

def _fmt_ts(val):
    if val is None or str(val) in ("NaT", "None", ""):
        return "—"
    if hasattr(val, "strftime"):
        return val.strftime("%d %b %Y %H:%M:%S")
    return str(val) if str(val) not in ("None", "") else "—"

def _fmt_dur(sec):
    if sec is None or (hasattr(sec, '__float__') and pd.isna(sec)):
        return "—"
    sec = int(sec)
    if sec < 60:
        return f"{sec}s"
    if sec < 3600:
        return f"{sec // 60}m {sec % 60}s"
    return f"{sec // 3600}h {(sec % 3600) // 60}m"

for _, row in df_track.iterrows():
    adj_id = row.get("ADJ_ID", "?")
    adj_label = fmt_adj_id(row.get("DIMENSION_ADJ_ID"))
    scope = str(row.get("PROCESS_TYPE", ""))
    current_stage = str(row.get("CURRENT_STAGE", ""))
    scope_cfg = SCOPE_CONFIG.get(scope, {})
    stage_cfg = STAGE_CONFIG.get(current_stage, {"icon": "", "color": "#9E9E9E"})

    with st.expander(
        f'{scope_cfg.get("icon", "")} {scope} · {row.get("ADJUSTMENT_TYPE", "")} · '
        f'{stage_cfg["icon"]} {current_stage} · ADJ {adj_label}',
        expanded=False,
    ):
        render_lifecycle_bar(row.to_dict())

        col_detail, col_pbi = st.columns([1, 1])

        with col_detail:
            section_title("Adjustment Info", "file-text")
            info_rows = [
                ("COB",           str(row.get("COBID", "—"))),
                ("Scope",         scope),
                ("Type",          str(row.get("ADJUSTMENT_TYPE", "—"))),
                ("Entity",        str(row.get("ENTITY_CODE", "")) or "—"),
                ("Book",          str(row.get("BOOK_CODE", "")) or "—"),
                ("Submitted by",  str(row.get("SUBMITTED_BY", "—"))),
                ("Submitted",     _fmt_ts(row.get("SUBMITTED_AT"))),
                ("Reason",        str(row.get("REASON", "")) or "—"),
            ]
            if row.get("GLOBAL_REFERENCE"):
                info_rows.append(("Reference", str(row.get("GLOBAL_REFERENCE"))))
            rows_html = "".join(
                f'<tr><td style="color:{P["grey_700"]};padding:3px 0;font-size:0.78rem;'
                f'white-space:nowrap;padding-right:12px">{k}</td>'
                f'<td style="font-size:0.8rem;font-weight:600">{v}</td></tr>'
                for k, v in info_rows if v and v != "—"
            )
            st.markdown(
                f'<div class="mcard" style="padding:0.6rem 0.8rem">'
                f'<table style="width:100%;border-collapse:collapse">{rows_html}</table>'
                f'</div>',
                unsafe_allow_html=True)

        with col_pbi:
            section_title("PowerBI Refresh", "line-chart")
            pbi_rows = [
                ("PBI Action ID",     str(row.get("PBI_ACTION_ID", "")) or "—"),
                ("Queued",            _fmt_ts(row.get("PBI_QUEUED_AT"))),
                ("Refresh Started",   _fmt_ts(row.get("PBI_STARTED_AT"))),
                ("Refresh Completed", _fmt_ts(row.get("PBI_COMPLETED_AT"))),
                ("Queue Wait",        _fmt_dur(row.get("PBI_QUEUE_WAIT_SEC"))),
                ("Refresh Duration",  _fmt_dur(row.get("PBI_REFRESH_DURATION_SEC"))),
                ("Report Status",     str(row.get("REPORT_STATUS", "")) or "Awaiting"),
            ]
            pbi_html = "".join(
                f'<tr><td style="color:{P["grey_700"]};padding:3px 0;font-size:0.78rem;'
                f'white-space:nowrap;padding-right:12px">{k}</td>'
                f'<td style="font-size:0.8rem;font-weight:600">{v}</td></tr>'
                for k, v in pbi_rows if v and v != "—"
            )
            st.markdown(
                f'<div class="mcard" style="padding:0.6rem 0.8rem">'
                f'<table style="width:100%;border-collapse:collapse">{pbi_html}</table>'
                f'</div>',
                unsafe_allow_html=True)

            if row.get("PROCESSING_STARTED_AT"):
                section_title("Processing", "zap")
                proc_rows = [
                    ("Started",  _fmt_ts(row.get("PROCESSING_STARTED_AT"))),
                    ("Ended",    _fmt_ts(row.get("PROCESSING_ENDED_AT"))),
                    ("Duration", _fmt_dur(row.get("PROCESSING_DURATION_SEC"))),
                ]
                proc_html = "".join(
                    f'<tr><td style="color:{P["grey_700"]};padding:3px 0;font-size:0.78rem;'
                    f'white-space:nowrap;padding-right:12px">{k}</td>'
                    f'<td style="font-size:0.8rem;font-weight:600">{v}</td></tr>'
                    for k, v in proc_rows if v and v != "—"
                )
                st.markdown(
                    f'<div class="mcard" style="padding:0.6rem 0.8rem">'
                    f'<table style="width:100%;border-collapse:collapse">{proc_html}</table>'
                    f'</div>',
                    unsafe_allow_html=True)

        if row.get("ERRORMESSAGE"):
            st.markdown(
                f'<div class="overlap-box" style="margin-top:0.5rem">'
                f'<h4>{icon("x-circle", size=13, color=P["danger"])} Error</h4>'
                f'<div style="font-size:0.82rem;font-family:monospace">'
                f'{row["ERRORMESSAGE"]}</div>'
                f'</div>',
                unsafe_allow_html=True)

        st.markdown("---")
        section_title("Status History", "clock")
        try:
            history = run_query(f"""
                SELECT NEW_STATUS, OLD_STATUS, CHANGED_BY, CHANGED_AT, COMMENT
                FROM ADJUSTMENT_APP.ADJ_STATUS_HISTORY
                WHERE ADJ_ID = '{adj_id}'
                ORDER BY CHANGED_AT DESC
            """)
            history_dicts = [dict(h) for h in history] if history else []
            render_status_timeline(history_dicts)
        except Exception:
            st.info("No history available.")
