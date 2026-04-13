"""
Adjustment Tracker — Lifecycle Overview
=========================================
Pipeline board showing all adjustments by lifecycle stage,
with deep-dive timeline per adjustment.
Reads from: VW_ADJUSTMENT_TRACK.
"""
import streamlit as st
import pandas as pd

st.set_page_config(
    page_title="Tracker · MUFG", page_icon="🔍",
    layout="wide", initial_sidebar_state="expanded",
)

from utils.styles import (
    inject_css, render_sidebar, render_lifecycle_bar, section_title,
    render_status_timeline,
    P, SCOPE_CONFIG, STAGE_CONFIG,
)
from utils.snowflake_conn import run_query, run_query_df, current_user_name

inject_css()
render_sidebar()

user = current_user_name()

st.markdown("## 🔍 Adjustment Tracker")
st.markdown(
    f"<span style='color:{P['grey_700']};font-size:0.9rem'>"
    f"Full lifecycle tracking — from submission to report refresh.</span>",
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
    filter_cob = st.selectbox("COB Date", options=["All"] + cob_options, index=0, key="tr_cob")
with f2:
    filter_scope = st.multiselect(
        "Scope", list(SCOPE_CONFIG.keys()), default=[], key="tr_scope")
with f3:
    show_all = st.checkbox("All users", value=False,
                           help="Show adjustments from all users", key="tr_all")
with f4:
    show_deleted = st.checkbox("Include deleted", value=False, key="tr_del")

st.markdown("<br/>", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# LOAD DATA
# ──────────────────────────────────────────────────────────────────────────────

try:
    where = ["1=1"]
    if not show_deleted:
        where.append("IS_DELETED = FALSE")
    if not show_all:
        where.append(f"SUBMITTED_BY = '{user}'")
    if filter_cob and filter_cob != "All":
        where.append(f"COBID = {filter_cob}")
    if filter_scope:
        in_list = ",".join(f"'{s}'" for s in filter_scope)
        where.append(f"PROCESS_TYPE IN ({in_list})")
    where_sql = " AND ".join(where)

    df_track = run_query_df(f"""
        SELECT *
        FROM ADJUSTMENT_APP.VW_ADJUSTMENT_TRACK
        WHERE {where_sql}
        ORDER BY SUBMITTED_AT DESC
        LIMIT 500
    """)
except Exception as e:
    df_track = pd.DataFrame()
    st.warning(f"Could not load tracking data: {e}")

if df_track.empty:
    st.markdown(
        f'<div class="mcard" style="text-align:center;padding:2.5rem;color:{P["grey_700"]}">'
        f'<div style="font-size:1.8rem">🕳️</div>'
        f'<div style="font-size:0.9rem;margin-top:0.5rem">No adjustments found</div>'
        f'</div>',
        unsafe_allow_html=True)
    st.stop()

# ──────────────────────────────────────────────────────────────────────────────
# PIPELINE BOARD — Overview by stage
# ──────────────────────────────────────────────────────────────────────────────

section_title("Pipeline Board", "📊")

BOARD_STAGES = [
    "Submitted", "Pending Approval", "Approved",
    "Processing", "PBI Queued", "PBI Refreshing", "Reports Ready",
]

stage_counts = {}
for stage in BOARD_STAGES:
    stage_counts[stage] = int(df_track[df_track["CURRENT_STAGE"] == stage].shape[0])

board_html = '<div class="tracker-board">'
for stage in BOARD_STAGES:
    cfg = STAGE_CONFIG.get(stage, {"color": "#9E9E9E", "icon": "", "bg": "#F5F5F5"})
    count = stage_counts[stage]
    items_df = df_track[df_track["CURRENT_STAGE"] == stage].head(10)

    items_html = ""
    for _, row in items_df.iterrows():
        scope = str(row.get("PROCESS_TYPE", ""))
        scope_cfg = SCOPE_CONFIG.get(scope, {})
        entity = str(row.get("ENTITY_CODE", "")) or ""
        book = str(row.get("BOOK_CODE", "")) or ""
        detail_parts = [x for x in [entity, book] if x]
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
        f'{cfg["icon"]} {stage}'
        f'<span class="board-col-count" style="color:{cfg["color"]}">{count}</span>'
        f'</div>'
        f'{items_html}'
        f'</div>')
board_html += '</div>'
st.markdown(board_html, unsafe_allow_html=True)

# Show failed/rejected count if any
failed_df = df_track[df_track["CURRENT_STAGE"].isin(["Failed", "Rejected"])]
if not failed_df.empty:
    st.markdown(
        f'<div style="background:{P["danger_lt"]};border-left:4px solid {P["danger"]};'
        f'border-radius:8px;padding:0.6rem 1rem;margin-bottom:1rem;font-size:0.82rem">'
        f'❌ <strong>{len(failed_df)}</strong> adjustment(s) in Failed/Rejected status'
        f'</div>',
        unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# DETAIL TABLE + DEEP-DIVE
# ──────────────────────────────────────────────────────────────────────────────

section_title("Adjustment Details", "📋")

display_cols = [
    "ADJ_ID", "PROCESS_TYPE", "ADJUSTMENT_TYPE", "ENTITY_CODE",
    "CURRENT_STAGE", "SUBMITTED_BY", "SUBMITTED_AT", "TOTAL_DURATION_SEC",
]
available_cols = [c for c in display_cols if c in df_track.columns]
df_display = df_track[available_cols].copy()

if "TOTAL_DURATION_SEC" in df_display.columns:
    def _fmt_duration(sec):
        if sec is None or pd.isna(sec):
            return "—"
        sec = int(sec)
        if sec < 60:
            return f"{sec}s"
        if sec < 3600:
            return f"{sec // 60}m {sec % 60}s"
        return f"{sec // 3600}h {(sec % 3600) // 60}m"
    df_display["TOTAL_DURATION_SEC"] = df_display["TOTAL_DURATION_SEC"].apply(_fmt_duration)
    df_display = df_display.rename(columns={"TOTAL_DURATION_SEC": "TOTAL_DURATION"})

st.dataframe(df_display.style.hide(axis='index'), use_container_width=True, height=300)

# ── Deep-dive expanders ──────────────────────────────────────────────────────

section_title("Deep Dive", "🔎")

for _, row in df_track.iterrows():
    adj_id = row.get("ADJ_ID", "?")
    scope = str(row.get("PROCESS_TYPE", ""))
    current_stage = str(row.get("CURRENT_STAGE", ""))
    scope_cfg = SCOPE_CONFIG.get(scope, {})
    stage_cfg = STAGE_CONFIG.get(current_stage, {"icon": "", "color": "#9E9E9E"})

    with st.expander(
        f'{scope_cfg.get("icon", "")} {scope} · {row.get("ADJUSTMENT_TYPE", "")} · '
        f'{stage_cfg["icon"]} {current_stage} · ADJ #{adj_id}',
        expanded=False,
    ):
        render_lifecycle_bar(row.to_dict())

        col_detail, col_pbi = st.columns([1, 1])

        with col_detail:
            section_title("Adjustment Info", "📋")

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
            section_title("PowerBI Refresh", "📈")

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
                section_title("Processing", "⚡")
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
                f'<h4>❌ Error</h4>'
                f'<div style="font-size:0.82rem;font-family:monospace">'
                f'{row["ERRORMESSAGE"]}</div>'
                f'</div>',
                unsafe_allow_html=True)

        st.markdown("---")
        section_title("Status History", "🕐")
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
