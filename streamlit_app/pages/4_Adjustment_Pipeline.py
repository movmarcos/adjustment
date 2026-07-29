"""
Adjustment Pipeline — live processing, problems first
=====================================================
Redesigned around what an analyst actually needs, in priority order:
  1. Needs Attention — failed runs, stuck (stale-Running) runs, blocked rows,
     each with a plain-language reason and the action that fixes it.
  2. In Flight — everything queued or running right now, with wait/run timers
     and Force-process where it applies.
  3. Pipeline flow + stage board — where things are end-to-end.
  4. All adjustments table + per-adjustment deep dive.

Reads from: VW_ADJUSTMENT_TRACK (incl. BLOCKED_BY info) and calls
SP_FORCE_PROCESS_ADJUSTMENT for the force action.
"""
import streamlit as st
import pandas as pd
import json
import html as _htmlmod

st.set_page_config(
    page_title="Pipeline · MUFG", page_icon="⏳",
    layout="wide", initial_sidebar_state="expanded",
)

from utils.styles import (
    inject_css, render_sidebar, render_lifecycle_bar, render_pipeline_diagram,
    section_title, render_status_timeline, fmt_adj_id,
    P, SCOPE_CONFIG, ALL_SCOPES, STAGE_CONFIG, icon, render_df_table,
)
from utils.snowflake_conn import run_query, run_query_df, current_user_name, safe_rerun

# A Pending/Approved item older than this (minutes) is flagged as possibly
# stuck — the scope tasks poll every minute, so anything beyond a couple of
# cycles is suspicious and Force-process is emphasised.
STUCK_AFTER_MIN = 3
# A Running item older than this is probably a dead run (task timeout is 3h).
RUNNING_WARN_MIN = 180
# The pipeline's reaper auto-resets Running rows to Failed at this age, and
# SP_FORCE_PROCESS will rescue a Running row from this age too.
REAPER_MIN = 240

inject_css()
render_sidebar()

user = current_user_name()

_hdr, _btn_col = st.columns([5, 1])
with _hdr:
    st.markdown("## Adjustment Pipeline")
    st.markdown(
        f"<span style='color:{P['grey_700']};font-size:0.9rem'>"
        "From submission to report refresh. Adjustments are processed "
        "automatically by Snowflake tasks that poll every minute — this page "
        "shows what is happening and flags anything that needs a human.</span>",
        unsafe_allow_html=True)
with _btn_col:
    st.markdown("<br/>", unsafe_allow_html=True)
    if st.button("Refresh", type="primary", use_container_width=True):
        safe_rerun()
st.markdown("<br/>", unsafe_allow_html=True)

# Outcome of the last force-process — stashed before the rerun (a message
# rendered just before st.rerun() is destroyed by it).
_flash = st.session_state.pop("pipe_flash", None)
if _flash:
    {"success": st.success, "warning": st.warning}.get(_flash[0], st.error)(_flash[1])

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
        "Scope", ALL_SCOPES, default=[], key="pl_scope")
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
        where.append(f"COBID = {int(filter_cob)}")
    if filter_scope:
        in_list = ",".join(f"'{s}'" for s in filter_scope)
        where.append(f"PROCESS_TYPE IN ({in_list})")
    where_sql = " AND ".join(where)

    df_track = run_query_df(f"""
        SELECT *,
               DATEDIFF('minute', SUBMITTED_AT,
                        CONVERT_TIMEZONE('Europe/London', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9)) AS WAIT_MIN,
               DATEDIFF('minute', PROCESSING_STARTED_AT,
                        CONVERT_TIMEZONE('Europe/London', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9)) AS RUNNING_MIN
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


def _int_or_none(v):
    try:
        return int(v) if pd.notna(v) else None
    except (TypeError, ValueError):
        return None


def _fmt_ts(val):
    if val is None or str(val) in ("NaT", "None", ""):
        return "—"
    if hasattr(val, "strftime"):
        return val.strftime("%d %b %Y %H:%M")
    return str(val)


# Row classifications used by the KPIs and the Needs Attention section
_status  = df_track["RUN_STATUS"].fillna("")
_blocked = df_track["BLOCKED_BY_ADJ_ID"].notna() if "BLOCKED_BY_ADJ_ID" in df_track.columns \
           else pd.Series(False, index=df_track.index)
_running_min = df_track["RUNNING_MIN"] if "RUNNING_MIN" in df_track.columns \
               else pd.Series(pd.NA, index=df_track.index)

is_queued  = _status.isin(["Pending", "Approved"]) & ~_blocked
is_blocked = _status.isin(["Pending", "Approved"]) & _blocked
is_running = _status == "Running"
is_stale   = is_running & _running_min.apply(
    lambda v: _int_or_none(v) is not None and _int_or_none(v) >= RUNNING_WARN_MIN)
is_failed  = _status == "Failed"
is_ready   = df_track["CURRENT_STAGE"].fillna("") == "Reports Ready"

# ──────────────────────────────────────────────────────────────────────────────
# KPI CARDS
# ──────────────────────────────────────────────────────────────────────────────

kpis = [
    ("In Queue",      int(is_queued.sum()),  P["warning"], "clock"),
    ("Blocked",       int(is_blocked.sum()), "#B45309",    "pause-circle"),
    ("Running",       int(is_running.sum()), P["info"],    "zap"),
    ("Failed",        int(is_failed.sum()),  P["danger"],  "x-circle"),
    ("Reports Ready", int(is_ready.sum()),   P["success"], "check-circle"),
]
kcols = st.columns(len(kpis))
for col, (label, val, color, icon_name) in zip(kcols, kpis):
    col.markdown(
        f'<div style="background:{P["white"]};border:1px solid {P["border"]};'
        f'border-top:3px solid {color};border-radius:8px;padding:0.8rem;text-align:center">'
        f'<div style="font-size:1.6rem;font-weight:800;color:{color};font-variant-numeric:tabular-nums">'
        f'{icon(icon_name, size=15, color=color)} {val}</div>'
        f'<div style="font-size:0.72rem;text-transform:uppercase;letter-spacing:.06em;'
        f'color:{P["grey_700"]};margin-top:3px">{label}</div>'
        f'</div>',
        unsafe_allow_html=True)

st.markdown("<br/>", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# FORCE-PROCESS ACTION (shared by the sections below)
# ──────────────────────────────────────────────────────────────────────────────

def _force_process(adj_id) -> None:
    """CALL SP_FORCE_PROCESS_ADJUSTMENT and translate its JSON result into a
    flash that survives the rerun. Never shows success for a non-forced row."""
    with st.spinner("Forcing through the pipeline…"):
        try:
            res = run_query(
                f"CALL ADJUSTMENT_APP.SP_FORCE_PROCESS_ADJUSTMENT('{adj_id}')")
            try:
                out = json.loads(str(res[0][0])) if res else {}
            except (ValueError, TypeError, IndexError):
                out = {}
            f_status = out.get("status", "unknown")
            short = f"ADJ #{str(adj_id)[:8]}…"
            if f_status == "forced":
                st.session_state["pipe_flash"] = (
                    "success", f"{short} processed successfully.")
            elif f_status in ("not_forceable", "blocked", "not_found", "deleted"):
                st.session_state["pipe_flash"] = (
                    "warning", f"{short} was NOT forced — {out.get('message', f_status)}")
            else:   # failed / unknown
                st.session_state["pipe_flash"] = (
                    "error", f"{short} force-processing FAILED: "
                             f"{out.get('error') or out.get('message') or res[0][0]}")
        except Exception as fe:
            st.session_state["pipe_flash"] = (
                "error", f"Force failed. The database reported: {fe}")
    safe_rerun()


def _item_header(row, status_label, status_color, status_icon_name):
    adj_label = fmt_adj_id(row.get("DIMENSION_ADJ_ID")) if pd.notna(row.get("DIMENSION_ADJ_ID")) \
                else f"#{str(row.get('ADJ_ID'))[:8]}…"
    scope    = str(row.get("PROCESS_TYPE", ""))
    adj_type = str(row.get("ADJUSTMENT_TYPE", ""))
    entity   = str(row.get("ENTITY_CODE") or "") or "—"
    st.markdown(
        f'<div style="display:flex;justify-content:space-between;align-items:flex-start">'
        f'  <div>'
        f'    <span style="font-weight:700;font-size:0.92rem">ADJ {adj_label}</span>'
        f'    &nbsp;<span style="font-size:0.75rem;color:{P["grey_700"]}">'
        f'    {scope} · {adj_type} · Entity: {entity} · COB {row.get("COBID", "?")} '
        f'    · by {row.get("SUBMITTED_BY", "?")}</span>'
        f'  </div>'
        f'  <span style="font-size:0.75rem;font-weight:700;color:{status_color}">'
        f'  {icon(status_icon_name, size=12, color=status_color)} {status_label}</span>'
        f'</div>',
        unsafe_allow_html=True)


# ──────────────────────────────────────────────────────────────────────────────
# NEEDS ATTENTION — failed, stuck-running, blocked
# ──────────────────────────────────────────────────────────────────────────────

attention_df = df_track[is_failed | is_stale | is_blocked]
if not attention_df.empty:
    section_title("Needs Attention", "alert-triangle")

    for _, row in attention_df.iterrows():
        adj_id = row["ADJ_ID"]
        with st.container(border=True):
            if bool(is_failed.get(row.name, False)):
                _item_header(row, "Failed", P["danger"], "x-circle")
                err = str(row.get("ERRORMESSAGE") or "").strip()
                if err:
                    st.markdown(
                        f'<div style="font-size:0.78rem;font-family:monospace;'
                        f'background:{P["danger_lt"]};border-radius:6px;'
                        f'padding:0.4rem 0.6rem;margin-top:0.3rem">{_htmlmod.escape(err)}</div>',
                        unsafe_allow_html=True)
                st.caption("What to do: open the **Adjustments** page, select this "
                           "adjustment and use **Retry** (re-queues it), or "
                           "**Delete** it if it is no longer wanted.")

            elif bool(is_stale.get(row.name, False)):
                _item_header(row, "Running (stuck?)", "#B45309", "alert-triangle")
                rmin = _int_or_none(row.get("RUNNING_MIN"))
                st.caption(
                    f"Running for **{rmin} min** — far beyond a normal run. The "
                    f"process that claimed it has probably died (e.g. a timeout). "
                    f"The pipeline automatically resets such rows to Failed after "
                    f"{REAPER_MIN} minutes, releasing anything queued behind them; "
                    f"after that, Retry re-queues it.")
                if rmin is not None and rmin >= REAPER_MIN:
                    fcol1, fcol2 = st.columns([3, 1])
                    with fcol1:
                        st.caption("It is past the reset threshold — you can also "
                                   "force it through right now:")
                    with fcol2:
                        if st.button("⏵ Force process", key=f"force_stale_{adj_id}",
                                     type="primary", use_container_width=True):
                            _force_process(adj_id)

            else:   # blocked
                _item_header(row, "Blocked", "#B45309", "pause-circle")
                blocker = row.get("BLOCKED_BY_DIM_ID")
                blocker_txt = (fmt_adj_id(blocker) if pd.notna(blocker)
                               else f"#{str(row.get('BLOCKED_BY_ADJ_ID'))[:8]}…")
                st.caption(
                    f"Waiting behind **ADJ {blocker_txt}**, which touches the same "
                    f"data (same COB and overlapping scope). Overlapping adjustments "
                    f"run one at a time so they never write over each other. It "
                    f"starts automatically as soon as the blocker finishes — "
                    f"nothing to do.")
    st.markdown("<br/>", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# IN FLIGHT — queued and running right now
# ──────────────────────────────────────────────────────────────────────────────

section_title("In Flight", "zap")

live_df = df_track[(is_queued | is_running) & ~is_stale]
if live_df.empty and attention_df.empty:
    st.markdown(
        f'<div class="mcard" style="text-align:center;padding:1.5rem;color:{P["grey_700"]}">'
        f'{icon("check-circle", size=13, color=P["success"])} Nothing in flight — '
        f'all adjustments are processed.</div>',
        unsafe_allow_html=True)
elif live_df.empty:
    st.caption("Nothing else in flight beyond the items above.")
else:
    for _, qi in live_df.iterrows():
        adj_id     = qi["ADJ_ID"]
        run_status = str(qi.get("RUN_STATUS", ""))

        with st.container(border=True):
            if run_status == "Running":
                _item_header(qi, "Running", P["info"], "zap")
                rmin = _int_or_none(qi.get("RUNNING_MIN"))
                if rmin is not None:
                    st.caption(f"Processing for {rmin} min. Large Entity Rolls "
                               f"can take ~20 minutes — this is normal.")
            else:
                label = "Approved — queued" if run_status == "Approved" else "Queued"
                _item_header(qi, label, "#0F766E" if run_status == "Approved" else P["warning"], "clock")
                wait_min = _int_or_none(qi.get("WAIT_MIN"))
                stuck = wait_min is not None and wait_min >= STUCK_AFTER_MIN
                wcol, bcol = st.columns([3, 1])
                with wcol:
                    if stuck:
                        st.markdown(
                            f"<span style='color:{P['danger']};font-size:0.76rem;font-weight:600'>"
                            f"{icon('alert-triangle', size=12, color='#B45309')} "
                            f"Waiting {wait_min} min — the task polls every minute, "
                            f"so this is unusual; you can force it through.</span>",
                            unsafe_allow_html=True)
                    elif wait_min is not None:
                        st.markdown(
                            f"<span style='color:{P['grey_700']};font-size:0.76rem'>"
                            f"Waiting {wait_min} min — picked up by the next "
                            f"1-minute poll.</span>",
                            unsafe_allow_html=True)
                with bcol:
                    if st.button("⏵ Force process", key=f"force_{adj_id}",
                                 type="primary" if stuck else "secondary",
                                 use_container_width=True,
                                 help="Bypass the task and process this adjustment now."):
                        _force_process(adj_id)

st.markdown("<br/>", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# PIPELINE FLOW + STAGE BOARD
# ──────────────────────────────────────────────────────────────────────────────

section_title("How Processing Works", "sliders")
stage = 3 if int(is_running.sum()) > 0 else (2 if int((is_queued | is_blocked).sum()) > 0 else 5)
render_pipeline_diagram(current_stage=stage)
st.markdown(
    f'<div style="display:grid;grid-template-columns:repeat(5,1fr);gap:10px;'
    f'font-size:0.78rem;color:{P["grey_700"]};text-align:center;margin-top:0.3rem">'
    f'<div>Adjustment saved to<br/><strong>ADJ_HEADER</strong></div>'
    f'<div><strong>Scope task</strong><br/>polls every 1 min<br/>exits fast when idle</div>'
    f'<div><strong>SP_RUN_PIPELINE</strong><br/>serialises overlaps, claims the '
    f'batch, processes it, releases waiting rows</div>'
    f'<div><strong>Dynamic Tables</strong><br/>auto-refresh (1 min lag)</div>'
    f'<div><strong>PowerBI Refresh</strong><br/>queued per run; picked up every ~5 min</div>'
    f'</div>',
    unsafe_allow_html=True)
st.markdown("<br/>", unsafe_allow_html=True)

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
            f'<div class="bi-scope">{icon(scope_cfg.get("icon", ""), size=11)} {scope}</div>'
            f'<div class="bi-detail">{detail}</div>'
            f'</div>')
    if count > 10:
        items_html += (
            f'<div style="font-size:0.65rem;color:{P["grey_700"]};text-align:center;padding:4px">'
            f'+ {count - 10} more</div>')
    board_html += (
        f'<div class="board-col" style="border-top-color:{cfg["color"]}">'
        f'<div class="board-col-header" style="color:{cfg["color"]}">'
        f'{icon(cfg["icon"], size=13)} {stg}'
        f'<span class="board-col-count" style="color:{cfg["color"]}">{count}</span>'
        f'</div>{items_html}</div>')
board_html += '</div>'
st.markdown(board_html, unsafe_allow_html=True)

st.markdown("<br/>", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# ALL ADJUSTMENTS TABLE
# ──────────────────────────────────────────────────────────────────────────────

section_title("Adjustment Details", "file-text")

display_cols = [
    "DIMENSION_ADJ_ID", "COBID", "PROCESS_TYPE", "ADJUSTMENT_TYPE", "ENTITY_CODE",
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
    "COBID":            "COB",
    "PROCESS_TYPE":     "Scope",
    "ADJUSTMENT_TYPE":  "Type",
    "ENTITY_CODE":      "Entity",
    "CURRENT_STAGE":    "Stage",
    "SUBMITTED_BY":     "Submitted By",
    "SUBMITTED_AT":     "Submitted",
    "TOTAL_DURATION":   "Total Duration",
})

render_df_table(df_display, max_rows=200, height=300)
st.caption(
    "**Total Duration** is the end-to-end lifecycle: from submission through "
    "approval, processing, and Power BI report refresh (submitted → reports ready). "
    "This differs from the Home page's **Processing Time**, which covers only the "
    "engine run (started → ended)."
)

# ──────────────────────────────────────────────────────────────────────────────
# DEEP DIVE
# ──────────────────────────────────────────────────────────────────────────────

section_title("Deep Dive", "search")


def _fmt_ts_long(val):
    if val is None or str(val) in ("NaT", "None", ""):
        return "—"
    if hasattr(val, "strftime"):
        return val.strftime("%d %b %Y %H:%M:%S")
    return str(val) if str(val) not in ("None", "") else "—"


def _kv_card(rows):
    rows_html = "".join(
        f'<tr><td style="color:{P["grey_700"]};padding:3px 0;font-size:0.78rem;'
        f'white-space:nowrap;padding-right:12px">{k}</td>'
        f'<td style="font-size:0.8rem;font-weight:600">{_htmlmod.escape(str(v))}</td></tr>'
        for k, v in rows if v and v != "—"
    )
    st.markdown(
        f'<div class="mcard" style="padding:0.6rem 0.8rem">'
        f'<table style="width:100%;border-collapse:collapse">{rows_html}</table>'
        f'</div>',
        unsafe_allow_html=True)


for _, row in df_track.iterrows():
    adj_id = row.get("ADJ_ID", "?")
    adj_label = fmt_adj_id(row.get("DIMENSION_ADJ_ID"))
    scope = str(row.get("PROCESS_TYPE", ""))
    current_stage = str(row.get("CURRENT_STAGE", ""))

    with st.expander(
        f'{scope} · {row.get("ADJUSTMENT_TYPE", "")} · '
        f'{current_stage} · ADJ {adj_label}',
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
                ("Submitted",     _fmt_ts_long(row.get("SUBMITTED_AT"))),
                ("Reason",        str(row.get("REASON", "")) or "—"),
            ]
            if row.get("GLOBAL_REFERENCE"):
                info_rows.append(("Reference", str(row.get("GLOBAL_REFERENCE"))))
            if pd.notna(row.get("BLOCKED_BY_ADJ_ID")):
                blocker = row.get("BLOCKED_BY_DIM_ID")
                info_rows.append((
                    "Blocked by",
                    fmt_adj_id(blocker) if pd.notna(blocker)
                    else f"#{str(row.get('BLOCKED_BY_ADJ_ID'))[:8]}…"))
            _kv_card(info_rows)

        with col_pbi:
            section_title("PowerBI Refresh", "line-chart")
            _kv_card([
                ("PBI Action ID",     str(row.get("PBI_ACTION_ID", "")) or "—"),
                ("Queued",            _fmt_ts_long(row.get("PBI_QUEUED_AT"))),
                ("Refresh Started",   _fmt_ts_long(row.get("PBI_STARTED_AT"))),
                ("Refresh Completed", _fmt_ts_long(row.get("PBI_COMPLETED_AT"))),
                ("Queue Wait",        _fmt_duration(row.get("PBI_QUEUE_WAIT_SEC"))),
                ("Refresh Duration",  _fmt_duration(row.get("PBI_REFRESH_DURATION_SEC"))),
                ("Report Status",     str(row.get("REPORT_STATUS", "")) or "Awaiting"),
            ])

            if row.get("PROCESSING_STARTED_AT"):
                section_title("Processing", "zap")
                _kv_card([
                    ("Started",  _fmt_ts_long(row.get("PROCESSING_STARTED_AT"))),
                    ("Ended",    _fmt_ts_long(row.get("PROCESSING_ENDED_AT"))),
                    ("Duration", _fmt_duration(row.get("PROCESSING_DURATION_SEC"))),
                ])

        if row.get("ERRORMESSAGE"):
            st.markdown(
                f'<div class="overlap-box" style="margin-top:0.5rem">'
                f'<h4>{icon("x-circle", size=13, color=P["danger"])} Error</h4>'
                f'<div style="font-size:0.82rem;font-family:monospace">'
                f'{_htmlmod.escape(str(row["ERRORMESSAGE"]))}</div>'
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
