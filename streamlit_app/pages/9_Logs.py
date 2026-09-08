"""
Logs — Processing Runs, Activity & Errors
=========================================
Technical-ops view of what the engine did, redesigned for scanability:

  • Processing Runs : one row per batch with a coloured Outcome — pick a
                      run to inspect its adjustments and errors below.
  • Activity Feed   : one grid, newest first, with a Day column (user
                      timezone) and Event / Now coloured by status.
  • Errors          : failed adjustments as always-visible cards with the
                      message and the fix (Retry on the Adjustments page).
  • Sign-Off        : every lifecycle transition as one grid, newest first.

Tabular lists render through render_df_table (the app's standard grid);
only the Errors cards stay as HTML because they carry multi-line messages
and the acknowledgement note.

Reads from ADJ_HEADER, VW_RECENT_ACTIVITY, VW_ERRORS — no extra tables.
"""
import streamlit as st
import pandas as pd

st.set_page_config(
    page_title="Logs · MUFG", page_icon="🧾",
    layout="wide", initial_sidebar_state="expanded",
)

from utils.styles import (
    inject_css, render_sidebar, section_title,
    P, SCOPE_CONFIG, ALL_SCOPES, STATUS_COLORS, fmt_adj_id, icon,
    fmt_user_dt, render_df_table,
)
from utils.snowflake_conn import run_query, run_query_df

inject_css()
render_sidebar()

st.markdown("## Logs")
st.markdown(
    f"<span style='color:{P['grey_700']};font-size:0.9rem'>"
    f"What the engine did — processing runs, activity feed, and errors.</span>",
    unsafe_allow_html=True)
st.markdown("<br/>", unsafe_allow_html=True)


# ── formatting helpers ────────────────────────────────────────────────────────

def _fmt_ts(val, fmt=None):
    """User-timezone timestamp; `fmt=None` uses the fmt_user_dt default so
    every tab shows the same shape."""
    if val is None or str(val) in ("NaT", "None", ""):
        return "—"
    if hasattr(val, "strftime"):
        return (fmt_user_dt(val, fmt) if fmt else fmt_user_dt(val)) or "—"
    return str(val)


def _fmt_dur(sec):
    if sec is None or (hasattr(sec, "__float__") and pd.isna(sec)):
        return "—"
    try:
        sec = int(sec)
    except (TypeError, ValueError):
        return "—"
    if sec < 0:
        return "—"
    if sec < 60:
        return f"{sec}s"
    if sec < 3600:
        return f"{sec // 60}m {sec % 60}s"
    return f"{sec // 3600}h {(sec % 3600) // 60}m"


def _fmt_int(v):
    if v is None or (hasattr(v, "__float__") and pd.isna(v)):
        return "—"
    try:
        return f"{int(v):,}"
    except (TypeError, ValueError):
        return str(v)


def _txt(v, default="—") -> str:
    """Plain text for a grid cell: None/NaN/blank → `default`."""
    if v is None:
        return default
    try:
        if pd.isna(v):
            return default
    except (TypeError, ValueError):
        pass
    s = str(v).strip()
    return s if s else default


def _id_str(v) -> str:
    """Identifiers (run id, COB) as plain strings so the grid never
    thousand-separates them the way it does numeric counts."""
    try:
        return str(int(v))
    except (TypeError, ValueError):
        return _txt(v)


def _day_str(v) -> str:
    """Calendar day in the USER's timezone (same clock as the row times), so
    an event never lands under the wrong date."""
    return fmt_user_dt(v, "%A %d %b %Y") or "Unknown date"


def _scope_color(v) -> str:
    return SCOPE_CONFIG.get(str(v), {}).get("color", P["grey_700"])


def _status_color(v) -> str:
    return STATUS_COLORS.get(str(v), P["grey_700"])


def _esc_html(v) -> str:
    """HTML-escape, whitespace-normalize AND neutralize '$' for the Errors
    cards (the one place this page still composes HTML): newlines end the
    markdown HTML block, and a $...$ pair triggers Streamlit's LaTeX math."""
    import html
    if v is None:
        return ""
    return html.escape(" ".join(str(v).split())).replace("$", "&#36;")


def _pill(text, color) -> str:
    """Inline chip used only inside the Errors cards."""
    return (f'<span style="background:{color}18;color:{color};border:1px solid {color}55;'
            f'border-radius:99px;padding:1px 9px;font-size:0.75rem;font-weight:700;'
            f'white-space:nowrap">{text}</span>')


def _scope_pill(scope) -> str:
    return _pill(_esc_html(_txt(scope)), _scope_color(scope))


# ── filters ───────────────────────────────────────────────────────────────────

f1, f2, f3 = st.columns([1, 2, 1])
with f1:
    try:
        cob_rows = run_query("""
            SELECT DISTINCT COBID FROM ADJUSTMENT_APP.ADJ_HEADER
            WHERE IS_DELETED = FALSE ORDER BY COBID DESC LIMIT 30
        """)
        cob_options = [int(r["COBID"]) for r in cob_rows] if cob_rows else []
    except Exception:
        cob_options = []
    filter_cob = st.selectbox("COB", options=["All"] + cob_options, index=0, key="lg_cob")
with f2:
    filter_scope = st.multiselect(
        "Scope", ALL_SCOPES, default=[], key="lg_scope")
with f3:
    row_limit = st.selectbox(
        "Max rows per tab", options=[100, 200, 500, 1000], index=1, key="lg_limit",
        help="Applies to Processing Runs, Activity Feed, Errors and Sign-Off.")


def _scope_filter(col="PROCESS_TYPE"):
    if not filter_scope:
        return ""
    in_list = ",".join(f"'{s}'" for s in filter_scope)
    return f" AND {col} IN ({in_list})"


def _cob_filter(col="COBID"):
    if filter_cob and filter_cob != "All":
        return f" AND {col} = {int(filter_cob)}"
    return ""


st.markdown("<br/>", unsafe_allow_html=True)

tab_runs, tab_activity, tab_errors, tab_signoff = st.tabs(
    ["Processing Runs", "Activity Feed", "Errors", "Sign-Off"])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — PROCESSING RUNS  (grouped by RUN_LOG_ID)
# ══════════════════════════════════════════════════════════════════════════════
with tab_runs:
    section_title("Processing Runs", "zap")
    st.markdown(
        f"<span style='color:{P['grey_700']};font-size:0.8rem'>"
        f"One row per processing batch (RUN_LOG_ID), newest first. "
        f"Pick a run below the list to inspect its adjustments."
        f"</span>", unsafe_allow_html=True)

    try:
        df_runs = run_query_df(f"""
            SELECT
                RUN_LOG_ID,
                ANY_VALUE(COBID)                                      AS COBID,
                ANY_VALUE(PROCESS_TYPE)                              AS PROCESS_TYPE,
                ANY_VALUE(ADJUSTMENT_ACTION)                         AS ADJUSTMENT_ACTION,
                COUNT(*)                                             AS ADJ_COUNT,
                SUM(COALESCE(RECORD_COUNT, 0))                       AS TOTAL_RECORDS,
                COUNT(CASE WHEN RUN_STATUS = 'Processed' THEN 1 END) AS PROCESSED_COUNT,
                COUNT(CASE WHEN RUN_STATUS = 'Failed'    THEN 1 END) AS FAILED_COUNT,
                COUNT(CASE WHEN RUN_STATUS = 'Running'   THEN 1 END) AS RUNNING_COUNT,
                MIN(START_DATE)                                      AS STARTED,
                MAX(PROCESS_DATE)                                    AS ENDED,
                DATEDIFF('second', MIN(START_DATE), MAX(PROCESS_DATE)) AS DURATION_SEC
            FROM ADJUSTMENT_APP.ADJ_HEADER
            WHERE RUN_LOG_ID IS NOT NULL
              AND IS_DELETED = FALSE
              {_cob_filter()}{_scope_filter()}
            GROUP BY RUN_LOG_ID
            ORDER BY COALESCE(MAX(PROCESS_DATE), MIN(START_DATE)) DESC NULLS LAST
            LIMIT {int(row_limit)}
        """)
    except Exception as e:
        df_runs = pd.DataFrame()
        st.warning(f"Could not load processing runs: {e}")

    if df_runs.empty:
        st.info("No processing runs match the filters.")
    else:
        # KPI strip for the current selection
        k1, k2, k3, k4 = st.columns(4)
        _tot_failed = int(df_runs["FAILED_COUNT"].sum())
        for col, (label, val, color) in zip(
            [k1, k2, k3, k4],
            [("Runs shown", len(df_runs), P["primary"]),
             ("Adjustments", int(df_runs["ADJ_COUNT"].sum()), P["info"]),
             ("Rows (all statuses)", _fmt_int(df_runs["TOTAL_RECORDS"].sum()), P["success"]),
             ("Failed adjustments", _tot_failed,
              P["danger"] if _tot_failed else P["grey_700"])],
        ):
            col.markdown(
                f'<div style="background:{P["white"]};border:1px solid {P["border"]};'
                f'border-top:3px solid {color};border-radius:8px;padding:0.7rem;'
                f'text-align:center">'
                f'<div style="font-size:1.35rem;font-weight:800;color:{color};'
                f'font-variant-numeric:tabular-nums">{val}</div>'
                f'<div style="font-size:0.75rem;text-transform:uppercase;'
                f'letter-spacing:.06em;color:{P["grey_700"]}">{label}</div></div>',
                unsafe_allow_html=True)
        st.markdown("<div style='height:0.6rem'></div>", unsafe_allow_html=True)

        def _run_outcome(r) -> str:
            if int(r.get("RUNNING_COUNT", 0) or 0) > 0:
                return "Running"
            if int(r.get("FAILED_COUNT", 0) or 0) > 0:
                return "Failed"
            return "OK"

        _OUTCOME_COLORS = {"Running": P["info"], "Failed": P["danger"],
                           "OK": P["success"]}
        df_runs_grid = pd.DataFrame([{
            "Outcome":  _run_outcome(r),
            "Run":      _id_str(r.get("RUN_LOG_ID")),
            "Scope":    _txt(r.get("PROCESS_TYPE")),
            "Action":   _txt(r.get("ADJUSTMENT_ACTION")),
            "COB":      _id_str(r.get("COBID")),
            "Adj":      int(r.get("ADJ_COUNT", 0) or 0),
            "Rows":     _fmt_int(r.get("TOTAL_RECORDS")),
            "Duration": _fmt_dur(r.get("DURATION_SEC")),
            "Ended":    _fmt_ts(r.get("ENDED")),
        } for _, r in df_runs.iterrows()])
        render_df_table(
            df_runs_grid, max_rows=int(row_limit),
            color_cols={"Outcome": _OUTCOME_COLORS, "Scope": _scope_color},
            right_cols=("Run", "COB", "Adj", "Rows", "Duration", "Ended"),
            nowrap_cols=("Outcome", "Run", "Scope", "COB", "Ended"),
            key="lg_runs_grid")

        # ── Drill-down: pick a run ────────────────────────────────────────────
        st.markdown("<br/>", unsafe_allow_html=True)
        section_title("Run Detail", "search")

        def _run_label(rid):
            r = df_runs[df_runs["RUN_LOG_ID"] == rid].iloc[0]
            state = ("Running" if int(r.get("RUNNING_COUNT", 0) or 0) > 0 else
                     "Failed" if int(r.get("FAILED_COUNT", 0) or 0) > 0 else "OK")
            return (f'Run {int(rid)} — {r.get("PROCESS_TYPE","")} · '
                    f'{r.get("ADJUSTMENT_ACTION","")} · COB '
                    f'{int(r["COBID"]) if pd.notna(r.get("COBID")) else "—"} · {state}')

        sel_run = st.selectbox(
            "Inspect run", df_runs["RUN_LOG_ID"].tolist(),
            format_func=_run_label, key="lg_run_pick")

        r = df_runs[df_runs["RUN_LOG_ID"] == sel_run].iloc[0]
        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("Adjustments", int(r.get("ADJ_COUNT", 0)))
        m2.metric("Processed", int(r.get("PROCESSED_COUNT", 0)))
        m3.metric("Failed", int(r.get("FAILED_COUNT", 0) or 0))
        m4.metric("Total rows", _fmt_int(r.get("TOTAL_RECORDS")))
        m5.metric("Duration", _fmt_dur(r.get("DURATION_SEC")))
        st.markdown(
            f"<span style='color:{P['grey_700']};font-size:0.8rem'>"
            f"Started {_fmt_ts(r.get('STARTED'))} · Ended {_fmt_ts(r.get('ENDED'))}"
            f"</span>", unsafe_allow_html=True)

        try:
            df_adj = run_query_df(f"""
                SELECT
                    DIMENSION_ADJ_ID, PROCESS_TYPE, ADJUSTMENT_TYPE,
                    COBID, SOURCE_COBID, ENTITY_CODE, BOOK_CODE,
                    RUN_STATUS, RECORD_COUNT,
                    START_DATE, PROCESS_DATE, USERNAME, ERRORMESSAGE
                FROM ADJUSTMENT_APP.ADJ_HEADER
                WHERE RUN_LOG_ID = {int(sel_run)}
                ORDER BY CREATED_DATE
            """)
        except Exception as e:
            df_adj = pd.DataFrame()
            st.warning(f"Could not load run detail: {e}")

        if not df_adj.empty:
            def _adj_dur(a):
                if pd.notna(a.get("START_DATE")) and pd.notna(a.get("PROCESS_DATE")):
                    return _fmt_dur((pd.Timestamp(a["PROCESS_DATE"])
                                     - pd.Timestamp(a["START_DATE"])).total_seconds())
                return _fmt_dur(None)

            df_adj_grid = pd.DataFrame([{
                "Adj ID":       fmt_adj_id(a.get("DIMENSION_ADJ_ID")),
                "Type":         _txt(a.get("ADJUSTMENT_TYPE")),
                "Entity":       _txt(a.get("ENTITY_CODE")),
                "Book":         _txt(a.get("BOOK_CODE")),
                "Status":       _txt(a.get("RUN_STATUS")),
                "Rows":         _fmt_int(a.get("RECORD_COUNT")),
                "Duration":     _adj_dur(a),
                "Submitted by": _txt(a.get("USERNAME")),
            } for _, a in df_adj.iterrows()])
            render_df_table(
                df_adj_grid, max_rows=len(df_adj_grid),
                color_cols={"Status": _status_color},
                right_cols=("Rows", "Duration"),
                nowrap_cols=("Adj ID", "Status"),
                key="lg_run_adj_grid")

            for _, a in df_adj[df_adj["ERRORMESSAGE"].notna()].iterrows():
                st.markdown(
                    f'<div class="overlap-box" style="margin-top:0.4rem">'
                    f'<h4>{icon("x-circle", size=13, color=P["danger"])} '
                    f'Adjustment {fmt_adj_id(a.get("DIMENSION_ADJ_ID"))} error</h4>'
                    f'<div style="font-size:0.8rem;font-family:monospace">'
                    f'{_esc_html(a["ERRORMESSAGE"])}</div></div>',
                    unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — ACTIVITY FEED  (VW_RECENT_ACTIVITY)
# ══════════════════════════════════════════════════════════════════════════════
with tab_activity:
    section_title("Activity Feed", "clock")
    st.markdown(
        f"<span style='color:{P['grey_700']};font-size:0.8rem'>"
        f"Every submission and status change, newest first.</span>",
        unsafe_allow_html=True)

    try:
        df_act = run_query_df(f"""
            SELECT
                EVENT_TIME, EVENT_TYPE, CURRENT_STATUS,
                PROCESS_TYPE, ADJUSTMENT_TYPE, ENTITY_CODE, BOOK_CODE,
                ACTOR, DIMENSION_ADJ_ID, EVENT_DETAIL
            FROM ADJUSTMENT_APP.VW_RECENT_ACTIVITY
            WHERE 1=1 {_cob_filter()}{_scope_filter()}
            ORDER BY EVENT_TIME DESC
            LIMIT {int(row_limit)}
        """)
    except Exception as e:
        df_act = pd.DataFrame()
        st.warning(f"Could not load activity feed: {e}")

    if df_act.empty:
        st.info("No activity matches the filters.")
    else:
        # ONE grid for the whole feed. The query is ORDER BY EVENT_TIME DESC,
        # so rows (and therefore the Day column) run newest first; Day is the
        # USER's timezone calendar day, the same clock as the Time column.
        # "Event" = what happened at that moment (EVENT_TYPE);
        # "Now"   = where the adjustment is today (CURRENT_STATUS).
        def _where(ev) -> str:
            bits = [b for b in (_txt(ev.get("ENTITY_CODE"), ""),
                                _txt(ev.get("BOOK_CODE"), "")) if b]
            return " · ".join(bits) if bits else "All"

        df_feed = pd.DataFrame([{
            "Day":           _day_str(ev.get("EVENT_TIME")),
            "Time":          _fmt_ts(ev.get("EVENT_TIME"), "%H:%M:%S"),
            "Event":         _txt(ev.get("EVENT_TYPE")),
            "Now":           _txt(ev.get("CURRENT_STATUS")),
            "Adj":           fmt_adj_id(ev.get("DIMENSION_ADJ_ID")),
            "Scope":         _txt(ev.get("PROCESS_TYPE")),
            "Type":          _txt(ev.get("ADJUSTMENT_TYPE")),
            "Entity / Book": _where(ev),
            "By":            _txt(ev.get("ACTOR")),
            "Detail":        _txt(ev.get("EVENT_DETAIL"), ""),
        } for _, ev in df_act.iterrows()])
        render_df_table(
            df_feed, max_rows=int(row_limit),
            color_cols={"Event": _status_color, "Now": _status_color,
                        "Scope": _scope_color},
            right_cols=("Time",),
            nowrap_cols=("Day", "Time", "Event", "Now", "Adj", "Scope"),
            wrap_cols={"Detail": 360},
            key="lg_activity_grid")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — ERRORS  (VW_ERRORS)
# ══════════════════════════════════════════════════════════════════════════════
with tab_errors:
    section_title("Errors", "x-circle")
    st.markdown(
        f"<span style='color:{P['grey_700']};font-size:0.8rem'>"
        f"Adjustments currently in Failed status — fix with "
        f"<strong>Retry</strong> on the Adjustments page, or acknowledge a "
        f"handled failure on Home (Current Errors) so System Status returns "
        f"to healthy.</span>",
        unsafe_allow_html=True)

    _err_cols = """
                ERROR_TIME, COBID, PROCESS_TYPE, ADJUSTMENT_TYPE,
                ENTITY_CODE, BOOK_CODE, USERNAME, DIMENSION_ADJ_ID, REASON, ERRORMESSAGE,
                COUNT(*) OVER () AS TOTAL_N"""
    _err_tail = f"""
            FROM ADJUSTMENT_APP.VW_ERRORS
            WHERE 1=1 {_cob_filter()}{_scope_filter()}
            ORDER BY ERROR_TIME DESC
            LIMIT {int(row_limit)}"""
    try:
        try:
            df_err = run_query_df(f"""
                SELECT {_err_cols},
                    IS_ACKNOWLEDGED, ERROR_ACK_BY, ERROR_ACK_NOTE
                {_err_tail}
            """)
        except Exception:
            # View not yet redeployed with the acknowledgement columns.
            df_err = run_query_df(f"""
                SELECT {_err_cols},
                    FALSE AS IS_ACKNOWLEDGED, NULL AS ERROR_ACK_BY,
                    NULL AS ERROR_ACK_NOTE
                {_err_tail}
            """)
    except Exception as e:
        df_err = pd.DataFrame()
        st.warning(f"Could not load errors: {e}")

    if df_err.empty:
        st.success("No failed adjustments.")
    else:
        _ack_mask = df_err["IS_ACKNOWLEDGED"].fillna(False).astype(bool)
        _acked_n = int(_ack_mask.sum())
        try:
            _total_n = int(df_err["TOTAL_N"].iloc[0])
        except (KeyError, TypeError, ValueError):
            _total_n = len(df_err)
        _shown_note = (f' <span style="color:{P["grey_700"]};font-weight:400">'
                       f'(showing first {len(df_err)})</span>'
                       if _total_n > len(df_err) else "")
        st.markdown(
            f'<div style="background:{P["danger_lt"]};border-left:4px solid {P["danger"]};'
            f'border-radius:8px;padding:0.6rem 1rem;margin-bottom:0.8rem;font-size:0.82rem">'
            f'{icon("x-circle", size=13, color=P["danger"])} '
            f'<strong>{_total_n}</strong> failed · '
            f'<strong>{_acked_n}</strong> acknowledged{_shown_note}</div>',
            unsafe_allow_html=True)

        # Always-visible cards: an error should not need a click to be seen.
        # Acknowledged failures mirror Home's ACK tag: greyed card, ACK pill,
        # and "no action needed" instead of the Retry hint.
        for _, e in df_err.iterrows():
            _is_ack = bool(e.get("IS_ACKNOWLEDGED") or False)
            _ack_by = _esc_html(e.get("ERROR_ACK_BY") or "")
            _ack_note = _esc_html(e.get("ERROR_ACK_NOTE") or "")
            _accent = P["grey_400"] if _is_ack else P["danger"]
            _ack_tag = (f' <span title="acknowledged by {_ack_by}" '
                        f'style="background:{P["grey_100"]};color:{P["grey_700"]};'
                        f'border-radius:99px;padding:0 6px;font-size:0.75rem;'
                        f'font-weight:700">ACK</span>' if _is_ack else "")
            meta_bits = " · ".join(b for b in [
                f'Entity {_esc_html(e.get("ENTITY_CODE"))}' if e.get("ENTITY_CODE") else "",
                f'Book {_esc_html(e.get("BOOK_CODE"))}' if e.get("BOOK_CODE") else "",
                f'by {_esc_html(e.get("USERNAME"))}' if e.get("USERNAME") else "",
            ] if b)
            reason = _esc_html(e.get("REASON") or "")
            err_msg = _esc_html(e.get("ERRORMESSAGE") or "(no message recorded)")
            if _is_ack:
                _next_step = (
                    f'<div style="font-size:0.78rem;margin-top:6px;color:{P["grey_700"]}">'
                    f'{icon("check-circle", size=12, color=P["grey_700"])} '
                    f'Acknowledged by <strong>{_ack_by or "—"}</strong> — no action needed.'
                    + (f' <em>{_ack_note}</em>' if _ack_note else "")
                    + '</div>')
            else:
                _next_step = (
                    f'<div style="font-size:0.78rem;margin-top:6px;color:{P["grey_700"]}">'
                    f'Fix with <strong>Retry</strong> on the Adjustments page, or '
                    f'acknowledge it on Home (Current Errors).</div>')
            st.markdown(
                f'<div style="background:{P["white"]};border:1px solid {P["border"]};'
                f'border-left:4px solid {_accent};border-radius:8px;'
                f'padding:0.7rem 1rem;margin-bottom:0.6rem'
                f'{";opacity:.6" if _is_ack else ""}">'
                f'<div style="display:flex;justify-content:space-between;'
                f'align-items:center;flex-wrap:wrap;gap:6px">'
                f'  <div style="font-size:0.88rem">'
                f'    <strong>ADJ {fmt_adj_id(e.get("DIMENSION_ADJ_ID"))}</strong>{_ack_tag}'
                f'    &nbsp;{_scope_pill(e.get("PROCESS_TYPE"))}'
                f'    &nbsp;<span style="color:{P["grey_700"]};font-size:0.78rem">'
                f'    {_esc_html(e.get("ADJUSTMENT_TYPE") or "")} · COB '
                f'    {e.get("COBID", "—")}</span>'
                f'  </div>'
                f'  <div style="font-size:0.75rem;color:{P["grey_700"]}">'
                f'  {_fmt_ts(e.get("ERROR_TIME"))}</div>'
                f'</div>'
                + (f'<div style="font-size:0.76rem;color:{P["grey_700"]};'
                   f'margin-top:2px">{meta_bits}</div>' if meta_bits else "")
                + (f'<div style="font-size:0.78rem;margin-top:4px">'
                   f'<span style="color:{P["grey_700"]}">Reason:</span> '
                   f'{reason}</div>' if reason else "")
                + f'<div style="font-size:0.79rem;font-family:monospace;'
                  f'background:{P["grey_100"] if _is_ack else P["danger_lt"]};'
                  f'border-radius:6px;padding:0.45rem 0.6rem;margin-top:6px;'
                  f'word-break:break-word">{err_msg}</div>'
                + _next_step
                + '</div>',
                unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — SIGN-OFF AUDIT  (full ADJ_SIGNOFF_HISTORY, newest first)
# ══════════════════════════════════════════════════════════════════════════════
with tab_signoff:
    section_title("Sign-Off Audit Trail", "lock")
    st.caption("Every sign-off lifecycle transition — sync, requests, "
               "approvals, rejections — newest first. Times in your selected "
               "timezone.")
    try:
        # Page filters (COB / Scope / Max rows) apply here like every other
        # tab; TOTAL_N tells the user when the row limit truncated the trail.
        _so_hist_all = run_query_df(f"""
            SELECT COBID, PROCESS_TYPE, COALESCE(ENTITY_CODE, '*') AS ENTITY_CODE,
                   SUB_TYPE, OLD_STATUS, NEW_STATUS, ACTION_BY, ACTION_AT, COMMENT,
                   COUNT(*) OVER () AS TOTAL_N
            FROM ADJUSTMENT_APP.ADJ_SIGNOFF_HISTORY
            WHERE 1=1 {_cob_filter()}{_scope_filter()}
            ORDER BY ACTION_AT DESC
            LIMIT {int(row_limit)}
        """)
        if _so_hist_all.empty:
            st.info("No sign-off activity matches the filters.")
        else:
            _f_event = st.multiselect(
                "Event", sorted(_so_hist_all["NEW_STATUS"].astype(str).unique()),
                default=[], key="lg_so_event")
            _df = _so_hist_all
            if _f_event:
                _df = _df[_df["NEW_STATUS"].astype(str).isin(_f_event)]
            try:
                _so_total = int(_so_hist_all["TOTAL_N"].iloc[0])
            except (KeyError, TypeError, ValueError):
                _so_total = len(_so_hist_all)
            if _so_total > len(_so_hist_all):
                st.caption(f"{len(_df)} transition(s) — showing first "
                           f"{len(_so_hist_all)} of {_so_total}; raise "
                           f"“Max rows per tab” or narrow the COB / Scope "
                           f"filters to see more.")
            else:
                st.caption(f"{len(_df)} transition(s)")

            # Same palette as the Sign-Off page: done=green, open work=red,
            # awaiting a decision=orange.
            _SO_EVENT = {
                "SIGNED_OFF":        ("SIGNED OFF",         P["success"]),
                "REOPENED":          ("RE-OPENED",          P["danger"]),
                "OPEN":              ("OPEN",               P["danger"]),
                "SIGNOFF_REQUESTED": ("SIGN-OFF REQUESTED", "#B45309"),
                "REOPEN_REQUESTED":  ("RE-OPEN REQUESTED",  "#B45309"),
            }

            def _so_label(status) -> str:
                s = _txt(status, "")
                if not s:
                    return "—"
                return _SO_EVENT.get(s.upper(), (s, P["grey_700"]))[0]

            def _so_entity(ev) -> str:
                ent = _txt(ev.get("ENTITY_CODE"), "*")
                sub = _txt(ev.get("SUB_TYPE"), "")
                return f"{ent} / {sub}" if sub else ent

            # ONE grid, newest first (query is ORDER BY ACTION_AT DESC); Day
            # is the user-timezone calendar day, same clock as Time.
            df_so_grid = pd.DataFrame([{
                "Day":     _day_str(ev.get("ACTION_AT")),
                "Time":    fmt_user_dt(ev.get("ACTION_AT"), "%H:%M:%S") or "—",
                "Event":   _so_label(ev.get("NEW_STATUS")),
                "COB":     _id_str(ev.get("COBID")),
                "Scope":   _txt(ev.get("PROCESS_TYPE")),
                "Entity":  _so_entity(ev),
                "From":    _so_label(ev.get("OLD_STATUS")),
                "By":      _txt(ev.get("ACTION_BY")),
                "Comment": _txt(ev.get("COMMENT"), ""),
            } for _, ev in _df.iterrows()])
            _ev_colors = {lbl: col for (lbl, col) in _SO_EVENT.values()}
            render_df_table(
                df_so_grid, max_rows=int(row_limit),
                color_cols={"Event": _ev_colors, "From": _ev_colors,
                            "Scope": _scope_color},
                right_cols=("Time", "COB"),
                nowrap_cols=("Day", "Time", "Event", "COB", "Scope", "From"),
                wrap_cols={"Comment": 360},
                key="lg_signoff_grid")
    except Exception as _ex:
        st.info(f"Sign-off history not available: {_ex}")
