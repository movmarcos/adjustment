"""
Logs — Processing Runs, Activity & Errors
=========================================
Technical-ops view of what the engine did, redesigned for scanability:

  • Processing Runs : one aligned row per batch with a status chip — pick a
                      run to inspect its adjustments and errors below.
  • Activity Feed   : a real feed (day-grouped, status badges, actor, detail)
                      instead of a raw dataframe dump.
  • Errors          : failed adjustments as always-visible cards with the
                      message and the fix (Retry on the Adjustments page).

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
    fmt_user_dt,
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

def _fmt_ts(val, fmt="%d %b %Y %H:%M:%S"):
    if val is None or str(val) in ("NaT", "None", ""):
        return "—"
    if hasattr(val, "strftime"):
        return fmt_user_dt(val, fmt)
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


def _esc_html(v) -> str:
    import html
    return html.escape(str(v)) if v is not None else ""


def _pill(text, color) -> str:
    return (f'<span style="background:{color}18;color:{color};border:1px solid {color}55;'
            f'border-radius:99px;padding:1px 9px;font-size:0.72rem;font-weight:700;'
            f'white-space:nowrap">{text}</span>')


def _scope_pill(scope) -> str:
    cfg = SCOPE_CONFIG.get(str(scope), {})
    return _pill(str(scope) or "—", cfg.get("color", P["grey_700"]))


def _status_pill(status) -> str:
    return _pill(str(status) or "—", STATUS_COLORS.get(str(status), P["grey_700"]))


def _table(headers, rows, aligns=None) -> str:
    """Styled, aligned HTML table — much easier to scan than expander lines."""
    aligns = aligns or ["left"] * len(headers)
    th = "".join(
        f'<th style="text-align:{a};padding:6px 10px;font-size:0.72rem;'
        f'text-transform:uppercase;letter-spacing:.05em;color:{P["grey_700"]};'
        f'border-bottom:2px solid {P["border"]};white-space:nowrap">{h}</th>'
        for h, a in zip(headers, aligns))
    trs = ""
    for r in rows:
        tds = "".join(
            f'<td style="text-align:{a};padding:5px 10px;font-size:0.82rem;'
            f'border-bottom:1px solid {P["border"]};vertical-align:middle;'
            f'font-variant-numeric:tabular-nums">{c}</td>'
            for c, a in zip(r, aligns))
        trs += f"<tr>{tds}</tr>"
    return (f'<div style="overflow-x:auto;background:{P["white"]};'
            f'border:1px solid {P["border"]};border-radius:8px">'
            f'<table style="width:100%;border-collapse:collapse">'
            f'<thead><tr>{th}</tr></thead><tbody>{trs}</tbody></table></div>')


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
    filter_cob = st.selectbox("COB Date", options=["All"] + cob_options, index=0, key="lg_cob")
with f2:
    filter_scope = st.multiselect(
        "Scope", ALL_SCOPES, default=[], key="lg_scope")
with f3:
    row_limit = st.selectbox("Rows", options=[100, 200, 500, 1000], index=1, key="lg_limit")


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

tab_runs, tab_activity, tab_errors = st.tabs(
    ["Processing Runs", "Activity Feed", "Errors"])


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
            ORDER BY MAX(PROCESS_DATE) DESC NULLS LAST
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
             ("Rows written", _fmt_int(df_runs["TOTAL_RECORDS"].sum()), P["success"]),
             ("With failures", _tot_failed,
              P["danger"] if _tot_failed else P["grey_700"])],
        ):
            col.markdown(
                f'<div style="background:{P["white"]};border:1px solid {P["border"]};'
                f'border-top:3px solid {color};border-radius:8px;padding:0.7rem;'
                f'text-align:center">'
                f'<div style="font-size:1.35rem;font-weight:800;color:{color};'
                f'font-variant-numeric:tabular-nums">{val}</div>'
                f'<div style="font-size:0.7rem;text-transform:uppercase;'
                f'letter-spacing:.06em;color:{P["grey_700"]}">{label}</div></div>',
                unsafe_allow_html=True)
        st.markdown("<div style='height:0.6rem'></div>", unsafe_allow_html=True)

        def _run_outcome(r):
            if int(r.get("RUNNING_COUNT", 0) or 0) > 0:
                return _pill("Running", P["info"])
            if int(r.get("FAILED_COUNT", 0) or 0) > 0:
                return _pill("Failed", P["danger"])
            return _pill("OK", P["success"])

        rows = []
        for _, r in df_runs.iterrows():
            rows.append([
                _run_outcome(r),
                f'<strong>{int(r["RUN_LOG_ID"])}</strong>',
                _scope_pill(r.get("PROCESS_TYPE")),
                _esc_html(r.get("ADJUSTMENT_ACTION") or "—"),
                int(r["COBID"]) if pd.notna(r.get("COBID")) else "—",
                int(r.get("ADJ_COUNT", 0)),
                _fmt_int(r.get("TOTAL_RECORDS")),
                _fmt_dur(r.get("DURATION_SEC")),
                _fmt_ts(r.get("ENDED"), "%d %b %H:%M"),
            ])
        _html_table = _table(
            ["", "Run", "Scope", "Action", "COB", "Adj", "Rows", "Duration", "Ended"],
            rows,
            aligns=["left", "right", "left", "left", "right", "right", "right",
                    "right", "right"])
        st.markdown(_html_table, unsafe_allow_html=True)

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
            adj_rows = []
            for _, a in df_adj.iterrows():
                adj_rows.append([
                    f'<strong>{fmt_adj_id(a.get("DIMENSION_ADJ_ID"))}</strong>',
                    _esc_html(a.get("ADJUSTMENT_TYPE") or "—"),
                    _esc_html(a.get("ENTITY_CODE") or "—"),
                    _esc_html(a.get("BOOK_CODE") or "—"),
                    _status_pill(a.get("RUN_STATUS")),
                    _fmt_int(a.get("RECORD_COUNT")),
                    _fmt_dur(
                        (pd.Timestamp(a["PROCESS_DATE"]) - pd.Timestamp(a["START_DATE"])).total_seconds()
                        if pd.notna(a.get("START_DATE")) and pd.notna(a.get("PROCESS_DATE"))
                        else None),
                    _esc_html(a.get("USERNAME") or "—"),
                ])
            st.markdown(_table(
                ["Adj ID", "Type", "Entity", "Book", "Status", "Rows",
                 "Duration", "Submitted by"],
                adj_rows,
                aligns=["left", "left", "left", "left", "left", "right",
                        "right", "left"]),
                unsafe_allow_html=True)

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
        # Day-grouped feed with badges — scannable, unlike a raw dataframe.
        df_act["_DAY"] = (pd.to_datetime(df_act["EVENT_TIME"], errors="coerce")
                          .dt.strftime("%A %d %b %Y").fillna("Unknown date"))

        for day, day_df in df_act.groupby("_DAY", sort=False):
            st.markdown(
                f'<div style="font-size:0.78rem;font-weight:700;'
                f'text-transform:uppercase;letter-spacing:.06em;'
                f'color:{P["grey_700"]};margin:0.9rem 0 0.3rem 0">{day}</div>',
                unsafe_allow_html=True)
            feed_rows = []
            for _, ev in day_df.iterrows():
                detail_bits = [b for b in [
                    _esc_html(ev.get("ENTITY_CODE") or ""),
                    _esc_html(ev.get("BOOK_CODE") or "")] if b]
                where_txt = " · ".join(detail_bits) if detail_bits else "All"
                feed_rows.append([
                    _fmt_ts(ev.get("EVENT_TIME"), "%H:%M:%S"),
                    _status_pill(ev.get("CURRENT_STATUS")),
                    f'<strong>{fmt_adj_id(ev.get("DIMENSION_ADJ_ID"))}</strong>',
                    _scope_pill(ev.get("PROCESS_TYPE")),
                    _esc_html(ev.get("ADJUSTMENT_TYPE") or "—"),
                    where_txt,
                    _esc_html(ev.get("ACTOR") or "—"),
                    f'<span style="color:{P["grey_700"]}">'
                    f'{_esc_html(ev.get("EVENT_DETAIL") or "")}</span>',
                ])
            st.markdown(_table(
                ["Time", "Status", "Adj", "Scope", "Type", "Entity / Book",
                 "By", "Detail"],
                feed_rows,
                aligns=["right", "left", "left", "left", "left", "left",
                        "left", "left"]),
                unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — ERRORS  (VW_ERRORS)
# ══════════════════════════════════════════════════════════════════════════════
with tab_errors:
    section_title("Errors", "x-circle")
    st.markdown(
        f"<span style='color:{P['grey_700']};font-size:0.8rem'>"
        f"Adjustments currently in Failed status — fix with "
        f"<strong>Retry</strong> on the Adjustments page.</span>",
        unsafe_allow_html=True)

    try:
        df_err = run_query_df(f"""
            SELECT
                ERROR_TIME, COBID, PROCESS_TYPE, ADJUSTMENT_TYPE,
                ENTITY_CODE, BOOK_CODE, USERNAME, DIMENSION_ADJ_ID, REASON, ERRORMESSAGE
            FROM ADJUSTMENT_APP.VW_ERRORS
            WHERE 1=1 {_cob_filter()}{_scope_filter()}
            ORDER BY ERROR_TIME DESC
            LIMIT {int(row_limit)}
        """)
    except Exception as e:
        df_err = pd.DataFrame()
        st.warning(f"Could not load errors: {e}")

    if df_err.empty:
        st.success("No failed adjustments.")
    else:
        st.markdown(
            f'<div style="background:{P["danger_lt"]};border-left:4px solid {P["danger"]};'
            f'border-radius:8px;padding:0.6rem 1rem;margin-bottom:0.8rem;font-size:0.82rem">'
            f'{icon("x-circle", size=13, color=P["danger"])} '
            f'<strong>{len(df_err)}</strong> failed adjustment(s)</div>',
            unsafe_allow_html=True)

        # Always-visible cards: an error should not need a click to be seen.
        for _, e in df_err.iterrows():
            meta_bits = " · ".join(b for b in [
                f'Entity {_esc_html(e.get("ENTITY_CODE"))}' if e.get("ENTITY_CODE") else "",
                f'Book {_esc_html(e.get("BOOK_CODE"))}' if e.get("BOOK_CODE") else "",
                f'by {_esc_html(e.get("USERNAME"))}' if e.get("USERNAME") else "",
            ] if b)
            reason = _esc_html(e.get("REASON") or "")
            err_msg = _esc_html(e.get("ERRORMESSAGE") or "(no message recorded)")
            st.markdown(
                f'<div style="background:{P["white"]};border:1px solid {P["border"]};'
                f'border-left:4px solid {P["danger"]};border-radius:8px;'
                f'padding:0.7rem 1rem;margin-bottom:0.6rem">'
                f'<div style="display:flex;justify-content:space-between;'
                f'align-items:center;flex-wrap:wrap;gap:6px">'
                f'  <div style="font-size:0.88rem">'
                f'    <strong>ADJ {fmt_adj_id(e.get("DIMENSION_ADJ_ID"))}</strong>'
                f'    &nbsp;{_scope_pill(e.get("PROCESS_TYPE"))}'
                f'    &nbsp;<span style="color:{P["grey_700"]};font-size:0.78rem">'
                f'    {_esc_html(e.get("ADJUSTMENT_TYPE") or "")} · COB '
                f'    {e.get("COBID", "—")}</span>'
                f'  </div>'
                f'  <div style="font-size:0.74rem;color:{P["grey_700"]}">'
                f'  {_fmt_ts(e.get("ERROR_TIME"))}</div>'
                f'</div>'
                + (f'<div style="font-size:0.76rem;color:{P["grey_700"]};'
                   f'margin-top:2px">{meta_bits}</div>' if meta_bits else "")
                + (f'<div style="font-size:0.78rem;margin-top:4px">'
                   f'<span style="color:{P["grey_700"]}">Reason:</span> '
                   f'{reason}</div>' if reason else "")
                + f'<div style="font-size:0.79rem;font-family:monospace;'
                  f'background:{P["danger_lt"]};border-radius:6px;'
                  f'padding:0.45rem 0.6rem;margin-top:6px;'
                  f'word-break:break-word">{err_msg}</div>'
                f'</div>',
                unsafe_allow_html=True)
