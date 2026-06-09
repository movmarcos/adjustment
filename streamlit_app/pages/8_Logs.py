"""
Logs — Processing Runs, Activity & Errors
=========================================
Technical-ops view of what the engine did.

  • Processing Runs : one row per RUN_LOG_ID (a SP_PROCESS_ADJUSTMENT batch) —
                      status, record counts, timing, drill into the adjustments.
  • Activity Feed   : every submission + status change (VW_RECENT_ACTIVITY).
  • Errors          : current Failed adjustments with messages (VW_ERRORS).

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
    P, SCOPE_CONFIG,
)
from utils.snowflake_conn import run_query, run_query_df

inject_css()
render_sidebar()

st.markdown("## 🧾 Logs")
st.markdown(
    f"<span style='color:{P['grey_700']};font-size:0.9rem'>"
    f"What the engine did — processing runs, activity feed, and errors.</span>",
    unsafe_allow_html=True)
st.markdown("<br/>", unsafe_allow_html=True)


# ── helpers ───────────────────────────────────────────────────────────────────

def _fmt_ts(val):
    if val is None or str(val) in ("NaT", "None", ""):
        return "—"
    if hasattr(val, "strftime"):
        return val.strftime("%d %b %Y %H:%M:%S")
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
        "Scope", list(SCOPE_CONFIG.keys()), default=[], key="lg_scope")
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
    ["⚡ Processing Runs", "🕐 Activity Feed", "❌ Errors"])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — PROCESSING RUNS  (grouped by RUN_LOG_ID)
# ══════════════════════════════════════════════════════════════════════════════
with tab_runs:
    section_title("Processing Runs", "⚡")
    st.markdown(
        f"<span style='color:{P['grey_700']};font-size:0.8rem'>"
        f"One row per processing batch (RUN_LOG_ID). Expand to see its adjustments."
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
        for _, r in df_runs.iterrows():
            scope = str(r.get("PROCESS_TYPE", "") or "")
            scfg = SCOPE_CONFIG.get(scope, {})
            failed = int(r.get("FAILED_COUNT", 0) or 0)
            run_icon = "❌" if failed else "✔️"
            header = (
                f'{run_icon}  Run {int(r["RUN_LOG_ID"])}  ·  {scfg.get("icon","")} {scope}'
                f'  ·  {r.get("ADJUSTMENT_ACTION","")}  ·  COB {int(r["COBID"]) if pd.notna(r.get("COBID")) else "—"}'
                f'  ·  {int(r.get("ADJ_COUNT",0))} adj  ·  {_fmt_int(r.get("TOTAL_RECORDS"))} rows'
                f'  ·  {_fmt_dur(r.get("DURATION_SEC"))}'
            )
            with st.expander(header, expanded=False):
                m1, m2, m3, m4, m5 = st.columns(5)
                m1.metric("Adjustments", int(r.get("ADJ_COUNT", 0)))
                m2.metric("Processed", int(r.get("PROCESSED_COUNT", 0)))
                m3.metric("Failed", failed)
                m4.metric("Total rows", _fmt_int(r.get("TOTAL_RECORDS")))
                m5.metric("Duration", _fmt_dur(r.get("DURATION_SEC")))
                st.markdown(
                    f"<span style='color:{P['grey_700']};font-size:0.8rem'>"
                    f"Started {_fmt_ts(r.get('STARTED'))} · Ended {_fmt_ts(r.get('ENDED'))}"
                    f"</span>", unsafe_allow_html=True)

                try:
                    df_adj = run_query_df(f"""
                        SELECT
                            DIMENSION_ADJ_ID AS ADJUSTMENT_ID,
                            PROCESS_TYPE, ADJUSTMENT_TYPE, ADJUSTMENT_ACTION,
                            COBID, SOURCE_COBID, ENTITY_CODE, BOOK_CODE,
                            RUN_STATUS, RECORD_COUNT,
                            START_DATE, PROCESS_DATE,
                            USERNAME, ERRORMESSAGE
                        FROM ADJUSTMENT_APP.ADJ_HEADER
                        WHERE RUN_LOG_ID = {int(r['RUN_LOG_ID'])}
                        ORDER BY CREATED_DATE
                    """)
                except Exception as e:
                    df_adj = pd.DataFrame()
                    st.warning(f"Could not load run detail: {e}")

                if not df_adj.empty:
                    st.dataframe(
                        df_adj.style.hide(axis="index"),
                        use_container_width=True, height=min(60 + 35 * len(df_adj), 320))

                    for _, a in df_adj[df_adj["ERRORMESSAGE"].notna()].iterrows():
                        st.markdown(
                            f'<div class="overlap-box" style="margin-top:0.4rem">'
                            f'<h4>❌ Adjustment {a.get("ADJUSTMENT_ID","?")} error</h4>'
                            f'<div style="font-size:0.8rem;font-family:monospace">'
                            f'{a["ERRORMESSAGE"]}</div></div>',
                            unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — ACTIVITY FEED  (VW_RECENT_ACTIVITY)
# ══════════════════════════════════════════════════════════════════════════════
with tab_activity:
    section_title("Activity Feed", "🕐")
    st.markdown(
        f"<span style='color:{P['grey_700']};font-size:0.8rem'>"
        f"Every submission and status change, newest first.</span>",
        unsafe_allow_html=True)

    try:
        df_act = run_query_df(f"""
            SELECT
                EVENT_TIME, EVENT_TYPE, CURRENT_STATUS,
                PROCESS_TYPE, ADJUSTMENT_TYPE, ENTITY_CODE, BOOK_CODE,
                ACTOR, ADJ_ID, EVENT_DETAIL
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
        st.dataframe(df_act.style.hide(axis="index"),
                     use_container_width=True, height=560)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — ERRORS  (VW_ERRORS)
# ══════════════════════════════════════════════════════════════════════════════
with tab_errors:
    section_title("Errors", "❌")
    st.markdown(
        f"<span style='color:{P['grey_700']};font-size:0.8rem'>"
        f"Adjustments currently in Failed status.</span>",
        unsafe_allow_html=True)

    try:
        df_err = run_query_df(f"""
            SELECT
                ERROR_TIME, COBID, PROCESS_TYPE, ADJUSTMENT_TYPE,
                ENTITY_CODE, BOOK_CODE, USERNAME, ADJ_ID, REASON, ERRORMESSAGE
            FROM ADJUSTMENT_APP.VW_ERRORS
            WHERE 1=1 {_cob_filter()}{_scope_filter()}
            ORDER BY ERROR_TIME DESC
            LIMIT {int(row_limit)}
        """)
    except Exception as e:
        df_err = pd.DataFrame()
        st.warning(f"Could not load errors: {e}")

    if df_err.empty:
        st.success("No failed adjustments. 🎉")
    else:
        st.markdown(
            f'<div style="background:{P["danger_lt"]};border-left:4px solid {P["danger"]};'
            f'border-radius:8px;padding:0.6rem 1rem;margin-bottom:0.8rem;font-size:0.82rem">'
            f'❌ <strong>{len(df_err)}</strong> failed adjustment(s)</div>',
            unsafe_allow_html=True)
        for _, e in df_err.iterrows():
            scope = str(e.get("PROCESS_TYPE", "") or "")
            scfg = SCOPE_CONFIG.get(scope, {})
            with st.expander(
                f'❌ {scfg.get("icon","")} {scope} · {e.get("ADJUSTMENT_TYPE","")} · '
                f'COB {e.get("COBID","—")} · {_fmt_ts(e.get("ERROR_TIME"))}',
                expanded=False,
            ):
                meta = [
                    ("Entity", str(e.get("ENTITY_CODE", "")) or "—"),
                    ("Book", str(e.get("BOOK_CODE", "")) or "—"),
                    ("User", str(e.get("USERNAME", "")) or "—"),
                    ("ADJ_ID", str(e.get("ADJ_ID", "")) or "—"),
                    ("Reason", str(e.get("REASON", "")) or "—"),
                ]
                st.markdown(
                    "<div class='mcard' style='padding:0.6rem 0.8rem'><table style='width:100%'>"
                    + "".join(
                        f"<tr><td style='color:{P['grey_700']};font-size:0.78rem;"
                        f"padding-right:12px'>{k}</td>"
                        f"<td style='font-size:0.8rem;font-weight:600'>{v}</td></tr>"
                        for k, v in meta if v and v != "—")
                    + "</table></div>",
                    unsafe_allow_html=True)
                if e.get("ERRORMESSAGE"):
                    st.markdown(
                        f'<div class="overlap-box" style="margin-top:0.5rem">'
                        f'<h4>Error message</h4>'
                        f'<div style="font-size:0.82rem;font-family:monospace">'
                        f'{e["ERRORMESSAGE"]}</div></div>',
                        unsafe_allow_html=True)
