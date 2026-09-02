"""
Tasks & Cost — system cost dashboard and pipeline task health
==============================================================
Three panels, COST FIRST (Marcos: the money is the headline):
  • Cost — SERVERLESS TASK USAGE ONLY, attributed exactly per task
    (SERVERLESS_TASK_HISTORY) × the configurable credit price (ADJ_APP_CONFIG
    key COST_PER_CREDIT_USD). The Streamlit/dynamic-table warehouse is
    deliberately EXCLUDED: it is shared with other processes, so its metering
    is not this solution's cost (Marcos, 2026-09). A task moved onto a shared
    warehouse would likewise stop being attributable — keep tasks serverless.
    ACCOUNT_USAGE preferred (long history, ~2h lag); INFORMATION_SCHEMA
    fallback (live, ≤14 days).
  • Scheduled tasks — every task in the schema with its compute model:
    "Serverless (SIZE)" when USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE drives
    it (SHOW TASKS returns an empty warehouse column), or "Warehouse: NAME"
    if a task is ever moved onto a dedicated warehouse.
  • Recent runs — INFORMATION_SCHEMA.TASK_HISTORY (live, last 7 days).
All queries are plain run_query/run_query_df with loud warnings on failure —
no caching, no async (SiS runtime).
"""
import datetime as dt

import streamlit as st
import pandas as pd

st.set_page_config(page_title="Tasks & Cost · MUFG", page_icon="📊",
                   layout="wide", initial_sidebar_state="expanded")

from utils.styles import (inject_css, render_sidebar, section_title, P,
                          kpi_card, render_df_table)
from utils.snowflake_conn import (run_query, run_query_df, current_user_name,
                                  safe_rerun)
import config

inject_css()
render_sidebar()
user = current_user_name()

st.markdown("## Tasks & Cost")
st.markdown(
    f"<span style='color:{P['grey_700']};font-size:0.9rem'>"
    "What the background tasks cost to run, and their health. Only "
    "serverless task credits are counted — they are attributable exactly to "
    "this solution. The query warehouse is shared with other processes, so "
    "its usage is not included here."
    "</span>", unsafe_allow_html=True)
st.markdown("<br/>", unsafe_allow_html=True)


def _esc(v):
    return str(v).replace("\\", "\\\\").replace("'", "''")


# ── Data fetch (no rendering): task list + SOLUTION tag per task ─────────────
_tasks = []
try:
    for r in run_query(f"SHOW TASKS IN SCHEMA {config.DATABASE}.ADJUSTMENT_APP"):
        d = r.as_dict() if hasattr(r, "as_dict") else dict(r)
        d = {str(k).lower(): v for k, v in d.items()}
        _tasks.append(d)
except Exception as ex:
    st.warning(f"Could not list tasks — the database reported: {ex}")

# Live TAG_REFERENCES per task (includes inherited tags).
_tag_map = {}
for t in _tasks:
    try:
        tr = run_query(
            f"""SELECT TAG_VALUE
                FROM TABLE({config.DATABASE}.INFORMATION_SCHEMA.TAG_REFERENCES(
                    '{config.DATABASE}.ADJUSTMENT_APP."{t["name"]}"', 'TASK'))
                WHERE TAG_NAME = 'SOLUTION' LIMIT 1""")
        if tr and tr[0][0]:
            _tag_map[t["name"]] = str(tr[0][0])
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# 1 · COST — serverless task credits × configurable credit price
# ══════════════════════════════════════════════════════════════════════════════
section_title("Cost", "dollar-sign")

# ── Controls: window + tag filter + credit price (persisted) ─────────────────
_cfg_price = None
try:
    _r = run_query("""SELECT CONFIG_VALUE FROM ADJUSTMENT_APP.ADJ_APP_CONFIG
                      WHERE CONFIG_KEY = 'COST_PER_CREDIT_USD'""")
    if _r and _r[0][0] is not None:
        _cfg_price = float(_r[0][0])
except Exception:
    pass

c1, c1b, c2, c3 = st.columns([1, 1, 1, 1.4])
with c1:
    days = st.selectbox("Window", [7, 14, 30, 60, 90], index=2,
                        format_func=lambda d: f"Last {d} days", key="cost_days")
with c1b:
    _tag_values = sorted({v for v in _tag_map.values() if v})
    sel_tag = st.selectbox(
        "Solution tag", ["All"] + _tag_values, index=0, key="cost_tag",
        help="Filters the per-task serverless costs to tasks carrying this "
             "SOLUTION tag value.")
with c2:
    price = st.number_input(
        "Price per credit (USD)", min_value=0.0, step=0.05,
        value=_cfg_price if _cfg_price is not None else 3.00, format="%.2f",
        key="cost_price",
        help="Your contracted Snowflake credit price. Saved for everyone.")
with c3:
    st.markdown("<div style='height:1.75rem'></div>", unsafe_allow_html=True)
    if st.button("Save price", key="cost_save") and price is not None:
        try:
            run_query(f"""
                MERGE INTO ADJUSTMENT_APP.ADJ_APP_CONFIG t
                USING (SELECT 'COST_PER_CREDIT_USD' AS K) s ON t.CONFIG_KEY = s.K
                WHEN MATCHED THEN UPDATE SET CONFIG_VALUE = '{price:.4f}',
                     UPDATED_BY = '{_esc(user)}', UPDATED_AT = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT (CONFIG_KEY, CONFIG_VALUE, DESCRIPTION, UPDATED_BY)
                VALUES ('COST_PER_CREDIT_USD', '{price:.4f}',
                        'Price of one Snowflake credit in USD (Tasks & Cost page).',
                        '{_esc(user)}')""")
            st.success(f"Saved: 1 credit = ${price:,.2f}")
        except Exception as ex:
            st.warning(f"Could not save the price — the database reported: {ex}")

# ── Serverless task credits: exact per-task attribution ──────────────────────
df_srv, srv_src = pd.DataFrame(), ""
try:
    df_srv = run_query_df(f"""
        SELECT TASK_NAME AS TASK, DATE(START_TIME) AS DAY,
               SUM(CREDITS_USED) AS CREDITS
        FROM SNOWFLAKE.ACCOUNT_USAGE.SERVERLESS_TASK_HISTORY
        WHERE DATABASE_NAME = '{config.DATABASE}'
          AND START_TIME >= DATEADD(day, -{int(days)}, CURRENT_TIMESTAMP())
        GROUP BY 1, 2""")
    srv_src = "ACCOUNT_USAGE (complete window, ~2h behind)"
except Exception:
    try:
        df_srv = run_query_df(f"""
            SELECT TASK_NAME AS TASK, DATE(START_TIME) AS DAY,
                   SUM(CREDITS_USED) AS CREDITS
            FROM TABLE({config.DATABASE}.INFORMATION_SCHEMA.SERVERLESS_TASK_HISTORY(
                DATE_RANGE_START => DATEADD(day, -{min(int(days), 14)}, CURRENT_TIMESTAMP())))
            GROUP BY 1, 2""")
        srv_src = ("INFORMATION_SCHEMA (live; capped at 14 days — grant "
                   "IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE for longer windows)")
    except Exception as ex:
        st.warning(f"Could not load serverless task credits — the database "
                   f"reported: {ex}")

# ── Tag filter: keep only serverless tasks carrying the selected tag ─────────
if sel_tag != "All" and not df_srv.empty:
    _tagged = {name for name, v in _tag_map.items() if v == sel_tag}
    df_srv = df_srv[df_srv["TASK"].isin(_tagged)]
    st.caption(f"Costs filtered to SOLUTION = **{sel_tag}** "
               f"({len(_tagged)} task(s)).")

# ── KPIs — task usage only ──────────────────────────────────────────────────
srv_credits = float(pd.to_numeric(df_srv.get("CREDITS"), errors="coerce").sum()) if not df_srv.empty else 0.0
total_cost = srv_credits * float(price or 0)
monthly = total_cost / int(days) * 30.44 if days else 0.0
_top_task, _top_cost = "—", ""
if not df_srv.empty:
    _g = df_srv.groupby("TASK")["CREDITS"].sum()
    _top_task = str(_g.idxmax()).replace("TASK_PROCESS_", "").replace("TASK_", "")
    _top_cost = f"${_g.max() * float(price or 0):,.2f} in the window"

st.markdown("<br/>", unsafe_allow_html=True)
m1, m2, m3, m4 = st.columns(4)
m1.markdown(kpi_card("Task credits", f"{srv_credits:,.3f}",
                     f"serverless, last {days}d"), unsafe_allow_html=True)
m2.markdown(kpi_card("Task cost", f"${total_cost:,.2f}",
                     f"at ${price:,.2f}/credit"), unsafe_allow_html=True)
m3.markdown(kpi_card("Monthly run-rate", f"${monthly:,.2f}",
                     "extrapolated from the window"), unsafe_allow_html=True)
m4.markdown(kpi_card("Biggest consumer", _top_task, _top_cost),
            unsafe_allow_html=True)
st.markdown("<br/>", unsafe_allow_html=True)

# ── Per-task breakdown + daily trend ─────────────────────────────────────────
if df_srv.empty:
    st.caption("No serverless task usage in the window.")
else:
    g = (df_srv.groupby("TASK", as_index=False)["CREDITS"].sum()
         .sort_values("CREDITS", ascending=False))
    g["COST_USD"] = g["CREDITS"] * float(price or 0)
    g["SHARE"] = g["CREDITS"] / g["CREDITS"].sum()
    render_df_table(g, formats={"CREDITS": "{:,.4f}", "COST_USD": "${:,.2f}",
                                "SHARE": "{:.1%}"})
    if srv_src:
        st.caption(f"Source: {srv_src}")

    import altair as alt
    _short = lambda c: str(c).replace("TASK_PROCESS_", "").replace("TASK_", "")

    # ── Daily credits per task ───────────────────────────────────────────────
    # Altair temporal X with an explicit date format — st.bar_chart's auto
    # axis showed weekday/month labels that read strangely across many months.
    st.markdown("<br/>", unsafe_allow_html=True)
    st.markdown("**Daily credits per task**")
    _dl = df_srv.copy()
    _dl["Task"] = _dl["TASK"].map(_short)
    _dl["Date"] = pd.to_datetime(_dl["DAY"], errors="coerce")
    _dl["Credits"] = pd.to_numeric(_dl["CREDITS"], errors="coerce")
    _daily_chart = (
        alt.Chart(_dl.dropna(subset=["Date"]))
        .mark_bar()
        .encode(
            x=alt.X("Date:T", title="Date",
                    axis=alt.Axis(format="%d %b %Y", labelAngle=-40,
                                  labelFontSize=12, titleFontSize=12)),
            y=alt.Y("sum(Credits):Q", title="Credits",
                    axis=alt.Axis(labelFontSize=12, titleFontSize=12)),
            color=alt.Color("Task:N", title="Task"),
            tooltip=["Date:T", "Task:N", alt.Tooltip("sum(Credits):Q", format=",.3f")],
        )
        .properties(height=300))
    st.altair_chart(_daily_chart, use_container_width=True)

    # ── Monthly cost per task ────────────────────────────────────────────────
    st.markdown("<br/>", unsafe_allow_html=True)
    st.markdown(f"**Monthly cost per task** (credits × ${price:,.2f})")
    _ml = df_srv.copy()
    _ml["Task"] = _ml["TASK"].map(_short)
    _ml["Month"] = pd.to_datetime(_ml["DAY"], errors="coerce").dt.strftime("%Y-%m")
    _ml["Cost"] = pd.to_numeric(_ml["CREDITS"], errors="coerce") * float(price or 0)
    _ml = _ml.dropna(subset=["Month"])
    _monthly_chart = (
        alt.Chart(_ml)
        .mark_bar()
        .encode(
            x=alt.X("Month:N", title="Month", sort="ascending",
                    axis=alt.Axis(labelAngle=0, labelFontSize=14,
                                  titleFontSize=13, labelPadding=6)),
            y=alt.Y("sum(Cost):Q", title="Cost (USD)",
                    axis=alt.Axis(labelFontSize=12, titleFontSize=13, format="$,.0f")),
            color=alt.Color("Task:N", title="Task"),
            tooltip=["Month:N", "Task:N", alt.Tooltip("sum(Cost):Q", format="$,.2f")],
        )
        .properties(height=340))
    st.altair_chart(_monthly_chart, use_container_width=True)
    if _ml["Month"].nunique() <= 1:
        st.caption("Only one month in the current window — widen the window "
                   "(top of this section) to compare months.")

st.caption("Why no warehouse figures: the Streamlit query / dynamic-table "
           "warehouse is shared with other processes, so its metering is not "
           "attributable to this solution and is deliberately excluded. If a "
           "task is ever moved from serverless onto a shared warehouse, its "
           "cost disappears from this page for the same reason — keep the "
           "tasks serverless to keep their cost visible.")

st.markdown("<br/>", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# 2 · SCHEDULED TASKS — compute model per task
# ══════════════════════════════════════════════════════════════════════════════
section_title("Scheduled Tasks", "clock")

if _tasks:
    rows_out = []
    for t in _tasks:
        wh = (t.get("warehouse") or "").strip()
        if wh:
            compute = f"Warehouse: {wh}"
        else:
            # Serverless — the size is a task parameter, not a SHOW TASKS col.
            size = "auto"
            try:
                p = run_query(
                    "SHOW PARAMETERS LIKE 'USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE' "
                    f"IN TASK {config.DATABASE}.ADJUSTMENT_APP.\"{t['name']}\"")
                if p:
                    pd_ = p[0].as_dict() if hasattr(p[0], "as_dict") else dict(p[0])
                    size = {str(k).lower(): v for k, v in pd_.items()}.get("value") or "auto"
            except Exception:
                pass
            compute = f"Serverless ({size})"
        rows_out.append({
            "TASK": t.get("name"),
            "SOLUTION": _tag_map.get(t.get("name"), "—"),
            "STATE": t.get("state"),
            "SCHEDULE": t.get("schedule"),
            "COMPUTE": compute,
            "COMMENT": (t.get("comment") or "")[:90],
        })
    df_tasks = pd.DataFrame(rows_out)
    n_susp = int((df_tasks["STATE"].str.upper() != "STARTED").sum())
    if n_susp:
        st.warning(f"**{n_susp} task(s) are not running** — the pipeline or "
                   "sign-off sync is stopped. Resume them or redeploy 06_tasks.sql.")
    render_df_table(df_tasks,
                    highlight=lambda r: str(r.get("STATE", "")).upper() != "STARTED")
else:
    st.caption("No tasks found in the schema.")

st.markdown("<br/>", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# 3 · RECENT RUNS — live task history
# ══════════════════════════════════════════════════════════════════════════════
section_title("Recent Runs", "activity")

df_hist = pd.DataFrame()
try:
    df_hist = run_query_df(f"""
        SELECT NAME AS TASK, STATE,
               CONVERT_TIMEZONE('Europe/London', SCHEDULED_TIME)::TIMESTAMP_NTZ AS SCHEDULED,
               CONVERT_TIMEZONE('Europe/London', COMPLETED_TIME)::TIMESTAMP_NTZ AS COMPLETED,
               ROUND(TIMESTAMPDIFF('millisecond', QUERY_START_TIME, COMPLETED_TIME) / 1000, 1)
                   AS DURATION_S,
               LEFT(COALESCE(ERROR_MESSAGE, ''), 160) AS ERROR
        FROM TABLE({config.DATABASE}.INFORMATION_SCHEMA.TASK_HISTORY(RESULT_LIMIT => 200))
        WHERE DATABASE_NAME = '{config.DATABASE}'
          AND STATE <> 'SKIPPED'
        ORDER BY SCHEDULED_TIME DESC
    """)
except Exception as ex:
    st.warning(f"Could not load task history — the database reported: {ex}")

if not df_hist.empty:
    _su = df_hist["STATE"].astype(str).str.upper()
    _last24 = pd.Timestamp.now() - pd.Timedelta(hours=24)
    _recent = df_hist[pd.to_datetime(df_hist["SCHEDULED"], errors="coerce") >= _last24]
    k1, k2, k3, k4 = st.columns(4)
    k1.markdown(kpi_card("Runs (24h)", len(_recent)), unsafe_allow_html=True)
    _fails = int((_recent["STATE"].astype(str).str.upper() == "FAILED").sum())
    k2.markdown(kpi_card("Failed (24h)", _fails,
                         variant="danger" if _fails else "primary"),
                unsafe_allow_html=True)
    k3.markdown(kpi_card("Executing now", int((_su == "EXECUTING").sum())),
                unsafe_allow_html=True)
    _avg = pd.to_numeric(_recent["DURATION_S"], errors="coerce").mean()
    k4.markdown(kpi_card("Avg duration (24h)",
                         f"{_avg:.1f}s" if pd.notna(_avg) else "—"),
                unsafe_allow_html=True)
    st.markdown("<br/>", unsafe_allow_html=True)
    render_df_table(df_hist, max_rows=100,
                    highlight=lambda r: str(r.get("STATE", "")).upper() == "FAILED")
    st.caption("Times shown in London. Source: INFORMATION_SCHEMA.TASK_HISTORY "
               "(live, last 7 days).")
else:
    st.caption("No task runs found.")
