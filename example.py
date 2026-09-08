"""Publish Readiness cockpit — the single view a BAU operator uses to answer:
"Which publish groups have NOT had their Control-M condition created yet, and
exactly which input file(s) are we still waiting on?"

A publish condition (row in BATCH.BATCH_CONDITIONS) is created for a
BATCH_MAPPING_CODE once every input SCHEDULED FOR THE SELECTED COB has staged
successfully. BATCH_ACTION_DAILY defines that day's expected membership, so a
weekly file (for example, Friday-only) is not counted as missing on other days.
BATCH.VW_BATCH_CONDITIONS remains the source of truth for publish eligibility.

Ported from the RAPTOR BAU Operations app.
"""
import streamlit as st
import pandas as pd
import altair as alt

from common import ui
from common.session import get_session, get_cobid

ui.page_header(
    "Publish Readiness",
    "A publish condition is created for a group once every input file mapped to "
    "it has staged successfully. Which groups are published, which are waiting, "
    "and exactly what is missing.",
)

session = get_session()
cobid = get_cobid()

STATE_COLORS = {
    "PUBLISHED": "#21c354",       # condition created
    "READY": "#faca2b",           # all inputs in, condition not yet created (anomaly)
    "WAITING": "#ff4b4b",         # still missing input file(s)
    "NOT_SCHEDULED": "#8a8a8a",   # no members expected today
}


@st.cache_data(ttl=60)
def get_adhoc_activity(_session, cobid: int) -> pd.DataFrame:
    return _session.sql(f"""
        WITH d AS (
            SELECT BATCH_ACTION_NAME,
                   MAX(BATCH_RESULT) AS BATCH_RESULT,
                   MAX(START_TIME) AS START_TIME,
                   MAX(END_TIME) AS END_TIME
            FROM PROD_RAPTOR.BATCH.BATCH_ACTION_DAILY
            WHERE COBID = {cobid}
            GROUP BY 1
        )
        SELECT DISTINCT
            m.BATCH_MAPPING_CODE,
            m.BATCH_ACTION_NAME,
            d.BATCH_RESULT,
            d.START_TIME,
            d.END_TIME
        FROM d
        JOIN PROD_RAPTOR.BATCH.BATCH_ACTION_MAPPING m
          ON m.BATCH_ACTION_NAME = d.BATCH_ACTION_NAME
        WHERE m.BATCH_MAPPING_CODE ILIKE '%ADHOC%'
        ORDER BY d.END_TIME DESC, m.BATCH_MAPPING_CODE, m.BATCH_ACTION_NAME
    """).to_pandas()


@st.cache_data(ttl=60)
def get_groups(_session, cobid: int) -> pd.DataFrame:
    return _session.sql(f"""
        WITH d AS (
            SELECT BATCH_ACTION_NAME,
                   MAX(IFF(UPPER(BATCH_RESULT) = 'SUCCESS', 1, 0)) AS DONE,
                   MAX(END_TIME) AS END_TIME
            FROM PROD_RAPTOR.BATCH.BATCH_ACTION_DAILY
            WHERE COBID = {cobid}
            GROUP BY 1
        ),
        daily_groups AS (
            SELECT DISTINCT m.BATCH_MAPPING_CODE
            FROM PROD_RAPTOR.BATCH.BATCH_ACTION_MAPPING m
                        WHERE m.BATCH_MAPPING_CODE NOT ILIKE '%ADHOC%'
                            AND NOT EXISTS (
                SELECT 1
                FROM PROD_RAPTOR.BATCH.BATCH_ACTION_MAPPING required
                WHERE required.BATCH_MAPPING_CODE = m.BATCH_MAPPING_CODE
                  AND NOT EXISTS (
                      SELECT 1 FROM d
                      WHERE d.BATCH_ACTION_NAME = required.BATCH_ACTION_NAME
                  )
            )
        ),
        mem AS (
            SELECT m.BATCH_MAPPING_CODE,
                   m.BATCH_ACTION_NAME,
                                     d.DONE,
                                     1 AS IN_DAILY
                        FROM d
                        JOIN PROD_RAPTOR.BATCH.BATCH_ACTION_MAPPING m
                            ON m.BATCH_ACTION_NAME = d.BATCH_ACTION_NAME
                        JOIN daily_groups dg
                            ON dg.BATCH_MAPPING_CODE = m.BATCH_MAPPING_CODE
        ),
        grp AS (
            SELECT BATCH_MAPPING_CODE,
                   COUNT(*) AS TOTAL_MEMBERS,
                   SUM(DONE) AS DONE_MEMBERS,
                   SUM(IN_DAILY) AS IN_DAILY_MEMBERS,
                   ARRAY_TO_STRING(
                       ARRAY_COMPACT(
                           ARRAY_AGG(IFF(DONE = 0, BATCH_ACTION_NAME, NULL))
                           WITHIN GROUP (ORDER BY BATCH_ACTION_NAME)
                       ), '\\n') AS MISSING_FILES
            FROM mem
            GROUP BY 1
        ),
        elig AS (
            SELECT DISTINCT BATCH_MAPPING_CODE
            FROM PROD_RAPTOR.BATCH.VW_BATCH_CONDITIONS
            WHERE COBID = {cobid}
        ),
        crt AS (
            SELECT BATCH_MAPPING_CODE,
                   MAX(CONDITION_NAME) AS CONDITION_NAME,
                   MAX(APPLIED_FLAG::INT) AS APPLIED,
                   MAX(BATCH_CONDITION_TIME) AS CONDITION_TIME
            FROM PROD_RAPTOR.BATCH.BATCH_CONDITIONS
            WHERE COBID = {cobid}
            GROUP BY 1
        )
        SELECT
            g.BATCH_MAPPING_CODE,
            SPLIT_PART(g.BATCH_MAPPING_CODE, '_', 1) AS FAMILY,
            g.TOTAL_MEMBERS,
            g.DONE_MEMBERS,
            (g.TOTAL_MEMBERS - g.DONE_MEMBERS) AS PENDING_MEMBERS,
            g.IN_DAILY_MEMBERS,
            g.MISSING_FILES,
            crt.CONDITION_NAME,
            crt.CONDITION_TIME,
            CASE
                WHEN crt.BATCH_MAPPING_CODE IS NOT NULL THEN 'PUBLISHED'
                WHEN elig.BATCH_MAPPING_CODE IS NOT NULL THEN 'READY'
                WHEN g.IN_DAILY_MEMBERS = 0 THEN 'NOT_SCHEDULED'
                ELSE 'WAITING'
            END AS STATE
        FROM grp g
        LEFT JOIN elig ON elig.BATCH_MAPPING_CODE = g.BATCH_MAPPING_CODE
        LEFT JOIN crt ON crt.BATCH_MAPPING_CODE = g.BATCH_MAPPING_CODE
        ORDER BY g.BATCH_MAPPING_CODE
    """).to_pandas()


@st.cache_data(ttl=60)
def get_blockers(_session, cobid: int) -> pd.DataFrame:
    return _session.sql(f"""
        WITH d AS (
            SELECT BATCH_ACTION_NAME,
                   MAX(IFF(UPPER(BATCH_RESULT) = 'SUCCESS', 1, 0)) AS DONE
            FROM PROD_RAPTOR.BATCH.BATCH_ACTION_DAILY
            WHERE COBID = {cobid}
            GROUP BY 1
        ),
        daily_groups AS (
            SELECT DISTINCT m.BATCH_MAPPING_CODE
            FROM PROD_RAPTOR.BATCH.BATCH_ACTION_MAPPING m
                        WHERE m.BATCH_MAPPING_CODE NOT ILIKE '%ADHOC%'
                            AND NOT EXISTS (
                SELECT 1
                FROM PROD_RAPTOR.BATCH.BATCH_ACTION_MAPPING required
                WHERE required.BATCH_MAPPING_CODE = m.BATCH_MAPPING_CODE
                  AND NOT EXISTS (
                      SELECT 1 FROM d
                      WHERE d.BATCH_ACTION_NAME = required.BATCH_ACTION_NAME
                  )
            )
        ),
        mem AS (
            SELECT m.BATCH_MAPPING_CODE,
                   m.BATCH_ACTION_NAME,
                                     d.DONE,
                                     1 AS IN_DAILY
                        FROM d
                        JOIN PROD_RAPTOR.BATCH.BATCH_ACTION_MAPPING m
                            ON m.BATCH_ACTION_NAME = d.BATCH_ACTION_NAME
                        JOIN daily_groups dg
                            ON dg.BATCH_MAPPING_CODE = m.BATCH_MAPPING_CODE
        ),
        sched AS (
            SELECT BATCH_MAPPING_CODE FROM mem GROUP BY 1 HAVING SUM(IN_DAILY) > 0
        ),
        crt AS (
            SELECT DISTINCT BATCH_MAPPING_CODE
            FROM PROD_RAPTOR.BATCH.BATCH_CONDITIONS
            WHERE COBID = {cobid}
        ),
        r AS (
            SELECT BATCH_ACTION_NAME,
                   MAX(STAGE_ME_STATUS) AS STAGE,
                   MAX(SOURCE_SYSTEM_CODE) AS SRC
            FROM PROD_RAPTOR.RAVEN.VW_LOG_STAGE_ME_STATUS_REPORTING
            WHERE RAVEN_COBID = {cobid}
            GROUP BY 1
        )
        SELECT
            mem.BATCH_ACTION_NAME AS MISSING_FILE,
            COALESCE(r.STAGE, 'NOT ARRIVED') AS FILE_STATUS,
            r.SRC AS SOURCE_SYSTEM,
            COUNT(DISTINCT mem.BATCH_MAPPING_CODE) AS GROUPS_BLOCKED,
            ARRAY_TO_STRING(
                ARRAY_AGG(DISTINCT mem.BATCH_MAPPING_CODE)
                WITHIN GROUP (ORDER BY mem.BATCH_MAPPING_CODE), ', ') AS GROUPS
        FROM mem
        JOIN sched ON sched.BATCH_MAPPING_CODE = mem.BATCH_MAPPING_CODE
        LEFT JOIN r ON r.BATCH_ACTION_NAME = mem.BATCH_ACTION_NAME
        WHERE mem.DONE = 0
          AND mem.BATCH_MAPPING_CODE NOT IN (SELECT BATCH_MAPPING_CODE FROM crt)
        GROUP BY 1, 2, 3
        ORDER BY GROUPS_BLOCKED DESC, MISSING_FILE
    """).to_pandas()


@st.cache_data(ttl=60)
def get_members(_session, cobid: int, code: str) -> pd.DataFrame:
    return _session.sql(f"""
        WITH d AS (
            SELECT BATCH_ACTION_NAME,
                   MAX(BATCH_RESULT) AS BATCH_RESULT,
                   MAX(START_TIME) AS START_TIME,
                   MAX(END_TIME) AS END_TIME
            FROM PROD_RAPTOR.BATCH.BATCH_ACTION_DAILY
            WHERE COBID = {cobid}
            GROUP BY 1
        ),
        daily_groups AS (
            SELECT DISTINCT m.BATCH_MAPPING_CODE
            FROM PROD_RAPTOR.BATCH.BATCH_ACTION_MAPPING m
                        WHERE m.BATCH_MAPPING_CODE NOT ILIKE '%ADHOC%'
                            AND NOT EXISTS (
                SELECT 1
                FROM PROD_RAPTOR.BATCH.BATCH_ACTION_MAPPING required
                WHERE required.BATCH_MAPPING_CODE = m.BATCH_MAPPING_CODE
                  AND NOT EXISTS (
                      SELECT 1 FROM d
                      WHERE d.BATCH_ACTION_NAME = required.BATCH_ACTION_NAME
                  )
            )
        ),
        r AS (
            SELECT BATCH_ACTION_NAME,
                   MAX(STAGE_ME_STATUS) AS STAGE,
                   MAX(FILE_NAME) AS FILE_NAME,
                   MAX(SOURCE_SYSTEM_CODE) AS SRC,
                   MAX(REGION) AS REGION,
                   MAX(END_TIMESTAMP) AS FILE_END,
                   MAX(EXPECTED_STAGING_TIMESTAMP) AS EXPECTED_END
            FROM PROD_RAPTOR.RAVEN.VW_LOG_STAGE_ME_STATUS_REPORTING
            WHERE RAVEN_COBID = {cobid}
            GROUP BY 1
        )
        SELECT
            m.BATCH_ACTION_NAME,
            IFF(UPPER(d.BATCH_RESULT) = 'SUCCESS', 1, 0) AS DONE,
            COALESCE(d.BATCH_RESULT,
                     IFF(d.BATCH_ACTION_NAME IS NULL, 'not scheduled', 'pending')) AS DAILY_RESULT,
            COALESCE(r.STAGE, 'NOT ARRIVED') AS FILE_STATUS,
            r.FILE_NAME,
            r.SRC AS SOURCE_SYSTEM,
            r.REGION,
            d.START_TIME,
            d.END_TIME,
            r.FILE_END,
            r.EXPECTED_END
                FROM d
                JOIN PROD_RAPTOR.BATCH.BATCH_ACTION_MAPPING m
                    ON m.BATCH_ACTION_NAME = d.BATCH_ACTION_NAME
                JOIN daily_groups dg
                    ON dg.BATCH_MAPPING_CODE = m.BATCH_MAPPING_CODE
        LEFT JOIN r ON r.BATCH_ACTION_NAME = m.BATCH_ACTION_NAME
        WHERE m.BATCH_MAPPING_CODE = '{code}'
        ORDER BY DONE, m.BATCH_ACTION_NAME
    """).to_pandas()


def color_state(val):
    c = STATE_COLORS.get(val)
    if c:
        text = "black" if val == "READY" else "white"
        return f"background-color: {c}; color: {text}"
    return ""


def color_daily(val):
    if val is None:
        return ""
    u = str(val).upper()
    if u == "SUCCESS":
        return "background-color: #21c354; color: white"
    if u in ("FAILED", "ERROR"):
        return "background-color: #ff4b4b; color: white"
    if u in ("PENDING", "NOT SCHEDULED", "NOT ARRIVED"):
        return "background-color: #faca2b; color: black"
    return ""


# ---------------------------------------------------------------- Page body ---
with st.spinner("Loading publish readiness..."):
    df = get_groups(session, cobid)

if df.empty:
    st.warning("No batch mapping data for this COB date.")
    st.stop()

# Focus on groups that are actually in play today (exclude not-scheduled from KPIs)
active = df[df["STATE"] != "NOT_SCHEDULED"]
published = int((df["STATE"] == "PUBLISHED").sum())
waiting = int((df["STATE"] == "WAITING").sum())
ready = int((df["STATE"] == "READY").sum())
total_active = len(active)
pct = (published / total_active * 100) if total_active else 0.0

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Groups In Play", total_active)
k2.metric("Published", published, help="Condition created in BATCH_CONDITIONS")
k3.metric("Waiting on Files", waiting, delta=f"-{waiting}" if waiting else None,
          delta_color="inverse")
k4.metric("Ready (no condition)", ready,
          help="All inputs staged but condition not yet created")
k5.metric("Published %", f"{pct:.0f}%")

adhoc = get_adhoc_activity(session, cobid)
with st.expander(
    f"Ad hoc activity — {adhoc['BATCH_MAPPING_CODE'].nunique() if not adhoc.empty else 0} groups",
    expanded=False,
):
    st.caption(
        "Mapping codes containing AdHoc are shown for awareness when they appear "
        "in BATCH_ACTION_DAILY, but are excluded from Groups In Play, readiness, "
        "blockers, and Published %."
    )
    if adhoc.empty:
        st.info("No ad hoc publish activity for the selected COB date.")
    else:
        st.dataframe(
            adhoc.style.map(color_daily, subset=["BATCH_RESULT"]),
            hide_index=True,
            use_container_width=True,
            column_config={
                "BATCH_MAPPING_CODE": "Ad Hoc Group",
                "BATCH_ACTION_NAME": "Input (Batch Action)",
                "BATCH_RESULT": "Daily Result",
                "START_TIME": "Start",
                "END_TIME": "End",
            },
        )

st.markdown("---")

# ---------------------------------------------------- State chart + blockers ---
c_left, c_right = st.columns([1, 2])

with c_left:
    st.subheader("Status breakdown")
    counts = (
        active["STATE"].value_counts().rename_axis("STATE").reset_index(name="COUNT")
    )
    donut = (
        alt.Chart(counts)
        .mark_arc(innerRadius=60)
        .encode(
            theta="COUNT:Q",
            color=alt.Color(
                "STATE:N",
                scale=alt.Scale(
                    domain=list(STATE_COLORS.keys()),
                    range=list(STATE_COLORS.values()),
                ),
                legend=alt.Legend(title="State"),
            ),
            tooltip=["STATE", "COUNT"],
        )
        .properties(height=260)
    )
    st.altair_chart(donut, use_container_width=True)

with c_right:
    st.subheader("Top blocking files")
    st.caption(
        "One missing file often blocks several groups — clear these first for "
        "maximum publishes unlocked."
    )
    df_block = get_blockers(session, cobid)
    if df_block.empty:
        st.success("No blocking files — every group in play has all inputs staged.")
    else:
        st.dataframe(
            df_block.style.map(color_daily, subset=["FILE_STATUS"]),
            hide_index=True,
            use_container_width=True,
            height=300,
            column_config={
                "MISSING_FILE": "Missing File",
                "FILE_STATUS": "File Status",
                "SOURCE_SYSTEM": "Source",
                "GROUPS_BLOCKED": st.column_config.NumberColumn("Groups Blocked"),
                "GROUPS": "Blocked Groups",
            },
        )

st.markdown("---")

# ------------------------------------------------------------ Family summary ---
st.subheader("By publish family")
fam = (
    active.groupby(["FAMILY", "STATE"]).size().reset_index(name="COUNT")
)
bar = (
    alt.Chart(fam)
    .mark_bar()
    .encode(
        x=alt.X("COUNT:Q", title="Groups"),
        y=alt.Y("FAMILY:N", sort="-x", title=None),
        color=alt.Color(
            "STATE:N",
            scale=alt.Scale(
                domain=list(STATE_COLORS.keys()),
                range=list(STATE_COLORS.values()),
            ),
            legend=alt.Legend(title="State"),
        ),
        tooltip=["FAMILY", "STATE", "COUNT"],
    )
    .properties(height=max(200, fam["FAMILY"].nunique() * 26))
)
st.altair_chart(bar, use_container_width=True)

st.markdown("---")

# ---------------------------------------------------------- Group explorer ---
st.subheader("Group explorer")

with st.expander("Filters", expanded=True):
    fc1, fc2, fc3 = st.columns(3)
    with fc1:
        states = st.multiselect(
            "State", options=list(STATE_COLORS.keys()),
            default=["WAITING", "READY"],
        )
    with fc2:
        fams = st.multiselect(
            "Family", options=sorted(df["FAMILY"].unique()), default=[],
        )
    with fc3:
        search = st.text_input("Search group / missing file", "")

fdf = df.copy()
if states:
    fdf = fdf[fdf["STATE"].isin(states)]
if fams:
    fdf = fdf[fdf["FAMILY"].isin(fams)]
if search:
    s = search.upper()
    fdf = fdf[
        fdf["BATCH_MAPPING_CODE"].str.upper().str.contains(s, na=False)
        | fdf["MISSING_FILES"].fillna("").str.upper().str.contains(s, na=False)
    ]

st.caption(f"{len(fdf)} of {len(df)} groups")
st.dataframe(
    fdf[[
        "STATE", "BATCH_MAPPING_CODE", "FAMILY", "DONE_MEMBERS", "TOTAL_MEMBERS",
        "PENDING_MEMBERS", "MISSING_FILES", "CONDITION_NAME", "CONDITION_TIME",
    ]].style.map(color_state, subset=["STATE"]),
    hide_index=True,
    use_container_width=True,
    height=360,
    column_config={
        "STATE": "State",
        "BATCH_MAPPING_CODE": "Group (Mapping Code)",
        "FAMILY": "Family",
        "DONE_MEMBERS": st.column_config.NumberColumn("Done"),
        "TOTAL_MEMBERS": st.column_config.NumberColumn("Total"),
        "PENDING_MEMBERS": st.column_config.NumberColumn("Missing"),
        "MISSING_FILES": "Missing Files",
        "CONDITION_NAME": "Condition",
        "CONDITION_TIME": "Condition Created",
    },
)

# --------------------------------------------------------------- Drill-down ---
st.subheader("Group drill-down")
codes = fdf["BATCH_MAPPING_CODE"].tolist() or df["BATCH_MAPPING_CODE"].tolist()
sel = st.selectbox("Select a group", options=codes)

if sel:
    row = df[df["BATCH_MAPPING_CODE"] == sel].iloc[0]
    state = row["STATE"]
    color = STATE_COLORS.get(state, "#8a8a8a")
    st.markdown(
        f"<span style='background:{color};color:white;padding:0.2rem 0.7rem;"
        f"border-radius:12px;font-weight:600'>{state}</span> "
        f"&nbsp;<b>{sel}</b> &nbsp;—&nbsp; "
        f"{int(row['DONE_MEMBERS'])}/{int(row['TOTAL_MEMBERS'])} inputs staged",
        unsafe_allow_html=True,
    )

    if state == "PUBLISHED":
        st.success(
            f"Condition **{row['CONDITION_NAME']}** created at "
            f"{row['CONDITION_TIME']}."
        )
    elif state == "READY":
        st.warning("All inputs staged — condition is eligible but not yet created.")
    elif state == "WAITING":
        missing = [m for m in str(row["MISSING_FILES"]).split("\n") if m]
        st.error(
            f"Missing {len(missing)} input(s) to create the condition:\n\n"
            + "\n".join(f"- `{m}`" for m in missing)
        )
    else:
        st.info("This group has no members expected on this COB date.")

    df_mem = get_members(session, cobid, sel)
    st.dataframe(
        df_mem.drop(columns=["DONE"]).style
        .map(color_daily, subset=["DAILY_RESULT", "FILE_STATUS"]),
        hide_index=True,
        use_container_width=True,
        column_config={
            "BATCH_ACTION_NAME": "Input (Batch Action)",
            "DAILY_RESULT": "Daily Result",
            "FILE_STATUS": "File Status",
            "FILE_NAME": "File",
            "SOURCE_SYSTEM": "Source",
            "REGION": "Region",
            "START_TIME": "Start",
            "END_TIME": "End",
            "FILE_END": "File Arrived",
            "EXPECTED_END": "Expected By",
        },
    )
