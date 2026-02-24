"""
Page 4 — Dashboard
Visual impact analysis with charts, KPIs, and before/after comparison.
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import streamlit as st
import pandas as pd
from data.state_manager import (
    init_state, get_adj_headers, get_adj_line_items,
    get_fact_table, get_fact_adjusted,
)

init_state()

st.title("📊 Impact Dashboard")

headers = get_adj_headers()
lines = get_adj_line_items()
fact_orig = get_fact_table()
fact_adj = get_fact_adjusted()

# ── KPI Section ──────────────────────────────────────────────────────
st.subheader("Key Metrics")

applied = headers[headers["ADJ_STATUS"] == "APPLIED"]
adjusted_rows = fact_adj[fact_adj["IS_ADJUSTED"] == True]

k1, k2, k3, k4, k5 = st.columns(5)
with k1:
    st.metric("Total Adjustments", len(headers))
with k2:
    st.metric("Applied", len(applied))
with k3:
    st.metric("Pending", len(headers[headers["ADJ_STATUS"] == "PENDING_APPROVAL"]))
with k4:
    total_impact = applied["TOTAL_DELTA_AMOUNT"].sum() if not applied.empty else 0
    st.metric("Net Impact", f"${total_impact:,.0f}")
with k5:
    st.metric("Rows Modified", len(adjusted_rows))

st.divider()

# ── Impact by Type ───────────────────────────────────────────────────
col_chart1, col_chart2 = st.columns(2)

with col_chart1:
    st.subheader("Impact by Adjustment Type")
    if not applied.empty:
        by_type = applied.groupby("ADJ_TYPE").agg(
            Count=("ADJ_ID", "count"),
            Total_Delta=("TOTAL_DELTA_AMOUNT", "sum"),
            Avg_Rows=("AFFECTED_ROWS", "mean"),
        ).reset_index()
        by_type["Total_Delta"] = by_type["Total_Delta"].round(2)
        by_type["Avg_Rows"] = by_type["Avg_Rows"].round(0)

        st.bar_chart(
            by_type.set_index("ADJ_TYPE")["Total_Delta"],
            height=300,
        )
        st.dataframe(by_type, use_container_width=True, hide_index=True)
    else:
        st.info("No applied adjustments yet.")

with col_chart2:
    st.subheader("Impact by Entity")
    applied_ids = applied["ADJ_ID"].tolist() if not applied.empty else []
    applied_lines = lines[lines["ADJ_ID"].isin(applied_ids)]

    if not applied_lines.empty:
        by_entity = applied_lines.groupby("ENTITY_KEY").agg(
            Rows=("LINE_ID", "count"),
            Amount_Delta=("AMOUNT_DELTA", "sum"),
        ).reset_index()
        by_entity["Amount_Delta"] = by_entity["Amount_Delta"].round(2)

        st.bar_chart(
            by_entity.set_index("ENTITY_KEY")["Amount_Delta"],
            height=300,
        )
        st.dataframe(by_entity, use_container_width=True, hide_index=True)
    else:
        st.info("No applied line items.")

st.divider()

# ── Before / After Comparison ────────────────────────────────────────
st.subheader("Before vs. After Comparison")
st.caption("This mirrors what the **Dynamic Table** MART.FACT_ADJUSTED provides in Snowflake.")

if len(adjusted_rows) > 0:
    # Merge original and adjusted
    compare = fact_orig[["FACT_ID", "ENTITY_KEY", "PRODUCT_KEY", "BUSINESS_DATE",
                          "AMOUNT", "QUANTITY", "NOTIONAL"]].merge(
        fact_adj[fact_adj["IS_ADJUSTED"] == True][
            ["FACT_ID", "AMOUNT", "QUANTITY", "NOTIONAL"]
        ],
        on="FACT_ID",
        suffixes=("_BEFORE", "_AFTER"),
    )
    compare["AMOUNT_DELTA"] = compare["AMOUNT_AFTER"] - compare["AMOUNT_BEFORE"]

    # Display
    tab_table, tab_chart_ba = st.tabs(["📋 Table", "📈 Chart"])

    with tab_table:
        display = compare.copy()
        for c in ["AMOUNT_BEFORE", "AMOUNT_AFTER", "AMOUNT_DELTA"]:
            display[c] = display[c].apply(lambda x: f"${x:,.2f}")
        st.dataframe(display, use_container_width=True, hide_index=True)

    with tab_chart_ba:
        chart_data = compare.groupby("ENTITY_KEY")[["AMOUNT_BEFORE", "AMOUNT_AFTER"]].sum().reset_index()
        chart_data.columns = ["Entity", "Before", "After"]
        st.bar_chart(chart_data.set_index("Entity"), height=400)
else:
    st.info("No adjusted rows. Apply an adjustment to see before/after comparison.")

st.divider()

# ── Adjustment Activity ──────────────────────────────────────────────
st.subheader("Adjustment Activity")

col_act1, col_act2 = st.columns(2)

with col_act1:
    st.markdown("**Status Distribution**")
    status_counts = headers["ADJ_STATUS"].value_counts().reset_index()
    status_counts.columns = ["Status", "Count"]
    st.bar_chart(status_counts.set_index("Status"), height=250)

with col_act2:
    st.markdown("**Top Adjusters**")
    user_counts = headers["CREATED_BY"].value_counts().reset_index()
    user_counts.columns = ["User", "Adjustments"]
    st.dataframe(user_counts, use_container_width=True, hide_index=True)

st.divider()

# ── Activity by Date ─────────────────────────────────────────────────
st.subheader("Adjustments by Target Date")
if not headers.empty:
    by_date_type = headers.groupby(["TARGET_DATE", "ADJ_TYPE"]).size().reset_index(name="Count")
    pivot = by_date_type.pivot(index="TARGET_DATE", columns="ADJ_TYPE", values="Count").fillna(0)
    st.bar_chart(pivot, height=300)

# ── Snowflake Feature Reference ─────────────────────────────────────
with st.expander("🔗 Snowflake Features Used in This Page"):
    st.markdown("""
    | Dashboard Component | Snowflake Feature |
    |---------------------|-------------------|
    | KPI metrics | Queries against **Dynamic Tables** (pre-materialized, fast) |
    | Before/After comparison | **Dynamic Table** `MART.FACT_ADJUSTED` with `IS_ADJUSTED` flag |
    | Impact by type/entity | **Dynamic Table** `MART.ADJUSTMENT_IMPACT_SUMMARY` (DOWNSTREAM lag) |
    | Activity over time | **Dynamic Table** `MART.DAILY_ADJUSTMENT_ACTIVITY` |
    | User masking | **Masking Policy** hides `CREATED_BY` for viewer roles |
    """)
