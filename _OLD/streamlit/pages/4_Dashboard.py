"""
Page 4: Dashboard
==================
Visual impact analysis and operational metrics:
- Adjustment volume over time
- Impact by type, entity, status
- Before/After comparison for adjusted data
- Active adjustment heatmap
"""

import streamlit as st
import pandas as pd

st.set_page_config(page_title="Dashboard", page_icon="📊", layout="wide")
st.title("📊 Adjustment Impact Dashboard")

# ─── Snowflake Session ───────────────────────────────────────────────────────
try:
    from snowflake.snowpark.context import get_active_session
    session = get_active_session()
except Exception:
    st.error("Could not connect to Snowflake.")
    st.stop()


# =============================================================================
# TOP-LEVEL KPIs
# =============================================================================
st.subheader("Key Metrics")

try:
    kpi_df = session.sql("""
        SELECT
            COUNT(DISTINCT CASE WHEN ADJ_STATUS = 'APPLIED' THEN ADJ_ID END) AS APPLIED_COUNT,
            COUNT(DISTINCT CASE WHEN ADJ_STATUS = 'PENDING_APPROVAL' THEN ADJ_ID END) AS PENDING_COUNT,
            COUNT(DISTINCT CASE WHEN ADJ_STATUS = 'REVERSED' THEN ADJ_ID END) AS REVERSED_COUNT,
            COUNT(DISTINCT ADJ_ID) AS TOTAL_COUNT,
            SUM(CASE WHEN ADJ_STATUS = 'APPLIED' THEN AFFECTED_ROWS ELSE 0 END) AS TOTAL_ROWS_APPLIED
        FROM ADJUSTMENT_DB.CORE.ADJ_HEADER
    """).to_pandas()

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Total Adjustments", int(kpi_df["TOTAL_COUNT"].iloc[0]))
    k2.metric("Applied", int(kpi_df["APPLIED_COUNT"].iloc[0]))
    k3.metric("Pending Approval", int(kpi_df["PENDING_COUNT"].iloc[0]))
    k4.metric("Reversed", int(kpi_df["REVERSED_COUNT"].iloc[0]))
    k5.metric("Rows Adjusted", f"{int(kpi_df['TOTAL_ROWS_APPLIED'].iloc[0]):,}")

except Exception as e:
    st.error(f"Error loading KPIs: {e}")


# =============================================================================
# IMPACT BY ADJUSTMENT TYPE
# =============================================================================
st.divider()
st.subheader("Impact by Adjustment Type")

try:
    type_impact_df = session.sql("""
        SELECT
            h.ADJ_TYPE,
            COUNT(DISTINCT h.ADJ_ID) AS ADJ_COUNT,
            SUM(h.AFFECTED_ROWS) AS TOTAL_ROWS,
            SUM(ABS(li.AMOUNT_DELTA)) AS TOTAL_ABS_AMOUNT_IMPACT,
            SUM(li.AMOUNT_DELTA) AS NET_AMOUNT_IMPACT
        FROM ADJUSTMENT_DB.CORE.ADJ_HEADER h
        LEFT JOIN ADJUSTMENT_DB.CORE.ADJ_LINE_ITEM li ON h.ADJ_ID = li.ADJ_ID
        WHERE h.ADJ_STATUS = 'APPLIED'
        GROUP BY h.ADJ_TYPE
    """).to_pandas()

    if not type_impact_df.empty:
        col_chart1, col_chart2 = st.columns(2)

        with col_chart1:
            st.markdown("**Adjustment Count by Type**")
            st.bar_chart(type_impact_df.set_index("ADJ_TYPE")["ADJ_COUNT"])

        with col_chart2:
            st.markdown("**Absolute Amount Impact by Type**")
            st.bar_chart(type_impact_df.set_index("ADJ_TYPE")["TOTAL_ABS_AMOUNT_IMPACT"])

        st.dataframe(
            type_impact_df.rename(columns={
                "ADJ_TYPE": "Type", "ADJ_COUNT": "Count", "TOTAL_ROWS": "Rows",
                "TOTAL_ABS_AMOUNT_IMPACT": "Abs. Impact", "NET_AMOUNT_IMPACT": "Net Impact"
            }),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No applied adjustments to analyze.")
except Exception as e:
    st.error(f"Error: {e}")


# =============================================================================
# ADJUSTMENTS OVER TIME
# =============================================================================
st.divider()
st.subheader("Adjustment Activity Over Time")

try:
    time_df = session.sql("""
        SELECT
            DATE_TRUNC('DAY', CREATED_AT)::DATE AS DAY,
            ADJ_TYPE,
            COUNT(*) AS ADJ_COUNT,
            SUM(AFFECTED_ROWS) AS ROWS_AFFECTED
        FROM ADJUSTMENT_DB.CORE.ADJ_HEADER
        WHERE CREATED_AT >= DATEADD('DAY', -90, CURRENT_TIMESTAMP())
        GROUP BY 1, 2
        ORDER BY 1
    """).to_pandas()

    if not time_df.empty:
        # Pivot for stacked chart
        pivot_df = time_df.pivot_table(
            index="DAY", columns="ADJ_TYPE", values="ADJ_COUNT", fill_value=0
        )
        st.line_chart(pivot_df)
    else:
        st.info("No adjustment activity data available.")
except Exception as e:
    st.error(f"Error: {e}")


# =============================================================================
# IMPACT BY ENTITY
# =============================================================================
st.divider()
st.subheader("Impact by Entity")

try:
    entity_df = session.sql("""
        SELECT
            li.ENTITY_KEY,
            COUNT(DISTINCT li.ADJ_ID) AS ADJ_COUNT,
            COUNT(li.LINE_ID) AS ROWS_AFFECTED,
            SUM(ABS(li.AMOUNT_DELTA)) AS ABS_AMOUNT_IMPACT,
            SUM(li.AMOUNT_DELTA) AS NET_AMOUNT_IMPACT,
            SUM(li.AMOUNT_ORIGINAL) AS ORIGINAL_AMOUNT
        FROM ADJUSTMENT_DB.CORE.ADJ_LINE_ITEM li
        INNER JOIN ADJUSTMENT_DB.CORE.ADJ_HEADER h ON li.ADJ_ID = h.ADJ_ID
        WHERE h.ADJ_STATUS = 'APPLIED'
        GROUP BY li.ENTITY_KEY
        ORDER BY ABS_AMOUNT_IMPACT DESC
    """).to_pandas()

    if not entity_df.empty:
        col_e1, col_e2 = st.columns(2)

        with col_e1:
            st.markdown("**Absolute Impact by Entity**")
            st.bar_chart(entity_df.set_index("ENTITY_KEY")["ABS_AMOUNT_IMPACT"])

        with col_e2:
            st.markdown("**Adjustment Count by Entity**")
            st.bar_chart(entity_df.set_index("ENTITY_KEY")["ADJ_COUNT"])

        st.dataframe(
            entity_df.rename(columns={
                "ENTITY_KEY": "Entity", "ADJ_COUNT": "Adjustments", "ROWS_AFFECTED": "Rows",
                "ABS_AMOUNT_IMPACT": "Abs. Impact", "NET_AMOUNT_IMPACT": "Net Impact",
                "ORIGINAL_AMOUNT": "Original Amount"
            }),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No entity-level impact data available.")
except Exception as e:
    st.error(f"Error: {e}")


# =============================================================================
# BEFORE / AFTER COMPARISON (Adjusted vs Original Fact)
# =============================================================================
st.divider()
st.subheader("Before vs. After: Adjusted Fact Data")
st.caption("Compare original fact totals to the adjusted (Dynamic Table) totals.")

try:
    date_options = session.sql("""
        SELECT DISTINCT BUSINESS_DATE
        FROM ADJUSTMENT_DB.FACT.FACT_TABLE
        ORDER BY BUSINESS_DATE DESC
        LIMIT 30
    """).to_pandas()["BUSINESS_DATE"].tolist()

    if date_options:
        selected_date = st.selectbox("Business Date", options=date_options)

        compare_df = session.sql(f"""
            SELECT 'Original' AS SOURCE, ENTITY_KEY, SUM(AMOUNT) AS TOTAL_AMOUNT
            FROM ADJUSTMENT_DB.FACT.FACT_TABLE
            WHERE BUSINESS_DATE = '{selected_date}'
            GROUP BY ENTITY_KEY

            UNION ALL

            SELECT 'Adjusted' AS SOURCE, ENTITY_KEY, SUM(AMOUNT) AS TOTAL_AMOUNT
            FROM ADJUSTMENT_DB.MART.FACT_ADJUSTED
            WHERE BUSINESS_DATE = '{selected_date}'
            GROUP BY ENTITY_KEY
        """).to_pandas()

        if not compare_df.empty:
            pivot_compare = compare_df.pivot_table(
                index="ENTITY_KEY", columns="SOURCE", values="TOTAL_AMOUNT", fill_value=0
            )

            st.bar_chart(pivot_compare)

            # Show delta
            if "Original" in pivot_compare.columns and "Adjusted" in pivot_compare.columns:
                pivot_compare["Delta"] = pivot_compare["Adjusted"] - pivot_compare["Original"]
                pivot_compare["Delta %"] = (
                    pivot_compare["Delta"] / pivot_compare["Original"].replace(0, float("nan")) * 100
                ).round(2)
                st.dataframe(pivot_compare, use_container_width=True)
        else:
            st.info("No data for this date.")
except Exception as e:
    st.error(f"Error: {e}")


# =============================================================================
# TOP ADJUSTERS (User Activity)
# =============================================================================
st.divider()
st.subheader("Top Adjusters (Last 30 Days)")

try:
    users_df = session.sql("""
        SELECT
            CREATED_BY,
            COUNT(*) AS TOTAL_ADJUSTMENTS,
            SUM(CASE WHEN ADJ_STATUS = 'APPLIED' THEN 1 ELSE 0 END) AS APPLIED,
            SUM(CASE WHEN ADJ_STATUS = 'REJECTED' THEN 1 ELSE 0 END) AS REJECTED,
            SUM(CASE WHEN ADJ_STATUS = 'REVERSED' THEN 1 ELSE 0 END) AS REVERSED,
            SUM(AFFECTED_ROWS) AS TOTAL_ROWS
        FROM ADJUSTMENT_DB.CORE.ADJ_HEADER
        WHERE CREATED_AT >= DATEADD('DAY', -30, CURRENT_TIMESTAMP())
        GROUP BY CREATED_BY
        ORDER BY TOTAL_ADJUSTMENTS DESC
        LIMIT 10
    """).to_pandas()

    if not users_df.empty:
        st.dataframe(
            users_df.rename(columns={
                "CREATED_BY": "User", "TOTAL_ADJUSTMENTS": "Total",
                "APPLIED": "Applied", "REJECTED": "Rejected",
                "REVERSED": "Reversed", "TOTAL_ROWS": "Rows Affected"
            }),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No user activity in the last 30 days.")
except Exception as e:
    st.error(f"Error: {e}")
