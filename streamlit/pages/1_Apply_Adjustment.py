"""
Page 1: Apply Adjustment
========================
Interactive page to:
1. Filter fact table rows using dynamic dimension filters
2. Choose adjustment type (Flatten / Scale / Roll)
3. Preview before/after impact
4. Submit the adjustment for approval
"""

import streamlit as st
import json
import pandas as pd

st.set_page_config(page_title="Apply Adjustment", page_icon="📝", layout="wide")
st.title("📝 Apply Adjustment")

# ─── Snowflake Session ───────────────────────────────────────────────────────
try:
    from snowflake.snowpark.context import get_active_session
    session = get_active_session()
except Exception:
    st.error("Could not connect to Snowflake. Ensure this app runs inside Streamlit in Snowflake.")
    st.stop()

# ─── Helper: Load distinct values for a dimension ────────────────────────────
@st.cache_data(ttl=300)
def load_dimension_values(sql_query: str) -> list:
    """Execute a SQL query and return the first column as a list."""
    try:
        rows = session.sql(sql_query).collect()
        return [str(r[0]) for r in rows if r[0] is not None]
    except Exception:
        return []

@st.cache_data(ttl=60)
def load_dimension_config() -> pd.DataFrame:
    """Load the dimension filter configuration."""
    return session.sql("""
        SELECT COLUMN_NAME, DISPLAY_LABEL, DATA_TYPE, IS_REQUIRED, DISTINCT_VALUES_SQL
        FROM ADJUSTMENT_DB.CORE.ADJ_DIMENSION_CONFIG
        WHERE IS_FILTERABLE = TRUE AND ACTIVE = TRUE
        ORDER BY SORT_ORDER
    """).to_pandas()


# =============================================================================
# STEP 1: FILTER CRITERIA
# =============================================================================
st.header("Step 1: Define Filter Criteria")
st.caption("Select the dimension values to identify which fact rows to adjust.")

dim_config = load_dimension_config()
filter_criteria = {}

# Create dynamic filter UI from config
cols = st.columns(3)
for idx, row in dim_config.iterrows():
    col_name = row["COLUMN_NAME"]
    display_label = row["DISPLAY_LABEL"]
    data_type = row["DATA_TYPE"]
    is_required = row["IS_REQUIRED"]
    values_sql = row["DISTINCT_VALUES_SQL"]

    col = cols[idx % 3]

    with col:
        if data_type == "DATE":
            # Date picker
            available_dates = load_dimension_values(values_sql)
            if available_dates:
                selected = st.selectbox(
                    f"{'🔴 ' if is_required else ''}{display_label}",
                    options=[""] + available_dates,
                    key=f"filter_{col_name}",
                    help="Required" if is_required else "Optional filter"
                )
                if selected:
                    filter_criteria[col_name.lower()] = selected
        else:
            # Multi-select for VARCHAR dimensions
            options = load_dimension_values(values_sql)
            if options:
                selected = st.multiselect(
                    f"{'🔴 ' if is_required else ''}{display_label}",
                    options=options,
                    key=f"filter_{col_name}",
                    help="Required" if is_required else "Optional filter"
                )
                if selected:
                    filter_criteria[col_name.lower()] = selected if len(selected) > 1 else selected[0]

# Validate required filters
required_cols = dim_config[dim_config["IS_REQUIRED"] == True]["COLUMN_NAME"].tolist()
missing_required = [c for c in required_cols if c.lower() not in filter_criteria]

if filter_criteria:
    st.success(f"Active filters: {json.dumps(filter_criteria, indent=2, default=str)}")
else:
    st.warning("Select at least one filter to continue.")

# Show matching row count
if filter_criteria and not missing_required:
    try:
        where_clauses = []
        for k, v in filter_criteria.items():
            col_upper = k.upper()
            if isinstance(v, list):
                quoted = ", ".join([f"'{x}'" for x in v])
                where_clauses.append(f"{col_upper} IN ({quoted})")
            else:
                where_clauses.append(f"{col_upper} = '{v}'")

        where_sql = " AND ".join(where_clauses)
        count_sql = f"SELECT COUNT(*) AS CNT FROM ADJUSTMENT_DB.FACT.FACT_TABLE WHERE {where_sql}"
        row_count = session.sql(count_sql).collect()[0]["CNT"]
        st.info(f"**{row_count}** fact rows match your filter criteria.")
    except Exception as e:
        st.error(f"Error counting rows: {e}")


# =============================================================================
# STEP 2: ADJUSTMENT TYPE & PARAMETERS
# =============================================================================
st.divider()
st.header("Step 2: Choose Adjustment Type")

adj_type = st.radio(
    "Adjustment Type",
    options=["FLATTEN", "SCALE", "ROLL"],
    horizontal=True,
    help="**Flatten**: Zero out values | **Scale**: Multiply by factor | **Roll**: Copy from previous day"
)

col_a, col_b = st.columns(2)

scale_factor = 1.0
roll_source_date = None

with col_a:
    if adj_type == "SCALE":
        scale_factor = st.number_input(
            "Scale Factor",
            min_value=-10.0, max_value=100.0, value=1.0, step=0.1,
            help="Factor to multiply the values by. E.g., 1.5 = increase 50%, 0.5 = decrease 50%"
        )
        st.caption(f"Result: original × {scale_factor}")
    elif adj_type == "ROLL":
        scale_factor = st.number_input(
            "Roll Scale Factor",
            min_value=0.0, max_value=100.0, value=1.0, step=0.1,
            help="Scale applied after copying from source date. Default 1.0 = exact copy."
        )

with col_b:
    if adj_type == "ROLL":
        available_dates = load_dimension_values(
            "SELECT DISTINCT BUSINESS_DATE FROM ADJUSTMENT_DB.FACT.FACT_TABLE ORDER BY 1 DESC"
        )
        roll_source_date = st.selectbox(
            "Roll Source Date",
            options=available_dates,
            help="The date to copy values FROM"
        )
        st.caption(f"Will copy values from {roll_source_date} → target date, scaled by {scale_factor}")

# Business justification
st.divider()
st.header("Step 3: Business Justification")

col_j1, col_j2 = st.columns([3, 1])
with col_j1:
    business_reason = st.text_area(
        "Business Reason *",
        placeholder="Explain why this adjustment is needed. This will be part of the permanent audit trail.",
        height=100
    )
with col_j2:
    ticket_ref = st.text_input(
        "Ticket Reference",
        placeholder="e.g., JIRA-12345",
        help="Optional: link to a JIRA, ServiceNow, or other ticket"
    )

# Target date
target_date = filter_criteria.get("business_date", None)
if not target_date:
    target_date = st.date_input("Target Business Date")


# =============================================================================
# STEP 4: PREVIEW
# =============================================================================
st.divider()
st.header("Step 4: Preview Impact")

can_preview = (
    filter_criteria
    and not missing_required
    and business_reason
    and target_date
)

if not can_preview:
    missing = []
    if not filter_criteria or missing_required:
        missing.append("filter criteria (including required fields)")
    if not business_reason:
        missing.append("business reason")
    if not target_date:
        missing.append("target date")
    st.warning(f"Complete the following to preview: {', '.join(missing)}")

if can_preview:
    if st.button("🔍 Preview Adjustment", type="primary", use_container_width=True):
        with st.spinner("Computing adjustment preview..."):
            try:
                filter_json = json.dumps(filter_criteria, default=str)
                roll_src = f"'{roll_source_date}'" if roll_source_date else "NULL"

                preview_sql = f"""
                    CALL ADJUSTMENT_DB.CORE.SP_PREVIEW_ADJUSTMENT(
                        '{adj_type}',
                        PARSE_JSON('{filter_json}'),
                        '{target_date}'::DATE,
                        {scale_factor},
                        {roll_src}
                    )
                """

                preview_df = session.sql(preview_sql).to_pandas()
                st.session_state.adj_preview_data = preview_df

            except Exception as e:
                st.error(f"Preview failed: {e}")

    # Display preview results
    if st.session_state.adj_preview_data is not None:
        preview_df = st.session_state.adj_preview_data
        st.subheader(f"Preview: {len(preview_df)} rows affected")

        # Summary metrics
        m1, m2, m3 = st.columns(3)
        m1.metric("Total Original Amount", f"${preview_df['AMOUNT_ORIGINAL'].sum():,.2f}")
        m2.metric("Total Delta", f"${preview_df['AMOUNT_DELTA'].sum():,.2f}")
        m3.metric("Total Projected", f"${preview_df['AMOUNT_PROJECTED'].sum():,.2f}")

        # Before/After comparison tabs
        tab1, tab2, tab3 = st.tabs(["📊 Before / After", "📋 Detail Table", "📈 Impact Chart"])

        with tab1:
            comparison_df = preview_df[[
                "ENTITY_KEY", "PRODUCT_KEY", "ACCOUNT_KEY", "CURRENCY_KEY",
                "AMOUNT_ORIGINAL", "AMOUNT_DELTA", "AMOUNT_PROJECTED"
            ]].copy()
            comparison_df.columns = [
                "Entity", "Product", "Account", "Currency",
                "Original", "Delta", "Projected"
            ]
            st.dataframe(comparison_df, use_container_width=True, hide_index=True)

        with tab2:
            st.dataframe(preview_df, use_container_width=True, hide_index=True)

        with tab3:
            chart_data = preview_df.groupby("ENTITY_KEY").agg({
                "AMOUNT_ORIGINAL": "sum",
                "AMOUNT_PROJECTED": "sum"
            }).reset_index()
            chart_data.columns = ["Entity", "Original", "Projected"]
            st.bar_chart(chart_data.set_index("Entity"))


# =============================================================================
# STEP 5: SUBMIT
# =============================================================================
st.divider()
st.header("Step 5: Submit Adjustment")

if st.session_state.adj_preview_data is not None and can_preview:
    col_sub1, col_sub2 = st.columns(2)

    with col_sub1:
        submit_as_draft = st.button("💾 Save as Draft", use_container_width=True)
    with col_sub2:
        submit_for_approval = st.button(
            "📤 Submit for Approval", type="primary", use_container_width=True
        )

    if submit_as_draft or submit_for_approval:
        with st.spinner("Creating adjustment..."):
            try:
                filter_json = json.dumps(filter_criteria, default=str)
                roll_src = f"'{roll_source_date}'" if roll_source_date else "NULL"
                ticket_val = f"'{ticket_ref}'" if ticket_ref else "NULL"

                create_sql = f"""
                    CALL ADJUSTMENT_DB.CORE.SP_CREATE_ADJUSTMENT(
                        '{adj_type}',
                        PARSE_JSON('{filter_json}'),
                        '{target_date}'::DATE,
                        {scale_factor},
                        {roll_src},
                        '{business_reason.replace("'", "''")}',
                        {ticket_val}
                    )
                """

                result = session.sql(create_sql).collect()
                result_data = json.loads(result[0][0]) if result else {}

                adj_id = result_data.get("adj_id")
                rows_affected = result_data.get("rows_affected", 0)

                # If submitting for approval, update status
                if submit_for_approval and adj_id:
                    session.sql(f"""
                        CALL ADJUSTMENT_DB.CORE.SP_UPDATE_ADJUSTMENT_STATUS(
                            {adj_id}, 'PENDING_APPROVAL', 'Submitted via Streamlit'
                        )
                    """).collect()

                status = "PENDING_APPROVAL" if submit_for_approval else "DRAFT"
                st.success(
                    f"✅ Adjustment **#{adj_id}** created successfully!\n\n"
                    f"- **Type:** {adj_type}\n"
                    f"- **Status:** {status}\n"
                    f"- **Rows affected:** {rows_affected}"
                )

                st.session_state.adj_preview_data = None
                st.session_state.current_adj_id = adj_id

            except Exception as e:
                st.error(f"Failed to create adjustment: {e}")
else:
    st.info("Preview the adjustment first, then you can submit it.")
