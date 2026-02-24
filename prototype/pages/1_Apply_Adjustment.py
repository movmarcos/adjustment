"""
Page 1 — Apply Adjustment
Five-step workflow: Filter → Type → Justification → Preview → Submit
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import streamlit as st
import pandas as pd
from datetime import date, timedelta
from data.state_manager import (
    init_state, get_fact_table, get_dim_config,
    preview_adjustment, create_adjustment, get_current_user,
)

init_state()

st.title("📝 Apply Adjustment")

# ── Step 1: Filters ──────────────────────────────────────────────────
st.header("Step 1 — Select Data to Adjust", divider="blue")

fact = get_fact_table()
dim_config = get_dim_config().sort_values("DISPLAY_ORDER")

col_date, col_spacer = st.columns([1, 2])
with col_date:
    available_dates = sorted(fact["BUSINESS_DATE"].unique())
    target_date = st.date_input(
        "📅 Business Date",
        value=date(2026, 2, 23),
        min_value=min(available_dates),
        max_value=max(available_dates),
    )

# Dynamic filter controls from dimension config
filters = {}
filter_cols = st.columns(3)
for i, (_, dim) in enumerate(dim_config.iterrows()):
    col_name = dim["DIM_COLUMN"]
    display_name = dim["DIM_NAME"]
    required = dim["IS_REQUIRED"]

    with filter_cols[i % 3]:
        unique_vals = sorted(fact[col_name].unique())
        options = ["(All)"] + list(unique_vals)
        default = 0

        selected = st.selectbox(
            f"{'🔴' if required else '⚪'} {display_name}",
            options,
            index=default,
            key=f"filter_{col_name}",
        )
        filters[col_name] = None if selected == "(All)" else selected

# Show matching row count
mask = fact["BUSINESS_DATE"] == pd.Timestamp(target_date)
for col, val in filters.items():
    if val:
        mask = mask & (fact[col] == val)
matching_count = len(fact[mask])

if matching_count > 0:
    st.success(f"**{matching_count}** rows match your filter criteria.")
else:
    st.warning("No rows match. Adjust your filters.")

# ── Step 2: Adjustment Type ──────────────────────────────────────────
st.header("Step 2 — Choose Adjustment Type", divider="blue")

col_type, col_params = st.columns(2)

with col_type:
    adj_type = st.radio(
        "Adjustment Type",
        ["FLATTEN", "SCALE", "ROLL"],
        captions=[
            "Zero out all selected values (delta = -current)",
            "Multiply by a factor (delta = current × (factor-1))",
            "Copy from a previous day (delta = source × scale - current)",
        ],
    )

scale_factor = 1.0
roll_source_date = None

with col_params:
    if adj_type == "FLATTEN":
        st.info("**Flatten** will set all selected measure values to **zero**.")

    elif adj_type == "SCALE":
        scale_factor = st.number_input(
            "Scale Factor",
            min_value=0.01,
            max_value=100.0,
            value=1.10,
            step=0.01,
            format="%.2f",
        )
        st.info(f"Values will be multiplied by **{scale_factor}** (delta = current × {scale_factor - 1:.2f})")

    elif adj_type == "ROLL":
        roll_source_date = st.date_input(
            "📅 Roll Source Date",
            value=target_date - timedelta(days=1),
            max_value=target_date - timedelta(days=1),
        )
        scale_factor = st.number_input(
            "Roll Scale Factor",
            min_value=0.01,
            max_value=100.0,
            value=1.0,
            step=0.01,
            format="%.2f",
            help="Default 1.0 copies as-is; use other values to scale while rolling.",
        )

# ── Step 3: Justification ───────────────────────────────────────────
st.header("Step 3 — Business Justification", divider="blue")

col_reason, col_ticket = st.columns(2)
with col_reason:
    business_reason = st.text_area(
        "Business Reason *",
        placeholder="Explain why this adjustment is needed...",
        height=100,
    )
with col_ticket:
    ticket_reference = st.text_input(
        "Ticket Reference",
        placeholder="e.g. JIRA-1234, INC-5678",
    )

# ── Step 4: Preview ─────────────────────────────────────────────────
st.header("Step 4 — Preview Impact", divider="blue")

if matching_count == 0:
    st.warning("No rows to preview. Adjust your filters above.")
elif st.button("🔍 Generate Preview", type="primary", use_container_width=True):
    with st.spinner("Computing deltas..."):
        preview_df = preview_adjustment(
            adj_type=adj_type,
            target_date=target_date,
            filters=filters,
            scale_factor=scale_factor,
            roll_source_date=roll_source_date,
        )

    if preview_df.empty:
        st.error("No matching data found for preview.")
    else:
        st.session_state["preview_result"] = preview_df
        st.success(f"Preview computed for **{len(preview_df)}** rows.")

# Display preview if available
if "preview_result" in st.session_state and st.session_state["preview_result"] is not None:
    preview_df = st.session_state["preview_result"]

    tab_comparison, tab_detail, tab_chart = st.tabs(
        ["📊 Comparison", "📋 Detail", "📈 Chart"]
    )

    with tab_comparison:
        # Summary comparison
        summary = pd.DataFrame({
            "Measure": ["AMOUNT", "QUANTITY", "NOTIONAL"],
            "Original Total": [
                preview_df["AMOUNT"].sum(),
                preview_df["QUANTITY"].sum(),
                preview_df["NOTIONAL"].sum(),
            ],
            "Delta Total": [
                preview_df["AMOUNT_DELTA"].sum(),
                preview_df["QUANTITY_DELTA"].sum(),
                preview_df["NOTIONAL_DELTA"].sum(),
            ],
            "New Total": [
                preview_df["AMOUNT_NEW"].sum(),
                preview_df["QUANTITY_NEW"].sum(),
                preview_df["NOTIONAL_NEW"].sum(),
            ],
        })
        summary["Change %"] = summary.apply(
            lambda r: f"{(r['Delta Total'] / r['Original Total'] * 100):+.1f}%"
            if r["Original Total"] != 0 else "N/A",
            axis=1,
        )

        for col in ["Original Total", "Delta Total", "New Total"]:
            summary[col] = summary[col].apply(lambda x: f"${x:,.2f}")

        st.dataframe(summary, use_container_width=True, hide_index=True)

    with tab_detail:
        display_cols = [
            "ENTITY_KEY", "PRODUCT_KEY", "ACCOUNT_KEY", "CURRENCY_KEY",
            "AMOUNT", "AMOUNT_DELTA", "AMOUNT_NEW",
        ]
        available_cols = [c for c in display_cols if c in preview_df.columns]
        detail = preview_df[available_cols].copy()

        for c in ["AMOUNT", "AMOUNT_DELTA", "AMOUNT_NEW"]:
            if c in detail.columns:
                detail[c] = detail[c].apply(lambda x: f"${x:,.2f}")

        st.dataframe(detail, use_container_width=True, hide_index=True)

    with tab_chart:
        chart_data = preview_df[["ENTITY_KEY", "AMOUNT", "AMOUNT_NEW"]].copy()
        chart_data.columns = ["Entity", "Before", "After"]
        chart_agg = chart_data.groupby("Entity")[["Before", "After"]].sum().reset_index()
        st.bar_chart(chart_agg.set_index("Entity"), height=350)

# ── Step 5: Submit ───────────────────────────────────────────────────
st.header("Step 5 — Submit", divider="blue")

if "preview_result" not in st.session_state or st.session_state.get("preview_result") is None:
    st.info("Generate a preview first to enable submission.")
else:
    if not business_reason.strip():
        st.warning("Please enter a business reason before submitting.")
    else:
        col_draft, col_submit = st.columns(2)

        with col_draft:
            if st.button("💾 Save as Draft", use_container_width=True):
                adj_id = create_adjustment(
                    adj_type=adj_type,
                    target_date=target_date,
                    filters=filters,
                    scale_factor=scale_factor,
                    roll_source_date=roll_source_date,
                    business_reason=business_reason,
                    ticket_reference=ticket_reference,
                    submit_for_approval=False,
                )
                st.session_state["preview_result"] = None
                st.success(f"✅ Draft saved as **ADJ-{adj_id}**")
                st.balloons()

        with col_submit:
            if st.button("🚀 Submit for Approval", use_container_width=True, type="primary"):
                adj_id = create_adjustment(
                    adj_type=adj_type,
                    target_date=target_date,
                    filters=filters,
                    scale_factor=scale_factor,
                    roll_source_date=roll_source_date,
                    business_reason=business_reason,
                    ticket_reference=ticket_reference,
                    submit_for_approval=True,
                )
                st.session_state["preview_result"] = None
                st.success(f"✅ **ADJ-{adj_id}** submitted for approval!")
                st.balloons()

# ── Snowflake feature reference ──────────────────────────────────────
with st.expander("🔗 Snowflake Features Used in This Page"):
    st.markdown("""
    | Action | Snowflake Feature |
    |--------|-------------------|
    | Dynamic filter controls | **Hybrid Table** read from `ADJ_DIMENSION_CONFIG` |
    | Row count display | `SELECT COUNT(*)` on `FACT_TABLE` with **Row Access Policy** |
    | Preview computation | **Snowpark Python SP** `SP_PREVIEW_ADJUSTMENT` (returns TABLE) |
    | Save / Submit | **Snowpark Python SP** `SP_CREATE_ADJUSTMENT` writes to **Hybrid Tables** |
    | Filter criteria storage | **VARIANT column** (JSON) in `ADJ_HEADER` |
    """)
