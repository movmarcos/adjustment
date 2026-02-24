"""
Page 2: Audit Trail
====================
Full visibility into all adjustments:
- Filterable list of all adjustment headers
- Status management (approve, reject, apply, reverse)
- Status history timeline
- Drill-down into line items
"""

import streamlit as st
import json
import pandas as pd

st.set_page_config(page_title="Audit Trail", page_icon="📋", layout="wide")
st.title("📋 Audit Trail")

# ─── Snowflake Session ───────────────────────────────────────────────────────
try:
    from snowflake.snowpark.context import get_active_session
    session = get_active_session()
except Exception:
    st.error("Could not connect to Snowflake.")
    st.stop()


# =============================================================================
# FILTERS
# =============================================================================
st.subheader("Filters")

fcol1, fcol2, fcol3, fcol4 = st.columns(4)

with fcol1:
    status_filter = st.multiselect(
        "Status",
        options=["DRAFT", "PENDING_APPROVAL", "APPROVED", "REJECTED", "APPLIED", "REVERSED"],
        default=["PENDING_APPROVAL", "APPROVED", "APPLIED"],
    )

with fcol2:
    type_filter = st.multiselect(
        "Adjustment Type",
        options=["FLATTEN", "SCALE", "ROLL"],
    )

with fcol3:
    date_range = st.date_input(
        "Created Date Range",
        value=[],
        help="Select start and end date"
    )

with fcol4:
    user_filter = st.text_input("Created By", placeholder="Username")

# Build query
where_parts = ["1=1"]
if status_filter:
    quoted = ", ".join([f"'{s}'" for s in status_filter])
    where_parts.append(f"h.ADJ_STATUS IN ({quoted})")
if type_filter:
    quoted = ", ".join([f"'{t}'" for t in type_filter])
    where_parts.append(f"h.ADJ_TYPE IN ({quoted})")
if date_range and len(date_range) == 2:
    where_parts.append(f"h.CREATED_AT >= '{date_range[0]}' AND h.CREATED_AT <= '{date_range[1]} 23:59:59'")
if user_filter:
    where_parts.append(f"UPPER(h.CREATED_BY) LIKE UPPER('%{user_filter}%')")

where_clause = " AND ".join(where_parts)


# =============================================================================
# ADJUSTMENT LIST
# =============================================================================
st.divider()
st.subheader("Adjustments")

query = f"""
    SELECT
        h.ADJ_ID,
        h.ADJ_TYPE,
        h.ADJ_STATUS,
        h.TARGET_DATE,
        h.SCALE_FACTOR,
        h.ROLL_SOURCE_DATE,
        h.AFFECTED_ROWS,
        h.BUSINESS_REASON,
        h.TICKET_REFERENCE,
        h.CREATED_BY,
        h.CREATED_AT,
        h.APPROVED_BY,
        h.APPLIED_AT,
        h.REVERSED_BY,
        h.REVERSED_AT,
        h.AI_IMPACT_SUMMARY,
        h.FILTER_CRITERIA::VARCHAR AS FILTER_CRITERIA
    FROM ADJUSTMENT_DB.CORE.ADJ_HEADER h
    WHERE {where_clause}
    ORDER BY h.CREATED_AT DESC
    LIMIT 100
"""

try:
    adj_df = session.sql(query).to_pandas()
except Exception as e:
    st.error(f"Error loading adjustments: {e}")
    st.stop()

if adj_df.empty:
    st.info("No adjustments match the current filters.")
    st.stop()

# Color-coded status badge
def status_badge(status):
    colors = {
        "DRAFT": "🔵", "PENDING_APPROVAL": "🟡", "APPROVED": "🟢",
        "REJECTED": "🔴", "APPLIED": "✅", "REVERSED": "⬛"
    }
    return f"{colors.get(status, '⚪')} {status}"

adj_df["STATUS_DISPLAY"] = adj_df["ADJ_STATUS"].apply(status_badge)

# Display table
display_cols = [
    "ADJ_ID", "STATUS_DISPLAY", "ADJ_TYPE", "TARGET_DATE", "AFFECTED_ROWS",
    "BUSINESS_REASON", "CREATED_BY", "CREATED_AT", "TICKET_REFERENCE"
]
st.dataframe(
    adj_df[display_cols].rename(columns={
        "ADJ_ID": "ID", "STATUS_DISPLAY": "Status", "ADJ_TYPE": "Type",
        "TARGET_DATE": "Target Date", "AFFECTED_ROWS": "Rows",
        "BUSINESS_REASON": "Reason", "CREATED_BY": "Created By",
        "CREATED_AT": "Created At", "TICKET_REFERENCE": "Ticket"
    }),
    use_container_width=True,
    hide_index=True,
)


# =============================================================================
# DETAIL VIEW & ACTIONS
# =============================================================================
st.divider()
st.subheader("Adjustment Detail & Actions")

selected_id = st.selectbox(
    "Select Adjustment ID to view details",
    options=adj_df["ADJ_ID"].tolist(),
    format_func=lambda x: f"ADJ #{x} — {adj_df[adj_df['ADJ_ID']==x]['ADJ_TYPE'].iloc[0]} ({adj_df[adj_df['ADJ_ID']==x]['ADJ_STATUS'].iloc[0]})"
)

if selected_id:
    adj_row = adj_df[adj_df["ADJ_ID"] == selected_id].iloc[0]

    # Detail cards
    d1, d2, d3 = st.columns(3)
    with d1:
        st.metric("Adjustment Type", adj_row["ADJ_TYPE"])
        st.metric("Status", adj_row["ADJ_STATUS"])
        st.metric("Target Date", str(adj_row["TARGET_DATE"]))

    with d2:
        st.metric("Rows Affected", adj_row["AFFECTED_ROWS"])
        st.metric("Scale Factor", adj_row.get("SCALE_FACTOR", "N/A"))
        if adj_row.get("ROLL_SOURCE_DATE"):
            st.metric("Roll Source Date", str(adj_row["ROLL_SOURCE_DATE"]))

    with d3:
        st.metric("Created By", adj_row["CREATED_BY"])
        st.metric("Created At", str(adj_row["CREATED_AT"])[:19])
        if adj_row.get("APPROVED_BY"):
            st.metric("Approved By", adj_row["APPROVED_BY"])

    # Business reason
    st.markdown(f"**Business Reason:** {adj_row['BUSINESS_REASON']}")
    if adj_row.get("TICKET_REFERENCE"):
        st.markdown(f"**Ticket:** {adj_row['TICKET_REFERENCE']}")

    # AI Summary
    if adj_row.get("AI_IMPACT_SUMMARY"):
        with st.expander("🤖 AI Impact Summary", expanded=True):
            st.markdown(adj_row["AI_IMPACT_SUMMARY"])

    # Filter criteria
    with st.expander("🔍 Filter Criteria"):
        try:
            criteria = json.loads(adj_row["FILTER_CRITERIA"]) if adj_row["FILTER_CRITERIA"] else {}
            st.json(criteria)
        except Exception:
            st.code(str(adj_row["FILTER_CRITERIA"]))

    # ─── Line Items ──────────────────────────────────────────────────────
    with st.expander("📊 Line Item Details", expanded=False):
        lines_df = session.sql(f"""
            SELECT
                LINE_ID, BUSINESS_DATE, ENTITY_KEY, PRODUCT_KEY, ACCOUNT_KEY,
                CURRENCY_KEY, COUNTERPARTY_KEY, COST_CENTER_KEY,
                AMOUNT_ORIGINAL, AMOUNT_DELTA,
                AMOUNT_ORIGINAL + AMOUNT_DELTA AS AMOUNT_PROJECTED,
                QUANTITY_ORIGINAL, QUANTITY_DELTA,
                NOTIONAL_ORIGINAL, NOTIONAL_DELTA
            FROM ADJUSTMENT_DB.CORE.ADJ_LINE_ITEM
            WHERE ADJ_ID = {selected_id}
            ORDER BY LINE_ID
        """).to_pandas()

        if not lines_df.empty:
            st.dataframe(lines_df, use_container_width=True, hide_index=True)
        else:
            st.info("No line items found.")

    # ─── Status History ──────────────────────────────────────────────────
    with st.expander("📜 Status History Timeline"):
        history_df = session.sql(f"""
            SELECT OLD_STATUS, NEW_STATUS, CHANGED_BY, CHANGED_AT, COMMENT
            FROM ADJUSTMENT_DB.CORE.ADJ_STATUS_HISTORY
            WHERE ADJ_ID = {selected_id}
            ORDER BY CHANGED_AT ASC
        """).to_pandas()

        if not history_df.empty:
            for _, h_row in history_df.iterrows():
                old = h_row["OLD_STATUS"] or "—"
                new = h_row["NEW_STATUS"]
                who = h_row["CHANGED_BY"]
                when = str(h_row["CHANGED_AT"])[:19]
                comment = h_row.get("COMMENT", "")
                st.markdown(f"**{old}** → **{new}** by `{who}` at `{when}`")
                if comment:
                    st.caption(f"_{comment}_")
        else:
            st.info("No status history recorded yet.")

    # ─── Action Buttons ──────────────────────────────────────────────────
    st.divider()
    st.subheader("Actions")

    current_status = adj_row["ADJ_STATUS"]

    action_col1, action_col2, action_col3 = st.columns(3)

    with action_col1:
        if current_status == "DRAFT":
            if st.button("📤 Submit for Approval", key="submit_approve", use_container_width=True):
                try:
                    result = session.sql(f"""
                        CALL ADJUSTMENT_DB.CORE.SP_UPDATE_ADJUSTMENT_STATUS(
                            {selected_id}, 'PENDING_APPROVAL', 'Submitted from Audit Trail'
                        )
                    """).collect()
                    st.success("Submitted for approval!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error: {e}")

    with action_col2:
        if current_status == "PENDING_APPROVAL":
            comment = st.text_input("Approval Comment", key="approve_comment")

            approve_col, reject_col = st.columns(2)
            with approve_col:
                if st.button("✅ Approve", key="btn_approve", type="primary", use_container_width=True):
                    try:
                        result = session.sql(f"""
                            CALL ADJUSTMENT_DB.CORE.SP_UPDATE_ADJUSTMENT_STATUS(
                                {selected_id}, 'APPROVED', '{comment}'
                            )
                        """).collect()
                        result_data = json.loads(result[0][0]) if result else {}
                        if result_data.get("success"):
                            st.success("Approved!")
                            st.rerun()
                        else:
                            st.error(result_data.get("error", "Unknown error"))
                    except Exception as e:
                        st.error(f"Error: {e}")

            with reject_col:
                if st.button("❌ Reject", key="btn_reject", use_container_width=True):
                    try:
                        result = session.sql(f"""
                            CALL ADJUSTMENT_DB.CORE.SP_UPDATE_ADJUSTMENT_STATUS(
                                {selected_id}, 'REJECTED', '{comment}'
                            )
                        """).collect()
                        st.success("Rejected.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {e}")

    with action_col3:
        if current_status == "APPROVED":
            if st.button("🚀 Apply Adjustment", key="btn_apply", type="primary", use_container_width=True):
                try:
                    result = session.sql(f"""
                        CALL ADJUSTMENT_DB.CORE.SP_UPDATE_ADJUSTMENT_STATUS(
                            {selected_id}, 'APPLIED', 'Applied from Audit Trail'
                        )
                    """).collect()
                    st.success("Adjustment applied! Dynamic tables will refresh automatically.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error: {e}")

        elif current_status == "APPLIED":
            reversal_reason = st.text_input("Reversal Reason", key="reversal_reason")
            if st.button("⏪ Reverse Adjustment", key="btn_reverse", use_container_width=True):
                if not reversal_reason:
                    st.warning("Please provide a reason for reversal.")
                else:
                    try:
                        result = session.sql(f"""
                            CALL ADJUSTMENT_DB.CORE.SP_REVERSE_ADJUSTMENT(
                                {selected_id}, '{reversal_reason}'
                            )
                        """).collect()
                        result_data = json.loads(result[0][0]) if result else {}
                        if result_data.get("success"):
                            new_id = result_data.get("reversal_adj_id")
                            st.success(f"Reversed! Reversal adjustment #{new_id} created and auto-applied.")
                            st.rerun()
                        else:
                            st.error(result_data.get("error", "Reversal failed"))
                    except Exception as e:
                        st.error(f"Error: {e}")
