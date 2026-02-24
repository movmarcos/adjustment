"""
Page 2 — Audit Trail
Full history of adjustments with detail views and action buttons.
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import streamlit as st
import pandas as pd
import json
from data.state_manager import (
    init_state, get_adj_headers, get_adj_line_items,
    get_status_history, update_status, reverse_adjustment,
    get_current_user,
)

init_state()

st.title("📋 Audit Trail")

headers = get_adj_headers()

if headers.empty:
    st.info("No adjustments yet. Create one on the **Apply Adjustment** page.")
    st.stop()

# ── Filters ──────────────────────────────────────────────────────────
st.subheader("Filter Adjustments")

col_f1, col_f2, col_f3, col_f4 = st.columns(4)

with col_f1:
    status_filter = st.multiselect(
        "Status",
        ["DRAFT", "PENDING_APPROVAL", "APPROVED", "REJECTED", "APPLIED", "REVERSED"],
        default=[],
        placeholder="All statuses",
    )
with col_f2:
    type_filter = st.multiselect(
        "Type",
        ["FLATTEN", "SCALE", "ROLL"],
        default=[],
        placeholder="All types",
    )
with col_f3:
    user_filter = st.selectbox(
        "Created By",
        ["(All)"] + sorted(headers["CREATED_BY"].unique().tolist()),
    )
with col_f4:
    sort_order = st.selectbox("Sort By", ["Newest First", "Oldest First"])

# Apply filters
filtered = headers.copy()
if status_filter:
    filtered = filtered[filtered["ADJ_STATUS"].isin(status_filter)]
if type_filter:
    filtered = filtered[filtered["ADJ_TYPE"].isin(type_filter)]
if user_filter != "(All)":
    filtered = filtered[filtered["CREATED_BY"] == user_filter]

ascending = sort_order == "Oldest First"
filtered = filtered.sort_values("CREATED_AT", ascending=ascending)

st.divider()

# ── Adjustment List ──────────────────────────────────────────────────
st.subheader(f"Adjustments ({len(filtered)})")

for _, row in filtered.iterrows():
    adj_id = int(row["ADJ_ID"])
    status = row["ADJ_STATUS"]
    adj_type = row["ADJ_TYPE"]

    # Status badge colors
    status_emoji = {
        "DRAFT": "📝", "PENDING_APPROVAL": "⏳", "APPROVED": "✅",
        "REJECTED": "❌", "APPLIED": "🟢", "REVERSED": "↩️",
    }
    emoji = status_emoji.get(status, "❓")

    with st.expander(
        f"{emoji} **ADJ-{adj_id}** | {adj_type} | {status} | "
        f"{row['TARGET_DATE']} | by {row['CREATED_BY']}",
        expanded=False,
    ):
        # ── Metrics row ──
        m1, m2, m3, m4 = st.columns(4)
        with m1:
            st.metric("Type", adj_type)
        with m2:
            st.metric("Status", status)
        with m3:
            st.metric("Affected Rows", int(row["AFFECTED_ROWS"]))
        with m4:
            delta_val = row["TOTAL_DELTA_AMOUNT"]
            st.metric("Total Delta", f"${delta_val:,.2f}")

        # ── AI Summary ──
        if row.get("AI_SUMMARY") and pd.notna(row["AI_SUMMARY"]):
            st.info(f"🤖 **AI Summary:** {row['AI_SUMMARY']}")

        # ── Details tabs ──
        tab_info, tab_lines, tab_history = st.tabs(
            ["📄 Details", "📊 Line Items", "📅 Status Timeline"]
        )

        with tab_info:
            info_cols = st.columns(2)
            with info_cols[0]:
                st.markdown(f"**Business Reason:** {row['BUSINESS_REASON']}")
                st.markdown(f"**Ticket:** {row['TICKET_REFERENCE'] or 'N/A'}")
                st.markdown(f"**Target Date:** {row['TARGET_DATE']}")
                if row.get("SCALE_FACTOR") and pd.notna(row["SCALE_FACTOR"]):
                    st.markdown(f"**Scale Factor:** {row['SCALE_FACTOR']}")
                if row.get("ROLL_SOURCE_DATE") and pd.notna(row["ROLL_SOURCE_DATE"]):
                    st.markdown(f"**Roll Source Date:** {row['ROLL_SOURCE_DATE']}")

            with info_cols[1]:
                st.markdown(f"**Created by:** {row['CREATED_BY']}")
                st.markdown(f"**Created at:** {row['CREATED_AT']}")
                if pd.notna(row.get("APPROVED_BY")):
                    st.markdown(f"**Approved by:** {row['APPROVED_BY']}")
                    st.markdown(f"**Approved at:** {row['APPROVED_AT']}")
                if pd.notna(row.get("APPLIED_AT")):
                    st.markdown(f"**Applied at:** {row['APPLIED_AT']}")

                # Show filter criteria
                try:
                    fc = json.loads(row["FILTER_CRITERIA"]) if row["FILTER_CRITERIA"] else {}
                    if fc:
                        st.markdown("**Filter Criteria:**")
                        for k, v in fc.items():
                            st.markdown(f"  - {k}: `{v}`")
                except (json.JSONDecodeError, TypeError):
                    pass

        with tab_lines:
            lines = get_adj_line_items()
            adj_lines = lines[lines["ADJ_ID"] == adj_id]
            if adj_lines.empty:
                st.info("No line items.")
            else:
                display = adj_lines[[
                    "ENTITY_KEY", "PRODUCT_KEY", "ACCOUNT_KEY", "CURRENCY_KEY",
                    "AMOUNT_ORIGINAL", "AMOUNT_DELTA",
                ]].copy()
                display["AMOUNT_NEW"] = display["AMOUNT_ORIGINAL"] + display["AMOUNT_DELTA"]

                for c in ["AMOUNT_ORIGINAL", "AMOUNT_DELTA", "AMOUNT_NEW"]:
                    display[c] = display[c].apply(lambda x: f"${x:,.2f}")

                st.dataframe(display, use_container_width=True, hide_index=True)

        with tab_history:
            hist = get_status_history()
            adj_hist = hist[hist["ADJ_ID"] == adj_id].sort_values("CHANGED_AT")
            if adj_hist.empty:
                st.info("No history recorded.")
            else:
                for _, h in adj_hist.iterrows():
                    old = h["OLD_STATUS"] or "—"
                    new = h["NEW_STATUS"]
                    st.markdown(
                        f"**{h['CHANGED_AT']}** — `{old}` → `{new}` "
                        f"by *{h['CHANGED_BY']}*"
                        + (f" — _{h['COMMENT']}_" if h.get("COMMENT") else "")
                    )

        # ── Action buttons ──
        st.divider()
        current_user = get_current_user()

        action_cols = st.columns(5)

        with action_cols[0]:
            if status == "DRAFT":
                if st.button("🚀 Submit", key=f"submit_{adj_id}", use_container_width=True):
                    ok, msg = update_status(adj_id, "PENDING_APPROVAL", "Submitted for approval")
                    if ok:
                        st.success(msg)
                        st.rerun()
                    else:
                        st.error(msg)

        with action_cols[1]:
            if status == "PENDING_APPROVAL":
                if st.button("✅ Approve", key=f"approve_{adj_id}", use_container_width=True, type="primary"):
                    ok, msg = update_status(adj_id, "APPROVED", f"Approved by {current_user}")
                    if ok:
                        st.success(msg)
                        st.rerun()
                    else:
                        st.error(msg)

        with action_cols[2]:
            if status == "PENDING_APPROVAL":
                if st.button("❌ Reject", key=f"reject_{adj_id}", use_container_width=True):
                    ok, msg = update_status(adj_id, "REJECTED", f"Rejected by {current_user}")
                    if ok:
                        st.warning(msg)
                        st.rerun()
                    else:
                        st.error(msg)

        with action_cols[3]:
            if status == "APPROVED":
                if st.button("⚡ Apply", key=f"apply_{adj_id}", use_container_width=True, type="primary"):
                    ok, msg = update_status(adj_id, "APPLIED", f"Applied by {current_user}")
                    if ok:
                        st.success(msg)
                        st.rerun()
                    else:
                        st.error(msg)

        with action_cols[4]:
            if status == "APPLIED":
                if st.button("↩️ Reverse", key=f"reverse_{adj_id}", use_container_width=True):
                    ok, msg = reverse_adjustment(adj_id)
                    if ok:
                        st.success(msg)
                        st.rerun()
                    else:
                        st.error(msg)

# ── Snowflake feature reference ──────────────────────────────────────
with st.expander("🔗 Snowflake Features Used in This Page"):
    st.markdown("""
    | Action | Snowflake Feature |
    |--------|-------------------|
    | Adjustment list with filters | **Hybrid Table** reads on `ADJ_HEADER` |
    | Line item detail | **Hybrid Table** join `ADJ_LINE_ITEM` → `ADJ_HEADER` (FK + INDEX) |
    | Status timeline | **ADJ_STATUS_HISTORY** table (populated by **Stream** + **Task**) |
    | AI Summary display | **Cortex COMPLETE** auto-generated summary |
    | Approve / Reject | **Snowpark Python SP** `SP_UPDATE_ADJUSTMENT_STATUS` |
    | Self-approval guard | Validation logic inside the stored procedure |
    | Reverse | **Snowpark Python SP** `SP_REVERSE_ADJUSTMENT` |
    """)
