"""
Fact Table Adjustment Engine — Local Prototype
==============================================
This is a fully functional prototype running on pandas DataFrames.
No Snowflake connection required. Same UI, same workflow.

Run with:
    cd prototype && streamlit run app.py
"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import streamlit as st
import pandas as pd
from data.state_manager import (
    init_state, get_fact_table, get_adj_headers,
    get_fact_adjusted, get_current_user,
)

# ── Page config ──────────────────────────────────────────────────────
st.set_page_config(
    page_title="Adjustment Engine (Prototype)",
    page_icon="⚙️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Initialise in-memory database ────────────────────────────────────
init_state()

# ── Sidebar ──────────────────────────────────────────────────────────
with st.sidebar:
    st.image("https://img.icons8.com/fluency/96/snowflake.png", width=48)
    st.title("Adjustment Engine")
    st.caption("LOCAL PROTOTYPE — no Snowflake required")
    st.divider()

    # User switcher (for demo purposes)
    users = ["sarah.chen", "james.kirk", "john.doe", "maria.santos", "admin.user"]
    current = get_current_user()
    new_user = st.selectbox(
        "👤 Current User",
        users,
        index=users.index(current) if current in users else 0,
        help="Switch users to test the approval workflow and self-approval guard.",
    )
    if new_user != current:
        st.session_state["current_user"] = new_user
        st.rerun()

    st.divider()
    st.markdown(
        """
        **Pages**
        - 🏠 Home *(this page)*
        - 📝 Apply Adjustment
        - 📋 Audit Trail
        - 🤖 AI Assistant
        - 📊 Dashboard

        ---
        *Prototype v1.0 — Feb 2026*
        """
    )

    if st.button("🔄 Reset All Data", use_container_width=True, type="secondary"):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.rerun()

# ── Home Page ────────────────────────────────────────────────────────
st.title("🏠 Adjustment Engine — Home")

st.info(
    "**Prototype Mode** — All data lives in memory. "
    "Changes persist during your session. Click *Reset All Data* in the sidebar to start fresh.",
    icon="💡",
)

# KPI cards
headers = get_adj_headers()

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total Adjustments", len(headers))
with col2:
    pending = len(headers[headers["ADJ_STATUS"] == "PENDING_APPROVAL"])
    st.metric("Pending Approval", pending, delta=f"{pending} awaiting" if pending else None)
with col3:
    applied = len(headers[headers["ADJ_STATUS"] == "APPLIED"])
    st.metric("Applied", applied)
with col4:
    total_impact = headers[headers["ADJ_STATUS"] == "APPLIED"]["TOTAL_DELTA_AMOUNT"].sum()
    st.metric("Total Impact", f"${total_impact:,.0f}")

st.divider()

# ── Recent adjustments ───────────────────────────────────────────────
st.subheader("Recent Adjustments")

if not headers.empty:
    display_cols = [
        "ADJ_ID", "ADJ_TYPE", "ADJ_STATUS", "TARGET_DATE",
        "AFFECTED_ROWS", "TOTAL_DELTA_AMOUNT",
        "CREATED_BY", "CREATED_AT",
    ]
    display = headers[display_cols].sort_values("CREATED_AT", ascending=False).copy()
    display["ADJ_ID"] = display["ADJ_ID"].apply(lambda x: f"ADJ-{int(x)}")
    display["TOTAL_DELTA_AMOUNT"] = display["TOTAL_DELTA_AMOUNT"].apply(lambda x: f"${x:,.2f}")

    # Color-code status
    def style_status(val):
        colors = {
            "DRAFT": "background-color: #e3f2fd",
            "PENDING_APPROVAL": "background-color: #fff3e0",
            "APPROVED": "background-color: #e8f5e9",
            "APPLIED": "background-color: #c8e6c9",
            "REJECTED": "background-color: #ffebee",
            "REVERSED": "background-color: #f3e5f5",
        }
        return colors.get(val, "")

    styled = display.style.applymap(style_status, subset=["ADJ_STATUS"])
    st.dataframe(styled, use_container_width=True, hide_index=True)
else:
    st.info("No adjustments yet. Go to **Apply Adjustment** to create one.")

st.divider()

# ── Fact data overview ───────────────────────────────────────────────
st.subheader("Fact Table Overview")

tab_orig, tab_adj = st.tabs(["📄 Original Data", "✅ Adjusted Data"])

with tab_orig:
    fact = get_fact_table()
    col_a, col_b = st.columns(2)
    with col_a:
        st.metric("Total Rows", len(fact))
    with col_b:
        st.metric("Total Amount", f"${fact['AMOUNT'].sum():,.0f}")
    st.dataframe(
        fact.sort_values(["BUSINESS_DATE", "ENTITY_KEY"]),
        use_container_width=True,
        hide_index=True,
        height=300,
    )

with tab_adj:
    adjusted = get_fact_adjusted()
    adjusted_rows = adjusted[adjusted["IS_ADJUSTED"] == True]
    col_a, col_b, col_c = st.columns(3)
    with col_a:
        st.metric("Total Rows", len(adjusted))
    with col_b:
        st.metric("Adjusted Amount", f"${adjusted['AMOUNT'].sum():,.0f}")
    with col_c:
        st.metric("Rows Modified", len(adjusted_rows))

    st.dataframe(
        adjusted.sort_values(["BUSINESS_DATE", "ENTITY_KEY"]),
        use_container_width=True,
        hide_index=True,
        height=300,
    )

# ── Snowflake mapping ───────────────────────────────────────────────
st.divider()
with st.expander("🔗 Snowflake Feature Mapping — What This Prototype Simulates"):
    st.markdown("""
    | Prototype Component | Snowflake Equivalent |
    |---|---|
    | `st.session_state` DataFrames | **Hybrid Tables** (ADJ_HEADER, ADJ_LINE_ITEM) |
    | `get_fact_adjusted()` recompute | **Dynamic Table** MART.FACT_ADJUSTED |
    | User switcher dropdown | **RBAC Roles** + Row Access Policy |
    | Status transition validation | **Stored Procedure** SP_UPDATE_ADJUSTMENT_STATUS |
    | Mock AI summary | **Cortex COMPLETE** (LLM) |
    | Preview before submit | **Stored Procedure** SP_PREVIEW_ADJUSTMENT |
    | Status history log | **Stream** + **Task** chain |
    """)
