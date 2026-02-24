"""
Adjustment Engine — Streamlit in Snowflake (SiS) Main App
=========================================================
Entry point for the multi-page Streamlit application.
Provides navigation, session state initialization, and global layout.
"""

import streamlit as st

# ─── Page Config ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Adjustment Engine",
    page_icon="⚙️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Session State Initialization ────────────────────────────────────────────
if "adj_preview_data" not in st.session_state:
    st.session_state.adj_preview_data = None
if "current_adj_id" not in st.session_state:
    st.session_state.current_adj_id = None

# ─── Sidebar ─────────────────────────────────────────────────────────────────
with st.sidebar:
    st.image("https://img.icons8.com/fluency/96/settings.png", width=64)
    st.title("Adjustment Engine")
    st.caption("Fact Table Adjustment Platform")
    st.divider()
    st.markdown("""
    **Pages:**
    - 📝 **Apply Adjustment** — Create & preview adjustments
    - 📋 **Audit Trail** — Full history & status tracking
    - 🤖 **AI Assistant** — Natural language queries via Cortex
    - 📊 **Dashboard** — Impact analysis & metrics
    """)
    st.divider()
    st.caption("Powered by Snowflake · Cortex AI · Streamlit")

# ─── Main Page (Home) ───────────────────────────────────────────────────────
st.title("⚙️ Adjustment Engine")
st.markdown("### Fact Table Adjustment Management Platform")

st.markdown("""
Welcome to the **Adjustment Engine** — a modern, auditable platform for managing
adjustments to your fact table data.

---

#### How It Works

| Step | Action | Page |
|------|--------|------|
| 1 | **Filter** the fact rows you want to adjust | 📝 Apply Adjustment |
| 2 | **Choose** adjustment type: Flatten, Scale, or Roll | 📝 Apply Adjustment |
| 3 | **Preview** the before/after impact | 📝 Apply Adjustment |
| 4 | **Submit** for approval | 📝 Apply Adjustment |
| 5 | **Approve** or reject the adjustment | 📋 Audit Trail |
| 6 | **Apply** the approved adjustment | 📋 Audit Trail |
| 7 | **Monitor** impact and trends | 📊 Dashboard |

---

#### Adjustment Types

| Type | Description | Use Case |
|------|-------------|----------|
| **Flatten** | Sets all selected measure values to 0 | Remove erroneous data, month-end cleanup |
| **Scale** | Multiplies values by a chosen factor | Revaluation, model parameter change |
| **Roll** | Copies values from a previous day | System outage, late feed, data recovery |

---
""")

# ─── Quick Stats ─────────────────────────────────────────────────────────────
try:
    from snowflake.snowpark.context import get_active_session
    session = get_active_session()

    col1, col2, col3, col4 = st.columns(4)

    # Pending approvals
    pending = session.sql("""
        SELECT COUNT(*) AS cnt FROM ADJUSTMENT_DB.CORE.ADJ_HEADER
        WHERE ADJ_STATUS = 'PENDING_APPROVAL'
    """).collect()[0]['CNT']

    # Applied today
    applied = session.sql("""
        SELECT COUNT(*) AS cnt FROM ADJUSTMENT_DB.CORE.ADJ_HEADER
        WHERE ADJ_STATUS = 'APPLIED'
          AND DATE_TRUNC('DAY', APPLIED_AT) = CURRENT_DATE()
    """).collect()[0]['CNT']

    # Total adjustments
    total = session.sql("""
        SELECT COUNT(*) AS cnt FROM ADJUSTMENT_DB.CORE.ADJ_HEADER
    """).collect()[0]['CNT']

    # Total rows affected
    rows = session.sql("""
        SELECT COALESCE(SUM(AFFECTED_ROWS), 0) AS cnt FROM ADJUSTMENT_DB.CORE.ADJ_HEADER
        WHERE ADJ_STATUS = 'APPLIED'
    """).collect()[0]['CNT']

    col1.metric("⏳ Pending Approval", pending)
    col2.metric("✅ Applied Today", applied)
    col3.metric("📋 Total Adjustments", total)
    col4.metric("📊 Rows Adjusted", f"{rows:,}")

except Exception:
    st.info("Connect to Snowflake to see live metrics. Navigate to a page to get started.")
