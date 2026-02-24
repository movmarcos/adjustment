"""
Page 3 — AI Assistant (Mock)
Simulates Cortex AI features with local logic.
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import streamlit as st
import pandas as pd
import re
from datetime import datetime
from data.state_manager import (
    init_state, get_adj_headers, get_adj_line_items,
    get_fact_table, get_fact_adjusted,
)

init_state()

st.title("🤖 AI Assistant")
st.caption(
    "In production, these features are powered by **Snowflake Cortex** (LLM, NL-to-SQL, Anomaly Detection). "
    "This prototype uses simple local logic to simulate the behaviour."
)

tab_chat, tab_summary, tab_anomaly, tab_risk = st.tabs(
    ["💬 Chat", "📝 Summaries", "🔍 Anomaly Detection", "⚠️ Risk Classification"]
)

# ══════════════════════════════════════════════════════════════════════
#  TAB 1: Chat (simulated NL-to-SQL)
# ══════════════════════════════════════════════════════════════════════
with tab_chat:
    st.subheader("Natural Language Query")
    st.info(
        "In Snowflake, this uses **Cortex COMPLETE** to translate your question to SQL. "
        "The prototype matches common patterns locally.",
        icon="❄️",
    )

    # Chat history
    if "chat_history" not in st.session_state:
        st.session_state["chat_history"] = []

    # Display chat
    for msg in st.session_state["chat_history"]:
        with st.chat_message(msg["role"]):
            if msg.get("dataframe") is not None:
                st.markdown(msg["content"])
                st.dataframe(msg["dataframe"], use_container_width=True, hide_index=True)
            else:
                st.markdown(msg["content"])

    # Input
    user_input = st.chat_input("Ask about adjustments... (e.g., 'show all pending adjustments')")

    if user_input:
        st.session_state["chat_history"].append({"role": "user", "content": user_input, "dataframe": None})

        # Simple pattern matching to simulate NL-to-SQL
        headers = get_adj_headers()
        response_text = ""
        response_df = None
        query = user_input.lower()

        if "pending" in query:
            result = headers[headers["ADJ_STATUS"] == "PENDING_APPROVAL"]
            response_text = f"Found **{len(result)}** pending adjustment(s):"
            response_df = result[["ADJ_ID", "ADJ_TYPE", "TARGET_DATE", "CREATED_BY", "TOTAL_DELTA_AMOUNT"]].copy()

        elif "applied" in query or "active" in query:
            result = headers[headers["ADJ_STATUS"] == "APPLIED"]
            response_text = f"Found **{len(result)}** applied adjustment(s):"
            response_df = result[["ADJ_ID", "ADJ_TYPE", "TARGET_DATE", "TOTAL_DELTA_AMOUNT", "APPLIED_AT"]].copy()

        elif "total impact" in query or "total delta" in query:
            applied = headers[headers["ADJ_STATUS"] == "APPLIED"]
            total = applied["TOTAL_DELTA_AMOUNT"].sum()
            response_text = f"Total impact of all applied adjustments: **${total:,.2f}**"

        elif "who" in query and ("most" in query or "created" in query):
            counts = headers["CREATED_BY"].value_counts().reset_index()
            counts.columns = ["User", "Adjustments"]
            response_text = "Adjustments by user:"
            response_df = counts

        elif "flatten" in query:
            result = headers[headers["ADJ_TYPE"] == "FLATTEN"]
            response_text = f"Found **{len(result)}** FLATTEN adjustment(s):"
            response_df = result[["ADJ_ID", "ADJ_STATUS", "TARGET_DATE", "TOTAL_DELTA_AMOUNT", "CREATED_BY"]].copy()

        elif "scale" in query:
            result = headers[headers["ADJ_TYPE"] == "SCALE"]
            response_text = f"Found **{len(result)}** SCALE adjustment(s):"
            response_df = result[["ADJ_ID", "ADJ_STATUS", "TARGET_DATE", "SCALE_FACTOR", "TOTAL_DELTA_AMOUNT"]].copy()

        elif "roll" in query:
            result = headers[headers["ADJ_TYPE"] == "ROLL"]
            response_text = f"Found **{len(result)}** ROLL adjustment(s):"
            response_df = result[["ADJ_ID", "ADJ_STATUS", "TARGET_DATE", "ROLL_SOURCE_DATE", "TOTAL_DELTA_AMOUNT"]].copy()

        elif re.search(r"adj[- ]?(\d+)", query):
            m = re.search(r"adj[- ]?(\d+)", query)
            aid = int(m.group(1))
            result = headers[headers["ADJ_ID"] == aid]
            if result.empty:
                response_text = f"Adjustment ADJ-{aid} not found."
            else:
                r = result.iloc[0]
                response_text = (
                    f"**ADJ-{aid}** — {r['ADJ_TYPE']} | Status: {r['ADJ_STATUS']}\n\n"
                    f"- Target Date: {r['TARGET_DATE']}\n"
                    f"- Affected Rows: {int(r['AFFECTED_ROWS'])}\n"
                    f"- Total Delta: ${r['TOTAL_DELTA_AMOUNT']:,.2f}\n"
                    f"- Created by: {r['CREATED_BY']}\n"
                    f"- Reason: {r['BUSINESS_REASON']}"
                )

        elif "summary" in query or "summarize" in query or "overview" in query:
            status_counts = headers["ADJ_STATUS"].value_counts()
            total = len(headers)
            response_text = f"**Adjustment Overview** — {total} total adjustments:\n\n"
            for status, count in status_counts.items():
                response_text += f"- {status}: {count}\n"

        else:
            response_text = (
                "I can answer questions like:\n"
                "- *Show all pending adjustments*\n"
                "- *What is the total impact?*\n"
                "- *Who created the most adjustments?*\n"
                "- *Tell me about ADJ-1*\n"
                "- *Show all flatten adjustments*\n"
                "- *Give me a summary*\n\n"
                "In production, **Cortex COMPLETE** handles arbitrary natural language queries."
            )

        st.session_state["chat_history"].append({
            "role": "assistant", "content": response_text, "dataframe": response_df
        })
        st.rerun()

    if st.button("🗑️ Clear Chat", key="clear_chat"):
        st.session_state["chat_history"] = []
        st.rerun()

# ══════════════════════════════════════════════════════════════════════
#  TAB 2: Auto Summaries
# ══════════════════════════════════════════════════════════════════════
with tab_summary:
    st.subheader("AI-Generated Summaries")
    st.info(
        "In Snowflake, the **Task DAG** auto-generates summaries via **Cortex COMPLETE** "
        "whenever an adjustment changes status.",
        icon="❄️",
    )

    headers = get_adj_headers()
    with_summary = headers[headers["AI_SUMMARY"].notna() & (headers["AI_SUMMARY"] != "")]

    if with_summary.empty:
        st.info("No AI summaries generated yet. Approve or apply an adjustment to trigger generation.")
    else:
        for _, row in with_summary.iterrows():
            with st.container(border=True):
                col1, col2 = st.columns([1, 3])
                with col1:
                    st.markdown(f"**ADJ-{int(row['ADJ_ID'])}**")
                    st.caption(f"{row['ADJ_TYPE']} | {row['ADJ_STATUS']}")
                with col2:
                    st.markdown(f"🤖 {row['AI_SUMMARY']}")

# ══════════════════════════════════════════════════════════════════════
#  TAB 3: Anomaly Detection
# ══════════════════════════════════════════════════════════════════════
with tab_anomaly:
    st.subheader("Anomaly Detection")
    st.info(
        "In Snowflake, the **TASK_ANOMALY_CHECK** runs daily via CRON and calls "
        "**SP_DETECT_ADJUSTMENT_ANOMALIES**. This prototype runs a simplified check.",
        icon="❄️",
    )

    if st.button("🔍 Run Anomaly Check", type="primary"):
        headers = get_adj_headers()

        anomalies = []

        # Check 1: Large adjustments (> $1M absolute delta)
        large = headers[headers["TOTAL_DELTA_AMOUNT"].abs() > 1_000_000]
        for _, r in large.iterrows():
            anomalies.append({
                "Adjustment": f"ADJ-{int(r['ADJ_ID'])}",
                "Check": "Large Adjustment",
                "Detail": f"${r['TOTAL_DELTA_AMOUNT']:,.2f} exceeds $1M threshold",
                "Severity": "HIGH",
            })

        # Check 2: Multiple adjustments by same user on same date
        user_date = headers.groupby(["CREATED_BY", "TARGET_DATE"]).size().reset_index(name="count")
        multi = user_date[user_date["count"] > 1]
        for _, r in multi.iterrows():
            anomalies.append({
                "Adjustment": f"Multiple ({r['count']})",
                "Check": "Concentration Risk",
                "Detail": f"{r['CREATED_BY']} made {r['count']} adjustments on {r['TARGET_DATE']}",
                "Severity": "MEDIUM",
            })

        # Check 3: Self-approved (shouldn't happen but check historical)
        self_approved = headers[
            (headers["APPROVED_BY"].notna())
            & (headers["CREATED_BY"] == headers["APPROVED_BY"])
        ]
        for _, r in self_approved.iterrows():
            anomalies.append({
                "Adjustment": f"ADJ-{int(r['ADJ_ID'])}",
                "Check": "Self-Approval",
                "Detail": f"Created and approved by {r['CREATED_BY']}",
                "Severity": "CRITICAL",
            })

        if anomalies:
            st.warning(f"Found **{len(anomalies)}** potential anomalies.")
            anomaly_df = pd.DataFrame(anomalies)

            def color_severity(val):
                colors = {
                    "LOW": "color: green",
                    "MEDIUM": "color: orange",
                    "HIGH": "color: red",
                    "CRITICAL": "color: white; background-color: red; font-weight: bold",
                }
                return colors.get(val, "")

            st.dataframe(
                anomaly_df.style.applymap(color_severity, subset=["Severity"]),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.success("✅ No anomalies detected.")

# ══════════════════════════════════════════════════════════════════════
#  TAB 4: Risk Classification
# ══════════════════════════════════════════════════════════════════════
with tab_risk:
    st.subheader("Risk Classification")
    st.info(
        "In Snowflake, the UDF **CLASSIFY_ADJUSTMENT_RISK** uses **Cortex COMPLETE** "
        "to assign a risk label. This prototype uses simple threshold rules.",
        icon="❄️",
    )

    headers = get_adj_headers()

    def classify_risk(row):
        delta = abs(row["TOTAL_DELTA_AMOUNT"])
        rows = row["AFFECTED_ROWS"]

        if delta > 5_000_000 or rows > 50:
            return "CRITICAL"
        elif delta > 1_000_000 or rows > 20:
            return "HIGH"
        elif delta > 100_000 or rows > 5:
            return "MEDIUM"
        else:
            return "LOW"

    risk_df = headers[["ADJ_ID", "ADJ_TYPE", "ADJ_STATUS", "TARGET_DATE",
                        "AFFECTED_ROWS", "TOTAL_DELTA_AMOUNT", "CREATED_BY"]].copy()
    risk_df["RISK_LEVEL"] = headers.apply(classify_risk, axis=1)
    risk_df["ADJ_ID"] = risk_df["ADJ_ID"].apply(lambda x: f"ADJ-{int(x)}")
    risk_df["TOTAL_DELTA_AMOUNT"] = risk_df["TOTAL_DELTA_AMOUNT"].apply(lambda x: f"${x:,.2f}")

    def color_risk(val):
        colors = {
            "LOW": "background-color: #c8e6c9",
            "MEDIUM": "background-color: #fff9c4",
            "HIGH": "background-color: #ffccbc",
            "CRITICAL": "background-color: #ef9a9a; font-weight: bold",
        }
        return colors.get(val, "")

    st.dataframe(
        risk_df.style.applymap(color_risk, subset=["RISK_LEVEL"]),
        use_container_width=True,
        hide_index=True,
    )

# ── Snowflake Feature Reference ─────────────────────────────────────
with st.expander("🔗 Snowflake Features Used in This Page"):
    st.markdown("""
    | Feature | Snowflake Implementation |
    |---------|--------------------------|
    | Chat (NL-to-SQL) | **Cortex COMPLETE** (`mistral-large2`) with SQL generation prompt |
    | Auto Summaries | **Task DAG** → **Cortex COMPLETE** after status change |
    | Anomaly Detection | **Scheduled Task** (CRON) → **SP_DETECT_ADJUSTMENT_ANOMALIES** |
    | Risk Classification | **UDF CLASSIFY_ADJUSTMENT_RISK** using **Cortex COMPLETE** |
    | Search | **Cortex Search Service** (Enterprise feature) |
    """)
