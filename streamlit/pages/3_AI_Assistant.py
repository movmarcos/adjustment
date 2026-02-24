"""
Page 3: AI Assistant
====================
Cortex AI-powered features:
- Natural language querying of adjustment data
- AI-generated adjustment summaries
- Anomaly detection results
- Risk classification
"""

import streamlit as st
import json

st.set_page_config(page_title="AI Assistant", page_icon="🤖", layout="wide")
st.title("🤖 AI Assistant")
st.caption("Powered by Snowflake Cortex AI")

# ─── Snowflake Session ───────────────────────────────────────────────────────
try:
    from snowflake.snowpark.context import get_active_session
    session = get_active_session()
except Exception:
    st.error("Could not connect to Snowflake.")
    st.stop()


# =============================================================================
# TAB LAYOUT
# =============================================================================
tab_chat, tab_summary, tab_anomaly, tab_risk = st.tabs([
    "💬 Ask Questions", "📝 Generate Summaries", "🔍 Anomaly Detection", "⚠️ Risk Classification"
])


# ─── TAB 1: Natural Language Chat ────────────────────────────────────────────
with tab_chat:
    st.subheader("Ask Questions About Adjustments")
    st.markdown("""
    Ask questions in plain English and Cortex AI will translate them into SQL queries.

    **Example questions:**
    - _"How many adjustments were applied this week?"_
    - _"Show me all FLATTEN adjustments with impact greater than $1M"_
    - _"Who created the most adjustments this month?"_
    - _"What is the total amount delta for US_BANK adjustments?"_
    """)

    # Chat history
    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []

    # Display chat history
    for msg in st.session_state.chat_history:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            if msg.get("sql"):
                with st.expander("View SQL"):
                    st.code(msg["sql"], language="sql")
            if msg.get("data") is not None:
                st.dataframe(msg["data"], use_container_width=True, hide_index=True)

    # User input
    user_question = st.chat_input("Ask about your adjustments...")

    if user_question:
        # Add user message
        st.session_state.chat_history.append({"role": "user", "content": user_question})

        with st.chat_message("user"):
            st.markdown(user_question)

        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                try:
                    # Step 1: Convert to SQL using Cortex
                    nl_result = session.sql(f"""
                        SELECT ADJUSTMENT_DB.AI.NL_TO_SQL('{user_question.replace("'", "''")}') AS SQL_QUERY
                    """).collect()

                    generated_sql = nl_result[0]["SQL_QUERY"].strip()

                    # Clean up: remove markdown code fences if present
                    if generated_sql.startswith("```"):
                        lines = generated_sql.split("\n")
                        generated_sql = "\n".join(
                            l for l in lines if not l.strip().startswith("```")
                        ).strip()

                    st.markdown("Here's what I found:")

                    with st.expander("View Generated SQL", expanded=False):
                        st.code(generated_sql, language="sql")

                    # Step 2: Execute the generated SQL
                    try:
                        result_df = session.sql(generated_sql).to_pandas()
                        st.dataframe(result_df, use_container_width=True, hide_index=True)

                        # Step 3: Generate a natural language summary of results
                        if len(result_df) > 0:
                            data_summary = result_df.head(10).to_string()
                            explain_result = session.sql(f"""
                                SELECT SNOWFLAKE.CORTEX.COMPLETE(
                                    'mistral-large2',
                                    'The user asked: "{user_question.replace("'", "''")}". '
                                    || 'Here are the query results (first 10 rows): {data_summary[:500].replace("'", "''")}. '
                                    || 'Provide a concise 1-2 sentence summary of the results.'
                                ) AS SUMMARY
                            """).collect()

                            summary = explain_result[0]["SUMMARY"]
                            st.markdown(f"**Summary:** {summary}")

                            st.session_state.chat_history.append({
                                "role": "assistant",
                                "content": f"**Summary:** {summary}",
                                "sql": generated_sql,
                                "data": result_df
                            })
                        else:
                            st.info("The query returned no results.")
                            st.session_state.chat_history.append({
                                "role": "assistant",
                                "content": "The query returned no results.",
                                "sql": generated_sql
                            })

                    except Exception as sql_err:
                        st.error(f"Could not execute generated SQL: {sql_err}")
                        st.session_state.chat_history.append({
                            "role": "assistant",
                            "content": f"Generated SQL but execution failed: {sql_err}",
                            "sql": generated_sql
                        })

                except Exception as e:
                    st.error(f"AI processing error: {e}")
                    st.session_state.chat_history.append({
                        "role": "assistant",
                        "content": f"Error: {e}"
                    })

    # Clear chat button
    if st.session_state.chat_history:
        if st.button("🗑️ Clear Chat History"):
            st.session_state.chat_history = []
            st.rerun()


# ─── TAB 2: AI Summaries ─────────────────────────────────────────────────────
with tab_summary:
    st.subheader("AI-Generated Adjustment Summaries")
    st.markdown("Generate human-readable summaries of adjustments using Cortex AI.")

    # Option to generate summaries for unsummarized adjustments
    unsummarized = session.sql("""
        SELECT COUNT(*) AS CNT FROM ADJUSTMENT_DB.CORE.ADJ_HEADER
        WHERE AI_IMPACT_SUMMARY IS NULL
          AND ADJ_STATUS IN ('APPLIED', 'APPROVED')
          AND AFFECTED_ROWS > 0
    """).collect()[0]["CNT"]

    if unsummarized > 0:
        st.warning(f"{unsummarized} adjustments are missing AI summaries.")
        if st.button("🤖 Generate Missing Summaries", type="primary"):
            with st.spinner("Generating AI summaries via Cortex..."):
                try:
                    session.sql("CALL ADJUSTMENT_DB.CORE.SP_GENERATE_AI_SUMMARY()").collect()
                    st.success("Summaries generated!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error: {e}")
    else:
        st.success("All applicable adjustments have AI summaries.")

    # Show existing summaries
    st.divider()
    summaries_df = session.sql("""
        SELECT ADJ_ID, ADJ_TYPE, ADJ_STATUS, TARGET_DATE, AFFECTED_ROWS,
               BUSINESS_REASON, AI_IMPACT_SUMMARY, CREATED_BY, CREATED_AT
        FROM ADJUSTMENT_DB.CORE.ADJ_HEADER
        WHERE AI_IMPACT_SUMMARY IS NOT NULL
        ORDER BY CREATED_AT DESC
        LIMIT 20
    """).to_pandas()

    if not summaries_df.empty:
        for _, row in summaries_df.iterrows():
            with st.expander(
                f"ADJ #{row['ADJ_ID']} — {row['ADJ_TYPE']} on {row['TARGET_DATE']} ({row['ADJ_STATUS']})"
            ):
                st.markdown(f"**AI Summary:** {row['AI_IMPACT_SUMMARY']}")
                st.caption(
                    f"Business Reason: {row['BUSINESS_REASON']} | "
                    f"Rows: {row['AFFECTED_ROWS']} | "
                    f"By: {row['CREATED_BY']}"
                )
    else:
        st.info("No AI summaries available yet.")


# ─── TAB 3: Anomaly Detection ────────────────────────────────────────────────
with tab_anomaly:
    st.subheader("Adjustment Anomaly Detection")
    st.markdown("""
    AI analyzes recent adjustment patterns to identify anomalies such as:
    - Unusually large adjustments
    - High frequency by a single user
    - Unusual patterns or outliers
    """)

    if st.button("🔍 Run Anomaly Detection", type="primary"):
        with st.spinner("Analyzing adjustment patterns with Cortex AI..."):
            try:
                result = session.sql(
                    "CALL ADJUSTMENT_DB.AI.SP_DETECT_ADJUSTMENT_ANOMALIES()"
                ).collect()

                analysis = result[0][0] if result else "No results"

                st.markdown("### Analysis Results")

                # Try to parse as JSON
                try:
                    anomalies = json.loads(analysis)
                    if isinstance(anomalies, list) and len(anomalies) > 0:
                        st.warning(f"⚠️ {len(anomalies)} potential anomalies detected!")
                        for a in anomalies:
                            adj_id = a.get("adj_id", "?")
                            reason = a.get("reason", "No details")
                            st.markdown(f"- **ADJ #{adj_id}**: {reason}")
                    elif isinstance(anomalies, list) and len(anomalies) == 0:
                        st.success("✅ No anomalies detected in recent adjustments.")
                    else:
                        st.markdown(analysis)
                except (json.JSONDecodeError, TypeError):
                    st.markdown(analysis)

            except Exception as e:
                st.error(f"Anomaly detection failed: {e}")

    # Show recent daily metrics
    st.divider()
    st.subheader("Recent Adjustment Activity (Last 30 Days)")

    try:
        metrics_df = session.sql("""
            SELECT * FROM ADJUSTMENT_DB.AI.V_DAILY_ADJ_METRICS
            WHERE TS >= DATEADD('DAY', -30, CURRENT_TIMESTAMP())
            ORDER BY TS DESC
        """).to_pandas()

        if not metrics_df.empty:
            st.line_chart(metrics_df.set_index("TS")[["ADJ_COUNT", "TOTAL_ABS_AMOUNT_DELTA"]])
            st.dataframe(metrics_df, use_container_width=True, hide_index=True)
        else:
            st.info("No adjustment activity data available yet.")
    except Exception:
        st.info("Daily metrics view not yet available.")


# ─── TAB 4: Risk Classification ──────────────────────────────────────────────
with tab_risk:
    st.subheader("Adjustment Risk Classification")
    st.markdown("AI classifies each adjustment's risk level based on the business reason and financial impact.")

    try:
        risk_df = session.sql("""
            SELECT
                h.ADJ_ID,
                h.ADJ_TYPE,
                h.ADJ_STATUS,
                h.TARGET_DATE,
                h.BUSINESS_REASON,
                COALESCE(SUM(ABS(li.AMOUNT_DELTA)), 0) AS ABS_IMPACT,
                ADJUSTMENT_DB.AI.CLASSIFY_ADJUSTMENT_RISK(
                    h.BUSINESS_REASON,
                    COALESCE(SUM(ABS(li.AMOUNT_DELTA)), 0)
                ) AS RISK_LEVEL
            FROM ADJUSTMENT_DB.CORE.ADJ_HEADER h
            LEFT JOIN ADJUSTMENT_DB.CORE.ADJ_LINE_ITEM li ON h.ADJ_ID = li.ADJ_ID
            WHERE h.ADJ_STATUS NOT IN ('REJECTED', 'REVERSED')
            GROUP BY h.ADJ_ID, h.ADJ_TYPE, h.ADJ_STATUS, h.TARGET_DATE, h.BUSINESS_REASON
            ORDER BY ABS_IMPACT DESC
            LIMIT 20
        """).to_pandas()

        if not risk_df.empty:
            # Color code risk
            def risk_color(risk):
                risk = str(risk).strip().upper()
                if "HIGH" in risk:
                    return "🔴 HIGH"
                elif "MEDIUM" in risk:
                    return "🟡 MEDIUM"
                elif "LOW" in risk:
                    return "🟢 LOW"
                return f"⚪ {risk}"

            risk_df["RISK_DISPLAY"] = risk_df["RISK_LEVEL"].apply(risk_color)
            st.dataframe(
                risk_df[["ADJ_ID", "ADJ_TYPE", "ADJ_STATUS", "TARGET_DATE",
                          "ABS_IMPACT", "RISK_DISPLAY", "BUSINESS_REASON"]].rename(columns={
                    "ADJ_ID": "ID", "ADJ_TYPE": "Type", "ADJ_STATUS": "Status",
                    "TARGET_DATE": "Date", "ABS_IMPACT": "Abs. Impact",
                    "RISK_DISPLAY": "Risk", "BUSINESS_REASON": "Reason"
                }),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("No adjustments to classify.")
    except Exception as e:
        st.error(f"Risk classification failed: {e}")
