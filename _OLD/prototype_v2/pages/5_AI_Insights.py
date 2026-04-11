"""
AI Insights — Cortex-Powered Analytics
========================================
Natural language chat, anomaly detection, risk classification,
and smart recommendations — powered by Snowflake Cortex in production.
"""
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from datetime import date, timedelta
from collections import Counter

st.set_page_config(
    page_title="AI Insights · MUFG",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded",
)

from _OLD.prototype_v2.data.state_manager import (
    init_state, get_all_adjustments, generate_chat_response,
    dashboard_kpis, current_user,
)
from _OLD.prototype_v2.data.mock_data import SCOPES
from _OLD.prototype_v2.data.styles import inject_css, render_sidebar, section_title, status_badge, P

init_state()
inject_css()
render_sidebar()

st.markdown("## 🤖 AI Insights")
st.markdown(
    f"<span style='color:{P['grey_700']};font-size:0.9rem'>"
    "Powered by Snowflake Cortex (mistral-large2 in production). "
    "Ask questions in natural language, detect anomalies, and classify adjustment risk."
    "</span>", unsafe_allow_html=True)
st.markdown("<br/>", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# TABS
# ──────────────────────────────────────────────────────────────────────────────

tab_chat, tab_anomaly, tab_risk, tab_reco = st.tabs([
    "💬 Chat with your Data",
    "🔎 Anomaly Detection",
    "⚠️ Risk Classification",
    "💡 Smart Recommendations",
])

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — CHAT
# ══════════════════════════════════════════════════════════════════════════════
with tab_chat:
    st.markdown(
        f'<div style="background:linear-gradient(135deg,{P["accent"]},#2D2D5E);'
        f'border-radius:10px;padding:1rem 1.2rem;margin-bottom:1rem;color:#E8E8F8;font-size:0.85rem">'
        f'<strong>🤖 Cortex AI</strong> — Ask me anything about your adjustments. '
        f'In production, I use <code>SNOWFLAKE.CORTEX.COMPLETE(\'mistral-large2\')</code> with '
        f'your full adjustment schema to generate real SQL and natural language answers.'
        f'</div>',
        unsafe_allow_html=True)

    # Chat history
    if "chat_history" not in st.session_state:
        st.session_state["chat_history"] = []

    # Suggested questions
    suggestions = [
        "How many adjustments are pending approval?",
        "What is the largest adjustment applied this month?",
        "Show me all rejected adjustments",
        "Which recurring adjustments are active?",
        "Are there any unusual or anomalous adjustments?",
        "What adjustments did I create?",
    ]

    st.markdown('<div style="font-size:0.78rem;font-weight:600;color:#9E9E9E;margin-bottom:0.4rem">SUGGESTED QUESTIONS</div>', unsafe_allow_html=True)
    sug_cols = st.columns(3)
    for i, sug in enumerate(suggestions):
        with sug_cols[i % 3]:
            if st.button(f'"{sug}"', key=f"sug_{i}", use_container_width=True):
                answer, sql = generate_chat_response(sug)
                st.session_state["chat_history"].append(
                    {"role": "user",      "content": sug})
                st.session_state["chat_history"].append(
                    {"role": "assistant", "content": answer, "sql": sql})
                st.rerun()

    st.markdown("<br/>", unsafe_allow_html=True)

    # Chat messages
    chat_container = st.container()
    with chat_container:
        for msg in st.session_state["chat_history"]:
            if msg["role"] == "user":
                st.markdown(
                    f'<div style="display:flex;justify-content:flex-end;margin:6px 0">'
                    f'<div style="background:{P["primary"]};color:white;border-radius:12px 12px 2px 12px;'
                    f'padding:0.55rem 0.9rem;max-width:70%;font-size:0.85rem">'
                    f'{msg["content"]}</div></div>',
                    unsafe_allow_html=True)
            else:
                st.markdown(
                    f'<div style="display:flex;justify-content:flex-start;margin:6px 0;gap:8px">'
                    f'<div style="background:{P["accent"]};color:white;border-radius:50%;'
                    f'width:28px;height:28px;display:flex;align-items:center;justify-content:center;'
                    f'font-size:0.8rem;flex-shrink:0">🤖</div>'
                    f'<div style="background:{P["white"]};border:1px solid {P["border"]};'
                    f'border-radius:2px 12px 12px 12px;padding:0.55rem 0.9rem;'
                    f'max-width:75%;font-size:0.85rem">{msg["content"]}'
                    f'</div></div>',
                    unsafe_allow_html=True)
                if msg.get("sql"):
                    with st.expander("View generated SQL", expanded=False):
                        st.code(msg["sql"], language="sql")

    # Input
    st.markdown("<br/>", unsafe_allow_html=True)
    inp_col, btn_col = st.columns([5, 1])
    with inp_col:
        user_q = st.text_input("Ask about your adjustments…",
                               key="chat_input", label_visibility="collapsed",
                               placeholder="e.g. How many adjustments were applied this week?")
    with btn_col:
        send = st.button("Send ➤", use_container_width=True, type="primary")

    if send and user_q.strip():
        answer, sql = generate_chat_response(user_q)
        st.session_state["chat_history"].append({"role": "user", "content": user_q})
        st.session_state["chat_history"].append({"role": "assistant", "content": answer, "sql": sql})
        st.rerun()

    if st.session_state["chat_history"]:
        if st.button("🗑️ Clear conversation", key="clear_chat"):
            st.session_state["chat_history"] = []
            st.rerun()

# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — ANOMALY DETECTION
# ══════════════════════════════════════════════════════════════════════════════
with tab_anomaly:
    st.markdown(
        f'<div style="background:{P["info_lt"]};border:1px solid #90CAF9;border-radius:8px;'
        f'padding:0.75rem 1rem;margin-bottom:1rem;font-size:0.85rem">'
        f'🔎 <strong>Anomaly Detection</strong> — In production this calls '
        f'<code>AI.SP_DETECT_ADJUSTMENT_ANOMALIES()</code> which uses Cortex to analyse '
        f'the last 30 days of adjustments and flag unusual patterns. '
        f'Below is a statistical analysis using mock data.'
        f'</div>',
        unsafe_allow_html=True)

    adjs = get_all_adjustments()
    applied = [a for a in adjs if a["adj_status"] == "APPLIED"]

    if len(applied) < 2:
        st.info("Not enough applied adjustments to analyse anomalies.")
    else:
        # ── Scale factor distribution ────────────────────────────────────────
        section_title("Scale Factor Distribution", "📊")
        scale_factors = [a["scale_factor"] for a in applied if a.get("scale_factor")]
        if scale_factors:
            import numpy as np
            mean_sf  = np.mean(scale_factors)
            std_sf   = np.std(scale_factors)
            threshold = 2.0  # 2-sigma
            outliers = [a for a in applied
                        if abs(a.get("scale_factor", 1) - mean_sf) > threshold * std_sf]

            fig = go.Figure()
            fig.add_trace(go.Histogram(
                x=scale_factors, nbinsx=15,
                marker_color=P["info"], opacity=0.7,
                name="Scale Factors",
                hovertemplate="Factor: %{x:.4f}<br>Count: %{y}<extra></extra>",
            ))
            for o in outliers:
                fig.add_vline(x=o["scale_factor"], line_color=P["danger"],
                              line_dash="dash", line_width=1)
            fig.add_vline(x=mean_sf, line_color=P["success"], line_width=2,
                          annotation_text="Mean", annotation_position="top right")
            fig.update_layout(
                xaxis_title="Scale Factor", yaxis_title="Count",
                plot_bgcolor="white", paper_bgcolor="white",
                margin=dict(l=10,r=10,t=10,b=10), height=200,
                font_family="Inter",
                xaxis=dict(showgrid=False),
                yaxis=dict(showgrid=True, gridcolor="#F5F5F5"),
            )
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

        # ── Anomaly cards ────────────────────────────────────────────────────
        section_title("Flagged Anomalies", "🚨")

        anomalies = []

        # Large scope
        avg_rows = sum(a["affected_rows"] for a in applied) / len(applied) if applied else 0
        for a in applied:
            if a["affected_rows"] > avg_rows * 2.5:
                scope = SCOPES.get(a["scope_key"], {})
                anomalies.append({
                    "adj_id":   a["adj_id"],
                    "type":     "LARGE SCOPE",
                    "color":    P["warning"],
                    "icon":     "📐",
                    "detail":   f'ADJ #{a["adj_id"]}: {a["affected_rows"]} rows affected '
                                f'({a["affected_rows"]/avg_rows:.1f}× the average of {avg_rows:.0f}). '
                                f'Scope: {scope.get("label","?")} · {a["adj_type"]} by {a["created_by"]}.',
                })

        # Extreme scale factors
        for a in applied:
            sf = a.get("scale_factor", 1.0)
            if sf and abs(sf - 1) > 0.10:
                anomalies.append({
                    "adj_id":   a["adj_id"],
                    "type":     "EXTREME SCALE",
                    "color":    P["danger"],
                    "icon":     "📈",
                    "detail":   f'ADJ #{a["adj_id"]}: Scale factor {sf:.4f}× '
                                f'({(sf-1)*100:+.1f}%) is unusually large. '
                                f'Created by {a["created_by"]} — review justification.',
                })

        # No ticket reference on large adjustments
        for a in applied:
            if a["affected_rows"] > 50 and not a.get("ticket_reference"):
                anomalies.append({
                    "adj_id":   a["adj_id"],
                    "type":     "MISSING REFERENCE",
                    "color":    P["grey_700"],
                    "icon":     "📎",
                    "detail":   f'ADJ #{a["adj_id"]}: {a["affected_rows"]} rows adjusted '
                                f'with no ticket reference. Best practice requires a JIRA or ServiceNow reference '
                                f'for adjustments affecting >50 records.',
                })

        if not anomalies:
            st.success("✅ No anomalies detected in applied adjustments.")
        else:
            st.markdown(f'<div style="font-size:0.85rem;margin-bottom:0.75rem">'
                        f'Found <strong>{len(anomalies)}</strong> anomalies worth reviewing:</div>',
                        unsafe_allow_html=True)
            for anm in anomalies:
                st.markdown(
                    f'<div style="background:{anm["color"]}14;border-left:4px solid {anm["color"]};'
                    f'border-radius:0 8px 8px 0;padding:0.75rem 1rem;margin-bottom:0.5rem;'
                    f'font-size:0.85rem">'
                    f'<span style="font-weight:700;color:{anm["color"]}">'
                    f'{anm["icon"]} {anm["type"]}</span>&nbsp;&nbsp;'
                    f'{anm["detail"]}</div>',
                    unsafe_allow_html=True)

        # ── Cortex note ─────────────────────────────────────────────────────
        with st.expander("🔧 How this works in production (Snowflake Cortex)"):
            st.code("""
-- Stored procedure AI.SP_DETECT_ADJUSTMENT_ANOMALIES()
-- Runs daily at 8 AM via TASK_ANOMALY_CHECK

SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'mistral-large2',
    CONCAT(
        'You are a financial data analyst. Analyse these adjustment records ',
        'and identify anomalies. Return a JSON array of findings. Records: ',
        (SELECT LISTAGG(adj_summary, ', ') FROM last_30_days_view)
    )
) AS anomaly_report;
            """, language="sql")

# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — RISK CLASSIFICATION
# ══════════════════════════════════════════════════════════════════════════════
with tab_risk:
    st.markdown(
        f'<div style="background:{P["warning_lt"]};border:1px solid #FFCC80;border-radius:8px;'
        f'padding:0.75rem 1rem;margin-bottom:1rem;font-size:0.85rem">'
        f'⚠️ <strong>Risk Classification</strong> — In production, each adjustment is scored by '
        f'<code>AI.CLASSIFY_ADJUSTMENT_RISK(reason, amount)</code> using Cortex. '
        f'This prototype uses a rule-based approximation.'
        f'</div>',
        unsafe_allow_html=True)

    adjs_all = get_all_adjustments()

    def classify_risk(adj):
        rows = adj["affected_rows"]
        sf   = abs(adj.get("scale_factor", 1.0) - 1.0) if adj.get("scale_factor") else 0
        t    = adj["adj_type"]
        if t == "FLATTEN" or rows > 100 or sf > 0.10:
            return "HIGH",   P["danger"],  "🔴"
        elif rows > 30 or sf > 0.05:
            return "MEDIUM", P["warning"], "🟡"
        else:
            return "LOW",    P["success"], "🟢"

    rows = []
    for a in sorted(adjs_all, key=lambda x: x["affected_rows"], reverse=True):
        risk, color, icon = classify_risk(a)
        scope = SCOPES.get(a["scope_key"], {})
        rows.append({
            "ADJ #":       a["adj_id"],
            "Risk":        f"{icon} {risk}",
            "Source":      f'{scope.get("icon","")} {scope.get("label","?")}',
            "Type":        a["adj_type"],
            "Status":      a["adj_status"].replace("_"," "),
            "Rows":        a["affected_rows"],
            "Scale":       f'{a["scale_factor"]:.4f}×' if a.get("scale_factor") else "—",
            "Created by":  a["created_by"],
            "Reason":      a["business_reason"][:50] + ("…" if len(a["business_reason"])>50 else ""),
            "_risk":       risk,  # for sorting
        })

    df = pd.DataFrame(rows)

    # Risk summary
    rc = Counter(r["_risk"] for r in rows)
    rm1, rm2, rm3 = st.columns(3)
    for col, (risk, color, icon) in zip(
        [rm1, rm2, rm3],
        [("HIGH", P["danger"],"🔴"), ("MEDIUM",P["warning"],"🟡"), ("LOW",P["success"],"🟢")]
    ):
        col.markdown(
            f'<div style="background:{color}14;border:1px solid {color}55;border-radius:8px;'
            f'padding:0.8rem;text-align:center">'
            f'<div style="font-size:1.5rem;font-weight:800;color:{color}">'
            f'{icon} {rc.get(risk,0)}</div>'
            f'<div style="font-size:0.72rem;text-transform:uppercase;letter-spacing:.06em;'
            f'color:{P["grey_700"]};margin-top:3px">{risk} Risk</div>'
            f'</div>',
            unsafe_allow_html=True)

    st.markdown("<br/>", unsafe_allow_html=True)

    # Filter
    risk_filter = st.multiselect("Filter by risk level", ["HIGH","MEDIUM","LOW"],
                                 default=[], key="risk_filter", placeholder="All risk levels…")
    display_df = df if not risk_filter else df[df["_risk"].isin(risk_filter)]
    display_df = display_df.drop(columns=["_risk"])

    def highlight_risk(val):
        if "HIGH"   in str(val): return f"color:{P['danger']};font-weight:600"
        if "MEDIUM" in str(val): return f"color:{P['warning']};font-weight:600"
        if "LOW"    in str(val): return f"color:{P['success']};font-weight:600"
        return ""

    st.dataframe(
        display_df.style.map(highlight_risk, subset=["Risk"]),
        use_container_width=True, hide_index=True, height=400,
    )

    with st.expander("🔧 How this works in production (Snowflake Cortex)"):
        st.code("""
-- SQL UDF  AI.CLASSIFY_ADJUSTMENT_RISK(reason VARCHAR, amount NUMBER)
CREATE OR REPLACE FUNCTION AI.CLASSIFY_ADJUSTMENT_RISK(reason VARCHAR, amount NUMBER)
RETURNS VARCHAR
LANGUAGE SQL AS $$
    SELECT SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large2',
        CONCAT('Classify the risk of this financial adjustment as LOW, MEDIUM, or HIGH. ',
               'Reason: ', reason,
               '. Amount: $', TO_VARCHAR(amount, '999,999,999.00'),
               '. Return only the word LOW, MEDIUM, or HIGH.')
    )
$$;
        """, language="sql")

# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — SMART RECOMMENDATIONS
# ══════════════════════════════════════════════════════════════════════════════
with tab_reco:
    st.markdown(
        f'<div style="background:linear-gradient(135deg,{P["accent"]},#2D2D5E);'
        f'border-radius:10px;padding:1rem 1.2rem;margin-bottom:1rem;color:#E8E8F8;font-size:0.85rem">'
        f'💡 <strong>Smart Recommendations</strong> — Cortex analyses patterns in your adjustment history '
        f'to surface proactive suggestions.'
        f'</div>',
        unsafe_allow_html=True)

    adjs_all = get_all_adjustments()

    # Generate mock recommendations
    recommendations = []

    pending_old = [
        a for a in adjs_all
        if a["adj_status"] == "PENDING_APPROVAL"
        and a.get("created_at")
        and (date.today() - a["created_at"].date()).days >= 1
    ]
    if pending_old:
        recommendations.append({
            "icon": "⏰",
            "priority": "HIGH",
            "color": P["danger"],
            "title": f"{len(pending_old)} adjustment(s) waiting over 24h for approval",
            "body": (f"ADJ #{', #'.join(str(a['adj_id']) for a in pending_old[:3])} "
                     f"have been pending for more than one business day. "
                     f"Consider nudging the approver or recalling and re-submitting if the scope has changed."),
            "action": "Go to Approval Queue",
        })

    approved_not_applied = [a for a in adjs_all if a["adj_status"] == "APPROVED"]
    if approved_not_applied:
        recommendations.append({
            "icon": "▶️",
            "priority": "MEDIUM",
            "color": P["warning"],
            "title": f"{len(approved_not_applied)} approved adjustment(s) not yet applied",
            "body": (f"These adjustments are approved and ready but haven't been applied to the data yet. "
                     f"If the COB window is closing, apply them soon to meet reporting deadlines."),
            "action": "Go to My Work",
        })

    # Recurring not set up for common patterns
    scope_counts = Counter(a["scope_key"] for a in adjs_all if a["adj_status"] == "APPLIED")
    most_common_scope = scope_counts.most_common(1)
    if most_common_scope and most_common_scope[0][1] >= 2:
        sk, cnt = most_common_scope[0]
        scope = SCOPES.get(sk, {})
        recur_for_scope = sum(1 for a in adjs_all
                              if a["scope_key"] == sk and a.get("frequency") == "RECURRING")
        if recur_for_scope == 0:
            recommendations.append({
                "icon": "🔁",
                "priority": "LOW",
                "color": P["info"],
                "title": f"Consider creating a recurring template for {scope.get('label','?')}",
                "body": (f"You've applied {cnt} manual adjustments to {scope.get('full_label','?')} "
                         f"— this looks like a repeating pattern. "
                         f"A recurring template would auto-apply after each COB without manual intervention."),
                "action": "New Adjustment → RECURRING",
            })

    # Missing ticket refs
    missing_ticket = [
        a for a in adjs_all
        if a["adj_status"] == "APPLIED"
        and not a.get("ticket_reference")
        and a["affected_rows"] > 20
    ]
    if missing_ticket:
        recommendations.append({
            "icon": "📎",
            "priority": "LOW",
            "color": P["grey_700"],
            "title": f"{len(missing_ticket)} applied adjustment(s) missing a ticket reference",
            "body": ("Linking adjustments to JIRA/ServiceNow tickets improves audit traceability "
                     "and is recommended for adjustments affecting >20 records."),
            "action": None,
        })

    if not recommendations:
        st.success("✅ No recommendations at this time — everything looks healthy!")
    else:
        for r in recommendations:
            st.markdown(
                f'<div style="background:{P["white"]};border:1px solid {P["border"]};'
                f'border-left:4px solid {r["color"]};border-radius:0 10px 10px 0;'
                f'padding:1rem 1.2rem;margin-bottom:0.75rem;'
                f'box-shadow:0 1px 4px rgba(0,0,0,.04)">'
                f'<div style="display:flex;align-items:center;gap:8px;margin-bottom:0.4rem">'
                f'<span style="font-size:1.1rem">{r["icon"]}</span>'
                f'<span style="font-weight:700;font-size:0.9rem">{r["title"]}</span>'
                f'<span style="background:{r["color"]}22;color:{r["color"]};border-radius:4px;'
                f'padding:1px 7px;font-size:0.68rem;font-weight:700;margin-left:auto">'
                f'{r["priority"]}</span>'
                f'</div>'
                f'<div style="font-size:0.83rem;color:{P["grey_700"]};line-height:1.5">'
                f'{r["body"]}</div>'
                + (f'<div style="margin-top:0.5rem;font-size:0.78rem;font-weight:600;'
                   f'color:{r["color"]}">→ {r["action"]}</div>' if r.get("action") else "")
                + f'</div>',
                unsafe_allow_html=True)

    st.markdown("<br/>", unsafe_allow_html=True)
    section_title("Cortex Analyst (Semantic Layer)", "🗺️")
    with st.expander("About the Cortex Analyst semantic model"):
        st.markdown("""
The production app uses a **Cortex Analyst semantic model** (`cortex_semantic_model.yaml`)
uploaded to `@ADJUSTMENT_DB.APP.AI_STAGE`.

This YAML file defines logical tables, column descriptions, and verified example queries,
enabling Cortex Analyst to answer complex business questions like:

- *"What was the net P&L impact of all adjustments last quarter?"*
- *"Which desk had the most adjustments in January?"*
- *"Compare original vs adjusted risk sensitivities for EU London"*

Cortex Analyst translates these directly to SQL, executes them, and returns
a natural language summary alongside the result set.
        """)
