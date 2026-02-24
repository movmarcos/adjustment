"""
🤖 AI Assistant — NL queries, summaries, anomaly detection
"""
import streamlit as st
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from data.state_manager import (init_state, get_fact_table, get_headers, get_lines,
                                 get_fact_adjusted, current_scope_cfg)
from data.styles import inject_css, section_header, scope_selector_sidebar, metric_card, format_number, status_badge
from data.mock_data import SCOPES
import pandas as pd
import numpy as np

st.set_page_config(page_title="AI Assistant", page_icon="🤖", layout="wide")
inject_css()
init_state()
scope_id = scope_selector_sidebar()
cfg = current_scope_cfg()

st.markdown(f"""
<div style="display:flex;align-items:center;gap:10px;margin-bottom:4px">
    <span style="font-size:2rem">🤖</span>
    <h1 style="margin:0;font-size:1.6rem;color:#0D47A1">AI Assistant</h1>
</div>
<span style="color:#607D8B;font-size:.88rem">Scope: <strong>{cfg['icon']} {cfg['name']}</strong>
&nbsp;·&nbsp; Ask questions in plain English</span>
""", unsafe_allow_html=True)
st.markdown("---")

def _process_query(query: str, scope_cfg: dict) -> str:
    q = query.lower()
    headers = get_headers()
    fact = get_fact_table()
    meas_keys = [m["key"] for m in scope_cfg["measures"]]

    if any(w in q for w in ["how many", "count", "total"]):
        if "adjustment" in q:
            n = len(headers)
            return f"There are **{n} adjustments** in the {scope_cfg['name']} scope."
        if "row" in q or "record" in q:
            return f"The fact table has **{len(fact):,} rows**."

    if any(w in q for w in ["pending", "approval"]):
        n = len(headers[headers["STATUS"] == "PENDING_APPROVAL"])
        return f"There are **{n}** adjustments pending approval."

    if any(w in q for w in ["largest", "biggest", "max"]):
        m = meas_keys[0]
        mx = fact[m].max()
        return f"The largest **{m}** value is **{format_number(mx)}**."

    if any(w in q for w in ["average", "mean"]):
        m = meas_keys[0]
        avg = fact[m].mean()
        return f"The average **{m}** is **{format_number(avg)}**."

    if any(w in q for w in ["scope", "table"]):
        scopes = ", ".join([f"{s['icon']} {s['name']}" for s in SCOPES.values()])
        return f"Available scopes: {scopes}. Currently viewing **{scope_cfg['name']}**."

    return ("I can help with questions about adjustments, row counts, pending items, "
            "and data statistics. Try: *'How many pending adjustments?'* or *'What is the largest MTM?'*")


# ─────────────────────────────────────────────────────────────────────
# TABS
# ─────────────────────────────────────────────────────────────────────
tab_chat, tab_summary, tab_anomaly, tab_risk = st.tabs([
    "💬 Chat", "📊 Summary", "🔍 Anomaly Detection", "⚠️ Risk Classification"
])

# ─────────────────────────────────────────────────────────────────────
# CHAT TAB
# ─────────────────────────────────────────────────────────────────────
with tab_chat:
    if "chat_history" not in st.session_state:
        st.session_state["chat_history"] = []

    for msg in st.session_state["chat_history"]:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"], unsafe_allow_html=True)

    prompt = st.chat_input("Ask about adjustments, data, or patterns…")
    if prompt:
        st.session_state["chat_history"].append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        response = _process_query(prompt, cfg)
        st.session_state["chat_history"].append({"role": "assistant", "content": response})
        with st.chat_message("assistant"):
            st.markdown(response, unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────
# SUMMARY TAB
# ─────────────────────────────────────────────────────────────────────
with tab_summary:
    section_header("Scope Summary")
    fact = get_fact_table()
    headers = get_headers()
    meas_keys = [m["key"] for m in cfg["measures"]]

    cols = st.columns(3)
    cols[0].markdown(metric_card("Fact Rows", f"{len(fact):,}"), unsafe_allow_html=True)
    cols[1].markdown(metric_card("Adjustments", len(headers)), unsafe_allow_html=True)
    cols[2].markdown(metric_card("Dimensions", len(cfg["dimensions"])), unsafe_allow_html=True)

    section_header("Measure Statistics")
    stats = fact[meas_keys].describe().T
    stats.columns = ["Count", "Mean", "Std", "Min", "25%", "50%", "75%", "Max"]
    st.dataframe(stats.style.format("{:,.0f}"), use_container_width=True)

    section_header("Adjustments by Status")
    if not headers.empty:
        status_counts = headers["STATUS"].value_counts()
        for status, count in status_counts.items():
            badge = status_badge(status)
            st.markdown(f"{badge} &nbsp; **{count}**", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────
# ANOMALY DETECTION TAB
# ─────────────────────────────────────────────────────────────────────
with tab_anomaly:
    section_header("Statistical Anomaly Detection")
    st.caption("Identifies values beyond 2 standard deviations from the mean.")

    fact = get_fact_table()
    meas_keys = [m["key"] for m in cfg["measures"]]

    anomalies_found = False
    for m in meas_keys:
        mean = fact[m].mean()
        std = fact[m].std()
        threshold = 2.0
        mask = (fact[m] - mean).abs() > threshold * std
        outliers = fact[mask]
        if not outliers.empty:
            anomalies_found = True
            st.markdown(f"""
            <div class="card" style="border-left:4px solid #FFA726">
                <strong style="color:#E65100">⚠️ {len(outliers)} anomalies</strong> in <strong>{m}</strong>
                <div style="font-size:.82rem;color:#607D8B;margin-top:4px">
                    Mean: {format_number(mean)} · Std: {format_number(std)} · Threshold: ±{threshold}σ
                </div>
            </div>
            """, unsafe_allow_html=True)
            with st.expander(f"View {m} outlier rows"):
                dim_cols = [d["key"] for d in cfg["dimensions"]]
                show = ["AS_OF_DATE"] + dim_cols + [m]
                st.dataframe(outliers[[c for c in show if c in outliers.columns]],
                            use_container_width=True, hide_index=True)
    if not anomalies_found:
        st.success("No statistical anomalies detected in the current data.")

# ─────────────────────────────────────────────────────────────────────
# RISK CLASSIFICATION TAB
# ─────────────────────────────────────────────────────────────────────
with tab_risk:
    section_header("Adjustment Risk Classification")
    st.caption("Classifies adjustments by potential risk based on impact magnitude.")

    headers = get_headers()
    adj_lines = get_lines()

    if headers.empty:
        st.info("No adjustments to classify.")
    else:
        for _, row in headers.iterrows():
            adj_id = row["ADJ_ID"]
            adj_l = adj_lines[adj_lines["ADJ_ID"] == adj_id]
            total_delta = adj_l["DELTA_VALUE"].abs().sum() if "DELTA_VALUE" in adj_l.columns and not adj_l.empty else 0

            if total_delta > 2_000_000:
                risk = "HIGH"
                risk_color = "#EF5350"
                risk_icon = "🔴"
            elif total_delta > 500_000:
                risk = "MEDIUM"
                risk_color = "#FFA726"
                risk_icon = "🟡"
            else:
                risk = "LOW"
                risk_color = "#66BB6A"
                risk_icon = "🟢"

            st.markdown(f"""
            <div class="card" style="display:flex;justify-content:space-between;align-items:center;
                         border-left:4px solid {risk_color}">
                <div>
                    <strong style="color:#0D47A1">{adj_id}</strong> · {row['ADJ_TYPE']}
                    &nbsp; {status_badge(row['STATUS'])}
                    <div style="font-size:.8rem;color:#607D8B;margin-top:2px">{row['JUSTIFICATION']}</div>
                </div>
                <div style="text-align:right">
                    <span style="font-size:1.3rem">{risk_icon}</span>
                    <div style="font-size:.8rem;font-weight:700;color:{risk_color}">{risk} RISK</div>
                    <div style="font-size:.75rem;color:#607D8B">Δ {format_number(total_delta)}</div>
                </div>
            </div>
            """, unsafe_allow_html=True)
