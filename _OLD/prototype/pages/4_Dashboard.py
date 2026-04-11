"""
📊 Dashboard — visual analytics
"""
import streamlit as st
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from _OLD.prototype.data.state_manager import (init_state, get_fact_table, get_headers, get_lines,
                                 get_fact_adjusted, current_scope_cfg)
from _OLD.prototype.data.styles import inject_css, section_header, top_navbar, scope_and_user_controls, metric_card, format_number
from _OLD.prototype.data.mock_data import SCOPES
import pandas as pd
import numpy as np

st.set_page_config(page_title="Dashboard", page_icon="📊", layout="wide", initial_sidebar_state="collapsed")
inject_css()
init_state()
top_navbar(active_page="Dashboard")
scope_id = scope_and_user_controls()
cfg = current_scope_cfg()

st.markdown(f"""
<div style="display:flex;align-items:center;gap:10px;margin-bottom:4px">
    <span style="font-size:2rem">📊</span>
    <h1 style="margin:0;font-size:1.6rem;color:#2D2D2D">Dashboard</h1>
</div>
<span style="color:#6B6B6B;font-size:.88rem">Scope: <strong>{cfg['icon']} {cfg['name']}</strong></span>
""", unsafe_allow_html=True)
st.markdown("---")

fact_all = get_fact_table()
adjusted_all = get_fact_adjusted()
headers = get_headers()
adj_lines = get_lines()
meas_keys = [m["key"] for m in cfg["measures"]]
dim_keys = [d["key"] for d in cfg["dimensions"]]

# ── COB Filter ──────────────────────────────────────────────────────
all_cobs = sorted(fact_all["AS_OF_DATE"].unique().tolist())
cob_selection = st.multiselect("**Filter by COB**", all_cobs, key="dash_cob_filter")

if cob_selection:
    fact = fact_all[fact_all["AS_OF_DATE"].isin(cob_selection)]
    adjusted = adjusted_all[adjusted_all["AS_OF_DATE"].isin(cob_selection)]
else:
    fact = fact_all
    adjusted = adjusted_all

# ── KPI row ─────────────────────────────────────────────────────────
section_header("Key Metrics")
kpi_cols = st.columns(4)

total_original = fact[meas_keys[0]].sum() if meas_keys else 0
total_adjusted = adjusted[meas_keys[0]].sum() if meas_keys else 0
impact = total_adjusted - total_original
impact_pct = (impact / total_original * 100) if total_original != 0 else 0

kpi_cols[0].markdown(metric_card("Original Total", format_number(total_original)), unsafe_allow_html=True)
kpi_cols[1].markdown(metric_card("Adjusted Total", format_number(total_adjusted)), unsafe_allow_html=True)
delta_str = f"{impact_pct:+.1f}%"
kpi_cols[2].markdown(metric_card("Net Impact", format_number(impact), delta_str), unsafe_allow_html=True)
kpi_cols[3].markdown(metric_card("Adjustments", len(headers)), unsafe_allow_html=True)

# ── Charts ──────────────────────────────────────────────────────────
try:
    import plotly.express as px
    import plotly.graph_objects as go
    HAS_PLOTLY = True
except ImportError:
    HAS_PLOTLY = False

if HAS_PLOTLY and not headers.empty:
    c1, c2 = st.columns(2)

    # Adjustments by type
    with c1:
        section_header("By Adjustment Type")
        type_counts = headers["ADJ_TYPE"].value_counts().reset_index()
        type_counts.columns = ["Type", "Count"]
        fig = px.bar(type_counts, x="Type", y="Count",
                     color="Type",
                     color_discrete_map={"FLATTEN": "#D50032", "SCALE": "#58595B", "ROLL": "#00875A"},
                     template="plotly_white")
        fig.update_layout(showlegend=False, margin=dict(t=10, b=10, l=10, r=10), height=280)
        st.plotly_chart(fig, use_container_width=True)

    # Adjustments by status
    with c2:
        section_header("By Status")
        from _OLD.prototype.data.state_manager import STATUS_COLORS
        status_counts = headers["STATUS"].value_counts().reset_index()
        status_counts.columns = ["Status", "Count"]
        colors = [STATUS_COLORS.get(s, "#78909C") for s in status_counts["Status"]]
        fig = px.pie(status_counts, names="Status", values="Count",
                     color_discrete_sequence=colors,
                     template="plotly_white")
        fig.update_layout(margin=dict(t=10, b=10, l=10, r=10), height=280)
        st.plotly_chart(fig, use_container_width=True)

    # Measure comparison by first dimension
    if dim_keys and meas_keys:
        section_header(f"{meas_keys[0]} by {cfg['dimensions'][0]['label']}")
        first_dim = dim_keys[0]
        first_meas = meas_keys[0]

        orig_by_dim = fact.groupby(first_dim)[first_meas].sum().reset_index()
        orig_by_dim.columns = [first_dim, "Original"]
        adj_by_dim = adjusted.groupby(first_dim)[first_meas].sum().reset_index()
        adj_by_dim.columns = [first_dim, "Adjusted"]
        merged = orig_by_dim.merge(adj_by_dim, on=first_dim, how="outer").fillna(0)

        fig = go.Figure()
        fig.add_trace(go.Bar(name="Original", x=merged[first_dim], y=merged["Original"],
                             marker_color="#C4C5C7"))
        fig.add_trace(go.Bar(name="Adjusted", x=merged[first_dim], y=merged["Adjusted"],
                             marker_color="#D50032"))
        fig.update_layout(barmode="group", template="plotly_white",
                         margin=dict(t=10, b=10, l=10, r=10), height=320,
                         legend=dict(orientation="h", y=-0.15))
        st.plotly_chart(fig, use_container_width=True)

    # Time series
    if "AS_OF_DATE" in fact.columns and meas_keys:
        section_header(f"{meas_keys[0]} Over Time")
        first_meas = meas_keys[0]
        ts_orig = fact.groupby("AS_OF_DATE")[first_meas].sum().reset_index()
        ts_orig.columns = ["Date", "Original"]
        ts_adj = adjusted.groupby("AS_OF_DATE")[first_meas].sum().reset_index()
        ts_adj.columns = ["Date", "Adjusted"]
        ts = ts_orig.merge(ts_adj, on="Date", how="outer").fillna(0)

        fig = go.Figure()
        fig.add_trace(go.Scatter(name="Original", x=ts["Date"], y=ts["Original"],
                                 mode="lines+markers", line=dict(color="#C4C5C7", width=2)))
        fig.add_trace(go.Scatter(name="Adjusted", x=ts["Date"], y=ts["Adjusted"],
                                 mode="lines+markers", line=dict(color="#D50032", width=2)))
        fig.update_layout(template="plotly_white",
                         margin=dict(t=10, b=10, l=10, r=10), height=300,
                         legend=dict(orientation="h", y=-0.2))
        st.plotly_chart(fig, use_container_width=True)

elif not HAS_PLOTLY:
    st.warning("Install `plotly` for interactive charts: `pip install plotly`")

# ── Raw data toggle ─────────────────────────────────────────────────
with st.expander("📄 View Raw Data"):
    tab_o, tab_a = st.tabs(["Original", "Adjusted"])
    with tab_o:
        st.dataframe(fact, use_container_width=True, hide_index=True)
    with tab_a:
        st.dataframe(adjusted, use_container_width=True, hide_index=True)
