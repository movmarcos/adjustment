"""
🏠 Home — Fact Table Adjustment Engine
"""
import streamlit as st
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from data.state_manager import init_state, get_fact_table, get_headers, get_lines, get_fact_adjusted, current_scope_cfg, STATUS_COLORS
from data.styles import inject_css, metric_card, status_badge, section_header, top_navbar, scope_and_user_controls, format_number
from data.mock_data import SCOPES

st.set_page_config(page_title="MUFG Adjustment Engine", page_icon="⚙️", layout="wide", initial_sidebar_state="collapsed")
inject_css()
init_state()
top_navbar(active_page="Home")
scope_id = scope_and_user_controls()
cfg = current_scope_cfg()

# ── Header ──────────────────────────────────────────────────────────
st.markdown(f"""
<div style="display:flex;align-items:center;gap:14px;margin-bottom:6px">
    <span style="font-size:2.4rem">{cfg['icon']}</span>
    <div>
        <h1 style="margin:0;padding:0;font-size:1.8rem;color:#2D2D2D">
            {cfg['name']} Adjustments
        </h1>
        <span style="color:#6B6B6B;font-size:.9rem">{cfg['table']} &nbsp;·&nbsp; {cfg['description']}</span>
    </div>
</div>
""", unsafe_allow_html=True)

st.markdown("---")

# ── KPIs ────────────────────────────────────────────────────────────
headers = get_headers()
fact = get_fact_table()
meas_keys = [m["key"] for m in cfg["measures"]]

total_adj   = len(headers)
pending     = len(headers[headers["STATUS"] == "PENDING_APPROVAL"])
applied     = len(headers[headers["STATUS"] == "APPLIED"])
fact_rows   = len(fact)

cols = st.columns(4)
cards = [
    ("Total Adjustments", total_adj, None, ""),
    ("Pending Approval",  pending,   None, ""),
    ("Applied",           applied,   None, ""),
    ("Fact Rows",         f"{fact_rows:,}", None, ""),
]
for col, (label, val, delta, px) in zip(cols, cards):
    col.markdown(metric_card(label, val, delta, px), unsafe_allow_html=True)

# ── Recent adjustments ──────────────────────────────────────────────
section_header("Recent Adjustments")

if headers.empty:
    st.info("No adjustments yet. Go to **Apply Adjustment** to create one.")
else:
    for _, row in headers.sort_values("CREATED_AT", ascending=False).head(5).iterrows():
        badge = status_badge(row["STATUS"])
        adj_type_icons = {"FLATTEN": "📉", "SCALE": "📐", "ROLL": "🔄"}
        icon = adj_type_icons.get(row["ADJ_TYPE"], "📝")
        st.markdown(f"""
        <div class="card" style="display:flex;justify-content:space-between;align-items:center">
            <div style="display:flex;align-items:center;gap:12px">
                <span style="font-size:1.5rem">{icon}</span>
                <div>
                    <strong style="color:#D50032">{row['ADJ_ID']}</strong>
                    &nbsp;{badge}&nbsp;
                    <span style="color:#6B6B6B;font-size:.82rem">{row['ADJ_TYPE']}</span>
                    <div style="font-size:.8rem;color:#6B6B6B;margin-top:2px">{row['JUSTIFICATION']}</div>
                </div>
            </div>
            <div style="text-align:right;font-size:.78rem;color:#6B6B6B">
                {row['BUSINESS_DATE']}<br/>
                by <strong>{row['CREATED_BY']}</strong>
            </div>
        </div>
        """, unsafe_allow_html=True)

# ── Quick data preview ──────────────────────────────────────────────
section_header("Data Preview")

tab_orig, tab_adj = st.tabs(["📄 Original Fact", "✅ Adjusted View"])
with tab_orig:
    st.dataframe(fact.head(30), use_container_width=True, hide_index=True)
with tab_adj:
    adj_fact = get_fact_adjusted()
    st.dataframe(adj_fact.head(30), use_container_width=True, hide_index=True)

# ── Scope info ──────────────────────────────────────────────────────
with st.expander("📋 Scope details"):
    d_cols = st.columns(2)
    with d_cols[0]:
        st.markdown("**Dimensions**")
        for d in cfg["dimensions"]:
            st.markdown(f"- **{d['label']}** — {len(d['values'])} values")
    with d_cols[1]:
        st.markdown("**Measures**")
        for m in cfg["measures"]:
            st.markdown(f"- **{m['label']}** ({m['unit']})")
