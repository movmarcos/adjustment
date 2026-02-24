"""
📝 Apply Adjustment — guided workflow
"""
import streamlit as st
import sys, os, json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from data.state_manager import (init_state, get_fact_table, preview_adjustment,
                                 create_adjustment, current_scope_cfg)
from data.styles import inject_css, section_header, scope_selector_sidebar, metric_card, format_number
from data.mock_data import SCOPES

st.set_page_config(page_title="Apply Adjustment", page_icon="📝", layout="wide")
inject_css()
init_state()
scope_id = scope_selector_sidebar()
cfg = current_scope_cfg()

st.markdown(f"""
<div style="display:flex;align-items:center;gap:10px;margin-bottom:4px">
    <span style="font-size:2rem">📝</span>
    <h1 style="margin:0;font-size:1.6rem;color:#0D47A1">Apply Adjustment</h1>
</div>
<span style="color:#607D8B;font-size:.88rem">Scope: <strong>{cfg['icon']} {cfg['name']}</strong></span>
""", unsafe_allow_html=True)

st.markdown("---")

# ── Step indicator ──────────────────────────────────────────────────
if "adj_step" not in st.session_state:
    st.session_state["adj_step"] = 1

steps = ["Filter", "Type & Params", "Justification", "Preview", "Submit"]
step = st.session_state["adj_step"]

cols = st.columns(len(steps))
for i, (col, label) in enumerate(zip(cols, steps), 1):
    if i < step:
        col.markdown(f'<div style="text-align:center"><span style="background:#66BB6A;color:#fff;border-radius:50%;padding:4px 10px;font-weight:700">{i}</span><br/><span style="font-size:.75rem;color:#66BB6A">{label}</span></div>', unsafe_allow_html=True)
    elif i == step:
        col.markdown(f'<div style="text-align:center"><span style="background:#0D47A1;color:#fff;border-radius:50%;padding:4px 10px;font-weight:700">{i}</span><br/><span style="font-size:.75rem;font-weight:700;color:#0D47A1">{label}</span></div>', unsafe_allow_html=True)
    else:
        col.markdown(f'<div style="text-align:center"><span style="background:#E0E4EA;color:#607D8B;border-radius:50%;padding:4px 10px;font-weight:700">{i}</span><br/><span style="font-size:.75rem;color:#607D8B">{label}</span></div>', unsafe_allow_html=True)

st.markdown("<br/>", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────
# STEP 1 — FILTERS
# ─────────────────────────────────────────────────────────────────────
if step == 1:
    section_header("Step 1 · Select Filters")
    st.caption("Choose which rows to adjust. Leave blank to select all.")

    fact = get_fact_table()
    filters = {}

    cols = st.columns(2)
    for i, dim in enumerate(cfg["dimensions"]):
        with cols[i % 2]:
            selected = st.multiselect(
                f"**{dim['label']}** (`{dim['key']}`)",
                options=dim["values"],
                key=f"filter_{dim['key']}",
            )
            if selected:
                filters[dim["key"]] = selected

    # Date filter
    dates = sorted(fact["AS_OF_DATE"].unique())
    sel_date = st.selectbox("**Business Date**", dates, key="filter_date")
    if sel_date:
        filters["AS_OF_DATE"] = sel_date

    st.session_state["adj_filters"] = filters

    # Preview count
    mask = fact.index >= 0  # all true
    import pandas as pd
    mask = pd.Series(True, index=fact.index)
    for k, v in filters.items():
        if k in fact.columns:
            mask &= fact[k].isin(v) if isinstance(v, list) else fact[k] == v
    n_match = mask.sum()

    st.markdown(f"""
    <div class="card" style="background:#E3F2FD;border-color:#90CAF9;margin-top:12px">
        <strong style="color:#0D47A1">🎯 {n_match} rows</strong> match your filters
    </div>
    """, unsafe_allow_html=True)

    col1, col2 = st.columns([6, 1])
    with col2:
        if st.button("Next →", type="primary", use_container_width=True):
            st.session_state["adj_step"] = 2
            st.rerun()

# ─────────────────────────────────────────────────────────────────────
# STEP 2 — ADJUSTMENT TYPE
# ─────────────────────────────────────────────────────────────────────
elif step == 2:
    section_header("Step 2 · Adjustment Type & Parameters")

    type_cols = st.columns(3)
    types = [
        ("FLATTEN", "📉", "Set all values to zero", "Applies a delta equal to the negative of the current value."),
        ("SCALE",   "📐", "Multiply by a factor", "Applies a delta = value × (factor − 1)."),
        ("ROLL",    "🔄", "Copy from another date", "Copies values from a source date, optionally scaled."),
    ]
    adj_type = st.session_state.get("adj_type", "FLATTEN")

    for col, (t, ico, short, desc) in zip(type_cols, types):
        active = "border-color:#0D47A1;background:#E3F2FD" if adj_type == t else ""
        with col:
            st.markdown(f"""
            <div class="card" style="text-align:center;cursor:pointer;{active}">
                <span style="font-size:2rem">{ico}</span>
                <div style="font-weight:700;margin:6px 0">{t}</div>
                <div style="font-size:.8rem;color:#607D8B">{short}</div>
            </div>
            """, unsafe_allow_html=True)

    adj_type = st.radio("Select type", ["FLATTEN", "SCALE", "ROLL"], horizontal=True,
                        index=["FLATTEN", "SCALE", "ROLL"].index(adj_type),
                        label_visibility="collapsed")
    st.session_state["adj_type"] = adj_type

    params = {}
    if adj_type == "SCALE":
        params["factor"] = st.slider("Scale factor", 0.0, 5.0, 1.1, 0.05)
    elif adj_type == "ROLL":
        fact = get_fact_table()
        dates = sorted(fact["AS_OF_DATE"].unique())
        params["source_date"] = st.selectbox("Source date to copy from", dates)
        params["scale"] = st.slider("Scale after roll", 0.0, 5.0, 1.0, 0.05)
    st.session_state["adj_params"] = params

    c1, c2, c3 = st.columns([1, 5, 1])
    with c1:
        if st.button("← Back", use_container_width=True):
            st.session_state["adj_step"] = 1
            st.rerun()
    with c3:
        if st.button("Next →", type="primary", use_container_width=True):
            st.session_state["adj_step"] = 3
            st.rerun()

# ─────────────────────────────────────────────────────────────────────
# STEP 3 — JUSTIFICATION
# ─────────────────────────────────────────────────────────────────────
elif step == 3:
    section_header("Step 3 · Justification & Business Date")

    justification = st.text_area("**Justification** (required)", height=100,
                                 placeholder="Explain why this adjustment is needed…",
                                 key="adj_justification_input")
    biz_date = st.date_input("**Business Date**", key="adj_biz_date_input")

    st.session_state["adj_justification"] = justification
    st.session_state["adj_biz_date"] = str(biz_date)

    c1, c2, c3 = st.columns([1, 5, 1])
    with c1:
        if st.button("← Back", use_container_width=True):
            st.session_state["adj_step"] = 2
            st.rerun()
    with c3:
        if st.button("Next →", type="primary", use_container_width=True, disabled=not justification):
            st.session_state["adj_step"] = 4
            st.rerun()

# ─────────────────────────────────────────────────────────────────────
# STEP 4 — PREVIEW
# ─────────────────────────────────────────────────────────────────────
elif step == 4:
    section_header("Step 4 · Preview Changes")

    filters = st.session_state.get("adj_filters", {})
    adj_type = st.session_state.get("adj_type", "FLATTEN")
    params = st.session_state.get("adj_params", {})

    preview = preview_adjustment(filters, adj_type, params)
    meas_keys = [m["key"] for m in cfg["measures"]]

    if preview.empty:
        st.warning("No rows matched. Go back and adjust filters.")
    else:
        # Summary metrics
        m_cols = st.columns(len(meas_keys))
        for col, m in zip(m_cols, meas_keys):
            orig_sum = preview[f"ORIG_{m}"].sum() if f"ORIG_{m}" in preview.columns else 0
            new_sum = preview[f"NEW_{m}"].sum() if f"NEW_{m}" in preview.columns else 0
            delta = new_sum - orig_sum
            delta_str = f"+{format_number(delta)}" if delta >= 0 else format_number(delta)
            col.markdown(metric_card(m, format_number(new_sum), delta_str), unsafe_allow_html=True)

        # Detail table
        show_cols = []
        for d in cfg["dimensions"]:
            if d["key"] in preview.columns:
                show_cols.append(d["key"])
        show_cols.append("AS_OF_DATE")
        for m in meas_keys:
            for suffix in ["ORIG_", "DELTA_", "NEW_"]:
                c = f"{suffix}{m}"
                if c in preview.columns:
                    show_cols.append(c)

        st.dataframe(preview[[c for c in show_cols if c in preview.columns]].head(50),
                     use_container_width=True, hide_index=True)

    c1, c2, c3 = st.columns([1, 5, 1])
    with c1:
        if st.button("← Back", use_container_width=True):
            st.session_state["adj_step"] = 3
            st.rerun()
    with c3:
        if st.button("Next →", type="primary", use_container_width=True, disabled=preview.empty):
            st.session_state["adj_step"] = 5
            st.rerun()

# ─────────────────────────────────────────────────────────────────────
# STEP 5 — SUBMIT
# ─────────────────────────────────────────────────────────────────────
elif step == 5:
    section_header("Step 5 · Confirm & Submit")

    filters = st.session_state.get("adj_filters", {})
    adj_type = st.session_state.get("adj_type", "FLATTEN")
    params = st.session_state.get("adj_params", {})
    justification = st.session_state.get("adj_justification", "")
    biz_date = st.session_state.get("adj_biz_date", "")

    st.markdown(f"""
    <div class="card">
        <table style="width:100%;font-size:.9rem">
        <tr><td style="color:#607D8B;padding:4px 12px 4px 0">Scope</td><td><strong>{cfg['icon']} {cfg['name']}</strong></td></tr>
        <tr><td style="color:#607D8B;padding:4px 12px 4px 0">Type</td><td><strong>{adj_type}</strong></td></tr>
        <tr><td style="color:#607D8B;padding:4px 12px 4px 0">Business Date</td><td>{biz_date}</td></tr>
        <tr><td style="color:#607D8B;padding:4px 12px 4px 0">Justification</td><td>{justification}</td></tr>
        <tr><td style="color:#607D8B;padding:4px 12px 4px 0">Filters</td><td><code>{json.dumps(filters, default=str)}</code></td></tr>
        <tr><td style="color:#607D8B;padding:4px 12px 4px 0">Parameters</td><td><code>{json.dumps(params, default=str)}</code></td></tr>
        </table>
    </div>
    """, unsafe_allow_html=True)

    c1, c2, c3 = st.columns([1, 4, 2])
    with c1:
        if st.button("← Back", use_container_width=True):
            st.session_state["adj_step"] = 4
            st.rerun()
    with c3:
        if st.button("✅ Create Adjustment", type="primary", use_container_width=True):
            adj_id = create_adjustment(filters, adj_type, params, justification, biz_date)
            st.session_state["adj_step"] = 1
            st.success(f"Adjustment **{adj_id}** created successfully!")
            st.balloons()
