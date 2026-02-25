"""
📝 Apply Adjustment — guided workflow
"""
import streamlit as st
import sys, os, json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from data.state_manager import (init_state, get_fact_table, preview_adjustment,
                                 create_adjustment, current_scope_cfg)
from data.styles import inject_css, section_header, top_navbar, scope_and_user_controls, metric_card, format_number
from data.mock_data import SCOPES

st.set_page_config(page_title="Apply Adjustment", page_icon="📝", layout="wide", initial_sidebar_state="collapsed")
inject_css()
init_state()
top_navbar(active_page="Apply Adjustment")
scope_id = scope_and_user_controls()
cfg = current_scope_cfg()

st.markdown(f"""
<div style="display:flex;align-items:center;gap:10px;margin-bottom:4px">
    <span style="font-size:2rem">📝</span>
    <h1 style="margin:0;font-size:1.6rem;color:#2D2D2D">Apply Adjustment</h1>
</div>
<span style="color:#6B6B6B;font-size:.88rem">Scope: <strong>{cfg['icon']} {cfg['name']}</strong></span>
""", unsafe_allow_html=True)

st.markdown("---")

# ── Step indicator ──────────────────────────────────────────────────
if "adj_step" not in st.session_state:
    st.session_state["adj_step"] = 1

steps = ["Configure", "Preview", "Submit"]
step = st.session_state["adj_step"]

cols = st.columns(len(steps))
for i, (col, label) in enumerate(zip(cols, steps), 1):
    if i < step:
        col.markdown(f'<div style="text-align:center"><span style="background:#00875A;color:#fff;border-radius:50%;padding:4px 10px;font-weight:700">{i}</span><br/><span style="font-size:.75rem;color:#00875A">{label}</span></div>', unsafe_allow_html=True)
    elif i == step:
        col.markdown(f'<div style="text-align:center"><span style="background:#D50032;color:#fff;border-radius:50%;padding:4px 10px;font-weight:700">{i}</span><br/><span style="font-size:.75rem;font-weight:700;color:#D50032">{label}</span></div>', unsafe_allow_html=True)
    else:
        col.markdown(f'<div style="text-align:center"><span style="background:#E5E5E7;color:#6B6B6B;border-radius:50%;padding:4px 10px;font-weight:700">{i}</span><br/><span style="font-size:.75rem;color:#6B6B6B">{label}</span></div>', unsafe_allow_html=True)

st.markdown("<br/>", unsafe_allow_html=True)

import pandas as pd

# ─────────────────────────────────────────────────────────────────────
# STEP 1 — CONFIGURE (Type → Justification → Filters)
# ─────────────────────────────────────────────────────────────────────
if step == 1:
    fact = get_fact_table()
    all_cobs = sorted(fact["AS_OF_DATE"].unique())

    # ── 1A  Adjustment Type & Parameters ─────────────────────────────
    section_header("Adjustment Type & Parameters")

    type_cols = st.columns(3)
    types = [
        ("FLATTEN", "📉", "Set all values to zero", "Delta = −current value"),
        ("SCALE",   "📐", "Multiply by a factor",   "Delta = value × (factor − 1)"),
        ("ROLL",    "🔄", "Copy from another date",  "Copies values, optionally scaled"),
    ]
    adj_type = st.session_state.get("adj_type", "FLATTEN")

    for col, (t, ico, short, desc) in zip(type_cols, types):
        active = "border-color:#D50032;background:#FFF5F7" if adj_type == t else ""
        with col:
            st.markdown(f"""
            <div class="card" style="text-align:center;cursor:pointer;{active}">
                <span style="font-size:2rem">{ico}</span>
                <div style="font-weight:700;margin:6px 0">{t}</div>
                <div style="font-size:.78rem;color:#607D8B">{short}</div>
                <div style="font-size:.7rem;color:#90A4AE;margin-top:2px">{desc}</div>
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
        params["scale"] = st.slider("Scale after roll", 0.0, 5.0, 1.0, 0.05)
    st.session_state["adj_params"] = params

    st.markdown("<div style='margin-top:8px'></div>", unsafe_allow_html=True)

    # ── 1B  Frequency ────────────────────────────────────────────────
    section_header("Frequency")

    freq_cols = st.columns(2)
    freq_options = ["ADHOC", "RECURRING"]
    frequency = st.session_state.get("adj_frequency", "ADHOC")
    with freq_cols[0]:
        for f_opt in freq_options:
            f_ico = "🔹" if f_opt == "ADHOC" else "🔁"
            f_desc = "One-time adjustment" if f_opt == "ADHOC" else "Repeats across a COB range"
            active_style = "border-color:#D50032;background:#FFF5F7" if frequency == f_opt else ""
            st.markdown(f"""
            <div class="card" style="text-align:center;cursor:pointer;padding:10px;{active_style}">
                <span style="font-size:1.4rem">{f_ico}</span>
                <div style="font-weight:700;margin:4px 0;font-size:.9rem">{f_opt}</div>
                <div style="font-size:.75rem;color:#607D8B">{f_desc}</div>
            </div>
            """, unsafe_allow_html=True)
    with freq_cols[1]:
        frequency = st.radio("Select frequency", freq_options, horizontal=True,
                             index=freq_options.index(frequency),
                             label_visibility="collapsed", key="freq_radio")
        st.session_state["adj_frequency"] = frequency

        if frequency == "RECURRING":
            rc1, rc2 = st.columns(2)
            with rc1:
                start_cob_sel = st.multiselect("**Start COB**", all_cobs,
                                                default=[all_cobs[0]] if all_cobs else [],
                                                max_selections=1, key="freq_start_cob")
            with rc2:
                end_cob_sel = st.multiselect("**End COB** (optional — leave empty for open-ended)",
                                              all_cobs, max_selections=1, key="freq_end_cob")
            st.session_state["adj_start_cob"] = start_cob_sel[0] if start_cob_sel else ""
            st.session_state["adj_end_cob"] = end_cob_sel[0] if end_cob_sel else ""
        else:
            st.session_state["adj_start_cob"] = ""
            st.session_state["adj_end_cob"] = ""

    st.markdown("<div style='margin-top:8px'></div>", unsafe_allow_html=True)

    # ── 1C  Justification ────────────────────────────────────────────
    section_header("Justification")

    justification = st.text_area("**Justification** (required)", height=80,
                                 placeholder="Explain why this adjustment is needed…",
                                 key="adj_justification_input")
    st.session_state["adj_justification"] = justification

    st.markdown("<div style='margin-top:8px'></div>", unsafe_allow_html=True)

    # ── 1D  Filters ──────────────────────────────────────────────────
    section_header("Filters")
    st.caption("Select the COB date to adjust and narrow which rows to include.")

    filters = {}

    # COB selectors (multiselect with max_selections=1 for type-to-search)
    if adj_type == "ROLL":
        cob_col1, cob_col2 = st.columns(2)
        with cob_col1:
            target_sel = st.multiselect("**Target COB** (date to adjust)", all_cobs,
                                        default=[all_cobs[-1]], max_selections=1,
                                        key="filter_target_cob")
        with cob_col2:
            target_cob = target_sel[0] if target_sel else all_cobs[-1]
            source_cobs = [d for d in all_cobs if d != target_cob]
            source_sel = st.multiselect("**Source COB** (date to copy from)", source_cobs,
                                        max_selections=1, key="filter_source_cob")
        target_cob = target_sel[0] if target_sel else all_cobs[-1]
        source_cob = source_sel[0] if source_sel else (source_cobs[0] if source_cobs else None)
        st.session_state["adj_params"]["source_date"] = source_cob
    else:
        target_sel = st.multiselect("**Target COB** (date to adjust)", all_cobs,
                                    default=[all_cobs[-1]], max_selections=1,
                                    key="filter_target_cob")
        target_cob = target_sel[0] if target_sel else all_cobs[-1]

    filters["AS_OF_DATE"] = target_cob
    st.session_state["adj_biz_date"] = target_cob

    # Dimension filters — single-select for Apply Adjustment
    filter_cols = st.columns(2)
    for i, dim in enumerate(cfg["dimensions"]):
        with filter_cols[i % 2]:
            selected = st.multiselect(
                f"**{dim['label']}** (`{dim['key']}`)",
                options=dim["values"],
                max_selections=1,
                key=f"filter_{dim['key']}",
            )
            if selected:
                filters[dim["key"]] = selected[0]

    st.session_state["adj_filters"] = filters

    # Preview match count
    mask = pd.Series(True, index=fact.index)
    for k, v in filters.items():
        if k in fact.columns:
            mask &= fact[k].isin(v) if isinstance(v, list) else fact[k] == v
    n_match = mask.sum()

    st.markdown(f"""
    <div class="card" style="background:#FFF5F7;border-color:#F5C6CE;margin-top:12px">
        <strong style="color:#D50032">🎯 {n_match} rows</strong> match your filters
    </div>
    """, unsafe_allow_html=True)

    # ── Next button ──────────────────────────────────────────────────
    col1, col2 = st.columns([6, 1])
    with col2:
        if st.button("Preview →", type="primary", use_container_width=True,
                     disabled=not justification):
            st.session_state["adj_step"] = 2
            st.rerun()
    if not justification:
        st.caption("⚠️ Justification is required before you can proceed.")

# ─────────────────────────────────────────────────────────────────────
# STEP 2 — PREVIEW
# ─────────────────────────────────────────────────────────────────────
elif step == 2:
    section_header("Step 2 · Preview Changes")

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
            st.session_state["adj_step"] = 1
            st.rerun()
    with c3:
        if st.button("Next →", type="primary", use_container_width=True, disabled=preview.empty):
            st.session_state["adj_step"] = 3
            st.rerun()

# ─────────────────────────────────────────────────────────────────────
# STEP 3 — SUBMIT
# ─────────────────────────────────────────────────────────────────────
elif step == 3:
    section_header("Step 3 · Confirm & Submit")

    filters = st.session_state.get("adj_filters", {})
    adj_type = st.session_state.get("adj_type", "FLATTEN")
    params = st.session_state.get("adj_params", {})
    justification = st.session_state.get("adj_justification", "")
    biz_date = st.session_state.get("adj_biz_date", "")

    frequency = st.session_state.get("adj_frequency", "ADHOC")
    start_cob = st.session_state.get("adj_start_cob", "")
    end_cob = st.session_state.get("adj_end_cob", "")
    freq_display = frequency
    if frequency == "RECURRING":
        end_label = end_cob if end_cob else "open-ended"
        freq_display = f"RECURRING ({start_cob} → {end_label})"

    st.markdown(f"""
    <div class="card">
        <table style="width:100%;font-size:.9rem">
        <tr><td style="color:#6B6B6B;padding:4px 12px 4px 0">Scope</td><td><strong>{cfg['icon']} {cfg['name']}</strong></td></tr>
        <tr><td style="color:#6B6B6B;padding:4px 12px 4px 0">Type</td><td><strong>{adj_type}</strong></td></tr>
        <tr><td style="color:#6B6B6B;padding:4px 12px 4px 0">Frequency</td><td><strong>{freq_display}</strong></td></tr>
        <tr><td style="color:#6B6B6B;padding:4px 12px 4px 0">Target COB</td><td>{biz_date}</td></tr>
        <tr><td style="color:#6B6B6B;padding:4px 12px 4px 0">Justification</td><td>{justification}</td></tr>
        <tr><td style="color:#6B6B6B;padding:4px 12px 4px 0">Filters</td><td><code>{json.dumps(filters, default=str)}</code></td></tr>
        <tr><td style="color:#607D8B;padding:4px 12px 4px 0">Parameters</td><td><code>{json.dumps(params, default=str)}</code></td></tr>
        </table>
    </div>
    """, unsafe_allow_html=True)

    c1, c2, c3 = st.columns([1, 4, 2])
    with c1:
        if st.button("← Back", use_container_width=True):
            st.session_state["adj_step"] = 2
            st.rerun()
    with c3:
        if st.button("✅ Create Adjustment", type="primary", use_container_width=True):
            adj_id = create_adjustment(filters, adj_type, params, justification, biz_date,
                                       frequency=frequency, start_cob=start_cob, end_cob=end_cob)
            st.session_state["adj_step"] = 1
            st.success(f"Adjustment **{adj_id}** created successfully!")
