"""
New Adjustment — Smart Wizard
===============================
4-step flow with AI co-pilot, real-time overlap detection, and live preview.
"""
import streamlit as st
import plotly.graph_objects as go
import pandas as pd
from datetime import date, timedelta

st.set_page_config(
    page_title="New Adjustment · MUFG",
    page_icon="✏️",
    layout="wide",
    initial_sidebar_state="expanded",
)

from data.state_manager import (
    init_state, reset_wizard, create_adjustment, update_status,
    check_overlaps, compute_preview, count_matched_rows,
    generate_ai_copilot, current_user, can_apply, dashboard_kpis,
    TYPE_LABELS, TYPE_FORMULA,
)
from data.mock_data import SCOPES, BUSINESS_DATES, LATEST_COB
from data.styles import (
    inject_css, render_sidebar, render_step_bar, render_filter_chips,
    render_ai_panel, render_overlap_warning, status_badge, section_title, fmt_number, P,
)

init_state()
inject_css()
render_sidebar()

# ──────────────────────────────────────────────────────────────────────────────
# HEADER
# ──────────────────────────────────────────────────────────────────────────────

h_col, btn_col = st.columns([4, 1])
with h_col:
    st.markdown("## ✏️ New Adjustment")
    st.markdown(f"<span style='color:{P['grey_700']};font-size:0.9rem'>Configure and submit a new data adjustment. The AI co-pilot will guide you through each step.</span>",
                unsafe_allow_html=True)
with btn_col:
    st.markdown("<br/>", unsafe_allow_html=True)
    if st.button("🔄 Reset Wizard", use_container_width=True):
        reset_wizard()
        st.rerun()

wiz = st.session_state["wizard"]

# ──────────────────────────────────────────────────────────────────────────────
# STEP INDICATORS
# ──────────────────────────────────────────────────────────────────────────────

STEPS = ["Source & Type", "Filters", "Preview", "Submit"]
render_step_bar(wiz["step"], STEPS)

st.markdown("<br/>", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# LAYOUT: wizard (left 2/3) + AI co-pilot (right 1/3)
# ──────────────────────────────────────────────────────────────────────────────

main_col, ai_col = st.columns([2.2, 1])

with ai_col:
    ai_content = generate_ai_copilot(wiz)
    render_ai_panel(ai_content)

# ══════════════════════════════════════════════════════════════════════════════
# STEP 1 — Source & Type
# ══════════════════════════════════════════════════════════════════════════════
with main_col:
    if wiz["step"] == 1:
        # ── Scope selector ──────────────────────────────────────────────────
        section_title("Select Data Source", "🔍")

        scope_cols = st.columns(3)
        for i, (sk, scope) in enumerate(SCOPES.items()):
            with scope_cols[i]:
                is_sel = wiz["scope_key"] == sk
                border = P["primary"] if is_sel else P["border"]
                bg     = scope["bg_color"] if is_sel else P["white"]
                st.markdown(
                    f'<div style="background:{bg};border:2px solid {border};border-radius:12px;'
                    f'padding:1.1rem;text-align:center;margin-bottom:0.5rem">'
                    f'<div style="font-size:2rem">{scope["icon"]}</div>'
                    f'<div style="font-weight:700;font-size:0.95rem;margin:0.35rem 0 0.15rem">'
                    f'{scope["full_label"]}</div>'
                    f'<div style="font-size:0.72rem;color:{P["grey_700"]};margin-bottom:0.5rem">'
                    f'{scope["source_system"]} · {len(scope["dimensions"])} dimensions</div>'
                    f'</div>',
                    unsafe_allow_html=True)
                if st.button(f'{"✓ Selected" if is_sel else "Select"} {scope["label"]}',
                             key=f"sel_scope_{sk}", use_container_width=True,
                             type="primary" if is_sel else "secondary"):
                    wiz["scope_key"] = sk
                    wiz["filter_criteria"] = {}
                    wiz["preview_data"] = None
                    wiz["overlaps"] = []
                    wiz["matched_rows"] = 0
                    st.rerun()

        if not wiz["scope_key"]:
            st.info("👆 Select a data source to continue.")

        if wiz["scope_key"]:
            # ── Adjustment Type ─────────────────────────────────────────────
            section_title("Adjustment Type", "⚡")

            type_cols = st.columns(4)
            TYPE_ICONS = {"FLATTEN": "🔴", "SCALE": "📊", "ROLL": "🔄", "COPY": "📋"}
            for i, (tkey, (tlabel, tdesc)) in enumerate(TYPE_LABELS.items()):
                with type_cols[i]:
                    is_sel = wiz["adj_type"] == tkey
                    border = P["primary"] if is_sel else P["border"]
                    bg     = "#FFF0F3" if is_sel else P["white"]
                    st.markdown(
                        f'<div style="background:{bg};border:2px solid {border};border-radius:10px;'
                        f'padding:0.9rem;text-align:center;min-height:100px">'
                        f'<div style="font-size:1.5rem">{TYPE_ICONS[tkey]}</div>'
                        f'<div style="font-weight:700;font-size:0.88rem;margin:0.3rem 0 0.15rem">{tlabel}</div>'
                        f'<div style="font-size:0.7rem;color:{P["grey_700"]}">{tdesc}</div>'
                        f'</div>',
                        unsafe_allow_html=True)
                    if st.button("✓ " + tlabel if is_sel else tlabel,
                                 key=f"sel_type_{tkey}", use_container_width=True,
                                 type="primary" if is_sel else "secondary"):
                        wiz["adj_type"] = tkey
                        wiz["preview_data"] = None
                        st.rerun()

            if wiz["adj_type"]:
                st.markdown(
                    f'<div style="background:{P["grey_100"]};border-radius:6px;padding:0.6rem 0.9rem;'
                    f'font-size:0.8rem;color:{P["grey_700"]};margin:0.5rem 0;font-family:monospace">'
                    f'Formula: {TYPE_FORMULA[wiz["adj_type"]]}</div>',
                    unsafe_allow_html=True)

            # ── Frequency ───────────────────────────────────────────────────
            section_title("Schedule", "📅")
            freq_col1, freq_col2 = st.columns([1, 2])
            with freq_col1:
                wiz["frequency"] = st.radio("Frequency", ["ADHOC", "RECURRING"],
                                            index=0 if wiz["frequency"] == "ADHOC" else 1,
                                            help="ADHOC: one-time. RECURRING: repeats each COB in the date range.",
                                            horizontal=True)
            if wiz["frequency"] == "RECURRING":
                with freq_col2:
                    r1, r2 = st.columns(2)
                    bd_str = [str(d) for d in BUSINESS_DATES]
                    with r1:
                        sc_idx = bd_str.index(str(wiz["start_cob"])) if str(wiz["start_cob"]) in bd_str else 0
                        wiz["start_cob"] = BUSINESS_DATES[
                            bd_str.index(st.selectbox("Start COB", bd_str, index=sc_idx, key="w_start_cob"))]
                    with r2:
                        ec_idx = bd_str.index(str(wiz["end_cob"])) if str(wiz["end_cob"]) in bd_str else len(bd_str)-1
                        wiz["end_cob"] = BUSINESS_DATES[
                            bd_str.index(st.selectbox("End COB", bd_str, index=ec_idx, key="w_end_cob"))]
                days_count = (wiz["end_cob"] - wiz["start_cob"]).days + 1
                st.markdown(f'<span class="tag recurring">RECURRING · {days_count} calendar days</span>',
                            unsafe_allow_html=True)

            # ── Scale / Roll params ─────────────────────────────────────────
            if wiz["adj_type"] in ("SCALE", "ROLL", "COPY"):
                section_title("Parameters", "🔧")
                p1, p2 = st.columns(2)
                with p1:
                    wiz["scale_factor"] = st.number_input(
                        "Scale Factor", value=float(wiz["scale_factor"]),
                        min_value=-10.0, max_value=100.0, step=0.01, format="%.4f",
                        help="1.0 = no change, 1.05 = +5%, 0.95 = -5%")
                with p2:
                    if wiz["adj_type"] in ("ROLL", "COPY"):
                        bd_str = [str(d) for d in BUSINESS_DATES]
                        src_default = str(wiz["roll_source_date"]) if wiz["roll_source_date"] else bd_str[-2]
                        src_idx = bd_str.index(src_default) if src_default in bd_str else len(bd_str) - 2
                        wiz["roll_source_date"] = BUSINESS_DATES[
                            bd_str.index(st.selectbox("Source COB Date", bd_str,
                                                       index=src_idx, key="w_roll_src"))]

            # ── Navigation ─────────────────────────────────────────────────
            st.markdown("<br/>", unsafe_allow_html=True)
            if wiz["adj_type"]:
                if st.button("Continue → Filters", type="primary", use_container_width=False):
                    wiz["step"] = 2
                    st.rerun()

# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 — Filters & Scope
# ══════════════════════════════════════════════════════════════════════════════
    elif wiz["step"] == 2:
        scope = SCOPES[wiz["scope_key"]]

        section_title("Target COB Date", "📅")
        bd_strs = [str(d) for d in BUSINESS_DATES]
        def_idx = bd_strs.index(str(wiz["target_date"])) if str(wiz["target_date"]) in bd_strs else len(bd_strs) - 1
        sel_str = st.selectbox("COB Date", bd_strs, index=def_idx, key="w_target_date")
        wiz["target_date"] = BUSINESS_DATES[bd_strs.index(sel_str)]

        # ── Dimension filters ───────────────────────────────────────────────
        section_title("Filter by Dimensions", "🔍")
        st.markdown(f'<span style="font-size:0.82rem;color:{P["grey_700"]}">Leave a filter empty to include all values for that dimension.</span>',
                    unsafe_allow_html=True)
        st.markdown("<br/>", unsafe_allow_html=True)

        filters = wiz.get("filter_criteria", {})
        dim_cols = st.columns(2)
        for i, dim in enumerate(scope["dimensions"]):
            col = dim["column"]
            with dim_cols[i % 2]:
                req_mark = " *" if dim["required"] else ""
                selected = filters.get(col, [])
                new_sel = st.multiselect(
                    f'{dim["label"]}{req_mark}',
                    options=dim["values"],
                    default=selected,
                    key=f"w_dim_{col}",
                    help=dim.get("tooltip", ""),
                    placeholder=f"All {dim['label'].lower()}s…"
                )
                filters[col] = new_sel

        wiz["filter_criteria"] = filters

        # Check required dimensions
        missing_required = [
            d["label"] for d in scope["dimensions"]
            if d["required"] and not filters.get(d["column"])
        ]

        # Live row count
        matched = count_matched_rows(wiz["scope_key"], filters, wiz["target_date"])
        wiz["matched_rows"] = matched

        count_color = P["success"] if matched > 0 else P["danger"]
        st.markdown(
            f'<div style="background:{P["grey_100"]};border-radius:8px;padding:0.75rem 1rem;'
            f'margin:0.75rem 0;display:flex;align-items:center;gap:12px">'
            f'<div style="font-size:1.5rem;font-weight:800;color:{count_color}">{matched}</div>'
            f'<div><div style="font-weight:600;font-size:0.85rem">records matched</div>'
            f'<div style="font-size:0.75rem;color:{P["grey_700"]}">on COB {wiz["target_date"].strftime("%d %b %Y")} '
            f'with your current filters</div></div>'
            f'</div>',
            unsafe_allow_html=True)

        # Overlap detection
        if matched > 0:
            overlaps = check_overlaps(wiz["scope_key"], wiz["target_date"], filters)
            wiz["overlaps"] = overlaps
            render_overlap_warning(overlaps)

        # Validation
        if missing_required:
            st.warning(f"Required fields missing: {', '.join(missing_required)}")

        # ── Navigation ─────────────────────────────────────────────────────
        st.markdown("<br/>", unsafe_allow_html=True)
        n1, n2 = st.columns([1, 4])
        with n1:
            if st.button("← Back", use_container_width=True):
                wiz["step"] = 1
                st.rerun()
        with n2:
            can_proceed = matched > 0 and not missing_required
            if st.button("Preview Impact →", type="primary", use_container_width=False,
                         disabled=not can_proceed):
                # Compute preview
                wiz["preview_data"] = compute_preview(
                    wiz["scope_key"], wiz["adj_type"], filters, wiz["target_date"],
                    wiz["scale_factor"], wiz.get("roll_source_date"))
                wiz["step"] = 3
                st.rerun()
            if not can_proceed and matched == 0:
                st.warning("No records matched — adjust your filters before continuing.")

# ══════════════════════════════════════════════════════════════════════════════
# STEP 3 — Preview
# ══════════════════════════════════════════════════════════════════════════════
    elif wiz["step"] == 3:
        scope  = SCOPES[wiz["scope_key"]]
        df     = wiz.get("preview_data")
        measures = scope["measures"]
        primary_m = next((m for m in measures if m["primary"]), measures[0])

        section_title("Impact Preview", "📊")

        if df is None or df.empty:
            st.warning("No data to preview — go back and adjust your filters.")
        else:
            # ── KPI summary ─────────────────────────────────────────────────
            m_orig = primary_m["column"] + "_original"
            m_delt = primary_m["column"] + "_delta"
            m_proj = primary_m["column"] + "_projected"

            orig_total  = df[m_orig].sum() if m_orig in df.columns else 0
            delt_total  = df[m_delt].sum() if m_delt in df.columns else 0
            proj_total  = df[m_proj].sum() if m_proj in df.columns else 0
            pct_change  = (delt_total / orig_total * 100) if orig_total != 0 else 0

            kpi_cols = st.columns(4)
            kpi_cols[0].metric("Records Affected", f"{len(df):,}")
            kpi_cols[1].metric("Original Total", fmt_number(orig_total, primary_m["fmt"]))
            kpi_cols[2].metric("Total Delta", fmt_number(delt_total, primary_m["fmt"]),
                               delta=f"{pct_change:+.2f}%")
            kpi_cols[3].metric("Projected Total", fmt_number(proj_total, primary_m["fmt"]))

            # ── Tabs: Chart | Table | All Measures ──────────────────────────
            tab_chart, tab_table, tab_measures = st.tabs(
                ["📈 Before vs After", "📋 Detail Table", "📐 All Measures"])

            with tab_chart:
                # Get grouping dimension (first dim with selections, or first dim)
                group_dim = None
                for dim in scope["dimensions"]:
                    col = dim["column"]
                    if col in df.columns and len(df[col].unique()) > 1:
                        group_dim = dim
                        break
                if group_dim is None:
                    group_dim = scope["dimensions"][0]

                gcol = group_dim["column"]
                if gcol in df.columns:
                    grp = df.groupby(gcol).agg(
                        orig=(m_orig, "sum"),
                        proj=(m_proj, "sum"),
                    ).reset_index() if m_orig in df.columns else pd.DataFrame()

                    if not grp.empty:
                        fig = go.Figure()
                        fig.add_trace(go.Bar(
                            name="Before (Original)",
                            x=grp[gcol], y=grp["orig"],
                            marker_color="#90A4AE",
                            hovertemplate="<b>%{x}</b><br>Original: %{y:,.0f}<extra></extra>",
                        ))
                        fig.add_trace(go.Bar(
                            name="After (Projected)",
                            x=grp[gcol], y=grp["proj"],
                            marker_color=P["primary"],
                            hovertemplate="<b>%{x}</b><br>Projected: %{y:,.0f}<extra></extra>",
                        ))
                        fig.update_layout(
                            barmode="group",
                            xaxis_title=group_dim["label"],
                            yaxis_title=primary_m["label"],
                            plot_bgcolor="white", paper_bgcolor="white",
                            legend=dict(orientation="h", yanchor="bottom", y=1),
                            margin=dict(l=10, r=10, t=30, b=10),
                            height=320, font_family="Inter",
                            xaxis=dict(showgrid=False),
                            yaxis=dict(showgrid=True, gridcolor="#F5F5F5"),
                        )
                        st.plotly_chart(fig, use_container_width=True,
                                        config={"displayModeBar": False})

            with tab_table:
                display_cols = (
                    [c for c in df.columns
                     if not c.endswith("_original") and not c.endswith("_delta")
                     and not c.endswith("_projected") and c not in ("fact_id", "source_system")]
                )
                for m in measures:
                    for sfx in ("_original", "_delta", "_projected"):
                        col = m["column"] + sfx
                        if col in df.columns:
                            display_cols.append(col)

                show_df = df[display_cols].copy()
                # Rename for readability
                renames = {}
                for m in measures:
                    renames[m["column"] + "_original"]  = f'{m["label"]} (Before)'
                    renames[m["column"] + "_delta"]     = f'{m["label"]} (Delta)'
                    renames[m["column"] + "_projected"] = f'{m["label"]} (After)'
                show_df = show_df.rename(columns=renames)
                st.dataframe(show_df, use_container_width=True, height=300)

            with tab_measures:
                for m in measures:
                    m_o = m["column"] + "_original"
                    m_d = m["column"] + "_delta"
                    m_p = m["column"] + "_projected"
                    if m_o not in df.columns:
                        continue
                    o, d, p = df[m_o].sum(), df[m_d].sum(), df[m_p].sum()
                    pct = (d / o * 100) if o != 0 else 0
                    c0, c1, c2, c3 = st.columns(4)
                    c0.markdown(f'<div style="font-weight:600;font-size:0.9rem;padding:0.5rem 0">'
                                f'{m["label"]}</div>', unsafe_allow_html=True)
                    c1.metric("Before", fmt_number(o, m["fmt"]))
                    c2.metric("Delta", fmt_number(d, m["fmt"]), delta=f"{pct:+.2f}%")
                    c3.metric("After", fmt_number(p, m["fmt"]))
                    st.markdown('<hr style="border-color:#F0F0F0;margin:0.3rem 0">', unsafe_allow_html=True)

        # ── Navigation ──────────────────────────────────────────────────────
        st.markdown("<br/>", unsafe_allow_html=True)
        n1, n2 = st.columns([1, 4])
        with n1:
            if st.button("← Back", use_container_width=True):
                wiz["step"] = 2
                st.rerun()
        with n2:
            if st.button("Continue → Submit", type="primary", use_container_width=False):
                wiz["step"] = 4
                st.rerun()

# ══════════════════════════════════════════════════════════════════════════════
# STEP 4 — Submit
# ══════════════════════════════════════════════════════════════════════════════
    elif wiz["step"] == 4:
        scope = SCOPES[wiz["scope_key"]]

        # ── Adjustment summary card ─────────────────────────────────────────
        section_title("Adjustment Summary", "📋")
        sum_col1, sum_col2 = st.columns(2)
        with sum_col1:
            st.markdown(
                f'<div class="mcard mcard-accent">'
                f'<div style="font-size:0.75rem;color:{P["grey_700"]};font-weight:600;text-transform:uppercase;letter-spacing:.06em;margin-bottom:0.7rem">Configuration</div>'
                f'<table style="width:100%;font-size:0.85rem;border-collapse:collapse">'
                f'<tr><td style="color:{P["grey_700"]};padding:3px 0;width:40%">Source</td><td><strong>{scope["icon"]} {scope["full_label"]}</strong></td></tr>'
                f'<tr><td style="color:{P["grey_700"]};padding:3px 0">Type</td><td><strong>{wiz["adj_type"]}</strong></td></tr>'
                f'<tr><td style="color:{P["grey_700"]};padding:3px 0">Schedule</td><td><strong>{wiz["frequency"]}</strong>'
                + (f' · {wiz["start_cob"]} → {wiz["end_cob"]}' if wiz["frequency"] == "RECURRING" else "") + "</td></tr>"
                f'<tr><td style="color:{P["grey_700"]};padding:3px 0">Target COB</td><td><strong>{wiz["target_date"].strftime("%d %b %Y")}</strong></td></tr>'
                + (f'<tr><td style="color:{P["grey_700"]};padding:3px 0">Scale Factor</td><td><strong>{wiz["scale_factor"]:.4f}×</strong></td></tr>' if wiz["adj_type"] in ("SCALE","ROLL","COPY") else "")
                + (f'<tr><td style="color:{P["grey_700"]};padding:3px 0">Source COB</td><td><strong>{wiz.get("roll_source_date","")}</strong></td></tr>' if wiz["adj_type"] in ("ROLL","COPY") else "")
                + f'<tr><td style="color:{P["grey_700"]};padding:3px 0">Records</td><td><strong style="color:{P["primary"]}">{wiz["matched_rows"]:,}</strong></td></tr>'
                f'</table>'
                f'</div>',
                unsafe_allow_html=True)

        with sum_col2:
            st.markdown(
                f'<div class="mcard mcard-accent" style="border-left-color:{scope["color"]}">'
                f'<div style="font-size:0.75rem;color:{P["grey_700"]};font-weight:600;text-transform:uppercase;letter-spacing:.06em;margin-bottom:0.7rem">Active Filters</div>',
                unsafe_allow_html=True)
            render_filter_chips(wiz["filter_criteria"], wiz["scope_key"])
            st.markdown('</div>', unsafe_allow_html=True)

            # Overlap reminder
            if wiz["overlaps"]:
                st.markdown(
                    f'<div style="background:{P["warning_lt"]};border:1px solid #FFCC80;'
                    f'border-radius:6px;padding:0.6rem;margin-top:0.5rem;font-size:0.82rem">'
                    f'⚠️ <strong>{len(wiz["overlaps"])} overlapping adjustment(s)</strong> detected. '
                    f'Proceed with caution.</div>',
                    unsafe_allow_html=True)

        # ── Justification ───────────────────────────────────────────────────
        section_title("Justification & Metadata", "📝")
        j1, j2 = st.columns([3, 1])
        with j1:
            wiz["business_reason"] = st.text_area(
                "Business Reason *",
                value=wiz.get("business_reason", ""),
                placeholder="Explain why this adjustment is needed, reference meetings, emails, or documentation…",
                height=100,
                help="Required: Describe the business rationale for this adjustment.")
        with j2:
            wiz["ticket_reference"] = st.text_input(
                "Ticket Reference",
                value=wiz.get("ticket_reference", ""),
                placeholder="JIRA-1234 / INC-456",
                help="Optional: Link to a JIRA ticket, ServiceNow incident, or email reference.")

        # ── Approval option ─────────────────────────────────────────────────
        section_title("Submission", "🚀")
        approval_col1, approval_col2 = st.columns([3, 1])
        with approval_col1:
            st.markdown(
                f'<div style="background:{P["info_lt"]};border:1px solid #90CAF9;border-radius:8px;'
                f'padding:0.75rem 1rem;font-size:0.85rem;margin-bottom:0.75rem">'
                f'💡 <strong>Approval Workflow:</strong> Adjustments can be saved as a draft for later, '
                f'or submitted directly for approval. Applied adjustments are processed asynchronously '
                f'by Snowflake Tasks — you can monitor progress in the <strong>Processing Queue</strong>.'
                f'</div>',
                unsafe_allow_html=True)
            wiz["submit_for_approval"] = st.checkbox(
                "Submit for approval immediately (recommended)",
                value=wiz.get("submit_for_approval", True),
                help="If checked, the adjustment will go to PENDING_APPROVAL status. "
                     "Otherwise it remains a DRAFT that you can submit later.")

        # ── Validation ──────────────────────────────────────────────────────
        can_submit = bool(wiz["business_reason"].strip()) and wiz["matched_rows"] > 0

        # ── Action buttons ──────────────────────────────────────────────────
        st.markdown("<br/>", unsafe_allow_html=True)
        nav1, nav2, nav3, nav4 = st.columns([1, 1, 2, 2])

        with nav1:
            if st.button("← Back", use_container_width=True):
                wiz["step"] = 3
                st.rerun()

        with nav3:
            if st.button("💾 Save as Draft", use_container_width=True,
                         disabled=not can_submit):
                adj_id = create_adjustment(
                    scope_key=wiz["scope_key"],
                    adj_type=wiz["adj_type"],
                    frequency=wiz["frequency"],
                    target_date=wiz["target_date"],
                    filter_criteria=wiz["filter_criteria"],
                    scale_factor=wiz["scale_factor"],
                    roll_source_date=wiz.get("roll_source_date"),
                    start_cob=wiz.get("start_cob") if wiz["frequency"]=="RECURRING" else None,
                    end_cob=wiz.get("end_cob") if wiz["frequency"]=="RECURRING" else None,
                    business_reason=wiz["business_reason"],
                    ticket_reference=wiz["ticket_reference"],
                    submit_for_approval=False,
                )
                st.success(f"✅ ADJ #{adj_id} saved as DRAFT. Find it in My Work.")
                reset_wizard()
                st.rerun()

        with nav4:
            submit_label = ("🚀 Submit for Approval" if wiz["submit_for_approval"]
                            else "💾 Save as Draft")
            if st.button(submit_label, type="primary", use_container_width=True,
                         disabled=not can_submit):
                adj_id = create_adjustment(
                    scope_key=wiz["scope_key"],
                    adj_type=wiz["adj_type"],
                    frequency=wiz["frequency"],
                    target_date=wiz["target_date"],
                    filter_criteria=wiz["filter_criteria"],
                    scale_factor=wiz["scale_factor"],
                    roll_source_date=wiz.get("roll_source_date"),
                    start_cob=wiz.get("start_cob") if wiz["frequency"]=="RECURRING" else None,
                    end_cob=wiz.get("end_cob") if wiz["frequency"]=="RECURRING" else None,
                    business_reason=wiz["business_reason"],
                    ticket_reference=wiz["ticket_reference"],
                    submit_for_approval=wiz["submit_for_approval"],
                )
                status = "PENDING_APPROVAL" if wiz["submit_for_approval"] else "DRAFT"
                icon   = "🚀" if wiz["submit_for_approval"] else "💾"
                st.success(f"{icon} ADJ #{adj_id} created → {status}. "
                           f"{'Waiting for approver.' if wiz['submit_for_approval'] else 'Find it in My Work.'}")
                reset_wizard()
                st.rerun()

        if not wiz["business_reason"].strip():
            st.warning("Please provide a business reason before submitting.")
