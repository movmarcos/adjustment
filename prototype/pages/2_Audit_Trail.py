"""
📋 Audit Trail — review and manage adjustments
"""
import streamlit as st
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from data.state_manager import (init_state, get_headers, get_lines, get_history,
                                 update_status, reverse_adjustment, current_scope_cfg,
                                 VALID_TRANSITIONS, STATUS_COLORS)
from data.styles import inject_css, section_header, status_badge, top_navbar, scope_and_user_controls, format_number

st.set_page_config(page_title="Audit Trail", page_icon="📋", layout="wide", initial_sidebar_state="collapsed")
inject_css()
init_state()
top_navbar(active_page="Audit Trail")
scope_id = scope_and_user_controls()
cfg = current_scope_cfg()

st.markdown(f"""
<div style="display:flex;align-items:center;gap:10px;margin-bottom:4px">
    <span style="font-size:2rem">📋</span>
    <h1 style="margin:0;font-size:1.6rem;color:#2D2D2D">Audit Trail</h1>
</div>
<span style="color:#6B6B6B;font-size:.88rem">Scope: <strong>{cfg['icon']} {cfg['name']}</strong></span>
""", unsafe_allow_html=True)
st.markdown("---")

headers = get_headers()
lines   = get_lines()
history = get_history()

if headers.empty:
    st.info("No adjustments found for this scope.")
    st.stop()

# ── Filters ─────────────────────────────────────────────────────────
f1, f2, f3, f4 = st.columns(4)
with f1:
    all_cobs = sorted(headers["BUSINESS_DATE"].dropna().unique().tolist())
    cob_filter = st.multiselect("COB", all_cobs, key="audit_cob")
with f2:
    status_filter = st.multiselect("Status", list(STATUS_COLORS.keys()), key="audit_status")
with f3:
    type_filter = st.multiselect("Type", ["FLATTEN", "SCALE", "ROLL"], key="audit_type")
with f4:
    user_filter = st.multiselect("Created By", headers["CREATED_BY"].unique().tolist(), key="audit_user")

filtered = headers.copy()
if cob_filter:
    filtered = filtered[filtered["BUSINESS_DATE"].isin(cob_filter)]
if status_filter:
    filtered = filtered[filtered["STATUS"].isin(status_filter)]
if type_filter:
    filtered = filtered[filtered["ADJ_TYPE"].isin(type_filter)]
if user_filter:
    filtered = filtered[filtered["CREATED_BY"].isin(user_filter)]

st.caption(f"Showing {len(filtered)} of {len(headers)} adjustments")

# ── Adjustment cards ────────────────────────────────────────────────
for _, row in filtered.sort_values("CREATED_AT", ascending=False).iterrows():
    adj_id = row["ADJ_ID"]
    badge = status_badge(row["STATUS"])
    type_icons = {"FLATTEN": "📉", "SCALE": "📐", "ROLL": "🔄"}
    icon = type_icons.get(row["ADJ_TYPE"], "📝")

    with st.expander(f"{icon}  **{adj_id}** — {row['ADJ_TYPE']}  |  {row['JUSTIFICATION'][:50]}"):
        # Header info
        ic1, ic2, ic3, ic4 = st.columns(4)
        ic1.markdown(f"**Status** {badge}", unsafe_allow_html=True)
        ic2.markdown(f"**Date** {row['BUSINESS_DATE']}")
        ic3.markdown(f"**Created by** {row['CREATED_BY']}")
        ic4.markdown(f"**Type** {row['ADJ_TYPE']}")

        freq = row.get("FREQUENCY", "ADHOC")
        if freq == "RECURRING":
            s_cob = row.get("START_COB", "")
            e_cob = row.get("END_COB", "") or "open-ended"
            st.markdown(f"**Frequency:** 🔁 RECURRING &nbsp;({s_cob} → {e_cob})")
        else:
            st.markdown("**Frequency:** 🔹 ADHOC")

        st.markdown(f"**Justification:** {row['JUSTIFICATION']}")

        # Lines
        adj_lines = lines[lines["ADJ_ID"] == adj_id]
        if not adj_lines.empty:
            section_header("Line Items")
            display_cols = [c for c in adj_lines.columns if c != "ADJ_ID"]
            st.dataframe(adj_lines[display_cols], use_container_width=True, hide_index=True)

        # History timeline
        adj_hist = history[history["ADJ_ID"] == adj_id].sort_values("CHANGED_AT")
        if not adj_hist.empty:
            section_header("Status History")
            for _, h in adj_hist.iterrows():
                from_b = status_badge(h["FROM_STATUS"]) if h["FROM_STATUS"] else ""
                to_b = status_badge(h["TO_STATUS"])
                ts = h["CHANGED_AT"][:16].replace("T", " ")
                st.markdown(f"""
                <div class="tl-item">
                    <div class="tl-time">{ts} · {h['CHANGED_BY']}</div>
                    <div class="tl-text">{from_b} → {to_b} &nbsp; {h.get('COMMENT','')}</div>
                </div>
                """, unsafe_allow_html=True)

        # Action buttons
        current_status = row["STATUS"]
        valid_next = VALID_TRANSITIONS.get(current_status, [])
        if valid_next:
            st.markdown("<br/>", unsafe_allow_html=True)
            btn_cols = st.columns(len(valid_next) + 2)
            for i, ns in enumerate(valid_next):
                color_map = {
                    "PENDING_APPROVAL": "primary",
                    "APPROVED": "primary",
                    "APPLIED": "primary",
                    "REJECTED": "secondary",
                    "REVERSED": "secondary",
                }
                with btn_cols[i]:
                    if st.button(f"→ {ns.replace('_',' ')}", key=f"btn_{adj_id}_{ns}",
                                 type=color_map.get(ns, "secondary"), use_container_width=True):
                        comment = st.session_state.get(f"comment_{adj_id}", "")
                        ok = update_status(adj_id, ns, comment)
                        if ok:
                            st.success(f"Moved to {ns}")
                            st.rerun()
                        else:
                            st.error("Transition failed")
