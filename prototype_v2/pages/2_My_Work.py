"""
My Work — Personal Workspace
==============================
All adjustments created by the current user, with actions per status.
"""
import streamlit as st
import pandas as pd
from datetime import date

st.set_page_config(
    page_title="My Work · MUFG",
    page_icon="📋",
    layout="wide",
    initial_sidebar_state="expanded",
)

from data.state_manager import (
    init_state, get_my_adjustments, get_all_adjustments, get_status_history,
    update_status, current_user, can_approve,
)
from data.mock_data import SCOPES, TYPE_LABELS
from data.styles import (
    inject_css, render_sidebar, render_filter_chips, render_status_timeline,
    status_badge, section_title, fmt_number, P, STATUS_COLORS,
)

init_state()
inject_css()
render_sidebar()

user = current_user()
st.markdown("## 📋 My Work")
st.markdown(f"<span style='color:{P['grey_700']};font-size:0.9rem'>All adjustments created by you, with full history and available actions.</span>",
            unsafe_allow_html=True)
st.markdown("<br/>", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# FILTERS
# ──────────────────────────────────────────────────────────────────────────────

f1, f2, f3, f4 = st.columns(4)
with f1:
    filter_status = st.multiselect("Status", ["DRAFT","PENDING_APPROVAL","APPROVED","APPLIED","REJECTED","REVERSED","CANCELLED"],
                                   default=[], placeholder="All statuses…")
with f2:
    filter_scope = st.multiselect("Source", list(SCOPES.keys()), default=[], placeholder="All sources…")
with f3:
    filter_type = st.multiselect("Type", ["FLATTEN","SCALE","ROLL","COPY"], default=[], placeholder="All types…")
with f4:
    show_all = st.checkbox("Show all users' adjustments", value=False,
                           help="When checked, shows all adjustments (not just yours).")

st.markdown("<br/>", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# DATA
# ──────────────────────────────────────────────────────────────────────────────

adjs = get_all_adjustments() if show_all else get_my_adjustments()
if filter_status:
    adjs = [a for a in adjs if a["adj_status"] in filter_status]
if filter_scope:
    adjs = [a for a in adjs if a["scope_key"] in filter_scope]
if filter_type:
    adjs = [a for a in adjs if a["adj_type"] in filter_type]

adjs = sorted(adjs, key=lambda a: a.get("created_at") or date.min, reverse=True)

# ──────────────────────────────────────────────────────────────────────────────
# TABS: by status group
# ──────────────────────────────────────────────────────────────────────────────

tab_labels = {
    "📝 Drafts":         ["DRAFT"],
    "⏳ Pending":        ["PENDING_APPROVAL"],
    "✅ Approved":       ["APPROVED"],
    "✔️ Applied":        ["APPLIED", "REVERSED"],
    "❌ Rejected":       ["REJECTED", "CANCELLED"],
    "🔁 Recurring":      None,  # special
}

counts = {}
for label, statuses in tab_labels.items():
    if statuses is None:
        counts[label] = sum(1 for a in adjs if a.get("frequency") == "RECURRING")
    else:
        counts[label] = sum(1 for a in adjs if a["adj_status"] in statuses)

tab_names = [f"{lbl} ({counts[lbl]})" for lbl in tab_labels]
tabs = st.tabs(tab_names)

def render_adj_card(adj: dict, expanded: bool = False):
    """Render a single adjustment card with expandable detail."""
    scope   = SCOPES.get(adj["scope_key"], {})
    s_color = STATUS_COLORS.get(adj["adj_status"], "#9E9E9E")
    created_ts = adj["created_at"].strftime("%d %b %Y %H:%M") if adj.get("created_at") else ""

    # Card header
    with st.expander(
        f'ADJ #{adj["adj_id"]} · {scope.get("icon","📊")} {scope.get("label","?")} · '
        f'{adj["adj_type"]} · {adj["adj_status"].replace("_"," ")} · {adj["affected_rows"]} rows',
        expanded=expanded,
    ):
        col_info, col_meta = st.columns([2, 1])

        with col_info:
            st.markdown(status_badge(adj["adj_status"]), unsafe_allow_html=True)
            if adj.get("frequency") == "RECURRING":
                st.markdown(f'<span class="tag recurring">RECURRING · {adj.get("start_cob")} → {adj.get("end_cob")}</span>',
                            unsafe_allow_html=True)
            st.markdown("<br/>", unsafe_allow_html=True)

            section_title("Filters Applied", "🔍")
            render_filter_chips(adj.get("filter_criteria", {}), adj["scope_key"])

            st.markdown(f'<br/><div style="font-size:0.85rem"><strong>Business Reason:</strong><br/>'
                        f'<span style="color:{P["grey_700"]}">{adj["business_reason"]}</span></div>',
                        unsafe_allow_html=True)
            if adj.get("ticket_reference"):
                st.markdown(f'<div style="margin-top:4px;font-size:0.82rem">'
                            f'<strong>Ticket:</strong> <code>{adj["ticket_reference"]}</code></div>',
                            unsafe_allow_html=True)

            if adj.get("ai_summary"):
                st.markdown("---")
                st.markdown(
                    f'<div style="background:{P["accent"]};color:#E8E8F8;border-radius:8px;'
                    f'padding:0.75rem 1rem;font-size:0.82rem;margin-top:0.5rem">'
                    f'<span style="font-size:0.7rem;font-weight:700;text-transform:uppercase;'
                    f'letter-spacing:.06em;opacity:0.7">🤖 AI Summary</span><br/><br/>'
                    f'{adj["ai_summary"]}'
                    f'</div>',
                    unsafe_allow_html=True)

        with col_meta:
            meta_rows = [
                ("Target COB",  adj["target_date"].strftime("%d %b %Y") if adj.get("target_date") else "—"),
                ("Records",     f'{adj["affected_rows"]:,}'),
                ("Created by",  adj["created_by"]),
                ("Created",     created_ts),
                ("Scale",       f'{adj["scale_factor"]:.4f}×' if adj.get("scale_factor") and adj["scale_factor"] != 1 else "—"),
                ("Approved by", adj.get("approved_by") or "—"),
                ("Applied by",  adj.get("applied_by") or "—"),
            ]
            rows_html = "".join(
                f'<tr><td style="color:{P["grey_700"]};padding:3px 0;font-size:0.8rem;'
                f'white-space:nowrap;padding-right:12px">{k}</td>'
                f'<td style="font-size:0.82rem;font-weight:600">{v}</td></tr>'
                for k, v in meta_rows if v != "—"
            )
            st.markdown(
                f'<div class="mcard" style="padding:0.8rem">'
                f'<table style="width:100%;border-collapse:collapse">{rows_html}</table>'
                f'</div>',
                unsafe_allow_html=True)

        # ── Status history ──────────────────────────────────────────────────
        st.markdown("---")
        section_title("Status History", "🕐")
        history = get_status_history(adj["adj_id"])
        render_status_timeline(history)

        # ── Actions ─────────────────────────────────────────────────────────
        st.markdown("---")
        section_title("Actions", "⚡")
        act_cols = st.columns(4)
        adj_id = adj["adj_id"]

        if adj["adj_status"] == "DRAFT":
            with act_cols[0]:
                if st.button("🚀 Submit for Approval", key=f"submit_{adj_id}",
                             use_container_width=True, type="primary"):
                    res = update_status(adj_id, "PENDING_APPROVAL", "Submitted for approval.")
                    if res["success"]:
                        st.success("Submitted for approval.")
                        st.rerun()
                    else:
                        st.error(res["error"])
            with act_cols[1]:
                if st.button("🚫 Cancel", key=f"cancel_{adj_id}", use_container_width=True):
                    res = update_status(adj_id, "CANCELLED", "Cancelled by creator.")
                    if res["success"]:
                        st.rerun()
                    else:
                        st.error(res["error"])

        elif adj["adj_status"] == "PENDING_APPROVAL":
            with act_cols[0]:
                if st.button("↩️ Recall to Draft", key=f"recall_{adj_id}",
                             use_container_width=True):
                    res = update_status(adj_id, "DRAFT", "Recalled by creator.")
                    if res["success"]:
                        st.rerun()
                    else:
                        st.error(res["error"])
            if can_approve() and adj["created_by"] != current_user()["id"]:
                with act_cols[1]:
                    comment = st.text_input("Approval comment", key=f"cmt_{adj_id}",
                                            placeholder="Optional comment…")
                with act_cols[2]:
                    if st.button("✅ Approve", key=f"approve_{adj_id}",
                                 use_container_width=True, type="primary"):
                        res = update_status(adj_id, "APPROVED", comment)
                        if res["success"]:
                            st.success("Approved.")
                            st.rerun()
                        else:
                            st.error(res["error"])
                with act_cols[3]:
                    if st.button("❌ Reject", key=f"reject_{adj_id}",
                                 use_container_width=True):
                        res = update_status(adj_id, "REJECTED", comment or "Rejected.")
                        if res["success"]:
                            st.rerun()
                        else:
                            st.error(res["error"])

        elif adj["adj_status"] == "APPROVED":
            with act_cols[0]:
                if st.button("▶️ Apply Now", key=f"apply_{adj_id}",
                             use_container_width=True, type="primary"):
                    res = update_status(adj_id, "APPLIED", "Applied by operator.")
                    if res["success"]:
                        st.success("Applied. Processing queued — check Processing Queue.")
                        st.rerun()
                    else:
                        st.error(res["error"])
            with act_cols[1]:
                if st.button("↩️ Recall", key=f"recall2_{adj_id}", use_container_width=True):
                    res = update_status(adj_id, "PENDING_APPROVAL", "Recalled for re-review.")
                    if res["success"]:
                        st.rerun()
                    else:
                        st.error(res["error"])

        elif adj["adj_status"] == "APPLIED":
            with act_cols[0]:
                reason = st.text_input("Reversal reason *", key=f"rev_reason_{adj_id}",
                                       placeholder="Why is this being reversed?")
            with act_cols[1]:
                if st.button("↩️ Reverse", key=f"reverse_{adj_id}",
                             use_container_width=True,
                             disabled=not reason):
                    res = update_status(adj_id, "REVERSED", reason)
                    if res["success"]:
                        st.success("Reversed. A negating adjustment has been created.")
                        st.rerun()
                    else:
                        st.error(res["error"])


# ── Render tabs ────────────────────────────────────────────────────────────────

for tab, (label, statuses) in zip(tabs, tab_labels.items()):
    with tab:
        if statuses is None:  # Recurring tab
            tab_adjs = [a for a in adjs if a.get("frequency") == "RECURRING"]
        else:
            tab_adjs = [a for a in adjs if a["adj_status"] in statuses]

        if not tab_adjs:
            st.markdown(
                f'<div class="mcard" style="text-align:center;padding:2.5rem;color:{P["grey_700"]}">'
                f'<div style="font-size:1.8rem">🕳️</div>'
                f'<div style="font-size:0.9rem;margin-top:0.5rem">No adjustments in this category</div>'
                f'</div>',
                unsafe_allow_html=True)
            continue

        # Quick summary row
        st.markdown(
            f'<div style="display:flex;gap:16px;margin-bottom:1rem;flex-wrap:wrap">',
            unsafe_allow_html=True)
        for scope_key in set(a["scope_key"] for a in tab_adjs):
            cnt = sum(1 for a in tab_adjs if a["scope_key"] == scope_key)
            scope = SCOPES.get(scope_key, {})
            st.markdown(
                f'<div style="background:{scope.get("bg_color",P["grey_100"])};border-radius:6px;'
                f'padding:4px 10px;font-size:0.78rem;font-weight:600;color:{scope.get("color",P["grey_700"])}">'
                f'{scope.get("icon","")} {scope.get("label","?")} · {cnt}</div>',
                unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

        for adj in tab_adjs:
            render_adj_card(adj, expanded=False)
