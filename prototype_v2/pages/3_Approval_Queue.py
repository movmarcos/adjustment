"""
Approval Queue — Review & Decide
==================================
Approvers see all pending adjustments with full context to approve/reject.
Non-approvers see a read-only view of the queue state.
"""
import streamlit as st
import plotly.graph_objects as go
from datetime import datetime

st.set_page_config(
    page_title="Approval Queue · MUFG",
    page_icon="✅",
    layout="wide",
    initial_sidebar_state="expanded",
)

from data.state_manager import (
    init_state, get_all_adjustments, get_pending_approvals,
    get_status_history, update_status,
    current_user, can_approve,
)
from data.mock_data import SCOPES
from data.styles import (
    inject_css, render_sidebar, render_filter_chips, render_status_timeline,
    status_badge, section_title, P, STATUS_COLORS,
)

init_state()
inject_css()
render_sidebar()

user = current_user()
is_approver = can_approve()

st.markdown("## ✅ Approval Queue")
if is_approver:
    st.markdown(f"<span style='color:{P['grey_700']};font-size:0.9rem'>Review pending adjustments and approve or reject. You cannot approve your own adjustments.</span>", unsafe_allow_html=True)
else:
    st.markdown(f"<span style='color:{P['warning']};font-size:0.9rem'>You have read-only access to the approval queue. Contact an Approver or Admin to review pending items.</span>", unsafe_allow_html=True)

st.markdown("<br/>", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# SUMMARY METRICS
# ──────────────────────────────────────────────────────────────────────────────

all_adjs   = get_all_adjustments()
pending    = [a for a in all_adjs if a["adj_status"] == "PENDING_APPROVAL"]
approved   = [a for a in all_adjs if a["adj_status"] == "APPROVED"]
rejected_today = [
    a for a in all_adjs
    if a["adj_status"] == "REJECTED"
    and a.get("created_at") and a["created_at"].date() == datetime.today().date()
]

m1, m2, m3, m4 = st.columns(4)
m1.metric("Pending Approval", len(pending),
          help="Awaiting a decision")
m2.metric("Approved (not yet applied)", len(approved),
          help="Approved but not yet applied to the data")
m3.metric("Rejected today", len(rejected_today))
m4.metric("You can approve", sum(1 for a in pending if a["created_by"] != user["id"]) if is_approver else 0,
          help="Pending adjustments that you did not create")

st.markdown("<br/>", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# FILTER BAR
# ──────────────────────────────────────────────────────────────────────────────

f1, f2, f3 = st.columns(3)
with f1:
    filter_scope = st.multiselect("Source", list(SCOPES.keys()), default=[],
                                  placeholder="All sources…", key="aq_scope")
with f2:
    filter_type = st.multiselect("Type", ["FLATTEN","SCALE","ROLL","COPY"], default=[],
                                 placeholder="All types…", key="aq_type")
with f3:
    filter_creator = st.text_input("Created by (search)", placeholder="e.g. james.wong",
                                   key="aq_creator")

st.markdown("---")

# ──────────────────────────────────────────────────────────────────────────────
# TABS: Pending | Approved | History
# ──────────────────────────────────────────────────────────────────────────────

tab_pending, tab_approved, tab_history = st.tabs([
    f"⏳ Pending Approval ({len(pending)})",
    f"✅ Approved — Awaiting Apply ({len(approved)})",
    "📜 Recent Decisions",
])

def apply_filters(lst):
    if filter_scope:
        lst = [a for a in lst if a["scope_key"] in filter_scope]
    if filter_type:
        lst = [a for a in lst if a["adj_type"] in filter_type]
    if filter_creator.strip():
        lst = [a for a in lst if filter_creator.strip().lower() in a["created_by"].lower()]
    return lst


def render_approval_card(adj: dict):
    scope = SCOPES.get(adj["scope_key"], {})
    s_color = STATUS_COLORS.get(adj["adj_status"], "#9E9E9E")
    age_hrs = (datetime.now() - adj["created_at"]).total_seconds() / 3600 if adj.get("created_at") else 0
    age_str = f"{int(age_hrs)}h ago" if age_hrs < 24 else f"{int(age_hrs/24)}d ago"

    is_own = adj["created_by"] == user["id"]

    with st.expander(
        f'{"🔒 " if is_own else ""}ADJ #{adj["adj_id"]} · '
        f'{scope.get("icon","📊")} {scope.get("label","?")} · {adj["adj_type"]} · '
        f'{adj["affected_rows"]} rows · by {adj["created_by"]} · {age_str}',
        expanded=False,
    ):
        left, right = st.columns([3, 2])

        with left:
            section_title("Scope & Configuration")
            st.markdown(
                f'<table style="font-size:0.85rem;border-collapse:collapse;width:100%">'
                f'<tr><td style="color:{P["grey_700"]};padding:3px 8px 3px 0;width:35%">Source</td>'
                f'<td><strong>{scope.get("icon","")} {scope.get("full_label","?")}</strong> ({scope.get("source_system","")})</td></tr>'
                f'<tr><td style="color:{P["grey_700"]};padding:3px 8px 3px 0">Type</td>'
                f'<td><strong>{adj["adj_type"]}</strong>'
                + (f' × {adj["scale_factor"]:.4f}' if adj.get("scale_factor") and adj["adj_type"]!="FLATTEN" else "")
                + '</td></tr>'
                f'<tr><td style="color:{P["grey_700"]};padding:3px 8px 3px 0">Target COB</td>'
                f'<td><strong>{adj["target_date"].strftime("%d %b %Y") if adj.get("target_date") else "?"}</strong></td></tr>'
                f'<tr><td style="color:{P["grey_700"]};padding:3px 8px 3px 0">Records</td>'
                f'<td><strong style="color:{P["primary"]}">{adj["affected_rows"]:,}</strong></td></tr>'
                f'<tr><td style="color:{P["grey_700"]};padding:3px 8px 3px 0">Submitted</td>'
                f'<td>{adj["created_at"].strftime("%d %b %Y %H:%M") if adj.get("created_at") else "?"}</td></tr>'
                + (f'<tr><td style="color:{P["grey_700"]};padding:3px 8px 3px 0">Ticket</td>'
                   f'<td><code>{adj["ticket_reference"]}</code></td></tr>' if adj.get("ticket_reference") else "")
                + f'</table>',
                unsafe_allow_html=True)

            st.markdown(f'<br/><strong style="font-size:0.83rem">Business Reason:</strong><br/>'
                        f'<div style="font-size:0.85rem;color:{P["grey_700"]};background:{P["grey_100"]};'
                        f'border-radius:6px;padding:0.6rem 0.8rem;margin-top:4px">'
                        f'{adj["business_reason"]}</div>', unsafe_allow_html=True)

            section_title("Active Filters")
            render_filter_chips(adj.get("filter_criteria", {}), adj["scope_key"])

        with right:
            section_title("Status History")
            history = get_status_history(adj["adj_id"])
            render_status_timeline(history)

        # ── Decision section ────────────────────────────────────────────────
        st.markdown("---")
        if is_own and is_approver:
            st.warning("🔒 You created this adjustment — self-approval is not permitted. Another approver must review it.")
        elif not is_approver:
            st.info("You have read-only access. An Approver or Admin must take action on this item.")
        else:
            # Approver actions
            section_title("Decision", "⚖️")
            act1, act2, act3 = st.columns([3, 1, 1])
            with act1:
                comment = st.text_input(
                    "Comment (optional for approval, recommended for rejection)",
                    key=f"aq_comment_{adj['adj_id']}",
                    placeholder="Add context for your decision…")
            with act2:
                if st.button("✅ Approve", key=f"aq_approve_{adj['adj_id']}",
                             use_container_width=True, type="primary"):
                    res = update_status(adj["adj_id"], "APPROVED",
                                       comment or "Approved.")
                    if res["success"]:
                        st.success(f"ADJ #{adj['adj_id']} approved.")
                        st.rerun()
                    else:
                        st.error(res["error"])
            with act3:
                if st.button("❌ Reject", key=f"aq_reject_{adj['adj_id']}",
                             use_container_width=True):
                    if not comment.strip():
                        st.warning("Please provide a rejection reason.")
                    else:
                        res = update_status(adj["adj_id"], "REJECTED", comment)
                        if res["success"]:
                            st.rerun()
                        else:
                            st.error(res["error"])


# ── Tab: Pending ───────────────────────────────────────────────────────────────
with tab_pending:
    filtered = apply_filters(pending)
    if not filtered:
        st.markdown(
            f'<div class="mcard" style="text-align:center;padding:3rem;color:{P["grey_700"]}">'
            f'<div style="font-size:2rem">✅</div>'
            f'<div style="margin-top:0.5rem;font-size:0.9rem">Queue is clear — nothing pending approval</div>'
            f'</div>', unsafe_allow_html=True)
    else:
        # Priority ordering: oldest first
        filtered_sorted = sorted(filtered, key=lambda a: a.get("created_at") or datetime.min)

        # Bulk overview
        own_count = sum(1 for a in filtered_sorted if a["created_by"] == user["id"])
        actionable = len(filtered_sorted) - own_count if is_approver else 0
        action_note = f'<strong style="color:{P["primary"]}">{actionable}</strong> you can action.' if is_approver else "Read-only view."
        st.markdown(
            f'<div style="background:{P["info_lt"]};border:1px solid #90CAF9;border-radius:8px;'
            f'padding:0.7rem 1rem;margin-bottom:1rem;font-size:0.85rem">'
            f'<strong>{len(filtered_sorted)} items</strong> pending. {action_note}'
            f'</div>',
            unsafe_allow_html=True)

        for adj in filtered_sorted:
            render_approval_card(adj)


# ── Tab: Approved ──────────────────────────────────────────────────────────────
with tab_approved:
    filtered_ap = apply_filters(approved)
    if not filtered_ap:
        st.markdown(
            f'<div class="mcard" style="text-align:center;padding:3rem;color:{P["grey_700"]}">'
            f'No approved adjustments waiting to be applied.</div>',
            unsafe_allow_html=True)
    else:
        st.markdown(
            f'<div style="background:{P["success_lt"]};border:1px solid #A5D6A7;border-radius:8px;'
            f'padding:0.7rem 1rem;margin-bottom:1rem;font-size:0.85rem">'
            f'{len(filtered_ap)} adjustment(s) approved and ready to apply. '
            f'Go to <strong>My Work</strong> to apply them individually.</div>',
            unsafe_allow_html=True)

        for adj in filtered_ap:
            scope = SCOPES.get(adj["scope_key"], {})
            approved_ts = adj.get("approved_at", "")
            if approved_ts:
                approved_ts = approved_ts.strftime("%d %b %Y %H:%M")
            st.markdown(
                f'<div class="adj-card">'
                f'<div class="adj-card-header">'
                f'<span class="adj-id">ADJ #{adj["adj_id"]}</span>'
                f'{status_badge(adj["adj_status"])}'
                f'</div>'
                f'<div style="font-size:0.88rem;font-weight:600">'
                f'{scope.get("icon","📊")} {scope.get("label","?")} · {adj["adj_type"]} · '
                f'{adj["affected_rows"]:,} rows · COB {adj["target_date"].strftime("%d %b %Y") if adj.get("target_date") else "?"}'
                f'</div>'
                f'<div class="adj-meta">Created by {adj["created_by"]} · '
                f'Approved by {adj.get("approved_by","?")} at {approved_ts}</div>'
                f'<div style="font-size:0.8rem;color:{P["grey_700"]};margin-top:4px;font-style:italic">'
                f'"{adj["business_reason"][:80]}{"…" if len(adj["business_reason"])>80 else ""}"</div>'
                f'</div>',
                unsafe_allow_html=True)


# ── Tab: History ───────────────────────────────────────────────────────────────
with tab_history:
    # Show recent approve/reject decisions
    all_history = st.session_state.get("status_history", [])
    decisions = [
        h for h in all_history
        if h["new_status"] in ("APPROVED", "REJECTED")
    ]
    decisions_sorted = sorted(decisions, key=lambda h: h["changed_at"], reverse=True)[:20]

    if not decisions_sorted:
        st.info("No recent decisions found.")
    else:
        # Table view
        rows = []
        for h in decisions_sorted:
            adj = st.session_state["adjustments"].get(h["adj_id"], {})
            scope = SCOPES.get(adj.get("scope_key",""), {})
            rows.append({
                "ADJ #": h["adj_id"],
                "Source": f'{scope.get("icon","")} {scope.get("label","?")}',
                "Type": adj.get("adj_type",""),
                "Decision": h["new_status"],
                "By": h["changed_by"],
                "When": h["changed_at"].strftime("%d %b %Y %H:%M") if hasattr(h["changed_at"],"strftime") else "",
                "Comment": h.get("comment","")[:60],
            })
        df = pd.DataFrame(rows)

        # Colour map for decisions
        def highlight_decision(val):
            if val == "APPROVED":
                return f"color: {P['success']}; font-weight: 600"
            elif val == "REJECTED":
                return f"color: {P['danger']}; font-weight: 600"
            return ""

        st.dataframe(
            df.style.map(highlight_decision, subset=["Decision"]),
            use_container_width=True, hide_index=True, height=400,
        )

import pandas as pd  # re-import needed at module level for tab_history
