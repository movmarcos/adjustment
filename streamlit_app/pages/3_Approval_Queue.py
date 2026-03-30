"""
Approval Queue — Review & Approve Adjustments
===============================================
Adjustments submitted with requires_approval go to 'Pending Approval' status.
Approvers can approve (→ Approved → processed by task) or reject.
Reads from: VW_APPROVAL_QUEUE, ADJ_STATUS_HISTORY.
"""
import streamlit as st
import pandas as pd

st.set_page_config(page_title="Approval Queue · MUFG", page_icon="✅", layout="wide", initial_sidebar_state="expanded")

from utils.styles import (
    inject_css, render_sidebar, render_filter_chips,
    section_title, status_badge, P, SCOPE_CONFIG, STATUS_COLORS,
)
from utils.snowflake_conn import run_query, run_query_df, current_user_name, safe_rerun

inject_css()
render_sidebar()

user = current_user_name()

st.markdown("## ✅ Approval Queue")
st.markdown(
    f"<span style='color:{P['grey_700']};font-size:0.9rem'>"
    "Review adjustments that require approval before processing. "
    "Approve to move them forward, or reject with a reason."
    "</span>", unsafe_allow_html=True)
st.markdown("<br/>", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# SUMMARY STATS
# ──────────────────────────────────────────────────────────────────────────────

try:
    df_stats = run_query_df("""
        SELECT
            COUNT(*)                                                       AS TOTAL_PENDING,
            COUNT(DISTINCT PROCESS_TYPE)                                   AS SCOPES,
            COUNT(DISTINCT SUBMITTED_BY)                                   AS SUBMITTERS
        FROM ADJUSTMENT_APP.VW_APPROVAL_QUEUE
    """)
    qs = df_stats.iloc[0].to_dict() if not df_stats.empty else {}
except Exception:
    qs = {}

c1, c2, c3 = st.columns(3)
stat_items = [
    ("Awaiting Approval", qs.get("TOTAL_PENDING", 0), P["info"],    "📝"),
    ("Scopes",            qs.get("SCOPES", 0),         P["primary"], "📊"),
    ("Submitters",        qs.get("SUBMITTERS", 0),      P["grey_700"],"👤"),
]
for col, (label, val, color, icon) in zip([c1, c2, c3], stat_items):
    col.markdown(
        f'<div style="background:{P["white"]};border:1px solid {P["border"]};'
        f'border-top:3px solid {color};border-radius:8px;padding:0.8rem;text-align:center">'
        f'<div style="font-size:1.6rem;font-weight:800;color:{color}">{icon} {int(val)}</div>'
        f'<div style="font-size:0.72rem;text-transform:uppercase;letter-spacing:.06em;'
        f'color:{P["grey_700"]};margin-top:3px">{label}</div>'
        f'</div>',
        unsafe_allow_html=True)

st.markdown("<br/>", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# FILTERS
# ──────────────────────────────────────────────────────────────────────────────

f1, f2 = st.columns(2)
with f1:
    filter_scope = st.multiselect(
        "Filter by Scope", list(SCOPE_CONFIG.keys()),
        default=[], key="aq_scope")
with f2:
    filter_type = st.multiselect(
        "Filter by Type", ["Flatten", "Scale", "Roll"],
        default=[], key="aq_type")

# ──────────────────────────────────────────────────────────────────────────────
# LOAD QUEUE
# ──────────────────────────────────────────────────────────────────────────────

try:
    where_parts = []
    if filter_scope:
        in_list = ",".join(f"'{s}'" for s in filter_scope)
        where_parts.append(f"PROCESS_TYPE IN ({in_list})")
    if filter_type:
        in_list = ",".join(f"'{t}'" for t in filter_type)
        where_parts.append(f"ADJUSTMENT_TYPE IN ({in_list})")

    where_sql = (" AND " + " AND ".join(where_parts)) if where_parts else ""

    df_queue = run_query_df(f"""
        SELECT *
        FROM ADJUSTMENT_APP.VW_APPROVAL_QUEUE
        WHERE 1=1 {where_sql}
        ORDER BY SUBMITTED_AT ASC
        LIMIT 100
    """)
except Exception as e:
    df_queue = pd.DataFrame()
    st.warning(f"Could not load approval queue: {e}")

# ──────────────────────────────────────────────────────────────────────────────
# QUEUE ITEMS
# ──────────────────────────────────────────────────────────────────────────────

section_title(f"Adjustments Awaiting Approval ({len(df_queue)})", "📝")

if df_queue.empty:
    st.markdown(
        f'<div class="mcard" style="text-align:center;padding:2.5rem;color:{P["grey_700"]}">'
        f'<div style="font-size:1.8rem">✅</div>'
        f'<div style="font-size:0.9rem;margin-top:0.5rem">No adjustments awaiting approval</div>'
        f'</div>',
        unsafe_allow_html=True)
else:
    for _, row in df_queue.iterrows():
        adj_id      = row.get("ADJ_ID", "?")
        scope       = str(row.get("PROCESS_TYPE", ""))
        adj_type    = str(row.get("ADJUSTMENT_TYPE", ""))
        entity      = str(row.get("ENTITY_CODE", "")) or "—"
        book        = str(row.get("BOOK_CODE", "")) or "—"
        submitted_by = str(row.get("SUBMITTED_BY", ""))
        submitted_at = row.get("SUBMITTED_AT", "")
        reason      = str(row.get("REASON", "")) or "—"
        scope_cfg   = SCOPE_CONFIG.get(scope, {})

        if hasattr(submitted_at, "strftime"):
            submitted_at = submitted_at.strftime("%d %b %Y %H:%M")

        with st.expander(
            f'ADJ #{adj_id} · {scope_cfg.get("icon","📊")} {scope} · '
            f'{adj_type} · by {submitted_by}',
            expanded=False,
        ):
            col_info, col_actions = st.columns([3, 1])

            with col_info:
                st.markdown(status_badge("Pending Approval"), unsafe_allow_html=True)
                st.markdown("<br/>", unsafe_allow_html=True)

                # Key details
                meta_html = (
                    f'<table style="font-size:0.85rem;border-collapse:collapse;width:100%">'
                    f'<tr><td style="color:{P["grey_700"]};padding:3px 12px 3px 0;width:30%">COB</td>'
                    f'<td style="font-weight:600">{row.get("COBID", "—")}</td></tr>'
                    f'<tr><td style="color:{P["grey_700"]};padding:3px 12px 3px 0">Type</td>'
                    f'<td style="font-weight:600">{adj_type}</td></tr>'
                    f'<tr><td style="color:{P["grey_700"]};padding:3px 12px 3px 0">Entity</td>'
                    f'<td style="font-weight:600">{entity}</td></tr>'
                    f'<tr><td style="color:{P["grey_700"]};padding:3px 12px 3px 0">Book</td>'
                    f'<td style="font-weight:600">{book}</td></tr>'
                    f'<tr><td style="color:{P["grey_700"]};padding:3px 12px 3px 0">Submitted</td>'
                    f'<td>{submitted_at}</td></tr>'
                    f'<tr><td style="color:{P["grey_700"]};padding:3px 12px 3px 0">By</td>'
                    f'<td>{submitted_by}</td></tr>'
                    f'</table>'
                )
                st.markdown(meta_html, unsafe_allow_html=True)

                section_title("Filters Applied", "🔍")
                render_filter_chips(row.to_dict())

                st.markdown(
                    f'<br/><div style="font-size:0.85rem"><strong>Business Reason:</strong><br/>'
                    f'<span style="color:{P["grey_700"]}">{reason}</span></div>',
                    unsafe_allow_html=True)

                if row.get("SCALE_FACTOR") and float(row.get("SCALE_FACTOR", 1)) != 1:
                    st.markdown(
                        f'<div style="font-size:0.85rem;margin-top:0.3rem">'
                        f'<strong>Scale Factor:</strong> {row["SCALE_FACTOR"]:.4f}×</div>',
                        unsafe_allow_html=True)

            with col_actions:
                st.markdown("<br/>", unsafe_allow_html=True)
                st.markdown(
                    f'<div style="text-align:center;margin-bottom:1rem;font-size:0.85rem;'
                    f'color:{P["grey_700"]};font-weight:600">Actions</div>',
                    unsafe_allow_html=True)

                # Approve
                if st.button("✅ Approve", key=f"approve_{adj_id}",
                             use_container_width=True, type="primary"):
                    try:
                        run_query(f"""
                            UPDATE ADJUSTMENT_APP.ADJ_HEADER
                            SET RUN_STATUS = 'Approved'
                            WHERE ADJ_ID = {adj_id}
                              AND RUN_STATUS = 'Pending Approval'
                        """)
                        run_query(f"""
                            INSERT INTO ADJUSTMENT_APP.ADJ_STATUS_HISTORY
                                (ADJ_ID, OLD_STATUS, NEW_STATUS, CHANGED_BY, COMMENT)
                            VALUES ({adj_id}, 'Pending Approval', 'Approved',
                                    '{user}', 'Approved by {user}')
                        """)
                        st.success(f"ADJ #{adj_id} approved!")
                        safe_rerun()
                    except Exception as ex:
                        st.error(str(ex))

                st.markdown("<div style='height:0.5rem'></div>", unsafe_allow_html=True)

                # Reject
                reject_reason = st.text_input(
                    "Rejection reason", key=f"reject_reason_{adj_id}",
                    label_visibility="collapsed")
                if st.button("❌ Reject", key=f"reject_{adj_id}",
                             use_container_width=True):
                    try:
                        comment = reject_reason or "Rejected"
                        run_query(f"""
                            UPDATE ADJUSTMENT_APP.ADJ_HEADER
                            SET RUN_STATUS = 'Rejected'
                            WHERE ADJ_ID = {adj_id}
                              AND RUN_STATUS = 'Pending Approval'
                        """)
                        run_query(f"""
                            INSERT INTO ADJUSTMENT_APP.ADJ_STATUS_HISTORY
                                (ADJ_ID, OLD_STATUS, NEW_STATUS, CHANGED_BY, COMMENT)
                            VALUES ({adj_id}, 'Pending Approval', 'Rejected',
                                    '{user}', '{comment}')
                        """)
                        st.success(f"ADJ #{adj_id} rejected.")
                        safe_rerun()
                    except Exception as ex:
                        st.error(str(ex))

# ──────────────────────────────────────────────────────────────────────────────
# RECENTLY APPROVED / REJECTED
# ──────────────────────────────────────────────────────────────────────────────

st.markdown("<br/>", unsafe_allow_html=True)
section_title("Recently Approved / Rejected", "📜")

try:
    df_recent = run_query_df("""
        SELECT h.ADJ_ID, h.COBID, h.PROCESS_TYPE, h.ADJUSTMENT_TYPE,
               h.ENTITY_CODE, h.RUN_STATUS, h.USERNAME AS SUBMITTED_BY,
               sh.CHANGED_BY AS ACTIONED_BY, sh.CHANGED_AT, sh.COMMENT
        FROM ADJUSTMENT_APP.ADJ_STATUS_HISTORY sh
        INNER JOIN ADJUSTMENT_APP.ADJ_HEADER h ON h.ADJ_ID = sh.ADJ_ID
        WHERE sh.NEW_STATUS IN ('Approved', 'Rejected')
          AND sh.OLD_STATUS = 'Pending Approval'
        ORDER BY sh.CHANGED_AT DESC
        LIMIT 20
    """)

    if not df_recent.empty:
        for _, row in df_recent.iterrows():
            status = row["RUN_STATUS"]
            color = STATUS_COLORS.get(row.get("NEW_STATUS", status), "#9E9E9E")
            at = row.get("CHANGED_AT", "")
            if hasattr(at, "strftime"):
                at = at.strftime("%d %b %H:%M")
            st.markdown(
                f'<div class="queue-item" style="border-left:3px solid {color}">'
                f'<div style="display:flex;justify-content:space-between">'
                f'<span style="font-weight:700;font-size:0.85rem">ADJ #{row["ADJ_ID"]} · '
                f'{row.get("PROCESS_TYPE","")} · {row.get("ADJUSTMENT_TYPE","")}</span>'
                f'<span style="font-size:0.75rem;color:{color};font-weight:600">'
                f'{row.get("RUN_STATUS","")}</span></div>'
                f'<div style="font-size:0.75rem;color:{P["grey_700"]};margin-top:3px">'
                f'By {row.get("ACTIONED_BY","?")} · {at}'
                + (f' · "{row.get("COMMENT","")}"' if row.get("COMMENT") else "")
                + f'</div></div>',
                unsafe_allow_html=True)
    else:
        st.info("No recent approval/rejection activity.")
except Exception:
    st.info("No approval history available yet.")
