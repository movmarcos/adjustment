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
    inject_css, render_sidebar, render_filter_chips, fmt_user_dt,
    section_title, status_badge, P, SCOPE_CONFIG, ALL_SCOPES, STATUS_COLORS, icon, bordered_container,
    render_grid, fmt_adj_id,
)
from utils.snowflake_conn import (run_query, run_query_df, current_user_name,
                                  safe_rerun, friendly_error)
import html as _htmlmod

def _esc(val):
    """Escape single quotes for safe SQL interpolation."""
    return str(val).replace("\\", "\\\\").replace("'", "''") if val is not None else ""


def _pill(text, color):
    return (f'<span style="background:{color}18;color:{color};'
            f'border:1px solid {color}55;border-radius:99px;padding:1px 10px;'
            f'font-size:0.74rem;font-weight:700;white-space:nowrap">{text}</span>')


def _scope_pill(scope):
    cfg = SCOPE_CONFIG.get(str(scope), {})
    return _pill(_htmlmod.escape(str(scope) or "—"), cfg.get("color", P["grey_700"]))


inject_css()
render_sidebar()

user = current_user_name()

# Decisions require an attributable identity — app-resolved viewer name,
# same source submit uses (the procs take it as p_caller, with CURRENT_USER()
# as fallback). Only a fully unresolved identity blocks decisions.
_identity_ok = bool(user) and str(user).strip().lower() != "unknown"
if not _identity_ok:
    st.error(
        "Your identity could not be resolved. Approvals are disabled — every "
        "decision must be attributable to a verified user. Contact an admin.")

st.markdown("## Approval Queue")
st.markdown(
    f"<span style='color:{P['grey_700']};font-size:0.9rem'>"
    "Review adjustments that require approval before processing. "
    "Approve to move them forward, or reject with a reason."
    "</span>", unsafe_allow_html=True)
st.markdown("<br/>", unsafe_allow_html=True)

# Outcome of the last approve/reject — stashed before the rerun (a message
# rendered just before st.rerun() is destroyed by it).
_flash = st.session_state.pop("apq_flash", None)
if _flash:
    (st.success if _flash[0] == "success" else st.warning)(_flash[1])

# ──────────────────────────────────────────────────────────────────────────────
# APPROVER AUTHORIZATION CHECK
# ──────────────────────────────────────────────────────────────────────────────

is_approver = False
approver_scopes = set()  # scopes the user can approve; empty set with is_approver=True means all scopes
try:
    df_approver = run_query_df(f"""
        SELECT PROCESS_TYPE
        FROM ADJUSTMENT_APP.ADJ_APPROVERS
        WHERE UPPER(USERNAME) = UPPER('{_esc(user)}')
          AND IS_ACTIVE = TRUE
    """)
    if not df_approver.empty:
        is_approver = True
        for _, r in df_approver.iterrows():
            pt = r.get("PROCESS_TYPE")
            if pt is None or str(pt).strip() == "" or str(pt) == "None":
                approver_scopes = set()  # NULL = all scopes
                break
            approver_scopes.add(str(pt).upper())
except Exception:
    pass

if not is_approver:
    st.warning("You are not registered as an approver. Contact an admin to be added to the approvers list.")

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

# Pending COB sign-off / re-open requests are decided on THIS page too, so
# the header boxes must count them alongside the adjustments.
try:
    _so_stats = run_query("""
        SELECT COUNT(*) AS N
        FROM ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS
        WHERE UPPER(SIGN_OFF_STATUS) IN ('SIGNOFF_REQUESTED', 'REOPEN_REQUESTED')
    """)
    n_so_pending = int(_so_stats[0]["N"]) if _so_stats else 0
except Exception:
    n_so_pending = 0

c1, c2, c3, c4 = st.columns(4)
stat_items = [
    ("Adjustments awaiting", qs.get("TOTAL_PENDING", 0), P["info"],   "clipboard"),
    ("Sign-off requests",    n_so_pending, "#B45309" if n_so_pending
                                           else P["grey_700"], "unlock"),
    ("Scopes",               qs.get("SCOPES", 0),        P["primary"], "bar-chart"),
    ("Submitters",           qs.get("SUBMITTERS", 0),    P["grey_700"], "user"),
]
for col, (label, val, color, icon_name) in zip([c1, c2, c3, c4], stat_items):
    col.markdown(
        f'<div style="background:{P["white"]};border:1px solid {P["border"]};'
        f'border-top:3px solid {color};border-radius:8px;padding:0.8rem;text-align:center">'
        f'<div style="font-size:1.6rem;font-weight:800;color:{color};font-variant-numeric:tabular-nums">{icon(icon_name, size=15, color=color)} {int(val)}</div>'
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
        "Filter by Scope", ALL_SCOPES,
        default=[], key="aq_scope")
with f2:
    _aq_type_labels = {"Flatten": "Flatten", "Scale": "Scale", "Roll": "Roll",
                       "Direct": "Direct Adjustment", "Upload": "VaR Upload",
                       "EROL": "Entity Roll"}
    filter_type = st.multiselect(
        "Filter by Type", list(_aq_type_labels.keys()),
        default=[], key="aq_type",
        format_func=lambda v: _aq_type_labels.get(v, v))

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

# ── Pre-load overlaps for all queued adj_ids (single query, not N queries) ───
df_overlaps = pd.DataFrame()
if not df_queue.empty:
    try:
        queued_ids = ",".join(f"'{str(i).replace(chr(92), chr(92)*2).replace(chr(39), chr(39)*2)}'" for i in df_queue["ADJ_ID"].dropna())
        df_overlaps = run_query_df(f"""
            SELECT ADJ_ID_A, ADJ_ID_B, COBID,
                   ENTITY_A, ENTITY_B, BOOK_A, BOOK_B, ALERT_MESSAGE
            FROM ADJUSTMENT_APP.DT_OVERLAP_ALERTS
            WHERE ADJ_ID_A IN ({queued_ids})
               OR ADJ_ID_B IN ({queued_ids})
        """)
    except Exception:
        df_overlaps = pd.DataFrame()

# ──────────────────────────────────────────────────────────────────────────────
# BULK DECISIONS — every item still goes through SP_DECIDE_ADJUSTMENT, so the
# 4-eyes rules (approver registration, scope, self-approval) are enforced
# per adjustment server-side; the loop just saves the clicking.
# ──────────────────────────────────────────────────────────────────────────────

def _bulk_eligible(df):
    """Rows the current user is allowed to decide (UX filter — the proc
    re-checks everything)."""
    out = []
    if df.empty or not is_approver or not _identity_ok:
        return out
    for _, r in df.iterrows():
        scope_ok = (not approver_scopes
                    or str(r.get("PROCESS_TYPE", "")).upper() in approver_scopes)
        own = (user and str(r.get("SUBMITTED_BY", "")).strip().upper()
               == user.strip().upper())
        if scope_ok and not own:
            out.append(r)
    return out


_bulk_rows = _bulk_eligible(df_queue)
if len(_bulk_rows) >= 2:
    with bordered_container():
        section_title("Bulk Decision", "layers")
        st.caption("Select several adjustments and decide them in one go — "
                   "each one is still individually enforced (scope, 4-eyes) "
                   "and audited.")

        def _bulk_label(i):
            r = _bulk_rows[i]
            return (f'#{str(r.get("ADJ_ID"))[:8]}… · {r.get("PROCESS_TYPE")} · '
                    f'{r.get("ADJUSTMENT_TYPE")} · COB {r.get("COBID")} · '
                    f'by {r.get("SUBMITTED_BY")}')

        _sel = st.multiselect(
            "Adjustments to decide", options=list(range(len(_bulk_rows))),
            format_func=_bulk_label, key="apq_bulk_sel")
        b1, b2, b3 = st.columns([1, 1, 2])
        with b3:
            _bulk_comment = st.text_input(
                "Comment / rejection reason (applied to all selected)",
                key="apq_bulk_comment", label_visibility="collapsed",
                placeholder="Comment / rejection reason (applied to all)")

        def _bulk_decide(decision):
            import json as _json
            done, skipped = 0, []
            for i in _sel:
                r = _bulk_rows[i]
                try:
                    res = run_query(
                        f"CALL ADJUSTMENT_APP.SP_DECIDE_ADJUSTMENT("
                        f"'{_esc(r.get('ADJ_ID'))}', '{decision}', "
                        f"'{_esc(_bulk_comment or f'{decision} (bulk) by {user}')}', "
                        f"'{_esc(user)}')")
                    out = _json.loads(str(res[0][0])) if res else {}
                    if out.get("status") == "ok":
                        done += 1
                    else:
                        skipped.append(f"#{str(r.get('ADJ_ID'))[:8]}… "
                                       f"({out.get('message', '?')})")
                except Exception as ex:
                    skipped.append(f"#{str(r.get('ADJ_ID'))[:8]}… ({ex})")
            msg = f"{done} adjustment(s) {decision.lower()}."
            if skipped:
                msg += " Not applied: " + "; ".join(skipped[:5])
                if len(skipped) > 5:
                    msg += f" (+{len(skipped) - 5} more)"
            st.session_state["apq_flash"] = (
                "success" if done and not skipped else "warning", msg)
            safe_rerun()

        with b1:
            if st.button(f"Approve selected ({len(_sel)})", key="apq_bulk_ok",
                         type="primary", use_container_width=True,
                         disabled=not _sel):
                _bulk_decide("Approved")
        with b2:
            if st.button(f"Reject selected ({len(_sel)})", key="apq_bulk_no",
                         use_container_width=True, disabled=not _sel):
                _bulk_decide("Rejected")

section_title(f"Adjustments Awaiting Approval ({len(df_queue)})", "clipboard")

if df_queue.empty:
    st.markdown(
        f'<div class="mcard" style="text-align:center;padding:2.5rem;color:{P["grey_700"]}">'
        f'<div>{icon("check-circle", size=28, color=P["success"], valign="0")}</div>'
        f'<div style="font-size:0.9rem;margin-top:0.5rem">No adjustments awaiting approval</div>'
        f'</div>',
        unsafe_allow_html=True)
else:
    for _, row in df_queue.iterrows():
        adj_id      = row.get("ADJ_ID", "?")
        adj_short   = f"#{str(adj_id)[:8]}…"   # no DIMENSION_ADJ_ID yet (pre-approval); short hash keeps rows distinct
        scope       = str(row.get("PROCESS_TYPE", ""))
        adj_type    = str(row.get("ADJUSTMENT_TYPE", ""))
        entity      = str(row.get("ENTITY_CODE", "")) or "—"
        book        = str(row.get("BOOK_CODE", "")) or "—"
        submitted_by = str(row.get("SUBMITTED_BY", ""))
        submitted_at = row.get("SUBMITTED_AT", "")
        reason      = _htmlmod.escape(str(row.get("REASON", "")) or "—")
        scope_cfg   = SCOPE_CONFIG.get(scope, {})

        if hasattr(submitted_at, "strftime"):
            submitted_at = fmt_user_dt(submitted_at)

        has_overlap = (not df_overlaps.empty and (
            (df_overlaps["ADJ_ID_A"] == adj_id).any() or
            (df_overlaps["ADJ_ID_B"] == adj_id).any()
        ))
        _cob_lbl = row.get("COBID", "—")
        expander_label = (
            f'ADJ {adj_short} · COB {_cob_lbl} · {scope} · {adj_type} · '
            f'entity {entity} · book {book} · by {submitted_by}'
            + ("  — ⚠ OVERLAP" if has_overlap else "")
        )
        with st.expander(expander_label, expanded=has_overlap):
            col_info, col_actions = st.columns([3, 1])

            with col_info:
                st.markdown(status_badge("Pending Approval"), unsafe_allow_html=True)
                st.markdown("<br/>", unsafe_allow_html=True)

                # Key details
                meta_html = (
                    f'<table style="font-size:0.85rem;border-collapse:collapse;width:100%">'
                    f'<tr><td style="color:{P["grey_700"]};padding:3px 12px 3px 0;width:30%">COB</td>'
                    f'<td style="font-weight:600">{row.get("COBID", "—")}</td></tr>'
                    f'<tr><td style="color:{P["grey_700"]};padding:3px 12px 3px 0">Source COB</td>'
                    f'<td style="font-weight:600">{row.get("SOURCE_COBID", "—") if row.get("SOURCE_COBID") else "—"}</td></tr>'
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

                section_title("Filters Applied", "search")
                render_filter_chips(row.to_dict())

                # ── Overlap warnings ──────────────────────────────────────
                if not df_overlaps.empty:
                    adj_overlaps = df_overlaps[
                        (df_overlaps["ADJ_ID_A"] == adj_id) |
                        (df_overlaps["ADJ_ID_B"] == adj_id)
                    ]
                    if not adj_overlaps.empty:
                        other_ids = adj_overlaps.apply(
                            lambda r: r["ADJ_ID_B"] if r["ADJ_ID_A"] == adj_id
                                      else r["ADJ_ID_A"], axis=1
                        ).tolist()
                        rows_html = "".join(
                            f'<tr>'
                            f'<td style="padding:3px 10px 3px 0;font-size:0.78rem;'
                            f'font-weight:700;white-space:nowrap">'
                            f'ADJ #{str(r["ADJ_ID_B"] if r["ADJ_ID_A"] == adj_id else r["ADJ_ID_A"])[:8]}…'
                            f'</td>'
                            f'<td style="padding:3px 0;font-size:0.78rem;color:{P["grey_700"]}">'
                            f'{_htmlmod.escape(str(r.get("ALERT_MESSAGE","")).strip()) or "Overlapping filters on same COB"}'
                            f'</td>'
                            f'</tr>'
                            for _, r in adj_overlaps.iterrows()
                        )
                        st.markdown(
                            f'<div style="background:#FFF8E1;border:1px solid #FFD54F;'
                            f'border-left:4px solid #F9A825;border-radius:8px;'
                            f'padding:0.7rem 1rem;margin:0.8rem 0">'
                            f'<div style="font-weight:700;font-size:0.82rem;color:#E65100;'
                            f'margin-bottom:0.4rem">{icon("alert-triangle", size=13, color="#B45309")} Overlap Detected with '
                            f'{len(other_ids)} adjustment(s)</div>'
                            f'<table style="width:100%;border-collapse:collapse">'
                            f'{rows_html}</table>'
                            f'<div style="font-size:0.72rem;color:#795548;margin-top:0.4rem">'
                            f'These adjustments target overlapping data. '
                            f'Review carefully before approving.</div>'
                            f'</div>',
                            unsafe_allow_html=True)

                st.markdown(
                    f'<br/><div style="font-size:0.85rem"><strong>Business Reason:</strong><br/>'
                    f'<span style="color:{P["grey_700"]}">{reason}</span></div>',
                    unsafe_allow_html=True)

                if (pd.notna(row.get("SCALE_FACTOR")) and row.get("SCALE_FACTOR")
                        and float(row.get("SCALE_FACTOR", 1)) != 1):
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

                # ── Authorization guards ──
                is_own_adjustment = (
                    user and submitted_by and
                    str(user).strip().upper() == str(submitted_by).strip().upper()
                )
                can_approve_scope = (
                    is_approver and (
                        not approver_scopes or  # empty set = all scopes
                        scope.upper() in approver_scopes
                    )
                )

                if is_own_adjustment:
                    st.markdown(
                        f'<div style="background:#FFF3CD;border:1px solid #FFECB5;'
                        f'border-radius:6px;padding:0.6rem;font-size:0.8rem;text-align:center;'
                        f'color:#664D03;margin-bottom:0.5rem">'
                        f'{icon("alert-triangle", size=13, color="#B45309")} You cannot approve your own adjustment</div>',
                        unsafe_allow_html=True)
                elif not can_approve_scope:
                    st.markdown(
                        f'<div style="background:#F8D7DA;border:1px solid #F5C2C7;'
                        f'border-radius:6px;padding:0.6rem;font-size:0.8rem;text-align:center;'
                        f'color:#842029;margin-bottom:0.5rem">'
                        f'{icon("lock", size=13, color=P["grey_700"])} Not authorized for {scope}</div>',
                        unsafe_allow_html=True)

                actions_enabled = (is_approver and can_approve_scope
                                   and not is_own_adjustment and _identity_ok)

                def _decide(new_status: str, comment: str) -> None:
                    """Decision enforced SERVER-SIDE by SP_DECIDE_ADJUSTMENT:
                    caller identity from CURRENT_USER(), active-approver +
                    scope check, self-approval refusal, guarded transition and
                    audit all happen in the database — the UI checks above are
                    UX only and cannot be bypassed by skipping them."""
                    import json as _json
                    res = run_query(
                        f"CALL ADJUSTMENT_APP.SP_DECIDE_ADJUSTMENT("
                        f"'{_esc(adj_id)}', '{_esc(new_status)}', '{_esc(comment)}', "
                        f"'{_esc(user)}')")
                    try:
                        out = _json.loads(str(res[0][0])) if res else {}
                    except (ValueError, TypeError, IndexError):
                        out = {}
                    if out.get("status") == "ok":
                        st.session_state["apq_flash"] = (
                            "success", f"ADJ {adj_short} {new_status.lower()}.")
                    else:
                        st.session_state["apq_flash"] = (
                            "warning",
                            f"ADJ {adj_short} was NOT {new_status.lower()} — "
                            f"{out.get('message', 'the decision was not applied')}")

                # Approve
                if st.button("Approve", key=f"approve_{adj_id}",
                             use_container_width=True, type="primary",
                             disabled=not actions_enabled):
                    try:
                        _decide("Approved", f"Approved by {user}")
                        safe_rerun()
                    except Exception as ex:
                        st.error(f"Approval failed. {friendly_error(ex)}")

                st.markdown("<div style='height:0.5rem'></div>", unsafe_allow_html=True)

                # Reject
                reject_reason = st.text_input(
                    "Rejection reason", key=f"reject_reason_{adj_id}",
                    label_visibility="collapsed")
                if st.button("Reject", key=f"reject_{adj_id}",
                             use_container_width=True,
                             disabled=not actions_enabled):
                    try:
                        _decide("Rejected", reject_reason or "Rejected")
                        safe_rerun()
                    except Exception as ex:
                        st.error(f"Rejection failed. {friendly_error(ex)}")

# ──────────────────────────────────────────────────────────────────────────────
# COB RE-OPEN REQUESTS (sign-off lifecycle)
# A signed-off COB blocks new adjustments. Business users request a re-open on
# the New Adjustment page; approvers action it here. Approving re-opens the
# COB (submissions allowed) until it is signed off again from the app.
# ──────────────────────────────────────────────────────────────────────────────

st.markdown("<br/>", unsafe_allow_html=True)
section_title("COB Sign-Off / Re-open Requests", "unlock")


def _decide_signoff_change(cobid, scope, entity, sub, approve, comment, verb):
    """Decision enforced SERVER-SIDE by SP_DECIDE_SIGNOFF_CHANGE (caller
    identity, active-approver + scope check, requester refusal, guarded
    transition + sign-off history) — one path for both sign-off and re-open
    requests. The UI guards are UX only."""
    import json as _json
    decision = "Approved" if approve else "Rejected"
    res = run_query(
        f"CALL ADJUSTMENT_APP.SP_DECIDE_SIGNOFF_CHANGE("
        f"{int(cobid)}, '{_esc(scope)}', '{_esc(entity or chr(42))}', "
        f"'{_esc(sub or '')}', "
        f"'{decision}', '{_esc(comment)}', '{_esc(user)}')")
    try:
        out = _json.loads(str(res[0][0])) if res else {}
    except (ValueError, TypeError, IndexError):
        out = {}
    if out.get("status") == "ok":
        done = (f"{verb} approved — now {out.get('new_status', '?')}"
                if approve else f"{verb} request rejected")
        st.session_state["apq_flash"] = (
            "success", f"COB {cobid} / {scope}: {done}.")
    else:
        st.session_state["apq_flash"] = (
            "warning",
            f"COB {cobid} / {scope} was NOT changed — "
            f"{out.get('message', 'the decision was not applied')}")


try:
    df_soreq = run_query_df("""
        SELECT COBID, PROCESS_TYPE, ENTITY_CODE, SUB_TYPE, SIGN_OFF_STATUS,
               SIGNOFF_SOURCE, SIGN_OFF_BY,
               REOPEN_REQUESTED_BY, REOPEN_REQUESTED_AT, REOPEN_REASON
        FROM ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS
        WHERE UPPER(SIGN_OFF_STATUS) IN ('REOPEN_REQUESTED', 'SIGNOFF_REQUESTED')
        ORDER BY REOPEN_REQUESTED_AT
    """)
except Exception as _ex:
    df_soreq = pd.DataFrame()
    st.info(f"Sign-off table not available: {_ex}")

if df_soreq.empty:
    st.caption("No pending COB sign-off or re-open requests.")
else:
    for _, rr in df_soreq.iterrows():
        r_cob    = int(rr["COBID"])
        r_scope  = str(rr["PROCESS_TYPE"])
        r_ent    = str(rr["ENTITY_CODE"] or "*")
        _sub_raw = rr.get("SUB_TYPE")
        r_sub    = "" if (_sub_raw is None or pd.isna(_sub_raw)) else str(_sub_raw)
        r_ent_tx = ("all entities" if r_ent == "*" else r_ent)                    + (f" / {r_sub}" if r_sub else "")
        r_by     = str(rr["REOPEN_REQUESTED_BY"] or "—")
        r_reason = str(rr["REOPEN_REASON"] or "—")
        _is_signoff = str(rr["SIGN_OFF_STATUS"]).upper() == "SIGNOFF_REQUESTED"
        r_verb   = "Sign-off" if _is_signoff else "Re-open"
        _verb_col = "#B45309" if _is_signoff else P["danger"]
        with bordered_container():
            c_info, c_act = st.columns([2.4, 1])
            with c_info:
                # Header: what + where, as pills so it scans like the rest of
                # the app (Sign-off orange, Re-open red; scope in its colour).
                st.markdown(
                    '<div style="display:flex;align-items:center;gap:8px;'
                    'flex-wrap:wrap;margin-bottom:4px">'
                    + _pill(f"{r_verb.upper()} REQUEST", _verb_col)
                    + _scope_pill(r_scope)
                    + f'<span style="font-weight:700;font-size:0.95rem">COB {r_cob}'
                    + f' · {_htmlmod.escape(r_ent_tx)}</span></div>',
                    unsafe_allow_html=True)
                st.markdown(
                    f'<div style="font-size:0.8rem;color:{P["grey_700"]}">'
                    f'Requested by <strong>{_htmlmod.escape(r_by)}</strong> at '
                    f'{fmt_user_dt(rr["REOPEN_REQUESTED_AT"])}</div>',
                    unsafe_allow_html=True)
                if _is_signoff:
                    _ctx = ("Approving <strong>signs the COB off</strong> "
                            "(blocks new adjustments); rejecting keeps it open.")
                else:
                    _ctx = ("Originally signed off by "
                            f"<strong>{_htmlmod.escape(str(rr['SIGN_OFF_BY'] or '—'))}</strong> "
                            f"({_htmlmod.escape(str(rr['SIGNOFF_SOURCE'] or 'EXTERNAL'))}). "
                            "Approving <strong>re-opens the COB</strong> for adjustments.")
                st.markdown(
                    f'<div style="font-size:0.8rem;color:{P["grey_700"]};'
                    f'margin-top:2px">{_ctx}</div>', unsafe_allow_html=True)
                st.markdown(
                    f'<div style="font-size:0.84rem;margin-top:6px">'
                    f'<span style="color:{P["grey_700"]}">Reason:</span> '
                    f'{_htmlmod.escape(r_reason)}</div>', unsafe_allow_html=True)
            with c_act:
                _own_req = (user and r_by != "—"
                            and user.strip().upper() == r_by.strip().upper())
                _can_scope = is_approver and (
                    not approver_scopes or r_scope.upper() in approver_scopes)
                if _own_req:
                    st.caption(f"You requested this {r_verb.lower()} — another "
                               f"approver must decide it.")
                elif not _can_scope:
                    st.caption(f"Not authorized for {r_scope}.")
                _enabled = _can_scope and not _own_req and _identity_ok
                if st.button(f"Approve {r_verb.lower()}",
                             key=f"ro_ok_{r_cob}_{r_scope}_{r_ent}_{r_sub}",
                             type="primary", use_container_width=True,
                             disabled=not _enabled):
                    try:
                        _decide_signoff_change(
                            r_cob, r_scope, r_ent, r_sub, True,
                            f"{r_verb} approved by {user}", r_verb)
                        safe_rerun()
                    except Exception as ex:
                        st.error(f"Approve failed. {friendly_error(ex)}")
                _ro_comment = st.text_input(
                    "Rejection reason", key=f"ro_rr_{r_cob}_{r_scope}_{r_ent}_{r_sub}",
                    label_visibility="collapsed",
                    placeholder="Rejection reason")
                if st.button("Reject", key=f"ro_no_{r_cob}_{r_scope}_{r_ent}_{r_sub}",
                             use_container_width=True, disabled=not _enabled):
                    try:
                        _decide_signoff_change(
                            r_cob, r_scope, r_ent, r_sub, False,
                            _ro_comment or f"{r_verb} request rejected", r_verb)
                        safe_rerun()
                    except Exception as ex:
                        st.error(f"Reject failed. {friendly_error(ex)}")

# ──────────────────────────────────────────────────────────────────────────────
# RECENTLY APPROVED / REJECTED
# ──────────────────────────────────────────────────────────────────────────────

st.markdown("<br/>", unsafe_allow_html=True)
section_title("Recently Approved / Rejected", "file-text")

def _rc_pill(text, color):
    return (f'<span style="background:{color}18;color:{color};'
            f'border:1px solid {color}55;border-radius:99px;padding:1px 10px;'
            f'font-size:0.74rem;font-weight:700;white-space:nowrap">{text}</span>')


def _rc_scope_pill(scope):
    cfg = SCOPE_CONFIG.get(str(scope), {})
    return _rc_pill(_htmlmod.escape(str(scope) or "—"),
                    cfg.get("color", P["grey_700"]))


def _rc_cell(v):
    s = "" if v is None else " ".join(str(v).split())
    return _htmlmod.escape(s).replace("$", "&#36;")


try:
    df_recent = run_query_df("""
        SELECT h.DIMENSION_ADJ_ID, h.COBID, h.PROCESS_TYPE,
               h.ADJUSTMENT_TYPE, h.ENTITY_CODE,
               h.USERNAME AS SUBMITTED_BY,
               sh.NEW_STATUS, sh.CHANGED_BY AS ACTIONED_BY, sh.CHANGED_AT,
               sh.COMMENT
        FROM ADJUSTMENT_APP.ADJ_STATUS_HISTORY sh
        INNER JOIN ADJUSTMENT_APP.ADJ_HEADER h ON h.ADJ_ID = sh.ADJ_ID
        WHERE sh.NEW_STATUS IN ('Approved', 'Rejected')
          AND sh.OLD_STATUS = 'Pending Approval'
        ORDER BY sh.CHANGED_AT DESC
        LIMIT 200
    """)

    if df_recent.empty:
        st.info("No recent approval/rejection activity.")
    else:
        df_recent = df_recent.reset_index(drop=True)

        # Filters — same idea as the Sign-Off status grid.
        _rc1, _rc2, _rc3 = st.columns(3)
        with _rc1:
            _cobs = sorted(df_recent["COBID"].dropna().astype(int).unique().tolist(),
                           reverse=True)
            f_cob = st.multiselect("COB", _cobs, default=[], key="rc_f_cob",
                                   format_func=lambda v: str(v),
                                   help="Empty = all COBs.")
        with _rc2:
            f_scope = st.multiselect(
                "Scope", sorted(df_recent["PROCESS_TYPE"].dropna().unique().tolist()),
                default=[], key="rc_f_scope")
        with _rc3:
            f_out = st.multiselect("Outcome", ["Approved", "Rejected"],
                                   default=[], key="rc_f_out")

        _d = df_recent
        if f_cob:
            _d = _d[_d["COBID"].astype("Int64").isin(f_cob)]
        if f_scope:
            _d = _d[_d["PROCESS_TYPE"].isin(f_scope)]
        if f_out:
            _d = _d[_d["NEW_STATUS"].isin(f_out)]

        if _d.empty:
            st.info("Nothing matches the filters.")
        else:
            _rows = []
            for _, r in _d.iterrows():
                _out = str(r.get("NEW_STATUS") or "")
                _oc = STATUS_COLORS.get(_out, P["grey_700"])
                _rows.append([
                    f'<strong>{_htmlmod.escape(str(fmt_adj_id(r.get("DIMENSION_ADJ_ID"))))}</strong>',
                    _rc_pill(_htmlmod.escape(_out.upper()), _oc),
                    f'<strong>{"" if pd.isna(r.get("COBID")) else int(r.get("COBID"))}</strong>',
                    _rc_scope_pill(r.get("PROCESS_TYPE")),
                    _rc_cell(r.get("ADJUSTMENT_TYPE")) or "—",
                    _rc_cell(r.get("ENTITY_CODE")) or "—",
                    _rc_cell(r.get("SUBMITTED_BY")) or "—",
                    _rc_cell(r.get("ACTIONED_BY")) or "—",
                    fmt_user_dt(r.get("CHANGED_AT"), "%d %b %Y %H:%M"),
                    f'<span style="color:{P["grey_700"]}">'
                    f'{_rc_cell(r.get("COMMENT"))[:160]}</span>',
                ])
            render_grid(
                ["Adj", "Outcome", "COB", "Scope", "Type", "Entity",
                 "Submitted by", "Decided by", "When", "Comment"],
                _rows)
            st.caption(f"{len(_d)} decision(s), newest first.")
except Exception as _ex:
    st.info(f"No approval history available yet. ({_ex})")
