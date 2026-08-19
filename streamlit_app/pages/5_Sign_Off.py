"""
Sign-Off — COB Sign-Off Status & Lifecycle
===========================================
There is NO standalone "open the COB" concept: open COBs come from the
upstream publish feed (BATCH.PUBLISH_SIGNOFF_STATUS), materialised by the
sync (button here + 30-minute task). This page raises the two lifecycle
changes the app can make — SIGN OFF an open/re-opened COB, and RE-OPEN a
signed-off one — and BOTH go through approval on the Approval Queue page
(the "requires approval" checkboxes are deliberately locked ON; enable
them in code only if the business later drops the approval requirement).
Reads: ADJ_SIGNOFF_STATUS. Writes via SP_REQUEST_SIGNOFF_CHANGE only.
"""
import streamlit as st
import pandas as pd

st.set_page_config(page_title="Sign-Off · MUFG", page_icon="🔒", layout="wide", initial_sidebar_state="expanded")

from utils.styles import (inject_css, render_sidebar, section_title, P,
                          SCOPE_CONFIG, icon, render_df_table)
from utils.snowflake_conn import run_query, run_query_df, current_user_name, safe_rerun


def _esc(val):
    """Escape single quotes for safe SQL interpolation."""
    return str(val).replace("\\", "\\\\").replace("'", "''") if val is not None else ""


inject_css()
render_sidebar()

user = current_user_name()

st.markdown("## COB Sign-Off")
st.markdown(
    f'<div style="background:{P["info_lt"]};border:1px solid #90CAF9;border-radius:8px;'
    f'padding:0.7rem 1rem;margin-bottom:1rem;font-size:0.85rem">'
    f'Open COBs come from the upstream publish system — the sync (button below, '
    f'also a 30-minute task) lists every COB/scope/entity that is open or signed '
    f'off. From here you can <strong>request a sign-off</strong> (open → signed '
    f'off) or <strong>request a re-open</strong> (signed off → open); both go to '
    f'an approver on the Approval Queue page before they take effect. While a COB '
    f'is <strong>SIGNED_OFF</strong> or has <strong>any request pending</strong>, '
    f'no new adjustments can be submitted for it. Every change is written to '
    f'<code>ADJ_SIGNOFF_HISTORY</code>.'
    f'</div>',
    unsafe_allow_html=True)

# Flash from the previous action — set before the rerun.
_flash = st.session_state.pop("so_flash", None)
if _flash:
    (st.success if _flash[0] == "success" else st.warning)(_flash[1])

# ── Current sign-off status (visible to every user) ──────────────────────────
section_title("Current Status", "lock")
df_signoff = pd.DataFrame()
try:
    df_signoff = run_query_df("""
        SELECT COBID, PROCESS_TYPE, ENTITY_CODE, SIGN_OFF_STATUS, SIGNOFF_SOURCE,
               SIGN_OFF_BY, SIGN_OFF_TIMESTAMP,
               REOPEN_REQUESTED_BY, REOPEN_REQUESTED_AT, REOPEN_REASON,
               REOPEN_APPROVED_BY, REOPEN_APPROVED_AT, UPDATED_DATE
        FROM ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS
        ORDER BY COBID DESC, PROCESS_TYPE
    """)

    if not df_signoff.empty:
        _su = df_signoff["SIGN_OFF_STATUS"].str.upper()
        _pending  = _su.isin(["SIGNOFF_REQUESTED", "REOPEN_REQUESTED"])
        _signed   = _su == "SIGNED_OFF"
        sm1, sm2, sm3, sm4 = st.columns(4)
        sm1.metric("Total Entries", len(df_signoff))
        sm2.metric("Signed off", int(_signed.sum()))
        sm3.metric("Pending approval", int(_pending.sum()))
        sm4.metric("Open / re-opened", int((~_signed & ~_pending).sum()))

        render_df_table(df_signoff, max_rows=200, height=300)
    else:
        st.info("No sign-off entries yet — run the upstream sync below to "
                "pull the open and signed-off COBs from the publish feed.")
except Exception as e:
    st.info(f"Sign-off table not available: {e}")

# ── Upstream sync (open + signed-off COBs from the publish feed) ─────────────
st.markdown("<br/>", unsafe_allow_html=True)
section_title("Upstream Sync", "refresh-cw")
st.caption("Pulls every COB/scope/entity from BATCH.PUBLISH_SIGNOFF_STATUS: "
           "not-signed-off rows appear as OPEN, signed-off rows as SIGNED_OFF. "
           "Runs automatically every 30 minutes — use the button to refresh now.")
def _sync_summary(raw) -> str:
    """Turn the sync SP's JSON result into a sentence a user can read."""
    import json as _json
    try:
        out = _json.loads(str(raw)) if not isinstance(raw, dict) else raw
    except (ValueError, TypeError):
        return f"Sync complete: {raw}"
    if out.get("status") == "skipped":
        return out.get("message", "Sync is currently paused.")
    opened = out.get("opened") or {}
    synced = out.get("synced") or {}
    n_open = sum(int(v or 0) for v in opened.values())
    n_sign = sum(int(v or 0) for v in synced.values())
    if not n_open and not n_sign:
        return ("Sync complete — everything is already up to date "
                "(no new open or signed-off COBs in the feed).")
    parts = []
    if n_open:
        _by = ", ".join(f"{s}: {int(v)}" for s, v in opened.items() if int(v or 0))
        parts.append(f"{n_open} new open entr{'y' if n_open == 1 else 'ies'} ({_by})")
    if n_sign:
        _by = ", ".join(f"{s}: {int(v)}" for s, v in synced.items() if int(v or 0))
        parts.append(f"{n_sign} newly signed off ({_by})")
    return "Sync complete — " + " and ".join(parts) + "."


if st.button("Sync from upstream feed now", key="signoff_sync_btn"):
    try:
        res = run_query("CALL ADJUSTMENT_APP.SP_SYNC_SIGNOFF_STATUS()")
        st.session_state["so_flash"] = (
            "success", _sync_summary(res[0][0] if res else "no result"))
    except Exception as ex:
        st.session_state["so_flash"] = (
            "warning", f"Sync failed. The database reported: {ex}")
    safe_rerun()

# ── Request a change (sign-off / re-open) — both approval-gated ──────────────
st.markdown("<br/>", unsafe_allow_html=True)
section_title("Request a Change", "edit")
st.caption("Both actions go to an approver on the Approval Queue page before "
           "they take effect. Requests block new adjustment submissions for "
           "the COB while pending.")

if df_signoff.empty:
    st.info("Nothing to request yet — sync from the upstream feed first.")
else:
    rc1, rc2, rc3 = st.columns(3)
    with rc1:
        _cobs = sorted(df_signoff["COBID"].unique(), reverse=True)
        sel_cob = st.selectbox("COBID", _cobs, key="so_req_cob")
    with rc2:
        _scopes = sorted(df_signoff[df_signoff["COBID"] == sel_cob]
                         ["PROCESS_TYPE"].unique())
        sel_scope = st.selectbox("Process Type", _scopes, key="so_req_scope")
    with rc3:
        _ents = sorted(df_signoff[
            (df_signoff["COBID"] == sel_cob)
            & (df_signoff["PROCESS_TYPE"] == sel_scope)
        ]["ENTITY_CODE"].fillna("*").unique())
        sel_entity = st.selectbox("Entity ('*' = whole scope)",
                                  _ents or ["*"], key="so_req_entity")

    _row = df_signoff[
        (df_signoff["COBID"] == sel_cob)
        & (df_signoff["PROCESS_TYPE"] == sel_scope)
        & (df_signoff["ENTITY_CODE"].fillna("*") == sel_entity)
    ]
    cur_status = (str(_row["SIGN_OFF_STATUS"].values[0]).upper()
                  if not _row.empty else "?")
    st.caption(f"Current status: **{cur_status}**")

    req_reason = st.text_input(
        "Reason for the request *", key="so_req_reason",
        placeholder="Why should this COB be signed off / re-opened?")

    def _request(action: str, verb: str) -> None:
        import json as _json
        res = run_query(
            f"CALL ADJUSTMENT_APP.SP_REQUEST_SIGNOFF_CHANGE("
            f"{int(sel_cob)}, '{_esc(sel_scope)}', '{_esc(sel_entity)}', "
            f"'{action}', '{_esc(req_reason.strip())}', TRUE, '{_esc(user)}')")
        try:
            out = _json.loads(str(res[0][0])) if res else {}
        except (ValueError, TypeError, IndexError):
            out = {}
        if out.get("status") == "ok":
            st.session_state["so_flash"] = (
                "success",
                f"{verb} requested for COB {sel_cob} / {sel_scope} "
                f"({sel_entity}) — awaiting approval on the Approval Queue "
                f"page.")
        else:
            st.session_state["so_flash"] = (
                "warning",
                f"{verb} request was NOT accepted — "
                f"{out.get('message', 'no detail')}")
        safe_rerun()

    a1, a2 = st.columns(2)
    with a1:
        # Approval is mandatory for sign-off today. The checkbox is rendered
        # LOCKED (disabled, ticked) on purpose — if the business later drops
        # the approval requirement, remove disabled=True and pass the value
        # to _request instead of the hard-coded TRUE.
        st.checkbox("Request approval for sign-off", value=True, disabled=True,
                    key="so_req_appr_signoff")
        if st.button("Request sign-off", key="so_btn_signoff",
                     type="primary", use_container_width=True,
                     disabled=(cur_status not in ("OPEN", "REOPENED")
                               or not req_reason.strip())):
            _request("SIGNOFF", "Sign-off")
        if cur_status not in ("OPEN", "REOPENED"):
            st.caption("Sign-off can only be requested for an OPEN or "
                       "REOPENED entry.")
    with a2:
        # Same locked-on pattern; re-open is expected to STAY approval-gated.
        st.checkbox("Request approval for re-open", value=True, disabled=True,
                    key="so_req_appr_reopen")
        if st.button("Request re-open", key="so_btn_reopen",
                     use_container_width=True,
                     disabled=(cur_status != "SIGNED_OFF"
                               or not req_reason.strip())):
            _request("REOPEN", "Re-open")
        if cur_status != "SIGNED_OFF":
            st.caption("Re-open can only be requested for a SIGNED_OFF entry.")

