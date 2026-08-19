"""
Sign-Off — COB Sign-Off Cockpit & Lifecycle
============================================
Redesigned 2026-08 (Marcos):
  • Sync sits at the TOP — open COBs come from the upstream publish feed
    (BATCH.PUBLISH_SIGNOFF_STATUS); there is no manual "open the COB".
  • A per-scope COCKPIT shows the sign-off state of ONE COB (latest by
    default) at a glance.
  • ONE contextual action per state — an OPEN/REOPENED entity offers
    "Request sign-off", a SIGNED_OFF one offers "Request re-open", a pending
    one shows who is waiting on whom. Both actions carry the
    "Requires approval" flag, ticked and LOCKED by policy (enable in code
    only if the business ever drops the approval requirement).
  • The full status grid is filterable and defaults to the latest COB.
Reads: ADJ_SIGNOFF_STATUS. Writes via SP_REQUEST_SIGNOFF_CHANGE only;
approvals happen on the Approval Queue page.
"""
import json
import streamlit as st
import pandas as pd

st.set_page_config(page_title="Sign-Off · MUFG", page_icon="🔒", layout="wide", initial_sidebar_state="expanded")

from utils.styles import (inject_css, render_sidebar, section_title, P,
                          ALL_SCOPES, icon, render_df_table, bordered_container)
from utils.snowflake_conn import run_query, run_query_df, current_user_name, safe_rerun


def _esc(val):
    """Escape a SQL string literal (backslashes first, then quotes)."""
    return str(val).replace("\\", "\\\\").replace("'", "''") if val is not None else ""


def _pill(text, color) -> str:
    return (f'<span style="background:{color}18;color:{color};border:1px solid {color}55;'
            f'border-radius:99px;padding:1px 10px;font-size:0.74rem;font-weight:700;'
            f'white-space:nowrap">{text}</span>')


# status → (label, color, blocks submissions?)
_STATUS_META = {
    "OPEN":              ("OPEN",              P["success"], False),
    "REOPENED":          ("RE-OPENED",         P["info"],    False),
    "SIGNOFF_REQUESTED": ("SIGN-OFF REQUESTED", "#B45309",   True),
    "REOPEN_REQUESTED":  ("RE-OPEN REQUESTED",  "#B45309",   True),
    "SIGNED_OFF":        ("SIGNED OFF",        P["danger"],  True),
}

inject_css()
render_sidebar()

user = current_user_name()


def _sync_summary(raw) -> str:
    """Turn the sync SP's JSON result into a sentence a user can read."""
    try:
        out = json.loads(str(raw)) if not isinstance(raw, dict) else raw
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


# ══════════════════════════════════════════════════════════════════════════════
# HEADER — title left, SYNC on top right
# ══════════════════════════════════════════════════════════════════════════════

_hd1, _hd2 = st.columns([3, 1.1])
with _hd1:
    st.markdown("## COB Sign-Off")
    st.markdown(
        f"<span style='color:{P['grey_700']};font-size:0.9rem'>"
        "Open COBs come from the upstream publish system. Signing off blocks "
        "new adjustments for a COB; re-opening allows them again — both go "
        "through approval on the Approval Queue page.</span>",
        unsafe_allow_html=True)
with _hd2:
    st.markdown("<div style='height:0.6rem'></div>", unsafe_allow_html=True)
    if st.button("⟳  Sync from upstream feed", key="signoff_sync_btn",
                 type="primary", use_container_width=True,
                 help="Pulls every COB/scope/entity from the publish feed: "
                      "not signed off → OPEN, signed off → SIGNED_OFF. Also "
                      "runs automatically every 30 minutes."):
        try:
            res = run_query("CALL ADJUSTMENT_APP.SP_SYNC_SIGNOFF_STATUS()")
            st.session_state["so_flash"] = (
                "success", _sync_summary(res[0][0] if res else "no result"))
        except Exception as ex:
            st.session_state["so_flash"] = (
                "warning", f"Sync failed. The database reported: {ex}")
        safe_rerun()

_flash = st.session_state.pop("so_flash", None)
if _flash:
    (st.success if _flash[0] == "success" else st.warning)(_flash[1])

# ══════════════════════════════════════════════════════════════════════════════
# LOAD — all sign-off rows (one query drives the whole page)
# ══════════════════════════════════════════════════════════════════════════════

df_all = pd.DataFrame()
try:
    df_all = run_query_df("""
        SELECT COBID, PROCESS_TYPE, ENTITY_CODE, SIGN_OFF_STATUS, SIGNOFF_SOURCE,
               SIGN_OFF_BY, SIGN_OFF_TIMESTAMP,
               REOPEN_REQUESTED_BY, REOPEN_REQUESTED_AT, REOPEN_REASON,
               REOPEN_APPROVED_BY, REOPEN_APPROVED_AT, UPDATED_DATE
        FROM ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS
        ORDER BY COBID DESC, PROCESS_TYPE, ENTITY_CODE
    """)
except Exception as e:
    st.info(f"Sign-off table not available: {e}")

if df_all.empty:
    st.info("No sign-off entries yet — press **Sync from upstream feed** "
            "(top right) to pull the open and signed-off COBs.")
    st.stop()

df_all["ENTITY_CODE"] = df_all["ENTITY_CODE"].fillna("*")
df_all["_SU"] = df_all["SIGN_OFF_STATUS"].astype(str).str.upper()
_all_cobs = sorted(df_all["COBID"].astype(int).unique().tolist(), reverse=True)

# ══════════════════════════════════════════════════════════════════════════════
# COCKPIT — the sign-off state of ONE COB, per scope, at a glance
# ══════════════════════════════════════════════════════════════════════════════

_ck1, _ck2 = st.columns([1, 3])
with _ck1:
    sel_cob = st.selectbox("COB", _all_cobs, index=0, key="so_cob",
                           format_func=lambda v: str(v),
                           help="Latest COB by default — every panel below "
                                "covers this COB.")
df_cob = df_all[df_all["COBID"].astype(int) == int(sel_cob)]
with _ck2:
    _n_blocked = int(df_cob["_SU"].isin(
        [s for s, (_, _, b) in _STATUS_META.items() if b]).sum())
    _n_pending = int(df_cob["_SU"].isin(
        ["SIGNOFF_REQUESTED", "REOPEN_REQUESTED"]).sum())
    st.markdown(
        f'<div style="margin-top:1.85rem;font-size:0.85rem;color:{P["grey_700"]}">'
        f'{len(df_cob)} entr(ies) on COB {sel_cob} · '
        f'{_n_blocked} blocking submissions · '
        f'{_n_pending} awaiting approval</div>',
        unsafe_allow_html=True)

section_title(f"Sign-Off Cockpit — COB {sel_cob}", "lock")

_scopes_shown = [s for s in ALL_SCOPES if s != "FRTBALL"]
_cols = st.columns(len(_scopes_shown))
for _c, _scope in zip(_cols, _scopes_shown):
    _rows = df_cob[df_cob["PROCESS_TYPE"].str.upper() == _scope.upper()]
    if _rows.empty:
        body = (f'<div style="font-size:0.78rem;color:{P["grey_700"]}">'
                f'no entry<br/><span style="font-size:0.7rem">(not in the '
                f'feed for this COB)</span></div>')
        border = P["border"]
    else:
        # One pill per distinct status, with entity counts
        _counts = _rows["_SU"].value_counts().to_dict()
        pills = []
        for _stat in ("OPEN", "REOPENED", "SIGNOFF_REQUESTED",
                      "REOPEN_REQUESTED", "SIGNED_OFF"):
            if _stat in _counts:
                lbl, col, _ = _STATUS_META[_stat]
                pills.append(_pill(f"{lbl} · {_counts[_stat]}", col))
        body = ('<div style="line-height:2">' + "<br/>".join(pills) + "</div>")
        _all_blocked = _rows["_SU"].isin(
            [s for s, (_, _, b) in _STATUS_META.items() if b]).all()
        border = P["danger"] if _all_blocked else (
            "#B45309" if _rows["_SU"].isin(
                ["SIGNOFF_REQUESTED", "REOPEN_REQUESTED"]).any()
            else P["success"])
    _c.markdown(
        f'<div style="background:{P["white"]};border:1px solid {P["border"]};'
        f'border-top:3px solid {border};border-radius:8px;padding:0.7rem;'
        f'text-align:center;min-height:110px">'
        f'<div style="font-size:0.8rem;font-weight:800;margin-bottom:6px">{_scope}</div>'
        f'{body}</div>',
        unsafe_allow_html=True)

st.markdown("<br/>", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# ACTION — one clear, contextual action for one entity of the selected COB
# ══════════════════════════════════════════════════════════════════════════════

with bordered_container():
    section_title("Change a Sign-Off", "edit")
    a1, a2 = st.columns(2)
    with a1:
        _scope_opts = sorted(df_cob["PROCESS_TYPE"].unique().tolist())
        act_scope = st.selectbox("Scope", _scope_opts, key="so_act_scope")
    with a2:
        _ent_opts = sorted(df_cob[df_cob["PROCESS_TYPE"] == act_scope]
                           ["ENTITY_CODE"].unique().tolist())
        act_entity = st.selectbox("Entity ('*' = whole scope)", _ent_opts,
                                  key="so_act_entity")

    _row = df_cob[(df_cob["PROCESS_TYPE"] == act_scope)
                  & (df_cob["ENTITY_CODE"] == act_entity)]
    cur = str(_row["_SU"].values[0]) if not _row.empty else "?"
    lbl, col, blocks = _STATUS_META.get(cur, (cur, P["grey_700"], False))
    _r = _row.iloc[0] if not _row.empty else {}

    st.markdown(
        f'<div style="margin:0.4rem 0 0.6rem;font-size:0.95rem">'
        f'COB <strong>{sel_cob}</strong> · <strong>{act_scope}</strong> · '
        f'<strong>{act_entity}</strong> is currently &nbsp;{_pill(lbl, col)}'
        f'&nbsp;<span style="font-size:0.8rem;color:{P["grey_700"]}">'
        f'{"— new adjustments are BLOCKED" if blocks else "— new adjustments are allowed"}'
        f'</span></div>',
        unsafe_allow_html=True)

    def _request(action: str, verb: str, reason: str) -> None:
        res = run_query(
            f"CALL ADJUSTMENT_APP.SP_REQUEST_SIGNOFF_CHANGE("
            f"{int(sel_cob)}, '{_esc(act_scope)}', '{_esc(act_entity)}', "
            f"'{action}', '{_esc(reason.strip()[:490])}', TRUE, '{_esc(user)}')")
        try:
            out = json.loads(str(res[0][0])) if res else {}
        except (ValueError, TypeError, IndexError):
            out = {}
        if out.get("status") == "ok":
            st.session_state["so_flash"] = (
                "success",
                f"{verb} requested for COB {sel_cob} / {act_scope} "
                f"({act_entity}) — an approver decides it on the Approval "
                f"Queue page. Submissions are blocked while it is pending.")
        else:
            st.session_state["so_flash"] = (
                "warning", f"{verb} request was NOT accepted — "
                           f"{out.get('message', 'no detail')}")
        safe_rerun()

    if cur in ("OPEN", "REOPENED"):
        st.markdown(
            f'<div style="font-size:0.85rem;color:{P["grey_700"]}">'
            f'The one available action: <strong>request a sign-off</strong>. '
            f'Once an approver approves it, the COB is closed for {act_scope} '
            f'and no new adjustments can be submitted.</div>',
            unsafe_allow_html=True)
        reason = st.text_input("Reason *", key="so_act_reason_s",
                               placeholder="e.g. all adjustments for this COB are done")
        # Approval is REQUIRED by policy — flag shown, ticked and locked.
        # To drop the requirement later: remove disabled=True and pass the
        # checkbox value to _request instead of the hard-coded TRUE.
        st.checkbox("Request approval (required by policy)", value=True,
                    disabled=True, key="so_act_appr_s")
        if st.button(f"Request sign-off — {act_scope} ({act_entity})",
                     key="so_act_btn_s", type="primary",
                     disabled=not reason.strip()):
            _request("SIGNOFF", "Sign-off", reason)

    elif cur == "SIGNED_OFF":
        _by = str(_r.get("SIGN_OFF_BY") or "the upstream feed")
        st.markdown(
            f'<div style="font-size:0.85rem;color:{P["grey_700"]}">'
            f'Signed off by <strong>{_by}</strong> '
            f'({_r.get("SIGNOFF_SOURCE") or "EXTERNAL"}). The one available '
            f'action: <strong>request a re-open</strong>. Once approved, new '
            f'adjustments are allowed again until the COB is signed off '
            f'again.</div>',
            unsafe_allow_html=True)
        reason = st.text_input("Reason *", key="so_act_reason_r",
                               placeholder="e.g. late booking needs an adjustment on this COB")
        st.checkbox("Request approval (required by policy)", value=True,
                    disabled=True, key="so_act_appr_r")
        if st.button(f"Request re-open — {act_scope} ({act_entity})",
                     key="so_act_btn_r", type="primary",
                     disabled=not reason.strip()):
            _request("REOPEN", "Re-open", reason)

    elif cur in ("SIGNOFF_REQUESTED", "REOPEN_REQUESTED"):
        _verb = "sign-off" if cur == "SIGNOFF_REQUESTED" else "re-open"
        st.warning(
            f"A {_verb} request by "
            f"**{_r.get('REOPEN_REQUESTED_BY') or '—'}** "
            f"({_r.get('REOPEN_REQUESTED_AT')}) is awaiting approval on the "
            f"**Approval Queue** page — no further action is possible here "
            f"until it is decided. Reason: {_r.get('REOPEN_REASON') or '—'}")

st.markdown("<br/>", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# STATUS GRID — filterable; defaults to the selected (latest) COB
# ══════════════════════════════════════════════════════════════════════════════

section_title("All Sign-Off Entries", "table")
g1, g2, g3, g4 = st.columns(4)
with g1:
    f_cobs = st.multiselect("COB", _all_cobs, default=[int(sel_cob)],
                            key="so_f_cob", format_func=lambda v: str(v),
                            help="Empty = all COBs.")
with g2:
    f_scopes = st.multiselect("Scope",
                              sorted(df_all["PROCESS_TYPE"].unique().tolist()),
                              default=[], key="so_f_scope")
with g3:
    f_status = st.multiselect(
        "Status", list(_STATUS_META.keys()), default=[], key="so_f_status",
        format_func=lambda v: _STATUS_META[v][0].title())
with g4:
    f_entity = st.multiselect("Entity",
                              sorted(df_all["ENTITY_CODE"].unique().tolist()),
                              default=[], key="so_f_entity")

df_grid = df_all
if f_cobs:
    df_grid = df_grid[df_grid["COBID"].astype(int).isin(f_cobs)]
if f_scopes:
    df_grid = df_grid[df_grid["PROCESS_TYPE"].isin(f_scopes)]
if f_status:
    df_grid = df_grid[df_grid["_SU"].isin(f_status)]
if f_entity:
    df_grid = df_grid[df_grid["ENTITY_CODE"].isin(f_entity)]

st.caption(f"{len(df_grid)} entr(ies)"
           + ("" if f_cobs or f_scopes or f_status or f_entity
              else " — no filters applied"))
render_df_table(df_grid.drop(columns=["_SU"]), max_rows=300, height=340)
