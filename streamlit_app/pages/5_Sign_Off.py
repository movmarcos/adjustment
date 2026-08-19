"""
Sign-Off — COB Sign-Off Cockpit & Lifecycle
============================================
Redesigned 2026-08 (Marcos):
  • Sync sits at the TOP — open COBs come from the upstream publish feed
    (BATCH.PUBLISH_SIGNOFF_STATUS); there is no manual "open the COB".
  • A per-scope COCKPIT shows the sign-off state of ONE COB (latest by
    default) at a glance.
  • ONE contextual action per state — an OPEN/REOPENED entity offers
    "Sign off" (approval OPTIONAL: the checkbox is unchecked by default and
    the sign-off applies immediately; tick it to route via an approver), a
    SIGNED_OFF one offers "Request re-open" (approval REQUIRED — checkbox
    ticked and locked by policy), a pending one shows who is waiting on whom.
  • The full status grid is filterable and defaults to the latest COB.
Reads: ADJ_SIGNOFF_STATUS. Writes via SP_REQUEST_SIGNOFF_CHANGE only;
approvals happen on the Approval Queue page.
"""
import json
import streamlit as st
import pandas as pd

st.set_page_config(page_title="Sign-Off · MUFG", page_icon="🔒", layout="wide", initial_sidebar_state="expanded")

from utils.styles import (inject_css, render_sidebar, section_title, P,
                          ALL_SCOPES, icon, bordered_container)
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
        "new adjustments for a COB (immediate — or via approval if you ask "
        "for it); re-opening allows them again and always goes through "
        "approval on the Approval Queue page.</span>",
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

def _fmt_ts(v):
    try:
        return pd.to_datetime(v).strftime("%d %b %H:%M")
    except (ValueError, TypeError):
        return ""


def _scope_summary(rows):
    """Collapse a scope's per-entity rows into ONE sign-off story:
    (effective label, color, submissions text/color, detail sentence).
    A scope is one row per entity in the table — but for a human it is one
    fact: signed off, open, partially signed off, or awaiting a decision."""
    stats = rows["_SU"].tolist()
    pending = rows[rows["_SU"].isin(["SIGNOFF_REQUESTED", "REOPEN_REQUESTED"])]
    signed  = rows[rows["_SU"] == "SIGNED_OFF"]
    blocked = rows["_SU"].isin(
        [k for k, (_, _, b) in _STATUS_META.items() if b])

    if not pending.empty:
        _p = pending.iloc[0]
        _verb = ("Sign-off" if _p["_SU"] == "SIGNOFF_REQUESTED" else "Re-open")
        eff, col = f"{_verb.upper()} PENDING", "#B45309"
        detail = (f"{_verb} requested by "
                  f"{_p.get('REOPEN_REQUESTED_BY') or '—'} "
                  f"{_fmt_ts(_p.get('REOPEN_REQUESTED_AT'))} — awaiting "
                  f"approval on the Approval Queue page")
    elif blocked.all():
        eff, col = "SIGNED OFF", P["danger"]
        _s = signed.iloc[0] if not signed.empty else rows.iloc[0]
        detail = (f"Signed off by {_s.get('SIGN_OFF_BY') or 'the upstream feed'} "
                  f"{_fmt_ts(_s.get('SIGN_OFF_TIMESTAMP'))} "
                  f"({_s.get('SIGNOFF_SOURCE') or 'EXTERNAL'})")
    elif not signed.empty:
        eff, col = "PARTIALLY SIGNED OFF", "#B45309"
        _open_n = int((~blocked).sum())
        detail = (f"{len(signed)} entit{'y' if len(signed) == 1 else 'ies'} "
                  f"signed off, {_open_n} still open")
    elif (rows["_SU"] == "REOPENED").any():
        eff, col = "RE-OPENED", P["info"]
        _r = rows[rows["_SU"] == "REOPENED"].iloc[0]
        detail = (f"Re-opened (approved by "
                  f"{_r.get('REOPEN_APPROVED_BY') or '—'}) — sign off again "
                  f"when done")
    else:
        eff, col = "OPEN", P["success"]
        detail = "Open per the upstream feed — adjustments allowed"

    n_block = int(blocked.sum())
    if n_block == len(rows):
        sub, sub_col = "Blocked", P["danger"]
    elif n_block:
        sub, sub_col = f"Blocked for {n_block}/{len(rows)} entities", "#B45309"
    else:
        sub, sub_col = "Allowed", P["success"]
    return eff, col, sub, sub_col, detail


def _entity_chips(rows):
    """Compact per-entity chips; a lone '*' row means the whole scope."""
    if len(rows) == 1 and str(rows.iloc[0]["ENTITY_CODE"]) == "*":
        return f'<span style="font-size:0.75rem;color:{P["grey_700"]}">whole scope</span>'
    chips = []
    for _, r in rows.sort_values("ENTITY_CODE").iterrows():
        lbl, col, _ = _STATUS_META.get(str(r["_SU"]),
                                       (str(r["_SU"]), P["grey_700"], False))
        chips.append(
            f'<span style="background:{col}14;color:{col};border:1px solid {col}44;'
            f'border-radius:99px;padding:0 8px;font-size:0.7rem;font-weight:700;'
            f'white-space:nowrap">{r["ENTITY_CODE"]}&nbsp;·&nbsp;{lbl}</span>')
    return '<span style="line-height:1.9">' + " ".join(chips) + "</span>"


section_title(f"Sign-Off Cockpit — COB {sel_cob}", "lock")

_scopes_shown = [s for s in ALL_SCOPES if s != "FRTBALL"]
_cols = st.columns(len(_scopes_shown))
for _c, _scope in zip(_cols, _scopes_shown):
    _rows = df_cob[df_cob["PROCESS_TYPE"].str.upper() == _scope.upper()]
    if _rows.empty:
        body = (f'<div style="font-size:0.76rem;color:{P["grey_700"]};'
                f'margin-top:8px">not in the feed<br/>for this COB</div>')
        border = P["border"]
    else:
        eff, col, sub, sub_col, _ = _scope_summary(_rows)
        _n_ent = len(_rows)
        _ent_txt = ("whole scope" if _n_ent == 1
                    and str(_rows.iloc[0]["ENTITY_CODE"]) == "*"
                    else f"{_n_ent} entit{'y' if _n_ent == 1 else 'ies'}")
        body = (f'<div style="margin:6px 0 4px">{_pill(eff, col)}</div>'
                f'<div style="font-size:0.7rem;color:{P["grey_700"]}">{_ent_txt}'
                f' · submissions <span style="color:{sub_col};font-weight:700">'
                f'{sub.split(" for ")[0].lower()}</span></div>')
        border = col
    _c.markdown(
        f'<div style="background:{P["white"]};border:1px solid {P["border"]};'
        f'border-top:3px solid {border};border-radius:8px;padding:0.7rem;'
        f'text-align:center;min-height:96px">'
        f'<div style="font-size:0.8rem;font-weight:800">{_scope}</div>'
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

    def _request(action: str, verb: str, reason: str,
                 requires_approval: bool) -> None:
        _appr = "TRUE" if requires_approval else "FALSE"
        res = run_query(
            f"CALL ADJUSTMENT_APP.SP_REQUEST_SIGNOFF_CHANGE("
            f"{int(sel_cob)}, '{_esc(act_scope)}', '{_esc(act_entity)}', "
            f"'{action}', '{_esc(reason.strip()[:490])}', {_appr}, "
            f"'{_esc(user)}')")
        try:
            out = json.loads(str(res[0][0])) if res else {}
        except (ValueError, TypeError, IndexError):
            out = {}
        if out.get("status") == "ok":
            if out.get("pending_approval"):
                msg = (f"{verb} requested for COB {sel_cob} / {act_scope} "
                       f"({act_entity}) — an approver decides it on the "
                       f"Approval Queue page. Submissions are blocked while "
                       f"it is pending.")
            elif action == "SIGNOFF":
                msg = (f"COB {sel_cob} / {act_scope} ({act_entity}) is now "
                       f"SIGNED OFF — new adjustment submissions are blocked.")
            else:
                msg = (f"COB {sel_cob} / {act_scope} ({act_entity}) is now "
                       f"RE-OPENED — new adjustment submissions are allowed.")
            st.session_state["so_flash"] = ("success", msg)
        else:
            st.session_state["so_flash"] = (
                "warning", f"{verb} was NOT applied — "
                           f"{out.get('message', 'no detail')}")
        safe_rerun()

    if cur in ("OPEN", "REOPENED"):
        st.markdown(
            f'<div style="font-size:0.85rem;color:{P["grey_700"]}">'
            f'The one available action: <strong>sign off</strong>. It applies '
            f'immediately and closes the COB for {act_scope} (no new '
            f'adjustments) — tick the box below if you want an approver to '
            f'confirm it first.</div>',
            unsafe_allow_html=True)
        reason = st.text_input("Reason *", key="so_act_reason_s",
                               placeholder="e.g. all adjustments for this COB are done")
        # Approval for SIGN-OFF is OPTIONAL (Marcos): unchecked by default →
        # the sign-off applies immediately; tick it to route via an approver.
        _appr_s = st.checkbox(
            "Request approval before signing off (optional)", value=False,
            key="so_act_appr_s",
            help="Unchecked: the sign-off applies immediately. Checked: an "
                 "approver must approve it on the Approval Queue page first.")
        _btn_lbl = ("Request sign-off" if _appr_s else "Sign off") \
            + f" — {act_scope} ({act_entity})"
        if st.button(_btn_lbl, key="so_act_btn_s", type="primary",
                     disabled=not reason.strip()):
            _request("SIGNOFF", "Sign-off", reason, _appr_s)

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
        # Re-open stays approval-gated by policy — ticked and locked.
        st.checkbox("Request approval (required by policy)", value=True,
                    disabled=True, key="so_act_appr_r")
        if st.button(f"Request re-open — {act_scope} ({act_entity})",
                     key="so_act_btn_r", type="primary",
                     disabled=not reason.strip()):
            _request("REOPEN", "Re-open", reason, True)

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

section_title("Sign-Off Status", "table")
st.caption("One line per COB and scope — the entity chips show the per-entity "
           "state when a scope is split. Defaults to the selected COB.")
g1, g2, g3 = st.columns(3)
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
        format_func=lambda v: _STATUS_META[v][0].title(),
        help="Keeps a COB/scope line when ANY of its entities has one of "
             "the selected statuses.")

df_grid = df_all
if f_cobs:
    df_grid = df_grid[df_grid["COBID"].astype(int).isin(f_cobs)]
if f_scopes:
    df_grid = df_grid[df_grid["PROCESS_TYPE"].isin(f_scopes)]
if f_status:
    _keep = df_grid[df_grid["_SU"].isin(f_status)][["COBID", "PROCESS_TYPE"]]
    df_grid = df_grid.merge(_keep.drop_duplicates(),
                            on=["COBID", "PROCESS_TYPE"], how="inner")

_groups = list(df_grid.groupby(["COBID", "PROCESS_TYPE"], sort=False))
if not _groups:
    st.info("Nothing matches the filters.")
else:
    _th = (f'style="text-align:left;padding:7px 12px;font-size:0.7rem;'
           f'text-transform:uppercase;letter-spacing:.05em;'
           f'color:{P["grey_700"]};border-bottom:2px solid {P["border"]};'
           f'white-space:nowrap"')
    _td = (f'style="padding:7px 12px;font-size:0.82rem;'
           f'border-bottom:1px solid {P["border"]};vertical-align:middle"')
    _rows_html = []
    for (g_cob, g_scope), g_rows in _groups:
        eff, col, sub, sub_col, detail = _scope_summary(g_rows)
        _rows_html.append(
            f'<tr>'
            f'<td {_td}><strong>{int(g_cob)}</strong></td>'
            f'<td {_td}><strong>{g_scope}</strong></td>'
            f'<td {_td}>{_pill(eff, col)}</td>'
            f'<td {_td}><span style="color:{sub_col};font-weight:700;'
            f'font-size:0.78rem">{sub}</span></td>'
            f'<td {_td}>{_entity_chips(g_rows)}</td>'
            f'<td {_td}><span style="color:{P["grey_700"]};font-size:0.8rem">'
            f'{detail}</span></td>'
            f'</tr>')
    st.markdown(
        f'<div style="background:{P["white"]};border:1px solid {P["border"]};'
        f'border-radius:10px;overflow-x:auto">'
        f'<table style="width:100%;border-collapse:collapse">'
        f'<tr><th {_th}>COB</th><th {_th}>Scope</th><th {_th}>Sign-off</th>'
        f'<th {_th}>Submissions</th><th {_th}>Entities</th>'
        f'<th {_th}>Detail</th></tr>'
        + "".join(_rows_html)
        + '</table></div>',
        unsafe_allow_html=True)
    st.caption(f"{len(_groups)} COB/scope line(s) · "
               f"{len(df_grid)} underlying entit(y/ies)")
