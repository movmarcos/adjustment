"""
Sign-Off — COB Sign-Off Status & Lifecycle
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
import html as _hesc
import streamlit as st
import pandas as pd

st.set_page_config(page_title="Sign-Off · MUFG", page_icon="🔒", layout="wide", initial_sidebar_state="expanded")

from utils.styles import (inject_css, render_sidebar, section_title, P,
                          ALL_SCOPES, icon, bordered_container, fmt_user_dt,
                          SCOPE_CONFIG)
from utils.snowflake_conn import run_query, run_query_df, current_user_name, safe_rerun


def _esc(val):
    """Escape a SQL string literal (backslashes first, then quotes)."""
    return str(val).replace("\\", "\\\\").replace("'", "''") if val is not None else ""


def _pill(text, color) -> str:
    return (f'<span style="background:{color}18;color:{color};border:1px solid {color}55;'
            f'border-radius:99px;padding:1px 10px;font-size:0.74rem;font-weight:700;'
            f'white-space:nowrap">{text}</span>')


# status → (label, color, blocks submissions?)
# Palette (Marcos): the day's GOAL is to close the COB — SIGNED OFF (done)
# is GREEN, OPEN/RE-OPENED (work outstanding) are RED, anything partial or
# awaiting a decision is ORANGE.
_STATUS_META = {
    "OPEN":              ("OPEN",              P["danger"],  False),
    "REOPENED":          ("RE-OPENED",         P["danger"],  False),
    "SIGNOFF_REQUESTED": ("SIGN-OFF REQUESTED", "#B45309",   True),
    "REOPEN_REQUESTED":  ("RE-OPEN REQUESTED",  "#B45309",   True),
    "SIGNED_OFF":        ("SIGNED OFF",        P["success"], True),
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

_STATUS_SQL = """
    SELECT COBID, PROCESS_TYPE, ENTITY_CODE, SUB_TYPE,
           SIGN_OFF_STATUS, SIGNOFF_SOURCE,
           SIGN_OFF_BY, SIGN_OFF_TIMESTAMP,
           REOPEN_REQUESTED_BY, REOPEN_REQUESTED_AT, REOPEN_REASON,
           REOPEN_APPROVED_BY, REOPEN_APPROVED_AT, UPDATED_DATE
    FROM ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS
    ORDER BY COBID DESC, PROCESS_TYPE, ENTITY_CODE
"""

# EXACTLY the Logs page's sign-off query — plain, no join, no window
# function. (The previous join+QUALIFY version returned nothing in this
# environment while the Logs tab's plain query worked; same grid, same SQL.)
_HIST_SQL = """
    SELECT COBID, PROCESS_TYPE, COALESCE(ENTITY_CODE, '*') AS ENTITY_CODE,
           SUB_TYPE, OLD_STATUS, NEW_STATUS, ACTION_BY, ACTION_AT, COMMENT
    FROM ADJUSTMENT_APP.ADJ_SIGNOFF_HISTORY
    ORDER BY ACTION_AT DESC
    LIMIT 300
"""


# Plain direct queries, NO st.cache_data and NO async — the SiS runtime this
# app runs on supports neither reliably (st.cache_data failed at call time and
# the page fell through to "no data"; collect_nowait hung forever). This is
# the exact pattern every other page's grids use. Do not re-add either.
df_all, df_hist_cached = pd.DataFrame(), pd.DataFrame()
try:
    df_all = run_query_df(_STATUS_SQL)
except Exception as e:
    st.warning(f"Could not load sign-off status — the database reported: {e}")
try:
    df_hist_cached = run_query_df(_HIST_SQL)
except Exception as e:
    st.warning(f"Could not load sign-off history — the database reported: {e}")

if df_all.empty:
    st.info("No sign-off entries yet — press **Sync from upstream feed** "
            "(top right) to pull the open and signed-off COBs.")
    st.stop()

df_all["ENTITY_CODE"] = df_all["ENTITY_CODE"].fillna("*")
df_all["SUB_TYPE"] = df_all["SUB_TYPE"].fillna("")
df_all["_SU"] = df_all["SIGN_OFF_STATUS"].astype(str).str.upper()
# Display key: entity plus the sub-type when one exists (extra granularity)
df_all["_ENT_LBL"] = df_all["ENTITY_CODE"] + df_all["SUB_TYPE"].apply(
    lambda v: f" / {v}" if str(v) else "")
_all_cobs = sorted(df_all["COBID"].astype(int).unique().tolist(), reverse=True)

# ══════════════════════════════════════════════════════════════════════════════
# COCKPIT — the sign-off state of ONE COB, per scope, at a glance
# ══════════════════════════════════════════════════════════════════════════════

# Prominent business-date picker. A segmented radio makes the choice (and the
# available dates) evident at a glance; it degrades to a selectbox only when
# there are too many dates to lay out as a row.
st.markdown(
    f'<div style="font-size:0.75rem;font-weight:700;text-transform:uppercase;'
    f'letter-spacing:.07em;color:{P["grey_700"]};margin-bottom:1px">'
    f'{icon("calendar", size=13)} &nbsp;Business date (COB) — everything below '
    f'covers the selected date</div>', unsafe_allow_html=True)

_COB_RADIO_MAX = 15
if len(_all_cobs) <= _COB_RADIO_MAX:
    sel_cob = st.radio("COB", _all_cobs, index=0, horizontal=True,
                       format_func=lambda v: str(v), key="so_cob_radio",
                       label_visibility="collapsed")
else:
    sel_cob = st.selectbox("COB", _all_cobs, index=0, key="so_cob",
                           format_func=lambda v: str(v),
                           label_visibility="collapsed")

df_cob = df_all[df_all["COBID"].astype(int) == int(sel_cob)]

# Summary as colored chips right under the picker.
_n_blocked = int(df_cob["_SU"].isin(
    [s for s, (_, _, b) in _STATUS_META.items() if b]).sum())
_n_pending = int(df_cob["_SU"].isin(
    ["SIGNOFF_REQUESTED", "REOPEN_REQUESTED"]).sum())
_n_signed = int((df_cob["_SU"] == "SIGNED_OFF").sum())
_n_open = int(df_cob["_SU"].isin(["OPEN", "REOPENED"]).sum())


def _chip(txt, col):
    return (f'<span style="background:{col}14;color:{col};border:1px solid {col}44;'
            f'border-radius:99px;padding:2px 11px;font-size:0.76rem;font-weight:700;'
            f'white-space:nowrap">{txt}</span>')


st.markdown(
    '<div style="display:flex;gap:8px;flex-wrap:wrap;margin:6px 0 2px">'
    + _chip(f'{len(df_cob)} on COB {sel_cob}', P["grey_700"])
    + _chip(f'{_n_open} open', P["danger"])
    + _chip(f'{_n_signed} signed off', P["success"])
    + (_chip(f'{_n_pending} awaiting approval', "#B45309") if _n_pending else "")
    + '</div>', unsafe_allow_html=True)

def _fmt_ts(v):
    return fmt_user_dt(v, "%d %b %H:%M")


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
        eff, col = "SIGNED OFF", P["success"]
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
        eff, col = "RE-OPENED", P["danger"]
        _r = rows[rows["_SU"] == "REOPENED"].iloc[0]
        detail = (f"Re-opened (approved by "
                  f"{_r.get('REOPEN_APPROVED_BY') or '—'}) — sign off again "
                  f"when done")
    else:
        eff, col = "OPEN", P["danger"]
        detail = "Open per the upstream feed — adjustments allowed"

    n_block = int(blocked.sum())
    if n_block == len(rows):
        sub = "Blocked"
    elif n_block:
        sub = f"Blocked for {n_block}/{len(rows)} entities"
    else:
        sub = "Allowed"
    # Informational only — neutral colour so it never fights the status pill
    # (green now means CLOSED, not "you may submit").
    return eff, col, sub, P["grey_700"], detail


def _entity_chips(rows):
    """Compact per-entity chips; a lone '*' row means the whole scope."""
    if (len(rows) == 1 and str(rows.iloc[0]["ENTITY_CODE"]) == "*"
            and not str(rows.iloc[0]["SUB_TYPE"])):
        return f'<span style="font-size:0.75rem;color:{P["grey_700"]}">whole scope</span>'
    chips = []
    for _, r in rows.sort_values(["ENTITY_CODE", "SUB_TYPE"]).iterrows():
        lbl, col, _ = _STATUS_META.get(str(r["_SU"]),
                                       (str(r["_SU"]), P["grey_700"], False))
        chips.append(
            f'<span style="background:{col}14;color:{col};border:1px solid {col}44;'
            f'border-radius:99px;padding:0 8px;font-size:0.7rem;font-weight:700;'
            f'white-space:nowrap">{_hesc.escape(str(r["_ENT_LBL"]))}&nbsp;·&nbsp;{lbl}</span>')
    return '<span style="line-height:1.9">' + " ".join(chips) + "</span>"


section_title(f"Scope status — COB {sel_cob}", "lock")

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

# ══════════════════════════════════════════════════════════════════════════════
# ACTIONS — two single-purpose cards: SIGN OFF (left) and RE-OPEN (right).
# Never mixed in one area: each card lists only the rows it can act on.
# ══════════════════════════════════════════════════════════════════════════════

def _row_label(t):
    return f"{t[0]} · {t[1]}" + (f" / {t[2]}" if t[2] else "")


def _request(scope_, entity_, sub_, action, verb, reason, requires_approval,
             reset_keys=()):
    _appr = "TRUE" if requires_approval else "FALSE"
    res = run_query(
        f"CALL ADJUSTMENT_APP.SP_REQUEST_SIGNOFF_CHANGE("
        f"{int(sel_cob)}, '{_esc(scope_)}', '{_esc(entity_)}', "
        f"'{_esc(sub_)}', "
        f"'{action}', '{_esc(reason.strip()[:490])}', {_appr}, "
        f"'{_esc(user)}')")
    try:
        out = json.loads(str(res[0][0])) if res else {}
    except (ValueError, TypeError, IndexError):
        out = {}
    _lbl = f"{scope_} · {entity_}" + (f" / {sub_}" if sub_ else "")
    if out.get("status") == "ok":
        if out.get("pending_approval"):
            msg = (f"{verb} requested for COB {sel_cob} / {_lbl} — an "
                   f"approver decides it on the Approval Queue page. "
                   f"Submissions are blocked while it is pending.")
        elif action == "SIGNOFF":
            msg = (f"COB {sel_cob} / {_lbl} is now SIGNED OFF — new "
                   f"adjustment submissions are blocked.")
        else:
            msg = (f"COB {sel_cob} / {_lbl} is now RE-OPENED — new "
                   f"adjustment submissions are allowed.")
        st.session_state["so_flash"] = ("success", msg)
    else:
        st.session_state["so_flash"] = (
            "warning", f"{verb} was NOT applied — "
                       f"{out.get('message', 'no detail')}")
    # Clear the form so the selection is empty again after the rerun — the
    # user must explicitly pick the next thing to sign off / re-open.
    for _rk in reset_keys:
        st.session_state.pop(_rk, None)
    safe_rerun()


# ── Three working areas as TABS (Marcos 2026-09: everything stacked made
# the page crowded). The cockpit above stays always visible; actions, the
# cross-COB grid and the audit feed each get their own room.
tab_act, tab_hist = st.tabs(
    ["Sign Off & Status", "Latest Changes"])

with tab_act:
    # Pending requests: a read-only strip — decisions live on the Approval Queue.
    _pending_rows = df_cob[df_cob["_SU"].isin(["SIGNOFF_REQUESTED",
                                               "REOPEN_REQUESTED"])]
    if not _pending_rows.empty:
        _plist = " · ".join(
            f"{r['PROCESS_TYPE']} {r['_ENT_LBL']} "
            f"({'sign-off' if r['_SU'] == 'SIGNOFF_REQUESTED' else 're-open'} "
            f"by {r.get('REOPEN_REQUESTED_BY') or '—'})"
            for _, r in _pending_rows.iterrows())
        st.warning(f"**Awaiting approval on the Approval Queue page:** {_plist}")

    _act_l, _act_r = st.columns(2)

    with _act_l:
        with bordered_container():
            st.markdown(
                f'<div style="font-size:0.95rem;font-weight:700;display:flex;'
                f'align-items:center;gap:7px">{icon("check-circle", size=15)}'
                f' Sign Off</div>', unsafe_allow_html=True)
            st.caption("Close a scope for this COB — new adjustments are blocked "
                       "once signed off.")
            _elig_s = sorted({(r["PROCESS_TYPE"], r["ENTITY_CODE"], r["SUB_TYPE"])
                              for _, r in df_cob[df_cob["_SU"].isin(
                                  ["OPEN", "REOPENED"])].iterrows()})
            if not _elig_s:
                st.info("Nothing to sign off — everything on this COB is already "
                        "signed off or awaiting approval.")
            else:
                # Empty by default — the user must explicitly choose what to
                # sign off (None sentinel first, shown as a placeholder).
                _sel_s = st.selectbox(
                    "What to sign off", [None] + _elig_s,
                    key="so_signoff_target",
                    format_func=lambda t: "— choose a scope / entity —"
                    if t is None else _row_label(t))
                _rsn_s = st.text_input(
                    "Reason *", key="so_signoff_reason",
                    placeholder="e.g. all adjustments for this COB are done")
                # Sign-off approval is OPTIONAL: unchecked = applies immediately.
                _appr_s = st.checkbox(
                    "Request approval first (optional)", value=False,
                    key="so_signoff_appr")
                if st.button(("Request sign-off" if _appr_s else "Sign off now"),
                             key="so_signoff_btn", type="primary",
                             use_container_width=True,
                             disabled=not (_sel_s and _rsn_s.strip())):
                    _request(_sel_s[0], _sel_s[1], _sel_s[2], "SIGNOFF",
                             "Sign-off", _rsn_s, _appr_s,
                             reset_keys=("so_signoff_target", "so_signoff_reason"))

    with _act_r:
        with bordered_container():
            st.markdown(
                f'<div style="font-size:0.95rem;font-weight:700;display:flex;'
                f'align-items:center;gap:7px">{icon("unlock", size=15)}'
                f' Re-Open</div>', unsafe_allow_html=True)
            st.caption("Allow adjustments again on a signed-off scope — always "
                       "needs an approver (4-eyes).")
            _elig_r = sorted({(r["PROCESS_TYPE"], r["ENTITY_CODE"], r["SUB_TYPE"])
                              for _, r in df_cob[df_cob["_SU"] == "SIGNED_OFF"]
                              .iterrows()})
            if not _elig_r:
                st.info("Nothing to re-open — nothing on this COB is signed off.")
            else:
                # Empty by default — force an explicit choice of what to re-open.
                _sel_r = st.selectbox(
                    "What to re-open", [None] + _elig_r,
                    key="so_reopen_target",
                    format_func=lambda t: "— choose a scope / entity —"
                    if t is None else _row_label(t))
                _rsn_r = st.text_input(
                    "Reason *", key="so_reopen_reason",
                    placeholder="e.g. late booking needs an adjustment on this COB")
                # Re-open approval is REQUIRED by policy — ticked and locked.
                st.checkbox("Request approval (required by policy)", value=True,
                            disabled=True, key="so_reopen_appr")
                if st.button("Request re-open", key="so_reopen_btn",
                             use_container_width=True,
                             disabled=not (_sel_r and _rsn_r.strip())):
                    _request(_sel_r[0], _sel_r[1], _sel_r[2], "REOPEN",
                             "Re-open", _rsn_r, True,
                             reset_keys=("so_reopen_target", "so_reopen_reason"))

    st.markdown("<br/>", unsafe_allow_html=True)


    # ══════════════════════════════════════════════════════════════════════════════
    # STATUS GRID — filterable; defaults to the selected (latest) COB.
    # Same tab as the actions (Marcos): sign off / re-open and the resulting
    # status live together; only the audit feed is a separate tab.
    # ══════════════════════════════════════════════════════════════════════════════

    st.markdown("<br/>", unsafe_allow_html=True)
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
                f'<td {_td}><strong>{_hesc.escape(str(g_scope))}</strong></td>'
                f'<td {_td}>{_pill(eff, col)}</td>'
                f'<td {_td}><span style="color:{sub_col};font-weight:700;'
                f'font-size:0.78rem">{sub}</span></td>'
                f'<td {_td}>{_entity_chips(g_rows)}</td>'
                f'<td {_td}><span style="color:{P["grey_700"]};font-size:0.8rem">'
                f'{_hesc.escape(" ".join(str(detail).split())).replace("$", "&#36;")}</span></td>'
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


with tab_hist:
    # ══════════════════════════════════════════════════════════════════════════════
    # LATEST CHANGES — sign-off lifecycle events, newest first. SAME grid as the
    # Logs page's Sign-Off tab: day-grouped, pill-badged rows.
    # ══════════════════════════════════════════════════════════════════════════════
    st.markdown("<br/>", unsafe_allow_html=True)
    section_title("Latest Changes", "file-text")
    st.caption("Sign-offs, re-opens and pending requests — newest first, with who "
               "and why. Times in your selected timezone.")

    df_hist = df_hist_cached

    if df_hist.empty:
        st.caption("No sign-off activity recorded yet.")
    else:
        def _nz(v):
            """NaN/None-safe string ('' for empty), whitespace-normalized."""
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return ""
            return " ".join(str(v).split())

        def _cell(v):
            """User text → safe HTML: escaped + '$' neutralized (a $…$ pair
            would trigger Streamlit's LaTeX and eat the markup)."""
            return _hesc.escape(_nz(v)).replace("$", "&#36;")

        def _cob(v):
            try:
                return str(int(v))
            except (TypeError, ValueError):
                return _cell(v) or "—"

        _EVENT_META = {
            "SIGNED_OFF":        ("SIGNED OFF",         P["success"]),
            "REOPENED":          ("RE-OPENED",          P["danger"]),
            "OPEN":              ("OPEN",               P["danger"]),
            "SIGNOFF_REQUESTED": ("SIGN-OFF REQUESTED", "#B45309"),
            "REOPEN_REQUESTED":  ("RE-OPEN REQUESTED",  "#B45309"),
        }

        def _ev_pill(status):
            lbl, col = _EVENT_META.get(str(status).upper(),
                                       (str(status) or "—", P["grey_700"]))
            return _pill(lbl, col)

        def _scope_pill(scope):
            cfg = SCOPE_CONFIG.get(str(scope), {})
            return _pill(str(scope) or "—", cfg.get("color", P["grey_700"]))

        # ONE table, ONE st.markdown call. Day changes are divider ROWS inside
        # the single table rather than separate markdown blocks — many small
        # raw-HTML blocks rendered blank until scrolled (lazy height/reflow).
        # A fixed-height scroll container keeps the DOM a single predictable
        # element regardless of row count.
        _df = df_hist.copy()
        _df["_DAY"] = _df["ACTION_AT"].apply(lambda v: fmt_user_dt(v, "%A %d %b %Y"))

        # No position:sticky — a frozen header paints its white background
        # OVER the rows while the PAGE scrolls. Plain header, plain flow.
        _th = (f'style="text-align:left;padding:7px 10px;font-size:0.72rem;'
               f'text-transform:uppercase;letter-spacing:.05em;'
               f'color:{P["grey_700"]};border-bottom:2px solid {P["border"]};'
               f'white-space:nowrap"')
        _td = (f'padding:5px 10px;font-size:0.82rem;'
               f'border-bottom:1px solid {P["border"]};vertical-align:middle;'
               f'font-variant-numeric:tabular-nums')

        parts = []
        _cur_day = None
        for _, h in _df.iterrows():
            if h["_DAY"] != _cur_day:
                _cur_day = h["_DAY"]
                parts.append(
                    f'<tr><td colspan="8" style="padding:9px 10px 3px;'
                    f'font-size:0.72rem;font-weight:700;text-transform:uppercase;'
                    f'letter-spacing:.06em;color:{P["grey_700"]};'
                    f'background:{P["bg"]}">{_hesc.escape(str(_cur_day))}</td></tr>')
            sub = _nz(h.get("SUB_TYPE"))
            ent = (_cell(h.get("ENTITY_CODE")) or "*") + (
                f' <span style="color:{P["grey_700"]}">/ {_cell(sub)}</span>'
                if sub else "")
            _old = _nz(h.get("OLD_STATUS")).upper()
            frm = (_ev_pill(_old) if _old
                   else f'<span style="color:{P["grey_700"]}">—</span>')
            cells = [
                fmt_user_dt(h.get("ACTION_AT"), "%H:%M:%S"),
                _ev_pill(h.get("NEW_STATUS")),
                f'<strong>{_cob(h.get("COBID"))}</strong>',
                _scope_pill(h.get("PROCESS_TYPE")),
                ent, frm,
                _cell(h.get("ACTION_BY")) or "—",
                f'<span style="color:{P["grey_700"]}">'
                f'{_cell(_nz(h.get("COMMENT"))[:160])}</span>',
            ]
            parts.append(
                "<tr>" + "".join(f'<td style="{_td}">{c}</td>' for c in cells)
                + "</tr>")

        _headers = ["Time", "Event", "COB", "Scope", "Entity", "From", "By",
                    "Comment"]
        st.markdown(
            f'<div style="overflow-x:auto;background:{P["white"]};'
            f'border:1px solid {P["border"]};border-radius:8px">'
            f'<table style="width:100%;border-collapse:collapse">'
            f'<thead><tr>{"".join(f"<th {_th}>{h}</th>" for h in _headers)}'
            f'</tr></thead><tbody>{"".join(parts)}</tbody></table></div>',
            unsafe_allow_html=True)
        st.caption(f"{len(_df)} event(s), newest first.")
