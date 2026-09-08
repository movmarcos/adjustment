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
import streamlit as st
import pandas as pd

st.set_page_config(page_title="Sign-Off · MUFG", page_icon="🔒", layout="wide", initial_sidebar_state="expanded")

from utils.styles import (wide_kwargs, inject_css, render_sidebar, section_title, P,
                          ALL_SCOPES, icon, bordered_container, fmt_user_dt,
                          SCOPE_CONFIG, render_df_table,
                          set_flash, render_flash, confirm_gate,
                          SIGNOFF_STATUS_META, signoff_status_label)
from utils.snowflake_conn import run_query, run_query_df, current_user_name, safe_rerun

_FLASH_KEY = "signoff"


def _esc(val):
    """Escape a SQL string literal (backslashes first, then quotes)."""
    return str(val).replace("\\", "\\\\").replace("'", "''") if val is not None else ""


def _pill(text, color) -> str:
    return (f'<span style="background:{color}18;color:{color};border:1px solid {color}55;'
            f'border-radius:99px;padding:1px 10px;font-size:0.75rem;font-weight:700;'
            f'white-space:nowrap">{text}</span>')


def _nz(v) -> str:
    """NaN/None-safe plain text ('' for empty), whitespace-normalized — grid
    cells are plain values now, no markup."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    return " ".join(str(v).split())


# Status label -> colour, straight from the shared sign-off palette; used by
# both grids' color_cols so a status reads the same everywhere.
_SIGNOFF_LABEL_COLORS = {m["label"]: m["color"] for m in SIGNOFF_STATUS_META.values()}


# status → (label, color, blocks submissions?)
# Palette (Marcos): the day's GOAL is to close the COB — SIGNED OFF (done)
# is GREEN, OPEN/RE-OPENED (work outstanding) are RED, anything partial or
# awaiting a decision is ORANGE. Labels/colours come from the shared
# SIGNOFF_STATUS_META in utils.styles (its `blocks` flag = submissions are
# blocked in that state; the local set is only a fallback).
_BLOCKS_SUBMISSIONS = {"SIGNOFF_REQUESTED", "REOPEN_REQUESTED", "SIGNED_OFF"}
_STATUS_META = {
    code: (meta["label"], meta["color"],
           bool(meta.get("blocks", code in _BLOCKS_SUBMISSIONS)))
    for code, meta in SIGNOFF_STATUS_META.items()
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
    # Secondary on purpose (S-M4): "Sign off now" is the page's one primary.
    if st.button("⟳  Sync from upstream feed", key="signoff_sync_btn",
                 type="secondary", **wide_kwargs(),
                 help="Pulls every COB/scope/entity from the publish feed: "
                      "not signed off → OPEN, signed off → SIGNED_OFF. Also "
                      "runs automatically every 30 minutes."):
        try:
            res = run_query("CALL ADJUSTMENT_APP.SP_SYNC_SIGNOFF_STATUS()")
            set_flash(_FLASH_KEY, "success",
                      _sync_summary(res[0][0] if res else "no result"))
        except Exception as ex:
            set_flash(_FLASH_KEY, "warning",
                      f"Sync failed. The database reported: {ex}")
        safe_rerun()

render_flash(_FLASH_KEY)

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

# Prominent business-date picker — a dropdown. Everything below covers the
# selected date.
st.markdown(
    f'<div style="font-size:0.75rem;font-weight:700;text-transform:uppercase;'
    f'letter-spacing:.07em;color:{P["grey_700"]};margin-bottom:1px">'
    f'{icon("calendar", size=13)} &nbsp;Business date (COB) — scope status and '
    f'actions cover the selected COB</div>', unsafe_allow_html=True)

_cb1, _cb2 = st.columns([1, 3])
with _cb1:
    sel_cob = st.selectbox("COB", _all_cobs, index=0, key="so_cob",
                           format_func=lambda v: str(v),
                           label_visibility="collapsed")

df_cob = df_all[df_all["COBID"].astype(int) == int(sel_cob)]

# S-H1: the COB multiselects further down (status grid, Latest Changes) must
# FOLLOW this picker. A widget only takes its default when first created, so
# push the new COB into their session state BEFORE they are instantiated,
# every time the picker changes. The user can still add/remove COBs in them
# afterwards — that selection survives until the picker moves again.
_FOLLOW_COB_KEYS = ("so_f_cob", "so_h_cob")
if st.session_state.get("_so_last_cob") != int(sel_cob):
    for _fk in _FOLLOW_COB_KEYS:
        st.session_state[_fk] = [int(sel_cob)]
    st.session_state["_so_last_cob"] = int(sel_cob)

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
    + (_chip(f'{_n_pending} awaiting approval', P["warning"]) if _n_pending else "")
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
        eff, col = f"{_verb.upper()} PENDING", P["warning"]
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
        eff, col = "PARTIALLY SIGNED OFF", P["warning"]
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
                f'<div style="font-size:0.75rem;color:{P["grey_700"]}">{_ent_txt}'
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
        set_flash(_FLASH_KEY, "success", msg)
    else:
        set_flash(_FLASH_KEY, "warning",
                  f"{verb} was NOT applied — {out.get('message', 'no detail')}")
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
                # S-H2: an immediate sign-off is irreversible without an
                # approver, so it needs an explicit confirmation. The gate is
                # keyed per target so a tick never carries over to a different
                # scope/entity picked afterwards.
                _conf_key = None
                _conf_s = True
                if _sel_s and not _appr_s:
                    _conf_key = "so_signoff_confirm_" + "_".join(
                        str(p or "") for p in _sel_s)
                    _conf_s = confirm_gate(
                        f"I confirm all adjustments for {_row_label(_sel_s)} "
                        f"on COB {sel_cob} are complete — submissions will be "
                        f"blocked immediately",
                        key=_conf_key)
                _can_s = bool(_sel_s and _rsn_s.strip() and _conf_s)
                if st.button(("Request sign-off" if _appr_s else "Sign off now"),
                             key="so_signoff_btn", type="primary",
                             **wide_kwargs(), disabled=not _can_s):
                    _request(_sel_s[0], _sel_s[1], _sel_s[2], "SIGNOFF",
                             "Sign-off", _rsn_s, _appr_s,
                             reset_keys=("so_signoff_target", "so_signoff_reason")
                             + ((_conf_key,) if _conf_key else ()))
                if not _can_s:
                    st.caption("Choose a scope/entity and enter a reason"
                               + (" — then tick the confirmation"
                                  if (_sel_s and not _appr_s) else "")
                               + " to enable")

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
                _can_r = bool(_sel_r and _rsn_r.strip())
                if st.button("Request re-open", key="so_reopen_btn",
                             **wide_kwargs(), disabled=not _can_r):
                    _request(_sel_r[0], _sel_r[1], _sel_r[2], "REOPEN",
                             "Re-open", _rsn_r, True,
                             reset_keys=("so_reopen_target", "so_reopen_reason"))
                if not _can_r:
                    st.caption("Choose a scope/entity and enter a reason to enable")

    st.markdown("<br/>", unsafe_allow_html=True)


    # ══════════════════════════════════════════════════════════════════════════════
    # STATUS GRID — filterable; defaults to the selected (latest) COB.
    # Same tab as the actions (Marcos): sign off / re-open and the resulting
    # status live together; only the audit feed is a separate tab.
    # ══════════════════════════════════════════════════════════════════════════════

    st.markdown("<br/>", unsafe_allow_html=True)
    section_title("Sign-Off Status", "table")
    st.caption("One line per COB, scope and entity ('*' = the whole scope). "
               "Follows the selected COB; add more COBs here to compare.")
    g1, g2, g3 = st.columns(3)
    with g1:
        # No `default=` — the value is seeded in session state by the
        # follow-the-picker block under the COB selectbox (S-H1).
        f_cobs = st.multiselect("COB", _all_cobs,
                                key="so_f_cob", format_func=lambda v: str(v),
                                help="Follows the selected COB; add more COBs "
                                     "here to compare. Empty = all COBs.")
    with g2:
        f_scopes = st.multiselect("Scope",
                                  sorted(df_all["PROCESS_TYPE"].unique().tolist()),
                                  default=[], key="so_f_scope")
    with g3:
        f_status = st.multiselect(
            "Status", list(_STATUS_META.keys()), default=[], key="so_f_status",
            format_func=lambda v: signoff_status_label(v).title(),
            help="Show only entities in one of the selected statuses.")

    df_grid = df_all
    if f_cobs:
        df_grid = df_grid[df_grid["COBID"].astype(int).isin(f_cobs)]
    if f_scopes:
        df_grid = df_grid[df_grid["PROCESS_TYPE"].isin(f_scopes)]
    if f_status:
        df_grid = df_grid[df_grid["_SU"].isin(f_status)]

    if df_grid.empty:
        st.info("Nothing matches the filters.")
    else:
        def _entity_comment(r):
            """The human context for an entity line: who asked for what (and
            why) while a request is pending or after a re-open; blank when
            the state came straight from the upstream feed."""
            su = str(r["_SU"])
            reason = _nz(r.get("REOPEN_REASON"))
            if su in ("SIGNOFF_REQUESTED", "REOPEN_REQUESTED"):
                verb = "Sign-off" if su == "SIGNOFF_REQUESTED" else "Re-open"
                head = (f"{verb} requested by {_nz(r.get('REOPEN_REQUESTED_BY')) or '—'} "
                        f"{_fmt_ts(r.get('REOPEN_REQUESTED_AT'))} — awaiting approval")
                return f"{head}: {reason}" if reason else head
            if su == "REOPENED":
                head = f"Re-opened (approved by {_nz(r.get('REOPEN_APPROVED_BY')) or '—'})"
                return f"{head}: {reason}" if reason else head
            return ""

        _rows = []
        for _, r in df_grid.iterrows():
            _su = str(r["_SU"])
            _blocks = _STATUS_META.get(_su, ("", "", False))[2]
            _rows.append({
                "COB": str(int(r["COBID"])),
                "Scope": _nz(r.get("PROCESS_TYPE")) or "—",
                "Entity": _nz(r.get("_ENT_LBL")) or "*",
                "Status": signoff_status_label(_su),
                "Submissions": "Blocked" if _blocks else "Allowed",
                "Signed off by": (_nz(r.get("SIGN_OFF_BY")) or "—"
                                  if _su == "SIGNED_OFF" else "—"),
                "When": (fmt_user_dt(r.get("SIGN_OFF_TIMESTAMP"), "%d %b %Y %H:%M")
                         if _su == "SIGNED_OFF" else ""),
                "Source": _nz(r.get("SIGNOFF_SOURCE")) or "—",
                "Comment": _entity_comment(r),
            })
        render_df_table(
            pd.DataFrame(_rows), max_rows=len(_rows), key="so_status",
            color_cols={
                "Status": _SIGNOFF_LABEL_COLORS,
                "Scope": lambda v: SCOPE_CONFIG.get(str(v), {}).get("color", P["grey_700"]),
            },
            wrap_cols={"Comment": 420})
        st.caption(f"{len(_rows)} entity line(s) across "
                   f"{df_grid.groupby(['COBID', 'PROCESS_TYPE']).ngroups} "
                   f"COB/scope(s)")


with tab_hist:
    # ══════════════════════════════════════════════════════════════════════════════
    # LATEST CHANGES — sign-off lifecycle events, newest first, one row per
    # event (same data as the Logs page's Sign-Off tab).
    # ══════════════════════════════════════════════════════════════════════════════
    st.markdown("<br/>", unsafe_allow_html=True)
    section_title("Latest Changes", "file-text")
    st.caption("Sign-offs, re-opens and pending requests — newest first, with who "
               "and why. Times in your selected timezone. Follows the selected "
               "COB; add more COBs to compare, or clear the filter to see all "
               "(last 300 events across every COB).")

    df_hist = df_hist_cached
    if not df_hist.empty:
        df_hist = df_hist.copy()
        df_hist["_COB_N"] = pd.to_numeric(df_hist["COBID"], errors="coerce")
    _hist_cobs = sorted(
        set(_all_cobs) | {int(v) for v in
                          (df_hist["_COB_N"].dropna().unique().tolist()
                           if not df_hist.empty else [])},
        reverse=True)
    # S-M3: same follow-the-picker seeding as the status grid (see S-H1).
    _h1, _ = st.columns([1, 2])
    with _h1:
        h_cobs = st.multiselect("COB", _hist_cobs, key="so_h_cob",
                                format_func=lambda v: str(v),
                                help="Follows the selected COB; add more COBs "
                                     "to compare. Empty = all COBs.")
    if h_cobs and not df_hist.empty:
        df_hist = df_hist[df_hist["_COB_N"].isin([int(v) for v in h_cobs])]

    if df_hist_cached.empty:
        st.caption("No sign-off activity recorded yet.")
    elif df_hist.empty:
        st.info("No sign-off activity for the selected COB(s) in the last 300 "
                "events — clear the COB filter to see everything.")
    else:
        def _cob(v):
            try:
                return str(int(v))
            except (TypeError, ValueError):
                return _nz(v) or "—"

        _rows = []
        for _, h in df_hist.iterrows():
            sub = _nz(h.get("SUB_TYPE"))
            _old = _nz(h.get("OLD_STATUS"))
            _rows.append({
                "When": fmt_user_dt(h.get("ACTION_AT"), "%d %b %Y %H:%M:%S"),
                "Event": signoff_status_label(h.get("NEW_STATUS")),
                "COB": _cob(h.get("COBID")),
                "Scope": _nz(h.get("PROCESS_TYPE")) or "—",
                "Entity": (_nz(h.get("ENTITY_CODE")) or "*") + (f" / {sub}" if sub else ""),
                "From": signoff_status_label(_old) if _old else "—",
                "By": _nz(h.get("ACTION_BY")) or "—",
                # Full comment — no truncation; the grid wraps it.
                "Comment": _nz(h.get("COMMENT")),
            })
        render_df_table(
            pd.DataFrame(_rows), max_rows=len(_rows), key="so_hist",
            color_cols={
                "Event": _SIGNOFF_LABEL_COLORS, "From": _SIGNOFF_LABEL_COLORS,
                "Scope": lambda v: SCOPE_CONFIG.get(str(v), {}).get("color", P["grey_700"]),
            },
            wrap_cols={"Comment": 420})
        st.caption(f"{len(_rows)} event(s), newest first.")
