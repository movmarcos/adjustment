"""
COB Cockpit — one screen to run the day
=======================================
Everything about closing a COB, per scope, on a single page:

  • per-scope status counts and a "ready to close" verdict
  • the sign-off lifecycle with inline actions (request re-open when signed
    off, sign off again when re-opened)
  • report freshness (PowerBI refresh state of the scope's adjustments)
  • the open items that still need a human (failed, awaiting approval,
    re-open requests)

Reads: ADJ_HEADER, VW_SIGNOFF_STATUS, VW_REPORT_REFRESH_STATUS.
Writes: sign-off lifecycle transitions (same rules as the New Adjustment
page), each audited in ADJ_SIGNOFF_HISTORY.
"""
import streamlit as st
import pandas as pd
import json
import html as _htmlmod

st.set_page_config(
    page_title="COB Cockpit · MUFG", page_icon="🎛️",
    layout="wide", initial_sidebar_state="expanded",
)

from utils.styles import (
    inject_css, render_sidebar, section_title, fmt_adj_id,
    P, SCOPE_CONFIG, ALL_SCOPES, STATUS_COLORS, icon, bordered_container,
)
from utils.snowflake_conn import (run_query, run_query_df, current_user_name,
                                  safe_rerun, friendly_error)

inject_css()
render_sidebar()

user = current_user_name()


def _esc(v):
    return str(v).replace("\\", "\\\\").replace("'", "''") if v is not None else ""


def _pill(text, color) -> str:
    return (f'<span style="background:{color}18;color:{color};border:1px solid {color}55;'
            f'border-radius:99px;padding:1px 10px;font-size:0.74rem;font-weight:700;'
            f'white-space:nowrap">{text}</span>')


# ── Header + COB selection ────────────────────────────────────────────────────

_hdr, _pick, _btn = st.columns([3.2, 1.2, 0.8])
with _hdr:
    st.markdown("## COB Cockpit")
    st.markdown(
        f"<span style='color:{P['grey_700']};font-size:0.9rem'>"
        "The close-of-business run sheet: adjustment status, sign-off and "
        "report freshness for every scope of one COB.</span>",
        unsafe_allow_html=True)
with _pick:
    try:
        _cobs = run_query("""
            SELECT DISTINCT COBID FROM ADJUSTMENT_APP.ADJ_HEADER
            WHERE IS_DELETED = FALSE
            UNION
            SELECT DISTINCT COBID FROM ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS
            ORDER BY COBID DESC LIMIT 30
        """)
        cob_options = [int(r["COBID"]) for r in _cobs] if _cobs else []
    except Exception:
        cob_options = []
    if not cob_options:
        st.info("No COBs with activity yet.")
        st.stop()
    cobid = st.selectbox("COB", cob_options, index=0, key="ck_cob",
                         format_func=lambda v: str(v))
with _btn:
    st.markdown("<br/>", unsafe_allow_html=True)
    if st.button("Refresh", type="primary", use_container_width=True):
        safe_rerun()

_flash = st.session_state.pop("ck_flash", None)
if _flash:
    (st.success if _flash[0] == "success" else st.warning)(_flash[1])

# ── Load the COB's data (three small queries) ────────────────────────────────

try:
    df_counts = run_query_df(f"""
        SELECT PROCESS_TYPE, RUN_STATUS, COUNT(*) AS N
        FROM ADJUSTMENT_APP.ADJ_HEADER
        WHERE COBID = {int(cobid)} AND IS_DELETED = FALSE
        GROUP BY PROCESS_TYPE, RUN_STATUS
    """)
except Exception as e:
    df_counts = pd.DataFrame(columns=["PROCESS_TYPE", "RUN_STATUS", "N"])
    st.warning(f"Could not load adjustment counts: {e}")

try:
    df_so = run_query_df(f"""
        SELECT PROCESS_TYPE, ENTITY_CODE, SUB_TYPE, SIGN_OFF_STATUS,
               SIGNOFF_SOURCE,
               SIGN_OFF_BY, REOPEN_REQUESTED_BY, REOPEN_REASON, REOPEN_APPROVED_BY
        FROM ADJUSTMENT_APP.VW_SIGNOFF_STATUS
        WHERE COBID = {int(cobid)}
    """)
except Exception:
    df_so = pd.DataFrame(columns=["PROCESS_TYPE", "SIGN_OFF_STATUS"])

try:
    df_rep = run_query_df(f"""
        SELECT PROCESS_TYPE, REPORT_STATUS, COUNT(*) AS N
        FROM ADJUSTMENT_APP.VW_REPORT_REFRESH_STATUS
        WHERE COBID = {int(cobid)}
        GROUP BY PROCESS_TYPE, REPORT_STATUS
    """)
except Exception:
    df_rep = pd.DataFrame(columns=["PROCESS_TYPE", "REPORT_STATUS", "N"])


def _n(scope, statuses):
    if df_counts.empty:
        return 0
    m = (df_counts["PROCESS_TYPE"].str.upper() == scope.upper()) & \
        (df_counts["RUN_STATUS"].isin(statuses))
    return int(df_counts[m]["N"].sum())


def _rep_n(scope, statuses):
    if df_rep.empty:
        return 0
    m = (df_rep["PROCESS_TYPE"].str.upper() == scope.upper()) & \
        (df_rep["REPORT_STATUS"].isin(statuses))
    return int(df_rep[m]["N"].sum())


def _so_rows(scope):
    """All sign-off lifecycle rows for the scope at this COB (per entity;
    ENTITY_CODE '*' = whole scope)."""
    if df_so.empty:
        return []
    m = df_so[df_so["PROCESS_TYPE"].str.upper() == scope.upper()]
    return [r.to_dict() for _, r in m.iterrows()]


OUTSTANDING = ["Pending", "Pending Approval", "Approved", "Running"]

# ── COB-level KPI row ────────────────────────────────────────────────────────

_tot   = int(df_counts["N"].sum()) if not df_counts.empty else 0
_out   = sum(_n(s, OUTSTANDING) for s in ALL_SCOPES)
_fail  = sum(_n(s, ["Failed"]) for s in ALL_SCOPES)
_appr  = sum(_n(s, ["Pending Approval"]) for s in ALL_SCOPES)
# Count matches the label: pending requests of BOTH kinds for THIS COB
# (each row = one request; sub-typed entries are distinct requests).
_reo   = int(df_so["SIGN_OFF_STATUS"].str.upper()
             .isin(["REOPEN_REQUESTED", "SIGNOFF_REQUESTED"]).sum()) \
         if not df_so.empty else 0

kcols = st.columns(5)
for col, (label, val, color, icon_name) in zip(kcols, [
    ("Adjustments",       _tot,  P["primary"], "file-text"),
    ("Outstanding",       _out,  P["warning"] if _out else P["success"], "clock"),
    ("Awaiting approval", _appr, P["info"] if _appr else P["success"], "clipboard"),
    ("Failed",            _fail, P["danger"] if _fail else P["success"], "x-circle"),
    ("Pending requests",  _reo,  "#B45309" if _reo else P["success"], "unlock"),
]):
    col.markdown(
        f'<div style="background:{P["white"]};border:1px solid {P["border"]};'
        f'border-top:3px solid {color};border-radius:8px;padding:0.75rem;text-align:center">'
        f'<div style="font-size:1.5rem;font-weight:800;color:{color};'
        f'font-variant-numeric:tabular-nums">{icon(icon_name, size=14, color=color)} {val}</div>'
        f'<div style="font-size:0.7rem;text-transform:uppercase;letter-spacing:.06em;'
        f'color:{P["grey_700"]};margin-top:2px">{label}</div></div>',
        unsafe_allow_html=True)

st.markdown("<br/>", unsafe_allow_html=True)

# ── Sign-off actions (same rules as the New Adjustment page) ─────────────────

def _signoff_request(scope, entity, sub, action, reason, requires_approval):
    """One path for both lifecycle changes: SP_REQUEST_SIGNOFF_CHANGE enforces
    the guarded transition + history. Sign-off applies DIRECTLY by default
    (approval optional — Marcos); re-open always goes through approval."""
    _appr = "TRUE" if requires_approval else "FALSE"
    res = run_query(
        f"CALL ADJUSTMENT_APP.SP_REQUEST_SIGNOFF_CHANGE("
        f"{int(cobid)}, '{_esc(scope)}', '{_esc(entity or '*')}', "
        f"'{_esc(sub or '')}', "
        f"'{action}', '{_esc(str(reason or '')[:490])}', {_appr}, "
        f"'{_esc(current_user_name())}')")
    try:
        out = json.loads(str(res[0][0])) if res else {}
    except (ValueError, TypeError, IndexError):
        out = {}
    if out.get("status") != "ok":
        return False, out.get("message",
                              "The request was not accepted — refresh and retry.")
    verb = "Sign-off" if action == "SIGNOFF" else "Re-open"
    if action == "REOPEN":
        try:
            _np = json.dumps({"process_type": scope, "cobid": int(cobid),
                              "requested_by": current_user_name(),
                              "reason": (f"[entity {entity or '*'}] "
                                         + str(reason)[:280])}).replace("\\", "\\\\").replace("'", "''")
            run_query(f"CALL ADJUSTMENT_APP.SP_NOTIFY('reopen_requested', '{_np}')")
        except Exception:
            pass
    _etx = "all entities" if (entity or "*") == "*" else entity
    if out.get("pending_approval"):
        return True, (f"{verb} request for COB {cobid} / {scope} ({_etx}) "
                      f"submitted — an approver decides it on the Approval "
                      f"Queue page. Submissions stay blocked while it is "
                      f"pending.")
    return True, (f"COB {cobid} / {scope} ({_etx}) signed off — new "
                  f"adjustment submissions are blocked again.")


def _request_reopen(scope, entity, reason, sub=None):
    return _signoff_request(scope, entity, sub, "REOPEN", reason, True)


def _resign_off(scope, entity, sub=None):
    return _signoff_request(scope, entity, sub, "SIGNOFF",
                            "Sign-off after re-open cycle (Cockpit)", False)


# ── Per-scope run sheet ──────────────────────────────────────────────────────

section_title("Scope Run Sheet", "table")
st.caption("A scope is **ready to close** when nothing is outstanding and "
           "nothing has failed. Sign-off states sync from the upstream publish "
           "feed every 30 minutes; submissions are blocked while signed off "
           "or while a re-open request is pending.")

for scope in ALL_SCOPES:
    queued    = _n(scope, ["Pending", "Approved"])
    awaiting  = _n(scope, ["Pending Approval"])
    running   = _n(scope, ["Running"])
    failed    = _n(scope, ["Failed"])
    processed = _n(scope, ["Processed"])
    total     = queued + awaiting + running + failed + processed
    # "In reports": PBI refresh done (VaR/Stress) or dbt rebuild trigger
    # written (Sensitivity/FRTB — Control-M runs the rebuild from there).
    ready     = _rep_n(scope, ["Reports Ready", "Rebuild Triggered"])

    so_rows = _so_rows(scope)
    _wc = next((r for r in so_rows
                if str(r.get("ENTITY_CODE") or "*") == "*"), None)
    _blocked_so = [r for r in so_rows
                   if str(r.get("SIGN_OFF_STATUS", "")).upper()
                   in ("SIGNED_OFF", "REOPEN_REQUESTED", "SIGNOFF_REQUESTED")]

    # Verdict chip
    outstanding = queued + awaiting + running
    if failed:
        verdict = _pill("Attention — failures", P["danger"])
    elif outstanding:
        verdict = _pill("In progress", P["warning"])
    elif (_wc is not None
          and str(_wc.get("SIGN_OFF_STATUS", "")).upper() == "SIGNED_OFF"):
        verdict = _pill("Closed (signed off)", P["grey_700"])
    else:
        verdict = _pill("Ready to close", P["success"])

    if not so_rows:
        so_pill = _pill("OPEN", P["danger"])
    elif len(so_rows) == 1 and _wc is not None:
        so_pill = {
            # Palette (Marcos): closed = green, open = red, partial/pending
            # = orange — the goal of the day is to close the COB.
            "SIGNED_OFF":        _pill("SIGNED OFF", P["success"]),
            "REOPEN_REQUESTED":  _pill("RE-OPEN REQUESTED", "#B45309"),
            "SIGNOFF_REQUESTED": _pill("SIGN-OFF REQUESTED", "#B45309"),
            "REOPENED":          _pill("RE-OPENED", P["danger"]),
        }.get(str(_wc.get("SIGN_OFF_STATUS", "")).upper(),
              _pill("OPEN", P["danger"]))
    else:
        _n_closed = len(_blocked_so)
        so_pill = _pill(
            f"{_n_closed}/{len(so_rows)} entities signed off",
            P["success"] if _n_closed == len(so_rows)
            else ("#B45309" if _n_closed else P["danger"]))

    scfg = SCOPE_CONFIG.get(scope, {})
    with bordered_container():
        h1, h2 = st.columns([2.6, 1.4])
        with h1:
            st.markdown(
                f'<div style="font-size:1rem;font-weight:800">'
                f'{icon(scfg.get("icon", "bar-chart"), size=15, color=scfg.get("color", P["primary"]))} '
                f'{scope} &nbsp;{verdict}&nbsp;{so_pill}</div>',
                unsafe_allow_html=True)
        with h2:
            st.markdown(
                f'<div style="text-align:right;font-size:0.76rem;color:{P["grey_700"]}">'
                f'{total} adjustment(s) · {ready}/{processed} in reports</div>',
                unsafe_allow_html=True)

        if total == 0 and not so_rows:
            st.caption("No adjustments and no sign-off entry for this COB.")
        else:
            c = st.columns(5)
            for col, (label, val, color) in zip(c, [
                ("Queued",            queued,    P["warning"] if queued else P["grey_400"]),
                ("Awaiting approval", awaiting,  P["info"] if awaiting else P["grey_400"]),
                ("Running",           running,   P["info"] if running else P["grey_400"]),
                ("Failed",            failed,    P["danger"] if failed else P["grey_400"]),
                ("Processed",         processed, P["success"] if processed else P["grey_400"]),
            ]):
                col.markdown(
                    f'<div style="text-align:center">'
                    f'<div style="font-size:1.15rem;font-weight:800;color:{color};'
                    f'font-variant-numeric:tabular-nums">{val}</div>'
                    f'<div style="font-size:0.68rem;text-transform:uppercase;'
                    f'letter-spacing:.05em;color:{P["grey_700"]}">{label}</div></div>',
                    unsafe_allow_html=True)

        # ── Sign-off lifecycle, one row per entity ('*' = whole scope) ───────
        for so in so_rows:
            so_status = str(so.get("SIGN_OFF_STATUS", "")).upper()
            so_ent    = str(so.get("ENTITY_CODE") or "*")
            _sub_raw  = so.get("SUB_TYPE")
            # NULL SUB_TYPE arrives as None OR pandas NaN — NaN is TRUTHY,
            # so a bare `or ""` would yield the literal string "nan".
            so_sub    = "" if (_sub_raw is None or pd.isna(_sub_raw)) \
                        else str(_sub_raw)
            ent_txt   = ("all entities" if so_ent == "*" else so_ent)                         + (f" / {so_sub}" if so_sub else "")
            _key      = f"{scope}_{so_ent}_{so_sub}"
            if so_status == "SIGNED_OFF":
                src = str(so.get("SIGNOFF_SOURCE") or "EXTERNAL")
                st.caption(f"**{ent_txt}** signed off by "
                           f"{so.get('SIGN_OFF_BY') or 'the upstream feed'} "
                           f"({src}). New adjustments are blocked for it.")
                rcol1, rcol2 = st.columns([3, 1])
                with rcol1:
                    reason = st.text_input(
                        "Reason for re-opening", key=f"ck_reason_{_key}",
                        placeholder=f"Why does {ent_txt} need re-opening?",
                        label_visibility="collapsed")
                with rcol2:
                    if st.button(f"Request re-open — {ent_txt}",
                                 key=f"ck_reopen_{_key}",
                                 use_container_width=True,
                                 disabled=not reason.strip()):
                        try:
                            ok, msg = _request_reopen(scope, so_ent,
                                                      reason.strip(),
                                                      so_sub or None)
                            st.session_state["ck_flash"] = (
                                "success" if ok else "warning", msg)
                        except Exception as ex:
                            st.session_state["ck_flash"] = (
                                "warning", friendly_error(ex))
                        safe_rerun()
            elif so_status == "SIGNOFF_REQUESTED":
                st.warning(f"{ent_txt}: sign-off requested by "
                           f"{so.get('REOPEN_REQUESTED_BY') or '—'} — awaiting "
                           f"approval on the Approval Queue page. Submissions "
                           f"are blocked while it is pending.")
            elif so_status == "REOPEN_REQUESTED":
                st.warning(f"{ent_txt}: re-open requested by "
                           f"{so.get('REOPEN_REQUESTED_BY') or '—'} — awaiting "
                           f"approval on the Approval Queue page. "
                           f"Reason: {so.get('REOPEN_REASON') or '—'}")
            elif so_status == "REOPENED":
                st.info(f"{ent_txt}: re-opened (approved by "
                        f"{so.get('REOPEN_APPROVED_BY') or '—'}) — submissions "
                        f"allowed. Sign off again once the entity is final.")
                _blockers = outstanding + failed
                sc1, sc2 = st.columns([3, 1])
                with sc1:
                    confirm = st.checkbox(
                        f"Confirm — {scope} / {ent_txt} for COB {cobid} is "
                        f"done; block new submissions again",
                        key=f"ck_resign_cf_{_key}", value=False)
                    if _blockers:
                        st.caption(f"⚠ {_blockers} adjustment(s) still "
                                   f"outstanding or failed on this scope.")
                with sc2:
                    if st.button(f"Sign off {ent_txt} again",
                                 key=f"ck_resign_{_key}",
                                 use_container_width=True,
                                 disabled=not confirm):
                        try:
                            ok, msg = _resign_off(scope, so_ent,
                                                  so_sub or None)
                            st.session_state["ck_flash"] = (
                                "success" if ok else "warning", msg)
                        except Exception as ex:
                            st.session_state["ck_flash"] = (
                                "warning", friendly_error(ex))
                        safe_rerun()


# ── Open items needing a human ───────────────────────────────────────────────

st.markdown("<br/>", unsafe_allow_html=True)
section_title("Open Items", "alert-triangle")

try:
    df_open = run_query_df(f"""
        SELECT DIMENSION_ADJ_ID, ADJ_ID, PROCESS_TYPE, ADJUSTMENT_TYPE,
               ENTITY_CODE, RUN_STATUS, USERNAME, CREATED_DATE, ERRORMESSAGE
        FROM ADJUSTMENT_APP.ADJ_HEADER
        WHERE COBID = {int(cobid)} AND IS_DELETED = FALSE
          AND RUN_STATUS IN ('Failed', 'Pending Approval')
        ORDER BY RUN_STATUS, CREATED_DATE
    """)
except Exception:
    df_open = pd.DataFrame()

if df_open.empty:
    st.caption("Nothing needs a human on this COB. 🎉")
else:
    rows_html = ""
    for _, r in df_open.iterrows():
        status = str(r["RUN_STATUS"])
        color  = STATUS_COLORS.get(status, P["grey_700"])
        action = ("Approve/reject it on the Approval Queue page"
                  if status == "Pending Approval"
                  else "Retry or delete it on the Adjustments page")
        err = str(r.get("ERRORMESSAGE") or "").strip()
        rows_html += (
            f'<tr>'
            f'<td style="padding:5px 10px;border-bottom:1px solid {P["border"]};'
            f'font-weight:700;white-space:nowrap">'
            f'{fmt_adj_id(r.get("DIMENSION_ADJ_ID")) if pd.notna(r.get("DIMENSION_ADJ_ID")) else "#" + str(r.get("ADJ_ID"))[:8] + "…"}</td>'
            f'<td style="padding:5px 10px;border-bottom:1px solid {P["border"]}">{_pill(status, color)}</td>'
            f'<td style="padding:5px 10px;border-bottom:1px solid {P["border"]}">'
            f'{_htmlmod.escape(str(r.get("PROCESS_TYPE") or ""))} · '
            f'{_htmlmod.escape(str(r.get("ADJUSTMENT_TYPE") or ""))} · '
            f'{_htmlmod.escape(str(r.get("ENTITY_CODE") or "—"))}</td>'
            f'<td style="padding:5px 10px;border-bottom:1px solid {P["border"]};'
            f'font-size:0.78rem;color:{P["grey_700"]}">'
            f'{_htmlmod.escape(str(r.get("USERNAME") or ""))}</td>'
            f'<td style="padding:5px 10px;border-bottom:1px solid {P["border"]};'
            f'font-size:0.78rem;color:{P["grey_700"]}">{action}'
            + (f'<br/><span style="font-family:monospace;font-size:0.72rem">'
               f'{_htmlmod.escape(err[:160])}</span>' if err else "")
            + '</td></tr>')
    st.markdown(
        f'<div style="overflow-x:auto;background:{P["white"]};'
        f'border:1px solid {P["border"]};border-radius:8px">'
        f'<table style="width:100%;border-collapse:collapse;font-size:0.82rem">'
        f'<tbody>{rows_html}</tbody></table></div>',
        unsafe_allow_html=True)
