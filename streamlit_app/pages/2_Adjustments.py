"""
Adjustments — browse & manage
=============================
All adjustments, with full history and actions. Defaults to everyone's; tick
"Only my adjustments" to narrow to the current user.
Reads from: VW_MY_WORK, ADJ_STATUS_HISTORY.
"""
import streamlit as st
import pandas as pd

st.set_page_config(page_title="Adjustments · MUFG", page_icon="📋", layout="wide", initial_sidebar_state="expanded")

from utils.styles import (
    inject_css, render_sidebar, render_filter_chips, render_status_timeline,
    render_lifecycle_bar,
    status_badge, section_title, P, SCOPE_CONFIG, ALL_SCOPES, STATUS_COLORS, STATUS_ICONS,
    fmt_adj_id, icon, render_activity_grid, SELECTION_UNSUPPORTED, bordered_container,
)
from utils.snowflake_conn import (run_query, run_query_df, current_user_name,
                                  safe_rerun, friendly_error)

inject_css()
render_sidebar()

user = current_user_name()

st.markdown("## Adjustments")
st.markdown(
    f"<span style='color:{P['grey_700']};font-size:0.9rem'>"
    f"All adjustments, with full history and available actions. "
    f"Tick <em>Only my adjustments</em> to see just yours.</span>",
    unsafe_allow_html=True)
st.markdown("<br/>", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# FILTERS
# ──────────────────────────────────────────────────────────────────────────────

# Distinct dimension values for the COB / Entity / Department / User filters below
# (reads the app's adjustment headers only — small, one lightweight query).
try:
    _fopts = run_query_df("""
        SELECT DISTINCT COBID, ENTITY_CODE, DEPARTMENT_CODE, SUBMITTED_BY
        FROM ADJUSTMENT_APP.VW_MY_WORK
    """)
except Exception:
    _fopts = pd.DataFrame(columns=["COBID", "ENTITY_CODE", "DEPARTMENT_CODE", "SUBMITTED_BY"])

def _distinct(col, reverse=False):
    if _fopts.empty or col not in _fopts.columns:
        return []
    return sorted(_fopts[col].dropna().unique().tolist(), reverse=reverse)

cob_opts    = [int(v) for v in _distinct("COBID", reverse=True)]
entity_opts = [str(v) for v in _distinct("ENTITY_CODE")]
dept_opts   = [str(v) for v in _distinct("DEPARTMENT_CODE")]
user_opts   = [str(v) for v in _distinct("SUBMITTED_BY")]

# ── Deep link from the Home KPI cards: ?status=Running / Pending,Approved ──
# Applied once per distinct param value, BEFORE the widget is instantiated,
# so the user can still clear/change the filter afterwards.
def _qp_status():
    try:
        if hasattr(st, "query_params"):                       # Streamlit ≥1.30
            raw = st.query_params.get("status")
            return raw if isinstance(raw, str) or raw is None else (raw[0] if raw else None)
        vals = st.experimental_get_query_params().get("status")   # 1.26
        return vals[0] if vals else None
    except Exception:
        return None

_status_param = _qp_status()
if _status_param and st.session_state.get("_applied_status_param") != _status_param:
    _wanted = [s.strip() for s in _status_param.split(",")
               if s.strip() in STATUS_COLORS]
    if _wanted:
        st.session_state["mw_status"] = _wanted
    st.session_state["_applied_status_param"] = _status_param

# Clear-filters: the button (below) sets a flag + reruns; the reset must
# happen HERE, before the filter widgets are instantiated on this run.
_FILTER_WIDGET_KEYS = ["mw_status", "mw_scope", "mw_type", "mw_cob",
                       "mw_entity", "mw_dept", "mw_user"]
if st.session_state.pop("_adj_clear_filters", False):
    for _fk in _FILTER_WIDGET_KEYS:
        st.session_state[_fk] = []
    st.session_state["mw_mine"] = False
    st.session_state["mw_deleted"] = False

with bordered_container():
    section_title("Filters", "search")
    f1, f2, f3, f4 = st.columns(4)
    with f1:
        filter_status = st.multiselect(
            "Status",
            list(STATUS_COLORS.keys()),
            default=[], key="mw_status")
    with f2:
        filter_scope = st.multiselect(
            "Scope", ALL_SCOPES,
            default=[], key="mw_scope")
    with f3:
        # Option values are the raw ADJUSTMENT_TYPE codes (used directly in the SQL
        # filter); the label maps the cryptic "EROL" code to "Entity Roll".
        _type_labels = {"Flatten": "Flatten", "Scale": "Scale", "Roll": "Roll",
                        "Direct": "Direct Adjustment", "Upload": "VaR Upload",
                        "EROL": "Entity Roll"}
        filter_type = st.multiselect(
            "Type", list(_type_labels.keys()),
            default=[], key="mw_type",
            format_func=lambda v: _type_labels.get(v, v))
    with f4:
        mine_only = st.checkbox("Only my adjustments", value=False, key="mw_mine",
                                help="When checked, shows only adjustments you submitted.")
        show_deleted = st.checkbox(
            "Show deleted", value=False, key="mw_deleted",
            help="Include deleted adjustments. Hidden by default.")

    f5, f6, f7, f8 = st.columns(4)
    with f5:
        filter_cob = st.multiselect("COB", cob_opts, default=[], key="mw_cob",
                                    format_func=lambda v: str(v))
    with f6:
        filter_entity = st.multiselect("Entity", entity_opts, default=[], key="mw_entity")
    with f7:
        filter_dept = st.multiselect("Department", dept_opts, default=[], key="mw_dept")
    with f8:
        filter_user = st.multiselect("User", user_opts, default=[], key="mw_user")

    _applied_n = sum(bool(st.session_state.get(k)) for k in _FILTER_WIDGET_KEYS) \
        + (1 if mine_only else 0) + (1 if show_deleted else 0)
    fc1, fc2 = st.columns([5, 1])
    with fc1:
        st.caption(f"{_applied_n} filter(s) applied." if _applied_n else
                   "No filters applied — showing the latest 200 adjustments.")
    with fc2:
        if st.button("Clear filters", key="adj_clear_btn", use_container_width=True,
                     disabled=not _applied_n):
            st.session_state["_adj_clear_filters"] = True
            safe_rerun()

st.markdown("<div style='height:0.6rem'></div>", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# LOAD DATA
# ──────────────────────────────────────────────────────────────────────────────

try:
    # Deleted rows are loaded too — they live in their own "Deleted" tab; the
    # active-status tabs filter them out below.
    where_clauses = ["1=1"]
    if mine_only:
        where_clauses.append(f"SUBMITTED_BY = '{user}'")
    if filter_status:
        in_list = ",".join(f"'{s}'" for s in filter_status)
        where_clauses.append(f"RUN_STATUS IN ({in_list})")
    if filter_scope:
        in_list = ",".join(f"'{s}'" for s in filter_scope)
        where_clauses.append(f"PROCESS_TYPE IN ({in_list})")
    if filter_type:
        in_list = ",".join(f"'{s}'" for s in filter_type)
        where_clauses.append(f"ADJUSTMENT_TYPE IN ({in_list})")
    if filter_cob:
        in_list = ",".join(str(int(c)) for c in filter_cob)
        where_clauses.append(f"COBID IN ({in_list})")
    if filter_entity:
        in_list = ",".join("'" + str(e).replace("'", "''") + "'" for e in filter_entity)
        where_clauses.append(f"ENTITY_CODE IN ({in_list})")
    if filter_dept:
        in_list = ",".join("'" + str(d).replace("'", "''") + "'" for d in filter_dept)
        where_clauses.append(f"DEPARTMENT_CODE IN ({in_list})")
    if filter_user:
        in_list = ",".join("'" + str(u).replace("'", "''") + "'" for u in filter_user)
        where_clauses.append(f"SUBMITTED_BY IN ({in_list})")

    where_sql = " AND ".join(where_clauses)
    df_adjs = run_query_df(f"""
        SELECT *
        FROM ADJUSTMENT_APP.VW_MY_WORK
        WHERE {where_sql}
        ORDER BY SUBMITTED_AT DESC
        LIMIT 200
    """)
except Exception as e:
    df_adjs = pd.DataFrame()
    st.warning(f"Could not load adjustments: {e}")

# Load lifecycle tracking data for all adjustments
df_track = pd.DataFrame()
try:
    df_track = run_query_df("""
        SELECT ADJ_ID, CURRENT_STAGE, REPORT_STATUS,
               SUBMITTED_AT, APPROVAL_REQUESTED_AT, APPROVED_AT,
               PROCESSING_STARTED_AT, PROCESSING_ENDED_AT,
               PBI_QUEUED_AT, PBI_STARTED_AT, PBI_COMPLETED_AT,
               PBI_REFRESH_DURATION_SEC, PBI_QUEUE_WAIT_SEC,
               REFRESH_PATH, DBT_TRIGGER_TIME,
               RUN_STATUS
    FROM ADJUSTMENT_APP.VW_ADJUSTMENT_TRACK
    """)
except Exception:
    pass

# Backwards compat alias
df_report_status = df_track


# ── Clone to another COB ──────────────────────────────────────────────────────

_CLONE_FIELDS = {
    "ENTITY_CODE": "entity_code", "SOURCE_SYSTEM_CODE": "source_system_code",
    "DEPARTMENT_CODE": "department_code", "BOOK_CODE": "book_code",
    "CURRENCY_CODE": "currency_code", "TRADE_TYPOLOGY": "trade_typology",
    "TRADE_CODE": "trade_code", "STRATEGY": "strategy",
    "TRADER_CODE": "trader_code", "VAR_COMPONENT_ID": "var_component_id",
    "VAR_SUB_COMPONENT_ID": "var_sub_component_id",
    "VAR_COMPONENT_NAME": "var_component_name",
    "VAR_SUB_COMPONENT_NAME": "var_sub_component_name",
    "GUARANTEED_ENTITY": "guaranteed_entity", "REGION_KEY": "region_key",
    "SCENARIO_DATE_ID": "scenario_date_id", "INSTRUMENT_CODE": "instrument_code",
    "SIMULATION_NAME": "simulation_name", "SIMULATION_SOURCE": "simulation_source",
    "TENOR_CODE": "tenor_code", "UNDERLYING_TENOR_CODE": "underlying_tenor_code",
    "CURVE_CODE": "curve_code", "MEASURE_TYPE_CODE": "measure_type_code",
    "DAY_TYPE": "day_type", "PRODUCT_CATEGORY_ATTRIBUTES": "product_category_attributes",
    "BATCH_REGION_AREA": "batch_region_area", "MUREX_FAMILY": "murex_family",
    "MUREX_GROUP": "murex_group", "ADJUSTMENT_VALUE_IN_USD": "adjustment_value_in_usd",
    "ADJUSTMENT_CATEGORY": "adjustment_category",
    "GLOBAL_REFERENCE": "global_reference", "FILE_NAME": "file_name",
}


def _clone_jsonable(v):
    """Snowpark Decimals/timestamps → JSON-friendly values."""
    if v is None or isinstance(v, (bool, int, float, str)):
        return v
    try:
        f = float(v)
        return int(f) if f.is_integer() else f
    except (TypeError, ValueError):
        return str(v)


def _do_clone(src_adj_id, new_cob) -> None:
    """Create a NEW adjustment at `new_cob` from an existing one's header
    (and, for VaR Upload / legacy Direct rows that carry line items, a copy
    of them). Goes through SP_SUBMIT_ADJUSTMENT like any submission —
    validation, sign-off check, overlap blocking and (if requested) approval
    all apply normally."""
    import json as _json
    import uuid as _uuid
    _sid = str(src_adj_id).replace("'", "''")
    copied_line_items_for = None
    try:
        src_rows = run_query(
            f"SELECT * FROM ADJUSTMENT_APP.ADJ_HEADER WHERE ADJ_ID = '{_sid}'")
        if not src_rows:
            st.session_state["adj_action_flash"] = (
                "warning", "Clone failed — the source adjustment was not found.")
            safe_rerun()
            return
        src = dict(src_rows[0].as_dict()) if hasattr(src_rows[0], "as_dict") \
            else dict(src_rows[0])

        src_cob  = int(src["COBID"])
        src_srcc = int(src["SOURCE_COBID"]) if src.get("SOURCE_COBID") is not None else src_cob
        adj_type = str(src.get("ADJUSTMENT_TYPE") or "")
        payload = {
            "cobid":           int(new_cob),
            "process_type":    str(src.get("PROCESS_TYPE")),
            "adjustment_type": adj_type,
            "username":        current_user_name(),
            # Same-COB operations follow the new COB; a cross-COB Roll/EROL
            # keeps its original source COB (edit the clone if that's wrong).
            "source_cobid":    int(new_cob) if src_srcc == src_cob else src_srcc,
            "scale_factor":    _clone_jsonable(src.get("SCALE_FACTOR")) or 1.0,
            "adjustment_occurrence": "ADHOC",
            "requires_approval": False,
            "reason": (f"Cloned from ADJ "
                       f"{fmt_adj_id(src.get('DIMENSION_ADJ_ID')) if src.get('DIMENSION_ADJ_ID') else '#' + _sid[:8]}"
                       f" (COB {src_cob}): {src.get('REASON') or ''}")[:990],
        }
        for col, key in _CLONE_FIELDS.items():
            val = _clone_jsonable(src.get(col))
            if val is not None and str(val).strip() != "":
                payload.setdefault(key, val)

        # VaR Upload (whole file = one entry): copy the line items under a
        # pre-generated ADJ_ID so header and payload rows share the same id
        # (mirrors the New Adjustment page's upload flow, including rollback
        # on rejection). Legacy pre-split "Direct" adjustments may also carry
        # line items (the old combined Direct/Upload flow used them); the
        # post-split "Direct" flow (each pasted row = its own adjustment)
        # never does — its value lives on the header (ADJUSTMENT_VALUE_IN_USD,
        # already copied via _CLONE_FIELDS above) — so a zero-row copy there
        # is expected, not an error.
        _adj_action = str(src.get("ADJUSTMENT_ACTION"))
        if _adj_action in ("Upload", "Direct"):
            new_id = str(_uuid.uuid4())
            rows = run_query(f"""
                INSERT INTO ADJUSTMENT_APP.ADJ_LINE_ITEM_JSON (ADJ_ID, ROW_NUM, PAYLOAD)
                SELECT '{new_id}', ROW_NUM, PAYLOAD
                FROM ADJUSTMENT_APP.ADJ_LINE_ITEM_JSON
                WHERE ADJ_ID = '{_sid}' AND IS_DELETED = FALSE
            """)
            _copied = int(rows[0][0]) if rows else 0
            if _copied == 0 and _adj_action == "Upload":
                st.session_state["adj_action_flash"] = (
                    "warning", "Clone failed — the source upload has no line "
                               "items to copy.")
                safe_rerun()
                return
            if _copied > 0:
                payload["adj_id"] = new_id
                copied_line_items_for = new_id

        json_str = _json.dumps(payload).replace("\\", "\\\\").replace("'", "''")
        res = run_query(f"CALL ADJUSTMENT_APP.SP_SUBMIT_ADJUSTMENT('{json_str}')")
        raw = res[0][0] if res else None
        out = _json.loads(str(raw)) if isinstance(raw, str) else (raw or {})
        status = out.get("status")
        if status in ("Pending", "Pending Approval", "Approved"):
            st.session_state["adj_action_flash"] = (
                "success",
                f"Cloned to COB {new_cob} — new adjustment created with "
                f"status '{status}'. {out.get('message', '')}")
        else:
            if copied_line_items_for:
                try:
                    run_query(f"DELETE FROM ADJUSTMENT_APP.ADJ_LINE_ITEM_JSON "
                              f"WHERE ADJ_ID = '{copied_line_items_for}'")
                except Exception:
                    pass
            st.session_state["adj_action_flash"] = (
                "warning",
                f"Clone was not accepted — {out.get('message', status or 'no response')}")
    except Exception as ex:
        if copied_line_items_for:
            try:
                run_query(f"DELETE FROM ADJUSTMENT_APP.ADJ_LINE_ITEM_JSON "
                          f"WHERE ADJ_ID = '{copied_line_items_for}'")
            except Exception:
                pass
        st.session_state["adj_action_flash"] = (
            "warning", f"Clone failed. {friendly_error(ex)}")
    safe_rerun()


def render_adj_card(row, expanded=False):
    """Render one adjustment's detail + actions (used as the grid's detail panel)."""
    adj_id      = row.get("ADJ_ID", "?")
    adj_label   = fmt_adj_id(row.get("DIMENSION_ADJ_ID"))
    scope       = str(row.get("PROCESS_TYPE", ""))
    adj_type    = str(row.get("ADJUSTMENT_TYPE", ""))
    run_status  = str(row.get("RUN_STATUS", ""))
    entity      = str(row.get("ENTITY_CODE", "")) or "—"
    book        = str(row.get("BOOK_CODE", "")) or "—"
    record_cnt  = row.get("RECORD_COUNT", 0)
    try:
        record_cnt = int(record_cnt) if record_cnt and record_cnt == record_cnt else 0
    except (ValueError, TypeError):
        record_cnt = 0
    scope_cfg   = SCOPE_CONFIG.get(scope, {})

    with st.expander(
        f'ADJ {adj_label} · {scope} · '
        f'{adj_type} · {run_status} · {record_cnt} rows',
        expanded=expanded,
    ):
        col_info, col_meta = st.columns([2, 1])

        with col_info:
            st.markdown(status_badge(run_status), unsafe_allow_html=True)

            # Lifecycle progress bar
            if not df_track.empty:
                track_match = df_track[df_track["ADJ_ID"] == adj_id]
                if not track_match.empty:
                    render_lifecycle_bar(track_match.iloc[0].to_dict())
                else:
                    st.markdown("<br/>", unsafe_allow_html=True)
            else:
                st.markdown("<br/>", unsafe_allow_html=True)

            section_title("Filters Applied", "search")
            render_filter_chips(row)

            import html as _htmlmod
            reason = _htmlmod.escape(str(row.get("REASON", "") or ""))
            st.markdown(
                f'<br/><div style="font-size:0.85rem"><strong>Business Reason:</strong><br/>'
                f'<span style="color:{P["grey_700"]}">{reason or "—"}</span></div>',
                unsafe_allow_html=True)

            if row.get("ERRORMESSAGE"):
                st.markdown(
                    f'<div class="overlap-box" style="margin-top:0.5rem">'
                    f'<h4>{icon("x-circle", size=13, color=P["danger"])} Error</h4>'
                    f'<div style="font-size:0.82rem;font-family:monospace">'
                    f'{_htmlmod.escape(str(row["ERRORMESSAGE"]))}</div>'
                    f'</div>',
                    unsafe_allow_html=True)

        with col_meta:
            def _fmt_ts(val):
                if val is None or str(val) == "NaT":
                    return "—"
                if hasattr(val, "strftime"):
                    return val.strftime("%d %b %Y %H:%M")
                return str(val) if str(val) not in ("None", "") else "—"

            submitted_at = _fmt_ts(row.get("SUBMITTED_AT"))
            start_date   = _fmt_ts(row.get("START_DATE"))
            process_date = _fmt_ts(row.get("PROCESS_DATE"))

            meta_rows = [
                ("Target COB",   str(row.get("COBID", "—"))),
                ("Records",      f"{record_cnt:,}" if record_cnt else "—"),
                ("Created by",   str(row.get("SUBMITTED_BY", "—"))),
                ("Created",      submitted_at),
                ("Scale",        f'{row.get("SCALE_FACTOR", 1):.4f}×' if row.get("SCALE_FACTOR") and float(row.get("SCALE_FACTOR", 1)) != 1 else "—"),
                ("Source COB",   str(row.get("SOURCE_COBID", "—")) if row.get("SOURCE_COBID") else "—"),
                ("Started",      start_date),
                ("Ended",        process_date),
                ("Occurrence",   str(row.get("ADJUSTMENT_OCCURRENCE", "—"))),
            ]
            # Report status (for Processed adjustments)
            if run_status == "Processed" and not df_track.empty:
                tr_match = df_track[df_track["ADJ_ID"] == adj_id]
                if not tr_match.empty:
                    tr = tr_match.iloc[0]
                    _rs_status = str(tr.get("REPORT_STATUS", "") or "")

                    _pbi_completed = tr.get("PBI_COMPLETED_AT")
                    _pbi_started = tr.get("PBI_STARTED_AT")
                    _pbi_queued = tr.get("PBI_QUEUED_AT")
                    _dbt_trigger = tr.get("DBT_TRIGGER_TIME")
                    _rs_time = (_dbt_trigger or _pbi_completed
                                or _pbi_started or _pbi_queued)
                    _rs_time_str = (_rs_time.strftime("%d %b %H:%M")
                                    if hasattr(_rs_time, "strftime") and str(_rs_time) != "NaT"
                                    else "")

                    _rs_icons = {
                        "Reports Ready": "check-circle",
                        "Refreshing": "refresh-cw",
                        "Queued": "clock",
                        "Awaiting": "clock",
                        "Rebuild Triggered": "check-circle",
                    }
                    _rs_messages = {
                        "Reports Ready": f"Reports Ready ({_rs_time_str})",
                        "Refreshing": f"Refreshing ({_rs_time_str})",
                        "Queued": "Queued — next ControlM cycle ~5 min",
                        "Awaiting": "Awaiting report refresh",
                        "Rebuild Triggered":
                            f"Rebuild triggered ({_rs_time_str}) — "
                            "Control-M will run the dbt refresh",
                    }
                    _rs_colors = {
                        "Reports Ready": "#15803D",
                        "Refreshing": "#1D4ED8",
                        "Queued": "#B45309",
                        "Awaiting": "#64748B",
                        "Rebuild Triggered": "#15803D",
                    }
                    color = _rs_colors.get(_rs_status, "#64748B")
                    _rs_icon = icon(_rs_icons.get(_rs_status, ""), size=12, color=color)
                    msg = _rs_messages.get(_rs_status, _rs_status)
                    meta_rows.append(("Report Status",
                        f'<span style="color:{color};font-weight:600">{_rs_icon} {msg}</span>'))
            rows_html = "".join(
                f'<tr><td style="color:{P["grey_700"]};padding:3px 0;font-size:0.8rem;'
                f'white-space:nowrap;padding-right:12px">{k}</td>'
                f'<td style="font-size:0.82rem;font-weight:600">{v}</td></tr>'
                for k, v in meta_rows if v and v != "—"
            )
            st.markdown(
                f'<div class="mcard" style="padding:0.8rem">'
                f'<table style="width:100%;border-collapse:collapse">{rows_html}</table>'
                f'</div>',
                unsafe_allow_html=True)

        # ── Status history ──────────────────────────────────────────────────
        st.markdown("---")
        section_title("Status History", "clock")
        try:
            history = run_query(f"""
                SELECT NEW_STATUS, OLD_STATUS, CHANGED_BY, CHANGED_AT, COMMENT
                FROM ADJUSTMENT_APP.ADJ_STATUS_HISTORY
                WHERE ADJ_ID = '{adj_id}'
                ORDER BY CHANGED_AT DESC
            """)
            # Convert Row objects to dicts
            history_dicts = [dict(h) for h in history] if history else []
            render_status_timeline(history_dicts)
        except Exception:
            st.info("No history available.")

        # ── Actions ─────────────────────────────────────────────────────────
        st.markdown("---")
        section_title("Actions", "zap")
        if bool(row.get("IS_DELETED")):
            st.caption("This adjustment has been deleted — actions are disabled.")
        act_cols = st.columns(4)   # deleted rows have RUN_STATUS='Deleted' → no buttons render

        # Flash from the previous action — set before the rerun (a message
        # rendered just before st.rerun() is destroyed by it).
        _flash = st.session_state.pop("adj_action_flash", None)
        if _flash:
            (st.success if _flash[0] == "success" else st.warning)(_flash[1])

        _aid = str(adj_id).replace("'", "''")
        _st  = str(run_status).replace("'", "''")
        _usr = str(user).replace("'", "''")

        _STALE_MSG = ("Nothing changed — this adjustment's status moved on since "
                      "the page loaded (another user or the pipeline acted on it). "
                      "The page now shows its current state.")

        def _updated_rows(rows) -> int:
            """Rows actually changed by an UPDATE (guarded-update check)."""
            try:
                return int(rows[0][0]) if rows else 0
            except (TypeError, ValueError, IndexError):
                return 0

        def _transition(new_status: str, extra_set: str = "", comment: str = "") -> bool:
            """Status transition guarded on the status shown on screen. Only
            writes history / reports success when the row actually moved —
            an action on a stale page changes nothing and says so."""
            n = _updated_rows(run_query(f"""
                UPDATE ADJUSTMENT_APP.ADJ_HEADER
                SET RUN_STATUS = '{new_status}'{extra_set}
                WHERE ADJ_ID = '{_aid}'
                  AND RUN_STATUS = '{_st}'
                  AND IS_DELETED = FALSE
            """))
            if n == 0:
                st.session_state["adj_action_flash"] = ("warning", _STALE_MSG)
                return False
            run_query(f"""
                INSERT INTO ADJUSTMENT_APP.ADJ_STATUS_HISTORY
                    (ADJ_ID, OLD_STATUS, NEW_STATUS, CHANGED_BY, COMMENT)
                VALUES ('{_aid}', '{_st}', '{new_status}', '{_usr}', '{comment}')
            """)
            return True

        def _do_delete() -> None:
            """Soft-delete the header + dimension row and remove the fact rows,
            all in ONE transaction — a failure anywhere rolls everything back,
            so the app never reports 'deleted' while numbers stay in reports."""
            process_type = str(row.get("PROCESS_TYPE", ""))
            txn = False
            try:
                dim_row = run_query(f"""
                    SELECT DIMENSION_ADJ_ID FROM ADJUSTMENT_APP.ADJ_HEADER
                    WHERE ADJ_ID = '{_aid}' LIMIT 1
                """)
                dim_adj_id = (int(dim_row[0]["DIMENSION_ADJ_ID"])
                              if dim_row and dim_row[0]["DIMENSION_ADJ_ID"] is not None
                              else None)
                run_query("BEGIN")
                txn = True
                n = _updated_rows(run_query(f"""
                    UPDATE ADJUSTMENT_APP.ADJ_HEADER
                    SET IS_DELETED = TRUE,
                        RUN_STATUS = 'Deleted',
                        DELETED_BY = '{_usr}',
                        DELETED_DATE = CURRENT_TIMESTAMP()
                    WHERE ADJ_ID = '{_aid}'
                      AND RUN_STATUS = '{_st}'
                      AND IS_DELETED = FALSE
                """))
                if n == 0:
                    run_query("ROLLBACK")
                    txn = False
                    st.session_state["adj_action_flash"] = ("warning", _STALE_MSG)
                    return
                run_query(f"""
                    INSERT INTO ADJUSTMENT_APP.ADJ_STATUS_HISTORY
                        (ADJ_ID, OLD_STATUS, NEW_STATUS, CHANGED_BY, COMMENT)
                    VALUES ('{_aid}', '{_st}', 'Deleted', '{_usr}', 'Adjustment deleted')
                """)
                # Processed adjustments have a dimension row + fact rows keyed
                # by DIMENSION_ADJ_ID — remove them in the same transaction.
                if dim_adj_id is not None:
                    run_query(f"""
                        UPDATE DIMENSION.ADJUSTMENT
                        SET IS_DELETED = TRUE,
                            RUN_STATUS  = 'Deleted',
                            DELETED_BY  = '{_usr}',
                            DELETED_DATE = CURRENT_TIMESTAMP()
                        WHERE ADJUSTMENT_ID = {dim_adj_id}
                    """)
                    if process_type:
                        settings = run_query(f"""
                            SELECT ADJUSTMENTS_TABLE, ADJUSTMENTS_SUMMARY_TABLE
                            FROM ADJUSTMENT_APP.ADJUSTMENTS_SETTINGS
                            WHERE UPPER(PROCESS_TYPE) = '{process_type.upper().replace("'", "''")}'
                            LIMIT 1
                        """)
                        if settings:
                            if settings[0]["ADJUSTMENTS_TABLE"]:
                                run_query(f"""
                                    DELETE FROM {settings[0]["ADJUSTMENTS_TABLE"]}
                                    WHERE ADJUSTMENT_ID = {dim_adj_id}
                                """)
                            if settings[0]["ADJUSTMENTS_SUMMARY_TABLE"]:
                                run_query(f"""
                                    DELETE FROM {settings[0]["ADJUSTMENTS_SUMMARY_TABLE"]}
                                    WHERE ADJUSTMENT_ID = {dim_adj_id}
                                """)
                run_query("COMMIT")
                txn = False
                st.session_state["adj_action_flash"] = (
                    "success",
                    "Adjustment deleted."
                    + (" Its rows were removed from the adjustment tables."
                       if dim_adj_id is not None else ""))
            except Exception as ex:
                if txn:
                    try:
                        run_query("ROLLBACK")
                    except Exception:
                        pass
                st.error(f"Delete failed — nothing was removed. {friendly_error(ex)}")

        def _delete_button(container) -> None:
            with container:
                _extra = (" and remove its processed rows from the adjustment "
                          "tables and reports" if run_status == "Processed" else "")
                confirmed = st.checkbox(
                    f"Confirm — permanently delete this adjustment{_extra}",
                    key=f"delcf_{adj_id}", value=False)
                if st.button("Delete", key=f"del_{adj_id}", use_container_width=True,
                             disabled=not confirmed):
                    _do_delete()
                    safe_rerun()

        if run_status == "Pending":
            _delete_button(act_cols[0])
            with act_cols[1]:
                if st.button("Submit for Approval", key=f"approv_{adj_id}",
                             use_container_width=True):
                    try:
                        if _transition("Pending Approval", comment="Submitted for approval"):
                            st.session_state["adj_action_flash"] = (
                                "success", "Submitted for approval.")
                        safe_rerun()
                    except Exception as ex:
                        st.error(f"Submit for approval failed. {friendly_error(ex)}")

        elif run_status == "Failed":
            with act_cols[0]:
                if st.button("Retry", key=f"retry_{adj_id}",
                             use_container_width=True, type="primary"):
                    try:
                        if _transition("Pending",
                                       extra_set=", ERRORMESSAGE = NULL, CLAIM_TOKEN = NULL",
                                       comment="Retrying after failure"):
                            st.session_state["adj_action_flash"] = (
                                "success", "Queued for retry — the pipeline picks "
                                           "it up within a minute.")
                        safe_rerun()
                    except Exception as ex:
                        st.error(f"Retry failed. {friendly_error(ex)}")
            _delete_button(act_cols[1])

        elif run_status == "Processed":
            _delete_button(act_cols[0])

        elif run_status == "Pending Approval":
            with act_cols[0]:
                if st.button("Recall to Pending", key=f"recall_{adj_id}",
                             use_container_width=True):
                    try:
                        if _transition("Pending", comment="Recalled by submitter"):
                            st.session_state["adj_action_flash"] = (
                                "success", "Recalled to Pending.")
                        safe_rerun()
                    except Exception as ex:
                        st.error(f"Recall failed. {friendly_error(ex)}")

        # ── Clone to another COB (any non-deleted adjustment) ────────────────
        if not bool(row.get("IS_DELETED")):
            st.markdown("---")
            section_title("Clone to another COB", "layers")
            st.caption(
                "Creates a NEW adjustment with the same scope, filters and "
                "values, targeting a different COB — the daily shortcut for "
                "repeating adjustments. It follows the normal flow "
                "(sign-off check, overlap blocking, approval if requested). "
                "Cross-COB Rolls keep their original source COB.")
            cc1, cc2, cc3 = st.columns([1.2, 1.6, 1])
            with cc1:
                _clone_raw = st.text_input(
                    "Target COB (YYYYMMDD)", key=f"clone_cob_{adj_id}",
                    placeholder="e.g. 20260729").strip()
            _clone_cob = None
            if _clone_raw:
                from datetime import datetime as _dt
                if _clone_raw.isdigit() and len(_clone_raw) == 8:
                    try:
                        _dt.strptime(_clone_raw, "%Y%m%d")
                        _clone_cob = int(_clone_raw)
                    except ValueError:
                        pass
                if _clone_cob is None:
                    with cc2:
                        st.error("Not a valid YYYYMMDD date.")
                elif _clone_cob == int(row.get("COBID") or 0):
                    with cc2:
                        st.error("Target COB is the same as the source.")
                    _clone_cob = None
            with cc3:
                st.markdown("<br/>", unsafe_allow_html=True)
                if st.button("Clone", key=f"clone_btn_{adj_id}",
                             use_container_width=True,
                             disabled=_clone_cob is None):
                    _do_clone(adj_id, _clone_cob)


# ── Browse + act ───────────────────────────────────────────────────────────────

if df_adjs.empty:
    view_df = df_adjs
else:
    is_del = df_adjs["IS_DELETED"].fillna(False).astype(bool)
    # Selecting a soft-deleted status (Deleted / Replaced / Superseded) in the
    # Status filter is an explicit request to see those rows — honour it even
    # when "Include deleted" is unticked, otherwise the filter would load the
    # rows and then silently mask them out again (showing 0 results).
    _deletedish = {"Deleted", "Replaced", "Superseded"}
    _wants_deleted = bool(set(filter_status or []) & _deletedish)
    view_df = df_adjs if (show_deleted or _wants_deleted) else df_adjs[~is_del]

view_df = view_df.reset_index(drop=True)

total = len(df_adjs)
shown = len(view_df)
with bordered_container():
    _rh1, _rh2 = st.columns([5, 1])
    with _rh1:
        section_title(f"Results — {shown} of {total}", "table")
        st.caption("Select a row to view its details and actions.")
    with _rh2:
        st.download_button(
            "⬇ Export CSV", view_df.to_csv(index=False).encode("utf-8-sig"),
            file_name="adjustments_export.csv", mime="text/csv",
            use_container_width=True,
            help="Download the filtered list for Excel.")

    selected = render_activity_grid(
        view_df, selectable=True, key="adj_grid",
        empty_msg="No adjustments match the current filter.")

    # Older Streamlit-in-Snowflake runtimes lack native row-selection; fall back to
    # a selectbox picker (same no-tabs single-grid design, just a different control).
    if selected is SELECTION_UNSUPPORTED:
        def _opt_label(i):
            if i is None:
                return "— select an adjustment to view details / actions —"
            r = view_df.iloc[i]
            return (f'{fmt_adj_id(r.get("DIMENSION_ADJ_ID"))} · {r.get("PROCESS_TYPE")} · '
                    f'{r.get("ADJUSTMENT_TYPE")} · {r.get("RUN_STATUS")} · '
                    f'{r.get("ENTITY_CODE") or "—"}')
        choice = st.selectbox(
            "Open an adjustment", options=[None] + list(range(len(view_df))),
            format_func=_opt_label, key="adj_pick", label_visibility="collapsed")
        selected = view_df.iloc[choice].to_dict() if choice is not None else None

# ── Bulk retry — all FAILED rows in the current filtered view ────────────────
_bulk_flash = st.session_state.pop("adj_bulk_flash", None)
if _bulk_flash:
    (st.success if _bulk_flash[0] == "success" else st.warning)(_bulk_flash[1])

_failed_view = (view_df[view_df["RUN_STATUS"] == "Failed"]
                if "RUN_STATUS" in view_df.columns else view_df.iloc[0:0])
if len(_failed_view) >= 2:
    with bordered_container():
        section_title(f"Bulk Retry — {len(_failed_view)} failed in this view",
                      "refresh-cw")
        st.caption("Re-queues every FAILED adjustment currently shown by the "
                   "filters above. Retries are safe: anything a failed run "
                   "wrote is cleaned up before re-processing.")
        br1, br2 = st.columns([3, 1])
        with br1:
            _br_confirm = st.checkbox(
                f"Confirm — retry all {len(_failed_view)} failed adjustment(s)",
                key="bulk_retry_confirm", value=False)
        with br2:
            if st.button("Retry all failed", key="bulk_retry_btn",
                         type="primary", use_container_width=True,
                         disabled=not _br_confirm):
                _ok, _skipped = 0, 0
                for _, _fr in _failed_view.iterrows():
                    try:
                        _fid = str(_fr.get("ADJ_ID")).replace("'", "''")
                        rows = run_query(f"""
                            UPDATE ADJUSTMENT_APP.ADJ_HEADER
                            SET RUN_STATUS = 'Pending',
                                ERRORMESSAGE = NULL, CLAIM_TOKEN = NULL
                            WHERE ADJ_ID = '{_fid}'
                              AND RUN_STATUS = 'Failed'
                              AND IS_DELETED = FALSE
                        """)
                        _nn = int(rows[0][0]) if rows else 0
                        if _nn:
                            run_query(f"""
                                INSERT INTO ADJUSTMENT_APP.ADJ_STATUS_HISTORY
                                    (ADJ_ID, OLD_STATUS, NEW_STATUS, CHANGED_BY, COMMENT)
                                VALUES ('{_fid}', 'Failed', 'Pending',
                                        '{str(user).replace(chr(39), chr(39)*2)}',
                                        'Bulk retry after failure')
                            """)
                            _ok += 1
                        else:
                            _skipped += 1
                    except Exception:
                        _skipped += 1
                msg = (f"{_ok} adjustment(s) re-queued — the pipeline picks "
                       f"them up within a minute.")
                if _skipped:
                    msg += (f" {_skipped} skipped (status changed or the "
                            f"update failed).")
                st.session_state["adj_bulk_flash"] = (
                    "success" if _ok and not _skipped else "warning", msg)
                safe_rerun()

if selected is not None:
    st.markdown("<div style='height:1rem'></div>", unsafe_allow_html=True)
    with bordered_container():
        section_title("Adjustment Detail", "file-text")
        render_adj_card(selected, expanded=True)
