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
    render_lifecycle_bar, fmt_user_dt,
    status_badge, section_title, P, SCOPE_CONFIG, STAGE_CONFIG, ALL_SCOPES,
    STATUS_COLORS, STATUS_ICONS,
    fmt_adj_id, icon, render_activity_grid, render_df_table, SELECTION_UNSUPPORTED,
    bordered_container,
    set_flash, render_flash, confirm_gate, ACTION_LABELS,
)
from utils.snowflake_conn import (run_query, run_query_df, current_user_name,
                                  safe_rerun, friendly_error)

inject_css()
render_sidebar()

user = current_user_name()

# Page-level flash key — every action on this page stores its outcome here
# (set_flash) BEFORE safe_rerun() and it is rendered ONCE, below the header,
# so it survives the rerun and never appears under a different adjustment.
_FLASH = "adjustments"


def _identity_keys(name: str) -> set:
    """Comparable forms of an identity (same approach as the Admin page): the
    viewer name may be an EMAIL while ADJ_ADMINS / SUBMITTED_BY hold Snowflake
    USERNAMES — compare on the full string AND the local part before '@',
    both upper-cased."""
    n = str(name or "").strip().upper()
    if not n:
        return set()
    keys = {n}
    if "@" in n:
        keys.add(n.split("@", 1)[0])
    return keys


_me_keys = _identity_keys(user)


def _load_is_admin() -> bool:
    """True when the current user is an active USER entry in ADJ_ADMINS.
    (ROLE entries need SHOW GRANTS OF ROLE, which only the Admin page
    resolves — deliberately not replicated here.)"""
    try:
        rows = run_query(
            "SELECT USERNAME, COALESCE(ADMIN_TYPE, 'USER') AS ADMIN_TYPE "
            "FROM ADJUSTMENT_APP.ADJ_ADMINS WHERE IS_ACTIVE = TRUE")
    except Exception:
        try:  # pre-ADMIN_TYPE deployment
            rows = run_query(
                "SELECT USERNAME, 'USER' AS ADMIN_TYPE "
                "FROM ADJUSTMENT_APP.ADJ_ADMINS WHERE IS_ACTIVE = TRUE")
        except Exception:
            return False
    for r in rows or []:
        if str(r["ADMIN_TYPE"]).upper() != "USER":
            continue
        if _identity_keys(r["USERNAME"]) & _me_keys:
            return True
    return False


if "_adj_is_admin" not in st.session_state:
    st.session_state["_adj_is_admin"] = _load_is_admin()
_is_admin = bool(st.session_state["_adj_is_admin"])

st.markdown("## Adjustments")
st.markdown(
    f"<span style='color:{P['grey_700']};font-size:0.9rem'>"
    f"Pipeline status at a glance, then all adjustments with full history and "
    f"actions. Tick <em>Only my adjustments</em> to see just yours.</span>",
    unsafe_allow_html=True)
render_flash(_FLASH)
st.markdown("<br/>", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# PIPELINE OVERVIEW — count boxes + stage board (merged in from the retired
# Adjustment Pipeline page). ONE lean query over VW_ADJUSTMENT_TRACK — just the
# columns the boxes and board need — so it stays fast (the old page pulled 500
# full rows plus many extra sections, which is why it was slow).
# ──────────────────────────────────────────────────────────────────────────────
df_pipe = pd.DataFrame()
try:
    df_pipe = run_query_df("""
        SELECT CURRENT_STAGE, RUN_STATUS, BLOCKED_BY_ADJ_ID,
               PROCESS_TYPE, ENTITY_CODE, BOOK_CODE
        FROM ADJUSTMENT_APP.VW_ADJUSTMENT_TRACK
        WHERE COALESCE(IS_DELETED, FALSE) = FALSE
        ORDER BY SUBMITTED_AT DESC
        LIMIT 500
    """)
except Exception as _pe:
    st.warning(f"Could not load pipeline overview: {_pe}")

if not df_pipe.empty:
    _st = df_pipe["RUN_STATUS"].fillna("")
    _blk = (df_pipe["BLOCKED_BY_ADJ_ID"].notna()
            if "BLOCKED_BY_ADJ_ID" in df_pipe.columns
            else pd.Series(False, index=df_pipe.index))
    _stage = df_pipe["CURRENT_STAGE"].fillna("")
    st.caption("Pipeline overview — counts over the last 500 submissions.")
    _boxes = [
        ("Awaiting Approval", int((_st == "Pending Approval").sum()), P["info"], "clipboard"),
        ("Queued",    int((_st.isin(["Pending", "Approved"]) & ~_blk).sum()), P["warning"], "clock"),
        ("Blocked",   int((_st.isin(["Pending", "Approved"]) & _blk).sum()),  P["warning"], "pause-circle"),
        ("Running",   int((_st == "Running").sum()),  P["info"],    "zap"),
        ("Failed",    int((_st == "Failed").sum()),   P["danger"],  "x-circle"),
        ("Reports Ready", int(_stage.isin(["Reports Ready", "Rebuild Triggered"]).sum()),
         P["success"], "check-circle"),
    ]
    _bcols = st.columns(len(_boxes))
    for _c, (_lbl, _val, _col, _ic) in zip(_bcols, _boxes):
        _c.markdown(
            f'<div style="background:{P["white"]};border:1px solid {P["border"]};'
            f'border-top:3px solid {_col};border-radius:8px;padding:0.8rem;text-align:center">'
            f'<div style="font-size:1.6rem;font-weight:800;color:{_col};'
            f'font-variant-numeric:tabular-nums">'
            f'{icon(_ic, size=15, color=_col)} {_val}</div>'
            f'<div style="font-size:0.75rem;text-transform:uppercase;letter-spacing:.06em;'
            f'color:{P["grey_700"]};margin-top:3px">{_lbl}</div></div>',
            unsafe_allow_html=True)
    st.caption("Blocked = waiting for an overlapping adjustment to finish first "
               "— no action needed.")

    # Awaiting Approval links to the Approval Queue page (decisions live there).
    if int((_st == "Pending Approval").sum()) > 0:
        try:
            st.page_link("pages/3_Approval_Queue.py",
                         label="→ Go to the Approval Queue to approve or reject",
                         icon="✅")
        except Exception:
            st.caption("Approve or reject pending items on the Approval Queue page.")

    st.markdown("<br/>", unsafe_allow_html=True)

    # ── Pipeline board — a column per stage with counts + a few items ─────────
    with st.expander("Pipeline board — where everything is right now", expanded=False):
        _BOARD_STAGES = [
            "Submitted", "Pending Approval", "Approved", "Processing",
            "PBI Queued", "PBI Refreshing", "Reports Ready",
            "Rebuild Pending", "Rebuild Triggered",
        ]
        # Stage codes are data values (matched against CURRENT_STAGE); the
        # labels shown to users are plain English.
        _STAGE_LABELS = {
            "PBI Queued": "Power BI refresh queued",
            "PBI Refreshing": "Power BI refreshing",
            "Rebuild Pending": "dbt rebuild pending",
            "Rebuild Triggered": "dbt rebuild triggered",
        }
        _bh = '<div class="tracker-board">'
        for _stg in _BOARD_STAGES:
            _cfg = STAGE_CONFIG.get(_stg, {"color": "#9E9E9E", "icon": "", "bg": "#F5F5F5"})
            _items = df_pipe[df_pipe["CURRENT_STAGE"] == _stg]
            _cnt = int(_items.shape[0])
            _ih = ""
            for _, _r in _items.head(10).iterrows():
                _sc = str(_r.get("PROCESS_TYPE", "") or "")
                _sccfg = SCOPE_CONFIG.get(_sc, {})
                _dp = [x for x in [str(_r.get("ENTITY_CODE", "") or ""),
                                   str(_r.get("BOOK_CODE", "") or "")] if x]
                _det = " · ".join(_dp) if _dp else "All"
                _ih += (f'<div class="board-item">'
                        f'<div class="bi-scope">{icon(_sccfg.get("icon", ""), size=11)} {_sc}</div>'
                        f'<div class="bi-detail">{_det}</div></div>')
            if _cnt > 10:
                _ih += (f'<div style="font-size:0.75rem;color:{P["grey_700"]};'
                        f'text-align:center;padding:4px">+ {_cnt - 10} more</div>')
            _bh += (f'<div class="board-col" style="border-top-color:{_cfg["color"]}">'
                    f'<div class="board-col-header" style="color:{_cfg["color"]}">'
                    f'{icon(_cfg["icon"], size=13)} {_STAGE_LABELS.get(_stg, _stg)}'
                    f'<span class="board-col-count" style="color:{_cfg["color"]}">{_cnt}</span>'
                    f'</div>{_ih}</div>')
        _bh += '</div>'
        st.markdown(_bh, unsafe_allow_html=True)

    st.markdown("<br/>", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# FILTERS
# ──────────────────────────────────────────────────────────────────────────────

# Distinct dimension values for the COB / Entity / Department / User filters
# below. Cached in session_state: filter options don't change meaningfully
# within a session, and re-running this DISTINCT query on every filter
# keystroke (each keystroke reruns the page) added avoidable latency.
if "_adj_filter_opts" not in st.session_state:
    try:
        st.session_state["_adj_filter_opts"] = run_query_df("""
            SELECT DISTINCT COBID, ENTITY_CODE, DEPARTMENT_CODE, SUBMITTED_BY
            FROM ADJUSTMENT_APP.VW_MY_WORK
        """)
    except Exception:
        st.session_state["_adj_filter_opts"] = pd.DataFrame(
            columns=["COBID", "ENTITY_CODE", "DEPARTMENT_CODE", "SUBMITTED_BY"])
_fopts = st.session_state["_adj_filter_opts"]

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
        _type_labels = {"Flatten": "Flatten", "Scale": "Scale", "Roll": "Roll"}
        for _code in ("Direct", "Upload", "EROL"):
            _type_labels[_code] = ACTION_LABELS.get(_code, _code)
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
                   "No filters applied — showing the newest 200 adjustments.")
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
        in_list = ",".join("'" + str(e).replace("\\", "\\\\").replace("'", "''") + "'" for e in filter_entity)
        where_clauses.append(f"ENTITY_CODE IN ({in_list})")
    if filter_dept:
        in_list = ",".join("'" + str(d).replace("\\", "\\\\").replace("'", "''") + "'" for d in filter_dept)
        where_clauses.append(f"DEPARTMENT_CODE IN ({in_list})")
    if filter_user:
        in_list = ",".join("'" + str(u).replace("\\", "\\\\").replace("'", "''") + "'" for u in filter_user)
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

# Lifecycle tracking — ONLY for the (≤200) adjustments actually shown.
# Previously this loaded ALL of VW_ADJUSTMENT_TRACK (no LIMIT) on EVERY
# rerun; since each filter keystroke reruns the page, and the view now
# self-joins ADJ_HEADER, that unbounded query per keystroke froze the page.
# Bounding it to the shown ADJ_IDs makes it cheap and constant.
df_track = pd.DataFrame()
if not df_adjs.empty and "ADJ_ID" in df_adjs.columns:
    _ids = [str(a) for a in df_adjs["ADJ_ID"].dropna().unique().tolist()]
    if _ids:
        _in = ",".join(
            "'" + i.replace("\\", "\\\\").replace("'", "''") + "'" for i in _ids)
        try:
            df_track = run_query_df(f"""
                SELECT ADJ_ID, CURRENT_STAGE, REPORT_STATUS,
                       SUBMITTED_AT, APPROVAL_REQUESTED_AT, APPROVED_AT,
                       PROCESSING_STARTED_AT, PROCESSING_ENDED_AT,
                       PBI_QUEUED_AT, PBI_STARTED_AT, PBI_COMPLETED_AT,
                       PBI_REFRESH_DURATION_SEC, PBI_QUEUE_WAIT_SEC,
                       REFRESH_PATH, DBT_TRIGGER_TIME,
                       RUN_STATUS
                FROM ADJUSTMENT_APP.VW_ADJUSTMENT_TRACK
                WHERE ADJ_ID IN ({_in})
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


def _do_clone(src_adj_id, new_cob, requires_approval: bool = False) -> None:
    """Create a NEW adjustment at `new_cob` from an existing one's header
    (and, for VaR Upload / legacy Direct rows that carry line items, a copy
    of them). Goes through SP_SUBMIT_ADJUSTMENT like any submission —
    validation, sign-off check, overlap blocking and (if requested) approval
    all apply normally."""
    import json as _json
    import uuid as _uuid
    _sid = str(src_adj_id).replace("\\", "\\\\").replace("'", "''")
    copied_line_items_for = None
    try:
        src_rows = run_query(
            f"SELECT * FROM ADJUSTMENT_APP.ADJ_HEADER WHERE ADJ_ID = '{_sid}'")
        if not src_rows:
            set_flash(_FLASH, "warning",
                      "Clone failed — the source adjustment was not found.")
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
            "requires_approval": bool(requires_approval),
            "reason": (f"Cloned from ADJ "
                       f"{fmt_adj_id(src.get('DIMENSION_ADJ_ID'), adj_id=str(src_adj_id))}"
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
            if _copied == 0 and (_adj_action == "Upload" or str(src.get("PROCESS_TYPE"))
                                 in ("FRTB", "FRTBDRC", "FRTBRRAO")):
                set_flash(_FLASH, "warning",
                          "Clone failed — the source upload has no line items to copy.")
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
            set_flash(_FLASH, "success",
                      f"Cloned to COB {new_cob} — new adjustment created with "
                      f"status '{status}'. {out.get('message', '')}")
        else:
            if copied_line_items_for:
                try:
                    run_query(f"DELETE FROM ADJUSTMENT_APP.ADJ_LINE_ITEM_JSON "
                              f"WHERE ADJ_ID = '{copied_line_items_for}'")
                except Exception:
                    pass
            set_flash(_FLASH, "warning",
                      f"Clone was not accepted — {out.get('message', status or 'no response')}")
    except Exception as ex:
        if copied_line_items_for:
            try:
                run_query(f"DELETE FROM ADJUSTMENT_APP.ADJ_LINE_ITEM_JSON "
                          f"WHERE ADJ_ID = '{copied_line_items_for}'")
            except Exception:
                pass
        set_flash(_FLASH, "warning", f"Clone failed. {friendly_error(ex)}")
    safe_rerun()


def render_adj_card(row, expanded=False):
    """Render one adjustment's detail + actions (used as the grid's detail panel)."""
    adj_id      = row.get("ADJ_ID", "?")
    adj_label   = fmt_adj_id(row.get("DIMENSION_ADJ_ID"), adj_id=adj_id)
    scope       = str(row.get("PROCESS_TYPE", ""))
    adj_type    = str(row.get("ADJUSTMENT_TYPE", ""))
    type_label  = ACTION_LABELS.get(adj_type, adj_type)
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
        f'{type_label} · {run_status} · {record_cnt} rows',
        expanded=expanded,
    ):
        col_info, col_meta = st.columns([2, 1])

        with col_info:
            st.markdown(status_badge(run_status), unsafe_allow_html=True)
            if run_status == "Pending" and not bool(row.get("IS_DELETED")):
                st.caption("Queued — the pipeline picks this up within a minute. "
                           "Delete it now if it should not run.")

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
                    return fmt_user_dt(val) or "—"
                return str(val) if str(val) not in ("None", "") else "—"

            submitted_at = _fmt_ts(row.get("SUBMITTED_AT"))
            start_date   = _fmt_ts(row.get("START_DATE"))
            process_date = _fmt_ts(row.get("PROCESS_DATE"))

            meta_rows = [
                ("Target COB",   str(row.get("COBID", "—"))),
                ("Records",      f"{record_cnt:,}" if record_cnt else "—"),
                ("Created by",   str(row.get("SUBMITTED_BY", "—"))),
                ("Created",      submitted_at),
                ("Scale",        f'{row.get("SCALE_FACTOR", 1):.4f}×'
                                 if pd.notna(row.get("SCALE_FACTOR"))
                                 and row.get("SCALE_FACTOR")
                                 and float(row.get("SCALE_FACTOR", 1)) != 1 else "—"),
                ("Source COB",   str(row.get("SOURCE_COBID", "—")) if row.get("SOURCE_COBID") else "—"),
                ("Started",      start_date),
                ("Ended",        process_date),
                ("Occurrence",   {"ADHOC": "One-off"}.get(
                                     str(row.get("ADJUSTMENT_OCCURRENCE") or "").upper(),
                                     str(row.get("ADJUSTMENT_OCCURRENCE") or "—"))),
            ]
            _meta_val_col = {}     # Value text → colour (Report Status only)
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
                    _rs_time_str = fmt_user_dt(_rs_time, "%d %b %H:%M")

                    _rs_messages = {
                        "Reports Ready": f"Reports Ready ({_rs_time_str})",
                        "Refreshing": f"Refreshing ({_rs_time_str})",
                        "Queued": "Queued — picked up by the next scheduled rebuild (~5 min)",
                        "Awaiting": "Awaiting report refresh",
                        "Rebuild Triggered":
                            f"dbt rebuild triggered ({_rs_time_str}) — "
                            "reports refresh on the next scheduled rebuild",
                    }
                    _rs_colors = {
                        "Reports Ready": P["success"],
                        "Refreshing": P["info"],
                        "Queued": P["warning"],
                        "Awaiting": "#64748B",
                        "Rebuild Triggered": P["success"],
                    }
                    color = _rs_colors.get(_rs_status, "#64748B")
                    msg = _rs_messages.get(_rs_status, _rs_status)
                    meta_rows.append(("Report Status", msg))
                    _meta_val_col[msg] = color
            df_meta = pd.DataFrame(
                [(k, v) for k, v in meta_rows if v and v != "—"],
                columns=["Field", "Value"])
            with bordered_container():
                render_df_table(df_meta, max_rows=len(df_meta),
                                color_cols={"Value": lambda v: _meta_val_col.get(v, "")},
                                key=f"adj_meta_{adj_id}")

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
            history_dicts = ([h.as_dict() if hasattr(h, "as_dict") else dict(h)
                              for h in history] if history else [])
            render_status_timeline(history_dicts)
        except Exception:
            st.info("No history available.")

        # ── Actions ─────────────────────────────────────────────────────────
        st.markdown("---")
        section_title("Actions", "zap")
        if bool(row.get("IS_DELETED")):
            st.caption("This adjustment has been deleted — actions are disabled.")
        act_cols = st.columns(4)   # deleted rows have RUN_STATUS='Deleted' → no buttons render

        _aid = str(adj_id).replace("\\", "\\\\").replace("'", "''")
        _st  = str(run_status).replace("\\", "\\\\").replace("'", "''")
        _usr = str(user).replace("\\", "\\\\").replace("'", "''")

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
                set_flash(_FLASH, "warning", _STALE_MSG)
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
                    set_flash(_FLASH, "warning", _STALE_MSG)
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
                        _pt_esc = (process_type.upper()
                                   .replace("\\", "\\\\").replace("'", "''"))
                        settings = run_query(f"""
                            SELECT ADJUSTMENTS_TABLE, ADJUSTMENTS_SUMMARY_TABLE
                            FROM ADJUSTMENT_APP.ADJUSTMENTS_SETTINGS
                            WHERE UPPER(PROCESS_TYPE) = '{_pt_esc}'
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
                set_flash(_FLASH, "success",
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

        def _signoff_blocked() -> bool:
            """True when the row's COB is signed off (or a re-open is pending)
            for its scope — for this entity or for the whole scope ('*').
            Deleting would silently change signed-off numbers, so the app
            asks for a re-open first (same gate SP_SUBMIT_ADJUSTMENT applies)."""
            try:
                _cob = int(row.get("COBID"))
            except (TypeError, ValueError):
                return False
            _pt = str(row.get("PROCESS_TYPE") or "").replace("\\", "\\\\").replace("'", "''")
            _en = str(row.get("ENTITY_CODE") or "").replace("\\", "\\\\").replace("'", "''")
            try:
                rows = run_query(f"""
                    SELECT COUNT(*) AS N
                    FROM ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS
                    WHERE COBID = {_cob}
                      AND UPPER(PROCESS_TYPE) = UPPER('{_pt}')
                      AND ENTITY_CODE IN ('{_en}', '*')
                      AND UPPER(SIGN_OFF_STATUS) IN ('SIGNED_OFF', 'REOPEN_REQUESTED')
                """)
                return bool(rows and int(rows[0][0]) > 0)
            except Exception:
                return False   # gate unavailable → SP-level checks still apply

        def _delete_button(container) -> None:
            with container:
                _owner = bool(_identity_keys(row.get("SUBMITTED_BY")) & _me_keys)
                if not (_owner or _is_admin):
                    st.caption("Only the person who submitted this adjustment "
                               "(or an administrator) can delete it.")
                    return
                if _signoff_blocked():
                    st.markdown(
                        f'<div style="font-size:0.85rem;color:{P["warning"]}">'
                        f'{icon("lock", size=13, color=P["warning"])} '
                        f'COB {row.get("COBID")} is signed off for {scope} — '
                        f'request a re-open on the Sign-Off page first.</div>',
                        unsafe_allow_html=True)
                    return
                _extra = (" and remove its processed rows from the adjustment "
                          "tables and reports" if run_status == "Processed" else "")
                confirmed = confirm_gate(
                    f"Confirm — permanently delete this adjustment{_extra}",
                    key=f"delcf_{adj_id}")
                if st.button("Delete", key=f"del_{adj_id}", use_container_width=True,
                             disabled=not confirmed):
                    _do_delete()
                    safe_rerun()

        if run_status == "Pending":
            # No "Submit for Approval" here: approval is chosen at submission
            # time and a Pending row is already queued for the pipeline.
            _delete_button(act_cols[0])

        elif run_status == "Failed":
            with act_cols[0]:
                if st.button("Retry", key=f"retry_{adj_id}",
                             use_container_width=True, type="primary"):
                    try:
                        if _transition("Pending",
                                       extra_set=", ERRORMESSAGE = NULL, CLAIM_TOKEN = NULL",
                                       comment="Retrying after failure"):
                            set_flash(_FLASH, "success",
                                      "Queued for retry — the pipeline picks it up "
                                      "within a minute.")
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
                            set_flash(_FLASH, "success", "Recalled to Pending.")
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
            # Default "Request approval" to what the source did: if its history
            # ever passed through Pending Approval, the clone asks for it too.
            _appr_key = f"_clone_appr_default_{adj_id}"
            if _appr_key not in st.session_state:
                _src_needed_approval = False
                try:
                    _hrows = run_query(f"""
                        SELECT COUNT(*) AS N FROM ADJUSTMENT_APP.ADJ_STATUS_HISTORY
                        WHERE ADJ_ID = '{_aid}' AND NEW_STATUS = 'Pending Approval'
                    """)
                    _src_needed_approval = bool(_hrows and int(_hrows[0][0]) > 0)
                except Exception:
                    pass
                st.session_state[_appr_key] = _src_needed_approval
            with cc2:
                _clone_appr = st.checkbox(
                    "Request approval", key=f"clone_appr_{adj_id}",
                    value=bool(st.session_state[_appr_key]),
                    help="Send the clone to the Approval Queue before it runs. "
                         "Pre-ticked when the original went through approval.")
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
                    _do_clone(adj_id, _clone_cob, requires_approval=_clone_appr)


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
        if total >= 200:
            st.caption("Showing the newest 200 matching adjustments — narrow "
                       "the filters to see older ones.")
    with _rh2:
        st.download_button(
            "⬇ Export CSV", view_df.to_csv(index=False).encode("utf-8-sig"),
            file_name="adjustments_export.csv", mime="text/csv",
            use_container_width=True,
            help="Download the filtered list for Excel.")

# The grid renders BARE on the page, outside the card: Grid Lab proved the
# fixed-height st.dataframe is clean under Menlo when unwrapped (style A),
# and the card's box-shadow/border-radius/overflow-clip around a live canvas
# is the remaining difference vs the page where it still white-tiled.
selected = render_activity_grid(
    view_df, selectable=True, key="adj_grid",
    empty_msg="No adjustments match the current filter.")

# Older Streamlit-in-Snowflake runtimes lack native row-selection; fall back to
# a selectbox picker (same no-tabs single-grid design, just a different control).
if selected is SELECTION_UNSUPPORTED:
    # Options are ADJ_IDs (not positions) and the labels carry NO status: after
    # an action the status changes, so a status-bearing label would change the
    # option set and reset the picker. The chosen ADJ_ID is remembered in
    # session_state and the index recomputed on every rerun, so the same
    # adjustment stays open after Delete / Retry / Recall / Clone.
    _pick_ids = (view_df["ADJ_ID"].astype(str).tolist()
                 if "ADJ_ID" in view_df.columns else [])
    _pick_pos = {}
    for _i, _pid in enumerate(_pick_ids):
        _pick_pos.setdefault(_pid, _i)
    _pick_options = [None] + list(_pick_pos.keys())
    _remembered = st.session_state.get("_adj_pick_id")
    _pick_index = (_pick_options.index(_remembered)
                   if _remembered in _pick_pos else 0)

    def _opt_label(aid):
        if aid is None:
            return "— select an adjustment to view details / actions —"
        r = view_df.iloc[_pick_pos[aid]]
        _t = str(r.get("ADJUSTMENT_TYPE") or "")
        _created = fmt_user_dt(r.get("SUBMITTED_AT"), "%d %b %H:%M")
        parts = [
            fmt_adj_id(r.get("DIMENSION_ADJ_ID"), adj_id=aid),
            str(r.get("PROCESS_TYPE") or "—"),
            ACTION_LABELS.get(_t, _t) or "—",
            f"COB {r.get('COBID')}" if r.get("COBID") else "—",
            str(r.get("ENTITY_CODE") or "—"),
        ]
        if _created:
            parts.append(_created)
        return " · ".join(parts)

    # Breathing room between the grid and the picker so they don't crowd.
    st.markdown("<div style='height:1rem'></div>", unsafe_allow_html=True)
    choice = st.selectbox(
        "Open an adjustment for details / actions",
        options=_pick_options, index=_pick_index,
        format_func=_opt_label, key="adj_pick")
    st.session_state["_adj_pick_id"] = choice
    selected = (view_df.iloc[_pick_pos[choice]].to_dict()
                if choice is not None else None)

# ── Bulk retry — all FAILED rows in the current filtered view ────────────────
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
                        _fid = str(_fr.get("ADJ_ID")).replace("\\", "\\\\").replace("'", "''")
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
                                        '{str(user).replace(chr(92), chr(92)*2).replace(chr(39), chr(39)*2)}',
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
                set_flash(_FLASH, "success" if _ok and not _skipped else "warning", msg)
                safe_rerun()

if selected is not None:
    st.markdown("<div style='height:1rem'></div>", unsafe_allow_html=True)
    with bordered_container():
        section_title("Adjustment Detail", "file-text")
        render_adj_card(selected, expanded=True)
