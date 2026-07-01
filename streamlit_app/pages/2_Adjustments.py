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
    status_badge, section_title, P, SCOPE_CONFIG, STATUS_COLORS, STATUS_ICONS,
    fmt_adj_id, icon, render_activity_grid, SELECTION_UNSUPPORTED,
)
from utils.snowflake_conn import run_query, run_query_df, current_user_name, safe_rerun

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

f1, f2, f3, f4 = st.columns(4)
with f1:
    filter_status = st.multiselect(
        "Status",
        list(STATUS_COLORS.keys()),
        default=[], key="mw_status")
with f2:
    filter_scope = st.multiselect(
        "Scope", list(SCOPE_CONFIG.keys()),
        default=[], key="mw_scope")
with f3:
    # Option values are the raw ADJUSTMENT_TYPE codes (used directly in the SQL
    # filter); the label maps the cryptic "EROL" code to "Entity Roll".
    _type_labels = {"Flatten": "Flatten", "Scale": "Scale", "Roll": "Roll",
                    "EROL": "Entity Roll"}
    filter_type = st.multiselect(
        "Type", list(_type_labels.keys()),
        default=[], key="mw_type",
        format_func=lambda v: _type_labels.get(v, v))
with f4:
    mine_only = st.checkbox("Only my adjustments", value=False,
                            help="When checked, shows only adjustments you submitted.")

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

st.markdown("<br/>", unsafe_allow_html=True)

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
               RUN_STATUS
    FROM ADJUSTMENT_APP.VW_ADJUSTMENT_TRACK
    """)
except Exception:
    pass

# Backwards compat alias
df_report_status = df_track


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

            reason = row.get("REASON", "")
            st.markdown(
                f'<br/><div style="font-size:0.85rem"><strong>Business Reason:</strong><br/>'
                f'<span style="color:{P["grey_700"]}">{reason or "—"}</span></div>',
                unsafe_allow_html=True)

            if row.get("ERRORMESSAGE"):
                st.markdown(
                    f'<div class="overlap-box" style="margin-top:0.5rem">'
                    f'<h4>{icon("x-circle", size=13, color=P["danger"])} Error</h4>'
                    f'<div style="font-size:0.82rem;font-family:monospace">{row["ERRORMESSAGE"]}</div>'
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
                    _rs_time = _pbi_completed or _pbi_started or _pbi_queued
                    _rs_time_str = (_rs_time.strftime("%d %b %H:%M")
                                    if hasattr(_rs_time, "strftime") and str(_rs_time) != "NaT"
                                    else "")

                    _rs_icons = {
                        "Reports Ready": "check-circle",
                        "Refreshing": "refresh-cw",
                        "Queued": "clock",
                        "Awaiting": "clock",
                    }
                    _rs_messages = {
                        "Reports Ready": f"Reports Ready ({_rs_time_str})",
                        "Refreshing": f"Refreshing ({_rs_time_str})",
                        "Queued": "Queued — next ControlM cycle ~5 min",
                        "Awaiting": "Awaiting report refresh",
                    }
                    _rs_colors = {
                        "Reports Ready": "#15803D",
                        "Refreshing": "#1D4ED8",
                        "Queued": "#B45309",
                        "Awaiting": "#64748B",
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

        if run_status in ("Pending", "Failed", "Processed"):
            with act_cols[0]:
                if st.button("Delete", key=f"del_{adj_id}", use_container_width=True):
                    try:
                        process_type = str(row.get("PROCESS_TYPE", ""))
                        # Get DIMENSION_ADJ_ID — used for DIMENSION.ADJUSTMENT and FACT table deletes
                        dim_row = run_query(f"""
                            SELECT DIMENSION_ADJ_ID FROM ADJUSTMENT_APP.ADJ_HEADER
                            WHERE ADJ_ID = '{adj_id}' LIMIT 1
                        """)
                        dim_adj_id = (dim_row[0]["DIMENSION_ADJ_ID"]
                                      if dim_row and dim_row[0]["DIMENSION_ADJ_ID"] else None)
                        # Soft-delete header
                        run_query(f"""
                            UPDATE ADJUSTMENT_APP.ADJ_HEADER
                            SET IS_DELETED = TRUE,
                                RUN_STATUS = 'Deleted'
                            WHERE ADJ_ID = '{adj_id}'
                        """)
                        run_query(f"""
                            INSERT INTO ADJUSTMENT_APP.ADJ_STATUS_HISTORY
                                (ADJ_ID, OLD_STATUS, NEW_STATUS, CHANGED_BY, COMMENT)
                            VALUES ('{adj_id}', '{run_status}', 'Deleted',
                                    '{user}', 'Adjustment deleted')
                        """)
                        # Soft-delete in DIMENSION.ADJUSTMENT (uses the dimension table's own ID)
                        if dim_adj_id:
                            try:
                                run_query(f"""
                                    UPDATE DIMENSION.ADJUSTMENT
                                    SET IS_DELETED = TRUE,
                                        RUN_STATUS  = 'Deleted',
                                        DELETED_BY  = '{user}',
                                        DELETED_DATE = CURRENT_TIMESTAMP()
                                    WHERE ADJUSTMENT_ID = {dim_adj_id}
                                """)
                            except Exception:
                                pass
                        # Remove rows from fact adjustment and summary tables
                        if process_type and dim_adj_id:
                            try:
                                settings = run_query(f"""
                                    SELECT ADJUSTMENTS_TABLE, ADJUSTMENTS_SUMMARY_TABLE
                                    FROM ADJUSTMENT_APP.ADJUSTMENTS_SETTINGS
                                    WHERE UPPER(PROCESS_TYPE) = '{process_type.upper()}'
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
                            except Exception:
                                pass
                        st.success("Adjustment deleted.")
                        safe_rerun()
                    except Exception as ex:
                        st.error(str(ex))
            with act_cols[1]:
                if st.button("Submit for Approval", key=f"approv_{adj_id}", use_container_width=True):
                    try:
                        run_query(f"""
                            UPDATE ADJUSTMENT_APP.ADJ_HEADER
                            SET RUN_STATUS = 'Pending Approval'
                            WHERE ADJ_ID = '{adj_id}'
                        """)
                        run_query(f"""
                            INSERT INTO ADJUSTMENT_APP.ADJ_STATUS_HISTORY
                                (ADJ_ID, OLD_STATUS, NEW_STATUS, CHANGED_BY, COMMENT)
                            VALUES ('{adj_id}', 'Pending', 'Pending Approval',
                                    '{user}', 'Submitted for approval')
                        """)
                        st.success("Submitted for approval.")
                        safe_rerun()
                    except Exception as ex:
                        st.error(str(ex))

        elif run_status == "Pending Approval":
            with act_cols[0]:
                if st.button("Recall to Pending", key=f"recall_{adj_id}", use_container_width=True):
                    try:
                        run_query(f"""
                            UPDATE ADJUSTMENT_APP.ADJ_HEADER
                            SET RUN_STATUS = 'Pending'
                            WHERE ADJ_ID = '{adj_id}'
                        """)
                        run_query(f"""
                            INSERT INTO ADJUSTMENT_APP.ADJ_STATUS_HISTORY
                                (ADJ_ID, OLD_STATUS, NEW_STATUS, CHANGED_BY, COMMENT)
                            VALUES ('{adj_id}', 'Pending Approval', 'Pending',
                                    '{user}', 'Recalled by submitter')
                        """)
                        st.success("Recalled to Pending.")
                        safe_rerun()
                    except Exception as ex:
                        st.error(str(ex))
        elif run_status == "Failed":
            with act_cols[0]:
                if st.button("Retry", key=f"retry_{adj_id}",
                             use_container_width=True, type="primary"):
                    try:
                        run_query(f"""
                            UPDATE ADJUSTMENT_APP.ADJ_HEADER
                            SET RUN_STATUS = 'Pending',
                                ERRORMESSAGE = NULL
                            WHERE ADJ_ID = '{adj_id}'
                        """)
                        run_query(f"""
                            INSERT INTO ADJUSTMENT_APP.ADJ_STATUS_HISTORY
                                (ADJ_ID, OLD_STATUS, NEW_STATUS, CHANGED_BY, COMMENT)
                            VALUES ('{adj_id}', 'Failed', 'Pending',
                                    '{user}', 'Retrying after failure')
                        """)
                        st.success("Queued for retry.")
                        safe_rerun()
                    except Exception as ex:
                        st.error(str(ex))


# ── Browse + act ───────────────────────────────────────────────────────────────

show_deleted = st.checkbox(
    "Show deleted", value=False,
    help="Include deleted adjustments. Hidden by default.")

if df_adjs.empty:
    view_df = df_adjs
else:
    is_del = df_adjs["IS_DELETED"].fillna(False).astype(bool)
    view_df = df_adjs if show_deleted else df_adjs[~is_del]

view_df = view_df.reset_index(drop=True)

total = len(df_adjs)
shown = len(view_df)
st.markdown(
    f"<span style='color:{P['grey_700']};font-size:0.82rem'>"
    f"Showing {shown} of {total} adjustments. Select one to view its details and actions."
    f"</span>",
    unsafe_allow_html=True)

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

if selected is not None:
    st.markdown("---")
    render_adj_card(selected, expanded=True)
