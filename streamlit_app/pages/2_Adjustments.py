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
    fmt_adj_id, icon,
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
    filter_type = st.multiselect(
        "Type", ["Flatten", "Scale", "Roll"],
        default=[], key="mw_type")
with f4:
    mine_only = st.checkbox("Only my adjustments", value=False,
                            help="When checked, shows only adjustments you submitted.")

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

# ──────────────────────────────────────────────────────────────────────────────
# STATUS TABS
# ──────────────────────────────────────────────────────────────────────────────

tab_labels = {
    "Pending":            ["Pending"],
    "Pending Approval":   ["Pending Approval"],
    "Approved":           ["Approved"],
    "Processed":          ["Processed"],
    "Errors / Rejected":  ["Failed", "Rejected", "Rejected - SignedOff"],
    "Deleted":            None,   # None = filter by IS_DELETED, not by status
}

def _tab_df(label, statuses):
    if df_adjs.empty:
        return pd.DataFrame()
    is_del = df_adjs["IS_DELETED"].fillna(False).astype(bool)
    if statuses is None:                          # Deleted tab
        return df_adjs[is_del]
    return df_adjs[df_adjs["RUN_STATUS"].isin(statuses) & ~is_del]

counts    = {lbl: len(_tab_df(lbl, st)) for lbl, st in tab_labels.items()}
tab_names = [f"{lbl} ({counts[lbl]})" for lbl in tab_labels]
tabs      = st.tabs(tab_names)


def render_adj_card(row):
    """Render one adjustment as an expandable card."""
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
        expanded=False,
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


# ── Render tabs ────────────────────────────────────────────────────────────────

for tab, (label, statuses) in zip(tabs, tab_labels.items()):
    with tab:
        tab_adjs = _tab_df(label, statuses)

        if tab_adjs.empty:
            st.markdown(
                f'<div class="mcard" style="text-align:center;padding:2.5rem;color:{P["grey_700"]}">'
                f'<div>{icon("inbox", size=28, color=P["grey_400"], valign="0")}</div>'
                f'<div style="font-size:0.9rem;margin-top:0.5rem">No adjustments in this category</div>'
                f'</div>',
                unsafe_allow_html=True)
            continue

        # Quick summary row by scope
        scopes_in_tab = tab_adjs["PROCESS_TYPE"].unique()
        summary_html = '<div style="display:flex;gap:16px;margin-bottom:1rem;flex-wrap:wrap">'
        for scope_key in scopes_in_tab:
            cnt = int(tab_adjs[tab_adjs["PROCESS_TYPE"] == scope_key].shape[0])
            cfg = SCOPE_CONFIG.get(scope_key, {})
            summary_html += (
                f'<div style="background:{cfg.get("bg", P["grey_100"])};border-radius:6px;'
                f'padding:4px 10px;font-size:0.78rem;font-weight:600;color:{cfg.get("color", P["grey_700"])}">'
                f'{cfg.get("icon", "")} {cfg.get("label", scope_key)} · {cnt}</div>')
        summary_html += '</div>'
        st.markdown(summary_html, unsafe_allow_html=True)

        for _, row in tab_adjs.iterrows():
            render_adj_card(row.to_dict())
