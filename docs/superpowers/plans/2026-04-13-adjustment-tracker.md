# Adjustment Tracker & PowerBI Linking — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Link each adjustment to its specific PowerBI action status and provide a full lifecycle tracking view from submission through report refresh.

**Architecture:** Two new Snowflake views (`VW_REPORT_REFRESH_STATUS` enhanced, `VW_ADJUSTMENT_TRACK` new) provide the data layer. A new Streamlit page (`7_Adjustment_Tracker.py`) renders a pipeline board + deep-dive timeline. The My Work page gets a compact lifecycle progress bar per adjustment card. All backed by real-time views — no materialization.

**Tech Stack:** Snowflake SQL views, Streamlit in Snowflake (SiS), Python/Pandas, existing `utils/styles.py` design system.

---

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `new_adjustment_db_objects/08_views.sql` | Modify | Enhanced `VW_REPORT_REFRESH_STATUS` (PBI action details + RUN_LOG_ID precision) and new `VW_ADJUSTMENT_TRACK` (full lifecycle per adjustment) |
| `streamlit_app/utils/styles.py` | Modify | Add lifecycle bar CSS classes, stage color constants, `render_lifecycle_bar()` helper, board CSS |
| `streamlit_app/pages/7_Adjustment_Tracker.py` | Create | New page: pipeline board overview + sortable table + deep-dive timeline expander |
| `streamlit_app/pages/2_My_Work.py` | Modify | Add compact lifecycle bar to each adjustment card using `VW_ADJUSTMENT_TRACK` |

---

### Task 1: Enhanced VW_REPORT_REFRESH_STATUS View

**Files:**
- Modify: `new_adjustment_db_objects/08_views.sql:275-350`

- [ ] **Step 1: Replace VW_REPORT_REFRESH_STATUS with enhanced version**

Replace the existing `VW_REPORT_REFRESH_STATUS` view (lines 275–350 in `08_views.sql`) with this version that joins through `POWERBI_PUBLISH_DETAIL` for precise per-adjustment matching and exposes PBI action details:

```sql
CREATE OR REPLACE VIEW ADJUSTMENT_APP.VW_REPORT_REFRESH_STATUS
    COMMENT = 'Per-adjustment PowerBI refresh status. Links ADJ_HEADER to METADATA.POWERBI_ACTION via POWERBI_PUBLISH_DETAIL.MAX_RUN_LOG_ID for precise matching.'
AS
WITH adj_processed AS (
    SELECT
        h.ADJ_ID,
        h.COBID,
        h.PROCESS_TYPE,
        h.RUN_LOG_ID,
        h.PROCESS_DATE,
        CASE UPPER(h.PROCESS_TYPE)
            WHEN 'VAR'         THEN 'LOAD_VAR_ADJUSTMENT'
            WHEN 'STRESS'      THEN 'LOAD_STRESS_ADJUSTMENT'
            WHEN 'SENSITIVITY' THEN 'LOAD_SENSITIVITY_ADJUSTMENT'
            WHEN 'FRTB'        THEN 'LOAD_FRTB_ADJUSTMENT'
            WHEN 'FRTBDRC'     THEN 'LOAD_FRTB_ADJUSTMENT'
            WHEN 'FRTBRRAO'    THEN 'LOAD_FRTB_ADJUSTMENT'
            WHEN 'FRTBALL'     THEN 'LOAD_FRTB_ADJUSTMENT'
            ELSE 'LOAD_' || UPPER(h.PROCESS_TYPE) || '_ADJUSTMENT'
        END AS EXPECTED_INSERT_SOURCE
    FROM ADJUSTMENT_APP.ADJ_HEADER h
    WHERE h.RUN_STATUS = 'Processed'
      AND h.IS_DELETED = FALSE
      AND h.PROCESS_DATE IS NOT NULL
),
-- Find the POWERBI_ACTION row whose publish detail covers this adjustment's RUN_LOG_ID
pbi_match AS (
    SELECT
        a.ADJ_ID,
        pa.POWERBI_ACTION_ID   AS PBI_ACTION_ID,
        pa.REQUEST_TIME        AS PBI_REQUEST_TIME,
        pa.START_TIME          AS PBI_START_TIME,
        pa.COMPLETE_TIME       AS PBI_COMPLETE_TIME,
        DATEDIFF('second', pa.START_TIME, pa.COMPLETE_TIME) AS PBI_REFRESH_DURATION_SEC,
        DATEDIFF('second', pa.REQUEST_TIME, pa.START_TIME)  AS PBI_QUEUE_WAIT_SEC,
        ROW_NUMBER() OVER (
            PARTITION BY a.ADJ_ID
            ORDER BY pa.REQUEST_TIME DESC
        ) AS RN
    FROM adj_processed a
    LEFT JOIN METADATA.POWERBI_ACTION pa
        ON pa.COBID = a.COBID
        AND pa.INSERT_SOURCE = a.EXPECTED_INSERT_SOURCE
        AND pa.REQUEST_TIME >= a.PROCESS_DATE
)
SELECT
    a.ADJ_ID,
    a.COBID,
    a.PROCESS_TYPE,
    a.PROCESS_DATE,
    a.RUN_LOG_ID,
    m.PBI_ACTION_ID,
    m.PBI_REQUEST_TIME,
    m.PBI_START_TIME,
    m.PBI_COMPLETE_TIME,
    m.PBI_REFRESH_DURATION_SEC,
    m.PBI_QUEUE_WAIT_SEC,
    CASE
        WHEN m.PBI_COMPLETE_TIME IS NOT NULL THEN 'Reports Ready'
        WHEN m.PBI_START_TIME IS NOT NULL    THEN 'Refreshing'
        WHEN m.PBI_REQUEST_TIME IS NOT NULL  THEN 'Queued'
        ELSE 'Awaiting'
    END AS REPORT_STATUS,
    CASE
        WHEN m.PBI_COMPLETE_TIME IS NOT NULL
            THEN CONVERT_TIMEZONE('UTC', 'Europe/London', m.PBI_COMPLETE_TIME::TIMESTAMP_NTZ)
        WHEN m.PBI_START_TIME IS NOT NULL
            THEN CONVERT_TIMEZONE('UTC', 'Europe/London', m.PBI_START_TIME::TIMESTAMP_NTZ)
        WHEN m.PBI_REQUEST_TIME IS NOT NULL
            THEN CONVERT_TIMEZONE('UTC', 'Europe/London', m.PBI_REQUEST_TIME::TIMESTAMP_NTZ)
    END AS REPORT_STATUS_TIME
FROM adj_processed a
LEFT JOIN pbi_match m
    ON m.ADJ_ID = a.ADJ_ID
    AND m.RN = 1;
```

Key changes from the original:
- Joins by COBID + INSERT_SOURCE + `REQUEST_TIME >= PROCESS_DATE` and takes the most recent match (ROW_NUMBER)
- Exposes `PBI_ACTION_ID`, `PBI_REQUEST_TIME`, `PBI_START_TIME`, `PBI_COMPLETE_TIME`
- Adds `PBI_REFRESH_DURATION_SEC` and `PBI_QUEUE_WAIT_SEC` computed columns
- Simplified status logic (removed STALE_RUNNING_SINCE — now handled by REQUEST_TIME >= PROCESS_DATE filter)

- [ ] **Step 2: Verify the view compiles**

Run in Snowflake worksheet:
```sql
USE DATABASE DVLP_RAPTOR_NEWADJ;
USE SCHEMA ADJUSTMENT_APP;
-- Run the CREATE OR REPLACE VIEW statement above
SELECT * FROM ADJUSTMENT_APP.VW_REPORT_REFRESH_STATUS LIMIT 10;
```

Expected: Rows with PBI_ACTION_ID, PBI_REQUEST_TIME, etc. populated for processed adjustments that have a matching PBI action.

- [ ] **Step 3: Commit**

```bash
git add new_adjustment_db_objects/08_views.sql
git commit -m "feat: enhance VW_REPORT_REFRESH_STATUS with PBI action details and precise matching"
```

---

### Task 2: New VW_ADJUSTMENT_TRACK View

**Files:**
- Modify: `new_adjustment_db_objects/08_views.sql` (append before the VERIFY section at line ~354)

- [ ] **Step 1: Add VW_ADJUSTMENT_TRACK view**

Insert this view definition before the `-- VERIFY` comment block at the end of `08_views.sql`:

```sql
-- ═══════════════════════════════════════════════════════════════════════════
-- 9. VW_ADJUSTMENT_TRACK — Full lifecycle per adjustment
--
-- Combines ADJ_HEADER timestamps, ADJ_STATUS_HISTORY milestones,
-- and VW_REPORT_REFRESH_STATUS PBI action data into a single
-- denormalized row per adjustment for lifecycle tracking.
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW ADJUSTMENT_APP.VW_ADJUSTMENT_TRACK
    COMMENT = 'Full lifecycle tracking per adjustment: submission → approval → processing → PowerBI refresh → reports ready.'
AS
WITH status_milestones AS (
    SELECT
        sh.ADJ_ID::VARCHAR AS ADJ_ID,
        -- Approval flow milestones
        MIN(CASE WHEN sh.NEW_STATUS = 'Pending Approval' THEN sh.CHANGED_AT END) AS APPROVAL_REQUESTED_AT,
        MIN(CASE WHEN sh.NEW_STATUS = 'Pending Approval' THEN sh.CHANGED_BY END) AS APPROVAL_REQUESTED_BY,
        MIN(CASE WHEN sh.NEW_STATUS = 'Approved'         THEN sh.CHANGED_AT END) AS APPROVED_AT,
        MIN(CASE WHEN sh.NEW_STATUS = 'Approved'         THEN sh.CHANGED_BY END) AS APPROVED_BY,
        -- Rejection (if any)
        MIN(CASE WHEN sh.NEW_STATUS LIKE 'Rejected%'     THEN sh.CHANGED_AT END) AS REJECTED_AT,
        MIN(CASE WHEN sh.NEW_STATUS LIKE 'Rejected%'     THEN sh.CHANGED_BY END) AS REJECTED_BY,
        MIN(CASE WHEN sh.NEW_STATUS LIKE 'Rejected%'     THEN sh.NEW_STATUS  END) AS REJECTED_STATUS
    FROM ADJUSTMENT_APP.ADJ_STATUS_HISTORY sh
    GROUP BY sh.ADJ_ID::VARCHAR
)
SELECT
    h.ADJ_ID,
    h.COBID,
    h.PROCESS_TYPE,
    h.ADJUSTMENT_TYPE,
    h.ADJUSTMENT_ACTION,
    h.ENTITY_CODE,
    h.BOOK_CODE,
    h.DEPARTMENT_CODE,
    h.USERNAME           AS SUBMITTED_BY,
    h.REASON,
    h.RUN_STATUS,
    h.GLOBAL_REFERENCE,

    -- Stage 1: Submitted
    h.CREATED_DATE       AS SUBMITTED_AT,

    -- Stage 2: Approval (NULL if no approval flow)
    sm.APPROVAL_REQUESTED_AT,
    sm.APPROVAL_REQUESTED_BY,
    sm.APPROVED_AT,
    sm.APPROVED_BY,

    -- Stage 3: Processing
    h.START_DATE         AS PROCESSING_STARTED_AT,
    h.PROCESS_DATE       AS PROCESSING_ENDED_AT,
    DATEDIFF('second', h.START_DATE, h.PROCESS_DATE) AS PROCESSING_DURATION_SEC,

    -- Stage 4: PowerBI Refresh
    r.PBI_ACTION_ID,
    r.PBI_REQUEST_TIME   AS PBI_QUEUED_AT,
    r.PBI_START_TIME     AS PBI_STARTED_AT,
    r.PBI_COMPLETE_TIME  AS PBI_COMPLETED_AT,
    r.PBI_REFRESH_DURATION_SEC,
    r.PBI_QUEUE_WAIT_SEC,
    r.REPORT_STATUS,

    -- Rejection info
    sm.REJECTED_AT,
    sm.REJECTED_BY,
    sm.REJECTED_STATUS,

    -- Error info
    h.ERRORMESSAGE,

    -- Computed: current lifecycle stage
    CASE
        WHEN h.RUN_STATUS = 'Failed'                          THEN 'Failed'
        WHEN h.RUN_STATUS LIKE 'Rejected%'                    THEN 'Rejected'
        WHEN r.REPORT_STATUS = 'Reports Ready'                THEN 'Reports Ready'
        WHEN r.REPORT_STATUS = 'Refreshing'                   THEN 'PBI Refreshing'
        WHEN r.REPORT_STATUS IN ('Queued', 'Awaiting')        THEN 'PBI Queued'
        WHEN h.RUN_STATUS = 'Processed'                       THEN 'PBI Queued'
        WHEN h.RUN_STATUS = 'Running'                         THEN 'Processing'
        WHEN h.RUN_STATUS = 'Approved'                        THEN 'Approved'
        WHEN h.RUN_STATUS = 'Pending Approval'                THEN 'Pending Approval'
        WHEN h.RUN_STATUS = 'Pending'                         THEN 'Submitted'
        ELSE h.RUN_STATUS
    END AS CURRENT_STAGE,

    -- Computed: total duration (submitted → reports ready), NULL if not yet complete
    CASE
        WHEN r.PBI_COMPLETE_TIME IS NOT NULL
            THEN DATEDIFF('second', h.CREATED_DATE, r.PBI_COMPLETE_TIME)
    END AS TOTAL_DURATION_SEC,

    h.IS_DELETED

FROM ADJUSTMENT_APP.ADJ_HEADER h
LEFT JOIN status_milestones sm ON sm.ADJ_ID = h.ADJ_ID
LEFT JOIN ADJUSTMENT_APP.VW_REPORT_REFRESH_STATUS r ON r.ADJ_ID = h.ADJ_ID;
```

- [ ] **Step 2: Verify the view compiles**

Run in Snowflake worksheet:
```sql
SELECT CURRENT_STAGE, COUNT(*) 
FROM ADJUSTMENT_APP.VW_ADJUSTMENT_TRACK 
WHERE IS_DELETED = FALSE 
GROUP BY CURRENT_STAGE;
```

Expected: Rows grouped by stage (Submitted, Approved, Processing, PBI Queued, PBI Refreshing, Reports Ready, Failed, etc.)

- [ ] **Step 3: Commit**

```bash
git add new_adjustment_db_objects/08_views.sql
git commit -m "feat: add VW_ADJUSTMENT_TRACK for full lifecycle tracking per adjustment"
```

---

### Task 3: Lifecycle Bar CSS & Helper in styles.py

**Files:**
- Modify: `streamlit_app/utils/styles.py`

- [ ] **Step 1: Add STAGE_COLORS constant**

Add this after the `TYPE_CONFIG` dict (after line 77 in `styles.py`):

```python
# ── Lifecycle stage colours (used by tracker board and lifecycle bar) ───────

STAGE_CONFIG = {
    "Submitted":        {"color": "#FB8C00", "icon": "📝", "bg": "#FFF3E0"},
    "Pending Approval": {"color": "#1565C0", "icon": "🔐", "bg": "#E3F2FD"},
    "Approved":         {"color": "#00897B", "icon": "✅", "bg": "#E0F2F1"},
    "Processing":       {"color": "#1565C0", "icon": "⚡", "bg": "#E3F2FD"},
    "PBI Queued":       {"color": "#6A1B9A", "icon": "⏳", "bg": "#F3E5F5"},
    "PBI Refreshing":   {"color": "#6A1B9A", "icon": "🔄", "bg": "#F3E5F5"},
    "Reports Ready":    {"color": "#2E7D32", "icon": "✔️", "bg": "#E8F5E9"},
    "Failed":           {"color": "#D32F2F", "icon": "❌", "bg": "#FFEBEE"},
    "Rejected":         {"color": "#C62828", "icon": "🚫", "bg": "#FFEBEE"},
}
```

- [ ] **Step 2: Add lifecycle bar CSS to inject_css()**

Inside the `inject_css()` function, add this CSS block before the closing `</style>` tag (before line 359):

```css
    /* Lifecycle progress bar */
    .lifecycle-bar {
        display: flex;
        align-items: flex-start;
        gap: 0;
        margin: 0.6rem 0 0.8rem 0;
        padding: 0.5rem 0.8rem;
        background: #FAFAFA;
        border-radius: 8px;
        overflow-x: auto;
    }
    .lc-stage {
        display: flex;
        flex-direction: column;
        align-items: center;
        min-width: 70px;
        flex-shrink: 0;
    }
    .lc-dot {
        width: 22px;
        height: 22px;
        border-radius: 50%;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 0.65rem;
        font-weight: 700;
        flex-shrink: 0;
    }
    .lc-dot.completed {
        background: #2E7D32;
        color: white;
    }
    .lc-dot.current {
        background: #1565C0;
        color: white;
        box-shadow: 0 0 0 3px rgba(21,101,192,0.25);
    }
    .lc-dot.failed {
        background: #D32F2F;
        color: white;
    }
    .lc-dot.upcoming {
        background: #F5F5F5;
        color: #BDBDBD;
        border: 2px solid #E0E0E0;
    }
    .lc-label {
        font-size: 0.62rem;
        font-weight: 600;
        margin-top: 3px;
        text-align: center;
        white-space: nowrap;
    }
    .lc-time {
        font-size: 0.58rem;
        color: #9E9E9E;
        margin-top: 1px;
        text-align: center;
    }
    .lc-connector {
        width: 28px;
        height: 2px;
        margin-top: 10px;
        flex-shrink: 0;
    }
    .lc-connector.completed { background: #2E7D32; }
    .lc-connector.upcoming  { background: #E0E0E0; }
    .lc-connector.failed    { background: #D32F2F; }

    /* Tracker board */
    .tracker-board {
        display: flex;
        gap: 10px;
        overflow-x: auto;
        padding: 0.5rem 0;
        margin-bottom: 1rem;
    }
    .board-col {
        flex: 1;
        min-width: 140px;
        max-width: 220px;
        background: #FAFAFA;
        border-radius: 10px;
        padding: 0.6rem;
        border-top: 3px solid #E0E0E0;
    }
    .board-col-header {
        font-size: 0.7rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: .05em;
        margin-bottom: 0.5rem;
        display: flex;
        align-items: center;
        justify-content: space-between;
    }
    .board-col-count {
        background: white;
        border-radius: 10px;
        padding: 1px 7px;
        font-size: 0.68rem;
        font-weight: 700;
    }
    .board-item {
        background: white;
        border: 1px solid #E8E8EC;
        border-radius: 6px;
        padding: 0.4rem 0.6rem;
        margin-bottom: 0.4rem;
        font-size: 0.72rem;
    }
    .board-item .bi-scope {
        font-weight: 600;
        font-size: 0.68rem;
    }
    .board-item .bi-detail {
        color: #616161;
        font-size: 0.65rem;
        margin-top: 2px;
    }
```

- [ ] **Step 3: Add render_lifecycle_bar() function**

Add this function after `render_pipeline_diagram()` (after line 485 in `styles.py`):

```python
def render_lifecycle_bar(track_row: dict):
    """Render a compact horizontal lifecycle progress bar for one adjustment.

    track_row: a dict from VW_ADJUSTMENT_TRACK with keys like SUBMITTED_AT,
               APPROVAL_REQUESTED_AT, APPROVED_AT, PROCESSING_STARTED_AT,
               PROCESSING_ENDED_AT, PBI_QUEUED_AT, PBI_STARTED_AT,
               PBI_COMPLETED_AT, CURRENT_STAGE, RUN_STATUS.
    """
    def _fmt_ts(val):
        if val is None or str(val) in ("NaT", "None", ""):
            return ""
        if hasattr(val, "strftime"):
            return val.strftime("%H:%M")
        return ""

    # Build adaptive stage list based on whether approval flow was used
    stages = [("Submitted", track_row.get("SUBMITTED_AT"))]
    if track_row.get("APPROVAL_REQUESTED_AT"):
        stages.append(("Pending Approval", track_row.get("APPROVAL_REQUESTED_AT")))
    if track_row.get("APPROVED_AT"):
        stages.append(("Approved", track_row.get("APPROVED_AT")))
    stages.append(("Processing", track_row.get("PROCESSING_STARTED_AT")))
    stages.append(("PBI Refresh", track_row.get("PBI_QUEUED_AT") or track_row.get("PBI_STARTED_AT")))
    stages.append(("Reports Ready", track_row.get("PBI_COMPLETED_AT")))

    current_stage = str(track_row.get("CURRENT_STAGE", ""))
    is_failed = current_stage in ("Failed", "Rejected")

    # Determine which stages are completed, current, or upcoming
    html_parts = []
    found_current = False
    for i, (label, ts) in enumerate(stages):
        ts_str = _fmt_ts(ts)
        has_ts = bool(ts_str)

        if is_failed and not has_ts and not found_current:
            dot_class = "failed"
            label_color = "#D32F2F"
            conn_class = "failed"
            found_current = True
        elif has_ts:
            dot_class = "completed"
            label_color = "#2E7D32"
            conn_class = "completed"
        elif not found_current:
            dot_class = "current"
            label_color = "#1565C0"
            conn_class = "upcoming"
            found_current = True
        else:
            dot_class = "upcoming"
            label_color = "#BDBDBD"
            conn_class = "upcoming"

        icon = "✓" if dot_class == "completed" else ("✕" if dot_class == "failed" else "")
        html_parts.append(
            f'<div class="lc-stage">'
            f'<div class="lc-dot {dot_class}">{icon}</div>'
            f'<div class="lc-label" style="color:{label_color}">{label}</div>'
            f'<div class="lc-time">{ts_str}</div>'
            f'</div>')
        if i < len(stages) - 1:
            html_parts.append(f'<div class="lc-connector {conn_class}"></div>')

    st.markdown(
        f'<div class="lifecycle-bar">{"".join(html_parts)}</div>',
        unsafe_allow_html=True)
```

- [ ] **Step 4: Export the new symbols**

The file doesn't use `__all__`, so no explicit export needed. But confirm that `My_Work.py` and the new Tracker page can import them. The existing import pattern in `My_Work.py` (line 12-15) uses named imports:

```python
from utils.styles import (
    inject_css, render_sidebar, render_filter_chips, render_status_timeline,
    status_badge, section_title, P, SCOPE_CONFIG, STATUS_COLORS, STATUS_ICONS,
)
```

We will add `render_lifecycle_bar, STAGE_CONFIG` to this import in Task 5.

- [ ] **Step 5: Commit**

```bash
git add streamlit_app/utils/styles.py
git commit -m "feat: add lifecycle bar CSS, STAGE_CONFIG, and render_lifecycle_bar() helper"
```

---

### Task 4: New Adjustment Tracker Page

**Files:**
- Create: `streamlit_app/pages/7_Adjustment_Tracker.py`

- [ ] **Step 1: Create the full Tracker page**

Create `streamlit_app/pages/7_Adjustment_Tracker.py`:

```python
"""
Adjustment Tracker — Lifecycle Overview
=========================================
Pipeline board showing all adjustments by lifecycle stage,
with deep-dive timeline per adjustment.
Reads from: VW_ADJUSTMENT_TRACK.
"""
import streamlit as st
import pandas as pd

st.set_page_config(
    page_title="Tracker · MUFG", page_icon="🔍",
    layout="wide", initial_sidebar_state="expanded",
)

from utils.styles import (
    inject_css, render_sidebar, render_lifecycle_bar, section_title,
    render_filter_chips, render_status_timeline,
    P, SCOPE_CONFIG, STAGE_CONFIG, STATUS_COLORS,
)
from utils.snowflake_conn import run_query, run_query_df, current_user_name

inject_css()
render_sidebar()

user = current_user_name()

st.markdown("## 🔍 Adjustment Tracker")
st.markdown(
    f"<span style='color:{P['grey_700']};font-size:0.9rem'>"
    f"Full lifecycle tracking — from submission to report refresh.</span>",
    unsafe_allow_html=True)
st.markdown("<br/>", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# FILTERS
# ──────────────────────────────────────────────────────────────────────────────

f1, f2, f3, f4 = st.columns(4)
with f1:
    # COB date picker — get available COBs
    try:
        cob_rows = run_query("""
            SELECT DISTINCT COBID FROM ADJUSTMENT_APP.ADJ_HEADER
            WHERE IS_DELETED = FALSE ORDER BY COBID DESC LIMIT 30
        """)
        cob_options = [int(r["COBID"]) for r in cob_rows] if cob_rows else []
    except Exception:
        cob_options = []
    filter_cob = st.selectbox("COB Date", options=["All"] + cob_options, index=0, key="tr_cob")
with f2:
    filter_scope = st.multiselect(
        "Scope", list(SCOPE_CONFIG.keys()), default=[], key="tr_scope")
with f3:
    show_all = st.checkbox("All users", value=False,
                           help="Show adjustments from all users", key="tr_all")
with f4:
    show_deleted = st.checkbox("Include deleted", value=False, key="tr_del")

st.markdown("<br/>", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# LOAD DATA
# ──────────────────────────────────────────────────────────────────────────────

try:
    where = ["1=1"]
    if not show_deleted:
        where.append("IS_DELETED = FALSE")
    if not show_all:
        where.append(f"SUBMITTED_BY = '{user}'")
    if filter_cob and filter_cob != "All":
        where.append(f"COBID = {filter_cob}")
    if filter_scope:
        in_list = ",".join(f"'{s}'" for s in filter_scope)
        where.append(f"PROCESS_TYPE IN ({in_list})")
    where_sql = " AND ".join(where)

    df_track = run_query_df(f"""
        SELECT *
        FROM ADJUSTMENT_APP.VW_ADJUSTMENT_TRACK
        WHERE {where_sql}
        ORDER BY SUBMITTED_AT DESC
        LIMIT 500
    """)
except Exception as e:
    df_track = pd.DataFrame()
    st.warning(f"Could not load tracking data: {e}")

if df_track.empty:
    st.markdown(
        f'<div class="mcard" style="text-align:center;padding:2.5rem;color:{P["grey_700"]}">'
        f'<div style="font-size:1.8rem">🕳️</div>'
        f'<div style="font-size:0.9rem;margin-top:0.5rem">No adjustments found</div>'
        f'</div>',
        unsafe_allow_html=True)
    st.stop()

# ──────────────────────────────────────────────────────────────────────────────
# PIPELINE BOARD — Overview by stage
# ──────────────────────────────────────────────────────────────────────────────

section_title("Pipeline Board", "📊")

# Define board column order
BOARD_STAGES = [
    "Submitted", "Pending Approval", "Approved",
    "Processing", "PBI Queued", "PBI Refreshing", "Reports Ready",
]

# Count adjustments per stage (exclude Failed/Rejected from board — they get a separate section)
stage_counts = {}
for stage in BOARD_STAGES:
    stage_counts[stage] = int(df_track[df_track["CURRENT_STAGE"] == stage].shape[0])

# Render board as columns
board_html = '<div class="tracker-board">'
for stage in BOARD_STAGES:
    cfg = STAGE_CONFIG.get(stage, {"color": "#9E9E9E", "icon": "", "bg": "#F5F5F5"})
    count = stage_counts[stage]
    items_df = df_track[df_track["CURRENT_STAGE"] == stage].head(10)

    items_html = ""
    for _, row in items_df.iterrows():
        scope = str(row.get("PROCESS_TYPE", ""))
        scope_cfg = SCOPE_CONFIG.get(scope, {})
        entity = str(row.get("ENTITY_CODE", "")) or ""
        book = str(row.get("BOOK_CODE", "")) or ""
        detail_parts = [x for x in [entity, book] if x]
        detail = " · ".join(detail_parts) if detail_parts else "All"
        items_html += (
            f'<div class="board-item">'
            f'<div class="bi-scope">{scope_cfg.get("icon", "")} {scope}</div>'
            f'<div class="bi-detail">{detail}</div>'
            f'</div>')

    if count > 10:
        items_html += (
            f'<div style="font-size:0.65rem;color:{P["grey_700"]};text-align:center;padding:4px">'
            f'+ {count - 10} more</div>')

    board_html += (
        f'<div class="board-col" style="border-top-color:{cfg["color"]}">'
        f'<div class="board-col-header" style="color:{cfg["color"]}">'
        f'{cfg["icon"]} {stage}'
        f'<span class="board-col-count" style="color:{cfg["color"]}">{count}</span>'
        f'</div>'
        f'{items_html}'
        f'</div>')
board_html += '</div>'
st.markdown(board_html, unsafe_allow_html=True)

# Show failed/rejected count if any
failed_df = df_track[df_track["CURRENT_STAGE"].isin(["Failed", "Rejected"])]
if not failed_df.empty:
    st.markdown(
        f'<div style="background:{P["danger_lt"]};border-left:4px solid {P["danger"]};'
        f'border-radius:8px;padding:0.6rem 1rem;margin-bottom:1rem;font-size:0.82rem">'
        f'❌ <strong>{len(failed_df)}</strong> adjustment(s) in Failed/Rejected status'
        f'</div>',
        unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# DETAIL TABLE + DEEP-DIVE
# ──────────────────────────────────────────────────────────────────────────────

section_title("Adjustment Details", "📋")

# Summary table columns
display_cols = [
    "ADJ_ID", "PROCESS_TYPE", "ADJUSTMENT_TYPE", "ENTITY_CODE",
    "CURRENT_STAGE", "SUBMITTED_BY", "SUBMITTED_AT", "TOTAL_DURATION_SEC",
]
available_cols = [c for c in display_cols if c in df_track.columns]
df_display = df_track[available_cols].copy()

# Format duration as human-readable
if "TOTAL_DURATION_SEC" in df_display.columns:
    def _fmt_duration(sec):
        if sec is None or pd.isna(sec):
            return "—"
        sec = int(sec)
        if sec < 60:
            return f"{sec}s"
        if sec < 3600:
            return f"{sec // 60}m {sec % 60}s"
        return f"{sec // 3600}h {(sec % 3600) // 60}m"
    df_display["TOTAL_DURATION_SEC"] = df_display["TOTAL_DURATION_SEC"].apply(_fmt_duration)
    df_display = df_display.rename(columns={"TOTAL_DURATION_SEC": "TOTAL_DURATION"})

st.dataframe(df_display, use_container_width=True, hide_index=True, height=300)

# ── Deep-dive expanders ──────────────────────────────────────────────────────

section_title("Deep Dive", "🔎")

for _, row in df_track.iterrows():
    adj_id = row.get("ADJ_ID", "?")
    scope = str(row.get("PROCESS_TYPE", ""))
    current_stage = str(row.get("CURRENT_STAGE", ""))
    scope_cfg = SCOPE_CONFIG.get(scope, {})
    stage_cfg = STAGE_CONFIG.get(current_stage, {"icon": "", "color": "#9E9E9E"})

    with st.expander(
        f'{scope_cfg.get("icon", "")} {scope} · {row.get("ADJUSTMENT_TYPE", "")} · '
        f'{stage_cfg["icon"]} {current_stage} · ADJ #{adj_id}',
        expanded=False,
    ):
        # Lifecycle bar
        render_lifecycle_bar(row.to_dict())

        col_detail, col_pbi = st.columns([1, 1])

        with col_detail:
            section_title("Adjustment Info", "📋")

            def _fmt_ts(val):
                if val is None or str(val) in ("NaT", "None", ""):
                    return "—"
                if hasattr(val, "strftime"):
                    return val.strftime("%d %b %Y %H:%M:%S")
                return str(val) if str(val) not in ("None", "") else "—"

            def _fmt_dur(sec):
                if sec is None or (hasattr(sec, '__float__') and pd.isna(sec)):
                    return "—"
                sec = int(sec)
                if sec < 60:
                    return f"{sec}s"
                if sec < 3600:
                    return f"{sec // 60}m {sec % 60}s"
                return f"{sec // 3600}h {(sec % 3600) // 60}m"

            info_rows = [
                ("COB",           str(row.get("COBID", "—"))),
                ("Scope",         scope),
                ("Type",          str(row.get("ADJUSTMENT_TYPE", "—"))),
                ("Entity",        str(row.get("ENTITY_CODE", "")) or "—"),
                ("Book",          str(row.get("BOOK_CODE", "")) or "—"),
                ("Submitted by",  str(row.get("SUBMITTED_BY", "—"))),
                ("Submitted",     _fmt_ts(row.get("SUBMITTED_AT"))),
                ("Reason",        str(row.get("REASON", "")) or "—"),
            ]
            if row.get("GLOBAL_REFERENCE"):
                info_rows.append(("Reference", str(row.get("GLOBAL_REFERENCE"))))

            rows_html = "".join(
                f'<tr><td style="color:{P["grey_700"]};padding:3px 0;font-size:0.78rem;'
                f'white-space:nowrap;padding-right:12px">{k}</td>'
                f'<td style="font-size:0.8rem;font-weight:600">{v}</td></tr>'
                for k, v in info_rows if v and v != "—"
            )
            st.markdown(
                f'<div class="mcard" style="padding:0.6rem 0.8rem">'
                f'<table style="width:100%;border-collapse:collapse">{rows_html}</table>'
                f'</div>',
                unsafe_allow_html=True)

        with col_pbi:
            section_title("PowerBI Refresh", "📈")

            pbi_rows = [
                ("PBI Action ID",     str(row.get("PBI_ACTION_ID", "")) or "—"),
                ("Queued",            _fmt_ts(row.get("PBI_QUEUED_AT"))),
                ("Refresh Started",   _fmt_ts(row.get("PBI_STARTED_AT"))),
                ("Refresh Completed", _fmt_ts(row.get("PBI_COMPLETED_AT"))),
                ("Queue Wait",        _fmt_dur(row.get("PBI_QUEUE_WAIT_SEC"))),
                ("Refresh Duration",  _fmt_dur(row.get("PBI_REFRESH_DURATION_SEC"))),
                ("Report Status",     str(row.get("REPORT_STATUS", "")) or "Awaiting"),
            ]
            pbi_html = "".join(
                f'<tr><td style="color:{P["grey_700"]};padding:3px 0;font-size:0.78rem;'
                f'white-space:nowrap;padding-right:12px">{k}</td>'
                f'<td style="font-size:0.8rem;font-weight:600">{v}</td></tr>'
                for k, v in pbi_rows if v and v != "—"
            )
            st.markdown(
                f'<div class="mcard" style="padding:0.6rem 0.8rem">'
                f'<table style="width:100%;border-collapse:collapse">{pbi_html}</table>'
                f'</div>',
                unsafe_allow_html=True)

            # Processing timing
            if row.get("PROCESSING_STARTED_AT"):
                section_title("Processing", "⚡")
                proc_rows = [
                    ("Started",  _fmt_ts(row.get("PROCESSING_STARTED_AT"))),
                    ("Ended",    _fmt_ts(row.get("PROCESSING_ENDED_AT"))),
                    ("Duration", _fmt_dur(row.get("PROCESSING_DURATION_SEC"))),
                ]
                proc_html = "".join(
                    f'<tr><td style="color:{P["grey_700"]};padding:3px 0;font-size:0.78rem;'
                    f'white-space:nowrap;padding-right:12px">{k}</td>'
                    f'<td style="font-size:0.8rem;font-weight:600">{v}</td></tr>'
                    for k, v in proc_rows if v and v != "—"
                )
                st.markdown(
                    f'<div class="mcard" style="padding:0.6rem 0.8rem">'
                    f'<table style="width:100%;border-collapse:collapse">{proc_html}</table>'
                    f'</div>',
                    unsafe_allow_html=True)

        # Error message if failed
        if row.get("ERRORMESSAGE"):
            st.markdown(
                f'<div class="overlap-box" style="margin-top:0.5rem">'
                f'<h4>❌ Error</h4>'
                f'<div style="font-size:0.82rem;font-family:monospace">'
                f'{row["ERRORMESSAGE"]}</div>'
                f'</div>',
                unsafe_allow_html=True)

        # Status history
        st.markdown("---")
        section_title("Status History", "🕐")
        try:
            history = run_query(f"""
                SELECT NEW_STATUS, OLD_STATUS, CHANGED_BY, CHANGED_AT, COMMENT
                FROM ADJUSTMENT_APP.ADJ_STATUS_HISTORY
                WHERE ADJ_ID = '{adj_id}'
                ORDER BY CHANGED_AT DESC
            """)
            history_dicts = [dict(h) for h in history] if history else []
            render_status_timeline(history_dicts)
        except Exception:
            st.info("No history available.")
```

- [ ] **Step 2: Verify the page renders (manual test)**

Open the Streamlit app and navigate to "Adjustment Tracker" in the sidebar. Verify:
- Filter bar appears (COB, Scope, All Users, Include deleted)
- Pipeline board shows colored columns with adjustment counts
- Detail table loads with sortable columns
- Deep-dive expanders open with lifecycle bar + PBI info + status history

- [ ] **Step 3: Commit**

```bash
git add streamlit_app/pages/7_Adjustment_Tracker.py
git commit -m "feat: add Adjustment Tracker page with pipeline board and deep-dive timeline"
```

---

### Task 5: Add Lifecycle Bar to My Work Cards

**Files:**
- Modify: `streamlit_app/pages/2_My_Work.py:12-15` (imports)
- Modify: `streamlit_app/pages/2_My_Work.py:90-97` (data loading)
- Modify: `streamlit_app/pages/2_My_Work.py:148-150` (card rendering)

- [ ] **Step 1: Update imports**

In `streamlit_app/pages/2_My_Work.py`, change the import block at lines 12-15 from:

```python
from utils.styles import (
    inject_css, render_sidebar, render_filter_chips, render_status_timeline,
    status_badge, section_title, P, SCOPE_CONFIG, STATUS_COLORS, STATUS_ICONS,
)
```

to:

```python
from utils.styles import (
    inject_css, render_sidebar, render_filter_chips, render_status_timeline,
    render_lifecycle_bar,
    status_badge, section_title, P, SCOPE_CONFIG, STATUS_COLORS, STATUS_ICONS,
)
```

- [ ] **Step 2: Load VW_ADJUSTMENT_TRACK data**

Replace the existing report status loading block (lines 90-97) from:

```python
# Load report refresh status for processed adjustments
df_report_status = pd.DataFrame()
try:
    df_report_status = run_query_df("""
        SELECT ADJ_ID, REPORT_STATUS, REPORT_STATUS_TIME
        FROM ADJUSTMENT_APP.VW_REPORT_REFRESH_STATUS
    """)
except Exception:
    pass
```

to:

```python
# Load lifecycle tracking data for all adjustments
df_track = pd.DataFrame()
try:
    df_track = run_query_df("""
        SELECT ADJ_ID, CURRENT_STAGE, REPORT_STATUS, REPORT_STATUS_TIME,
               SUBMITTED_AT, APPROVAL_REQUESTED_AT, APPROVED_AT,
               PROCESSING_STARTED_AT, PROCESSING_ENDED_AT,
               PBI_QUEUED_AT, PBI_STARTED_AT, PBI_COMPLETED_AT,
               PBI_REFRESH_DURATION_SEC, PBI_QUEUE_WAIT_SEC,
               RUN_STATUS
        FROM ADJUSTMENT_APP.VW_ADJUSTMENT_TRACK
    """)
except Exception:
    pass

# Backwards compat: keep df_report_status for the metadata row
df_report_status = df_track[["ADJ_ID", "REPORT_STATUS"]].copy() if not df_track.empty else pd.DataFrame()
if not df_report_status.empty and "REPORT_STATUS_TIME" not in df_report_status.columns:
    try:
        df_report_status = run_query_df("""
            SELECT ADJ_ID, REPORT_STATUS, REPORT_STATUS_TIME
            FROM ADJUSTMENT_APP.VW_REPORT_REFRESH_STATUS
        """)
    except Exception:
        pass
```

- [ ] **Step 3: Add lifecycle bar to render_adj_card()**

In the `render_adj_card()` function, after the status badge rendering (line 149: `st.markdown(status_badge(run_status), unsafe_allow_html=True)`) and before the filter chips section, add the lifecycle bar:

Find this block (around line 148-150):

```python
        with col_info:
            st.markdown(status_badge(run_status), unsafe_allow_html=True)
            st.markdown("<br/>", unsafe_allow_html=True)
```

Replace it with:

```python
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
```

- [ ] **Step 4: Update the Report Status metadata row**

The existing Report Status metadata row (lines 193-228) should now also show PBI Action ID when available. Find this block:

```python
            # Report status (only for Processed adjustments)
            if run_status == "Processed" and not df_report_status.empty:
                rs_match = df_report_status[df_report_status["ADJ_ID"] == adj_id]
```

Replace the entire report status block (lines 193-228) with:

```python
            # Report status (for Processed adjustments)
            if run_status == "Processed" and not df_track.empty:
                tr_match = df_track[df_track["ADJ_ID"] == adj_id]
                if not tr_match.empty:
                    tr = tr_match.iloc[0]
                    _rs_status = str(tr.get("REPORT_STATUS", "") or "")
                    _pbi_started = tr.get("PBI_STARTED_AT")
                    _pbi_completed = tr.get("PBI_COMPLETED_AT")
                    _pbi_queued = tr.get("PBI_QUEUED_AT")

                    _rs_time = _pbi_completed or _pbi_started or _pbi_queued
                    _rs_time_str = (_rs_time.strftime("%d %b %H:%M")
                                    if hasattr(_rs_time, "strftime") and str(_rs_time) != "NaT"
                                    else "")

                    _rs_icons = {
                        "Reports Ready": "✅",
                        "Refreshing": "🔄",
                        "Queued": "⏳",
                        "Awaiting": "⏳",
                    }
                    _rs_messages = {
                        "Reports Ready": f"Reports Ready ({_rs_time_str})",
                        "Refreshing": f"Refreshing ({_rs_time_str})",
                        "Queued": "Queued — next ControlM cycle ~5 min",
                        "Awaiting": "Awaiting report refresh",
                    }
                    _rs_colors = {
                        "Reports Ready": "#2E7D32",
                        "Refreshing": "#1565C0",
                        "Queued": "#E65100",
                        "Awaiting": "#757575",
                    }
                    icon = _rs_icons.get(_rs_status, "")
                    color = _rs_colors.get(_rs_status, "#757575")
                    msg = _rs_messages.get(_rs_status, _rs_status)
                    meta_rows.append(("Report Status",
                        f'<span style="color:{color};font-weight:600">{icon} {msg}</span>'))
```

- [ ] **Step 5: Verify My Work page renders (manual test)**

Open the Streamlit app, navigate to My Work. Verify:
- Each adjustment card shows the lifecycle progress bar below the status badge
- Processed adjustments show PBI status in both the lifecycle bar and the metadata table
- Non-processed adjustments (Pending, Approved) show partial lifecycle bar with grey upcoming stages
- Cards without approval flow show the shorter track (Submitted → Processing → PBI → Ready)

- [ ] **Step 6: Commit**

```bash
git add streamlit_app/pages/2_My_Work.py
git commit -m "feat: add compact lifecycle bar to My Work adjustment cards"
```

---

## Self-Review Checklist

**Spec coverage:**
- [x] Enhanced VW_REPORT_REFRESH_STATUS with PBI action details — Task 1
- [x] New VW_ADJUSTMENT_TRACK with full lifecycle — Task 2
- [x] Lifecycle bar CSS + helper — Task 3
- [x] Tracker page with pipeline board overview — Task 4
- [x] Tracker page with deep-dive timeline — Task 4
- [x] My Work compact lifecycle bar — Task 5
- [x] Adaptive track (approval vs no-approval) — Task 3 (`_build_stage_list` checks `APPROVAL_REQUESTED_AT`)
- [x] PBI action linking (action ID, timings) — Tasks 1, 2, 4

**Placeholder scan:** No TBD/TODO/placeholders found.

**Type consistency:**
- `STAGE_CONFIG` defined in Task 3, imported in Task 4 — consistent
- `render_lifecycle_bar` defined in Task 3, imported in Tasks 4 and 5 — consistent
- `VW_ADJUSTMENT_TRACK` column names (SUBMITTED_AT, APPROVAL_REQUESTED_AT, etc.) match between Task 2 SQL and Task 3/4/5 Python — consistent
- `df_track` variable name used consistently in Tasks 4 and 5

---

Plan complete and saved to `docs/superpowers/plans/2026-04-13-adjustment-tracker.md`. Two execution options:

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints

Which approach?