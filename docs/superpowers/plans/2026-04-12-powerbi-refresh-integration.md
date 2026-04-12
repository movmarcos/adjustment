# PowerBI Refresh Integration — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** After adjustments are processed, trigger PowerBI refresh and give users real-time visibility into whether their reports reflect their adjustment data.

**Architecture:** Add `RUN_LOG_ID` to `ADJ_HEADER` for direct traceability. Call `FACT.UPDATE_POWERBI_FOR_ADJUSTMENTS` at the end of each processing path. Build a report-status resolver that compares adjustment `PROCESS_DATE` against `METADATA.POWERBI_ACTION` timelines. Surface this in both Processing Queue (global view) and My Work (per-adjustment status).

**Tech Stack:** Snowflake SQL, Snowpark Python, Streamlit

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `new_adjustment_db_objects/01_tables.sql` | Modify | Add `RUN_LOG_ID` column to `ADJ_HEADER` |
| `new_adjustment_db_objects/05_sp_process_adjustment.sql` | Modify | Store `RUN_LOG_ID`, call `UPDATE_POWERBI_FOR_ADJUSTMENTS` |
| `new_adjustment_db_objects/08_views.sql` | Modify | Add `RUN_LOG_ID` to `VW_MY_WORK`, create `VW_REPORT_REFRESH_STATUS` |
| `streamlit_app/utils/styles.py` | Modify | Update `render_pipeline_diagram` to 5 stages |
| `streamlit_app/pages/4_Processing_Queue.py` | Modify | Add Report Refresh Status section, update pipeline to 5 nodes |
| `streamlit_app/pages/2_My_Work.py` | Modify | Add per-adjustment Report Status in card metadata |
| `streamlit_app/pages/6_Documentation.py` | Modify | Document PowerBI refresh step |

---

### Task 1: Add RUN_LOG_ID to ADJ_HEADER schema

**Files:**
- Modify: `new_adjustment_db_objects/01_tables.sql:86-88` (after ERRORMESSAGE)

- [ ] **Step 1: Add RUN_LOG_ID column**

In `01_tables.sql`, after the `ERRORMESSAGE` line (line 88), add:

```sql
    RUN_LOG_ID                  NUMBER(38,0),                    -- Set by SP_PROCESS_ADJUSTMENT from BATCH.SEQ_RUN_LOG
```

- [ ] **Step 2: Add RUN_LOG_ID to VW_MY_WORK**

In `08_views.sql`, in the `VW_MY_WORK` view (line 158-184), add `h.RUN_LOG_ID` and `h.PROCESS_TYPE` (already present) to the SELECT list. Add after `h.PROCESS_DATE`:

```sql
    h.RUN_LOG_ID,
```

- [ ] **Step 3: Commit**

```bash
git add new_adjustment_db_objects/01_tables.sql new_adjustment_db_objects/08_views.sql
git commit -m "feat: add RUN_LOG_ID column to ADJ_HEADER and VW_MY_WORK"
```

---

### Task 2: Create VW_REPORT_REFRESH_STATUS view

**Files:**
- Modify: `new_adjustment_db_objects/08_views.sql` (append before VERIFY section)

This view joins `ADJ_HEADER` (Processed adjustments) with `METADATA.POWERBI_ACTION` to compute per-adjustment report status. The Streamlit pages query this view instead of running the complex status logic themselves.

- [ ] **Step 1: Add the view**

In `08_views.sql`, before the `-- VERIFY` section (line 258), add:

```sql
-- ═══════════════════════════════════════════════════════════════════════════
-- 8. VW_REPORT_REFRESH_STATUS — PowerBI refresh status per processed adjustment
--
-- Computes whether a processed adjustment's data has been reflected in
-- PowerBI reports. Joins ADJ_HEADER.PROCESS_DATE against
-- METADATA.POWERBI_ACTION timelines.
--
-- Status logic:
--   1. Find POWERBI_ACTION rows matching scope (via INSERT_SOURCE) + COBID
--   2. If a COMPLETED action has REQUEST_TIME >= adjustment PROCESS_DATE → Reports Ready
--   3. If a RUNNING action has REQUEST_TIME >= PROCESS_DATE → Refreshing
--   4. If a QUEUED action has REQUEST_TIME >= PROCESS_DATE → Queued
--   5. If a RUNNING action with REQUEST_TIME < PROCESS_DATE → Next Cycle
--   6. Otherwise → Awaiting
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW ADJUSTMENT_APP.VW_REPORT_REFRESH_STATUS
    COMMENT = 'Per-adjustment PowerBI refresh status. Joins ADJ_HEADER with METADATA.POWERBI_ACTION to determine if reports reflect the adjustment.'
AS
WITH adj_processed AS (
    SELECT
        h.ADJ_ID,
        h.COBID,
        h.PROCESS_TYPE,
        h.RUN_LOG_ID,
        h.PROCESS_DATE,
        -- Map PROCESS_TYPE to the INSERT_SOURCE pattern used by UPDATE_POWERBI_FOR_ADJUSTMENTS
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
-- Find the best matching POWERBI_ACTION per adjustment
pbi_match AS (
    SELECT
        a.ADJ_ID,
        -- Completed action AFTER adjustment was processed = reports include this data
        MAX(CASE WHEN pa.COMPLETE_TIME IS NOT NULL
                  AND pa.REQUEST_TIME >= a.PROCESS_DATE
             THEN pa.COMPLETE_TIME END) AS COMPLETED_AT,
        -- Running action AFTER adjustment was processed = refresh in progress
        MAX(CASE WHEN pa.START_TIME IS NOT NULL
                  AND pa.COMPLETE_TIME IS NULL
                  AND pa.REQUEST_TIME >= a.PROCESS_DATE
             THEN pa.START_TIME END) AS RUNNING_SINCE,
        -- Queued action AFTER adjustment was processed = waiting for ControlM
        MAX(CASE WHEN pa.START_TIME IS NULL
                  AND pa.COMPLETE_TIME IS NULL
                  AND pa.REQUEST_TIME >= a.PROCESS_DATE
             THEN pa.REQUEST_TIME END) AS QUEUED_AT,
        -- Running action BEFORE adjustment was processed = won't include this data
        MAX(CASE WHEN pa.START_TIME IS NOT NULL
                  AND pa.COMPLETE_TIME IS NULL
                  AND pa.REQUEST_TIME < a.PROCESS_DATE
             THEN pa.START_TIME END) AS STALE_RUNNING_SINCE
    FROM adj_processed a
    LEFT JOIN METADATA.POWERBI_ACTION pa
        ON pa.COBID = a.COBID
        AND pa.INSERT_SOURCE = a.EXPECTED_INSERT_SOURCE
    GROUP BY a.ADJ_ID
)
SELECT
    a.ADJ_ID,
    a.COBID,
    a.PROCESS_TYPE,
    a.PROCESS_DATE,
    a.RUN_LOG_ID,
    m.COMPLETED_AT,
    m.RUNNING_SINCE,
    m.QUEUED_AT,
    m.STALE_RUNNING_SINCE,
    CASE
        WHEN m.COMPLETED_AT IS NOT NULL THEN 'Reports Ready'
        WHEN m.RUNNING_SINCE IS NOT NULL THEN 'Refreshing'
        WHEN m.QUEUED_AT IS NOT NULL THEN 'Queued'
        WHEN m.STALE_RUNNING_SINCE IS NOT NULL THEN 'Next Cycle'
        ELSE 'Awaiting'
    END AS REPORT_STATUS,
    CASE
        WHEN m.COMPLETED_AT IS NOT NULL
            THEN CONVERT_TIMEZONE('UTC', 'Europe/London', m.COMPLETED_AT::TIMESTAMP_NTZ)
        WHEN m.RUNNING_SINCE IS NOT NULL
            THEN CONVERT_TIMEZONE('UTC', 'Europe/London', m.RUNNING_SINCE::TIMESTAMP_NTZ)
        WHEN m.QUEUED_AT IS NOT NULL
            THEN CONVERT_TIMEZONE('UTC', 'Europe/London', m.QUEUED_AT::TIMESTAMP_NTZ)
    END AS REPORT_STATUS_TIME
FROM adj_processed a
LEFT JOIN pbi_match m ON m.ADJ_ID = a.ADJ_ID;
```

- [ ] **Step 2: Commit**

```bash
git add new_adjustment_db_objects/08_views.sql
git commit -m "feat: add VW_REPORT_REFRESH_STATUS view for PowerBI tracking"
```

---

### Task 3: Call UPDATE_POWERBI_FOR_ADJUSTMENTS in SP_PROCESS_ADJUSTMENT

**Files:**
- Modify: `new_adjustment_db_objects/05_sp_process_adjustment.sql`

Three changes: (a) store RUN_LOG_ID in ADJ_HEADER at the start, (b) add a helper function for the PowerBI call, (c) call it at the end of each processing path (Direct, Scale, EntityRoll).

- [ ] **Step 1: Add helper function and store RUN_LOG_ID**

After the `log_status_history` function (around line 99), add the PowerBI helper:

```python
# ── INSERT_SOURCE mapping for PowerBI ────────────────────────────────────
PBI_INSERT_SOURCE = {
    'VAR':         'LOAD_VAR_ADJUSTMENT',
    'STRESS':      'LOAD_STRESS_ADJUSTMENT',
    'SENSITIVITY': 'LOAD_SENSITIVITY_ADJUSTMENT',
    'FRTB':        'LOAD_FRTB_ADJUSTMENT',
    'FRTBDRC':     'LOAD_FRTB_ADJUSTMENT',
    'FRTBRRAO':    'LOAD_FRTB_ADJUSTMENT',
    'FRTBALL':     'LOAD_FRTB_ADJUSTMENT',
}

def trigger_powerbi_refresh(session, process_type, run_log_id):
    """Call FACT.UPDATE_POWERBI_FOR_ADJUSTMENTS to queue a PowerBI refresh.
    Mirrors the call in FACT.PROCESS_ADJUSTMENTS (the reference procedure)."""
    insert_source = PBI_INSERT_SOURCE.get(process_type.upper(),
                                          f'LOAD_{process_type.upper()}_ADJUSTMENT')
    try:
        session.sql(f"""
            CALL FACT.UPDATE_POWERBI_FOR_ADJUSTMENTS(
                '{process_type}',
                'RaptorReporting',
                '{insert_source}',
                '{run_log_id}',
                '0'
            )
        """).collect()
    except Exception as pbi_err:
        # PowerBI refresh failure should NOT fail the adjustment itself
        print(f"Warning: PowerBI refresh trigger failed: {pbi_err}")
```

- [ ] **Step 2: Store RUN_LOG_ID in ADJ_HEADER after creating it**

After line 235 (`run_log_id = session.sql(...)`), add:

```python
        result["run_log_id"] = run_log_id
```

This line already exists. No change needed for the result dict.

Now find each path where `adj_ids_str` is built and add a SQL to store the run_log_id. There are three locations:

**Direct path** — after line 273 (`adj_ids_str = ...`), add:

```python
            # Store RUN_LOG_ID in ADJ_HEADER for traceability
            session.sql(f"""
                UPDATE ADJUSTMENT_APP.ADJ_HEADER
                SET RUN_LOG_ID = {run_log_id}
                WHERE ADJ_ID IN ({adj_ids_str})
            """).collect()
```

**Scale path** — after line 358 (`adj_ids_str = ...`), add the same:

```python
            # Store RUN_LOG_ID in ADJ_HEADER for traceability
            session.sql(f"""
                UPDATE ADJUSTMENT_APP.ADJ_HEADER
                SET RUN_LOG_ID = {run_log_id}
                WHERE ADJ_ID IN ({adj_ids_str})
            """).collect()
```

**EntityRoll path** — find the equivalent `adj_ids_str` line in the EntityRoll section and add the same UPDATE.

- [ ] **Step 3: Call trigger_powerbi_refresh at the end of each path**

**Direct path** — after `result["message"] = "Direct adjustments processed successfully"` (line 341), add:

```python
            # ── Trigger PowerBI refresh ─────────────────────────────────
            trigger_powerbi_refresh(session, process_type, run_log_id)
```

**Scale path** — after `result["message"] = "Scale adjustments processed successfully"` (line 773), add the same call:

```python
            # ── Trigger PowerBI refresh ─────────────────────────────────
            trigger_powerbi_refresh(session, process_type, run_log_id)
```

**EntityRoll path** — after the EntityRoll result message (line 917), add the same call:

```python
            # ── Trigger PowerBI refresh ─────────────────────────────────
            trigger_powerbi_refresh(session, process_type, run_log_id)
```

- [ ] **Step 4: Commit**

```bash
git add new_adjustment_db_objects/05_sp_process_adjustment.sql
git commit -m "feat: store RUN_LOG_ID and trigger PowerBI refresh after processing"
```

---

### Task 4: Update pipeline diagram to 5 stages

**Files:**
- Modify: `streamlit_app/utils/styles.py:464-480`

- [ ] **Step 1: Update render_pipeline_diagram**

Replace the `render_pipeline_diagram` function (lines 464-480):

```python
def render_pipeline_diagram(current_stage: int = 0):
    stages = [
        ("\U0001f4be", "ADJ Header\nInsert"),
        ("\u23f0", "Task Polls\n(\u22641 min)"),
        ("\U0001f504", "SP_RUN_PIPELINE\nExecutes"),
        ("\U0001f4ca", "Dynamic Table\nRefresh"),
        ("\U0001f4c8", "Report\nRefresh"),
    ]
    nodes = []
    for i, (icon, label) in enumerate(stages, 1):
        state_class = "done" if i < current_stage else ("active" if i == current_stage else "")
        nodes.append(
            f'<div class="pipe-node {state_class}">'
            f'<div class="pn-icon">{icon}</div>'
            f'<div class="pn-label">{label}</div></div>')
        if i < len(stages):
            nodes.append('<div class="pipe-arrow">\u2192</div>')
    st.markdown(f'<div class="pipeline">{"".join(nodes)}</div>', unsafe_allow_html=True)
```

- [ ] **Step 2: Commit**

```bash
git add streamlit_app/utils/styles.py
git commit -m "feat: add Report Refresh as 5th pipeline stage"
```

---

### Task 5: Add Report Refresh Status section to Processing Queue

**Files:**
- Modify: `streamlit_app/pages/4_Processing_Queue.py`

- [ ] **Step 1: Update pipeline description labels**

Update the description grid (around line 60-68) to include the 5th node:

```python
st.markdown(
    f'<div style="display:grid;grid-template-columns:repeat(5,1fr);gap:10px;'
    f'font-size:0.78rem;color:{P["grey_700"]};text-align:center;margin-top:0.3rem">'
    f'<div>Adjustment saved to<br/><strong>ADJ_HEADER</strong></div>'
    f'<div><strong>Scope task</strong><br/>polls every 1 min<br/>exits fast when idle</div>'
    f'<div><strong>SP_RUN_PIPELINE</strong><br/>claim \u2192 block \u2192 process \u2192 unblock</div>'
    f'<div><strong>Dynamic Tables</strong><br/>auto-refresh (1 min lag)</div>'
    f'<div><strong>PowerBI Refresh</strong><br/>ControlM every ~5 min</div>'
    f'</div>',
    unsafe_allow_html=True)
```

- [ ] **Step 2: Update pipeline stage logic to detect report refresh**

Update the stage detection (around line 50-56) to account for the 5th stage. Add a check for Processed adjustments that have PowerBI actions pending:

```python
# Determine pipeline stage
if running_count > 0:
    stage = 3   # SP executing
elif pending_count > 0:
    stage = 2   # Adjustment queued, task will poll within 1 minute
else:
    stage = 5   # All done / idle

# Check if there are report refreshes in progress
try:
    df_pbi = run_query_df("""
        SELECT REPORT_STATUS, COUNT(*) AS CNT
        FROM ADJUSTMENT_APP.VW_REPORT_REFRESH_STATUS
        WHERE REPORT_STATUS IN ('Queued', 'Refreshing', 'Next Cycle')
        GROUP BY REPORT_STATUS
    """)
    pbi_pending = int(df_pbi["CNT"].sum()) if not df_pbi.empty else 0
    if pbi_pending > 0 and stage == 5:
        stage = 5  # report refresh in progress (5th stage active)
    elif pbi_pending > 0 and running_count == 0 and pending_count == 0:
        stage = 5
except Exception:
    pbi_pending = 0
```

- [ ] **Step 3: Add Report Refresh Status section**

After the "Recently Processed" section (after line 214), add a new section:

```python
# ──────────────────────────────────────────────────────────────────────────────
# REPORT REFRESH STATUS
# ──────────────────────────────────────────────────────────────────────────────

st.markdown("<br/>", unsafe_allow_html=True)
section_title("Report Refresh Status", "\U0001f4c8")

try:
    df_pbi_status = run_query_df("""
        SELECT
            CASE
                WHEN START_TIME IS NULL THEN 'Queued'
                WHEN COMPLETE_TIME IS NULL THEN 'Running'
                ELSE 'Completed'
            END AS STATUS,
            CASE
                WHEN OBJECT_NAME = 'RaptorReporting' AND INSERT_SOURCE LIKE '%VAR%' THEN 'VaR'
                WHEN OBJECT_NAME = 'RaptorReporting' AND INSERT_SOURCE LIKE '%STRESS%' THEN 'Stress'
                WHEN OBJECT_NAME = 'RaptorReporting' AND INSERT_SOURCE LIKE '%SENSITIVITY%' THEN 'Sensitivity'
                WHEN OBJECT_NAME = 'RaptorReporting' AND INSERT_SOURCE LIKE '%FRTB%' THEN 'FRTB'
                WHEN OBJECT_NAME = 'VaR Adjustment Summary Import' THEN 'VaR'
                WHEN OBJECT_NAME = 'Stress Measures Adjustment Import' THEN 'Stress'
                WHEN OBJECT_NAME = 'Sensitivity Summary Adjustment Import' THEN 'Sensitivity'
                ELSE OBJECT_NAME
            END AS SCOPE,
            COBID,
            OBJECT_NAME,
            INSERT_SOURCE,
            CONVERT_TIMEZONE('UTC', 'Europe/London', REQUEST_TIME::TIMESTAMP_NTZ) AS REQUEST_TIME,
            CONVERT_TIMEZONE('UTC', 'Europe/London', START_TIME::TIMESTAMP_NTZ) AS START_TIME,
            CONVERT_TIMEZONE('UTC', 'Europe/London', COMPLETE_TIME::TIMESTAMP_NTZ) AS COMPLETE_TIME
        FROM METADATA.POWERBI_ACTION
        WHERE INSERT_SOURCE LIKE 'LOAD_%_ADJUSTMENT'
        ORDER BY REQUEST_TIME DESC
        LIMIT 30
    """)

    if not df_pbi_status.empty:
        # Summary cards by status
        pbi_queued    = int(df_pbi_status[df_pbi_status["STATUS"] == "Queued"].shape[0])
        pbi_running   = int(df_pbi_status[df_pbi_status["STATUS"] == "Running"].shape[0])
        pbi_completed = int(df_pbi_status[df_pbi_status["STATUS"] == "Completed"].shape[0])

        pc1, pc2, pc3 = st.columns(3)
        pbi_stats = [
            ("\u23f3 Queued",    pbi_queued,    P["warning"]),
            ("\U0001f504 Running", pbi_running,  "#1565C0"),
            ("\u2705 Completed",  pbi_completed, P["success"]),
        ]
        for col, (lbl, val, color) in zip([pc1, pc2, pc3], pbi_stats):
            col.markdown(
                f'<div style="background:{P["white"]};border:1px solid {P["border"]};'
                f'border-top:3px solid {color};border-radius:8px;padding:0.6rem;text-align:center">'
                f'<div style="font-size:1.3rem;font-weight:800;color:{color}">{val}</div>'
                f'<div style="font-size:0.7rem;text-transform:uppercase;letter-spacing:.06em;'
                f'color:{P["grey_700"]};margin-top:2px">{lbl}</div>'
                f'</div>',
                unsafe_allow_html=True)

        st.markdown("<br/>", unsafe_allow_html=True)

        # Detail table
        def color_pbi_status(val):
            if val == "Completed": return f"color:{P['success']};font-weight:600"
            if val == "Running":   return f"color:#1565C0;font-weight:600"
            if val == "Queued":    return f"color:{P['warning']};font-weight:600"
            return ""

        display = ["STATUS", "SCOPE", "COBID", "OBJECT_NAME",
                    "REQUEST_TIME", "START_TIME", "COMPLETE_TIME"]
        existing = [c for c in display if c in df_pbi_status.columns]
        st.dataframe(
            df_pbi_status[existing].style.map(color_pbi_status, subset=["STATUS"]),
            use_container_width=True, height=250,
        )
    else:
        st.info("No adjustment-related PowerBI refresh actions found.")
except Exception as pbi_ex:
    st.info(f"PowerBI refresh status not available: {pbi_ex}")
```

- [ ] **Step 4: Update the Snowflake Task Schedule expander**

In the expander content (around line 236-247), add step 5 to the Processing Flow and a note about PowerBI:

Add after step 5 in the flow:
```
6. `SP_PROCESS_ADJUSTMENT` calls `FACT.UPDATE_POWERBI_FOR_ADJUSTMENTS` → queues PowerBI refresh in `METADATA.POWERBI_ACTION`
7. ControlM job (every ~5 min) picks up queued actions and triggers the PowerBI dataset refresh
8. Once the refresh completes, reports reflect the adjustment data
```

- [ ] **Step 5: Commit**

```bash
git add streamlit_app/pages/4_Processing_Queue.py
git commit -m "feat: add Report Refresh Status section to Processing Queue"
```

---

### Task 6: Add per-adjustment Report Status in My Work cards

**Files:**
- Modify: `streamlit_app/pages/2_My_Work.py:116-208` (render_adj_card function)

- [ ] **Step 1: Query report refresh status for processed adjustments**

After loading `df_adjs` (around line 84), add a query to fetch report status and merge it:

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

- [ ] **Step 2: Add report status to the adjustment card**

In `render_adj_card` (around line 116), after the `meta_rows` list (line 181), add report status lookup:

```python
            # Report status (only for Processed adjustments)
            report_status_label = None
            if run_status == "Processed" and not df_report_status.empty:
                rs_match = df_report_status[df_report_status["ADJ_ID"] == adj_id]
                if not rs_match.empty:
                    rs = rs_match.iloc[0]
                    _rs_status = str(rs.get("REPORT_STATUS", ""))
                    _rs_time = rs.get("REPORT_STATUS_TIME")
                    _rs_time_str = (_rs_time.strftime("%d %b %H:%M")
                                    if hasattr(_rs_time, "strftime") and str(_rs_time) != "NaT"
                                    else "")

                    _rs_icons = {
                        "Reports Ready": "\u2705",
                        "Refreshing": "\U0001f504",
                        "Queued": "\u23f3",
                        "Next Cycle": "\u23f3",
                        "Awaiting": "\u23f3",
                    }
                    _rs_colors = {
                        "Reports Ready": P["success"],
                        "Refreshing": "#1565C0",
                        "Queued": P["warning"],
                        "Next Cycle": P["warning"],
                        "Awaiting": P["grey_700"],
                    }
                    _rs_messages = {
                        "Reports Ready": f"Reports Ready ({_rs_time_str})",
                        "Refreshing": f"Refreshing ({_rs_time_str})",
                        "Queued": "Queued \u2014 next ControlM cycle ~5 min",
                        "Next Cycle": "Current refresh won't include this \u2014 next cycle will",
                        "Awaiting": "Awaiting report refresh",
                    }
                    icon = _rs_icons.get(_rs_status, "")
                    color = _rs_colors.get(_rs_status, P["grey_700"])
                    msg = _rs_messages.get(_rs_status, _rs_status)
                    report_status_label = (icon, color, msg)
```

Then add it to `meta_rows` (before the list is rendered):

```python
            if report_status_label:
                icon, color, msg = report_status_label
                meta_rows.append(("Report Status",
                    f'<span style="color:{color};font-weight:600">{icon} {msg}</span>'))
```

And update the `rows_html` rendering to allow HTML in values (change `{v}` to use `unsafe_allow_html`-compatible rendering — since meta_rows is already inside an `st.markdown(unsafe_allow_html=True)` block, HTML in values renders correctly).

- [ ] **Step 3: Commit**

```bash
git add streamlit_app/pages/2_My_Work.py
git commit -m "feat: add per-adjustment Report Status in My Work cards"
```

---

### Task 7: Update Documentation page

**Files:**
- Modify: `streamlit_app/pages/6_Documentation.py`

- [ ] **Step 1: Add PowerBI refresh to the processing flow documentation**

Find the Scale Processing Flow section (around line 922) and add a step after the existing flow description. Add a new section for PowerBI refresh:

```python
    # ── PowerBI Refresh ──────────────────────────────────────────────────
    section_title("PowerBI Report Refresh", "\U0001f4c8")
    st.markdown(f"""
    After processing completes, the system automatically queues a **PowerBI dataset refresh**
    by calling `FACT.UPDATE_POWERBI_FOR_ADJUSTMENTS`. This writes to `METADATA.POWERBI_ACTION`,
    which is monitored by a **ControlM job running every ~5 minutes**.

    **Report Status indicators** (visible in My Work and Processing Queue):

    | Status | Meaning |
    |--------|---------|
    | \u2705 **Reports Ready** | PowerBI refresh completed — reports reflect your adjustment |
    | \U0001f504 **Refreshing** | PowerBI refresh is currently running |
    | \u23f3 **Queued** | Refresh queued, ControlM will pick it up within ~5 min |
    | \u23f3 **Next Cycle** | A refresh is running but started before your adjustment — the next cycle will include it |
    | \u23f3 **Awaiting** | Refresh not yet queued |

    **Important:** If a PowerBI refresh is already running when your adjustment completes,
    that running refresh will **not** include your data (it started reading before your data was written).
    The system detects this and shows "Next Cycle" — your data will be included in the next
    refresh (~5 minutes).
    """, unsafe_allow_html=True)
```

- [ ] **Step 2: Add METADATA.POWERBI_ACTION to the object inventory**

In the object inventory table (around line 1110), add:

```python
        ("METADATA.POWERBI_ACTION",  "TABLE (external)", "—",  "PowerBI refresh action queue. Written by UPDATE_POWERBI_FOR_ADJUSTMENTS, read by ControlM."),
        ("VW_REPORT_REFRESH_STATUS", "VIEW",             "08_views.sql", "Per-adjustment PowerBI refresh status. Joins ADJ_HEADER with POWERBI_ACTION."),
```

- [ ] **Step 3: Update the pipeline flow description**

In the Processing Flow section, add step 6-8 after step 5:

```
6. `SP_PROCESS_ADJUSTMENT` calls `FACT.UPDATE_POWERBI_FOR_ADJUSTMENTS` → queues PowerBI refresh
7. ControlM job (~5 min) picks up queued actions → triggers PowerBI dataset refresh
8. Reports updated — status shows "Reports Ready" in My Work and Processing Queue
```

- [ ] **Step 4: Commit**

```bash
git add streamlit_app/pages/6_Documentation.py
git commit -m "docs: add PowerBI refresh step to documentation"
```

---

### Task 8: Final integration commit

- [ ] **Step 1: Verify all files are committed**

```bash
git status
git log --oneline -8
```

- [ ] **Step 2: Push**

```bash
git push
```
