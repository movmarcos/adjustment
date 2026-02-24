# Fact Table Adjustment Engine

## Overview

A modern Snowflake-native adjustment platform that enables users to apply **Flatten**, **Scale**, and **Roll** adjustments to fact table data with full auditability, approval workflows, and AI-powered insights via Cortex.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                     Streamlit in Snowflake (SiS)                     │
│  ┌──────────┐  ┌───────────┐  ┌──────────────┐  ┌───────────────┐  │
│  │  Apply    │  │  Audit    │  │  AI          │  │  Impact       │  │
│  │  Adjust.  │  │  Trail    │  │  Assistant   │  │  Dashboard    │  │
│  └────┬─────┘  └─────┬─────┘  └──────┬───────┘  └──────┬────────┘  │
└───────┼───────────────┼───────────────┼─────────────────┼───────────┘
        │               │               │                 │
┌───────▼───────────────▼───────────────▼─────────────────▼───────────┐
│                        Snowflake Engine                              │
│                                                                      │
│  ┌─────────────────┐    ┌─────────────────┐    ┌──────────────────┐ │
│  │  FACT_TABLE      │    │  ADJ_HEADER      │    │  ADJ_LINE_ITEM  │ │
│  │  (immutable)     │    │  (Hybrid Table)  │    │  (delta values) │ │
│  └────────┬─────────┘    └────────┬─────────┘    └────────┬────────┘ │
│           │                       │                        │         │
│           ▼                       ▼                        ▼         │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │              DYNAMIC TABLE: FACT_ADJUSTED                      │  │
│  │     (auto-materialized UNION ALL + GROUP BY)                   │  │
│  │     Replaces manual views & materialized tables                │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  ┌─────────────┐  ┌──────────────┐  ┌──────────────────────────┐   │
│  │  STREAM on   │  │  TASK chain  │  │  Cortex AI               │   │
│  │  ADJ tables  │  │  (process +  │  │  - LLM Summarization     │   │
│  │              │  │   notify)    │  │  - Anomaly Detection     │   │
│  └─────────────┘  └──────────────┘  │  - Natural Language SQL   │   │
│                                      └──────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Key Design Decisions

### 1. Delta Pattern (Preserved & Enhanced)

The existing delta pattern (storing offset values that sum to the target) is **the correct approach** for financial data. We enhance it with:

| Adjustment | Delta Formula | Result when summed |
|------------|--------------|-------------------|
| **Flatten** | `delta = -1 × current_value` | `current + delta = 0` |
| **Scale(f)** | `delta = current_value × (f - 1)` | `current + delta = current × f` |
| **Roll(s)** | `delta = prev_day_value × s - current_value` | `current + delta = prev_day × s` |

### 2. Dynamic Tables Replace Manual Materialization

Instead of manually maintaining UNION ALL + GROUP BY views and materialized tables:

- **Dynamic Tables** auto-refresh incrementally when upstream data changes
- Configurable lag (e.g., 1 minute) balances freshness vs. cost
- Eliminates manual ETL orchestration for the adjusted view

### 3. Hybrid Tables for Adjustment Metadata

- `ADJ_HEADER` and `ADJ_LINE_ITEM` use **Hybrid Tables** for row-level ACID transactions
- Supports the interactive Streamlit write pattern (single-row inserts/updates)
- Enforces referential integrity with primary/foreign keys

### 4. Approval Workflow with State Machine

```
DRAFT → PENDING_APPROVAL → APPROVED → APPLIED
                         → REJECTED
              APPLIED    → REVERSED
```

### 5. Cortex AI Integration

| Feature | Cortex Function | Use Case |
|---------|----------------|----------|
| Audit Summarization | `SNOWFLAKE.CORTEX.COMPLETE()` | "Summarize all adjustments this week" |
| Anomaly Detection | `SNOWFLAKE.CORTEX.ANOMALY_DETECTION` | Flag unusual adjustment patterns |
| Natural Language Query | `SNOWFLAKE.CORTEX.COMPLETE()` | "Show me all flatten adjustments > $1M" |
| Impact Explanation | `SNOWFLAKE.CORTEX.COMPLETE()` | Auto-generate business explanation of impact |

---

## Modern Snowflake Features Used

| Feature | Purpose |
|---------|---------|
| **Dynamic Tables** | Auto-materialized adjusted fact view |
| **Hybrid Tables** | OLTP writes for adjustment metadata |
| **Streams** | CDC on adjustment tables |
| **Tasks** | Automated processing & notification chain |
| **Cortex LLM** | AI summarization, NL querying |
| **Cortex Anomaly Detection** | Detect unusual adjustment patterns |
| **Snowpark Python** | Stored procedure logic |
| **Streamlit in Snowflake** | Native UI, no external infra |
| **Object Tagging** | Data governance & lineage |
| **Row Access Policies** | Fine-grained security |
| **Notification Integration** | Email/Slack alerts on adjustments |

---

## The Journey — From Login to Adjusted Data

> _Follow an operator named Sarah as she opens the app, creates a Flatten adjustment on incorrect FX positions, gets it approved, and watches the adjusted numbers flow into downstream reports — all without a single manual ETL job._

---

### Chapter 1: Sarah Opens the App

Sarah navigates to the **Streamlit in Snowflake** URL published by her team. There is no external server, no VPN tunnel, no separate credentials — the app runs **inside Snowflake** as a first-class object.

```
Browser → Snowflake Front-end → Streamlit in Snowflake (SiS)
```

| Snowflake feature | Role |
|---|---|
| **Streamlit in Snowflake (SiS)** | Hosts the entire UI natively; no external infrastructure needed |
| **Row Access Policy** | Silently filters data so Sarah only sees the entities she is authorized for (e.g., `US_BANK`, `EU_BANK`) |
| **Role Hierarchy** (RBAC) | Her Snowflake role (`ADJ_OPERATOR`) determines which buttons and actions are visible |

The **Home page** loads four KPI cards — _Total Adjustments_, _Pending Approval_, _Applied Today_, _Total Impact_ — each powered by a live `SELECT` against:

- `CORE.ADJ_HEADER` → a **Hybrid Table** serving low-latency, row-level reads
- `MART.FACT_ADJUSTED` → a **Dynamic Table** that is always up-to-date

Sarah already has full visibility before she touches anything.

---

### Chapter 2: Building the Filter

Sarah clicks **"Apply Adjustment"**. The page presents a dynamic set of filter controls — one per dimension — built from the configuration table `ADJ_DIMENSION_CONFIG`.

She selects:
- **Entity**: `US_BANK`
- **Product**: `FX_SPOT`
- **Business Date**: `2026-02-23`

Behind the scenes the app queries `FACT_TABLE` with her filters to count affected rows and preview current values.

| Snowflake feature | Role |
|---|---|
| **Hybrid Table** (`ADJ_DIMENSION_CONFIG`) | Stores dimension metadata (name, column, display order) with fast point reads |
| **Row Access Policy** | The `WHERE` clause is invisibly enriched — Sarah can never see or adjust entities she doesn't own |
| **Object Tagging** | Every table carries governance tags (`DATA_CLASSIFICATION`, `DATA_DOMAIN`) for compliance tracking |

---

### Chapter 3: Choosing the Adjustment Type

Sarah selects **Flatten** — she needs to zero out the amounts for those FX_SPOT positions because they were booked in error.

The UI shows her a simple explanation:

> _Flatten sets all selected measure values to zero by inserting a negative delta equal to the current value._

She adds a **business reason** (_"Erroneous FX bookings on 2/23 — ticket FX-4521"_) and a **ticket reference**.

No Snowflake query runs yet — this is pure client-side form state inside Streamlit's `st.session_state`.

---

### Chapter 4: The Preview — Before She Commits

Sarah clicks **"Preview"**. This is the critical moment: can she see the impact _before_ anything is written?

The app calls a **Snowpark Python Stored Procedure**:

```sql
CALL CORE.SP_PREVIEW_ADJUSTMENT('FLATTEN', '2026-02-23', NULL, NULL, 1.0,
     '{"ENTITY_KEY": "US_BANK", "PRODUCT_KEY": "FX_SPOT"}');
```

This procedure:
1. Reads the current fact rows matching her filters
2. Computes deltas in-memory using Snowpark DataFrames (`delta = -1 × current_value`)
3. Returns a result table with `ORIGINAL_VALUE`, `DELTA`, and `NEW_VALUE` columns — **nothing is written**

The Streamlit page renders three tabs:

| Tab | What Sarah sees |
|-----|-----------------|
| **Comparison** | Side-by-side table: original vs. adjusted, with totals |
| **Detail** | Row-level breakdown per dimension combination |
| **Chart** | Bar chart overlaying before / after values |

| Snowflake feature | Role |
|---|---|
| **Snowpark Python** | Stored procedure runs server-side; Delta math computed as DataFrame transformations |
| **Stored Procedure (TABLE return)** | Returns a tabular result directly to Streamlit, no temp table needed |
| **Warehouse** (`ADJUSTMENT_WH`) | The interactive warehouse processes this read-only query |

Sarah sees that 7 rows will be affected, totaling a $2.3M net delta. She's satisfied.

---

### Chapter 5: Submitting the Adjustment

Sarah clicks **"Submit for Approval"**. Behind the curtain, a second stored procedure fires:

```sql
CALL CORE.SP_CREATE_ADJUSTMENT('FLATTEN', '2026-02-23', NULL, NULL, 1.0,
     '{"ENTITY_KEY": "US_BANK", "PRODUCT_KEY": "FX_SPOT"}',
     'Erroneous FX bookings on 2/23', 'FX-4521', TRUE);
```

One atomic operation now:
1. **Inserts a row** into `ADJ_HEADER` → status = `PENDING_APPROVAL` (because she chose "Submit for Approval")
2. **Inserts 7 rows** into `ADJ_LINE_ITEM` → each storing the `FACT_ID`, the delta values, **and** a snapshot of the original values
3. **Inserts a row** into `ADJ_STATUS_HISTORY` → recording the initial state

| Snowflake feature | Role |
|---|---|
| **Hybrid Tables** | `ADJ_HEADER` and `ADJ_LINE_ITEM` support **row-level ACID transactions** — all inserts succeed or none do |
| **Primary Key / Foreign Key** | Referential integrity enforced at the database level (LINE_ITEM → HEADER) |
| **INDEX** on Hybrid Table | `IDX_LINE_ITEM_ADJ_ID` accelerates the join for later reads |
| **VARIANT column** (`FILTER_CRITERIA`) | Stores the exact filter JSON Sarah used — fully auditable, no schema coupling |
| **Snowpark Python** | Procedure logic: compute deltas, insert header + lines, log history — all in one server-side transaction |

Sarah sees a green success banner: _"Adjustment ADJ-1047 submitted for approval."_

---

### Chapter 6: The Stream Wakes Up

Within seconds, a **Stream** on `ADJ_HEADER` detects the new row.

The **Task** `TASK_LOG_STATUS_CHANGES` fires (runs every minute on warehouse `ADJUSTMENT_TASK_WH`):

1. Reads the stream's change data (INSERT of the new header)
2. Logs it to `ADJ_STATUS_HISTORY` with `CHANGED_BY = 'SYSTEM'`

Because this task has a **child task**, the next link in the chain fires automatically:

**`TASK_AI_SUMMARIZE`** calls Cortex:

```sql
SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2',
       'Summarize this adjustment: FLATTEN on US_BANK / FX_SPOT ...');
```

The AI-generated summary is stored back in `ADJ_HEADER.AI_SUMMARY`:

> _"Flatten adjustment zeroing out $2.3M across 7 FX_SPOT positions for US_BANK on Feb 23, due to erroneous bookings. Ticket FX-4521."_

| Snowflake feature | Role |
|---|---|
| **Stream** (CDC) | Captures every insert/update on `ADJ_HEADER` without polling |
| **Task** (DAG chain) | Serverless task graph: log → summarize, triggered by stream data |
| **Task Warehouse** (`ADJUSTMENT_TASK_WH`) | Dedicated background warehouse — never competes with interactive queries |
| **Cortex COMPLETE (LLM)** | Generates a human-readable summary without leaving Snowflake |

---

### Chapter 7: The Approver Reviews

James, an approver (`ADJ_APPROVER` role), opens the **Audit Trail** page. He sees Sarah's adjustment highlighted as `PENDING_APPROVAL`.

He clicks into it and sees:
- The AI summary (generated automatically in Chapter 6)
- The filter criteria (JSON)
- The line-item detail (all 7 rows with original → delta → new)
- A **status timeline** showing every lifecycle event

James also uses the **AI Assistant** page to ask a natural-language question:

> _"Are there any other pending adjustments for US_BANK this week?"_

Cortex translates this to SQL via the `NL_TO_SQL` UDF, runs it, and returns the answer — **2 other adjustments pending**.

| Snowflake feature | Role |
|---|---|
| **Cortex COMPLETE (NL-to-SQL)** | Converts English question → SQL → result, all server-side |
| **Cortex COMPLETE (CLASSIFY_ADJUSTMENT_RISK)** | Risk label (`LOW` / `MEDIUM` / `HIGH` / `CRITICAL`) shown alongside the adjustment |
| **Hybrid Table reads** | Low-latency point lookups for header + line items |
| **Row Access Policy** | James can see `US_BANK` because he's in the access mapping; other entities are invisible |

Satisfied, James clicks **"Approve"**. The procedure `SP_UPDATE_ADJUSTMENT_STATUS` runs:
- Validates the state transition: `PENDING_APPROVAL → APPROVED` ✔
- Validates James ≠ Sarah (self-approval blocked) ✔
- Updates `ADJ_HEADER.ADJ_STATUS = 'APPROVED'`
- Logs to `ADJ_STATUS_HISTORY`

---

### Chapter 8: Applying the Adjustment

A senior operator (or James himself) now clicks **"Apply"**. The status moves:

```
APPROVED → APPLIED
```

The procedure:
1. Sets `ADJ_STATUS = 'APPLIED'` and `APPLIED_AT = CURRENT_TIMESTAMP()`
2. Logs the transition in `ADJ_STATUS_HISTORY`

**No rows in `FACT_TABLE` are modified.** The original data is sacred.

| Snowflake feature | Role |
|---|---|
| **Hybrid Table** (atomic UPDATE) | Status change is a single-row ACID update |
| **Immutable FACT_TABLE** | Source of truth is never mutated; adjustments are purely additive deltas |

---

### Chapter 9: The Dynamic Table Refreshes — Adjusted Data Appears

This is where the magic happens. **Within ~60 seconds** (the configured `TARGET_LAG = '1 MINUTE'`), the Dynamic Table `MART.FACT_ADJUSTED` auto-refreshes.

Under the hood, the Dynamic Table's definition is:

```sql
-- Original fact rows
SELECT ... FROM FACT.FACT_TABLE

UNION ALL

-- Applied deltas
SELECT ... FROM CORE.ADJ_LINE_ITEM li
JOIN CORE.ADJ_HEADER h ON li.ADJ_ID = h.ADJ_ID
WHERE h.ADJ_STATUS = 'APPLIED'
```

…grouped and summed by all dimensions and business date. The result: **every row now reflects the adjusted value**, with an `IS_ADJUSTED = TRUE` flag on modified rows.

Two downstream Dynamic Tables cascade automatically:

| Dynamic Table | Lag | Purpose |
|---|---|---|
| `MART.FACT_ADJUSTED` | 1 minute | The adjusted fact — consumed by all reports |
| `MART.ADJUSTMENT_IMPACT_SUMMARY` | `DOWNSTREAM` (refreshes after FACT_ADJUSTED) | Pre-aggregated impact by type, entity, date |
| `MART.DAILY_ADJUSTMENT_ACTIVITY` | 5 minutes | Daily counts, users, status breakdown |

| Snowflake feature | Role |
|---|---|
| **Dynamic Table** (incremental refresh) | Replaces the old manual `UNION ALL + GROUP BY` view and the separate materialized table — fully automated |
| **Dynamic Table chaining** (`DOWNSTREAM` lag) | Impact summary waits for FACT_ADJUSTED to be current before it refreshes — no race conditions |
| **Zero ETL orchestration** | No Airflow, no dbt run, no cron job — Snowflake handles the refresh pipeline internally |

---

### Chapter 10: Sarah Checks the Dashboard

Sarah opens the **Impact Dashboard**. Everything is live:

- **KPI cards**: Total adjusted amount, number of adjustments applied today
- **Impact by Type**: Bar chart showing Flatten / Scale / Roll totals
- **Before vs. After**: Side-by-side comparison pulled from `FACT_ADJUSTED` where `IS_ADJUSTED = TRUE`
- **Activity Over Time**: Line chart from `DAILY_ADJUSTMENT_ACTIVITY`
- **Top Adjusters**: Leaderboard from `ADJ_HEADER`

She confirms: the 7 FX_SPOT rows for `US_BANK` on Feb 23 now show **$0** — exactly what was intended.

| Snowflake feature | Role |
|---|---|
| **Dynamic Tables** | All dashboard queries hit pre-materialized tables — fast, cheap, always fresh |
| **Masking Policy** | If a viewer-only analyst opens the same page, `CREATED_BY` fields show `***MASKED***` |
| **Warehouse** (`ADJUSTMENT_WH`) | Serves all interactive dashboard queries |

---

### Chapter 11: The Daily AI Check

At **8:00 AM ET** the next morning, a scheduled task runs automatically:

```sql
TASK_ANOMALY_CHECK  (CRON: 0 8 * * * America/New_York)
  → CALL AI.SP_DETECT_ADJUSTMENT_ANOMALIES()
```

This procedure:
1. Queries yesterday's adjustment activity
2. Computes metrics: count, total delta, max single delta, unique users
3. Flags anomalies (e.g., unusually large adjustments, adjustments outside business hours, single user making too many changes)

The results are stored in `AI.ANOMALY_DETECTION_RESULTS` and surfaced in the **AI Assistant** page.

| Snowflake feature | Role |
|---|---|
| **Scheduled Task** (CRON) | Fully serverless, runs on its own warehouse, no external scheduler |
| **Cortex AI** | Powers the anomaly detection logic and narrative generation |
| **Notification Integration** _(optional)_ | Can send email/Slack alerts when anomalies are found |

---

### Chapter 12: If Something Goes Wrong — Reversal

Weeks later, Sarah realizes one of the 7 rows was actually correct and shouldn't have been flattened. She opens the Audit Trail, finds ADJ-1047, and clicks **"Reverse"**.

The procedure `SP_REVERSE_ADJUSTMENT`:
1. Reads the original line items for ADJ-1047
2. Inserts a **new** adjustment (ADJ-1048) with **negated deltas** (i.e., `+original_value` to undo the `-original_value`)
3. Sets ADJ-1047 status to `REVERSED`
4. The new ADJ-1048 goes through the same approval workflow

The Dynamic Table refreshes again. The values are restored. The full audit chain is preserved:

```
ADJ-1047: DRAFT → PENDING → APPROVED → APPLIED → REVERSED
ADJ-1048: DRAFT → PENDING → APPROVED → APPLIED  (the reversal)
```

| Snowflake feature | Role |
|---|---|
| **Delta pattern** | Reversal is just another delta — no DELETE, no UPDATE on fact data |
| **Hybrid Tables** | New header + line items inserted atomically |
| **Dynamic Table** | Auto-refreshes to reflect the undone adjustment |
| **Full audit trail** | Both the original and the reversal are permanently recorded |

---

### The Complete Flow — One Picture

```
 ┌─────────────────────────────────────────────────────────────────────────────┐
 │                          USER JOURNEY TIMELINE                              │
 │                                                                             │
 │  ① LOGIN                ② FILTER               ③ CONFIGURE                 │
 │  ┌──────────┐           ┌──────────┐           ┌──────────┐                │
 │  │ SiS App  │──────────▶│ Hybrid   │──────────▶│ Session  │                │
 │  │ loads    │  RBAC +   │ Table    │  Row      │ State in │                │
 │  │          │  RAP      │ reads    │  Access   │ Streamlit│                │
 │  └──────────┘           └──────────┘  Policy   └──────────┘                │
 │       │                                              │                      │
 │       │            ④ PREVIEW                         │                      │
 │       │            ┌──────────────────┐              │                      │
 │       │            │ Snowpark SP      │◀─────────────┘                      │
 │       │            │ (read-only,      │                                     │
 │       │            │  returns TABLE)  │                                     │
 │       │            └──────────────────┘                                     │
 │       │                     │                                               │
 │       │            ⑤ SUBMIT                                                 │
 │       │            ┌──────────────────┐                                     │
 │       │            │ Snowpark SP      │                                     │
 │       │            │ (ACID write to   │                                     │
 │       │            │  Hybrid Tables)  │                                     │
 │       │            └────────┬─────────┘                                     │
 │       │                     │                                               │
 │       │            ⑥ STREAM + TASK CHAIN                                    │
 │       │            ┌──────────────────┐    ┌─────────────────┐              │
 │       │            │ Stream detects   │───▶│ Task logs +     │              │
 │       │            │ new row (CDC)    │    │ Cortex AI       │              │
 │       │            └──────────────────┘    │ summarizes      │              │
 │       │                                    └─────────────────┘              │
 │       │            ⑦ APPROVAL                                               │
 │       │            ┌──────────────────┐                                     │
 │       │            │ Approver reviews │                                     │
 │       │            │ (NL-to-SQL,      │                                     │
 │       │            │  risk scoring)   │                                     │
 │       │            └────────┬─────────┘                                     │
 │       │                     │                                               │
 │       │            ⑧ APPLY                                                  │
 │       │            ┌──────────────────┐                                     │
 │       │            │ Status → APPLIED │                                     │
 │       │            │ (Hybrid Table    │                                     │
 │       │            │  atomic update)  │                                     │
 │       │            └────────┬─────────┘                                     │
 │       │                     │                                               │
 │       │            ⑨ DYNAMIC TABLE REFRESH (~60s)                           │
 │       │            ┌──────────────────────────────────────────┐             │
 │       │            │ FACT_ADJUSTED auto-materializes          │             │
 │       │            │ ├─▶ IMPACT_SUMMARY  (DOWNSTREAM)        │             │
 │       │            │ └─▶ DAILY_ACTIVITY  (5 min lag)         │             │
 │       │            └────────┬─────────────────────────────────┘             │
 │       │                     │                                               │
 │       │            ⑩ DASHBOARD                                              │
 │       ▼            ┌──────────────────┐                                     │
 │  ┌──────────┐      │ Charts, KPIs,    │                                     │
 │  │ Sarah    │─────▶│ before/after —   │                                     │
 │  │ confirms │      │ all from Dynamic │                                     │
 │  │ data ✓   │      │ Tables           │                                     │
 │  └──────────┘      └──────────────────┘                                     │
 └─────────────────────────────────────────────────────────────────────────────┘
```

### Snowflake Features — Where They Appear

| # | Step | Snowflake Features |
|---|------|--------------------|
| ① | Login | **Streamlit in Snowflake**, RBAC roles, Row Access Policy |
| ② | Filter | **Hybrid Table** reads, Row Access Policy, Object Tagging |
| ③ | Configure | Streamlit session state (client-side) |
| ④ | Preview | **Snowpark Python SP** (TABLE return), Warehouse |
| ⑤ | Submit | **Snowpark Python SP**, **Hybrid Tables** (ACID writes), PK/FK, VARIANT column |
| ⑥ | CDC + AI | **Stream** (change tracking), **Task DAG**, **Cortex COMPLETE** (LLM) |
| ⑦ | Approval | **Cortex NL-to-SQL**, **Cortex Risk Classification**, Hybrid Table reads |
| ⑧ | Apply | **Hybrid Table** atomic UPDATE, status history logging |
| ⑨ | Refresh | **Dynamic Tables** (incremental, chained, DOWNSTREAM lag) |
| ⑩ | Dashboard | **Dynamic Tables** (pre-materialized), **Masking Policy**, Warehouse |
| ⑪ | Daily AI | **Scheduled Task** (CRON), **Cortex Anomaly Detection** |
| ⑫ | Reversal | Delta pattern (negated deltas), **Hybrid Tables**, **Dynamic Table** auto-refresh |

---

## Project Structure

```
adjustment/
├── README.md
├── snowflake/
│   ├── 01_setup_database.sql
│   ├── 02_tables.sql
│   ├── 03_dynamic_tables.sql
│   ├── 04_streams_tasks.sql
│   ├── 05_stored_procedures.sql
│   ├── 06_cortex_integration.sql
│   ├── 07_security_governance.sql
│   └── 08_sample_data.sql
├── streamlit/
│   ├── app.py
│   ├── pages/
│   │   ├── 1_Apply_Adjustment.py
│   │   ├── 2_Audit_Trail.py
│   │   ├── 3_AI_Assistant.py
│   │   └── 4_Dashboard.py
│   └── utils/
│       ├── snowflake_conn.py
│       ├── adjustment_engine.py
│       └── cortex_helpers.py
└── environment.yml
```

---

## Deployment

### Option A: Streamlit in Snowflake (Recommended)

```sql
CREATE STREAMLIT ADJUSTMENT_DB.APP.ADJUSTMENT_ENGINE
  ROOT_LOCATION = '@ADJUSTMENT_DB.APP.STREAMLIT_STAGE'
  MAIN_FILE = 'app.py'
  QUERY_WAREHOUSE = 'ADJUSTMENT_WH';
```

### Option B: External Streamlit

```bash
pip install -r requirements.txt
streamlit run streamlit/app.py
```

---

## Getting Started

1. Execute SQL scripts in order (`01` → `08`) against your Snowflake account
2. Deploy the Streamlit app (Option A or B)
3. Load your fact table data or use the sample data from `08_sample_data.sql`
4. Access the app and start creating adjustments

---

## Visibility Improvements

The #1 user complaint — **lack of visibility** — is addressed through:

1. **Full Audit Trail**: Every adjustment has who, what, when, why, status history
2. **Before/After Preview**: See impact before applying
3. **AI Summarization**: Natural language summaries of adjustment activity
4. **Impact Dashboard**: Visual impact analysis with drill-down
5. **Email/Slack Notifications**: Real-time alerts on adjustment lifecycle
6. **Approval Workflow**: No adjustment applied without review
7. **Reversal Capability**: Any applied adjustment can be cleanly reversed
