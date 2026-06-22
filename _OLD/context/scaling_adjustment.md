# Scaling_Adjustment — Adjustment Context

> **Source:** Extracted from [adjustment_metadata.md](adjustment_metadata.md) + source code in `codes/scaling_adjustment/`
>
> **Last updated:** 2026-03-28
>
> **Goal:** This is the most complex of the three adjustment pipelines. Part of the
> redesign project — make the adjustment process simpler, easy for everyone in the team
> to understand, and quick to process.

---

## Overview

| Property | Value |
|---|---|
| **Description** | Most used adjustment during the day. Comes from SSRS reporting; Python code identifies new adjustments and exports the file |
| **File Name** | `Scaling_Adjustment_\|:YYYYMMDD:\|.csv` |
| **Path** | `\\mfil\proddfs\fsroot\sfs-prod\raptor\extracts\general\VaRUpload` |
| **Table Destination** | `STAGING.IDM_SCALING_ADJUSTMENT` |
| **Rows Per File** | Can be more than 1000 |

---

## File Layout (columns in order)

```
ID, COBID, ProcessType, AdjustmentType, SourceCOBID, EntityCode, SourceSystemCode,
DepartmentCode, BookCode, CurrencyCode, TradeTypology, TradeCode, Strategy, ScaleFactor,
TraderCode, VaRComponentId, VaRSubComponentId, GuaranteedEntity, RegionKey, ScenarioDateId,
AdjustmentValueInUSD, ErrorMessage, ApprovalId, Reason, ActiveStatus, GlobalId, UserName,
CreatedDate, ExtractDate, InstrumentCode, SimulationName, ProductCategoryAttributes,
Simulation_Source, DayType, CurveCode, MeasureTypeCode, TenorCode, UnderlyingTenorCode
```

---

## End-to-End Pipeline

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│  1. CSV file lands on network share → ingested into STAGING.IDM_SCALING_ADJ     │
└──────────────────┬───────────────────────────────────────────────────────────────┘
                   │
                   ▼  (CDC stream)
┌──────────────────────────────────────────────────────────────────────────────────┐
│  2. STAGING.IDM_SCALING_ADJUSTMENT_STREAM                                       │
│     Captures INSERT actions via METADATA$ACTION                                 │
└──────────────────┬───────────────────────────────────────────────────────────────┘
                   │
                   ▼  (view)
┌──────────────────────────────────────────────────────────────────────────────────┐
│  3. STAGING.IDM_SCALING_ADJUSTMENT_STREAM_DATA                                  │
│     • Takes only the LATEST version of each (COBID, GLOBAL_ID)                  │
│     • Uses EXCEPT against DIMENSION.ADJUSTMENT to find actual changes           │
│     • Derives IS_10DAY flag from DIMENSION.VAR_SUB_COMPONENT                    │
│     • Determines RUN_STATUS via sign-off checks (VaR vs Sensitivity/FRTB)       │
│     • Maps ACTIVE_STATUS = 'Deleted' → IS_DELETED = TRUE                        │
└──────────────────┬───────────────────────────────────────────────────────────────┘
                   │
                   ▼  (Task 1)
┌──────────────────────────────────────────────────────────────────────────────────┐
│  4. PUBLISH_SCALING_ADJUSTMENT_TASK                                             │
│     MERGE INTO DIMENSION.ADJUSTMENT on (GLOBAL_REFERENCE, COBID)                │
└──────────────────┬───────────────────────────────────────────────────────────────┘
                   │
         ┌─────────┼─────────────────────┐
         ▼         ▼                     ▼
┌────────────┐ ┌────────────┐ ┌──────────────────────┐
│  Task 2a   │ │  Task 2b   │ │  Task 2c             │
│  VaR       │ │  Stress    │ │  FRTB/Sensitivity     │
│  CALL      │ │  CALL      │ │  CALL                │
│  PROCESS_  │ │  PROCESS_  │ │  ADJUSTMENT.         │
│  ADJUST-   │ │  ADJUST-   │ │  PROCESS_            │
│  MENTS     │ │  MENTS     │ │  ADJUSTMENT          │
│  ('VAR')   │ │  ('STRESS')│ │  (process_type,      │
└────────────┘ └────────────┘ │   action, cobid)     │
                              └──────────────────────┘
```

> **Key difference vs. VaR_Upload & Global_Adj:** After the publish task, **three parallel
> child tasks** run — one per process type. VaR and Stress use the legacy JavaScript
> procedure (`FACT.PROCESS_ADJUSTMENTS`). FRTB/Sensitivity uses the **new Python procedure**
> (`ADJUSTMENT.PROCESS_ADJUSTMENT`) which is the target architecture for all process types.

---

## All Tables & Objects Involved

| Object | Type | Role |
|---|---|---|
| `STAGING.IDM_SCALING_ADJUSTMENT` | Table | Landing table — raw CSV rows |
| `STAGING.IDM_SCALING_ADJUSTMENT_STREAM` | Stream | CDC capture on landing table |
| `STAGING.IDM_SCALING_ADJUSTMENT_STREAM_DATA` | View | Change detection, IS_10DAY derivation, sign-off status |
| `DIMENSION.ADJUSTMENT` | Table | Adjustment dimension (**shared with VaR_Upload**) |
| `DIMENSION.VAR_SUB_COMPONENT` | Lookup | 10-day VaR component detection |
| `DIMENSION.ENTITY` | Lookup | Entity key resolution |
| `DIMENSION.BOOK` | Lookup | Book key resolution |
| `DIMENSION.TRADE` | Lookup | Trade key resolution |
| `DIMENSION.COMMON_INSTRUMENT` | Lookup | Instrument key resolution |
| `DIMENSION.SHIFT_TYPE` | Lookup | Shift type resolution (Stress) |
| `DIMENSION.RISK_CLASS` | Lookup | Risk class resolution (FRTB/Sensitivity) |
| `DIMENSION.LIQUIDITY_HORIZON` | Lookup | Liquidity horizon resolution (FRTB) |
| `DIMENSION.STRESS_SIMULATION` | Lookup | Stress simulation resolution |
| `FACT.VAR_MEASURES` | Fact table | Source for VaR adjustments (read from, scale against) |
| `FACT.VAR_MEASURES_ADJUSTMENT` | Fact table | Target — VaR adjustment detail rows |
| `FACT.VAR_MEASURES_ADJUSTMENT_SUMMARY` | Fact table | Target — VaR adjustment summarised rows |
| `FACT.STRESS_MEASURES` | Fact table | Source for Stress adjustments |
| `FACT.STRESS_MEASURES_ADJUSTMENT` | Fact table | Target — Stress adjustment detail rows |
| `FACT.STRESS_MEASURES_ADJUSTMENT_SUMMARY` | Fact table | Target — Stress adjustment summarised rows |
| `FACT.ES_MEASURES` | Fact table | Source for ES adjustments (configured but not triggered) |
| `FACT.ES_MEASURES_ADJUSTMENT` | Fact table | Target — ES adjustment detail rows |
| `FACT.ES_MEASURES_ADJUSTMENT_SUMMARY` | Fact table | Target — ES adjustment summarised rows |
| `FACT.VAR_WINDOW_PNL_VECTOR_ELEMENT_ORDINAL` | Lookup | Scenario date filtering by VaR window |
| `DIMENSION.VAR_WINDOW` | Lookup | Window names (Stressed VaR, 1/2/3 Year VaR) |
| `BATCH.RUN_LOG` | Table | Run logging |
| `BATCH.SEQ_RUN_LOG` | Sequence | Run log ID generation |
| `BATCH.PUBLISH_VAR_SIGNOFF_STATUS` | Lookup | VaR sign-off check |
| `BATCH.PUBLISH_SIGNOFF_STATUS_EXCEPTION` | Lookup | Sensitivity/FRTB sign-off check |
| `METADATA.POWERBI_PUBLISH_INFO` | Table | PowerBI refresh tracking |
| `METADATA.POWERBI_PUBLISH_DETAIL` | Table | PowerBI refresh detail |
| `METADATA.POWERBI_ACTION` | Table | PowerBI action queue |
| `METADATA.VW_POWERBI_ACTION_INSERT_SOURCE` | View | PowerBI action deduplication |
| `ADJUSTMENT.ADJUSTMENTS_SETTINGS` | Config table | Seed table — maps process types to fact/adjustment tables, metrics, PKs |
| `ADJUSTMENT.PROCESS_ADJUSTMENT` | Stored Procedure (Python) | **New** — config-driven adjustment processor for Direct + Scale actions |
| `RAVEN.LOG_STAGE_ME_STATUS` | Table | FRTB/Sensitivity log entry (legacy — may be removed) |

> **33+ objects** involved — by far the most complex pipeline.

---

## CDC View Logic — `IDM_SCALING_ADJUSTMENT_STREAM_DATA`

Unlike VaR_Upload and Global_Adj, this view uses a **different CDC pattern**:

### Step 1 — Latest Version Only

```sql
WITH ADJUSTMENT_LAST_VERSION AS (
    SELECT COBID, GLOBAL_ID, MAX(RAVEN_STAGE_TIMESTAMP) RAVEN_STAGE_TIMESTAMP
    FROM STAGING.IDM_SCALING_ADJUSTMENT_STREAM
    GROUP BY COBID, GLOBAL_ID
)
```

If the same `(COBID, GLOBAL_ID)` appears multiple times in the stream, only the latest
`RAVEN_STAGE_TIMESTAMP` is kept. This deduplicates re-sent adjustments.

### Step 2 — Change Detection via EXCEPT

```sql
ADJUSTMENT_TRACK AS (
    -- Stream rows (INSERT only, latest version)
    SELECT ... FROM STAGING.IDM_SCALING_ADJUSTMENT_STREAM
    WHERE METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = 'FALSE'
      AND EXISTS (... latest version check ...)
    EXCEPT
    -- Current state in DIMENSION.ADJUSTMENT
    SELECT ... FROM DIMENSION.ADJUSTMENT
)
```

Instead of classifying into I/U/D types like the other pipelines, this view:
- Takes all INSERT stream rows (latest version)
- `EXCEPT`s the current state of `DIMENSION.ADJUSTMENT`
- Only rows with **actual value changes** survive

> ⚠️ **No explicit DML_TYPE classification** — unlike VaR_Upload and Global_Adj. The task
> always does MERGE (UPDATE or INSERT) — there is no separate delete handling in the task.

### Step 3 — IS_10DAY Flag Derivation

```sql
CASE
    WHEN SC.VAR_SUB_COMPONENT_ID IS NOT NULL THEN 'Y'   -- sub-component is 10-day
    WHEN SC1.VAR_COMPONENT_ID IS NOT NULL THEN 'Y'      -- component has 10-day sub-components
    WHEN SC1.VAR_COMPONENT_ID = 11 THEN 'Y'             -- component 11 = 10Day VaR
    ELSE 'N'
END AS IS_10DAY
```

### Step 4 — Sign-Off Status (dual logic)

```sql
CASE
    -- VaR process types: use BATCH.PUBLISH_VAR_SIGNOFF_STATUS
    WHEN PROCESS_TYPE NOT IN ('Sensitivity', 'FRTB') THEN
        CASE WHEN PVSO.PUBLISH_STATUS IS NULL THEN 'Pending'
             WHEN IS_10DAY = 'Y' THEN 'Pending'         -- 10-day always bypasses sign-off
             ELSE 'Rejected - SignedOff'
        END
    -- Sensitivity/FRTB: use BATCH.PUBLISH_SIGNOFF_STATUS_EXCEPTION
    WHEN PROCESS_TYPE IN ('Sensitivity', 'FRTB') THEN
        CASE WHEN PSOE.PUBLISH_STATUS IS NULL THEN 'Pending'
             ELSE 'Rejected - SignedOff'
        END
    ELSE 'Pending'
END AS RUN_STATUS
```

**Two different sign-off tables** are checked depending on `PROCESS_TYPE`:

| Process Type | Sign-Off Table | 10-Day Bypass? |
|---|---|---|
| VaR (and anything not Sensitivity/FRTB) | `BATCH.PUBLISH_VAR_SIGNOFF_STATUS` | ✅ Yes |
| Sensitivity, FRTB | `BATCH.PUBLISH_SIGNOFF_STATUS_EXCEPTION` (SubType = 'NonCVA') | ❌ No |

### Step 5 — ActiveStatus → IS_DELETED Mapping

```sql
IFF(ACTIVE_STATUS = 'Deleted', TRUE, FALSE) AS IS_DELETED
```

The source file has `ActiveStatus` as a text field; the view converts it to a boolean.

---

## Task 1 — MERGE into DIMENSION.ADJUSTMENT

### `DVLP_RAPTOR_NEWADJ.STAGING.PUBLISH_SCALING_ADJUSTMENT_TASK`

| Property | Value |
|---|---|
| Warehouse | `PROD_RAPTOR_WH_M` |
| Schedule | 1 minute |
| Guard | `SYSTEM$STREAM_HAS_DATA('STAGING.IDM_SCALING_ADJUSTMENT_STREAM')` |
| Source | `STAGING.IDM_SCALING_ADJUSTMENT_STREAM_DATA` |
| Target | `DIMENSION.ADJUSTMENT` |

**Match key:**

```sql
tgt.GLOBAL_REFERENCE = src.GLOBAL_ID AND tgt.COBID = src.COBID
```

### MERGE Behaviour

| Condition | Action |
|---|---|
| Matched | UPDATE all 36 fields; derives `DELETED_BY`/`DELETED_DATE` from `IS_DELETED` |
| Not matched | INSERT full row with same `IS_DELETED` → `DELETED_BY`/`DELETED_DATE` logic |

> Unlike VaR_Upload (10-column match) or Global_Adj (1-column), this uses a **2-column composite key**.

---

## Tasks 2a/2b/2c — Parallel Downstream Processing

After Task 1, **three child tasks** run in parallel:

### Task 2a — `LOAD_VAR_SCALING_ADJUSTMENT_TASK`

| Property | Value |
|---|---|
| Warehouse | `PROD_RAPTOR_WH_M` |
| Trigger | `AFTER PUBLISH_SCALING_ADJUSTMENT_TASK` |
| Action | `CALL FACT.PROCESS_ADJUSTMENTS('VAR', '0')` |
| Fact source | `FACT.VAR_MEASURES` |
| Fact target | `FACT.VAR_MEASURES_ADJUSTMENT` + `_SUMMARY` |
| Metric | `PNL_VECTOR_VALUE_IN_USD` |

### Task 2b — `LOAD_STRESS_SCALING_ADJUSTMENT_TASK`

| Property | Value |
|---|---|
| Warehouse | `PROD_RAPTOR_WH_M` |
| Trigger | `AFTER PUBLISH_SCALING_ADJUSTMENT_TASK` |
| Action | `CALL FACT.PROCESS_ADJUSTMENTS('STRESS', '0')` |
| Fact source | `FACT.STRESS_MEASURES` |
| Fact target | `FACT.STRESS_MEASURES_ADJUSTMENT` + `_SUMMARY` |
| Metric | `SIMULATION_PL_IN_USD` |

### Task 2c — `LOAD_FRTB_SENSITIVITY_SCALING_ADJUSTMENT_TASK`

| Property | Value |
|---|---|
| Warehouse | Serverless (XSMALL) |
| Trigger | `AFTER PUBLISH_SCALING_ADJUSTMENT_TASK` |
| Action | `CALL ADJUSTMENT.PROCESS_ADJUSTMENT(process_type, adjustment_action, cobid)` |
| Procedure | `ADJUSTMENT.PROCESS_ADJUSTMENT` — **Python (Snowpark)** |
| Language | Python 3.11 |

> Task 2c calls the **new Python procedure** `ADJUSTMENT.PROCESS_ADJUSTMENT` which replaces
> the legacy dummy log entry. This procedure is the target-state design and will eventually
> replace the JavaScript `FACT.PROCESS_ADJUSTMENTS` used by Tasks 2a and 2b.

---

## Core Stored Procedure: `FACT.PROCESS_ADJUSTMENTS`

This is the **generic adjustment processing engine** — one procedure for VaR, ES, and Stress.

### Parameters

| Parameter | Value |
|---|---|
| `P_PROCESS_TYPE` | `'VAR'`, `'ES'`, or `'STRESS'` |
| `DEBUG_FLAG` | `'0'` (production) |

### Process Type → Table Mapping

| Process Type | Fact Source | Adjustment Detail | Adjustment Summary | Metric |
|---|---|---|---|---|
| `VAR` | `FACT.VAR_MEASURES` | `FACT.VAR_MEASURES_ADJUSTMENT` | `FACT.VAR_MEASURES_ADJUSTMENT_SUMMARY` | `PNL_VECTOR_VALUE_IN_USD` |
| `ES` | `FACT.ES_MEASURES` | `FACT.ES_MEASURES_ADJUSTMENT` | `FACT.ES_MEASURES_ADJUSTMENT_SUMMARY` | `PNL_VECTOR_VALUE_IN_USD` |
| `STRESS` | `FACT.STRESS_MEASURES` | `FACT.STRESS_MEASURES_ADJUSTMENT` | `FACT.STRESS_MEASURES_ADJUSTMENT_SUMMARY` | `SIMULATION_PL_IN_USD` |

### Procedure Steps (per adjustment)

The procedure loops over each pending adjustment and processes it inside a transaction:

| Step | Action | Detail |
|---|---|---|
| 1 | **Stage pending adjustments** | `CREATE TEMP TABLE FACT.TEMP_STAGE_ADJUSTMENT` from `DIMENSION.ADJUSTMENT` where `RUN_STATUS = 'Pending'` and `ADJUSTMENT_TYPE IN ('Flatten', 'Scale')` |
| 2 | **Set Running** | `UPDATE DIMENSION.ADJUSTMENT SET RUN_STATUS = 'Running'` for staged rows |
| 3 | **Concurrency check** | If 0 rows updated → another session is already processing; exit early |
| 4 | **Loop** each adjustment | Orders by `COBID, CREATED_DATE, ADJUSTMENT_ID` |
| 5 | **Create RUN_LOG** | Generates sequence ID, inserts into `BATCH.RUN_LOG` |
| 6 | **Begin transaction** | Per-adjustment transaction boundary |
| 7 | **Delete existing** | Removes from both detail + summary fact tables for this adjustment |
| 8 | **Determine action** | See Action Logic below |
| 9 | **Execute adjustment** | Calls `FACT.PROCESS_ADJUSTMENT_STEP` one or more times |
| 10 | **Re-calc summary** | Aggregates detail → summary with `SCALE_FACTOR = 1`, `IS_OFFICIAL_SOURCE = true` |
| 11 | **Set Processed** | `UPDATE DIMENSION.ADJUSTMENT SET RUN_STATUS = 'Processed'` + record count |
| 12 | **Commit** | Transaction committed |
| 13 | **Update run log** | Stores full adjRes JSON with all step details |
| 14 | **Error handling** | On failure: rollback, set `RUN_STATUS = 'Error'`, log error |
| 15 | **PowerBI update** | After loop: `CALL FACT.UPDATE_POWERBI_FOR_ADJUSTMENTS(...)` |

### Action Logic (Flatten vs. Scale)

The procedure determines the action based on `ADJUSTMENT_TYPE` and whether `SOURCE_COBID == COBID`:

```
┌──────────────────────────────────────────────────────────────────────────┐
│  ADJUSTMENT_TYPE = 'Flatten'                                            │
│  → ACTION = 'FLATTEN TARGET COB'                                       │
│  → SCALE_FACTOR set to -1                                              │
│  → Insert records from fact × -1 (nets to zero)                        │
│  → 1 step                                                              │
└──────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────┐
│  ADJUSTMENT_TYPE = 'Scale' AND SOURCE_COBID == COBID                   │
│  → ACTION = 'SCALE CURRENT COB'                                       │
│  → SCALE_FACTOR set to (scale_factor - 1)                             │
│  → Insert records from fact × (sf - 1) → fact + adj = fact × sf       │
│  → 1 step                                                              │
└──────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────┐
│  ADJUSTMENT_TYPE = 'Scale' AND SOURCE_COBID != COBID                   │
│  → ACTION = 'SCALE SOURCE COB'                                        │
│  → 3 steps:                                                            │
│    Step 1: Flatten target COB (× -1)                                   │
│    Step 2: Insert from source COB fact × scale_factor                  │
│    Step 3: Insert from source COB adjustments × scale_factor           │
│  → Effectively: roll + scale                                           │
└──────────────────────────────────────────────────────────────────────────┘
```

### Full Entity Roll vs. Filtered Adjustment

The procedure checks if **any filter columns** are present in the adjustment record:

```javascript
if ("BOOK_CODE" in adjRec || "DEPARTMENT_CODE" in adjRec || "TRADER_CODE" in adjRec ||
    "GUARANTEED_ENTITY" in adjRec || "REGION_KEY" in adjRec || "TRADE_CODE" in adjRec ||
    "TRADE_TYPOLOGY" in adjRec || "STRATEGY" in adjRec || "INSTRUMENT_CODE" in adjRec ||
    "VAR_COMPONENT_ID" in adjRec || "VAR_SUB_COMPONENT_ID" in adjRec || ... )
{
    full_entity_roll = false   // filtered → include ALL scenario date windows
}
```

- **Full entity roll** (`full_entity_roll = true`): Only processes 4 standard VaR windows (Stressed VaR, 1/2/3 Year VaR)
- **Filtered adjustment** (`full_entity_roll = false`): Processes ALL scenario dates (`INCLUDE_ALL_WINDOWS = true`)

---

## Sub-Procedure: `FACT.PROCESS_ADJUSTMENT_STEP`

This is the **dynamic SQL engine** that generates and executes the actual INSERT/DELETE statements.

### Parameters

| # | Parameter | Purpose |
|---|---|---|
| 1 | `P_SOURCE_TABLE` | Where to read from (fact or adjustment table). `NULL` = DELETE mode |
| 2 | `P_TARGET_TABLE` | Where to write to (adjustment table) |
| 3 | `P_METRIC_NAME` | Column to scale (e.g. `PNL_VECTOR_VALUE_IN_USD`) |
| 4 | `P_RUN_LOG_ID` | Run log entry |
| 5 | `P_ADJUSTMENT_RECORD` | JSON of the adjustment row from `DIMENSION.ADJUSTMENT` |
| 6 | `P_AGGREGATE_RESULTS` | `'true'` → GROUP BY + SUM; `'false'` → row-level |
| 7 | `P_DESCRIPTION` | Logging description |

### How It Works

1. **Gets column lists** from source and target tables dynamically via `BATCH.GET_COLUMNS`
2. **Finds common columns** between source and target
3. **Builds WHERE clause dynamically** by checking which filter fields exist in the adjustment record JSON
4. **Resolves dimension keys** dynamically — only adds JOIN conditions for filters that are present:

| Filter Field(s) | Dimension Lookup | Target Column |
|---|---|---|
| `ENTITY_CODE` | `DIMENSION.ENTITY` | `ENTITY_KEY` |
| `BOOK_CODE`, `DEPARTMENT_CODE`, `TRADER_CODE`, `GUARANTEED_ENTITY`, `REGION_KEY` | `DIMENSION.BOOK` | `BOOK_KEY` |
| `TRADE_CODE`, `TRADE_TYPOLOGY`, `STRATEGY` | `DIMENSION.TRADE` | `TRADE_KEY` |
| `INSTRUMENT_CODE` | `DIMENSION.COMMON_INSTRUMENT` | `COMMON_INSTRUMENT_KEY` |
| `VAR_COMPONENT_ID`, `VAR_SUB_COMPONENT_ID`, `DAY_TYPE` | `DIMENSION.VAR_SUB_COMPONENT` | `VAR_SUBCOMPONENT_ID` |
| `SHIFT_TYPE` | `DIMENSION.SHIFT_TYPE` | `SHIFT_TYPE_KEY` |
| `RISK_CLASS`, `RISK_COMPONENT`, `RISK_SUB_COMPONENT` | `DIMENSION.RISK_CLASS` | `RISK_CLASS_KEY` |
| `LIQUIDITY_HORIZON` | `DIMENSION.LIQUIDITY_HORIZON` | `LIQUIDITY_HORIZON_KEY` |
| `SIMULATION_NAME`, `SIMULATION_SOURCE` | `DIMENSION.STRESS_SIMULATION` | `STRESS_SIMULATION_KEY` |
| `CURRENCY_CODE` | Direct | `CURRENCY_CODE` or `TRADE_CURRENCY` |
| `SOURCE_SYSTEM_CODE` | Direct | `SOURCE_SYSTEM_CODE` |
| `SCENARIO_DATE_ID` | Direct | `SCENARIO_DATE_ID` |
| `IS_OFFICIAL_SOURCE` | Direct | `IS_OFFICIAL_SOURCE` |

5. **Applies VaR window filter** when `INCLUDE_ALL_WINDOWS = false`:
   - Restricts to scenario dates in 4 windows: Stressed VaR, 1/2/3 Year VaR
   - Special handling for VaR component 11 (10Day VaR)

6. **Executes** the dynamically built SQL with bind parameters (29 possible binds)

7. **Returns** JSON result with status, SQL text, bind values, row count, and query ID

---

## New Procedure: `ADJUSTMENT.PROCESS_ADJUSTMENT` (Python / Snowpark)

This is the **new replacement procedure** — written in Python 3.11 using Snowpark.
Currently used by Task 2c (FRTB/Sensitivity) and intended to replace the legacy
JavaScript procedures for all process types.

### Parameters

| Parameter | Type | Purpose |
|---|---|---|
| `process_type` | STRING | e.g. `'VAR'`, `'ES'`, `'STRESS'`, `'FRTB'`, `'Sensitivity'` |
| `adjustment_action` | STRING | `'Direct'` or `'Scale'` |
| `cobid` | INT | Close of Business ID |

### Configuration-Driven via `ADJUSTMENT.ADJUSTMENTS_SETTINGS`

Instead of hard-coding table names, the procedure reads a **seed/config table**:

| Setting Column | Purpose | Example |
|---|---|---|
| `FACT_TABLE` | Source fact table | `FACT.VAR_MEASURES` |
| `FACT_AJUSTED_TABLE` | Source when source COB ≠ target COB | `FACT.VAR_MEASURES_ADJUSTMENT` |
| `FACT_TABLE_PK` | Primary key column(s), `;`-separated | `ENTITY_KEY;BOOK_KEY;...` |
| `ADJUSTMENTS_TABLE` | Target adjustment detail table | `FACT.VAR_MEASURES_ADJUSTMENT` |
| `ADJUSTMENTS_SUMMARY_TABLE` | Target summary table (optional) | `FACT.VAR_MEASURES_ADJUSTMENT_SUMMARY` |
| `ADJUSTMENT_BASE_TABLE` | Base adjustment dimension | `DIMENSION.ADJUSTMENT` variant |
| `METRIC_NAME` | Local currency metric column | `PNL_VECTOR_VALUE` |
| `METRIC_USD_NAME` | USD metric column | `PNL_VECTOR_VALUE_IN_USD` |

> ✅ **Key simplification:** All table names and metric columns are configurable —
> adding a new process type only requires a new row in the settings table.

### Direct Adjustment Flow

1. Filter adjustments where `adjustment_action = 'Direct'` and `is_positive_adjustment = True`
2. Map adjustment columns to fact table columns via `check_columns()` (missing keys default to `-1`, others to `NaN`)
3. Delete existing rows from fact adjustment table matching `(COBID, ADJUSTMENT_ID)`
4. Exclude soft-deleted adjustments (`IS_DELETED = True`)
5. Insert valid adjustments via `session.write_pandas()`
6. Update `DIMENSION.ADJUSTMENT` status → `'Processed'`
7. Row count validation (prints warning, does not raise)

### Scale Adjustment Flow

1. Filter adjustments where `adjustment_action != 'Direct'` and `is_positive_adjustment = True`
2. Dynamically determine join columns between fact and adjustment tables
3. Build a single `CREATE TEMPORARY TABLE ... AS` statement with three `UNION ALL` branches:
   - **Scale Current COB:** `fact.COBID = adjust.SOURCE_COBID AND adjust.COBID = adjust.SOURCE_COBID`
   - **Scale Other COB:** `fact.COBID = adjust.SOURCE_COBID AND adjust.COBID <> adjust.SOURCE_COBID` (reads from `FACT_AJUSTED_TABLE`)
   - **Flatten Current COB:** `fact.COBID = adjust.COBID AND adjust.COBID <> adjust.SOURCE_COBID` (scale factor = `-1`)
4. Apply `DENSE_RANK()` to keep only the latest adjustment per surrogate key
5. Filter out zero-value adjustments (`metric_usd <> 0`)
6. Delete old adjustments from detail + summary tables
7. Insert from temp table → permanent table
8. **Rolling adjustment fix:** For cross-COB adjustments, update `TRADE_KEY`, `COMMON_INSTRUMENT_KEY`, and `COMMON_INSTRUMENT_FCD_KEY` using SCD2 effective date lookups
9. Rebuild summary table (`GROUP BY ALL` with `SUM`)
10. Update status → `'Processed'`

### Special Filters (Dynamic Dimension Lookups)

The procedure adds `EXISTS` sub-queries for dimension filtering:

| Filter | Dimension Table | Match Columns |
|---|---|---|
| `DEPARTMENT_CODE` | `DIMENSION.BOOK` | `book_key` + `department_code` |
| `STRATEGY`, `TRADE_TYPOLOGY` | `DIMENSION.TRADE` | `trade_key` + `strategy` + `trade_typology` |

> These are NULL-safe: `OR adjust.column IS NULL` means "no filter on this field."

### Surrogate Key Generation

When `FACT_TABLE_PK` contains multiple columns (`;`-separated), the procedure builds:
```sql
md5(coalesce(cast(COL1 as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || ...)
```
Used to deduplicate via `DENSE_RANK()` partition.

### Key Differences vs. Legacy `FACT.PROCESS_ADJUSTMENTS` (JavaScript)

| Aspect | Legacy (JS) | New (Python) |
|---|---|---|
| Language | JavaScript | Python 3.11 + Snowpark |
| Execution | `EXECUTE AS OWNER` | `EXECUTE AS CALLER` |
| Config | Hard-coded table mapping | Config table `ADJUSTMENTS_SETTINGS` |
| Parameters | `(process_type, debug_flag)` | `(process_type, adjustment_action, cobid)` |
| Action routing | Procedure determines action from data | Caller specifies `adjustment_action` |
| Loops | Per-adjustment loop with per-row transactions | Single batch operation per action type |
| Dynamic SQL | 29 bind params, builds WHERE clause per field | Simpler join-based approach with EXISTS sub-queries |
| Rolling fix | Not present (separate step?) | Built-in: updates TRADE_KEY, INSTRUMENT_KEY for cross-COB |
| Concurrency | Row-lock check (`Running` status) | No explicit concurrency guard |
| PowerBI | Calls `UPDATE_POWERBI_FOR_ADJUSTMENTS` | Not included (handled separately) |
| Error handling | Per-adjustment rollback | Try/catch around entire batch |

### Source Code Reference

`context/codes/scaling_adjustment/procedure/adjustment.process_adjustment.sql`

---

## RUN_STATUS State Machine

```
                          ┌──────────────────────┐
  File lands  ──────────► │  Pending             │
                          └──────────┬───────────┘
                                     │  (proc starts)
                          ┌──────────▼───────────┐
                          │  Running              │
                          └──────────┬───────────┘
                         ┌───────────┼───────────┐
                         ▼                       ▼
                  ┌─────────┐              ┌─────────┐
                  │Processed│              │  Error  │
                  └─────────┘              └─────────┘

  Sign-off check ──► Rejected - SignedOff (not processed further)
```

> Unlike VaR_Upload, there is no `Deleted` status — soft-deleted adjustments are handled
> by the `IS_DELETED` flag check within the procedure.

---

## PowerBI Update: `FACT.UPDATE_POWERBI_FOR_ADJUSTMENTS`

Called once after all adjustments are processed. Updates three metadata tables:

| Table | Action |
|---|---|
| `METADATA.POWERBI_PUBLISH_INFO` | MERGE — upserts `last_updated_adj` per COBID + data_group. Special case: `sensitivity` data group also writes a `sensitivity_detail` row. |
| `METADATA.POWERBI_PUBLISH_DETAIL` | INSERT — links run log IDs to PowerBI objects |
| `METADATA.POWERBI_ACTION` | INSERT — queues PowerBI refresh actions (deduped via `VW_POWERBI_ACTION_INSERT_SOURCE`) |

---

## Sign-Off Logic

Two sign-off tables are used depending on process type:

| Process Type | Table | Match Columns | 10-Day Bypass? |
|---|---|---|---|
| VaR (default) | `BATCH.PUBLISH_VAR_SIGNOFF_STATUS` | `COBID + ENTITY_CODE` | ✅ Yes |
| Sensitivity, FRTB | `BATCH.PUBLISH_SIGNOFF_STATUS_EXCEPTION` | `COBID + ENTITY_CODE + PROCESS_TYPE` (SubType='NonCVA') | ❌ No |

---

## Comparison with Other Adjustment Pipelines

| Aspect | Scaling_Adjustment | VaR_Upload | Global_Adj |
|---|---|---|---|
| **Objects** | 31+ | 17+ | 4 |
| **Tasks** | 4 (1 + 3 parallel) | 2 (chained) | 1 |
| **Views** | 1 (complex CDC + sign-off) | 2 (CDC + sign-off + UNPIVOT) | 1 (simple CDC) |
| **Stored procedures** | 4 (PROCESS_ADJUSTMENTS + PROCESS_ADJUSTMENT_STEP + UPDATE_POWERBI + **PROCESS_ADJUSTMENT** [Python]) | 1 (LOAD_VAR_ADJUSTMENT_UPLOAD) | None |
| **Fact table sources** | 3 (VAR, ES, STRESS) | 0 (no source read) | 0 |
| **Fact table targets** | 6 (3 detail + 3 summary) | 2 (detail + summary) | 0 |
| **Generic/reusable** | ✅ Yes (parameterised by process type) | ❌ No (VaR-specific) | ❌ No |
| **Dynamic SQL** | ✅ Yes (builds WHERE clause from adjustment record) | ❌ No (static SQL) | ❌ No |
| **Transactions** | ✅ Per-adjustment with rollback | ❌ No (all-or-nothing) | ❌ No |
| **Concurrency guard** | ✅ Yes (row-lock check) | ❌ No | ❌ No |
| **Match key** | 2 columns (`GLOBAL_REFERENCE + COBID`) | 10 columns with `equal_null()` | 1 column (`ADJUSTMENT_ID`) |
| **Handles deletes?** | ✅ Via IS_DELETED flag in MERGE | ✅ Via soft-delete in MERGE | ❌ No |
| **Row volume** | 1000+ | 700+ (×21 after unpivot) | <5 |

---

## Complexity & Pain Points (for redesign)

1. **Most complex pipeline** — 31+ objects, 4 tasks, 3 procedures, dynamic SQL generation
2. **JavaScript stored procedures** — `PROCESS_ADJUSTMENTS` (410 lines) and `PROCESS_ADJUSTMENT_STEP` (341 lines) are both JavaScript, difficult to test and debug
3. **Dynamic SQL in PROCESS_ADJUSTMENT_STEP** — builds INSERT/DELETE statements at runtime based on which filter fields exist in the JSON; extremely flexible but opaque
4. **29 bind parameters** — the step procedure supports up to 29 bind variables, many of which are nullable; hard to reason about which are active
5. **Three separate action paths** (FLATTEN / SCALE CURRENT / SCALE SOURCE) with subtly different scale factor manipulation — `SCALE_FACTOR - 1` for same-COB scaling, full factor for cross-COB
6. **Full entity roll vs. filtered** — window restriction logic (`INCLUDE_ALL_WINDOWS`) is embedded deep in the procedure, not visible from outside
7. **EXCEPT against live DIMENSION.ADJUSTMENT** in the view — the change detection compares stream rows against the current state of the dimension table, which means the view's output depends on timing
8. **Three parallel tasks after publish** — VaR, Stress, and FRTB all trigger from the same stream but each runs independently; if one fails, the others still proceed
9. **FRTB/Sensitivity uses new Python procedure** — Task 2c now calls `ADJUSTMENT.PROCESS_ADJUSTMENT` (Python/Snowpark) while Tasks 2a/2b still use the legacy JavaScript `FACT.PROCESS_ADJUSTMENTS`; two different code paths for the same logical operation
10. **PowerBI coupling** — `UPDATE_POWERBI_FOR_ADJUSTMENTS` + `VW_POWERBI_ACTION_INSERT_SOURCE` (200+ line view with 4 anti-join conditions) are tightly coupled to the adjustment procedure
11. **Shared DIMENSION.ADJUSTMENT table** with VaR_Upload — changes in one pipeline can affect the other's processing
12. **Stress Upload side-effect** — when `P_PROCESS_TYPE = 'STRESS'`, the procedure first calls `FACT.LOAD_STRESS_ADJUSTMENT_UPLOAD()` before processing scaling adjustments
13. **No ES task** — the procedure supports ES (`FACT.ES_MEASURES`) but there is no task configured to call `PROCESS_ADJUSTMENTS('ES')` — dead code or triggered elsewhere

---

## Source Code Reference

All DDL is stored in `context/codes/scaling_adjustment/`:

| File | Object |
|---|---|
| `task/STAGING.PUBLISH_SCALING_ADJUSTMENT_TASK.sql` | Task 1 — MERGE into `DIMENSION.ADJUSTMENT` |
| `task/STAGING.LOAD_VAR_SCALING_ADJUSTMENT_TASK.sql` | Task 2a — calls `PROCESS_ADJUSTMENTS('VAR')` |
| `task/STAGING.LOAD_STRESS_SCALING_ADJUSTMENT_TASK.sql` | Task 2b — calls `PROCESS_ADJUSTMENTS('STRESS')` |
| `task/STAGING.LOAD_FRTB_SENSITIVITY_SCALING_ADJUSTMENT_TASK.sql` | Task 2c — dummy FRTB/Sensitivity log |
| `view/STAING.IDM_SCALING_ADJUSTMENT_STREAM_DATA.sql` | View — CDC, change detect, IS_10DAY, sign-off |
| `procedure/FACT.PROCESS_ADJUSTMENTS.sql` | Main orchestrator — loops adjustments, determines action |
| `procedure/FACT.PROCESS_ADJUSTMENT_STEP.sql` | Dynamic SQL engine — builds and executes INSERT/DELETE |
| `procedure/adjustment.process_adjustment.sql` | **New** Python/Snowpark procedure — config-driven, handles Direct + Scale |
| `procedure/FACT.UPDATE_POWERBI_FOR_ADJUSTMENTS.sql` | PowerBI metadata update |
| `procedure/METADATA.VW_POWERBI_ACTION_INSERT_SOURCE.sql` | PowerBI action deduplication view |

---

## Next Steps / To Do

- [ ] Document the SSRS → Python export process that generates the source file
- [ ] Document `DIMENSION.ADJUSTMENT` shared table DDL (used by both VaR_Upload and Scaling)
- [ ] Document `BATCH.GET_COLUMNS` procedure (used by PROCESS_ADJUSTMENT_STEP)
- [x] ~~Clarify where FRTB/Sensitivity adjustments are actually processed~~ → Task 2c calls `ADJUSTMENT.PROCESS_ADJUSTMENT`
- [ ] Document `ADJUSTMENT.ADJUSTMENTS_SETTINGS` seed table contents (all process type configurations)
- [ ] Plan migration of Tasks 2a/2b from legacy JS procedures to new Python procedure
- [ ] Clarify if/where ES adjustments are triggered (no task exists for ES)
- [ ] Document `FACT.LOAD_STRESS_ADJUSTMENT_UPLOAD` procedure (called as side-effect)
- [ ] Map which filter fields are actually used in practice vs. theoretical support for 29 binds
- [ ] Identify simplification opportunities for the redesign
- [ ] Define the target-state architecture
