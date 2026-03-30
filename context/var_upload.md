# VaR_Upload — Adjustment Context

> **Source:** Extracted from [adjustment_metadata.md](adjustment_metadata.md) + source code in `codes/VaR_Upload/`
>
> **Last updated:** 2026-03-28
>
> **Goal:** This is the first adjustment type to be redesigned — make it simpler,
> easy for everyone in the team to understand, and quick to process.

---

## Overview

| Property | Value |
|---|---|
| **Description** | Users upload the adjustment file direct to the folder |
| **File Name** | `VaR_Upload_\|:YYYYMMDD:\|_*.csv` |
| **Path** | `\\mfil\proddfs\fsroot\sfs-prod\raptor\extracts\general\VaRUpload` |
| **Table Destination** | `STAGING.IDM_VAR_UPLOAD` |
| **Rows Per File** | Can be more than 700 |

---

## File Layout (columns in order)

```
COBId, EntityCode, SourceSystemCode, BookCode, CurrencyCode, ScenarioDate, TradeCode,
AllVaR, AllVaRSkew, BasisVaR, BondAssetSpreadVaR, CrossEffects, EquityPriceVaR,
EquityVegaVaR, FXRateVaR, FXVolatilityVaR, IRCapVolVaR, IRCapVolVaRSkew, IRSkewVolVaR,
IRSwaptionVolVaR, IRSwaptionVolVaRSkew, InflationRateCurveVaR, InflationVolVaR,
InterestRateCurveVaR, InterestRateVegaVaR, MTGSprdVaR, OASVaR, ParCreditSpreadVaR,
Category, Detail
```

> The file has **21 VaR measure columns** (AllVaR → ParCreditSpreadVaR) that are
> wide-format. The pipeline **UNPIVOTs** them into individual rows downstream.

---

## End-to-End Pipeline

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│  1. CSV file lands on network share → ingested into STAGING.IDM_VAR_UPLOAD      │
└──────────────────┬───────────────────────────────────────────────────────────────┘
                   │
                   ▼  (CDC stream)
┌──────────────────────────────────────────────────────────────────────────────────┐
│  2. STAGING.IDM_VAR_UPLOAD_STREAM                                               │
│     Captures INSERT / DELETE / UPDATE actions via METADATA$ACTION               │
└──────────────────┬───────────────────────────────────────────────────────────────┘
                   │
                   ▼  (view)
┌──────────────────────────────────────────────────────────────────────────────────┐
│  3. STAGING.IDM_VAR_UPLOAD_STREAM_DATA                                          │
│     • Classifies CDC rows as I (insert), U (update), D (delete)                 │
│     • UNPIVOTs 21 VaR columns → rows (VAR_SUB_COMPONENT_NAME, VALUE)            │
│     • Joins DIMENSION.VAR_SUB_COMPONENT to resolve component IDs                │
│     • Generates GLOBAL_REFERENCE = SHA1(COBID || ENTITY_CODE || FILENAME)       │
│     • Handles TENDAY (10-day VaR) naming conventions                            │
└──────────────────┬───────────────────────────────────────────────────────────────┘
                   │
                   ▼  (view)
┌──────────────────────────────────────────────────────────────────────────────────┐
│  4. STAGING.IDM_VAR_UPLOAD_STREAM_DATA_MERGE                                    │
│     • Joins BATCH.PUBLISH_VAR_SIGNOFF_STATUS to determine RUN_STATUS            │
│       - Not signed off or TENDAY → 'Pending'                                    │
│       - Signed off → 'Rejected - SignedOff'                                     │
│     • Adds DELETE rows for existing records in ADJUSTMENT.IDM_VAR_UPLOAD         │
│       that are no longer in the stream (same file re-upload scenario)            │
└──────────────────┬───────────────────────────────────────────────────────────────┘
                   │
                   ▼  (Task 1)
┌──────────────────────────────────────────────────────────────────────────────────┐
│  5. PUBLISH_VAR_UPLOAD_TASK                                                     │
│     MERGE INTO ADJUSTMENT.IDM_VAR_UPLOAD (unpivoted row-level data)             │
└──────────────────┬───────────────────────────────────────────────────────────────┘
                   │
                   ▼  (Task 2 — AFTER Task 1)
┌──────────────────────────────────────────────────────────────────────────────────┐
│  6. LOAD_VAR_ADJUSTMENT_UPLOAD_TASK                                             │
│     CALL FACT.LOAD_VAR_ADJUSTMENT_UPLOAD('0')                                   │
│     • Aggregates rows → MERGE INTO DIMENSION.ADJUSTMENT                          │
│     • Resolves dimension keys (Trade, Book, Instrument)                          │
│     • Writes to FACT.VAR_MEASURES_ADJUSTMENT + _SUMMARY                          │
│     • Updates RUN_STATUS, run logging, PowerBI metadata                          │
└──────────────────────────────────────────────────────────────────────────────────┘
```

---

## All Tables & Objects Involved

| Object | Type | Role |
|---|---|---|
| `STAGING.IDM_VAR_UPLOAD` | Table | Landing table — raw CSV rows |
| `STAGING.IDM_VAR_UPLOAD_STREAM` | Stream | CDC capture on landing table |
| `STAGING.IDM_VAR_UPLOAD_STREAM_DATA` | View | Classifies CDC, UNPIVOTs 21 VaR columns, resolves component IDs |
| `STAGING.IDM_VAR_UPLOAD_STREAM_DATA_MERGE` | View | Adds RUN_STATUS via sign-off check; adds DELETE rows for re-uploads |
| `ADJUSTMENT.IDM_VAR_UPLOAD` | Table | Intermediate — unpivoted row-level adjustment data |
| `DIMENSION.ADJUSTMENT` | Table | Aggregated adjustment dimension (shared with Scaling_Adjustment) |
| `DIMENSION.VAR_SUB_COMPONENT` | Lookup | Maps VaR component names → IDs (incl. 10-day variants) |
| `DIMENSION.TRADE` | Lookup | Resolves trade keys; falls back to `BOOK_CODE/ADJUSTMENT` |
| `DIMENSION.BOOK` | Lookup | Resolves book keys; falls back to `DEPARTMENT_CODE/ADJUSTMENT` |
| `DIMENSION.COMMON_INSTRUMENT` | Lookup | Resolves instrument keys from trade |
| `FACT.VAR_MEASURES_ADJUSTMENT` | Fact table | Final destination — detail-level VaR adjustment values |
| `FACT.VAR_MEASURES_ADJUSTMENT_SUMMARY` | Fact table | Final destination — summarised VaR adjustment values |
| `BATCH.PUBLISH_VAR_SIGNOFF_STATUS` | Lookup | Determines if entity/COB has been signed off |
| `BATCH.RUN_LOG` | Table | Processing run log |
| `BATCH.SEQ_RUN_LOG` | Sequence | Generates run log IDs |
| `METADATA.POWERBI_PUBLISH_INFO` | Table | PowerBI refresh tracking |
| `METADATA.POWERBI_PUBLISH_DETAIL` | Table | PowerBI refresh detail |
| `METADATA.POWERBI_ACTION` | Table | PowerBI action queue |

---

## Key Transformations

### 1. UNPIVOT (in view `IDM_VAR_UPLOAD_STREAM_DATA`)

The 21 wide-format VaR columns are unpivoted into individual rows:

```sql
UNPIVOT(
    ADJUSTMENT_VALUE_IN_USD FOR VAR_SUB_COMPONENT_NAME
        IN (ALL_VAR, ALL_VAR_SKEW, BASIS_VAR, BOND_ASSET_SPREAD_VAR,
            CROSS_EFFECTS, EQUITY_PRICE_VAR, EQUITY_VEGA_VAR, FX_RATE_VAR,
            FX_VOLATILITY_VAR, INFLATION_RATE_CURVE_VAR, INFLATION_VOL_VAR,
            INTEREST_RATE_CURVE_VAR, INTEREST_RATE_VEGA_VAR, IR_CAP_VOL_VAR,
            IR_CAP_VOL_VAR_SKEW, IR_SKEW_VOL_VAR, IR_SWAPTION_VOL_VAR,
            IR_SWAPTION_VOL_VAR_SKEW, MTG_SPRD_VAR, OAS_VAR, PAR_CREDIT_SPREAD_VAR))
```

This means **one CSV row with 700 rows → ~14,700 unpivoted rows** (700 × 21 components).

### 2. TENDAY Component Name Resolution

When `TENDAY = 'Y'`, component names get a `(10 Day)` suffix, with special cases:

| Column Name | 10-Day Name |
|---|---|
| `INFLATION_RATE_CURVE_VAR` | `Inflation VaR (10 Day)` |
| `EQUITY_PRICE_VAR` | `Equity Price (10 Day)` |
| `PAR_CREDIT_SPREAD_VAR` | `Par Credit Spread VAR (10 Day)` |
| All others | `<NAME WITH SPACES> (10 Day)` |

### 3. GLOBAL_REFERENCE Generation

```sql
UPPER(SHA1(COBID || ENTITY_CODE || RAVEN_FILENAME))
```

This is the **primary link** between `ADJUSTMENT.IDM_VAR_UPLOAD` and `DIMENSION.ADJUSTMENT`.

### 4. REASON Construction (in procedure)

```sql
CONCAT(CATEGORY, '|', DETAIL) AS REASON
```

The match key for the `DIMENSION.ADJUSTMENT` merge is `GLOBAL_REFERENCE + REASON`.

### 5. MSSQL_ADJUSTMENT_ID Extraction (in procedure)

```sql
MAX(SPLIT_PART(SPLIT_PART(FILE_NAME, '_', -1), '.', 1)) AS MSSQL_ADJUSTMENT_ID
```

Extracts the numeric ID from the filename suffix (e.g. `VaR_Upload_20260328_42.csv` → `42`).

### 6. Aggregation for DIMENSION.ADJUSTMENT (in procedure)

Rows in `ADJUSTMENT.IDM_VAR_UPLOAD` are **grouped/aggregated** before merging into `DIMENSION.ADJUSTMENT`:

```sql
SELECT COBID, ENTITY_CODE, PROCESS_TYPE, ADJUSTMENT_TYPE,
       SUM(ADJUSTMENT_VALUE_IN_USD) ADJUSTMENT_VALUE_IN_USD,
       USERNAME, RUN_STATUS, FILE_NAME, GLOBAL_REFERENCE,
       CONCAT(CATEGORY, '|', DETAIL) AS REASON,
       COUNT(*) - SUM(IFF(IS_DELETED = TRUE, 1, 0)) AS COUNT_ACTIVE,
       MAX(SPLIT_PART(SPLIT_PART(FILE_NAME, '_', -1), '.', 1)) AS MSSQL_ADJUSTMENT_ID,
       BOOK_CODE, DEPARTMENT_CODE
FROM ADJUSTMENT.IDM_VAR_UPLOAD
WHERE RUN_STATUS = 'Pending'
GROUP BY ...
```

### 7. Dimension Key Resolution (in procedure)

The procedure joins to resolve fact-table foreign keys:

- **Trade** → `DIMENSION.TRADE` on `TRADE_CODE + BOOK_CODE + ENTITY_CODE` (fallback: `BOOK_CODE/ADJUSTMENT`)
- **Book** → `DIMENSION.BOOK` on `BOOK_CODE` (fallback: `DEPARTMENT_CODE/ADJUSTMENT`)
- **Instrument** → `DIMENSION.COMMON_INSTRUMENT` via `TRADE.INSTRUMENT_KEY`

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
                         ▼           ▼           ▼
                  ┌─────────┐ ┌──────────┐ ┌─────────┐
                  │Processed│ │ Deleted  │ │  Error  │
                  └─────────┘ └──────────┘ └─────────┘

  Sign-off check ──► Rejected - SignedOff (inserted separately, not processed further)
```

- **Pending** → file arrived, not yet signed off
- **Running** → procedure is actively processing
- **Processed** → successfully written to fact tables (`RECORD_COUNT > 0`)
- **Deleted** → all active rows were soft-deleted (`RECORD_COUNT = 0`)
- **Error** → procedure failed (error message stored in `ERRORMESSAGE`)
- **Rejected - SignedOff** → entity/COB was already signed off; inserted into `DIMENSION.ADJUSTMENT` but **not processed into fact tables**

---

## Sign-Off Logic

The view `IDM_VAR_UPLOAD_STREAM_DATA_MERGE` checks `BATCH.PUBLISH_VAR_SIGNOFF_STATUS`:

```sql
CASE
    WHEN PVSO.PUBLISH_STATUS IS NULL THEN 'Pending'     -- not signed off
    WHEN SD.TENDAY = 'Y'             THEN 'Pending'     -- 10-day always allowed
    ELSE 'Rejected - SignedOff'                          -- blocked
END AS RUN_STATUS
```

This means **10-day VaR adjustments bypass the sign-off gate**.

---

## Task 1 — MERGE into ADJUSTMENT.IDM_VAR_UPLOAD

### `DVLP_RAPTOR_NEWADJ.STAGING.PUBLISH_VAR_UPLOAD_TASK`

| Property | Value |
|---|---|
| Warehouse | `PROD_RAPTOR_WH_XS` |
| Schedule | 1 minute |
| Guard | `SYSTEM$STREAM_HAS_DATA('STAGING.IDM_VAR_UPLOAD_STREAM')` |
| Source | `STAGING.IDM_VAR_UPLOAD_STREAM_DATA_MERGE` |
| Target | `ADJUSTMENT.IDM_VAR_UPLOAD` |

**Match key (10 columns + IS_DELETED guard):**

```sql
equal_null(src.COBID, tgt.COBID)
AND equal_null(src.ENTITY_CODE, tgt.ENTITY_CODE)
AND equal_null(src.SOURCE_SYSTEM_CODE, tgt.SOURCE_SYSTEM_CODE)
AND equal_null(src.BOOK_CODE, tgt.BOOK_CODE)
AND equal_null(src.CURRENCY_CODE, tgt.CURRENCY_CODE)
AND equal_null(src.SCENARIO_DATE_ID, tgt.SCENARIO_DATE_ID)
AND equal_null(src.TRADE_CODE, tgt.TRADE_CODE)
AND equal_null(src.VAR_SUB_COMPONENT_ID, tgt.VAR_SUB_COMPONENT_ID)
AND equal_null(src.FILE_NAME, tgt.FILE_NAME)
AND IFNULL(tgt.IS_DELETED, FALSE) = FALSE
AND equal_null(src.DEPARTMENT_CODE, tgt.DEPARTMENT_CODE)
```

**MERGE behaviour:**

| Condition | Action |
|---|---|
| Matched + `DML_TYPE = 'I'` or `'U'` | UPDATE `ADJUSTMENT_VALUE_IN_USD`, `RUN_STATUS`, `CATEGORY`, `DETAIL`; reset `IS_DELETED = FALSE` |
| Matched + `DML_TYPE = 'D'` | Soft-delete: `IS_DELETED = TRUE`, `DELETED_BY = 'SYSTEM'`, `DELETED_DATE = current_timestamp()` |
| Not matched + `DML_TYPE = 'I'` or `'U'` | INSERT new row with `IS_DELETED = FALSE` |

---

## Task 2 — Stored Procedure: FACT.LOAD_VAR_ADJUSTMENT_UPLOAD

### `DVLP_RAPTOR_NEWADJ.STAGING.LOAD_VAR_ADJUSTMENT_UPLOAD_TASK`

| Property | Value |
|---|---|
| Warehouse | `PROD_RAPTOR_WH_XS` |
| Trigger | `AFTER PUBLISH_VAR_UPLOAD_TASK` |
| Action | `CALL FACT.LOAD_VAR_ADJUSTMENT_UPLOAD('0')` |
| Language | JavaScript |

### Procedure Steps (in execution order)

| Step | Action | Detail |
|---|---|---|
| 1 | **MERGE → DIMENSION.ADJUSTMENT** (Pending) | Aggregates `ADJUSTMENT.IDM_VAR_UPLOAD` rows where `RUN_STATUS = 'Pending'`, grouped by `GLOBAL_REFERENCE + REASON`. Upserts into `DIMENSION.ADJUSTMENT`. |
| 2 | **MERGE → DIMENSION.ADJUSTMENT** (SignedOff) | Same aggregation but for `RUN_STATUS = 'Rejected - SignedOff'`. **INSERT only** (no update on match). |
| 3 | **Set Running** on `DIMENSION.ADJUSTMENT` | `UPDATE SET RUN_STATUS = 'Running' WHERE RUN_STATUS = 'Pending' AND ADJUSTMENT_TYPE = 'Upload' AND PROCESS_TYPE = 'VaR'` |
| 4 | **Create RUN_LOG entries** | Temp table `FACT.TEMP_UPLOAD_RUNLOG` with sequence IDs, insert into `BATCH.RUN_LOG` |
| 5 | **Set Running** on `ADJUSTMENT.IDM_VAR_UPLOAD` | Updates all rows sharing `GLOBAL_REFERENCE` with any `Pending` row |
| 6 | **Build temp fact table** | `FACT.TEMP_ADJUSTMENT_UPLOAD` — joins `DIMENSION.ADJUSTMENT` + `ADJUSTMENT.IDM_VAR_UPLOAD` + `DIMENSION.TRADE` + `DIMENSION.BOOK` + `DIMENSION.COMMON_INSTRUMENT` |
| 7 | **DELETE existing adjustments** | Removes from `FACT.VAR_MEASURES_ADJUSTMENT` and `_SUMMARY` where adjustment is `Running` (allows re-processing) |
| 8 | **INSERT → FACT.VAR_MEASURES_ADJUSTMENT** | Detail-level rows where `IS_DELETED = FALSE AND PNL_VECTOR_VALUE_IN_USD <> 0` |
| 9 | **INSERT → FACT.VAR_MEASURES_ADJUSTMENT_SUMMARY** | Grouped/summarised rows (`SUM(PNL_VECTOR_VALUE_IN_USD)`) with `HAVING SUM <> 0` |
| 10 | **Set Processed/Deleted** | `DIMENSION.ADJUSTMENT`: `RUN_STATUS = IFF(RECORD_COUNT = 0, 'Deleted', 'Processed')`; `ADJUSTMENT.IDM_VAR_UPLOAD`: `RUN_STATUS = 'Processed'` |
| 11 | **Update PowerBI metadata** | Writes to `METADATA.POWERBI_PUBLISH_INFO`, `_DETAIL`, and `POWERBI_ACTION` to trigger downstream refresh |
| 12 | **Error handling** | On failure: sets `RUN_STATUS = 'Error'` + stores error message; updates `BATCH.RUN_LOG` with error |

---

## Complexity & Pain Points (for redesign)

Based on code analysis, the current process has several areas of complexity:

1. **Two-layer view chain before the merge task even runs** — `STREAM_DATA` → `STREAM_DATA_MERGE`, each with complex CDC logic
2. **UNPIVOT of 21 columns** amplifies row count ~21× — a 700-row file becomes ~14,700 rows
3. **10-column match key** in Task 1 with `equal_null()` on every column — fragile and hard to debug
4. **Two separate MERGEs** in the procedure (Pending vs. SignedOff) with near-identical logic
5. **DELETE + re-INSERT pattern** for fact tables (not idempotent-friendly)
6. **JavaScript stored procedure** — harder to test, debug, and maintain than SQL
7. **Multiple dimension lookups** with fallback logic (Trade → `BOOK_CODE/ADJUSTMENT`, Book → `DEPARTMENT_CODE/ADJUSTMENT`)
8. **PowerBI metadata updates** tightly coupled into the adjustment procedure
9. **Sign-off bypass** for 10-day VaR is an exception embedded in view logic, not configuration
10. **Shared `DIMENSION.ADJUSTMENT`** table with Scaling_Adjustment — changes here can affect the other pipeline

---

## Source Code Reference

All DDL is stored in `context/codes/VaR_Upload/`:

| File | Object |
|---|---|
| `task/STAGING.PUBLISH_VAR_UPLOAD_TASK.SQL` | Task 1 — stream-triggered MERGE into `ADJUSTMENT.IDM_VAR_UPLOAD` |
| `task/STAGING.LOAD_VAR_ADJUSTMENT_UPLOAD_TASK.sql` | Task 2 — calls stored procedure |
| `view/STAGING.IDM_VAR_UPLOAD_STREAM_DATA.sql` | View — CDC classification, UNPIVOT, component ID resolution |
| `view/STAGING.IDM_VAR_UPLOAD_STREAM_DATA_MERGE.sql` | View — sign-off status check, re-upload DELETE logic |
| `procedure/FACT.LOAD_VAR_ADJUSTMENT_UPLOAD.sql` | Stored procedure — aggregation, dimension resolution, fact load |

---

## Next Steps / To Do

- [ ] Document target table DDL for `ADJUSTMENT.IDM_VAR_UPLOAD`
- [ ] Document `DIMENSION.ADJUSTMENT` shared table DDL
- [ ] Document `FACT.VAR_MEASURES_ADJUSTMENT` and `_SUMMARY` DDL
- [ ] Map out which columns are actually used downstream (many may be unused)
- [ ] Identify simplification opportunities for the redesign
- [ ] Define the target-state architecture
