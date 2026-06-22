# Global_Adj — Adjustment Context

> **Source:** Extracted from [adjustment_metadata.md](adjustment_metadata.md) + source code in `codes/global_adj/`
>
> **Last updated:** 2026-03-28
>
> **Goal:** Part of the redesign project — make the adjustment process simpler,
> easy for everyone in the team to understand, and quick to process.

---

## Overview

| Property | Value |
|---|---|
| **Description** | Not sure how it is created |
| **File Name** | `Global_Adj_SF_\|:YYYYMMDD:\|_*.csv` |
| **Path** | `\\mfil\proddfs\fsroot\sfs-prod\raptor\extracts\general\GlobalAdjustmentSF` |
| **Table Destination** | `STAGING.IDM_GLOBAL_ADJUSTMENT_SF` |
| **Rows Per File** | Less than 5 usually |

---

## File Layout (columns in order)

```
COBId, AdjustmentId, SourceCOBId, ApprovalId, AdjustmentType, Reason, FormData,
AdjustedBy, AdjustmentCreated, AdjustmentCompleted, Status, EntityCode, EntityKey,
DepartmentCode, IsDeleted, DeletedBy, AdjustmentDeleted, TempId
```

---

## End-to-End Pipeline

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│  1. CSV file lands on network share → ingested into STAGING.IDM_GLOBAL_ADJ_SF   │
└──────────────────┬───────────────────────────────────────────────────────────────┘
                   │
                   ▼  (CDC stream)
┌──────────────────────────────────────────────────────────────────────────────────┐
│  2. STAGING.IDM_GLOBAL_ADJUSTMENT_SF_STREAM                                     │
│     Captures INSERT / DELETE actions via METADATA$ACTION                         │
└──────────────────┬───────────────────────────────────────────────────────────────┘
                   │
                   ▼  (view)
┌──────────────────────────────────────────────────────────────────────────────────┐
│  3. STAGING.IDM_GLOBAL_ADJUSTMENT_SF_STREAM_DATA                                │
│     • Classifies CDC rows as I (insert), U (update), D (delete)                 │
│     • Matches on ADJUSTMENT_ID to pair DELETE + INSERT = UPDATE                 │
│     • No transforms — columns pass through as-is                                │
└──────────────────┬───────────────────────────────────────────────────────────────┘
                   │
                   ▼  (Task — single step, no further tasks)
┌──────────────────────────────────────────────────────────────────────────────────┐
│  4. GLOBAL_ADJUSTMENT_SF_TASK                                                   │
│     MERGE INTO ADJUSTMENT.GLOBAL_ADJUSTMENT_SF on ADJUSTMENT_ID                 │
│     Adds ADJUSTMENT_UPDATED timestamp (Europe/London TZ)                        │
└──────────────────────────────────────────────────────────────────────────────────┘
                   │
                   ✕  (pipeline ends — no downstream tasks)
```

> **Key difference vs. VaR_Upload & Scaling_Adjustment:** This pipeline is **one task only**.
> There is no stored procedure, no dimension/fact table writes, and no further processing.
> The data lands in `ADJUSTMENT.GLOBAL_ADJUSTMENT_SF` and stops.

---

## All Tables & Objects Involved

| Object | Type | Role |
|---|---|---|
| `STAGING.IDM_GLOBAL_ADJUSTMENT_SF` | Table | Landing table — raw CSV rows |
| `STAGING.IDM_GLOBAL_ADJUSTMENT_SF_STREAM` | Stream | CDC capture on landing table |
| `STAGING.IDM_GLOBAL_ADJUSTMENT_SF_STREAM_DATA` | View | Classifies CDC rows as I / U / D |
| `ADJUSTMENT.GLOBAL_ADJUSTMENT_SF` | Table | Final destination — one row per adjustment |

> Compared to VaR_Upload (17 objects), this pipeline only touches **4 objects**.

---

## CDC View Logic — `IDM_GLOBAL_ADJUSTMENT_SF_STREAM_DATA`

The view reads from `STAGING.IDM_GLOBAL_ADJUSTMENT_SF_STREAM` and classifies each CDC
record into one of three DML types by inspecting `METADATA$ACTION` and `METADATA$ISUPDATE`:

### Classification Rules

```
┌─────────────────────────────────────────────────────────────────────────┐
│  Stream row: METADATA$ACTION = 'DELETE', METADATA$ISUPDATE = 'FALSE'  │
│                                                                         │
│  Is there a matching INSERT for the same ADJUSTMENT_ID?                 │
│     NO  → DML_TYPE = 'D'  (true delete from source)                    │
│     YES → skip (handled by UPDATE logic below)                          │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│  Stream row: METADATA$ACTION = 'INSERT', METADATA$ISUPDATE = 'FALSE'  │
│                                                                         │
│  Is there a matching DELETE for the same ADJUSTMENT_ID?                 │
│     NO  → DML_TYPE = 'I'  (true insert into source)                    │
│     YES → DML_TYPE = 'U'  (update — new values via EXCEPT)             │
└─────────────────────────────────────────────────────────────────────────┘
```

### How Updates Are Detected

Snowflake streams represent an UPDATE as a paired DELETE + INSERT. The view detects this
by checking if both actions exist for the same `ADJUSTMENT_ID`:

```sql
-- UPDATE = INSERT rows that have a matching DELETE, EXCEPT the DELETE rows themselves
-- (EXCEPT filters out rows where all column values are identical — i.e. no actual change)
SELECT ... FROM STREAM WHERE ACTION = 'INSERT'
  AND EXISTS (SELECT 1 FROM STREAM WHERE ACTION = 'DELETE' AND same ADJUSTMENT_ID)
EXCEPT
SELECT ... FROM STREAM WHERE ACTION = 'DELETE'
```

The `EXCEPT` ensures that only rows with **actually changed values** produce a `DML_TYPE = 'U'`.
If a DELETE+INSERT pair has identical data, the EXCEPT removes it (no-op).

> This is the same CDC pattern used across all three adjustment pipelines. The only
> difference is the match column: `ADJUSTMENT_ID` here vs. a composite key in VaR_Upload.

---

## Task — MERGE into ADJUSTMENT.GLOBAL_ADJUSTMENT_SF

### `DVLP_RAPTOR_NEWADJ.STAGING.GLOBAL_ADJUSTMENT_SF_TASK`

| Property | Value |
|---|---|
| Warehouse | `PROD_RAPTOR_WH` |
| Schedule | 1 minute |
| Guard | `SYSTEM$STREAM_HAS_DATA('STAGING.IDM_GLOBAL_ADJUSTMENT_SF_STREAM')` |
| Source | `STAGING.IDM_GLOBAL_ADJUSTMENT_SF_STREAM_DATA` |
| Target | `ADJUSTMENT.GLOBAL_ADJUSTMENT_SF` |

**Match key:**

```sql
tgt.ADJUSTMENT_ID = src.ADJUSTMENT_ID
```

> Single-column match — much simpler than VaR_Upload's 10-column key.

### MERGE Behaviour

| Condition | Action |
|---|---|
| Matched + `DML_TYPE = 'U'` or `'I'` | UPDATE all 16 fields + set `ADJUSTMENT_UPDATED = current_timestamp (Europe/London)` |
| Not matched + `DML_TYPE = 'I'` | INSERT full row including `ADJUSTMENT_UPDATED` |
| Matched + `DML_TYPE = 'D'` | **No action** — deletes from source are captured by the stream but not handled by the task |

> ⚠️ **Note:** The task does NOT handle `DML_TYPE = 'D'`. If a row is deleted from the source
> SQL Server table, it will appear in the stream view with `DML_TYPE = 'D'` but the MERGE
> statement has no `WHEN MATCHED AND DML_TYPE = 'D'` clause. The delete is effectively **ignored**.

### Columns Updated/Inserted

| # | Column | Source | Notes |
|---|---|---|---|
| 1 | `COBID` | `src.COBID` | INSERT only |
| 2 | `ADJUSTMENT_ID` | `src.ADJUSTMENT_ID` | Match key (INSERT only) |
| 3 | `SOURCE_COBID` | `src.SOURCE_COBID` | |
| 4 | `APPROVAL_ID` | `src.APPROVAL_ID` | |
| 5 | `ADJUSTMENT_TYPE` | `src.ADJUSTMENT_TYPE` | |
| 6 | `REASON` | `src.REASON` | |
| 7 | `FORM_DATA` | `src.FORM_DATA` | May contain structured/JSON data |
| 8 | `ADJUSTED_BY` | `src.ADJUSTED_BY` | |
| 9 | `ADJUSTMENT_CREATED` | `src.ADJUSTMENT_CREATED` | |
| 10 | `ADJUSTMENT_COMPLETED` | `src.ADJUSTMENT_COMPLETED` | |
| 11 | `STATUS` | `src.STATUS` | |
| 12 | `ENTITY_CODE` | `src.ENTITY_CODE` | |
| 13 | `ENTITY_KEY` | `src.ENTITY_KEY` | |
| 14 | `DEPARTMENT_CODE` | `src.DEPARTMENT_CODE` | |
| 15 | `IS_DELETED` | `src.IS_DELETED` | Soft-delete flag from source |
| 16 | `DELETED_BY` | `src.DELETED_BY` | |
| 17 | `ADJUSTMENT_DELETED` | `src.ADJUSTMENT_DELETED` | Timestamp of deletion |
| 18 | `TEMP_ID` | `src.TEMP_ID` | |
| 19 | `ADJUSTMENT_UPDATED` | _generated_ | `convert_timezone('Europe/London', current_timestamp)::TIMESTAMP_TZ` |

---

## Comparison with Other Adjustment Pipelines

| Aspect | Global_Adj | VaR_Upload | Scaling_Adjustment |
|---|---|---|---|
| **Objects** | 4 | 17+ | TBD |
| **Tasks** | 1 | 2 (chained) | 2 (chained) |
| **Views** | 1 (CDC classify) | 2 (CDC + sign-off + UNPIVOT) | 1 (CDC classify) |
| **Stored procedures** | None | 1 (JavaScript, 500+ lines) | TBD |
| **Transforms** | None — pass-through | UNPIVOT 21 cols, aggregation, dim resolution | TBD |
| **Match key** | 1 column (`ADJUSTMENT_ID`) | 10 columns with `equal_null()` | 2 columns |
| **Final destination** | `ADJUSTMENT.GLOBAL_ADJUSTMENT_SF` | `FACT.VAR_MEASURES_ADJUSTMENT` + `_SUMMARY` | `DIMENSION.ADJUSTMENT` |
| **Handles deletes?** | ❌ No (DML_TYPE='D' ignored) | ✅ Yes (soft-delete) | ✅ Yes (soft-delete) |
| **Row volume** | <5 per file | 700+ (×21 after unpivot) | 1000+ |

---

## Complexity & Pain Points (for redesign)

Based on code analysis, the current process has these characteristics:

1. **Simplest of the three pipelines** — only 4 objects, one task, no stored procedure
2. **Delete handling gap** — `DML_TYPE = 'D'` from the stream is never acted on; deletes from the source system are silently ignored
3. **Source origin is unclear** — documentation says "not sure how it is created"; the stream view comment references `SQL Server table: GLOBAL.StagingAdjustmentSnowflake` which suggests a SQL Server → Snowflake replication feed
4. **No downstream processing** — data sits in `ADJUSTMENT.GLOBAL_ADJUSTMENT_SF` with no further fact-table writes, unlike VaR_Upload which flows into fact tables and PowerBI
5. **IS_DELETED is a source-side field** — unlike VaR_Upload where `IS_DELETED` is managed by the task, here it comes from the source as-is
6. **FORM_DATA column** — likely contains structured/JSON data from a form UI; could be valuable for the redesign if it stores adjustment parameters
7. **Warehouse sizing** — uses `PROD_RAPTOR_WH` (larger) despite being the lowest-volume pipeline (<5 rows); could use `_XS`

---

## Source Code Reference

All DDL is stored in `context/codes/global_adj/`:

| File | Object |
|---|---|
| `task/STAGING.GLOBAL_ADJUSTMENT_SF_TASK.sql` | Task — stream-triggered MERGE into `ADJUSTMENT.GLOBAL_ADJUSTMENT_SF` |
| `view/STAGING.IDM_GLOBAL_ADJUSTMENT_SF_STREAM_DATA.sql` | View — CDC classification (I / U / D) |

---

## Next Steps / To Do

- [ ] Clarify how the source file/data is created (SQL Server replication? manual?)
- [ ] Investigate what `FORM_DATA` contains (JSON structure?)
- [ ] Decide whether deletes should be handled (add `WHEN MATCHED AND DML_TYPE = 'D'`)
- [ ] Determine if `ADJUSTMENT.GLOBAL_ADJUSTMENT_SF` feeds anything downstream (reports, views, etc.)
- [ ] Document target table DDL for `ADJUSTMENT.GLOBAL_ADJUSTMENT_SF`
- [ ] Evaluate whether this pipeline should write to `DIMENSION.ADJUSTMENT` like the others
- [ ] Define the target-state architecture
