# PowerBI Stress Refresh — Diagnostic Runbook

**Purpose:** a Stress adjustment (COB `20260701`) reaches *Processed* but no row
appears in `METADATA.POWERBI_ACTION`. Evidence so far points to corrupted seed
data in `METADATA.POWERBI_INSERT_SOURCES` (exact-match on
`'LOAD_STRESS_ADJUSTMENT'` returns nothing while a full scan shows the rows —
classic hidden-whitespace symptom). This runbook confirms it, fixes it, and
re-triggers the refresh.

**How to use:** run each step in order in the target environment
(`DVLP_RAPTOR_NEWADJ` or wherever you're testing). Paste each result into the
`RESULT:` block under the step, then send the file back.

Replace these placeholders before running:

| Placeholder | Meaning | Example |
|---|---|---|
| `<RUN_LOG_ID>` | `run_log_id` from the pipeline result JSON of the processed stress adjustment | `12345` |
| `<COBID>` | COB used in the test | `20260701` |

---

## Step 1 — Confirm hidden characters in the seed rows

A clean `LOAD_STRESS_ADJUSTMENT` has `LEN = 22` and
`HEX = 4C4F41445F5354524553535F41444A5553544D454E54`.
Anything longer/different confirms the corruption.

```sql
SELECT '[' || INSERT_SOURCE || ']'        AS src_bracketed,
       LENGTH(INSERT_SOURCE)              AS src_len,
       HEX_ENCODE(INSERT_SOURCE)          AS src_hex,
       '[' || POWERBI_OBJECT_NAME || ']'  AS obj_bracketed,
       LENGTH(POWERBI_OBJECT_NAME)        AS obj_len,
       POWERBI_OBJECT_TYPE
FROM METADATA.POWERBI_INSERT_SOURCES
WHERE INSERT_SOURCE ILIKE '%STRESS%';
```

```
RESULT:

```

Also grab the working VaR row for comparison (clean `LOAD_VAR_ADJUSTMENT` has `LEN = 19`):

```sql
SELECT '[' || INSERT_SOURCE || ']' AS src_bracketed,
       LENGTH(INSERT_SOURCE)       AS src_len,
       HEX_ENCODE(INSERT_SOURCE)   AS src_hex
FROM METADATA.POWERBI_INSERT_SOURCES
WHERE INSERT_SOURCE ILIKE '%VAR%';
```

```
RESULT:

```

## Step 2 — Fix the seed rows (only if Step 1 shows `src_len > 22`)

Normalizes whitespace (spaces, tabs, CR/LF, non-breaking spaces) and trims:

```sql
UPDATE METADATA.POWERBI_INSERT_SOURCES
SET INSERT_SOURCE       = TRIM(REGEXP_REPLACE(INSERT_SOURCE,       '[\\s\\u00A0]+', ' ')),
    POWERBI_OBJECT_NAME = TRIM(REGEXP_REPLACE(POWERBI_OBJECT_NAME, '[\\s\\u00A0]+', ' '))
WHERE INSERT_SOURCE ILIKE '%STRESS%';
```

Verify the exact match now works (must return the rows):

```sql
SELECT * FROM METADATA.POWERBI_INSERT_SOURCES
WHERE INSERT_SOURCE = 'LOAD_STRESS_ADJUSTMENT';
```

```
RESULT (number of rows + values):

```

## Step 3 — Re-trigger the refresh (no reprocessing needed)

The adjustment's numbers are already applied; only the hand-off is missing.
Re-run the legacy proc against the original run log:

```sql
CALL FACT.UPDATE_POWERBI_FOR_ADJUSTMENTS(
    'STRESS', 'RaptorReporting', 'LOAD_STRESS_ADJUSTMENT', '<RUN_LOG_ID>', '0');
```

```
RESULT (return value — should be "Success"):

```

## Step 4 — Verify the action row landed

```sql
-- Publish detail (intermediate table — should now have stress rows for the COB)
SELECT * FROM METADATA.POWERBI_PUBLISH_DETAIL
WHERE COBID = <COBID> AND INSERT_SOURCE = 'LOAD_STRESS_ADJUSTMENT';
```

```
RESULT:

```

```sql
-- The actual refresh request (expect ACTION_STATUS = 'W' rows)
SELECT DATASET_NAME, OBJECT_NAME, OBJECT_TYPE, ACTION_TYPE, ACTION_STATUS,
       COBID, INSERT_SOURCE, REQUEST_TIME
FROM METADATA.POWERBI_ACTION
ORDER BY REQUEST_TIME DESC NULLS LAST
LIMIT 10;
```

```
RESULT:

```

**If the action row is there → done, stop here.** Send the file back anyway so
the finding is recorded.

---

## Step 5 — Fallback (only if Step 4 shows publish detail but NO action row)

The next gate is the action view's data-group classification, which uses
**case-sensitive** `LIKE '%stress%'` (lowercase) and an inner join to the
retention config. These pin down which gate drops the row:

```sql
-- 5a. What the action view emits for the COB (empty = view is dropping it)
SELECT * FROM METADATA.VW_POWERBI_ACTION_INSERT_SOURCE
WHERE COBID = <COBID> AND ORIGINAL_COBID = <COBID>;
```

```
RESULT:

```

```sql
-- 5b. Case-sensitivity probe: does this environment match lowercase 'stress'
--     against the seeded names? (TRUE anywhere = classification can work)
SELECT POWERBI_OBJECT_NAME,
       POWERBI_OBJECT_NAME LIKE '%stress%'   AS obj_matches_lower,
       INSERT_SOURCE       LIKE '%stress%'   AS src_matches_lower
FROM METADATA.POWERBI_INSERT_SOURCES
WHERE INSERT_SOURCE = 'LOAD_STRESS_ADJUSTMENT';
```

```
RESULT:

```

```sql
-- 5c. Retention config (inner join in the view — a 'stress' row must exist)
SELECT * FROM METADATA.POWERBI_DATA_RETENTION
WHERE DATASET_NAME = 'RaptorReporting';
```

```
RESULT:

```

```sql
-- 5d. Waiting actions that could suppress new ones (dedupe guards in the view)
SELECT DATASET_NAME, OBJECT_NAME, OBJECT_TYPE, ACTION_TYPE, ACTION_STATUS, COBID
FROM METADATA.POWERBI_ACTION
WHERE ACTION_STATUS = 'W';
```

```
RESULT:

```

```sql
-- 5e. COB present on the business-day grid (required by the view's BDR join)
SELECT * FROM METADATA.VW_BUSINESS_DAY_RANGE
WHERE ORIGINAL_COBID = <COBID> AND COBID = <COBID>;
```

```
RESULT:

```

## Step 6 — Legacy stress upload proc (context, run once regardless)

The legacy scaling flow called this proc as a stress-only side effect; the new
SP does not. Capturing its DDL tells us whether it performs its own PowerBI
publish that we may also need to replicate:

```sql
SELECT GET_DDL('PROCEDURE', 'FACT.LOAD_STRESS_ADJUSTMENT_UPLOAD()');
```

```
RESULT (paste full DDL):

```

---

## Send-back checklist

- [ ] Step 1 lengths/hex pasted (stress + VaR comparison)
- [ ] Step 2 run? (yes/no) and post-fix exact-match result
- [ ] Step 3 return value
- [ ] Step 4 both results
- [ ] Step 5 only if needed
- [ ] Step 6 DDL pasted
