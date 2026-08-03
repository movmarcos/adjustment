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
"SRC_BRACKETED","SRC_LEN","SRC_HEX","OBJ_BRACKETED","OBJ_LEN","POWERBI_OBJECT_TYPE"
[STRESS_REGIONAL_MUFGBK_EM_QP],28,"5354524553535F524547494F4E414C5F4D554647424B5F454D5F5150",[RaptorReporting],15,D
[STRESS_REGIONAL_MUFGBK_EM_QS],28,"5354524553535F524547494F4E414C5F4D554647424B5F454D5F5153",[RaptorReporting],15,D
[STRESS_REGIONAL_MUSCAN_AM_QP],28,"5354524553535F524547494F4E414C5F4D555343414E5F414D5F5150",[RaptorReporting],15,D
[STRESS_REGIONAL_MUSEU_EM_QP],27,"5354524553535F524547494F4E414C5F4D555345555F454D5F5150",[RaptorReporting],15,D
[STRESS_REGIONAL_MUSEU_EM_QS],27,"5354524553535F524547494F4E414C5F4D555345555F454D5F5153",[RaptorReporting],15,D
[STRESS_REGIONAL_MUSI_AM_QP],26,"5354524553535F524547494F4E414C5F4D5553495F414D5F5150",[RaptorReporting],15,D
[STRESS_REGIONAL_MUSI_AM_QS],26,"5354524553535F524547494F4E414C5F4D5553495F414D5F5153",[RaptorReporting],15,D
[STRESS_REGIONAL_MUSI_AP_QP],26,"5354524553535F524547494F4E414C5F4D5553495F41505F5150",[RaptorReporting],15,D
[STRESS_REGIONAL_MUSI_EM_QP],26,"5354524553535F524547494F4E414C5F4D5553495F454D5F5150",[RaptorReporting],15,D
[STRESS_REGIONAL_MUSI_EM_QS],26,"5354524553535F524547494F4E414C5F4D5553495F454D5F5153",[RaptorReporting],15,D
[STRESS_REGIONAL_MUSUSA_AM_PP],28,"5354524553535F524547494F4E414C5F4D55535553415F414D5F5050",[RaptorReporting],15,D
[STRESS_REGIONAL_MUSUSA_AM_QP],28,"5354524553535F524547494F4E414C5F4D55535553415F414D5F5150",[RaptorReporting],15,D
[LOAD_STRESS_ADJUSTMENT],22,"4C4F41445F5354524553535F41444A5553544D454E54",[Stress Measures Adjustment Import],33,T
[LOAD_STRESS_ADJUSTMENT_UPLOAD],29,"4C4F41445F5354524553535F41444A5553544D454E545F55504C4F4144",[Stress Measures Adjustment Import],33,T
[LOAD_STRESS_ADJUSTMENT],22,"4C4F41445F5354524553535F41444A5553544D454E54",[Stress Summary PBI Report],25,T
[LOAD_STRESS_ADJUSTMENT_UPLOAD],29,"4C4F41445F5354524553535F41444A5553544D454E545F55504C4F4144",[Stress Summary PBI Report],25,T

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
"SRC_BRACKETED","SRC_LEN","SRC_HEX"
[VAR_REGIONAL_MUFGBK_EM_QP],25,"5641525F524547494F4E414C5F4D554647424B5F454D5F5150"
[VAR_REGIONAL_MUSCAN_AM_QP],25,"5641525F524547494F4E414C5F4D555343414E5F414D5F5150"
[VAR_REGIONAL_MUSEU_EM_QP],24,"5641525F524547494F4E414C5F4D555345555F454D5F5150"
[VAR_REGIONAL_MUSEU_EM_QS],24,"5641525F524547494F4E414C5F4D555345555F454D5F5153"
[VAR_REGIONAL_MUSI_AM_QP],23,"5641525F524547494F4E414C5F4D5553495F414D5F5150"
[VAR_REGIONAL_MUSI_AM_QS],23,"5641525F524547494F4E414C5F4D5553495F414D5F5153"
[VAR_REGIONAL_MUSI_AP_QP],23,"5641525F524547494F4E414C5F4D5553495F41505F5150"
[VAR_REGIONAL_MUSI_EM_QP],23,"5641525F524547494F4E414C5F4D5553495F454D5F5150"
[VAR_REGIONAL_MUSI_EM_QS],23,"5641525F524547494F4E414C5F4D5553495F454D5F5153"
[VAR_REGIONAL_MUSUSA_AM_PP],25,"5641525F524547494F4E414C5F4D55535553415F414D5F5050"
[VAR_REGIONAL_MUSUSA_AM_QP],25,"5641525F524547494F4E414C5F4D55535553415F414D5F5150"
[VAR_REGIONAL_MUSEU_EM_QS_NONSLA],31,"5641525F524547494F4E414C5F4D555345555F454D5F51535F4E4F4E534C41"
[VAR_REGIONAL_MUSEU_EM_QP_NONSLA],31,"5641525F524547494F4E414C5F4D555345555F454D5F51505F4E4F4E534C41"
[VAR_REGIONAL_MUSI_AM_QP_NONSLA],30,"5641525F524547494F4E414C5F4D5553495F414D5F51505F4E4F4E534C41"
[VAR_REGIONAL_MUSI_AM_QS_NONSLA],30,"5641525F524547494F4E414C5F4D5553495F414D5F51535F4E4F4E534C41"
[VAR_REGIONAL_MUSI_AP_QP_NONSLA],30,"5641525F524547494F4E414C5F4D5553495F41505F51505F4E4F4E534C41"
[VAR_REGIONAL_MUSI_EM_QS_NONSLA],30,"5641525F524547494F4E414C5F4D5553495F454D5F51535F4E4F4E534C41"
[VAR_REGIONAL_MUSI_EM_QP_NONSLA],30,"5641525F524547494F4E414C5F4D5553495F454D5F51505F4E4F4E534C41"
[VAR_REGIONAL_MUSUSA_AM_QP_NONSLA],32,"5641525F524547494F4E414C5F4D55535553415F414D5F51505F4E4F4E534C41"
[VAR_REGIONAL_MUSCAN_AM_QP_NONSLA],32,"5641525F524547494F4E414C5F4D555343414E5F414D5F51505F4E4F4E534C41"
[VAR_REGIONAL_MUFGBK_EM_QP_10DAY],31,"5641525F524547494F4E414C5F4D554647424B5F454D5F51505F3130444159"
[VAR_REGIONAL_MUFGBK_EM_QP_MAX],29,"5641525F524547494F4E414C5F4D554647424B5F454D5F51505F4D4158"
[VAR_REGIONAL_MUSCAN_AM_QP_10DAY],31,"5641525F524547494F4E414C5F4D555343414E5F414D5F51505F3130444159"
[VAR_REGIONAL_MUSCAN_AM_QP_MAX],29,"5641525F524547494F4E414C5F4D555343414E5F414D5F51505F4D4158"
[VAR_REGIONAL_MUSEU_EM_QP_10DAY],30,"5641525F524547494F4E414C5F4D555345555F454D5F51505F3130444159"
[VAR_REGIONAL_MUSEU_EM_QP_MAX],28,"5641525F524547494F4E414C5F4D555345555F454D5F51505F4D4158"
[VAR_REGIONAL_MUSEU_EM_QS_10DAY],30,"5641525F524547494F4E414C5F4D555345555F454D5F51535F3130444159"
[VAR_REGIONAL_MUSI_AM_QP_10DAY],29,"5641525F524547494F4E414C5F4D5553495F414D5F51505F3130444159"
[VAR_REGIONAL_MUSI_AM_QP_MAX],27,"5641525F524547494F4E414C5F4D5553495F414D5F51505F4D4158"
[VAR_REGIONAL_MUSI_AM_QS_10DAY],29,"5641525F524547494F4E414C5F4D5553495F414D5F51535F3130444159"
[VAR_REGIONAL_MUSI_AP_QP_10DAY],29,"5641525F524547494F4E414C5F4D5553495F41505F51505F3130444159"
[VAR_REGIONAL_MUSI_AP_QP_MAX],27,"5641525F524547494F4E414C5F4D5553495F41505F51505F4D4158"
[VAR_REGIONAL_MUSI_EM_QP_10DAY],29,"5641525F524547494F4E414C5F4D5553495F454D5F51505F3130444159"
[VAR_REGIONAL_MUSI_EM_QP_MAX],27,"5641525F524547494F4E414C5F4D5553495F454D5F51505F4D4158"
[VAR_REGIONAL_MUSI_EM_QS_10DAY],29,"5641525F524547494F4E414C5F4D5553495F454D5F51535F3130444159"
[VAR_REGIONAL_MUSUSA_AM_PP_10DAY],31,"5641525F524547494F4E414C5F4D55535553415F414D5F50505F3130444159"
[VAR_REGIONAL_MUSUSA_AM_PP_MAX],29,"5641525F524547494F4E414C5F4D55535553415F414D5F50505F4D4158"
[VAR_REGIONAL_MUSUSA_AM_QP_10DAY],31,"5641525F524547494F4E414C5F4D55535553415F414D5F51505F3130444159"
[VAR_REGIONAL_MUSUSA_AM_QP_MAX],29,"5641525F524547494F4E414C5F4D55535553415F414D5F51505F4D4158"
[LOAD_VAR_ADJUSTMENT_UPLOAD],26,"4C4F41445F5641525F41444A5553544D454E545F55504C4F4144"
[LOAD_VAR_ADJUSTMENT_UPLOAD],26,"4C4F41445F5641525F41444A5553544D454E545F55504C4F4144"
[LOAD_VAR_ADJUSTMENT],19,"4C4F41445F5641525F41444A5553544D454E54"
[LOAD_VAR_ADJUSTMENT],19,"4C4F41445F5641525F41444A5553544D454E54"

```

## Step 2 — Fix the seed rows (only if Step 1 shows `src_len > 22`)

Normalizes whitespace (spaces, tabs, CR/LF, non-breaking spaces) and trims:

```sql
UPDATE METADATA.POWERBI_INSERT_SOURCES
SET INSERT_SOURCE       = TRIM(REGEXP_REPLACE(INSERT_SOURCE,       '[\\s\\u00A0]+', ' ')),
    POWERBI_OBJECT_NAME = TRIM(REGEXP_REPLACE(POWERBI_OBJECT_NAME, '[\\s\\u00A0]+', ' '))
WHERE INSERT_SOURCE ILIKE '%STRESS%';
```
SQL Error [100048] [2201B]: Invalid regular expression: '[\s\u00A0]+', invalid escape sequence: \u

Verify the exact match now works (must return the rows):

```sql
SELECT * FROM METADATA.POWERBI_INSERT_SOURCES
WHERE INSERT_SOURCE = 'LOAD_STRESS_ADJUSTMENT';
```

```
RESULT (number of rows + values):
"INSERT_SOURCE","ENTITY","VARLOADTYPE","SOURCESYSTEM","REGION","POWERBI_OBJECT_TYPE","POWERBI_OBJECT_NAME"
LOAD_STRESS_ADJUSTMENT,ANY,ANY,ANY,ANY,T,Stress Measures Adjustment Import
LOAD_STRESS_ADJUSTMENT,ANY,ANY,ANY,ANY,T,Stress Summary PBI Report

```

## Step 3 — Re-trigger the refresh (no reprocessing needed)

The adjustment's numbers are already applied; only the hand-off is missing.
Re-run the legacy proc against the original run log:

```sql
CALL FACT.UPDATE_POWERBI_FOR_ADJUSTMENTS(
    'STRESS', 'RaptorReporting', 'LOAD_STRESS_ADJUSTMENT', '952277', '0');
```

```
RESULT (return value — should be "Success"):
Success
```

## Step 4 — Verify the action row landed

```sql
-- Publish detail (intermediate table — should now have stress rows for the COB)
SELECT * FROM METADATA.POWERBI_PUBLISH_DETAIL
WHERE COBID = <COBID> AND INSERT_SOURCE = 'LOAD_STRESS_ADJUSTMENT';
```

```
RESULT:
"COBID","INSERT_SOURCE","MAX_RUN_LOG_ID","INSERT_TIME","COMMENTS","POWERBI_OBJECT_NAME","POWERBI_OBJECT_TYPE"
20260701,LOAD_STRESS_ADJUSTMENT,952238,2026-07-20 09:36:39.637 +0100,,Stress Measures Adjustment Import,T
20260701,LOAD_STRESS_ADJUSTMENT,952238,2026-07-20 09:36:39.637 +0100,,Stress Summary PBI Report,T
20260701,LOAD_STRESS_ADJUSTMENT,952237,2026-07-20 09:34:42.739 +0100,,Stress Measures Adjustment Import,T
20260701,LOAD_STRESS_ADJUSTMENT,952237,2026-07-20 09:34:42.739 +0100,,Stress Summary PBI Report,T
20260701,LOAD_STRESS_ADJUSTMENT,952179,2026-07-02 14:26:14.739 +0100,,Stress Measures Adjustment Import,T
20260701,LOAD_STRESS_ADJUSTMENT,952179,2026-07-02 14:26:14.739 +0100,,Stress Summary PBI Report,T
20260701,LOAD_STRESS_ADJUSTMENT,952146,2026-07-02 13:06:14.855 +0100,,Stress Measures Adjustment Import,T
20260701,LOAD_STRESS_ADJUSTMENT,952146,2026-07-02 13:06:14.855 +0100,,Stress Summary PBI Report,T
20260701,LOAD_STRESS_ADJUSTMENT,952140,2026-07-02 12:52:17.060 +0100,,Stress Measures Adjustment Import,T
20260701,LOAD_STRESS_ADJUSTMENT,952140,2026-07-02 12:52:17.060 +0100,,Stress Summary PBI Report,T
20260701,LOAD_STRESS_ADJUSTMENT,952062,2026-07-02 10:59:21.557 +0100,,Stress Measures Adjustment Import,T
20260701,LOAD_STRESS_ADJUSTMENT,952062,2026-07-02 10:59:21.557 +0100,,Stress Summary PBI Report,T
20260701,LOAD_STRESS_ADJUSTMENT,951977,2026-07-02 08:19:13.083 +0100,,Stress Measures Adjustment Import,T
20260701,LOAD_STRESS_ADJUSTMENT,951977,2026-07-02 08:19:13.083 +0100,,Stress Summary PBI Report,T
20260701,LOAD_STRESS_ADJUSTMENT,951915,2026-07-02 07:20:40.430 +0100,,Stress Measures Adjustment Import,T
20260701,LOAD_STRESS_ADJUSTMENT,951915,2026-07-02 07:20:40.430 +0100,,Stress Summary PBI Report,T
20260701,LOAD_STRESS_ADJUSTMENT,951838,2026-07-02 05:15:56.483 +0100,,Stress Measures Adjustment Import,T
20260701,LOAD_STRESS_ADJUSTMENT,951838,2026-07-02 05:15:56.483 +0100,,Stress Summary PBI Report,T
20260701,LOAD_STRESS_ADJUSTMENT,951693,2026-07-02 04:33:08.690 +0100,,Stress Measures Adjustment Import,T
20260701,LOAD_STRESS_ADJUSTMENT,951693,2026-07-02 04:33:08.690 +0100,,Stress Summary PBI Report,T
20260701,LOAD_STRESS_ADJUSTMENT,951499,2026-07-01 23:39:21.761 +0100,,Stress Measures Adjustment Import,T
20260701,LOAD_STRESS_ADJUSTMENT,951499,2026-07-01 23:39:21.761 +0100,,Stress Summary PBI Report,T
20260701,LOAD_STRESS_ADJUSTMENT,952277,2026-08-03 15:48:49.773 +0100,,Stress Measures Adjustment Import,T
20260701,LOAD_STRESS_ADJUSTMENT,952277,2026-08-03 15:48:49.773 +0100,,Stress Summary PBI Report,T
20260701,LOAD_STRESS_ADJUSTMENT,952277,2026-08-03 15:48:49.773 +0100,,Stress Measures Adjustment Import,T
20260701,LOAD_STRESS_ADJUSTMENT,952277,2026-08-03 15:48:49.773 +0100,,Stress Summary PBI Report,T

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
"DATASET_NAME","OBJECT_NAME","OBJECT_TYPE","ACTION_TYPE","ACTION_STATUS","COBID","INSERT_SOURCE","REQUEST_TIME"
RaptorReporting,Sensitivity Summary Fields,T,P,W,20260623,LOAD_SENSITIVITY_ADJUSTMENT,2026-07-13 17:17:22.139 +0100
RaptorReporting,Sensitivity Summary Adjustment Import,T,P,W,20260623,LOAD_SENSITIVITY_ADJUSTMENT,2026-07-13 17:17:22.139 +0100
RaptorReporting,Sensitivity Detail Fields,T,P,W,20260623,LOAD_SENSITIVITY_ADJUSTMENT,2026-07-13 17:17:22.139 +0100
RaptorReporting,MarketRiskMeasureDepartment,T,P,W,20260623,LOAD_SENSITIVITY_ADJUSTMENT,2026-07-13 17:17:22.139 +0100
RaptorReporting,Sensitivity Detail Adjustment Import,T,P,W,20260623,LOAD_SENSITIVITY_ADJUSTMENT,2026-07-13 17:17:22.139 +0100
RaptorReporting,Stress Measures Adjustment Import,T,P,W,20260623,LOAD_STRESS_ADJUSTMENT,2026-07-13 16:59:18.860 +0100
RaptorReporting,Stress Summary PBI Report,T,P,W,20260623,LOAD_STRESS_ADJUSTMENT,2026-07-13 16:59:18.860 +0100
RaptorReporting,VaR Adjustment Summary Import,T,P,W,20260623,LOAD_VAR_ADJUSTMENT,2026-07-13 16:49:33.576 +0100
RaptorReporting,VAR_SUMMARY_REPORT,T,P,W,20260623,LOAD_VAR_ADJUSTMENT,2026-07-13 16:49:33.576 +0100
FRTB SA DRC RRAO Model,FRTB SA DRC RRAO Model,D,P,R,,,2026-07-02 14:28:01.420 +0100

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
<empty same cob>
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
"POWERBI_OBJECT_NAME","OBJ_MATCHES_LOWER","SRC_MATCHES_LOWER"
Stress Measures Adjustment Import,true,true
Stress Summary PBI Report,true,true

```

```sql
-- 5c. Retention config (inner join in the view — a 'stress' row must exist)
SELECT * FROM METADATA.POWERBI_DATA_RETENTION
WHERE DATASET_NAME = 'RaptorReporting';
```

```
RESULT:
"DATASET_NAME","DATA_GROUP_NAME","WORKSPACE_NAME","RETENTION_BUSINESS_DAYS","RETENTION_END_OF_WEEK","RETENTION_MONTH_ENDS","RETAINED_DATES","RETENTION_QUARTER_ENDS"
RaptorReporting,var,TECBI_MR_PROD,5,1,0,"",0
RaptorReporting,stress,TECBI_MR_PROD,10,1,0,"",0
RaptorReporting,sensitivity,TECBI_MR_PROD,45,1,0,"",0
RaptorReporting,Adhoc,TECBI_MR_PROD,200,0,0,"",0
RaptorReporting,sensitivity_detail,TECBI_MR_PROD,5,1,1,,0

```

```sql
-- 5d. Waiting actions that could suppress new ones (dedupe guards in the view)
SELECT DATASET_NAME, OBJECT_NAME, OBJECT_TYPE, ACTION_TYPE, ACTION_STATUS, COBID
FROM METADATA.POWERBI_ACTION
WHERE ACTION_STATUS = 'W';
```

```
RESULT:
"DATASET_NAME","OBJECT_NAME","OBJECT_TYPE","ACTION_TYPE","ACTION_STATUS","COBID"
RaptorReporting,Sensitivity Summary Adjustment Import,T,P,W,20260623
RaptorReporting,MarketRiskMeasureDepartment,T,P,W,20260623
RaptorReporting,Sensitivity Summary Fields,T,P,W,20260623
RaptorReporting,Sensitivity Detail Adjustment Import,T,P,W,20260623
RaptorReporting,Sensitivity Detail Fields,T,P,W,20260623
RaptorReporting,Stress Measures Adjustment Import,T,P,W,20260623
RaptorReporting,Stress Summary PBI Report,T,P,W,20260623
RaptorReporting,VaR Adjustment Summary Import,T,P,W,20260623
RaptorReporting,VAR_SUMMARY_REPORT,T,P,W,20260623

```

```sql
-- 5e. COB present on the business-day grid (required by the view's BDR join)
SELECT * FROM METADATA.VW_BUSINESS_DAY_RANGE
WHERE ORIGINAL_COBID = <COBID> AND COBID = <COBID>;
```

```
RESULT:
"ORIGINAL_COBID","COBID","BUSINESS_DAYS_IN_PAST"
20260701,20260701,1

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
CREATE OR REPLACE PROCEDURE "LOAD_STRESS_ADJUSTMENT_UPLOAD"()
RETURNS VARCHAR(100)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
    function fix_binds(param) {
          return param === undefined ? null
              : typeof(param) == "object" ? (param instanceof Date ? param.toJSON() : JSON.stringify(param))
              : param;
    }

    try {
        text_display = "Nothing to do!";
         text_display = "select  1"
        snowflake.execute( {sqlText: text_display} );
        result = "Succeeded";
         
        // Find Adjustments to run
        var sqlGetUpload = `
        CREATE OR REPLACE TEMPORARY TABLE STAGING.TEMP_STRESS_UPLOAD AS
        SELECT 
            ADJUSTMENT_ID,
            COBID,
            IS_DELETED
        FROM DIMENSION.ADJUSTMENT s
        WHERE RUN_STATUS = ''Pending''
        AND PROCESS_TYPE =''Stress''
        AND ADJUSTMENT_TYPE = ''Direct''`;

        snowflake.execute( {sqlText: sqlGetUpload} );

        var adj = snowflake.execute({sqlText: `SELECT COUNT(*) FROM STAGING.TEMP_STRESS_UPLOAD`});
        adj.next();
        var AdjustmentCount = adj.getColumnValue(1);

        if (AdjustmentCount > 0) 
        { 
            // Set Status to Running
            snowflake.execute({sqlText:  `UPDATE DIMENSION.ADJUSTMENT SET RUN_STATUS =''Running''
                        WHERE EXISTS (SELECT 1 FROM STAGING.TEMP_STRESS_UPLOAD t
                                    WHERE t.ADJUSTMENT_ID = DIMENSION.ADJUSTMENT.ADJUSTMENT_ID    AND t.COBID = DIMENSION.ADJUSTMENT.COBID)`});
    
            // Delete data for existing Adjustment ID
            snowflake.execute({
                sqlText:  `DELETE FROM FACT.STRESS_MEASURES_ADJUSTMENT a
                    WHERE EXISTS (SELECT 1 FROM STAGING.TEMP_STRESS_UPLOAD t
                        WHERE t.ADJUSTMENT_ID = a.ADJUSTMENT_ID AND t.COBID = a.COBID)`});
    
            snowflake.execute({
                sqlText:  `DELETE FROM FACT.STRESS_MEASURES_ADJUSTMENT_SUMMARY a
                    WHERE EXISTS (SELECT 1 FROM STAGING.TEMP_STRESS_UPLOAD t
                        WHERE t.ADJUSTMENT_ID = a.ADJUSTMENT_ID    AND t.COBID = a.COBID)`});
        
            // Get new Run Log ID
            snowflake.execute({
                sqlText: `CREATE OR REPLACE TEMPORARY TABLE FACT.TEMP_ADJUSTMENT_UPLOAD_RUNLOG AS (
                    SELECT BATCH.SEQ_RUN_LOG.nextval AS Run_log_id, COBID, TO_NUMBER(0) as RECORD_COUNT
                    FROM (SELECT DISTINCT COBID FROM STAGING.TEMP_STRESS_UPLOAD WHERE IS_DELETED = FALSE))`});
                   
            snowflake.execute({
                sqlText: `INSERT INTO BATCH.RUN_LOG (RUN_LOG_ID, COBID, PROC_NAME, PROC_PARAMETERS, BATCH_ACTION_DAILY_KEY, RECORD_COUNT, ERROR, ERROR_MESSAGE)
                    SELECT Run_log_id, COBID, ''FACT.LOAD_STRESS_ADJUSTMENT_UPLOAD'',''stress'', 0, 0, FALSE, ''''
                    FROM FACT.TEMP_ADJUSTMENT_UPLOAD_RUNLOG`});
        
            // Create new PRODUCT CATEGORY ATTRIBUTES if not exists 
            snowflake.execute({
                sqlText: `INSERT INTO DIMENSION.PRODUCT_CATEGORY_ATTRIBUTES(
            PRODUCT_CATEGORY_ATTRIBUTES_KEY, 
            MUREX_FAMILY, 
            MUREX_GROUP, 
            TRADE_TYPOLOGY, 
            INSTRUMENT_PRODUCT_CATEGORY, 
            GOV_OR_NON_GOV, 
            G4_OR_NON_G4,
            MARKET_SECTOR_DESCRIPTION, 
            IS_HEDGE_BOOK_INCLUDED
            )
            SELECT
                DIMENSION.SEQ_PRODUCT_CATEGORY_ATTRIBUTES.nextval,
                MUREX_FAMILY,
                MUREX_GROUP, 
                TRADE_TYPOLOGY, 
                INSTRUMENT_PRODUCT_CATEGORY, 
                GOV_OR_NON_GOV, 
                G4_OR_NON_G4,
                MARKET_SECTOR_DESCRIPTION, 
                IS_HEDGE_BOOK_INCLUDED
			FROM (
            SELECT DISTINCT
                TRIM(SPLIT_PART(a.PRODUCT_CATEGORY_ATTRIBUTES , ''|'',1)) AS MUREX_FAMILY,
                TRIM(SPLIT_PART(a.PRODUCT_CATEGORY_ATTRIBUTES , ''|'',2)) AS MUREX_GROUP, 
                TRIM(SPLIT_PART(a.PRODUCT_CATEGORY_ATTRIBUTES , ''|'',3)) AS TRADE_TYPOLOGY, 
                TRIM(SPLIT_PART(a.PRODUCT_CATEGORY_ATTRIBUTES , ''|'',4)) AS INSTRUMENT_PRODUCT_CATEGORY, 
                TRIM(SPLIT_PART(a.PRODUCT_CATEGORY_ATTRIBUTES , ''|'',5)) AS GOV_OR_NON_GOV, 
                TRIM(SPLIT_PART(a.PRODUCT_CATEGORY_ATTRIBUTES , ''|'',6)) AS G4_OR_NON_G4,
                TRIM(SPLIT_PART(a.PRODUCT_CATEGORY_ATTRIBUTES , ''|'',7)) AS MARKET_SECTOR_DESCRIPTION, 
                TRIM(SPLIT_PART(a.PRODUCT_CATEGORY_ATTRIBUTES , ''|'',8)) AS IS_HEDGE_BOOK_INCLUDED
            FROM DIMENSION.ADJUSTMENT a
            WHERE NULLIF(REPLACE(a.PRODUCT_CATEGORY_ATTRIBUTES,''|'',''''),'''') IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1 
                    FROM DIMENSION.PRODUCT_CATEGORY_ATTRIBUTES p 
                    WHERE p.PCA_CONCAT_KEY = REPLACE(a.PRODUCT_CATEGORY_ATTRIBUTES, '' '','''')
                    )
                AND EXISTS (
                    SELECT 1
                    FROM STAGING.TEMP_STRESS_UPLOAD t
                    WHERE t.ADJUSTMENT_ID = a.ADJUSTMENT_ID
                    AND t.COBID = a.COBID
                    AND t.IS_DELETED = FALSE
                    )
				)`});    
        
            // Create new PRODUCT CATEGORY ATTRIBUTES if not exists 
            snowflake.execute({
                sqlText: `INSERT INTO FACT.STRESS_MEASURES_ADJUSTMENT(
                COBID, 
                REGION_AREA_KEY, 
                ENTITY_KEY, 
                TRADE_KEY, 
                BOOK_KEY, 
                TRADE_CURRENCY, 
                COMMON_INSTRUMENT_KEY, 
                COMMON_INSTRUMENT_FCD_KEY, 
                COUNTERPARTY_KEY, 
                PRODUCT_CATEGORY_ATTRIBUTES_KEY, 
                ADJUSTMENT_ID, 
                STRESS_SIMULATION_KEY, 
                SOURCE_SYSTEM_CODE, 
                IS_OFFICIAL_SOURCE, 
                SIMULATION_PL_IN_USD, 
                LOAD_SET, 
                RUN_LOG_ID
                )
                SELECT
                    a.COBID ,
                    -1 AS REGION_AREA_KEY,
                    E.ENTITY_KEY ,
                    IFNULL(T.TRADE_KEY, -1) AS TRADE_KEY,
                    b.BOOK_KEY ,
                    IFNULL(a.CURRENCY_CODE, ''N/A'') AS TRADE_CURRENCY,
                    COALESCE(CI.COMMON_INSTRUMENT_KEY, CI2.COMMON_INSTRUMENT_KEY, -1) AS COMMON_INSTRUMENT_KEY,
                    IFNULL(CIF.COMMON_INSTRUMENT_FCD_KEY, -1) AS COMMON_INSTRUMENT_FCD_KEY,
                    -1 AS COUNTERPARTY_KEY,
                    IFNULL(PCA.PRODUCT_CATEGORY_ATTRIBUTES_KEY, -1) AS PRODUCT_CATEGORY_ATTRIBUTES_KEY,
                    a.ADJUSTMENT_ID ,
                    IFNULL(SS.STRESS_SIMULATION_KEY,-1) AS STRESS_SIMULATION_KEY,
                    ''QP'' AS SOURCE_SYSTEM_CODE,
                    TRUE,
                    IFNULL(a.ADJUSTMENT_VALUE_IN_USD, 0),
                    ''STRESS_UPLOAD'',
                    rl.RUN_LOG_ID
                FROM DIMENSION.ADJUSTMENT a
                    INNER JOIN DIMENSION.ENTITY E 
                        ON E.ENTITY_CODE = a.ENTITY_CODE 
                    INNER JOIN DIMENSION.BOOK b 
                        ON b.BOOK_CODE = a.BOOK_CODE 
                        AND b.IS_CURRENT_ROW = TRUE
                        AND b.GUARANTEED_ENTITY = IFNULL(a.GUARANTEED_ENTITY , ''N/A'')
                    LEFT JOIN DIMENSION.TRADE T
                        ON T.IS_CURRENT_ROW = TRUE
                        AND T.TRADE_CODE = IFNULL(NULLIF(a.TRADE_CODE, ''N/A'') , CONCAT(a.BOOK_CODE, ''/Adjustment''))
                    LEFT JOIN DIMENSION.COMMON_INSTRUMENT CI
                        ON CI.IS_CURRENT_ROW =TRUE
                        AND CI.INSTRUMENT_CODE = a.INSTRUMENT_CODE
                    LEFT JOIN DIMENSION.COMMON_INSTRUMENT CI2
                        ON CI2.IS_CURRENT_ROW =TRUE
                        AND CI2.INSTRUMENT_KEY = T.INSTRUMENT_KEY
                    LEFT JOIN DIMENSION.COMMON_INSTRUMENT_FCD CIF
                        ON CIF.IS_CURRENT_ROW = TRUE
                        AND CIF.INSTRUMENT_KEY = IFNULL(CI.INSTRUMENT_KEY, T.INSTRUMENT_KEY)
                    LEFT JOIN DIMENSION.STRESS_SIMULATION SS
                        ON SS.STRESS_SIMULATION_NAME = a.SIMULATION_NAME
                    LEFT JOIN DIMENSION.PRODUCT_CATEGORY_ATTRIBUTES PCA
                        ON PCA.PCA_CONCAT_KEY  = REPLACE(a.PRODUCT_CATEGORY_ATTRIBUTES, '' '','''')
                    INNER JOIN FACT.TEMP_ADJUSTMENT_UPLOAD_RUNLOG rl
                        ON rl.COBID = a.COBID 
                WHERE 
                    EXISTS (SELECT 1
                            FROM STAGING.TEMP_STRESS_UPLOAD t
                            WHERE t.ADJUSTMENT_ID = a.ADJUSTMENT_ID
                            AND t.COBID = a.COBID
                            AND t.IS_DELETED = FALSE)`});
        
            // Add data to stres summary
            snowflake.execute({
                sqlText: `INSERT INTO FACT.STRESS_MEASURES_ADJUSTMENT_SUMMARY (
                    COBID, 
                    REGION_AREA_KEY, 
                    ENTITY_KEY, 
                    BOOK_KEY, 
                    TRADE_CURRENCY, 
                    COMMON_INSTRUMENT_KEY, 
                    COMMON_INSTRUMENT_FCD_KEY, 
                    COUNTERPARTY_KEY, 
                    PRODUCT_CATEGORY_ATTRIBUTES_KEY, 
                    ADJUSTMENT_ID, 
                    STRESS_SIMULATION_KEY, 
                    SOURCE_SYSTEM_CODE, 
                    SIMULATION_PL_IN_USD, 
                    LOAD_SET, 
                    RUN_LOG_ID
                    )
                    SELECT
                        a.COBID ,
                        a.REGION_AREA_KEY,
                        a.ENTITY_KEY ,
                        a.BOOK_KEY ,
                        a.TRADE_CURRENCY,
                        a.COMMON_INSTRUMENT_KEY,
                        a.COMMON_INSTRUMENT_FCD_KEY,
                        a.COUNTERPARTY_KEY,
                        a.PRODUCT_CATEGORY_ATTRIBUTES_KEY,
                        a.ADJUSTMENT_ID,
                        a.STRESS_SIMULATION_KEY,
                        a.SOURCE_SYSTEM_CODE,
                        a.SIMULATION_PL_IN_USD,
                        a.LOAD_SET,
                        a.RUN_LOG_ID
                    FROM 
                        FACT.STRESS_MEASURES_ADJUSTMENT a
                    WHERE 
                        EXISTS (SELECT 1
                                FROM STAGING.TEMP_STRESS_UPLOAD t
                                WHERE t.COBID = a.COBID 
                                AND t.ADJUSTMENT_ID = a.ADJUSTMENT_ID
                                AND t.IS_DELETED = FALSE)`});    
            
            // Completed Update status
            snowflake.execute({
                sqlText: `UPDATE DIMENSION.ADJUSTMENT
                    SET RUN_STATUS =''Processed'', PROCESS_DATE = CURRENT_TIMESTAMP(), RECORD_COUNT=1
                    WHERE EXISTS (SELECT 1 FROM STAGING.TEMP_STRESS_UPLOAD t
                        WHERE t.COBID = DIMENSION.ADJUSTMENT.COBID 
                        AND t.ADJUSTMENT_ID = DIMENSION.ADJUSTMENT.ADJUSTMENT_ID)`});
    
            // End Run Log
            snowflake.execute({
                sqlText: `UPDATE BATCH.RUN_LOG 
                    SET END_TIME = CURRENT_TIMESTAMP()
                    WHERE RUN_LOG_ID IN (SELECT RUN_LOG_ID FROM FACT.TEMP_ADJUSTMENT_UPLOAD_RUNLOG)`});
    
             var rs = snowflake.execute({sqlText: 
                `SELECT array_to_string(arrayagg(RUN_LOG_ID) within group ( order by COBID), '', '' ) as RunLogList 
                    FROM  FACT.TEMP_ADJUSTMENT_UPLOAD_RUNLOG`
                });
            rs.next();
            var RunLogList = rs.getColumnValue(1);
    
            snowflake.execute({
                sqlText: `call FACT.UPDATE_POWERBI_FOR_ADJUSTMENTS(''STRESS'',''RaptorReporting'',''LOAD_STRESS_ADJUSTMENT_UPLOAD'',:1,''0'')`,
                binds: [RunLogList]
                })
        }
        
    }
    catch(err){
                
        var ErrorMessage = "Failed: Code: " + err.code + " State: " + err.state;
            ErrorMessage += " Message: " + err.message;
            ErrorMessage += " Stack Trace:" + err.stackTraceTxt;

        // Set Run Status to Processed
        var AdjustmentErrorMessage = ["Stress Upload Load Error", err.message].join(" : ")
        snowflake.execute({
            sqlText:  `UPDATE DIMENSION.ADJUSTMENT fa SET RUN_STATUS = ''Error'', ERRORMESSAGE = :1
                       WHERE RUN_STATUS = ''Running''
                       AND ADJUSTMENT_TYPE = ''Upload''
                       AND PROCESS_TYPE = ''VaR''`,
            binds: [AdjustmentErrorMessage]
            });

         var sqlRunLog = `UPDATE BATCH.RUN_LOG
            SET ERROR=TRUE, ERROR_MESSAGE=:1
            FROM FACT.TEMP_ADJUSTMENT_UPLOAD_RUNLOG
            WHERE FACT.TEMP_ADJUSTMENT_UPLOAD_RUNLOG.COBID = BATCH.RUN_LOG.COBID
            AND FACT.TEMP_ADJUSTMENT_UPLOAD_RUNLOG.RUN_LOG_ID = BATCH.RUN_LOG.RUN_LOG_ID`;
  
       snowflake.execute({
            sqlText: sqlRunLog,
            binds: [ErrorMessage]
            });

        result = "ERROR: " + ErrorMessage;
    }

    return result;
';
```

---

## Send-back checklist

- [ ] Step 1 lengths/hex pasted (stress + VaR comparison)
- [ ] Step 2 run? (yes/no) and post-fix exact-match result
- [ ] Step 3 return value
- [ ] Step 4 both results
- [ ] Step 5 only if needed
- [ ] Step 6 DDL pasted
