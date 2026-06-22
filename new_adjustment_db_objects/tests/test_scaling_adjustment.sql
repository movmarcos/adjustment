-- =============================================================================
-- TEST HARNESS — Scaling Adjustment parity (Scale / Flatten / Roll)
-- =============================================================================
-- Purpose
--   Submit ONE scaling adjustment with parameters you control, process it
--   synchronously, then dump the adjustment (delta) table and the adjusted
--   table/view so the result can be compared.
--
-- Parity workflow (DVLP vs PROD)
--   1. Edit SECTION 1 (parameters) to match a known adjustment.
--   2. Run the whole script in DVLP, capture the SECTION 5 results.
--   3. Switch the database (SECTION 0) to PROD, run again with the SAME
--      parameters, capture the results.
--   4. Hand both outputs to the validator — the delta and adjusted totals
--      should match.
--
-- Notes
--   * Covers the Scaling category only: adjustment_type = Scale | Flatten | Roll.
--   * OBJECT_CONSTRUCT drops NULL keys, so any filter left NULL = wildcard.
--   * Re-running with the same p_reference soft-deletes the previous test
--     adjustment automatically (handled by SP_SUBMIT_ADJUSTMENT), so it is safe
--     to iterate. The adjustment is LEFT IN PLACE (see SECTION 6 to remove it).
--   * ⚠ SP_RUN_PIPELINE processes ALL eligible Pending adjustments in the
--     scope+COB, not only this one. Run on a controlled test COB, or when no
--     other pending work exists in that scope.
-- =============================================================================


-- 0. ENVIRONMENT  ── change the database when running in PROD ──────────────────
USE DATABASE {{DATABASE}};              -- <<< {{DATABASE}} = DATABASE in config.py; replace inline for a PROD run
USE SCHEMA  ADJUSTMENT_APP;


-- 1. PARAMETERS  ── edit these; use the SAME values in DVLP and PROD ───────────

--  Core
SET p_process_type    = 'VaR';          -- VaR | Stress | Sensitivity | FRTB | FRTBDRC | FRTBRRAO | FRTBALL
SET p_adjustment_type = 'Scale';        -- Scale | Flatten | Roll
SET p_cobid           = 20260328;       -- target COB (YYYYMMDD)
SET p_source_cobid    = 20260328;       -- Scale/Flatten: = target COB.  Roll: the prior COB to roll FROM.
SET p_scale_factor    = 1.05;           -- Scale: e.g. 1.05 (+5%).  Flatten: ignored.  Roll: e.g. 1.0
SET p_reference       = 'PARITY_TEST_001';  -- unique tag; identifies + replaces this test adjustment

--  Main filters  (entity is REQUIRED; at least one of department/book is required)
SET p_entity_code        = 'MUSI';
SET p_source_system_code = NULL;
SET p_department_code    = NULL;
SET p_book_code          = 'BOOK123';

--  Specific (scope) filters  — leave NULL if your scope does not use them
SET p_currency_code      = NULL;
SET p_trade_typology     = NULL;        -- Stress / Sensitivity / FRTB
SET p_strategy           = NULL;        -- Sensitivity / FRTB
SET p_instrument_code    = NULL;        -- Stress / Sensitivity / FRTB
SET p_measure_type_code  = NULL;        -- Sensitivity / FRTB
SET p_simulation_name    = NULL;        -- Stress
SET p_simulation_source  = NULL;        -- Stress


-- 2. DERIVED  ── no need to edit below this line ──────────────────────────────

-- stream scope (the FRTB family all run under the 'FRTB' stream)
SET p_scope = (SELECT CASE
    WHEN UPPER($p_process_type) IN ('FRTB','FRTBDRC','FRTBRRAO','FRTBALL') THEN 'FRTB'
    WHEN UPPER($p_process_type) = 'VAR'         THEN 'VaR'
    WHEN UPPER($p_process_type) = 'STRESS'      THEN 'Stress'
    WHEN UPPER($p_process_type) = 'SENSITIVITY' THEN 'Sensitivity'
    ELSE $p_process_type END);

SET p_pipeline = (SELECT CASE
    WHEN UPPER($p_process_type) IN ('FRTB','FRTBDRC','FRTBRRAO','FRTBALL')
         THEN '["FRTB","FRTBDRC","FRTBRRAO","FRTBALL"]'
    ELSE '["' || $p_process_type || '"]' END);

-- table names + USD metric for this scope (used by the SECTION 5 verification)
SET (p_fact_tbl, p_adj_tbl, p_adjusted_tbl, p_metric_usd) = (
    SELECT FACT_TABLE, ADJUSTMENTS_TABLE, FACT_ADJUSTED_TABLE, METRIC_USD_NAME
    FROM   ADJUSTMENT_APP.ADJUSTMENTS_SETTINGS
    WHERE  UPPER(PROCESS_TYPE) = UPPER($p_process_type));

-- submit payload (NULL filters are dropped by OBJECT_CONSTRUCT → wildcard)
SET p_payload = (SELECT TO_JSON(OBJECT_CONSTRUCT(
    'cobid',              $p_cobid,
    'process_type',       $p_process_type,
    'adjustment_type',    $p_adjustment_type,
    'source_cobid',       $p_source_cobid,
    'scale_factor',       $p_scale_factor,
    'global_reference',   $p_reference,
    'reason',             'Parity test ' || $p_reference,
    'requires_approval',  FALSE,
    'entity_code',        $p_entity_code,
    'source_system_code', $p_source_system_code,
    'department_code',    $p_department_code,
    'book_code',          $p_book_code,
    'currency_code',      $p_currency_code,
    'trade_typology',     $p_trade_typology,
    'strategy',           $p_strategy,
    'instrument_code',    $p_instrument_code,
    'measure_type_code',  $p_measure_type_code,
    'simulation_name',    $p_simulation_name,
    'simulation_source',  $p_simulation_source
)));

-- resolved config — also tells you exactly which tables to compare in prod
SELECT $p_process_type AS PROCESS_TYPE, $p_scope AS STREAM_SCOPE,
       $p_adjustment_type AS ADJ_TYPE, $p_cobid AS COBID,
       $p_source_cobid AS SOURCE_COBID, $p_scale_factor AS SCALE_FACTOR,
       $p_fact_tbl AS FACT_TABLE, $p_adj_tbl AS ADJUSTMENT_TABLE,
       $p_adjusted_tbl AS ADJUSTED_TABLE, $p_metric_usd AS METRIC_USD,
       $p_payload AS SUBMIT_PAYLOAD;


-- 3. SUBMIT + PROCESS  ────────────────────────────────────────────────────────
CALL ADJUSTMENT_APP.SP_SUBMIT_ADJUSTMENT($p_payload);
CALL ADJUSTMENT_APP.SP_RUN_PIPELINE($p_scope, $p_pipeline);


-- 4. RESOLVE IDENTIFIERS  ── MAX() guarantees one row even if nothing matched ──
SET (p_adj_id, p_dim_adj_id, p_run_status, p_err) = (
    SELECT MAX(ADJ_ID), MAX(DIMENSION_ADJ_ID), MAX(RUN_STATUS), MAX(ERRORMESSAGE)
    FROM   ADJUSTMENT_APP.ADJ_HEADER
    WHERE  COBID = $p_cobid
      AND  UPPER(GLOBAL_REFERENCE) = UPPER($p_reference)
      AND  IS_DELETED = FALSE);


-- 5. VERIFICATION  ────────────────────────────────────────────────────────────

-- 5a. Header — expect RUN_STATUS = 'Processed' and BLOCKED_BY_ADJ_ID = NULL
SELECT ADJ_ID, DIMENSION_ADJ_ID, RUN_STATUS, BLOCKED_BY_ADJ_ID,
       SCALE_FACTOR, SCALE_FACTOR_ADJUSTED, COBID, SOURCE_COBID,
       RECORD_COUNT, ERRORMESSAGE,
       ENTITY_CODE, DEPARTMENT_CODE, BOOK_CODE
FROM   ADJUSTMENT_APP.ADJ_HEADER
WHERE  COBID = $p_cobid
  AND  UPPER(GLOBAL_REFERENCE) = UPPER($p_reference)
  AND  IS_DELETED = FALSE
ORDER  BY CREATED_DATE DESC;

-- 5b. ADJUSTMENT (delta) table — totals for THIS adjustment (exact, by ADJUSTMENT_ID)
EXECUTE IMMEDIATE
    'SELECT ''adjustment_delta'' AS RESULT_SET, '
 || 'COUNT(*) AS ROW_COUNT, '
 || 'COUNT_IF(' || $p_metric_usd || ' <> 0) AS NONZERO_ROWS, '
 || 'SUM('       || $p_metric_usd || ') AS TOTAL_ADJUSTMENT_USD '
 || 'FROM '      || $p_adj_tbl    || ' '
 || 'WHERE COBID = ' || $p_cobid::STRING
 || ' AND ADJUSTMENT_ID = ' || COALESCE($p_dim_adj_id::STRING, '-1');

-- 5c. ADJUSTMENT (delta) table — sample rows (largest magnitude first)
EXECUTE IMMEDIATE
    'SELECT * FROM ' || $p_adj_tbl || ' '
 || 'WHERE COBID = ' || $p_cobid::STRING
 || ' AND ADJUSTMENT_ID = ' || COALESCE($p_dim_adj_id::STRING, '-1')
 || ' ORDER BY ABS(' || $p_metric_usd || ') DESC LIMIT 100';

-- 5d. ADJUSTED table/view — totals
--   VaR & Stress: FACT_ADJUSTED_TABLE *is* the adjustment table → filter by ADJUSTMENT_ID.
--   Sensitivity & FRTB: FACT_ADJUSTED_TABLE is a combined *_ADJUSTED view that may NOT
--   carry ADJUSTMENT_ID. If this 5d errors or returns nothing meaningful, comment it
--   out and use 5d-alt below (filter by COB + entity).
EXECUTE IMMEDIATE
    'SELECT ''adjusted_view'' AS RESULT_SET, '
 || 'COUNT(*) AS ROW_COUNT, '
 || 'SUM('      || $p_metric_usd || ') AS TOTAL_ADJUSTED_USD '
 || 'FROM '     || $p_adjusted_tbl || ' '
 || 'WHERE COBID = ' || $p_cobid::STRING
 || ' AND ADJUSTMENT_ID = ' || COALESCE($p_dim_adj_id::STRING, '-1');

-- 5d-alt (Sensitivity / FRTB combined *_ADJUSTED views — filter by COB + entity):
-- EXECUTE IMMEDIATE
--     'SELECT ''adjusted_view'' AS RESULT_SET, COUNT(*) AS ROW_COUNT, '
--  || 'SUM(' || $p_metric_usd || ') AS TOTAL_ADJUSTED_USD '
--  || 'FROM ' || $p_adjusted_tbl || ' '
--  || 'WHERE COBID = ' || $p_cobid::STRING
--  || ' AND ENTITY_CODE = ''' || $p_entity_code || '''';

-- 5e. ORIGINAL (unadjusted) total at the COB, by entity — context for the delta.
--   NOTE: some fact tables key dimensions (ENTITY_KEY) rather than store ENTITY_CODE;
--   if this errors for your scope, drop the ENTITY_CODE predicate or adjust it.
EXECUTE IMMEDIATE
    'SELECT ''original'' AS RESULT_SET, COUNT(*) AS ROW_COUNT, '
 || 'SUM(' || $p_metric_usd || ') AS TOTAL_ORIGINAL_USD '
 || 'FROM ' || $p_fact_tbl || ' '
 || 'WHERE COBID = ' || $p_cobid::STRING
 || ' AND ENTITY_CODE = ''' || $p_entity_code || '''';


-- 6. (OPTIONAL) CLEANUP  ── adjustment is kept by default; uncomment to remove ──
-- UPDATE ADJUSTMENT_APP.ADJ_HEADER
--    SET IS_DELETED = TRUE, RUN_STATUS = 'Replaced',
--        ERRORMESSAGE = 'Removed by test harness'
--  WHERE COBID = $p_cobid
--    AND UPPER(GLOBAL_REFERENCE) = UPPER($p_reference);
-- EXECUTE IMMEDIATE
--     'DELETE FROM ' || $p_adj_tbl
--  || ' WHERE COBID = ' || $p_cobid::STRING
--  || ' AND ADJUSTMENT_ID = ' || COALESCE($p_dim_adj_id::STRING, '-1');
