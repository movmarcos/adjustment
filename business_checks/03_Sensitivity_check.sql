-- =============================================================================
-- BUSINESS NUMBER CHECK — Sensitivity
-- =============================================================================
-- Tie out one adjustment's numbers between the new engine (DVLP_RAPTOR_NEWADJ)
-- and production (PROD_RAPTOR). Run in one DB, export to Excel; repeat in the
-- other DB and compare.
--
-- HOW TO USE
--   1. Connect a worksheet to the database you want to check.
--   2. Set the database context — uncomment ONE:
--        -- USE DATABASE DVLP_RAPTOR_NEWADJ;
--        -- USE DATABASE PROD_RAPTOR;
--   3. Set the numeric ADJUSTMENT_ID (from DIMENSION.ADJUSTMENT — NOT the long
--      hash/UUID). Each database has its OWN id for the same adjustment, so set
--      the right one for the DB you are connected to.
--   4. Run each query and export the grid to Excel.
-- =============================================================================

SET ADJ_ID = 100123;   -- <<< CHANGE ME for each database


-- 1) HEADER — one row describing the adjustment ---------------------------------
SELECT
    ADJUSTMENT_ID, COBID, SOURCE_COBID,
    PROCESS_TYPE, ADJUSTMENT_TYPE, SCALE_FACTOR,
    ENTITY_CODE, SOURCE_SYSTEM_CODE, DEPARTMENT_CODE, BOOK_CODE,
    CURRENCY_CODE, TRADE_CODE, TRADE_TYPOLOGY, STRATEGY,
    TENOR_CODE, UNDERLYING_TENOR_CODE, CURVE_CODE, MEASURE_TYPE_CODE,
    ADJUSTMENT_VALUE_IN_USD, RECORD_COUNT, RUN_STATUS,
    USERNAME, CREATED_DATE, PROCESS_DATE, REASON, GLOBAL_REFERENCE
FROM DIMENSION.ADJUSTMENT
WHERE ADJUSTMENT_ID = $ADJ_ID;


-- 2) IMPACT SUMMARY — row count + total USD impact ------------------------------
SELECT
    ADJUSTMENT_ID,
    COUNT(*)                       AS ROW_COUNT,
    SUM(MEASURE_VALUE_USD)         AS TOTAL_MEASURE_VALUE_USD
FROM FACT.SENSITIVITY_MEASURES_ADJUSTMENT
WHERE ADJUSTMENT_ID = $ADJ_ID
GROUP BY ADJUSTMENT_ID;


-- 3) IMPACT DETAIL — every position-level row written by the adjustment ---------
SELECT *
FROM FACT.SENSITIVITY_MEASURES_ADJUSTMENT
WHERE ADJUSTMENT_ID = $ADJ_ID
ORDER BY COBID;
</content>
