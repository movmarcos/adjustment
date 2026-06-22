-- =============================================================================
-- TEST HARNESS — Entity Roll v2 (EROL): flatten + copy via offset rows
-- =============================================================================
-- Purpose
--   Submit ONE Entity Roll, approve it, process it synchronously, then verify
--   the spec's guarantees (docs/superpowers/specs/2026-06-11-entity-roll-flatten-design.md):
--     (a) adjusted(target, entity) == adjusted(source, entity)
--     (b) FACT_TABLE at the target COB is untouched (zero DML)
--     (c) DIMENSION.ADJUSTMENT row has ADJUSTMENT_TYPE = 'EROL'
--     (d) deleting the adjustment's delta rows restores the pre-roll state
--
-- Notes
--   * EROL writes offset rows into the ADJUSTMENTS_TABLE only: leg ① cancels
--     the target entity's adjusted values, leg ② adds the source COB's, netted
--     per position. Nothing is deleted from FACT or the combined view.
--   * ⚠ SP_RUN_PIPELINE processes ALL eligible adjustments in the scope+COB.
--     Run on a controlled test COB, or when no other pending work exists.
--   * The summary table for the target COB is rebuilt — expected.
-- =============================================================================


-- 0. ENVIRONMENT  ── change the database when running in PROD ─────────────────
USE DATABASE {{DATABASE}};              -- <<< {{DATABASE}} = DATABASE in config.py; replace inline for a PROD run
USE SCHEMA  ADJUSTMENT_APP;


-- 1. PARAMETERS  ── edit these ────────────────────────────────────────────────
SET p_process_type = 'VaR';             -- VaR | Stress | Sensitivity | FRTB | FRTBDRC | FRTBRRAO
SET p_cobid        = 20260328;          -- TARGET COB (YYYYMMDD)
SET p_source_cobid = 20260327;          -- SOURCE COB to roll FROM (must differ)
SET p_entity_code  = 'MUSE';            -- entity to roll
SET p_reference    = 'EROL_TEST_001';   -- unique tag for this test adjustment
SET p_username     = CURRENT_USER();

-- Resolve scope config (table names used by the assertions below)
SET q_fact_table = (SELECT FACT_TABLE             FROM ADJUSTMENTS_SETTINGS WHERE UPPER(PROCESS_TYPE) = UPPER($p_process_type));
SET q_adj_table  = (SELECT ADJUSTMENTS_TABLE      FROM ADJUSTMENTS_SETTINGS WHERE UPPER(PROCESS_TYPE) = UPPER($p_process_type));
SET q_comb_table = (SELECT FACT_ADJUSTED_TABLE    FROM ADJUSTMENTS_SETTINGS WHERE UPPER(PROCESS_TYPE) = UPPER($p_process_type));
SET q_metric_usd = (SELECT METRIC_USD_NAME        FROM ADJUSTMENTS_SETTINGS WHERE UPPER(PROCESS_TYPE) = UPPER($p_process_type));


-- 2. BASELINE  ── capture pre-roll state (run BEFORE submitting) ──────────────
-- 2a. Fact-table row count at the target (must be unchanged afterwards)
CREATE OR REPLACE TEMPORARY TABLE EROL_TEST_BASELINE AS
SELECT
    (SELECT COUNT(*) FROM IDENTIFIER($q_fact_table)
      WHERE COBID = $p_cobid)                                      AS FACT_ROWS_TARGET_COB,
    (SELECT SUM(IDENTIFIER($q_metric_usd)) FROM IDENTIFIER($q_comb_table)
      WHERE COBID = $p_cobid       AND ENTITY_CODE = $p_entity_code) AS ADJUSTED_TARGET_BEFORE,
    (SELECT SUM(IDENTIFIER($q_metric_usd)) FROM IDENTIFIER($q_comb_table)
      WHERE COBID = $p_source_cobid AND ENTITY_CODE = $p_entity_code) AS ADJUSTED_SOURCE;
-- NB: if the combined view keys entity by ENTITY_KEY instead of ENTITY_CODE,
-- swap the predicate to: ENTITY_KEY IN (SELECT ENTITY_KEY FROM DIMENSION.ENTITY
-- WHERE ENTITY_CODE = $p_entity_code)

SELECT * FROM EROL_TEST_BASELINE;


-- 3. SUBMIT  ── EROL adjustment via SP_SUBMIT_ADJUSTMENT ──────────────────────
CALL SP_SUBMIT_ADJUSTMENT(TO_JSON(OBJECT_CONSTRUCT(
    'cobid',                 $p_cobid,
    'process_type',          $p_process_type,
    'adjustment_type',       'EROL',
    'username',              $p_username,
    'source_cobid',          $p_source_cobid,
    'entity_code',           $p_entity_code,
    'reason',                'EROL parity test',
    'global_reference',      $p_reference,
    'requires_approval',     TRUE,
    'adjustment_occurrence', 'ADHOC'
)));

-- 3a. Approve it (EROL always requires approval; test approves directly)
UPDATE ADJ_HEADER
SET RUN_STATUS = 'Approved'
WHERE COBID = $p_cobid
  AND UPPER(PROCESS_TYPE) = UPPER($p_process_type)
  AND ADJUSTMENT_TYPE = 'EROL'
  AND ENTITY_CODE = $p_entity_code
  AND RUN_STATUS = 'Pending Approval'
  AND IS_DELETED = FALSE;

INSERT INTO ADJ_STATUS_HISTORY (ADJ_ID, OLD_STATUS, NEW_STATUS, CHANGED_BY, COMMENT)
SELECT ADJ_ID, 'Pending Approval', 'Approved', $p_username, 'EROL test auto-approval'
FROM ADJ_HEADER
WHERE COBID = $p_cobid AND ADJUSTMENT_TYPE = 'EROL'
  AND ENTITY_CODE = $p_entity_code AND RUN_STATUS = 'Approved'
  AND IS_DELETED = FALSE
QUALIFY ROW_NUMBER() OVER (ORDER BY CREATED_DATE DESC) = 1;


-- 4. PROCESS  ── run the pipeline for the scope ───────────────────────────────
CALL SP_RUN_PIPELINE($p_process_type, CONCAT('["', $p_process_type, '"]'));

-- header should now be Processed with a DIMENSION_ADJ_ID and RECORD_COUNT
SELECT ADJ_ID, DIMENSION_ADJ_ID, RUN_STATUS, RECORD_COUNT, ERRORMESSAGE
FROM ADJ_HEADER
WHERE COBID = $p_cobid AND ADJUSTMENT_TYPE = 'EROL'
  AND ENTITY_CODE = $p_entity_code AND IS_DELETED = FALSE
ORDER BY CREATED_DATE DESC LIMIT 1;

SET q_dim_id = (SELECT DIMENSION_ADJ_ID FROM ADJ_HEADER
                WHERE COBID = $p_cobid AND ADJUSTMENT_TYPE = 'EROL'
                  AND ENTITY_CODE = $p_entity_code AND IS_DELETED = FALSE
                ORDER BY CREATED_DATE DESC LIMIT 1);


-- 5. ASSERTIONS ────────────────────────────────────────────────────────────────
-- (a) net delta written == adjusted(source) − adjusted(target before):
--     original(target) + Σ(deltas) must equal adjusted(source)
-- (b) fact rows at the target COB unchanged
-- (c) DIMENSION.ADJUSTMENT.ADJUSTMENT_TYPE = 'EROL', Processed
SELECT
    b.ADJUSTED_SOURCE,
    b.ADJUSTED_TARGET_BEFORE,
    d.NET_DELTA,
    b.ADJUSTED_TARGET_BEFORE + d.NET_DELTA                          AS ADJUSTED_TARGET_AFTER,
    IFF(ABS(COALESCE(b.ADJUSTED_TARGET_BEFORE,0) + COALESCE(d.NET_DELTA,0)
            - COALESCE(b.ADJUSTED_SOURCE,0)) < 0.01,
        'PASS', 'FAIL')                                             AS A_TARGET_MIRRORS_SOURCE,
    IFF(b.FACT_ROWS_TARGET_COB =
        (SELECT COUNT(*) FROM IDENTIFIER($q_fact_table) WHERE COBID = $p_cobid),
        'PASS', 'FAIL')                                             AS B_FACT_TABLE_UNTOUCHED,
    (SELECT IFF(ADJUSTMENT_TYPE = 'EROL' AND RUN_STATUS = 'Processed', 'PASS', 'FAIL')
     FROM DIMENSION.ADJUSTMENT WHERE ADJUSTMENT_ID = $q_dim_id)     AS C_DIMENSION_TYPE_EROL
FROM EROL_TEST_BASELINE b
CROSS JOIN (
    SELECT COALESCE(SUM(IDENTIFIER($q_metric_usd)), 0) AS NET_DELTA
    FROM IDENTIFIER($q_adj_table)
    WHERE COBID = $p_cobid AND ADJUSTMENT_ID = $q_dim_id
) d;


-- 6. REVERSIBILITY (d) + CLEANUP  ── optional, removes the test adjustment ─────
-- Deleting the roll's delta rows must restore the pre-roll adjusted total.
-- DELETE FROM IDENTIFIER($q_adj_table) WHERE COBID = $p_cobid AND ADJUSTMENT_ID = $q_dim_id;
-- SELECT IFF(ABS(SUM(IDENTIFIER($q_metric_usd))
--            - (SELECT ADJUSTED_TARGET_BEFORE FROM EROL_TEST_BASELINE)) < 0.01,
--        'PASS', 'FAIL') AS D_DELETE_RESTORES
-- FROM IDENTIFIER($q_comb_table)
-- WHERE COBID = $p_cobid AND ENTITY_CODE = $p_entity_code;
-- UPDATE ADJ_HEADER SET IS_DELETED = TRUE, RUN_STATUS = 'Deleted'
-- WHERE COBID = $p_cobid AND ADJUSTMENT_TYPE = 'EROL'
--   AND ENTITY_CODE = $p_entity_code AND DIMENSION_ADJ_ID = $q_dim_id;
