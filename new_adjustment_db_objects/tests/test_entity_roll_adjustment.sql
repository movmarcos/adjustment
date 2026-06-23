-- =============================================================================
-- TEST HARNESS — Entity Roll v3 (EROL): destructive replace + flatten/copy
-- =============================================================================
-- Purpose
--   Submit ONE Entity Roll, approve it, process it synchronously, then verify
--   (docs/superpowers/specs/2026-06-23-entity-roll-destructive-replace-design.md):
--     (a) adjusted(target, entity) == adjusted(source, entity)
--     (b) FACT_TABLE at the target COB is untouched (zero DML)
--     (c) DIMENSION.ADJUSTMENT row has ADJUSTMENT_TYPE = 'EROL'
--     (e) every prior entity-scoped adjustment at the target COB is now
--         IS_DELETED + RUN_STATUS='Superseded', and its rows are gone from the
--         ADJUSTMENTS_TABLE at the target COB
--
-- Notes
--   * Processing first SUPERSEDES every non-deleted adjustment whose
--     ADJ_HEADER.ENTITY_CODE = the rolled entity at the target COB (flags it
--     deleted, removes its ADJUSTMENTS_TABLE rows). Globals (ENTITY_CODE NULL)
--     are untouched. It then flattens the post-delete combined view and copies
--     the source COB, so combined(target) = adjusted(source).
--   * To exercise (e), seed a prior entity-scoped adjustment (e.g. a Scale on
--     the same entity+COB) BEFORE running this harness; it is captured at
--     baseline and checked after processing. With no priors, (e) is vacuously
--     PASS.
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

-- 2b. Capture prior entity-scoped adjustments at the target COB (for (e)).
--     These are exactly the rows the roll must supersede.
CREATE OR REPLACE TEMPORARY TABLE EROL_TEST_PRIORS AS
SELECT ADJ_ID, DIMENSION_ADJ_ID
FROM ADJ_HEADER
WHERE COBID = $p_cobid
  AND ENTITY_CODE = $p_entity_code
  AND IS_DELETED = FALSE
  AND ADJUSTMENT_TYPE <> 'EROL';     -- exclude any EROL we are about to submit

SELECT COUNT(*) AS PRIOR_COUNT FROM EROL_TEST_PRIORS;


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

-- 5e. (e) every captured prior entity-scoped adjustment is superseded and its
--     rows are gone from the adjustment table at the target COB.
SELECT
    (SELECT COUNT(*) FROM EROL_TEST_PRIORS)                          AS PRIOR_COUNT,
    IFF(NOT EXISTS (
            SELECT 1 FROM EROL_TEST_PRIORS p
            JOIN ADJ_HEADER h ON h.ADJ_ID = p.ADJ_ID
            WHERE h.IS_DELETED = FALSE OR h.RUN_STATUS <> 'Superseded'),
        'PASS', 'FAIL')                                              AS E1_HEADERS_SUPERSEDED,
    IFF(NOT EXISTS (
            SELECT 1 FROM IDENTIFIER($q_adj_table) a
            JOIN EROL_TEST_PRIORS p ON a.ADJUSTMENT_ID = p.DIMENSION_ADJ_ID
            WHERE a.COBID = $p_cobid),
        'PASS', 'FAIL')                                              AS E2_PRIOR_ROWS_REMOVED;


-- 6. CLEANUP  ── optional, removes the test roll ──────────────────────────────
-- NB: the roll is no longer fully reversible — superseded priors were
-- permanently removed, so deleting the roll restores the entity to its
-- original + global state (NOT the pre-roll state with the old adjustments).
-- DELETE FROM IDENTIFIER($q_adj_table) WHERE COBID = $p_cobid AND ADJUSTMENT_ID = $q_dim_id;
-- UPDATE ADJ_HEADER SET IS_DELETED = TRUE, RUN_STATUS = 'Deleted'
-- WHERE COBID = $p_cobid AND ADJUSTMENT_TYPE = 'EROL'
--   AND ENTITY_CODE = $p_entity_code AND DIMENSION_ADJ_ID = $q_dim_id;
