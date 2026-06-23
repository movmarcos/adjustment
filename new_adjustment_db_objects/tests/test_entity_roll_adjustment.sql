-- =============================================================================
-- TEST HARNESS — Entity Roll v3 (EROL): destructive replace + flatten/copy
-- =============================================================================
-- Purpose
--   Submit ONE Entity Roll, approve it, process it synchronously, then verify
--   (docs/superpowers/specs/2026-06-23-entity-roll-destructive-replace-design.md):
--     (a) adjusted(target, entity) == adjusted(source, entity)
--     (b) FACT_TABLE at the target COB is untouched (zero DML)
--     (c) DIMENSION.ADJUSTMENT row has ADJUSTMENT_TYPE = 'EROL'
--     (e) every prior adjustment for the entity at the target COB is wiped —
--         no prior rows remain in the ADJUSTMENTS_TABLE, and the matching
--         DIMENSION.ADJUSTMENT + ADJ_HEADER rows are IS_DELETED/'Superseded'
--
-- Notes
--   * Processing WIPES every adjustment for the entity at the target COB:
--     it flags all ADJ_HEADER + DIMENSION.ADJUSTMENT rows for the COB+entity
--     deleted (adjustments can be loaded by external systems that bypass
--     ADJ_HEADER) and DELETEs every entity row at the COB from the
--     ADJUSTMENTS_TABLE, then rebuilds the entity from the source COB so
--     combined(target) = adjusted(source).
--   * To exercise (e), seed one or more prior adjustments for the same
--     entity+COB BEFORE running this harness; they are captured at baseline
--     (across DIMENSION + fact) and checked after processing. With no priors,
--     (e) is vacuously PASS.
--   * ⚠ SP_RUN_PIPELINE processes ALL eligible adjustments in the scope+COB.
--     Run on a controlled test COB, or when no other pending work exists.
--   * The summary table for the rolled entity is rebuilt — expected.
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

-- 2b. Capture prior adjustments for the entity at the target COB (for (e)).
--     The fact table is authoritative — it includes adjustments loaded by
--     external systems that never created an ADJ_HEADER row.
CREATE OR REPLACE TEMPORARY TABLE EROL_TEST_PRIORS AS
SELECT DISTINCT ADJUSTMENT_ID
FROM IDENTIFIER($q_adj_table)
WHERE COBID = $p_cobid
  AND ENTITY_KEY IN (SELECT ENTITY_KEY FROM DIMENSION.ENTITY WHERE ENTITY_CODE = $p_entity_code);
-- NB: swap ENTITY_KEY → ENTITY_CODE above if this scope's adjustment table is
-- keyed by ENTITY_CODE instead.

-- Reconciliation: the three sources should agree on the entity's adjustments.
SELECT
  (SELECT COUNT(*) FROM ADJ_HEADER
    WHERE COBID = $p_cobid AND ENTITY_CODE = $p_entity_code AND IS_DELETED = FALSE) AS HEADER_CNT,
  (SELECT COUNT(*) FROM DIMENSION.ADJUSTMENT
    WHERE COBID = $p_cobid AND ENTITY_CODE = $p_entity_code AND IS_DELETED = FALSE) AS DIM_CNT,
  (SELECT COUNT(*) FROM EROL_TEST_PRIORS)                                          AS FACT_CNT;


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

-- 5e. (e) every prior adjustment for the entity at the COB is wiped: no prior
--     fact rows remain, and DIMENSION + ADJ_HEADER are superseded.
SELECT
    (SELECT COUNT(*) FROM EROL_TEST_PRIORS)                          AS PRIOR_FACT_IDS,
    IFF(NOT EXISTS (
            SELECT 1 FROM IDENTIFIER($q_adj_table) a
            JOIN EROL_TEST_PRIORS p ON a.ADJUSTMENT_ID = p.ADJUSTMENT_ID
            WHERE a.COBID = $p_cobid AND a.ADJUSTMENT_ID <> $q_dim_id),
        'PASS', 'FAIL')                                              AS E1_PRIOR_ROWS_REMOVED,
    IFF(NOT EXISTS (
            SELECT 1 FROM DIMENSION.ADJUSTMENT
            WHERE COBID = $p_cobid AND ENTITY_CODE = $p_entity_code
              AND ADJUSTMENT_ID <> $q_dim_id AND IS_DELETED = FALSE),
        'PASS', 'FAIL')                                              AS E2_DIMENSION_SUPERSEDED,
    IFF(NOT EXISTS (
            SELECT 1 FROM ADJ_HEADER
            WHERE COBID = $p_cobid AND ENTITY_CODE = $p_entity_code
              AND ADJUSTMENT_TYPE <> 'EROL' AND IS_DELETED = FALSE),
        'PASS', 'FAIL')                                              AS E3_HEADER_SUPERSEDED;


-- 6. CLEANUP  ── optional, removes the test roll ──────────────────────────────
-- NB: the roll is no longer fully reversible — superseded priors were
-- permanently removed, so deleting the roll restores the entity to its
-- original + global state (NOT the pre-roll state with the old adjustments).
-- DELETE FROM IDENTIFIER($q_adj_table) WHERE COBID = $p_cobid AND ADJUSTMENT_ID = $q_dim_id;
-- UPDATE ADJ_HEADER SET IS_DELETED = TRUE, RUN_STATUS = 'Deleted'
-- WHERE COBID = $p_cobid AND ADJUSTMENT_TYPE = 'EROL'
--   AND ENTITY_CODE = $p_entity_code AND DIMENSION_ADJ_ID = $q_dim_id;
