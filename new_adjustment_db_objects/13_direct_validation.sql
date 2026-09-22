-- =============================================================================
-- 13_DIRECT_VALIDATION.SQL — validation for Direct Adjustment staging rows
--
-- ONE parameterised definition (VW_DIRECT_VALIDATE) plus six thin per-scope
-- aliases (VW_DIRECT_VALIDATE_<SCOPE>) so every existing caller keeps working
-- unchanged — the Streamlit page (1_New_Adjustment.py:1504) and
-- SP_SUBMIT_DIRECT_BATCH (14:178) both look up a view by that exact name.
-- The six scopes used to be six near-identical CREATE VIEW statements (see
-- audit finding S4); collapsing them here means every shared rule — the
-- dimension pinning (I10), the VALUE_USD scale (M12) and the magnitude check
-- (M14) — is now written ONCE instead of six times.
--
-- Generic rules v1, unchanged in substance from the six-view version:
--   • required fields present (config-driven via DIRECT_ACCEPTED_COLUMNS,
--     looked up per scope — req_stress / req_sensitivity / req_var /
--     req_frtb / req_frtbdrc / req_frtbrrao)
--   • VALUE_USD numeric, <> 0, and not so large it cannot fit the header
--     column (NUMBER(20,6) — see M14)
--   • every supplied code exists in its dimension (case-insensitive)
--
-- STRUCTURE (why LEFT JOINs, not EXISTS): Snowflake rejects correlated
-- subqueries inside select-list expressions ("Unsupported subquery type
-- cannot be evaluated"), so every check is an uncorrelated LEFT JOIN
-- against a DISTINCT code set — a NULL join hit means "unknown code".
-- The entity/department-qualified BOOK and DEPARTMENT checks (whose
-- predicates depend on other stage columns) are pre-grouped per stage row
-- (ok_book / ok_dept) so they cannot fan out the outer row set.
--
-- DIMENSION PINNING (I10 — standing rule: never scan DIMENSION.TRADE
-- without pinned literals or a pruning join key): ok_trade / ok_instr /
-- ok_book / ok_dept used to scan their whole dimension table with UPPER()
-- wrapped around the dimension column, which defeats pruning and has no
-- correlation to the batch being validated. staged_trade / staged_instr /
-- staged_book / staged_dept pre-collect the DISTINCT codes actually present
-- anywhere in ADJ_DIRECT_STAGE (both as typed and upper-cased), and
-- pinned_book / ok_trade / ok_instr filter their dimension with a BARE
-- (unwrapped, prunable) equality against that small set before the
-- case-insensitive match is evaluated. This is a one-time cost shared by
-- all six scopes below instead of six independent full scans.
--
-- ADJ_DIRECT_STAGE carries no PROCESS_TYPE column (a batch is scoped to one
-- process type by the caller, not by a stored column), so this view cannot
-- select its rule set with a single runtime parameter — each scope's rule
-- set is still its own UNION ALL branch below, tagged with a literal
-- PROCESS_TYPE. What is shared (dimension pinning, VALUE_USD parsing,
-- ENTITY/BOOK/DEPARTMENT/TRADE/INSTRUMENT checks, and the two CTEs used by
-- more than one scope — ok_pca by Stress+Sensitivity, ok_mt by
-- Sensitivity+the FRTB trio) is now defined once, in the WITH clause below,
-- and referenced by every branch that needs it.
--
-- PER-SCOPE DIFFERENCES (unchanged from the six-view version — see the
-- audit report for the full diff): Stress adds SIMULATION_NAME /
-- SIMULATION_SOURCE / SIMULATION_NAME-belongs-to-SOURCE / PCA checks; VaR
-- has only the ENTITY/BOOK/DEPARTMENT/TRADE/INSTRUMENT checks (no PCA, no
-- tenor/curve/measure-type, no simulation); Sensitivity adds TENOR_CODE
-- (keyed by TENOR_CODE + CURRENCY_CODE — see DIMENSION.TENOR_CURRENCY),
-- CURVE_CODE, MEASURE_TYPE_CODE and PCA; the FRTB trio (FRTB, FRTBDRC,
-- FRTBRRAO) are identical to each other — ENTITY/BOOK/DEPARTMENT/TRADE/
-- INSTRUMENT/MEASURE_TYPE_CODE only — and each still gets its own branch
-- because FRTB cross-field rules are expected to diverge per scope once
-- requirements arrive (comment retained at each FRTB branch below).
--
-- FRTB cross-field rules will be added to the three FRTB branches ONLY,
-- when the requirements arrive — that is why each scope still has its own
-- branch rather than one shared rule list.
-- =============================================================================
USE SCHEMA ADJUSTMENT_APP;

CREATE OR REPLACE VIEW ADJUSTMENT_APP.VW_DIRECT_VALIDATE
    COMMENT = 'Row validation for all Direct Adjustment scopes, one PROCESS_TYPE branch each. Not queried directly by the app — see the per-scope VW_DIRECT_VALIDATE_<SCOPE> aliases below, which are.'
AS
WITH
req_stress AS (
    SELECT
        MAX(IFF(IS_REQUIRED AND STAGE_COLUMN = 'ENTITY_CODE', 1, 0)) AS REQ_ENTITY,
        MAX(IFF(IS_REQUIRED AND STAGE_COLUMN = 'VALUE_USD',  1, 0)) AS REQ_VALUE,
        MAX(IFF(IS_REQUIRED AND STAGE_COLUMN = 'BOOK_CODE',  1, 0)) AS REQ_BOOK
    FROM ADJUSTMENT_APP.DIRECT_ACCEPTED_COLUMNS
    WHERE PROCESS_TYPE = 'Stress' AND IS_ACTIVE
),
req_sensitivity AS (
    SELECT
        MAX(IFF(IS_REQUIRED AND STAGE_COLUMN = 'ENTITY_CODE', 1, 0)) AS REQ_ENTITY,
        MAX(IFF(IS_REQUIRED AND STAGE_COLUMN = 'VALUE_USD',  1, 0)) AS REQ_VALUE,
        MAX(IFF(IS_REQUIRED AND STAGE_COLUMN = 'BOOK_CODE',  1, 0)) AS REQ_BOOK
    FROM ADJUSTMENT_APP.DIRECT_ACCEPTED_COLUMNS
    WHERE PROCESS_TYPE = 'Sensitivity' AND IS_ACTIVE
),
req_var AS (
    SELECT
        MAX(IFF(IS_REQUIRED AND STAGE_COLUMN = 'ENTITY_CODE', 1, 0)) AS REQ_ENTITY,
        MAX(IFF(IS_REQUIRED AND STAGE_COLUMN = 'VALUE_USD',  1, 0)) AS REQ_VALUE,
        MAX(IFF(IS_REQUIRED AND STAGE_COLUMN = 'BOOK_CODE',  1, 0)) AS REQ_BOOK
    FROM ADJUSTMENT_APP.DIRECT_ACCEPTED_COLUMNS
    WHERE PROCESS_TYPE = 'VaR' AND IS_ACTIVE
),
req_frtb AS (
    SELECT
        MAX(IFF(IS_REQUIRED AND STAGE_COLUMN = 'ENTITY_CODE', 1, 0)) AS REQ_ENTITY,
        MAX(IFF(IS_REQUIRED AND STAGE_COLUMN = 'VALUE_USD',  1, 0)) AS REQ_VALUE,
        MAX(IFF(IS_REQUIRED AND STAGE_COLUMN = 'BOOK_CODE',  1, 0)) AS REQ_BOOK
    FROM ADJUSTMENT_APP.DIRECT_ACCEPTED_COLUMNS
    WHERE PROCESS_TYPE = 'FRTB' AND IS_ACTIVE
),
req_frtbdrc AS (
    SELECT
        MAX(IFF(IS_REQUIRED AND STAGE_COLUMN = 'ENTITY_CODE', 1, 0)) AS REQ_ENTITY,
        MAX(IFF(IS_REQUIRED AND STAGE_COLUMN = 'VALUE_USD',  1, 0)) AS REQ_VALUE,
        MAX(IFF(IS_REQUIRED AND STAGE_COLUMN = 'BOOK_CODE',  1, 0)) AS REQ_BOOK
    FROM ADJUSTMENT_APP.DIRECT_ACCEPTED_COLUMNS
    WHERE PROCESS_TYPE = 'FRTBDRC' AND IS_ACTIVE
),
req_frtbrrao AS (
    SELECT
        MAX(IFF(IS_REQUIRED AND STAGE_COLUMN = 'ENTITY_CODE', 1, 0)) AS REQ_ENTITY,
        MAX(IFF(IS_REQUIRED AND STAGE_COLUMN = 'VALUE_USD',  1, 0)) AS REQ_VALUE,
        MAX(IFF(IS_REQUIRED AND STAGE_COLUMN = 'BOOK_CODE',  1, 0)) AS REQ_BOOK
    FROM ADJUSTMENT_APP.DIRECT_ACCEPTED_COLUMNS
    WHERE PROCESS_TYPE = 'FRTBRRAO' AND IS_ACTIVE
),
-- ── shared across all six scopes ────────────────────────────────────────
ok_entity AS (SELECT DISTINCT UPPER(ENTITY_CODE) EC FROM DIMENSION.ENTITY),
-- I10: pin DIMENSION.TRADE to the codes actually staged (both spellings, so
-- the bare/prunable dimension-side predicate still catches a differently-cased
-- match), instead of an uncorrelated UPPER()-wrapped scan of the whole table.
staged_trade AS (
    SELECT DISTINCT TRADE_CODE, UPPER(TRADE_CODE) AS U
    FROM ADJUSTMENT_APP.ADJ_DIRECT_STAGE
    WHERE TRADE_CODE IS NOT NULL AND TRADE_CODE <> ''
),
ok_trade AS (
    SELECT DISTINCT UPPER(t.TRADE_CODE) TC
    FROM DIMENSION.TRADE t
    WHERE t.IS_CURRENT_ROW = TRUE
      AND t.TRADE_CODE IN (SELECT TRADE_CODE FROM staged_trade
                           UNION SELECT U FROM staged_trade)
),
-- I10: same pinning for DIMENSION.COMMON_INSTRUMENT.
staged_instr AS (
    SELECT DISTINCT INSTRUMENT_CODE, UPPER(INSTRUMENT_CODE) AS U
    FROM ADJUSTMENT_APP.ADJ_DIRECT_STAGE
    WHERE INSTRUMENT_CODE IS NOT NULL AND INSTRUMENT_CODE <> ''
),
ok_instr AS (
    SELECT DISTINCT UPPER(ci.INSTRUMENT_CODE) IC
    FROM DIMENSION.COMMON_INSTRUMENT ci
    WHERE ci.IS_CURRENT_ROW = TRUE
      AND ci.INSTRUMENT_CODE IN (SELECT INSTRUMENT_CODE FROM staged_instr
                                 UNION SELECT U FROM staged_instr)
),
-- I10: ok_book / ok_dept are entity/department-qualified per stage row, so
-- they were already correlated to ADJ_DIRECT_STAGE — but the join predicate
-- itself (UPPER(b.BOOK_CODE) = UPPER(s2.BOOK_CODE)) still wraps the
-- dimension column and defeats pruning. pinned_book narrows DIMENSION.BOOK
-- to the small set of codes actually staged (both spellings) with a BARE
-- predicate first, then ok_book/ok_dept do the same case-insensitive,
-- entity/department-qualified match as before against that narrow set.
staged_book AS (
    SELECT DISTINCT BOOK_CODE, UPPER(BOOK_CODE) AS U
    FROM ADJUSTMENT_APP.ADJ_DIRECT_STAGE
    WHERE BOOK_CODE IS NOT NULL AND BOOK_CODE <> ''
),
staged_dept AS (
    SELECT DISTINCT DEPARTMENT_CODE, UPPER(DEPARTMENT_CODE) AS U
    FROM ADJUSTMENT_APP.ADJ_DIRECT_STAGE
    WHERE DEPARTMENT_CODE IS NOT NULL AND DEPARTMENT_CODE <> ''
),
pinned_book AS (
    SELECT b.*
    FROM DIMENSION.BOOK b
    WHERE b.IS_CURRENT_ROW = TRUE
      AND (b.BOOK_CODE IN (SELECT BOOK_CODE FROM staged_book
                           UNION SELECT U FROM staged_book)
        OR b.DEPARTMENT_CODE IN (SELECT DEPARTMENT_CODE FROM staged_dept
                                 UNION SELECT U FROM staged_dept))
),
ok_book AS (
    SELECT s2.BATCH_ID, s2.ROW_NUM
    FROM ADJUSTMENT_APP.ADJ_DIRECT_STAGE s2
    JOIN pinned_book b
      ON UPPER(b.BOOK_CODE) = UPPER(s2.BOOK_CODE)
     AND (s2.ENTITY_CODE IS NULL OR UPPER(b.ENTITY_CODE) = UPPER(s2.ENTITY_CODE))
     AND (s2.DEPARTMENT_CODE IS NULL OR UPPER(b.DEPARTMENT_CODE) = UPPER(s2.DEPARTMENT_CODE))
    GROUP BY s2.BATCH_ID, s2.ROW_NUM
),
ok_dept AS (
    SELECT s2.BATCH_ID, s2.ROW_NUM
    FROM ADJUSTMENT_APP.ADJ_DIRECT_STAGE s2
    JOIN pinned_book b
      ON UPPER(b.DEPARTMENT_CODE) = UPPER(s2.DEPARTMENT_CODE)
     AND (s2.ENTITY_CODE IS NULL OR UPPER(b.ENTITY_CODE) = UPPER(s2.ENTITY_CODE))
    GROUP BY s2.BATCH_ID, s2.ROW_NUM
),
-- ── Stress only ──────────────────────────────────────────────────────────
ok_simname AS (SELECT DISTINCT UPPER(STRESS_SIMULATION_NAME) SN
               FROM DIMENSION.STRESS_SIMULATION),
ok_simsrc  AS (SELECT DISTINCT UPPER(SIMULATION_SOURCE) SS
               FROM DIMENSION.STRESS_SIMULATION),
ok_simpair AS (SELECT DISTINCT UPPER(STRESS_SIMULATION_NAME) SN,
                      UPPER(SIMULATION_SOURCE) SS
               FROM DIMENSION.STRESS_SIMULATION),
-- Stress + Sensitivity
ok_pca AS (SELECT DISTINCT UPPER(REPLACE(PCA_CONCAT_KEY, ' ', '')) PK
           FROM DIMENSION.PRODUCT_CATEGORY_ATTRIBUTES),
-- Sensitivity + the FRTB trio
ok_mt AS (SELECT DISTINCT UPPER(MEASURE_TYPE_CODE) MC FROM DIMENSION.MEASURE_TYPE),
-- Sensitivity only
ok_tenor AS (SELECT DISTINCT UPPER(TENOR_CURRENCY_CODE) TCC FROM DIMENSION.TENOR_CURRENCY),
ok_curve AS (SELECT DISTINCT UPPER(CURVE_CODE) CC FROM DIMENSION.CURVE_CURRENCY),
-- M12: scale 10, matching the engine (05:88, 15:285-289) instead of the old
-- scale 6, which silently zeroed (and so rejected as "must not be zero")
-- any value below 5e-7 that the engine would otherwise have kept. Parsed
-- once here and joined by BATCH_ID/ROW_NUM so every scope below shares one
-- definition of "the numeric value of this row".
parsed_value AS (
    SELECT BATCH_ID, ROW_NUM, TRY_TO_NUMBER(VALUE_USD, 38, 10) AS V
    FROM ADJUSTMENT_APP.ADJ_DIRECT_STAGE
)
SELECT PROCESS_TYPE, BATCH_ID, ROW_NUM,
       ARRAY_SIZE(ERRS) = 0 AS IS_VALID,
       ERRS                 AS VALIDATION_ERRORS
FROM (
    -- ══ Stress ══════════════════════════════════════════════════════════
    SELECT
        'Stress' AS PROCESS_TYPE, s.BATCH_ID, s.ROW_NUM,
        ARRAY_COMPACT(ARRAY_CONSTRUCT(
            IFF(s.ENTITY_CODE IS NULL AND req.REQ_ENTITY = 1,
                'ENTITY_CODE is required', NULL),
            IFF(s.VALUE_USD IS NULL AND req.REQ_VALUE = 1,
                'VALUE_USD is required', NULL),
            IFF(s.BOOK_CODE IS NULL AND COALESCE(req.REQ_BOOK, 0) = 1,
                'BOOK_CODE is required', NULL),
            IFF(s.VALUE_USD IS NOT NULL AND pv.V IS NULL,
                'VALUE_USD is not numeric: ' || s.VALUE_USD, NULL),
            IFF(pv.V = 0,
                'VALUE_USD must not be zero', NULL),
            -- M14: NUMBER(20,6) on ADJ_HEADER.ADJUSTMENT_VALUE_IN_USD (01:73)
            -- holds at most 14 digits before the decimal point; anything
            -- bigger fails the SP_SUBMIT_DIRECT_BATCH insert with a raw
            -- Snowflake "Numeric value out of range" instead of a validation
            -- message, and rolls back the whole batch.
            IFF(ABS(pv.V) >= 1e14,
                'VALUE_USD is too large (max 14 digits before the decimal point)', NULL),
            IFF(s.ENTITY_CODE IS NOT NULL AND oe.EC IS NULL,
                'Unknown ENTITY_CODE: ' || s.ENTITY_CODE, NULL),
            IFF(s.BOOK_CODE IS NOT NULL AND ob.ROW_NUM IS NULL,
                'Unknown BOOK_CODE (for entity/department): ' || s.BOOK_CODE, NULL),
            IFF(s.DEPARTMENT_CODE IS NOT NULL AND od.ROW_NUM IS NULL,
                'Unknown DEPARTMENT_CODE (for entity): ' || s.DEPARTMENT_CODE, NULL),
            IFF(s.TRADE_CODE IS NOT NULL AND ot.TC IS NULL,
                'Unknown TRADE_CODE: ' || s.TRADE_CODE, NULL),
            IFF(s.INSTRUMENT_CODE IS NOT NULL AND oi.IC IS NULL,
                'Unknown INSTRUMENT_CODE: ' || s.INSTRUMENT_CODE, NULL),
            IFF(s.SIMULATION_NAME IS NOT NULL AND osn.SN IS NULL,
                'Unknown SIMULATION_NAME: ' || s.SIMULATION_NAME, NULL),
            IFF(s.SIMULATION_SOURCE IS NOT NULL AND oss.SS IS NULL,
                'Unknown SIMULATION_SOURCE: ' || s.SIMULATION_SOURCE, NULL),
            IFF(s.SIMULATION_NAME IS NOT NULL AND s.SIMULATION_SOURCE IS NOT NULL
                AND osn.SN IS NOT NULL AND oss.SS IS NOT NULL AND osp.SN IS NULL,
                'SIMULATION_NAME ''' || s.SIMULATION_NAME ||
                ''' does not belong to SIMULATION_SOURCE ''' || s.SIMULATION_SOURCE || '''', NULL),
            IFF(s.PRODUCT_CATEGORY_ATTRIBUTES IS NOT NULL AND op.PK IS NULL,
                'Unknown PRODUCT_CATEGORY_ATTRIBUTES: ' || s.PRODUCT_CATEGORY_ATTRIBUTES, NULL)
        )) AS ERRS
    FROM ADJUSTMENT_APP.ADJ_DIRECT_STAGE s
    CROSS JOIN req_stress req
    LEFT JOIN parsed_value pv ON pv.BATCH_ID = s.BATCH_ID AND pv.ROW_NUM = s.ROW_NUM
    LEFT JOIN ok_entity  oe  ON oe.EC = UPPER(s.ENTITY_CODE)
    LEFT JOIN ok_book    ob  ON ob.BATCH_ID = s.BATCH_ID AND ob.ROW_NUM = s.ROW_NUM
    LEFT JOIN ok_dept    od  ON od.BATCH_ID = s.BATCH_ID AND od.ROW_NUM = s.ROW_NUM
    LEFT JOIN ok_trade   ot  ON ot.TC = UPPER(s.TRADE_CODE)
    LEFT JOIN ok_instr   oi  ON oi.IC = UPPER(s.INSTRUMENT_CODE)
    LEFT JOIN ok_simname osn ON osn.SN = UPPER(s.SIMULATION_NAME)
    LEFT JOIN ok_simsrc  oss ON oss.SS = UPPER(s.SIMULATION_SOURCE)
    LEFT JOIN ok_simpair osp ON osp.SN = UPPER(s.SIMULATION_NAME)
                            AND osp.SS = UPPER(s.SIMULATION_SOURCE)
    LEFT JOIN ok_pca     op  ON op.PK = UPPER(REPLACE(s.PRODUCT_CATEGORY_ATTRIBUTES, ' ', ''))

    UNION ALL

    -- ══ Sensitivity ═════════════════════════════════════════════════════
    SELECT
        'Sensitivity' AS PROCESS_TYPE, s.BATCH_ID, s.ROW_NUM,
        ARRAY_COMPACT(ARRAY_CONSTRUCT(
            IFF(s.ENTITY_CODE IS NULL AND req.REQ_ENTITY = 1,
                'ENTITY_CODE is required', NULL),
            IFF(s.VALUE_USD IS NULL AND req.REQ_VALUE = 1,
                'VALUE_USD is required', NULL),
            IFF(s.BOOK_CODE IS NULL AND COALESCE(req.REQ_BOOK, 0) = 1,
                'BOOK_CODE is required', NULL),
            IFF(s.VALUE_USD IS NOT NULL AND pv.V IS NULL,
                'VALUE_USD is not numeric: ' || s.VALUE_USD, NULL),
            IFF(pv.V = 0,
                'VALUE_USD must not be zero', NULL),
            IFF(ABS(pv.V) >= 1e14,
                'VALUE_USD is too large (max 14 digits before the decimal point)', NULL),
            IFF(s.ENTITY_CODE IS NOT NULL AND oe.EC IS NULL,
                'Unknown ENTITY_CODE: ' || s.ENTITY_CODE, NULL),
            IFF(s.BOOK_CODE IS NOT NULL AND ob.ROW_NUM IS NULL,
                'Unknown BOOK_CODE (for entity/department): ' || s.BOOK_CODE, NULL),
            IFF(s.DEPARTMENT_CODE IS NOT NULL AND od.ROW_NUM IS NULL,
                'Unknown DEPARTMENT_CODE (for entity): ' || s.DEPARTMENT_CODE, NULL),
            IFF(s.TRADE_CODE IS NOT NULL AND ot.TC IS NULL,
                'Unknown TRADE_CODE: ' || s.TRADE_CODE, NULL),
            IFF(s.INSTRUMENT_CODE IS NOT NULL AND oi.IC IS NULL,
                'Unknown INSTRUMENT_CODE: ' || s.INSTRUMENT_CODE, NULL),
            IFF(s.TENOR_CODE IS NOT NULL AND otc.TCC IS NULL,
                'Unknown TENOR_CODE (for currency): ' || s.TENOR_CODE, NULL),
            IFF(s.CURVE_CODE IS NOT NULL AND oc.CC IS NULL,
                'Unknown CURVE_CODE: ' || s.CURVE_CODE, NULL),
            IFF(s.MEASURE_TYPE_CODE IS NOT NULL AND om.MC IS NULL,
                'Unknown MEASURE_TYPE_CODE: ' || s.MEASURE_TYPE_CODE, NULL),
            IFF(s.PRODUCT_CATEGORY_ATTRIBUTES IS NOT NULL AND op.PK IS NULL,
                'Unknown PRODUCT_CATEGORY_ATTRIBUTES: ' || s.PRODUCT_CATEGORY_ATTRIBUTES, NULL)
        )) AS ERRS
    FROM ADJUSTMENT_APP.ADJ_DIRECT_STAGE s
    CROSS JOIN req_sensitivity req
    LEFT JOIN parsed_value pv ON pv.BATCH_ID = s.BATCH_ID AND pv.ROW_NUM = s.ROW_NUM
    LEFT JOIN ok_entity oe  ON oe.EC = UPPER(s.ENTITY_CODE)
    LEFT JOIN ok_book   ob  ON ob.BATCH_ID = s.BATCH_ID AND ob.ROW_NUM = s.ROW_NUM
    LEFT JOIN ok_dept   od  ON od.BATCH_ID = s.BATCH_ID AND od.ROW_NUM = s.ROW_NUM
    LEFT JOIN ok_trade  ot  ON ot.TC = UPPER(s.TRADE_CODE)
    LEFT JOIN ok_instr  oi  ON oi.IC = UPPER(s.INSTRUMENT_CODE)
    LEFT JOIN ok_tenor  otc ON otc.TCC = UPPER(CONCAT(s.TENOR_CODE, '_',
                                    COALESCE(s.CURRENCY_CODE, 'USD')))
    LEFT JOIN ok_curve  oc  ON oc.CC = UPPER(s.CURVE_CODE)
    LEFT JOIN ok_mt     om  ON om.MC = UPPER(s.MEASURE_TYPE_CODE)
    LEFT JOIN ok_pca    op  ON op.PK = UPPER(REPLACE(s.PRODUCT_CATEGORY_ATTRIBUTES, ' ', ''))

    UNION ALL

    -- ══ VaR ═════════════════════════════════════════════════════════════
    SELECT
        'VaR' AS PROCESS_TYPE, s.BATCH_ID, s.ROW_NUM,
        ARRAY_COMPACT(ARRAY_CONSTRUCT(
            IFF(s.ENTITY_CODE IS NULL AND req.REQ_ENTITY = 1,
                'ENTITY_CODE is required', NULL),
            IFF(s.VALUE_USD IS NULL AND req.REQ_VALUE = 1,
                'VALUE_USD is required', NULL),
            IFF(s.BOOK_CODE IS NULL AND COALESCE(req.REQ_BOOK, 0) = 1,
                'BOOK_CODE is required', NULL),
            IFF(s.VALUE_USD IS NOT NULL AND pv.V IS NULL,
                'VALUE_USD is not numeric: ' || s.VALUE_USD, NULL),
            IFF(pv.V = 0,
                'VALUE_USD must not be zero', NULL),
            IFF(ABS(pv.V) >= 1e14,
                'VALUE_USD is too large (max 14 digits before the decimal point)', NULL),
            IFF(s.ENTITY_CODE IS NOT NULL AND oe.EC IS NULL,
                'Unknown ENTITY_CODE: ' || s.ENTITY_CODE, NULL),
            IFF(s.BOOK_CODE IS NOT NULL AND ob.ROW_NUM IS NULL,
                'Unknown BOOK_CODE (for entity/department): ' || s.BOOK_CODE, NULL),
            IFF(s.DEPARTMENT_CODE IS NOT NULL AND od.ROW_NUM IS NULL,
                'Unknown DEPARTMENT_CODE (for entity): ' || s.DEPARTMENT_CODE, NULL),
            IFF(s.TRADE_CODE IS NOT NULL AND ot.TC IS NULL,
                'Unknown TRADE_CODE: ' || s.TRADE_CODE, NULL),
            IFF(s.INSTRUMENT_CODE IS NOT NULL AND oi.IC IS NULL,
                'Unknown INSTRUMENT_CODE: ' || s.INSTRUMENT_CODE, NULL)
        )) AS ERRS
    FROM ADJUSTMENT_APP.ADJ_DIRECT_STAGE s
    CROSS JOIN req_var req
    LEFT JOIN parsed_value pv ON pv.BATCH_ID = s.BATCH_ID AND pv.ROW_NUM = s.ROW_NUM
    LEFT JOIN ok_entity oe ON oe.EC = UPPER(s.ENTITY_CODE)
    LEFT JOIN ok_book   ob ON ob.BATCH_ID = s.BATCH_ID AND ob.ROW_NUM = s.ROW_NUM
    LEFT JOIN ok_dept   od ON od.BATCH_ID = s.BATCH_ID AND od.ROW_NUM = s.ROW_NUM
    LEFT JOIN ok_trade  ot ON ot.TC = UPPER(s.TRADE_CODE)
    LEFT JOIN ok_instr  oi ON oi.IC = UPPER(s.INSTRUMENT_CODE)

    UNION ALL

    -- ══ FRTB ════════════════════════════════════════════════════════════
    -- FRTB cross-field validation rules land HERE (requirements pending).
    SELECT
        'FRTB' AS PROCESS_TYPE, s.BATCH_ID, s.ROW_NUM,
        ARRAY_COMPACT(ARRAY_CONSTRUCT(
            IFF(s.ENTITY_CODE IS NULL AND req.REQ_ENTITY = 1,
                'ENTITY_CODE is required', NULL),
            IFF(s.VALUE_USD IS NULL AND req.REQ_VALUE = 1,
                'VALUE_USD is required', NULL),
            IFF(s.BOOK_CODE IS NULL AND COALESCE(req.REQ_BOOK, 0) = 1,
                'BOOK_CODE is required', NULL),
            IFF(s.VALUE_USD IS NOT NULL AND pv.V IS NULL,
                'VALUE_USD is not numeric: ' || s.VALUE_USD, NULL),
            IFF(pv.V = 0,
                'VALUE_USD must not be zero', NULL),
            IFF(ABS(pv.V) >= 1e14,
                'VALUE_USD is too large (max 14 digits before the decimal point)', NULL),
            IFF(s.ENTITY_CODE IS NOT NULL AND oe.EC IS NULL,
                'Unknown ENTITY_CODE: ' || s.ENTITY_CODE, NULL),
            IFF(s.BOOK_CODE IS NOT NULL AND ob.ROW_NUM IS NULL,
                'Unknown BOOK_CODE (for entity/department): ' || s.BOOK_CODE, NULL),
            IFF(s.DEPARTMENT_CODE IS NOT NULL AND od.ROW_NUM IS NULL,
                'Unknown DEPARTMENT_CODE (for entity): ' || s.DEPARTMENT_CODE, NULL),
            IFF(s.TRADE_CODE IS NOT NULL AND ot.TC IS NULL,
                'Unknown TRADE_CODE: ' || s.TRADE_CODE, NULL),
            IFF(s.INSTRUMENT_CODE IS NOT NULL AND oi.IC IS NULL,
                'Unknown INSTRUMENT_CODE: ' || s.INSTRUMENT_CODE, NULL),
            IFF(s.MEASURE_TYPE_CODE IS NOT NULL AND om.MC IS NULL,
                'Unknown MEASURE_TYPE_CODE: ' || s.MEASURE_TYPE_CODE, NULL)
        )) AS ERRS
    FROM ADJUSTMENT_APP.ADJ_DIRECT_STAGE s
    CROSS JOIN req_frtb req
    LEFT JOIN parsed_value pv ON pv.BATCH_ID = s.BATCH_ID AND pv.ROW_NUM = s.ROW_NUM
    LEFT JOIN ok_entity oe ON oe.EC = UPPER(s.ENTITY_CODE)
    LEFT JOIN ok_book   ob ON ob.BATCH_ID = s.BATCH_ID AND ob.ROW_NUM = s.ROW_NUM
    LEFT JOIN ok_dept   od ON od.BATCH_ID = s.BATCH_ID AND od.ROW_NUM = s.ROW_NUM
    LEFT JOIN ok_trade  ot ON ot.TC = UPPER(s.TRADE_CODE)
    LEFT JOIN ok_instr  oi ON oi.IC = UPPER(s.INSTRUMENT_CODE)
    LEFT JOIN ok_mt     om ON om.MC = UPPER(s.MEASURE_TYPE_CODE)

    UNION ALL

    -- ══ FRTBDRC ═════════════════════════════════════════════════════════
    -- FRTB cross-field validation rules land HERE (requirements pending).
    SELECT
        'FRTBDRC' AS PROCESS_TYPE, s.BATCH_ID, s.ROW_NUM,
        ARRAY_COMPACT(ARRAY_CONSTRUCT(
            IFF(s.ENTITY_CODE IS NULL AND req.REQ_ENTITY = 1,
                'ENTITY_CODE is required', NULL),
            IFF(s.VALUE_USD IS NULL AND req.REQ_VALUE = 1,
                'VALUE_USD is required', NULL),
            IFF(s.BOOK_CODE IS NULL AND COALESCE(req.REQ_BOOK, 0) = 1,
                'BOOK_CODE is required', NULL),
            IFF(s.VALUE_USD IS NOT NULL AND pv.V IS NULL,
                'VALUE_USD is not numeric: ' || s.VALUE_USD, NULL),
            IFF(pv.V = 0,
                'VALUE_USD must not be zero', NULL),
            IFF(ABS(pv.V) >= 1e14,
                'VALUE_USD is too large (max 14 digits before the decimal point)', NULL),
            IFF(s.ENTITY_CODE IS NOT NULL AND oe.EC IS NULL,
                'Unknown ENTITY_CODE: ' || s.ENTITY_CODE, NULL),
            IFF(s.BOOK_CODE IS NOT NULL AND ob.ROW_NUM IS NULL,
                'Unknown BOOK_CODE (for entity/department): ' || s.BOOK_CODE, NULL),
            IFF(s.DEPARTMENT_CODE IS NOT NULL AND od.ROW_NUM IS NULL,
                'Unknown DEPARTMENT_CODE (for entity): ' || s.DEPARTMENT_CODE, NULL),
            IFF(s.TRADE_CODE IS NOT NULL AND ot.TC IS NULL,
                'Unknown TRADE_CODE: ' || s.TRADE_CODE, NULL),
            IFF(s.INSTRUMENT_CODE IS NOT NULL AND oi.IC IS NULL,
                'Unknown INSTRUMENT_CODE: ' || s.INSTRUMENT_CODE, NULL),
            IFF(s.MEASURE_TYPE_CODE IS NOT NULL AND om.MC IS NULL,
                'Unknown MEASURE_TYPE_CODE: ' || s.MEASURE_TYPE_CODE, NULL)
        )) AS ERRS
    FROM ADJUSTMENT_APP.ADJ_DIRECT_STAGE s
    CROSS JOIN req_frtbdrc req
    LEFT JOIN parsed_value pv ON pv.BATCH_ID = s.BATCH_ID AND pv.ROW_NUM = s.ROW_NUM
    LEFT JOIN ok_entity oe ON oe.EC = UPPER(s.ENTITY_CODE)
    LEFT JOIN ok_book   ob ON ob.BATCH_ID = s.BATCH_ID AND ob.ROW_NUM = s.ROW_NUM
    LEFT JOIN ok_dept   od ON od.BATCH_ID = s.BATCH_ID AND od.ROW_NUM = s.ROW_NUM
    LEFT JOIN ok_trade  ot ON ot.TC = UPPER(s.TRADE_CODE)
    LEFT JOIN ok_instr  oi ON oi.IC = UPPER(s.INSTRUMENT_CODE)
    LEFT JOIN ok_mt     om ON om.MC = UPPER(s.MEASURE_TYPE_CODE)

    UNION ALL

    -- ══ FRTBRRAO ════════════════════════════════════════════════════════
    -- FRTB cross-field validation rules land HERE (requirements pending).
    SELECT
        'FRTBRRAO' AS PROCESS_TYPE, s.BATCH_ID, s.ROW_NUM,
        ARRAY_COMPACT(ARRAY_CONSTRUCT(
            IFF(s.ENTITY_CODE IS NULL AND req.REQ_ENTITY = 1,
                'ENTITY_CODE is required', NULL),
            IFF(s.VALUE_USD IS NULL AND req.REQ_VALUE = 1,
                'VALUE_USD is required', NULL),
            IFF(s.BOOK_CODE IS NULL AND COALESCE(req.REQ_BOOK, 0) = 1,
                'BOOK_CODE is required', NULL),
            IFF(s.VALUE_USD IS NOT NULL AND pv.V IS NULL,
                'VALUE_USD is not numeric: ' || s.VALUE_USD, NULL),
            IFF(pv.V = 0,
                'VALUE_USD must not be zero', NULL),
            IFF(ABS(pv.V) >= 1e14,
                'VALUE_USD is too large (max 14 digits before the decimal point)', NULL),
            IFF(s.ENTITY_CODE IS NOT NULL AND oe.EC IS NULL,
                'Unknown ENTITY_CODE: ' || s.ENTITY_CODE, NULL),
            IFF(s.BOOK_CODE IS NOT NULL AND ob.ROW_NUM IS NULL,
                'Unknown BOOK_CODE (for entity/department): ' || s.BOOK_CODE, NULL),
            IFF(s.DEPARTMENT_CODE IS NOT NULL AND od.ROW_NUM IS NULL,
                'Unknown DEPARTMENT_CODE (for entity): ' || s.DEPARTMENT_CODE, NULL),
            IFF(s.TRADE_CODE IS NOT NULL AND ot.TC IS NULL,
                'Unknown TRADE_CODE: ' || s.TRADE_CODE, NULL),
            IFF(s.INSTRUMENT_CODE IS NOT NULL AND oi.IC IS NULL,
                'Unknown INSTRUMENT_CODE: ' || s.INSTRUMENT_CODE, NULL),
            IFF(s.MEASURE_TYPE_CODE IS NOT NULL AND om.MC IS NULL,
                'Unknown MEASURE_TYPE_CODE: ' || s.MEASURE_TYPE_CODE, NULL)
        )) AS ERRS
    FROM ADJUSTMENT_APP.ADJ_DIRECT_STAGE s
    CROSS JOIN req_frtbrrao req
    LEFT JOIN parsed_value pv ON pv.BATCH_ID = s.BATCH_ID AND pv.ROW_NUM = s.ROW_NUM
    LEFT JOIN ok_entity oe ON oe.EC = UPPER(s.ENTITY_CODE)
    LEFT JOIN ok_book   ob ON ob.BATCH_ID = s.BATCH_ID AND ob.ROW_NUM = s.ROW_NUM
    LEFT JOIN ok_dept   od ON od.BATCH_ID = s.BATCH_ID AND od.ROW_NUM = s.ROW_NUM
    LEFT JOIN ok_trade  ot ON ot.TC = UPPER(s.TRADE_CODE)
    LEFT JOIN ok_instr  oi ON oi.IC = UPPER(s.INSTRUMENT_CODE)
    LEFT JOIN ok_mt     om ON om.MC = UPPER(s.MEASURE_TYPE_CODE)
);

-- ═══════════════════════════════════════════════════════════════════════════
-- Thin per-scope aliases — these are the names every caller actually uses
-- (streamlit_app/pages/1_New_Adjustment.py:1504, 14_sp_submit_direct_batch.sql:178
-- via SELECT ... FROM ADJUSTMENT_APP.VW_DIRECT_VALIDATE_{process_type.upper()}).
-- Each is just a PROCESS_TYPE filter over VW_DIRECT_VALIDATE, so the rule
-- set for a given scope lives in exactly one place (above).
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW ADJUSTMENT_APP.VW_DIRECT_VALIDATE_STRESS
    COMMENT = 'Row validation for Stress Direct Adjustment staging rows. Alias over VW_DIRECT_VALIDATE WHERE PROCESS_TYPE = ''Stress''.'
AS
SELECT BATCH_ID, ROW_NUM, IS_VALID, VALIDATION_ERRORS
FROM ADJUSTMENT_APP.VW_DIRECT_VALIDATE
WHERE PROCESS_TYPE = 'Stress';

CREATE OR REPLACE VIEW ADJUSTMENT_APP.VW_DIRECT_VALIDATE_SENSITIVITY
    COMMENT = 'Row validation for Sensitivity Direct Adjustment staging rows. Alias over VW_DIRECT_VALIDATE WHERE PROCESS_TYPE = ''Sensitivity''.'
AS
SELECT BATCH_ID, ROW_NUM, IS_VALID, VALIDATION_ERRORS
FROM ADJUSTMENT_APP.VW_DIRECT_VALIDATE
WHERE PROCESS_TYPE = 'Sensitivity';

CREATE OR REPLACE VIEW ADJUSTMENT_APP.VW_DIRECT_VALIDATE_VAR
    COMMENT = 'Row validation for VaR Direct Adjustment staging rows. Alias over VW_DIRECT_VALIDATE WHERE PROCESS_TYPE = ''VaR''.'
AS
SELECT BATCH_ID, ROW_NUM, IS_VALID, VALIDATION_ERRORS
FROM ADJUSTMENT_APP.VW_DIRECT_VALIDATE
WHERE PROCESS_TYPE = 'VaR';

CREATE OR REPLACE VIEW ADJUSTMENT_APP.VW_DIRECT_VALIDATE_FRTB
    COMMENT = 'Row validation for FRTB Direct Adjustment staging rows. Alias over VW_DIRECT_VALIDATE WHERE PROCESS_TYPE = ''FRTB''.'
AS
SELECT BATCH_ID, ROW_NUM, IS_VALID, VALIDATION_ERRORS
FROM ADJUSTMENT_APP.VW_DIRECT_VALIDATE
WHERE PROCESS_TYPE = 'FRTB';

CREATE OR REPLACE VIEW ADJUSTMENT_APP.VW_DIRECT_VALIDATE_FRTBDRC
    COMMENT = 'Row validation for FRTBDRC Direct Adjustment staging rows. Alias over VW_DIRECT_VALIDATE WHERE PROCESS_TYPE = ''FRTBDRC''.'
AS
SELECT BATCH_ID, ROW_NUM, IS_VALID, VALIDATION_ERRORS
FROM ADJUSTMENT_APP.VW_DIRECT_VALIDATE
WHERE PROCESS_TYPE = 'FRTBDRC';

CREATE OR REPLACE VIEW ADJUSTMENT_APP.VW_DIRECT_VALIDATE_FRTBRRAO
    COMMENT = 'Row validation for FRTBRRAO Direct Adjustment staging rows. Alias over VW_DIRECT_VALIDATE WHERE PROCESS_TYPE = ''FRTBRRAO''.'
AS
SELECT BATCH_ID, ROW_NUM, IS_VALID, VALIDATION_ERRORS
FROM ADJUSTMENT_APP.VW_DIRECT_VALIDATE
WHERE PROCESS_TYPE = 'FRTBRRAO';
