-- =============================================================================
-- 15_DIRECT_FRTB_UPLOAD.SQL — FRTB / FRTBDRC / FRTBRRAO Direct (file) uploads
--
-- Onboards the FRTB team's "direct adjustment" initiative into the main app.
-- Design decision (vs the prototype's one-table-per-type + own app):
--   • NO new landing tables — rows ride the existing Upload pipeline:
--     one file = one adjustment; rows stored in ADJ_LINE_ITEM_JSON; header +
--     DIMENSION.ADJUSTMENT, approval, audit, retry cleanup and the RAVEN
--     hand-off all come from the existing engine for free.
--   • The team's REAL asset — the enrichment views mapping raw columns to the
--     FRTBSA_*_ADJUSTMENT fact shape — is ported below as views over the
--     JSON line items, fixed to be batch-scoped and deterministic:
--       - keyed by ADJ_ID (prototype validated/loaded the WHOLE table),
--       - FRTBSA_*_KEY = MD5(adj_id, row) not filename+wallclock,
--       - RUN_LOG_ID/ADJUSTMENT_ID from ADJ_HEADER (set at processing time),
--         not SEQ.nextval per row.
--   • Field lists / mandatory matrix / conditional rules come from the team's
--     confirmed spec (Excel + prototype TYPE_CONFIG), stored as config in
--     DIRECT_SCOPE_SCHEMA so the app validates BEFORE anything is written.
--   • SBM is onboarded as scope 'FRTB' (SBM == FRTB in our configs).
-- Processing: 05_sp_process_adjustment upload path dispatches to the
-- WRITER_OVERRIDE functions registered below (write_direct_frtb_*).
-- =============================================================================

USE SCHEMA ADJUSTMENT_APP;

-- Conditional-rule + header-alias config for upload scopes.
ALTER TABLE ADJUSTMENT_APP.DIRECT_SCOPE_SCHEMA
    ADD COLUMN IF NOT EXISTS VALIDATION_RULES VARIANT;
ALTER TABLE ADJUSTMENT_APP.DIRECT_SCOPE_SCHEMA
    ADD COLUMN IF NOT EXISTS ALIASES VARIANT;

-- ═══════════════════════════════════════════════════════════════════════════
-- 1. DIRECT_SCOPE_SCHEMA — FRTB (SBM)
-- ═══════════════════════════════════════════════════════════════════════════
BEGIN TRANSACTION;
DELETE FROM ADJUSTMENT_APP.DIRECT_SCOPE_SCHEMA WHERE PROCESS_TYPE = 'FRTB';
INSERT INTO ADJUSTMENT_APP.DIRECT_SCOPE_SCHEMA
    (PROCESS_TYPE, EXPECTED_COLUMNS, UNPIVOT, FACT_MAPPING, RESOLUTIONS,
     METRIC_FIELD, METRIC_USD_FIELD, WRITER_OVERRIDE, IS_ACTIVE,
     VALIDATION_RULES, ALIASES)
SELECT
    'FRTB',
    PARSE_JSON('[
        {"name":"COBID","type":"number","required":true},
        {"name":"ENTITY_CODE","type":"string","required":true},
        {"name":"MUFG_PRODUCT_CODE","type":"string","required":true},
        {"name":"BUSINESS_PRODUCT_CODE4","type":"string","required":true},
        {"name":"TRADING_DESK","type":"string","required":true},
        {"name":"SENSITIVITY_TYPE","type":"string","required":true},
        {"name":"RISK_CLASS","type":"string","required":true},
        {"name":"AMOUNT","type":"number","required":true},
        {"name":"AMOUNT_IN_USD","type":"number","required":true},
        {"name":"TRADE_CODE","type":"string"},{"name":"BOND_CDS","type":"string"},
        {"name":"BT_TYPE","type":"string"},{"name":"BUCKET","type":"string"},
        {"name":"BUSINESS_PRODUCT_CODE1","type":"string"},
        {"name":"BUSINESS_PRODUCT_CODE2","type":"string"},
        {"name":"BUSINESS_PRODUCT_CODE3","type":"string"},
        {"name":"CCY1","type":"string"},{"name":"CCY1_OFFSHORE_QUALIFIER","type":"string"},
        {"name":"CCY2","type":"string"},{"name":"CCY2_OFFSHORE_QUALIFIER","type":"string"},
        {"name":"CCY_AMT","type":"string"},{"name":"CCY_AMT_OFFSHORE_QUALIFIER","type":"string"},
        {"name":"CDS_CURRENCY","type":"string"},{"name":"CLIENT_CODE","type":"string"},
        {"name":"COUNTRY_CODE","type":"string"},{"name":"CURVATURE_SCENARIO","type":"string"},
        {"name":"CURVE_TYPE","type":"string"},{"name":"DELTA_SUBTRACT","type":"number"},
        {"name":"EBA_BUCKET","type":"string"},{"name":"EBA_BUCKET_NAME","type":"string"},
        {"name":"FED_BUCKET","type":"string"},{"name":"FED_BUCKET_NAME","type":"string"},
        {"name":"GUARANTEED_TYPE","type":"string"},{"name":"ISSUER_CODE","type":"string"},
        {"name":"ISSUER_NAME","type":"string"},{"name":"MARKET_CAP","type":"string"},
        {"name":"MATURITY_OF_THE_OPTION","type":"string"},
        {"name":"MEASURE_TYPE_CODE","type":"string"},
        {"name":"PRA_BUCKET","type":"string"},{"name":"PRA_BUCKET_NAME","type":"string"},
        {"name":"PV_CURRENT","type":"number"},{"name":"PV_SIMULATED","type":"number"},
        {"name":"RATING","type":"string"},{"name":"REGION_AREA_CODE","type":"string"},
        {"name":"REMARKS","type":"string"},{"name":"SECTOR_NAME","type":"string"},
        {"name":"SECURITY_CODE","type":"string"},{"name":"SECURITY_CODE_TYPE","type":"string"},
        {"name":"SECURITY_INFORMATION1","type":"string"},
        {"name":"SECURITY_INFORMATION2","type":"string"},
        {"name":"SECURITY_INFORMATION3","type":"string"},
        {"name":"SIMULATION_ID","type":"string"},{"name":"SIMULATION_NAME","type":"string"},
        {"name":"SOURCE_SYSTEM","type":"string"},{"name":"SPOT_REPO","type":"string"},
        {"name":"TENOR_CODE","type":"string"},{"name":"TRANCHE","type":"string"},
        {"name":"UNDERLYING_TENOR_CODE","type":"string"},{"name":"VERTEX","type":"string"}
    ]'),
    NULL,
    PARSE_JSON('[
        {"payload_field":"ENTITY_CODE","target_column":"ENTITY_CODE","type":"string"},
        {"payload_field":"MEASURE_TYPE_CODE","target_column":"MEASURE_TYPE_CODE","type":"string"}
    ]'),
    PARSE_JSON('[
        {"source_field":"ENTITY_CODE","dimension_table":"DIMENSION.ENTITY",
         "match_column":"ENTITY_CODE","key_column":"ENTITY_KEY","target_column":"ENTITY_CODE"},
        {"source_field":"MEASURE_TYPE_CODE","dimension_table":"DIMENSION.MEASURE_TYPE",
         "match_column":"MEASURE_TYPE_CODE","key_column":"MEASURE_TYPE_KEY","target_column":"MEASURE_TYPE_CODE"}
    ]'),
    'AMOUNT', 'AMOUNT_IN_USD', 'write_direct_frtb_sbm', TRUE,
    PARSE_JSON('[
        {"field":"CCY1","conditions":[["RISK_CLASS","FX|GIRR",false]],"error":"CCY1 is required for FX and GIRR positions"},
        {"field":"CCY_AMT","conditions":[["RISK_CLASS","FX|GIRR",false]],"error":"CCY_AMT is required for FX and GIRR positions"},
        {"field":"CCY2","conditions":[["RISK_CLASS","FX",false]],"error":"CCY2 is required for FX positions"},
        {"field":"MATURITY_OF_THE_OPTION","conditions":[["RISK_CLASS","FX",false],["SENSITIVITY_TYPE","VEGA",false]],"error":"MATURITY_OF_THE_OPTION is required for FX Vega positions"},
        {"field":"UNDERLYING_TENOR_CODE","conditions":[["RISK_CLASS","FX",false],["SENSITIVITY_TYPE","VEGA",false]],"error":"UNDERLYING_TENOR_CODE (VERTEX_UNDERLYING) is required for FX Vega positions"},
        {"field":"CURVE_TYPE","conditions":[["RISK_CLASS","GIRR",false],["SENSITIVITY_TYPE","CURVATURE",false]],"error":"CURVE_TYPE is required for GIRR Curvature positions"},
        {"field":"VERTEX","conditions":[["RISK_CLASS","GIRR",false],["SENSITIVITY_TYPE","DELTA",false]],"error":"VERTEX is required for GIRR Delta positions"},
        {"field":"VERTEX","conditions":[["RISK_CLASS","CSR",false],["SENSITIVITY_TYPE","DELTA",false]],"error":"VERTEX is required for CSR Delta positions"},
        {"field":"CURVATURE_SCENARIO","conditions":[["SENSITIVITY_TYPE","CURVATURE",false]],"error":"CURVATURE_SCENARIO is required for Curvature positions"},
        {"field":"BUCKET","conditions":[["RISK_CLASS","EQUIT|CSR",false]],"error":"BUCKET is required for Equity and CSR positions"},
        {"field":"SECURITY_CODE_TYPE","conditions":[["RISK_CLASS","EQUIT|CSR",false]],"error":"SECURITY_CODE_TYPE is required for Equity and CSR positions"},
        {"field":"SECURITY_CODE","conditions":[["RISK_CLASS","EQUIT|CSR",false]],"error":"SECURITY_CODE is required for Equity and CSR positions"},
        {"field":"SECURITY_INFORMATION1","conditions":[["RISK_CLASS","EQUIT|CSR",false]],"error":"SECURITY_INFORMATION1 is required for Equity and CSR positions"},
        {"field":"SECURITY_INFORMATION2","conditions":[["RISK_CLASS","EQUIT|CSR",false]],"error":"SECURITY_INFORMATION2 is required for Equity and CSR positions"},
        {"field":"ISSUER_CODE","conditions":[["RISK_CLASS","EQUIT|CSR",false]],"error":"ISSUER_CODE is required for Equity and CSR positions"},
        {"field":"ISSUER_NAME","conditions":[["RISK_CLASS","EQUIT|CSR",false]],"error":"ISSUER_NAME is required for Equity and CSR positions"},
        {"field":"PRA_BUCKET","conditions":[["RISK_CLASS","EQUIT|CSR",false]],"error":"PRA_BUCKET is required for Equity and CSR positions"},
        {"field":"SECURITY_INFORMATION3","conditions":[["RISK_CLASS","CSR",false]],"error":"SECURITY_INFORMATION3 (Sector) is required for CSR positions"}
    ]'),
    PARSE_JSON('{"EVALUATION_DATE":"COBID","VERTEX_UNDERLYING":"UNDERLYING_TENOR_CODE",
                 "TRADE_ID":"TRADE_CODE","BUSINESS_ORGANIZATION_CODE":"BOOK_CODE"}');
COMMIT;

-- ═══════════════════════════════════════════════════════════════════════════
-- 2. DIRECT_SCOPE_SCHEMA — FRTBDRC
-- ═══════════════════════════════════════════════════════════════════════════
BEGIN TRANSACTION;
DELETE FROM ADJUSTMENT_APP.DIRECT_SCOPE_SCHEMA WHERE PROCESS_TYPE = 'FRTBDRC';
INSERT INTO ADJUSTMENT_APP.DIRECT_SCOPE_SCHEMA
    (PROCESS_TYPE, EXPECTED_COLUMNS, UNPIVOT, FACT_MAPPING, RESOLUTIONS,
     METRIC_FIELD, METRIC_USD_FIELD, WRITER_OVERRIDE, IS_ACTIVE,
     VALIDATION_RULES, ALIASES)
SELECT
    'FRTBDRC',
    PARSE_JSON('[
        {"name":"COBID","type":"number","required":true},
        {"name":"ENTITY_CODE","type":"string","required":true},
        {"name":"TRADE_CODE","type":"string","required":true},
        {"name":"BOOK_CODE","type":"string","required":true},
        {"name":"MUFG_PRODUCT_CODE","type":"string","required":true},
        {"name":"BUSINESS_PRODUCT_CODE4","type":"string","required":true},
        {"name":"TRADING_DESK","type":"string","required":true},
        {"name":"CCY1","type":"string","required":true},
        {"name":"CCY_AMT","type":"string","required":true},
        {"name":"SECURITY_CODE_TYPE","type":"string","required":true},
        {"name":"SECURITY_CODE","type":"string","required":true},
        {"name":"ISSUER_CODE","type":"string","required":true},
        {"name":"MATURITY_DATE","type":"date","required":true},
        {"name":"JTD_RISK_DIRECTION","type":"string","required":true},
        {"name":"RISK_CLASS","type":"string","required":true},
        {"name":"BUCKET","type":"string","required":true},
        {"name":"JTD_LOSS","type":"number","required":true},
        {"name":"JTD_LOSS_USD","type":"number","required":true},
        {"name":"ATTACHMENT","type":"string"},{"name":"BT_TYPE","type":"string"},
        {"name":"BUSINESS_PRODUCT_CODE1","type":"string"},
        {"name":"BUSINESS_PRODUCT_CODE2","type":"string"},
        {"name":"BUSINESS_PRODUCT_CODE3","type":"string"},
        {"name":"BUY_SELL","type":"string"},{"name":"CALL_PUT","type":"string"},
        {"name":"CCY1_OFFSHORE_QUALIFIER","type":"string"},
        {"name":"CCY2","type":"string"},{"name":"CCY2_OFFSHORE_QUALIFIER","type":"string"},
        {"name":"CCY_AMT_OFFSHORE_QUALIFIER","type":"string"},
        {"name":"COUNTER_PARTY_CODE_HD","type":"string"},
        {"name":"DEFAULT_RISK_WEIGHT","type":"string"},{"name":"DETACHMENT","type":"string"},
        {"name":"EBA_BUCKET","type":"string"},{"name":"EBA_BUCKET_NAME","type":"string"},
        {"name":"FED_BUCKET","type":"string"},{"name":"FED_BUCKET_NAME","type":"string"},
        {"name":"GMRM_LGD","type":"string"},{"name":"GMRM_PRODUCT","type":"string"},
        {"name":"INSTRUMENT_NAME","type":"string"},{"name":"ISSUE_NAME","type":"string"},
        {"name":"ISSUER_NAME","type":"string"},{"name":"ISSUER_RATING","type":"string"},
        {"name":"JTD_LOSS_ORIGINAL","type":"number"},
        {"name":"JTD_LOSS_USD_ORIGINAL","type":"number"},
        {"name":"LGD","type":"string"},{"name":"MARKET_PRICE","type":"string"},
        {"name":"MEASURE_TYPE_CODE","type":"string"},{"name":"NOTIONAL","type":"string"},
        {"name":"NOTIONAL_AMOUNT","type":"number"},
        {"name":"PRA_BUCKET","type":"string"},{"name":"PRA_BUCKET_NAME","type":"string"},
        {"name":"REGION","type":"string"},{"name":"REGION_AREA_CODE","type":"string"},
        {"name":"SECTOR_NAME","type":"string"},
        {"name":"SECURITY_INFORMATION1","type":"string"},
        {"name":"SECURITY_INFORMATION2","type":"string"},
        {"name":"SECURITY_INFORMATION3","type":"string"},
        {"name":"SIMULATION_ID","type":"string"},{"name":"SIMULATION_NAME","type":"string"},
        {"name":"STRIKE","type":"string"}
    ]'),
    NULL,
    PARSE_JSON('[
        {"payload_field":"ENTITY_CODE","target_column":"ENTITY_CODE","type":"string"},
        {"payload_field":"BOOK_CODE","target_column":"BOOK_CODE","type":"string"},
        {"payload_field":"MEASURE_TYPE_CODE","target_column":"MEASURE_TYPE_CODE","type":"string"}
    ]'),
    PARSE_JSON('[
        {"source_field":"ENTITY_CODE","dimension_table":"DIMENSION.ENTITY",
         "match_column":"ENTITY_CODE","key_column":"ENTITY_KEY","target_column":"ENTITY_CODE"},
        {"source_field":"BOOK_CODE","dimension_table":"DIMENSION.BOOK",
         "match_column":"BOOK_CODE","key_column":"BOOK_KEY","target_column":"BOOK_CODE"},
        {"source_field":"MEASURE_TYPE_CODE","dimension_table":"DIMENSION.MEASURE_TYPE",
         "match_column":"MEASURE_TYPE_CODE","key_column":"MEASURE_TYPE_KEY","target_column":"MEASURE_TYPE_CODE"}
    ]'),
    'JTD_LOSS', 'JTD_LOSS_USD', 'write_direct_frtb_drc', TRUE,
    PARSE_JSON('[
        {"field":"ISSUER_NAME","conditions":[["RISK_CLASS","NON.*SEC.*CREDIT",false]],"error":"ISSUER_NAME is required for Non-Sec (Credit)"},
        {"field":"ISSUER_NAME","conditions":[["RISK_CLASS","NON.*SEC.*EQUITY",false]],"error":"ISSUER_NAME is required for Non-Sec (Equity)"},
        {"field":"DEFAULT_RISK_WEIGHT","conditions":[["RISK_CLASS","NON.*SEC.*EQUITY",false]],"error":"DEFAULT_RISK_WEIGHT is required for Non-Sec (Equity)"},
        {"field":"DEFAULT_RISK_WEIGHT","conditions":[["RISK_CLASS","SEC",false],["RISK_CLASS","NON.*SEC",true]],"error":"DEFAULT_RISK_WEIGHT is required for Sec"},
        {"field":"LGD","conditions":[["RISK_CLASS","NON.*SEC.*CREDIT",false]],"error":"LGD is required for Non-Sec (Credit)"}
    ]'),
    PARSE_JSON('{"EVALUATION_DATE":"COBID","TRADE_ID":"TRADE_CODE",
                 "BUSINESS_ORGANIZATION_CODE":"BOOK_CODE"}');
COMMIT;

-- ═══════════════════════════════════════════════════════════════════════════
-- 3. DIRECT_SCOPE_SCHEMA — FRTBRRAO
-- ═══════════════════════════════════════════════════════════════════════════
BEGIN TRANSACTION;
DELETE FROM ADJUSTMENT_APP.DIRECT_SCOPE_SCHEMA WHERE PROCESS_TYPE = 'FRTBRRAO';
INSERT INTO ADJUSTMENT_APP.DIRECT_SCOPE_SCHEMA
    (PROCESS_TYPE, EXPECTED_COLUMNS, UNPIVOT, FACT_MAPPING, RESOLUTIONS,
     METRIC_FIELD, METRIC_USD_FIELD, WRITER_OVERRIDE, IS_ACTIVE,
     VALIDATION_RULES, ALIASES)
SELECT
    'FRTBRRAO',
    PARSE_JSON('[
        {"name":"COBID","type":"number","required":true},
        {"name":"ENTITY_CODE","type":"string","required":true},
        {"name":"TRADE_CODE","type":"string","required":true},
        {"name":"BOOK_CODE","type":"string","required":true},
        {"name":"MUFG_PRODUCT_CODE","type":"string","required":true},
        {"name":"BUSINESS_PRODUCT_CODE4","type":"string","required":true},
        {"name":"TRADING_DESK","type":"string","required":true},
        {"name":"CCY1","type":"string","required":true},
        {"name":"CCY_AMT","type":"string","required":true},
        {"name":"SA_RRAO_PRODUCT_TYPE","type":"string","required":true},
        {"name":"NOTIONAL_AMOUNT","type":"number","required":true},
        {"name":"NOTIONAL_AMOUNT_USD","type":"number","required":true},
        {"name":"BT_TYPE","type":"string"},
        {"name":"BUSINESS_PRODUCT_CODE1","type":"string"},
        {"name":"BUSINESS_PRODUCT_CODE2","type":"string"},
        {"name":"BUSINESS_PRODUCT_CODE3","type":"string"},
        {"name":"PRODUCT_CATEGORY_ATTRIBUTES","type":"string"},
        {"name":"CCY_AMT_OFFSHORE_QUALIFIER","type":"string"},
        {"name":"CCY1_OFFSHORE_QUALIFIER","type":"string"},
        {"name":"CCY2","type":"string"},{"name":"CCY2_OFFSHORE_QUALIFIER","type":"string"},
        {"name":"INSTRUMENT_CODE","type":"string"},{"name":"INSTRUMENT_NAME","type":"string"},
        {"name":"ISSUER_CODE","type":"string"},{"name":"MEASURE_TYPE_CODE","type":"string"},
        {"name":"REGION_AREA_CODE","type":"string"},
        {"name":"SIMULATION_ID","type":"string"},{"name":"SIMULATION_NAME","type":"string"},
        {"name":"STRATEGY","type":"string"}
    ]'),
    NULL,
    PARSE_JSON('[
        {"payload_field":"ENTITY_CODE","target_column":"ENTITY_CODE","type":"string"},
        {"payload_field":"BOOK_CODE","target_column":"BOOK_CODE","type":"string"},
        {"payload_field":"MEASURE_TYPE_CODE","target_column":"MEASURE_TYPE_CODE","type":"string"}
    ]'),
    PARSE_JSON('[
        {"source_field":"ENTITY_CODE","dimension_table":"DIMENSION.ENTITY",
         "match_column":"ENTITY_CODE","key_column":"ENTITY_KEY","target_column":"ENTITY_CODE"},
        {"source_field":"BOOK_CODE","dimension_table":"DIMENSION.BOOK",
         "match_column":"BOOK_CODE","key_column":"BOOK_KEY","target_column":"BOOK_CODE"},
        {"source_field":"MEASURE_TYPE_CODE","dimension_table":"DIMENSION.MEASURE_TYPE",
         "match_column":"MEASURE_TYPE_CODE","key_column":"MEASURE_TYPE_KEY","target_column":"MEASURE_TYPE_CODE"}
    ]'),
    'NOTIONAL_AMOUNT', 'NOTIONAL_AMOUNT_USD', 'write_direct_frtb_rrao', TRUE,
    PARSE_JSON('[]'),
    PARSE_JSON('{"EVALUATION_DATE":"COBID","TRADE_ID":"TRADE_CODE",
                 "BUSINESS_ORGANIZATION_CODE":"BOOK_CODE"}');
COMMIT;

-- ═══════════════════════════════════════════════════════════════════════════
-- 4. VW_DIRECT_FRTB_ENRICHED — SBM line items → FRTBSA_SENSITIVITY shape
--    Ported from the team's VW_ADJUSTMENTS_DIRECT_SBM with the adaptations
--    listed in the file header. Reads ADJ_LINE_ITEM_JSON keyed by ADJ_ID.
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW ADJUSTMENT_APP.VW_DIRECT_FRTB_ENRICHED AS
WITH base AS (
    SELECT
        j.ADJ_ID, j.ROW_NUM,
        h.DIMENSION_ADJ_ID                         AS ADJUSTMENT_ID,
        h.COBID, h.RUN_LOG_ID, h.FILE_NAME,
        TO_DATE(h.COBID::VARCHAR, 'YYYYMMDD')      AS EVAL_DATE,
        TRY_TO_NUMBER(TO_VARCHAR(j.PAYLOAD:"AMOUNT"), 38, 10)        AS AMOUNT,
        TRY_TO_NUMBER(TO_VARCHAR(j.PAYLOAD:"AMOUNT_IN_USD"), 38, 10) AS AMOUNT_IN_USD,
        TRY_TO_NUMBER(TO_VARCHAR(j.PAYLOAD:"DELTA_SUBTRACT"), 38, 10) AS DELTA_SUBTRACT,
        TRY_TO_NUMBER(TO_VARCHAR(j.PAYLOAD:"PV_CURRENT"), 38, 10)     AS PV_CURRENT,
        TRY_TO_NUMBER(TO_VARCHAR(j.PAYLOAD:"PV_SIMULATED"), 38, 10)   AS PV_SIMULATED,
        TO_VARCHAR(j.PAYLOAD:"BOND_CDS")            AS BOND_CDS,
        TO_VARCHAR(j.PAYLOAD:"BT_TYPE")             AS BT_TYPE,
        TO_VARCHAR(j.PAYLOAD:"BUCKET")              AS BUCKET,
        TO_VARCHAR(j.PAYLOAD:"BUSINESS_PRODUCT_CODE1") AS BUSINESS_PRODUCT_CODE1,
        TO_VARCHAR(j.PAYLOAD:"BUSINESS_PRODUCT_CODE2") AS BUSINESS_PRODUCT_CODE2,
        TO_VARCHAR(j.PAYLOAD:"BUSINESS_PRODUCT_CODE3") AS BUSINESS_PRODUCT_CODE3,
        TO_VARCHAR(j.PAYLOAD:"BUSINESS_PRODUCT_CODE4") AS BUSINESS_PRODUCT_CODE4,
        TO_VARCHAR(j.PAYLOAD:"CCY1")                AS CCY1,
        TO_VARCHAR(j.PAYLOAD:"CCY1_OFFSHORE_QUALIFIER") AS CCY1_OFFSHORE_QUALIFIER,
        TO_VARCHAR(j.PAYLOAD:"CCY2")                AS CCY2,
        TO_VARCHAR(j.PAYLOAD:"CCY2_OFFSHORE_QUALIFIER") AS CCY2_OFFSHORE_QUALIFIER,
        TO_VARCHAR(j.PAYLOAD:"CCY_AMT")             AS CCY_AMT,
        TO_VARCHAR(j.PAYLOAD:"CCY_AMT_OFFSHORE_QUALIFIER") AS CCY_AMT_OFFSHORE_QUALIFIER,
        TO_VARCHAR(j.PAYLOAD:"CDS_CURRENCY")        AS CDS_CURRENCY,
        TO_VARCHAR(j.PAYLOAD:"CLIENT_CODE")         AS CLIENT_CODE,
        TO_VARCHAR(j.PAYLOAD:"COUNTRY_CODE")        AS COUNTRY_CODE,
        TO_VARCHAR(j.PAYLOAD:"CURVATURE_SCENARIO")  AS CURVATURE_SCENARIO,
        TO_VARCHAR(j.PAYLOAD:"CURVE_TYPE")          AS CURVE_TYPE,
        TO_VARCHAR(j.PAYLOAD:"EBA_BUCKET")          AS EBA_BUCKET,
        TO_VARCHAR(j.PAYLOAD:"EBA_BUCKET_NAME")     AS EBA_BUCKET_NAME,
        TO_VARCHAR(j.PAYLOAD:"ENTITY_CODE")         AS ENTITY_CODE,
        TO_VARCHAR(j.PAYLOAD:"FED_BUCKET")          AS FED_BUCKET,
        TO_VARCHAR(j.PAYLOAD:"FED_BUCKET_NAME")     AS FED_BUCKET_NAME,
        TO_VARCHAR(j.PAYLOAD:"GUARANTEED_TYPE")     AS GUARANTEED_TYPE,
        TO_VARCHAR(j.PAYLOAD:"ISSUER_CODE")         AS ISSUER_CODE,
        TO_VARCHAR(j.PAYLOAD:"MARKET_CAP")          AS MARKET_CAP,
        TO_VARCHAR(j.PAYLOAD:"MATURITY_OF_THE_OPTION") AS MATURITY_OF_THE_OPTION,
        TO_VARCHAR(j.PAYLOAD:"MEASURE_TYPE_CODE")   AS MEASURE_TYPE_CODE,
        TO_VARCHAR(j.PAYLOAD:"MUFG_PRODUCT_CODE")   AS MUFG_PRODUCT_CODE,
        TO_VARCHAR(j.PAYLOAD:"TRADE_CODE")          AS TRADE_CODE,
        TO_VARCHAR(j.PAYLOAD:"PRA_BUCKET")          AS PRA_BUCKET,
        TO_VARCHAR(j.PAYLOAD:"PRA_BUCKET_NAME")     AS PRA_BUCKET_NAME,
        TO_VARCHAR(j.PAYLOAD:"RATING")              AS RATING,
        TO_VARCHAR(j.PAYLOAD:"REGION_AREA_CODE")    AS REGION_AREA_CODE,
        TO_VARCHAR(j.PAYLOAD:"REMARKS")             AS REMARKS,
        TO_VARCHAR(j.PAYLOAD:"RISK_CLASS")          AS RISK_CLASS,
        TO_VARCHAR(j.PAYLOAD:"SECTOR_NAME")         AS SECTOR_NAME,
        TO_VARCHAR(j.PAYLOAD:"SECURITY_CODE")       AS SECURITY_CODE,
        TO_VARCHAR(j.PAYLOAD:"SECURITY_CODE_TYPE")  AS SECURITY_CODE_TYPE,
        TO_VARCHAR(j.PAYLOAD:"SECURITY_INFORMATION1") AS SECURITY_INFORMATION1,
        TO_VARCHAR(j.PAYLOAD:"SECURITY_INFORMATION2") AS SECURITY_INFORMATION2,
        TO_VARCHAR(j.PAYLOAD:"SECURITY_INFORMATION3") AS SECURITY_INFORMATION3,
        TO_VARCHAR(j.PAYLOAD:"SENSITIVITY_TYPE")    AS SENSITIVITY_TYPE,
        TO_VARCHAR(j.PAYLOAD:"SIMULATION_ID")       AS SIMULATION_ID,
        TO_VARCHAR(j.PAYLOAD:"SIMULATION_NAME")     AS SIMULATION_NAME,
        TO_VARCHAR(j.PAYLOAD:"SOURCE_SYSTEM")       AS SOURCE_SYSTEM,
        TO_VARCHAR(j.PAYLOAD:"SPOT_REPO")           AS SPOT_REPO,
        TO_VARCHAR(j.PAYLOAD:"TENOR_CODE")          AS TENOR_CODE,
        TO_VARCHAR(j.PAYLOAD:"TRADING_DESK")        AS TRADING_DESK,
        TO_VARCHAR(j.PAYLOAD:"TRANCHE")             AS TRANCHE,
        TO_VARCHAR(j.PAYLOAD:"UNDERLYING_TENOR_CODE") AS UNDERLYING_TENOR_CODE,
        TO_VARCHAR(j.PAYLOAD:"VERTEX")              AS VERTEX
    FROM ADJUSTMENT_APP.ADJ_LINE_ITEM_JSON j
    JOIN ADJUSTMENT_APP.ADJ_HEADER h ON h.ADJ_ID = j.ADJ_ID
    WHERE j.IS_DELETED = FALSE
      AND h.IS_DELETED = FALSE
      AND UPPER(h.PROCESS_TYPE) = 'FRTB'
      AND h.ADJUSTMENT_ACTION = 'Upload'
),
enriched AS (
    SELECT base.*,
        'DIRECT-SBM' AS LOAD_SET,
        'NonCVA'     AS LOAD_SOURCE,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(9) AS LOAD_TIMESTAMP,
        COALESCE(NULLIF(base.CCY1, ''), base.CCY_AMT) AS CURRENCY_CODE,
        IFF(LEN(CONCAT(base.CCY2, '-', base.CCY1)) < 7, NULL,
            CONCAT(base.CCY2, '-', base.CCY1)) AS CURRENCY_PAIR_CODE,
        -- Deterministic per (adjustment, row): retries overwrite, never fork
        MD5(base.ADJ_ID || '-' || base.ROW_NUM || '-DIRECT') AS FRTBSA_SENSITIVITY_KEY
    FROM base
)
SELECT
    enriched.*,
    COALESCE(td.TRADE_SOURCE_SYSTEM_CODE, 'MS') AS SOURCE_SYSTEM_CODE,
    COALESCE(td.BOOK_CODE, b.BOOK_CODE) AS BOOK_CODE,
    COALESCE(td.BOOK_CODE, b.BOOK_CODE) AS BOOK_CODE_SABRE,
    COALESCE(b.BOOK_KEY, -1) AS BOOK_KEY,
    enriched.ENTITY_CODE AS ENTITY_CODE_SABRE,
    IFF(COALESCE(enriched.CURVE_TYPE, '') = '', 'N/A', enriched.CURVE_TYPE) AS CURVE_CODE,
    COALESCE(cc.CURVE_CURRENCY_KEY, -1) AS CURVE_CURRENCY_KEY,
    COALESCE(cp.CURRENCY_PAIR_KEY, -1) AS CURRENCY_PAIR_KEY,
    CASE enriched.GUARANTEED_TYPE WHEN 'YES' THEN UPPER(enriched.ENTITY_CODE)
         ELSE 'N/A' END AS GUARANTEED_ENTITY,
    COALESCE(fi.FRTB_INSTRUMENT_KEY, -1) AS FRTB_INSTRUMENT_KEY,
    COALESCE(ci.INSTRUMENT_CODE, ci2.INSTRUMENT_CODE, 'N/A')::VARCHAR(100)
        COLLATE 'en-ci' AS INSTRUMENT_CODE,
    TRUE AS IS_BELOW_THE_LINE_TYPOLOGY,
    TRUE AS IS_FRTB_STATUS_ACTIVE,
    COALESCE(mt.MEASURE_TYPE_KEY, -1) AS MEASURE_TYPE_KEY,
    COALESCE(NULLIF(td.MUREX_INSTRUMENT, ''), 'N/A') AS MUREX_INSTRUMENT,
    td.MUREX_VERSION AS MUREX_VERSION,
    COALESCE(td.PRODUCT_CATEGORY_ATTRIBUTES_KEY, -1) AS PRODUCT_CATEGORY_ATTRIBUTES_KEY,
    5 AS RAPTOR_LOGIC_TEMPLATE_KEY,
    CASE WHEN UPPER(COALESCE(enriched.ENTITY_CODE, '')) IN ('MUSI', 'MUSEU')
         THEN 'LONDON' ELSE 'NEW YORK' END AS REGION_DATA_SET_CODE,
    CASE WHEN UPPER(COALESCE(enriched.ENTITY_CODE, '')) = 'MUSI'   THEN enriched.PRA_BUCKET
         WHEN UPPER(COALESCE(enriched.ENTITY_CODE, '')) = 'MUSEU'  THEN enriched.EBA_BUCKET
         WHEN UPPER(COALESCE(enriched.ENTITY_CODE, '')) = 'MUSUSA' THEN enriched.FED_BUCKET
         WHEN UPPER(COALESCE(enriched.ENTITY_CODE, '')) = 'MUSCAN' THEN enriched.BUCKET
         ELSE NULL END AS REGULATOR_BUCKET,
    CASE WHEN UPPER(COALESCE(enriched.ENTITY_CODE, '')) = 'MUSI'   THEN enriched.PRA_BUCKET_NAME
         WHEN UPPER(COALESCE(enriched.ENTITY_CODE, '')) = 'MUSEU'  THEN enriched.EBA_BUCKET_NAME
         WHEN UPPER(COALESCE(enriched.ENTITY_CODE, '')) = 'MUSUSA' THEN enriched.FED_BUCKET_NAME
         ELSE NULL END AS REGULATOR_BUCKET_NAME,
    FALSE AS IS_CVA_MEASURE_TYPE,
    IFF(enriched.MEASURE_TYPE_CODE IN ('FRTBEQSPOTDELTA', 'FRTBEQREPODELTA'),
        TRUE, FALSE) AS IS_DUPLICATE_MEASURE_TYPE,
    TRUE  AS IS_OFFICIAL_SOURCE,
    FALSE AS IS_CVA_COUNTERPARTY,
    enriched.EVAL_DATE AS EVALUATION_DATE,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(9) AS ADJUSTMENT_CREATED_TIMESTAMP,
    COALESCE(fci.COMMON_INSTRUMENT_FCD_KEY, fci2.COMMON_INSTRUMENT_FCD_KEY, -1)
        AS COMMON_INSTRUMENT_FCD_KEY,
    COALESCE(ci.COMMON_INSTRUMENT_KEY, ci2.COMMON_INSTRUMENT_KEY, -1)
        AS COMMON_INSTRUMENT_KEY,
    enriched.COUNTRY_CODE AS SABRE_COUNTRY_CODE,
    COALESCE(tc.TENOR_CURRENCY_KEY, -1) AS TENOR_CURRENCY_KEY,
    COALESCE(td.TRADE_KEY, -1) AS TRADE_KEY,
    COALESCE(NULLIF(td.SABRE_TRADE_CODE, ''), td.TRADE_CODE) AS TRADE_NAME,
    td.TRADE_TYPOLOGY AS TRADE_TYPOLOGY,
    NULL AS ULTIMATE_PARENT,
    COALESCE(tim.ISIN, enriched.SECURITY_CODE) AS UNDERLYING,
    enriched.UNDERLYING_TENOR_CODE AS VERTEX_UNDERLYING,
    REPLACE(enriched.SECURITY_CODE, '.', '') AS SABRE_SECURITY_CODE,
    NULL AS SENIORITY,
    enriched.AMOUNT        AS AMOUNT_ORIGINAL,
    enriched.AMOUNT_IN_USD AS AMOUNT_IN_USD_ORIGINAL,
    -- Per-row key ⇒ zero test is per-row (the prototype's window collapsed
    -- to the same thing because its key was per-row too)
    COALESCE(enriched.AMOUNT_IN_USD, 0) = 0 AS IS_ZERO_FILTER,
    enriched.FILE_NAME AS RAVEN_FILENAME,
    enriched.ROW_NUM   AS RAVEN_FILE_ROW_NUMBER
FROM enriched
LEFT JOIN DIMENSION.TRADE td
  ON td.TRADE_CODE = NULLIF(enriched.TRADE_CODE, '')
 AND td.ENTITY_CODE = enriched.ENTITY_CODE
 AND enriched.EVAL_DATE BETWEEN td.EFFECTIVE_START_DATE AND td.EFFECTIVE_END_DATE
LEFT JOIN DIMENSION.BOOK b
  ON td.BOOK_CODE = b.BOOK_CODE
 AND enriched.EVAL_DATE BETWEEN b.EFFECTIVE_START_DATE AND b.EFFECTIVE_END_DATE
LEFT JOIN DIMENSION.MEASURE_TYPE mt
  ON enriched.MEASURE_TYPE_CODE = mt.MEASURE_TYPE_CODE
LEFT JOIN DIMENSION.TENOR_CURRENCY tc
  ON enriched.TENOR_CODE = tc.TENOR_CURRENCY_CODE
LEFT JOIN (SELECT TICKER, MAX(ISIN) AS ISIN
           FROM STATIC_STAGING.TICKER_ISIN_MAP GROUP BY TICKER) tim
  ON enriched.SECURITY_CODE = tim.TICKER
LEFT JOIN DIMENSION.CURRENCY_PAIR cp
  ON enriched.CURRENCY_PAIR_CODE = cp.CURRENCY_PAIR
LEFT JOIN DIMENSION.CURVE_CURRENCY cc
  ON IFF(COALESCE(enriched.CURVE_TYPE, '') = '', 'N/A', enriched.CURVE_TYPE) = cc.CURVE_CODE
LEFT JOIN DIMENSION.COMMON_INSTRUMENT ci
  ON NULLIF(enriched.SECURITY_CODE, '') = ci.INSTRUMENT_CODE
 AND enriched.EVAL_DATE BETWEEN ci.EFFECTIVE_START_DATE AND ci.EFFECTIVE_END_DATE
LEFT JOIN DIMENSION.COMMON_INSTRUMENT ci2
  ON td.INSTRUMENT_KEY = ci2.INSTRUMENT_KEY
 AND enriched.EVAL_DATE BETWEEN ci2.EFFECTIVE_START_DATE AND ci2.EFFECTIVE_END_DATE
LEFT JOIN DIMENSION.COMMON_INSTRUMENT_FCD fci
  ON NULLIF(enriched.SECURITY_CODE, '') = fci.INSTRUMENT_CODE
 AND fci.IS_CURRENT_ROW = TRUE
LEFT JOIN DIMENSION.COMMON_INSTRUMENT_FCD fci2
  ON td.INSTRUMENT_KEY = fci2.INSTRUMENT_KEY
 AND fci2.IS_CURRENT_ROW = TRUE
LEFT JOIN DIMENSION.FRTB_INSTRUMENT fi
  ON COALESCE(NULLIF(enriched.SECURITY_CODE, ''), 'NA') = fi.FRTB_INSTRUMENT_CODE
 AND COALESCE(NULLIF(enriched.ISSUER_CODE, ''), 'NA') = fi.FRTB_ISSUER_CODE
 AND enriched.EVAL_DATE BETWEEN fi.EFFECTIVE_START_DATE AND fi.EFFECTIVE_END_DATE
QUALIFY ROW_NUMBER() OVER (
    -- One output row per line item: dimension joins (TRADE by code+entity,
    -- instruments by code) can fan out when a code maps to multiple rows
    -- in the effective window; keep the best-resolved candidate.
    PARTITION BY ADJ_ID, ROW_NUM
    ORDER BY td.TRADE_KEY DESC NULLS LAST, ci.COMMON_INSTRUMENT_KEY DESC NULLS LAST) = 1;

-- ═══════════════════════════════════════════════════════════════════════════
-- 5. VW_DIRECT_FRTBDRC_ENRICHED — DRC line items → FRTBSA_DRC shape
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW ADJUSTMENT_APP.VW_DIRECT_FRTBDRC_ENRICHED AS
WITH base AS (
    SELECT
        j.ADJ_ID, j.ROW_NUM,
        h.DIMENSION_ADJ_ID AS ADJUSTMENT_ID,
        h.COBID, h.RUN_LOG_ID, h.FILE_NAME,
        TO_DATE(h.COBID::VARCHAR, 'YYYYMMDD') AS EVAL_DATE,
        TRY_TO_NUMBER(TO_VARCHAR(j.PAYLOAD:"JTD_LOSS"), 38, 10)              AS JTD_LOSS,
        TRY_TO_NUMBER(TO_VARCHAR(j.PAYLOAD:"JTD_LOSS_ORIGINAL"), 38, 10)     AS JTD_LOSS_ORIGINAL,
        TRY_TO_NUMBER(TO_VARCHAR(j.PAYLOAD:"JTD_LOSS_USD"), 38, 10)          AS JTD_LOSS_USD,
        TRY_TO_NUMBER(TO_VARCHAR(j.PAYLOAD:"JTD_LOSS_USD_ORIGINAL"), 38, 10) AS JTD_LOSS_USD_ORIGINAL,
        TRY_TO_NUMBER(TO_VARCHAR(j.PAYLOAD:"NOTIONAL_AMOUNT"), 38, 10)      AS NOTIONAL_AMOUNT,
        TRY_TO_DATE(TO_VARCHAR(j.PAYLOAD:"MATURITY_DATE"))                   AS MATURITY_DATE,
        TO_VARCHAR(j.PAYLOAD:"ATTACHMENT")   AS ATTACHMENT,
        TO_VARCHAR(j.PAYLOAD:"BOOK_CODE")    AS BOOK_CODE,
        TO_VARCHAR(j.PAYLOAD:"BT_TYPE")      AS BT_TYPE,
        TO_VARCHAR(j.PAYLOAD:"BUCKET")       AS BUCKET,
        TO_VARCHAR(j.PAYLOAD:"BUSINESS_PRODUCT_CODE1") AS BUSINESS_PRODUCT_CODE1,
        TO_VARCHAR(j.PAYLOAD:"BUSINESS_PRODUCT_CODE2") AS BUSINESS_PRODUCT_CODE2,
        TO_VARCHAR(j.PAYLOAD:"BUSINESS_PRODUCT_CODE3") AS BUSINESS_PRODUCT_CODE3,
        TO_VARCHAR(j.PAYLOAD:"BUSINESS_PRODUCT_CODE4") AS BUSINESS_PRODUCT_CODE4,
        TO_VARCHAR(j.PAYLOAD:"BUY_SELL")     AS BUY_SELL,
        TO_VARCHAR(j.PAYLOAD:"CALL_PUT")     AS CALL_PUT,
        TO_VARCHAR(j.PAYLOAD:"CCY1")         AS CCY1,
        TO_VARCHAR(j.PAYLOAD:"CCY1_OFFSHORE_QUALIFIER") AS CCY1_OFFSHORE_QUALIFIER,
        TO_VARCHAR(j.PAYLOAD:"CCY2")         AS CCY2,
        TO_VARCHAR(j.PAYLOAD:"CCY2_OFFSHORE_QUALIFIER") AS CCY2_OFFSHORE_QUALIFIER,
        TO_VARCHAR(j.PAYLOAD:"CCY_AMT")      AS CCY_AMT,
        TO_VARCHAR(j.PAYLOAD:"CCY_AMT_OFFSHORE_QUALIFIER") AS CCY_AMT_OFFSHORE_QUALIFIER,
        TO_VARCHAR(j.PAYLOAD:"COUNTER_PARTY_CODE_HD") AS COUNTER_PARTY_CODE_HD,
        TO_VARCHAR(j.PAYLOAD:"DEFAULT_RISK_WEIGHT")   AS DEFAULT_RISK_WEIGHT,
        TO_VARCHAR(j.PAYLOAD:"DETACHMENT")   AS DETACHMENT,
        TO_VARCHAR(j.PAYLOAD:"EBA_BUCKET")   AS EBA_BUCKET,
        TO_VARCHAR(j.PAYLOAD:"EBA_BUCKET_NAME") AS EBA_BUCKET_NAME,
        TO_VARCHAR(j.PAYLOAD:"ENTITY_CODE")  AS ENTITY_CODE,
        TO_VARCHAR(j.PAYLOAD:"FED_BUCKET")   AS FED_BUCKET,
        TO_VARCHAR(j.PAYLOAD:"FED_BUCKET_NAME") AS FED_BUCKET_NAME,
        TO_VARCHAR(j.PAYLOAD:"GMRM_LGD")     AS GMRM_LGD,
        TO_VARCHAR(j.PAYLOAD:"GMRM_PRODUCT") AS GMRM_PRODUCT,
        TO_VARCHAR(j.PAYLOAD:"INSTRUMENT_NAME") AS INSTRUMENT_NAME,
        TO_VARCHAR(j.PAYLOAD:"ISSUE_NAME")   AS ISSUE_NAME,
        TO_VARCHAR(j.PAYLOAD:"ISSUER_CODE")  AS ISSUER_CODE,
        TO_VARCHAR(j.PAYLOAD:"ISSUER_RATING") AS ISSUER_RATING,
        TO_VARCHAR(j.PAYLOAD:"JTD_RISK_DIRECTION") AS JTD_RISK_DIRECTION,
        TO_VARCHAR(j.PAYLOAD:"LGD")          AS LGD,
        TO_VARCHAR(j.PAYLOAD:"MARKET_PRICE") AS MARKET_PRICE,
        TO_VARCHAR(j.PAYLOAD:"MEASURE_TYPE_CODE") AS MEASURE_TYPE_CODE,
        TO_VARCHAR(j.PAYLOAD:"MUFG_PRODUCT_CODE") AS MUFG_PRODUCT_CODE,
        TO_VARCHAR(j.PAYLOAD:"NOTIONAL")     AS NOTIONAL,
        TO_VARCHAR(j.PAYLOAD:"TRADE_CODE")   AS TRADE_CODE,
        TO_VARCHAR(j.PAYLOAD:"PRA_BUCKET")   AS PRA_BUCKET,
        TO_VARCHAR(j.PAYLOAD:"PRA_BUCKET_NAME") AS PRA_BUCKET_NAME,
        TO_VARCHAR(j.PAYLOAD:"REGION")       AS REGION,
        TO_VARCHAR(j.PAYLOAD:"REGION_AREA_CODE") AS REGION_AREA_CODE,
        TO_VARCHAR(j.PAYLOAD:"RISK_CLASS")   AS RISK_CLASS,
        TO_VARCHAR(j.PAYLOAD:"SECTOR_NAME")  AS SECTOR_NAME,
        TO_VARCHAR(j.PAYLOAD:"SECURITY_CODE") AS SECURITY_CODE,
        TO_VARCHAR(j.PAYLOAD:"SECURITY_CODE_TYPE") AS SECURITY_CODE_TYPE,
        TO_VARCHAR(j.PAYLOAD:"SECURITY_INFORMATION1") AS SECURITY_INFORMATION1,
        TO_VARCHAR(j.PAYLOAD:"SECURITY_INFORMATION2") AS SECURITY_INFORMATION2,
        TO_VARCHAR(j.PAYLOAD:"SECURITY_INFORMATION3") AS SECURITY_INFORMATION3,
        TO_VARCHAR(j.PAYLOAD:"SIMULATION_ID") AS SIMULATION_ID,
        TO_VARCHAR(j.PAYLOAD:"SIMULATION_NAME") AS SIMULATION_NAME,
        TO_VARCHAR(j.PAYLOAD:"STRIKE")       AS STRIKE,
        TO_VARCHAR(j.PAYLOAD:"TRADING_DESK") AS TRADING_DESK
    FROM ADJUSTMENT_APP.ADJ_LINE_ITEM_JSON j
    JOIN ADJUSTMENT_APP.ADJ_HEADER h ON h.ADJ_ID = j.ADJ_ID
    WHERE j.IS_DELETED = FALSE
      AND h.IS_DELETED = FALSE
      AND UPPER(h.PROCESS_TYPE) = 'FRTBDRC'
      AND h.ADJUSTMENT_ACTION = 'Upload'
),
enriched AS (
    SELECT base.*,
        'DIRECT-DRC' AS LOAD_SET,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(9) AS LOAD_TIMESTAMP,
        COALESCE(NULLIF(base.CCY1, ''), base.CCY2) AS CURRENCY_CODE,
        IFF(LEN(CONCAT(base.CCY1, '-', base.CCY2)) < 7, NULL,
            CONCAT(base.CCY1, '-', base.CCY2)) AS CURRENCY_PAIR_CODE,
        MD5(base.ADJ_ID || '-' || base.ROW_NUM || '-DIRECT') AS FRTBSA_DRC_KEY
    FROM base
)
SELECT
    enriched.*,
    COALESCE(b.BOOK_KEY, -1) AS BOOK_KEY,
    COALESCE(td.PRODUCT_CATEGORY_ATTRIBUTES_KEY, -1) AS PRODUCT_CATEGORY_ATTRIBUTES_KEY,
    COALESCE(cp.CURRENCY_PAIR_KEY, -1) AS CURRENCY_PAIR_KEY,
    COALESCE(td.TRADE_KEY, -1) AS TRADE_KEY,
    NULL AS STRUCTURE_NUMBER,
    COALESCE(fci.COMMON_INSTRUMENT_FCD_KEY, fci2.COMMON_INSTRUMENT_FCD_KEY, -1)
        AS COMMON_INSTRUMENT_FCD_KEY,
    COALESCE(ci.COMMON_INSTRUMENT_KEY, ci2.COMMON_INSTRUMENT_KEY, -1)
        AS COMMON_INSTRUMENT_KEY,
    COALESCE(fi.FRTB_INSTRUMENT_KEY, -1) AS FRTB_INSTRUMENT_KEY,
    TRUE AS IS_BELOW_THE_LINE_TYPOLOGY,
    TRUE AS IS_FRTB_STATUS_ACTIVE,
    enriched.EVAL_DATE AS EVALUATION_DATE,
    COALESCE(mt.MEASURE_TYPE_KEY, -1) AS MEASURE_TYPE_KEY,
    COALESCE(NULLIF(td.MUREX_INSTRUMENT, ''), 'N/A') AS MUREX_INSTRUMENT,
    td.MUREX_VERSION AS MUREX_VERSION,
    5 AS RAPTOR_LOGIC_TEMPLATE_KEY,
    -- Keyed by ENTITY_CODE (required field), matching the SBM view. The
    -- team's original DRC view compared the optional REGION field against
    -- entity codes — every MUSI row fell through to NEW YORK/NULL bucket.
    CASE WHEN UPPER(COALESCE(enriched.ENTITY_CODE, '')) IN ('MUSI', 'MUSEU')
         THEN 'LONDON' ELSE 'NEW YORK' END AS REGION_DATA_SET_CODE,
    CASE WHEN UPPER(COALESCE(enriched.ENTITY_CODE, '')) = 'MUSI'   THEN enriched.PRA_BUCKET
         WHEN UPPER(COALESCE(enriched.ENTITY_CODE, '')) = 'MUSEU'  THEN enriched.EBA_BUCKET
         WHEN UPPER(COALESCE(enriched.ENTITY_CODE, '')) = 'MUSUSA' THEN enriched.FED_BUCKET
         WHEN UPPER(COALESCE(enriched.ENTITY_CODE, '')) = 'MUSCAN' THEN enriched.BUCKET
         ELSE NULL END AS REGULATOR_BUCKET,
    CASE WHEN UPPER(COALESCE(enriched.ENTITY_CODE, '')) = 'MUSI'   THEN enriched.PRA_BUCKET_NAME
         WHEN UPPER(COALESCE(enriched.ENTITY_CODE, '')) = 'MUSEU'  THEN enriched.EBA_BUCKET_NAME
         WHEN UPPER(COALESCE(enriched.ENTITY_CODE, '')) = 'MUSUSA' THEN enriched.FED_BUCKET_NAME
         ELSE NULL END AS REGULATOR_BUCKET_NAME,
    REPLACE(enriched.SECURITY_CODE, '.', '') AS SABRE_SECURITY_CODE,
    enriched.SECURITY_CODE_TYPE AS SABRE_SECURITY_CODE_TYPE,
    COALESCE(tim.ISIN, enriched.SECURITY_CODE) AS UNDERLYING,
    td.TRADE_TYPOLOGY AS TRADE_TYPOLOGY,
    COALESCE(td.TRADE_SOURCE_SYSTEM_CODE, 'MS') AS SOURCE_SYSTEM_CODE,
    COALESCE(enriched.JTD_LOSS_USD, 0) = 0 AS IS_ZERO_FILTER,
    TRUE AS IS_OFFICIAL_SOURCE,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(9) AS ADJUSTMENT_CREATED_TIMESTAMP,
    enriched.FILE_NAME AS RAVEN_FILENAME,
    enriched.ROW_NUM   AS RAVEN_FILE_ROW_NUMBER
FROM enriched
LEFT JOIN DIMENSION.BOOK b
  ON enriched.BOOK_CODE = b.BOOK_CODE
 AND enriched.EVAL_DATE BETWEEN b.EFFECTIVE_START_DATE AND b.EFFECTIVE_END_DATE
LEFT JOIN DIMENSION.TRADE td
  ON td.TRADE_CODE = COALESCE(NULLIF(enriched.TRADE_CODE, ''),
                              CONCAT(enriched.BOOK_CODE, '/Adjustment'))
 AND td.BOOK_CODE = COALESCE(enriched.BOOK_CODE, td.BOOK_CODE)
 AND td.ENTITY_CODE = enriched.ENTITY_CODE
 AND enriched.EVAL_DATE BETWEEN td.EFFECTIVE_START_DATE AND td.EFFECTIVE_END_DATE
LEFT JOIN DIMENSION.MEASURE_TYPE mt
  ON enriched.MEASURE_TYPE_CODE = mt.MEASURE_TYPE_CODE
LEFT JOIN (SELECT TICKER, MAX(ISIN) AS ISIN
           FROM STATIC_STAGING.TICKER_ISIN_MAP GROUP BY TICKER) tim
  ON enriched.SECURITY_CODE = tim.TICKER
LEFT JOIN DIMENSION.COMMON_INSTRUMENT ci
  ON COALESCE(NULLIF(enriched.SECURITY_CODE, ''), NULLIF(enriched.INSTRUMENT_NAME, ''))
       = ci.INSTRUMENT_CODE
 AND enriched.EVAL_DATE BETWEEN ci.EFFECTIVE_START_DATE AND ci.EFFECTIVE_END_DATE
LEFT JOIN DIMENSION.COMMON_INSTRUMENT ci2
  ON td.INSTRUMENT_KEY = ci2.INSTRUMENT_KEY
 AND enriched.EVAL_DATE BETWEEN ci2.EFFECTIVE_START_DATE AND ci2.EFFECTIVE_END_DATE
LEFT JOIN DIMENSION.COMMON_INSTRUMENT_FCD fci
  ON COALESCE(NULLIF(enriched.SECURITY_CODE, ''), NULLIF(enriched.INSTRUMENT_NAME, ''))
       = fci.INSTRUMENT_CODE
 AND fci.IS_CURRENT_ROW = TRUE
LEFT JOIN DIMENSION.COMMON_INSTRUMENT_FCD fci2
  ON td.INSTRUMENT_KEY = fci2.INSTRUMENT_KEY
 AND fci2.IS_CURRENT_ROW = TRUE
LEFT JOIN DIMENSION.CURRENCY_PAIR cp
  ON enriched.CURRENCY_PAIR_CODE = cp.CURRENCY_PAIR
LEFT JOIN DIMENSION.FRTB_INSTRUMENT fi
  ON COALESCE(NULLIF(enriched.SECURITY_CODE, ''), NULLIF(enriched.INSTRUMENT_NAME, ''), 'NA')
       = fi.FRTB_INSTRUMENT_CODE
 AND COALESCE(NULLIF(enriched.ISSUER_CODE, ''), 'NA') = fi.FRTB_ISSUER_CODE
 AND enriched.EVAL_DATE BETWEEN fi.EFFECTIVE_START_DATE AND fi.EFFECTIVE_END_DATE
QUALIFY ROW_NUMBER() OVER (
    -- One output row per line item: dimension joins (TRADE by code+entity,
    -- instruments by code) can fan out when a code maps to multiple rows
    -- in the effective window; keep the best-resolved candidate.
    PARTITION BY ADJ_ID, ROW_NUM
    ORDER BY td.TRADE_KEY DESC NULLS LAST, ci.COMMON_INSTRUMENT_KEY DESC NULLS LAST) = 1;

-- ═══════════════════════════════════════════════════════════════════════════
-- 6. VW_DIRECT_FRTBRRAO_ENRICHED — RRAO line items → FRTBSA_RRAO shape
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW ADJUSTMENT_APP.VW_DIRECT_FRTBRRAO_ENRICHED AS
WITH base AS (
    SELECT
        j.ADJ_ID, j.ROW_NUM,
        h.DIMENSION_ADJ_ID AS ADJUSTMENT_ID,
        h.COBID, h.RUN_LOG_ID, h.FILE_NAME,
        TO_DATE(h.COBID::VARCHAR, 'YYYYMMDD') AS EVAL_DATE,
        TRY_TO_NUMBER(TO_VARCHAR(j.PAYLOAD:"NOTIONAL_AMOUNT"), 38, 10)     AS NOTIONAL_AMOUNT,
        TRY_TO_NUMBER(TO_VARCHAR(j.PAYLOAD:"NOTIONAL_AMOUNT_USD"), 38, 10) AS NOTIONAL_AMOUNT_USD,
        TO_VARCHAR(j.PAYLOAD:"BOOK_CODE")   AS BOOK_CODE,
        TO_VARCHAR(j.PAYLOAD:"BT_TYPE")     AS BT_TYPE,
        TO_VARCHAR(j.PAYLOAD:"BUSINESS_PRODUCT_CODE1") AS BUSINESS_PRODUCT_CODE1,
        TO_VARCHAR(j.PAYLOAD:"BUSINESS_PRODUCT_CODE2") AS BUSINESS_PRODUCT_CODE2,
        TO_VARCHAR(j.PAYLOAD:"BUSINESS_PRODUCT_CODE3") AS BUSINESS_PRODUCT_CODE3,
        TO_VARCHAR(j.PAYLOAD:"BUSINESS_PRODUCT_CODE4") AS BUSINESS_PRODUCT_CODE4,
        TO_VARCHAR(j.PAYLOAD:"PRODUCT_CATEGORY_ATTRIBUTES") AS PRODUCT_CATEGORY_ATTRIBUTES,
        TO_VARCHAR(j.PAYLOAD:"CCY_AMT")     AS CCY_AMT,
        TO_VARCHAR(j.PAYLOAD:"CCY_AMT_OFFSHORE_QUALIFIER") AS CCY_AMT_OFFSHORE_QUALIFIER,
        TO_VARCHAR(j.PAYLOAD:"CCY1")        AS CCY1,
        TO_VARCHAR(j.PAYLOAD:"CCY1_OFFSHORE_QUALIFIER") AS CCY1_OFFSHORE_QUALIFIER,
        TO_VARCHAR(j.PAYLOAD:"CCY2")        AS CCY2,
        TO_VARCHAR(j.PAYLOAD:"CCY2_OFFSHORE_QUALIFIER") AS CCY2_OFFSHORE_QUALIFIER,
        TO_VARCHAR(j.PAYLOAD:"ENTITY_CODE") AS ENTITY_CODE,
        TO_VARCHAR(j.PAYLOAD:"INSTRUMENT_CODE") AS INSTRUMENT_CODE,
        TO_VARCHAR(j.PAYLOAD:"INSTRUMENT_NAME") AS INSTRUMENT_NAME,
        TO_VARCHAR(j.PAYLOAD:"ISSUER_CODE") AS ISSUER_CODE,
        TO_VARCHAR(j.PAYLOAD:"MEASURE_TYPE_CODE") AS MEASURE_TYPE_CODE,
        TO_VARCHAR(j.PAYLOAD:"MUFG_PRODUCT_CODE") AS MUFG_PRODUCT_CODE,
        TO_VARCHAR(j.PAYLOAD:"TRADE_CODE")  AS TRADE_CODE,
        TO_VARCHAR(j.PAYLOAD:"REGION_AREA_CODE")     AS REGION_AREA_CODE,
        TO_VARCHAR(j.PAYLOAD:"SA_RRAO_PRODUCT_TYPE") AS SA_RRAO_PRODUCT_TYPE,
        TO_VARCHAR(j.PAYLOAD:"SIMULATION_ID")   AS SIMULATION_ID,
        TO_VARCHAR(j.PAYLOAD:"SIMULATION_NAME") AS SIMULATION_NAME,
        TO_VARCHAR(j.PAYLOAD:"STRATEGY")     AS STRATEGY,
        TO_VARCHAR(j.PAYLOAD:"TRADING_DESK") AS TRADING_DESK
    FROM ADJUSTMENT_APP.ADJ_LINE_ITEM_JSON j
    JOIN ADJUSTMENT_APP.ADJ_HEADER h ON h.ADJ_ID = j.ADJ_ID
    WHERE j.IS_DELETED = FALSE
      AND h.IS_DELETED = FALSE
      AND UPPER(h.PROCESS_TYPE) = 'FRTBRRAO'
      AND h.ADJUSTMENT_ACTION = 'Upload'
),
enriched AS (
    SELECT base.*,
        MD5(base.ADJ_ID || '-' || base.ROW_NUM || '-DIRECT') AS FRTBSA_RRAO_KEY,
        'DIRECT-RRAO' AS LOAD_SET,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(9) AS LOAD_TIMESTAMP,
        COALESCE(NULLIF(base.CCY1, ''), base.CCY2) AS CURRENCY_CODE,
        IFF(LEN(CONCAT(base.CCY1, '-', base.CCY2)) < 7, NULL,
            CONCAT(base.CCY1, '-', base.CCY2)) AS CURRENCY_PAIR_CODE
    FROM base
)
SELECT
    enriched.*,
    COALESCE(b.BOOK_KEY, -1) AS BOOK_KEY,
    COALESCE(pca.PRODUCT_CATEGORY_ATTRIBUTES_KEY, -1) AS PRODUCT_CATEGORY_ATTRIBUTES_KEY,
    COALESCE(cp.CURRENCY_PAIR_KEY, -1) AS CURRENCY_PAIR_KEY,
    COALESCE(td.TRADE_KEY, -1) AS TRADE_KEY,
    COALESCE(td.TRADE_SOURCE_SYSTEM_CODE, 'MS') AS TRADE_SOURCE_SYSTEM_CODE,
    COALESCE(td.TRADE_SOURCE_SYSTEM_CODE, 'MS') AS SOURCE_SYSTEM_CODE,
    1 AS STRUCTURE_NUMBER,
    enriched.EVAL_DATE AS EVALUATION_DATE,
    COALESCE(ci.COMMON_INSTRUMENT_KEY, -1)      AS COMMON_INSTRUMENT_KEY,
    COALESCE(fci.COMMON_INSTRUMENT_FCD_KEY, -1) AS COMMON_INSTRUMENT_FCD_KEY,
    TRUE AS IS_BELOW_THE_LINE_TYPOLOGY,
    TRUE AS IS_FRTB_STATUS_ACTIVE,
    COALESCE(mt.MEASURE_TYPE_KEY, -1) AS MEASURE_TYPE_KEY,
    enriched.NOTIONAL_AMOUNT     AS NOTIONAL_AMOUNT_ORIGINAL,
    enriched.NOTIONAL_AMOUNT_USD AS NOTIONAL_AMOUNT_USD_ORIGINAL,
    5 AS RAPTOR_LOGIC_TEMPLATE_KEY,
    COALESCE(enriched.NOTIONAL_AMOUNT_USD, 0) = 0 AS IS_ZERO_FILTER,
    TRUE AS IS_OFFICIAL_SOURCE,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(9) AS ADJUSTMENT_CREATED_TIMESTAMP,
    enriched.FILE_NAME AS RAVEN_FILENAME,
    enriched.ROW_NUM   AS RAVEN_FILE_ROW_NUMBER
FROM enriched
LEFT JOIN DIMENSION.BOOK b
  ON enriched.BOOK_CODE = b.BOOK_CODE
 AND enriched.EVAL_DATE BETWEEN b.EFFECTIVE_START_DATE AND b.EFFECTIVE_END_DATE
LEFT JOIN DIMENSION.TRADE td
  ON td.TRADE_CODE = COALESCE(NULLIF(enriched.TRADE_CODE, ''),
                              CONCAT(enriched.BOOK_CODE, '/Adjustment'))
 AND td.BOOK_CODE = COALESCE(enriched.BOOK_CODE, td.BOOK_CODE)
 AND td.ENTITY_CODE = enriched.ENTITY_CODE
 AND enriched.EVAL_DATE BETWEEN td.EFFECTIVE_START_DATE AND td.EFFECTIVE_END_DATE
LEFT JOIN DIMENSION.PRODUCT_CATEGORY_ATTRIBUTES pca
  ON pca.PCA_CONCAT_KEY = REPLACE(enriched.PRODUCT_CATEGORY_ATTRIBUTES, ' ', '')
LEFT JOIN DIMENSION.MEASURE_TYPE mt
  ON enriched.MEASURE_TYPE_CODE = mt.MEASURE_TYPE_CODE
LEFT JOIN DIMENSION.CURRENCY_PAIR cp
  ON enriched.CURRENCY_PAIR_CODE = cp.CURRENCY_PAIR
LEFT JOIN DIMENSION.COMMON_INSTRUMENT ci
  ON enriched.INSTRUMENT_CODE = ci.INSTRUMENT_CODE
 AND enriched.EVAL_DATE BETWEEN ci.EFFECTIVE_START_DATE AND ci.EFFECTIVE_END_DATE
LEFT JOIN DIMENSION.COMMON_INSTRUMENT_FCD fci
  ON enriched.INSTRUMENT_CODE = fci.INSTRUMENT_CODE
 AND fci.IS_CURRENT_ROW = TRUE
QUALIFY ROW_NUMBER() OVER (
    -- One output row per line item: dimension joins (TRADE by code+entity,
    -- instruments by code) can fan out when a code maps to multiple rows
    -- in the effective window; keep the best-resolved candidate.
    PARTITION BY ADJ_ID, ROW_NUM
    ORDER BY td.TRADE_KEY DESC NULLS LAST, ci.COMMON_INSTRUMENT_KEY DESC NULLS LAST) = 1;

-- ═══════════════════════════════════════════════════════════════════════════
-- VERIFY
-- ═══════════════════════════════════════════════════════════════════════════
SELECT PROCESS_TYPE, WRITER_OVERRIDE, IS_ACTIVE
FROM ADJUSTMENT_APP.DIRECT_SCOPE_SCHEMA
WHERE PROCESS_TYPE IN ('FRTB', 'FRTBDRC', 'FRTBRRAO');
