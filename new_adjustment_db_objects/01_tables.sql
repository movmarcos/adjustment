-- =============================================================================
-- 01_TABLES.SQL
-- All tables for the unified adjustment process.
-- Source of truth: ADJUSTMENT_APP.ADJ_HEADER is where every adjustment starts.
-- Input: Streamlit on Snowflake (no files, no staging tables).
--
-- All objects live in the ADJUSTMENT_APP schema.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS ADJUSTMENT_APP;
USE SCHEMA ADJUSTMENT_APP;

-- ═══════════════════════════════════════════════════════════════════════════
-- PREREQUISITE (run once by ACCOUNTADMIN):
--   GRANT READ SESSION ON ACCOUNT TO ROLE {{ROLE_OWNER}};
-- This allows CURRENT_USER() to return the viewer's identity in SiS.
-- Without it, CURRENT_USER() returns NULL and USERNAME will fail.
-- ═══════════════════════════════════════════════════════════════════════════

-- ═══════════════════════════════════════════════════════════════════════════
-- 1. ADJ_HEADER — The single point of entry for ALL adjustments
--
-- Every adjustment (ad-hoc or recurring) starts as a row here.
-- Streamlit writes here directly via SP_SUBMIT_ADJUSTMENT.
-- The processing procedure reads from this table (config-driven).
--
-- Columns mirror DIMENSION.ADJUSTMENT filter dimensions so the processing
-- procedure can join to fact tables using the same column names.
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR ALTER TABLE ADJUSTMENT_APP.ADJ_HEADER (
    -- Identity
    ADJ_ID                      VARCHAR(36)  NOT NULL DEFAULT UUID_STRING(),
    COBID                       NUMBER(38,0) NOT NULL,

    -- Scope & type
    PROCESS_TYPE                VARCHAR(30)  NOT NULL,      -- VaR, Stress, FRTB, Sensitivity
    ADJUSTMENT_TYPE             VARCHAR(20)  NOT NULL,      -- Flatten, Scale, Upload
    ADJUSTMENT_ACTION           VARCHAR(10)  NOT NULL,      -- Direct (Upload), Scale (Scale/Flatten)
    SOURCE_COBID                NUMBER(38,0),               -- Same as COBID for same-COB; different for Roll
    SCALE_FACTOR                NUMBER(10,4) DEFAULT 1,     -- User-specified: 1.0 for Flatten, actual for Scale
    SCALE_FACTOR_ADJUSTED       NUMBER(10,4) DEFAULT -1,    -- Computed: -1 (Flatten), sf-1 (same-COB), sf (cross-COB)

    -- Filter dimensions (NULL = all values / no filter on this dimension)
    ENTITY_CODE                 VARCHAR(10)  COLLATE 'en-ci',
    SOURCE_SYSTEM_CODE          VARCHAR(2)   COLLATE 'en-ci',
    DEPARTMENT_CODE             VARCHAR(10)  COLLATE 'en-ci',
    BOOK_CODE                   VARCHAR(20)  COLLATE 'en-ci',
    CURRENCY_CODE               VARCHAR(3)   COLLATE 'en-ci',
    TRADE_TYPOLOGY              VARCHAR(4)   COLLATE 'en-ci',
    TRADE_CODE                  VARCHAR(100) COLLATE 'en-ci',
    STRATEGY                    VARCHAR(50)  COLLATE 'en-ci',
    TRADER_CODE                 VARCHAR(50)  COLLATE 'en-ci',
    VAR_COMPONENT_ID            NUMBER(38,0),
    VAR_SUB_COMPONENT_ID        NUMBER(38,0),
    GUARANTEED_ENTITY           VARCHAR(10)  COLLATE 'en-ci',
    REGION_KEY                  NUMBER(38,0),
    SCENARIO_DATE_ID            NUMBER(38,0),
    INSTRUMENT_CODE             VARCHAR(50)  COLLATE 'en-ci',
    SIMULATION_NAME             VARCHAR(250) COLLATE 'en-ci',
    SIMULATION_SOURCE           VARCHAR(30)  COLLATE 'en-ci',
    TENOR_CODE                  VARCHAR(10)  COLLATE 'en-ci',
    UNDERLYING_TENOR_CODE       VARCHAR(10)  COLLATE 'en-ci',
    CURVE_CODE                  VARCHAR(50)  COLLATE 'en-ci',
    MEASURE_TYPE_CODE           VARCHAR(30)  COLLATE 'en-ci',
    DAY_TYPE                    NUMBER(38,0),
    PRODUCT_CATEGORY_ATTRIBUTES VARCHAR(255) COLLATE 'en-ci',
    BATCH_REGION_AREA           VARCHAR(10)  COLLATE 'en-ci',
    MUREX_FAMILY                VARCHAR(5)   COLLATE 'en-ci',
    MUREX_GROUP                 VARCHAR(5)   COLLATE 'en-ci',

    -- Adjustment value (for Direct/Upload; NULL for Scale/Flatten)
    ADJUSTMENT_VALUE_IN_USD     NUMBER(20,6),

    -- Business context
    REASON                      VARCHAR(1000) COLLATE 'en-ci',

    -- Cross-reference to dimension table
    DIMENSION_ADJ_ID            NUMBER(38,0) DEFAULT NULL,       -- DIMENSION.ADJUSTMENT.ADJUSTMENT_ID set after processing

    -- Workflow status
    RUN_STATUS                  VARCHAR(30)  COLLATE 'en-ci' DEFAULT 'Pending',
    IS_POSITIVE_ADJUSTMENT      BOOLEAN      DEFAULT TRUE,       -- FALSE = superseded / inactive
    START_DATE                  TIMESTAMP_NTZ(9),                -- Set when status → Running (processing starts)
    PROCESS_DATE                TIMESTAMP_NTZ(9),                -- Set when status → Processed / Failed (processing ends)
    RECORD_COUNT                NUMBER(38,0),
    ERRORMESSAGE                VARCHAR(1000) COLLATE 'en-ci',
    RUN_LOG_ID                  NUMBER(38,0),                    -- Set by SP_PROCESS_ADJUSTMENT from BATCH.SEQ_RUN_LOG

    -- Soft delete
    IS_DELETED                  BOOLEAN      DEFAULT FALSE,
    DELETED_BY                  VARCHAR(40)  COLLATE 'en-ci',
    DELETED_DATE                TIMESTAMP_NTZ(9),

    -- User & timing
    USERNAME                    VARCHAR(50)  COLLATE 'en-ci' NOT NULL,
    CREATED_DATE                TIMESTAMP_NTZ(9) NOT NULL DEFAULT CONVERT_TIMEZONE('Europe/London', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ(9),

    -- Mode & origin
    ADJUSTMENT_OCCURRENCE       VARCHAR(20)  DEFAULT 'ADHOC',    -- ADHOC or RECURRING
    GLOBAL_REFERENCE            VARCHAR(50)  COLLATE 'en-ci',    -- Unique ref for dedup / linking
    FILE_NAME                   VARCHAR(500) COLLATE 'en-ci',    -- For CSV uploads via Streamlit
    APPROVAL_ID                 NUMBER(38,0),                     -- Optional: set when requires_approval = true
    BLOCKED_BY_ADJ_ID           VARCHAR(36)  DEFAULT NULL,        -- FK to ADJ_HEADER.ADJ_ID; NULL = eligible to run

    ADJUSTMENT_CATEGORY         VARCHAR(100)  COLLATE 'en-ci',   -- from ADJ_CATEGORY; required in UI

    -- Concurrency: set by the run (SP_RUN_PIPELINE / SP_FORCE_PROCESS) that
    -- claimed this row to Running. SP_PROCESS_ADJUSTMENT only touches rows
    -- carrying its caller's token, so two concurrent runs can never process
    -- (or fail) each other's claims.
    CLAIM_TOKEN                 VARCHAR(36)  DEFAULT NULL,

    -- VaR component filters by NAME (what users know; the engine matches
    -- DIMENSION.VAR_SUB_COMPONENT on the names, the fact table still joins by
    -- VAR_SUBCOMPONENT_ID). The legacy VAR_COMPONENT_ID / VAR_SUB_COMPONENT_ID
    -- columns above remain for pre-existing rows and are still honoured.
    -- NOTE: new columns must stay at the END (CREATE OR ALTER append-only).
    VAR_COMPONENT_NAME          VARCHAR(200) COLLATE 'en-ci',
    VAR_SUB_COMPONENT_NAME      VARCHAR(200) COLLATE 'en-ci',

    CONSTRAINT PK_ADJ_HEADER PRIMARY KEY (ADJ_ID)
)
COMMENT = 'Single point of entry for ALL adjustments. Streamlit writes here via SP_SUBMIT_ADJUSTMENT. Processing reads from here.';


-- ═══════════════════════════════════════════════════════════════════════════
-- 2. ADJ_LINE_ITEM — Detail rows for Direct/Upload adjustments
--
-- For VaR_Upload: user uploads CSV via Streamlit, the app parses + UNPIVOTs
-- the 21 VaR columns, then writes one row per (entity, book, scenario,
-- VaR component) here.
--
-- For Scale/Flatten: NOT used — the processing procedure reads fact tables
-- directly and applies the scale factor.
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR ALTER TABLE ADJUSTMENT_APP.ADJ_LINE_ITEM (
    LINE_ID                     NUMBER(38,0) NOT NULL AUTOINCREMENT,
    ADJ_ID                      VARCHAR(36)  NOT NULL,  -- FK to ADJ_HEADER

    -- Dimension codes (matched to fact table columns for joining)
    COBID                       NUMBER(38,0),
    ENTITY_CODE                 VARCHAR(10)  COLLATE 'en-ci',
    SOURCE_SYSTEM_CODE          VARCHAR(2)   COLLATE 'en-ci',
    BOOK_CODE                   VARCHAR(20)  COLLATE 'en-ci',
    DEPARTMENT_CODE             VARCHAR(10)  COLLATE 'en-ci',
    CURRENCY_CODE               VARCHAR(3)   COLLATE 'en-ci',
    TRADE_CODE                  VARCHAR(100) COLLATE 'en-ci',
    TRADE_TYPOLOGY              VARCHAR(4)   COLLATE 'en-ci',
    STRATEGY                    VARCHAR(50)  COLLATE 'en-ci',
    SCENARIO_DATE_ID            NUMBER(38,0),
    VAR_COMPONENT_ID            NUMBER(38,0),
    VAR_SUB_COMPONENT_ID        NUMBER(38,0),
    INSTRUMENT_CODE             VARCHAR(50)  COLLATE 'en-ci',
    SIMULATION_NAME             VARCHAR(250) COLLATE 'en-ci',
    SIMULATION_SOURCE           VARCHAR(30)  COLLATE 'en-ci',
    TENOR_CODE                  VARCHAR(10)  COLLATE 'en-ci',
    UNDERLYING_TENOR_CODE       VARCHAR(10)  COLLATE 'en-ci',
    CURVE_CODE                  VARCHAR(50)  COLLATE 'en-ci',
    MEASURE_TYPE_CODE           VARCHAR(30)  COLLATE 'en-ci',
    DAY_TYPE                    NUMBER(38,0),
    TRADER_CODE                 VARCHAR(50)  COLLATE 'en-ci',
    GUARANTEED_ENTITY           VARCHAR(10)  COLLATE 'en-ci',
    REGION_KEY                  NUMBER(38,0),

    -- Metric values (the actual adjustment amounts)
    ADJUSTMENT_VALUE            NUMBER(20,6),
    ADJUSTMENT_VALUE_IN_USD     NUMBER(20,6),

    -- VaR_Upload specifics (Category + Detail → REASON on header)
    CATEGORY                    VARCHAR(200) COLLATE 'en-ci',
    DETAIL                      VARCHAR(2000) COLLATE 'en-ci',

    -- Status & metadata
    IS_DELETED                  BOOLEAN      DEFAULT FALSE,
    RUN_STATUS                  VARCHAR(30)  DEFAULT 'Pending',
    CREATED_DATE                TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_ADJ_LINE_ITEM PRIMARY KEY (LINE_ID)
)
COMMENT = 'Detail rows for Direct/Upload adjustments. Each row = one dimension combination with its adjustment value.';


-- ═══════════════════════════════════════════════════════════════════════════
-- ADJ_LINE_ITEM_JSON — Direct Adjustment uploads (semi-structured)
-- One row per uploaded CSV line; raw fields live in PAYLOAD (VARIANT).
-- Per-scope interpretation is driven by DIRECT_SCOPE_SCHEMA at processing time.
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR ALTER TABLE ADJUSTMENT_APP.ADJ_LINE_ITEM_JSON (
    LINE_ID      NUMBER(38,0) NOT NULL AUTOINCREMENT,
    ADJ_ID       VARCHAR(36)  NOT NULL,            -- FK to ADJ_HEADER
    ROW_NUM      NUMBER(38,0),                     -- 1-based line order within the upload
    PAYLOAD      VARIANT,                          -- the raw CSV row as a JSON object
    IS_DELETED   BOOLEAN          DEFAULT FALSE,
    RUN_STATUS   VARCHAR(30)      DEFAULT 'Pending',
    CREATED_DATE TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_ADJ_LINE_ITEM_JSON PRIMARY KEY (LINE_ID)
)
COMMENT = 'Direct Adjustment uploads: one row per CSV line, raw fields in PAYLOAD (VARIANT).';

-- ═══════════════════════════════════════════════════════════════════════════
-- ADJ_DIRECT_STAGE — parse target for Direct Adjustment paste/upload.
-- The app writes one row per pasted CSV line (BATCH_ID = one paste), the
-- per-scope VW_DIRECT_VALIDATE_* views validate them, and submit turns each
-- VALID row into its own ADJ_HEADER. Rows are deleted after submit/cancel;
-- anything older than 2 days is abandoned and may be purged.
-- ═══════════════════════════════════════════════════════════════════════════
-- widths mirror ADJ_HEADER so green rows cannot fail at submit with truncation errors
CREATE OR ALTER TABLE ADJUSTMENT_APP.ADJ_DIRECT_STAGE (
    BATCH_ID            VARCHAR(36)  NOT NULL,
    ROW_NUM             NUMBER(38,0) NOT NULL,
    ENTITY_CODE         VARCHAR(10),
    SOURCE_SYSTEM_CODE  VARCHAR(2),
    DEPARTMENT_CODE     VARCHAR(10),
    BOOK_CODE           VARCHAR(20),
    TRADE_CODE          VARCHAR(100),
    TRADE_TYPOLOGY      VARCHAR(4),
    STRATEGY            VARCHAR(50),
    INSTRUMENT_CODE     VARCHAR(50),
    SIMULATION_NAME     VARCHAR(250),
    SIMULATION_SOURCE   VARCHAR(30),
    MEASURE_TYPE_CODE   VARCHAR(30),
    CURRENCY_CODE       VARCHAR(3),
    VALUE_USD           VARCHAR(100),          -- raw text; numeric check is a validation rule
    USERNAME            VARCHAR(200),
    CREATED_DATE        TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    TENOR_CODE          VARCHAR(10),
    CURVE_CODE          VARCHAR(50),
    PRODUCT_CATEGORY_ATTRIBUTES VARCHAR(255),
    REASON              VARCHAR(1000),
    CONSTRAINT PK_ADJ_DIRECT_STAGE PRIMARY KEY (BATCH_ID, ROW_NUM)
)
COMMENT = 'Direct Adjustment staging: one row per pasted/uploaded CSV line, validated by VW_DIRECT_VALIDATE_<scope>, then submitted one header per valid row.';

-- ═══════════════════════════════════════════════════════════════════════════
-- DIRECT_ACCEPTED_COLUMNS — per-scope accepted CSV header names for Direct.
-- Column order/case in the paste never matter: the app matches each CSV
-- header (upper-cased, trimmed) against ACCEPTED_NAME and writes the cell to
-- STAGE_COLUMN. IS_REQUIRED drives the validation views' required checks.
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR ALTER TABLE ADJUSTMENT_APP.DIRECT_ACCEPTED_COLUMNS (
    PROCESS_TYPE   VARCHAR(20)  NOT NULL,
    ACCEPTED_NAME  VARCHAR(100) NOT NULL,      -- stored upper-case
    STAGE_COLUMN   VARCHAR(50)  NOT NULL,      -- a column of ADJ_DIRECT_STAGE
    IS_REQUIRED    BOOLEAN      DEFAULT FALSE, -- required flags live on the canonical name row
    IS_ACTIVE      BOOLEAN      DEFAULT TRUE,
    DISPLAY_ORDER  NUMBER(3,0),               -- grid/template position; NULL = CSV-only
    CONSTRAINT PK_DIRECT_ACCEPTED_COLUMNS PRIMARY KEY (PROCESS_TYPE, ACCEPTED_NAME)
)
COMMENT = 'Accepted CSV header names (incl. aliases) per scope for Direct Adjustment; drives order-free parsing and required-field validation.';

-- Seeds — canonical name + aliases per scope, one block per scope (FRTB/
-- FRTBDRC/FRTBRRAO share an identical column set, so those three are a
-- single CROSS JOIN block). DISPLAY_ORDER drives the grid/template column
-- order; NULL = CSV-only (accepted on paste/upload but not shown as a grid
-- column). MERGE keeps re-runs idempotent. Every (PROCESS_TYPE,
-- ACCEPTED_NAME) pair below is written exactly once per scope block, and
-- the six PROCESS_TYPE values never overlap across blocks, so there is no
-- risk of a duplicate source row at MERGE time.
MERGE INTO ADJUSTMENT_APP.DIRECT_ACCEPTED_COLUMNS t
USING (
    -- VaR (Direct does VaR too — only the legacy-layout file feed is VaR Upload)
    SELECT 'VaR' AS PROCESS_TYPE, c.ACCEPTED_NAME, c.STAGE_COLUMN, c.IS_REQUIRED, c.DISPLAY_ORDER
    FROM (
        SELECT 'ENTITY_CODE' ACCEPTED_NAME,'ENTITY_CODE' STAGE_COLUMN, TRUE  IS_REQUIRED, 10  DISPLAY_ORDER UNION ALL
        SELECT 'ENTITY',            'ENTITY_CODE',        FALSE, NULL UNION ALL
        SELECT 'SOURCE_SYSTEM_CODE','SOURCE_SYSTEM_CODE', FALSE, 20 UNION ALL
        SELECT 'DEPARTMENT_CODE',   'DEPARTMENT_CODE',    FALSE, 30 UNION ALL
        SELECT 'DEPARTMENT',        'DEPARTMENT_CODE',    FALSE, NULL UNION ALL
        SELECT 'BOOK_CODE',         'BOOK_CODE',          FALSE, 40 UNION ALL
        SELECT 'BOOK',              'BOOK_CODE',          FALSE, NULL UNION ALL
        SELECT 'TRADE_CODE',        'TRADE_CODE',         FALSE, 50 UNION ALL
        SELECT 'TRADE',             'TRADE_CODE',         FALSE, NULL UNION ALL
        SELECT 'TRADE_TYPOLOGY',    'TRADE_TYPOLOGY',     FALSE, 60 UNION ALL
        SELECT 'STRATEGY',          'STRATEGY',           FALSE, 70 UNION ALL
        SELECT 'INSTRUMENT_CODE',   'INSTRUMENT_CODE',    FALSE, 80 UNION ALL
        SELECT 'INSTRUMENT',        'INSTRUMENT_CODE',    FALSE, NULL UNION ALL
        SELECT 'CURRENCY_CODE',     'CURRENCY_CODE',      FALSE, 90 UNION ALL
        SELECT 'CURRENCY',          'CURRENCY_CODE',      FALSE, NULL UNION ALL
        SELECT 'VALUE_USD',         'VALUE_USD',          TRUE,  100 UNION ALL
        SELECT 'VALUE',             'VALUE_USD',          FALSE, NULL UNION ALL
        SELECT 'AMOUNT',            'VALUE_USD',          FALSE, NULL UNION ALL
        SELECT 'ADJUSTMENT_VALUE',  'VALUE_USD',          FALSE, NULL UNION ALL
        SELECT 'REASON',            'REASON',             FALSE, 110
    ) c

    UNION ALL

    -- Stress
    SELECT 'Stress' AS PROCESS_TYPE, c.ACCEPTED_NAME, c.STAGE_COLUMN, c.IS_REQUIRED, c.DISPLAY_ORDER
    FROM (
        SELECT 'ENTITY_CODE' ACCEPTED_NAME,'ENTITY_CODE' STAGE_COLUMN, TRUE  IS_REQUIRED, 10  DISPLAY_ORDER UNION ALL
        SELECT 'ENTITY',            'ENTITY_CODE',        FALSE, NULL UNION ALL
        SELECT 'SOURCE_SYSTEM_CODE','SOURCE_SYSTEM_CODE', FALSE, 20 UNION ALL
        SELECT 'DEPARTMENT_CODE',   'DEPARTMENT_CODE',    FALSE, NULL UNION ALL
        SELECT 'DEPARTMENT',        'DEPARTMENT_CODE',    FALSE, NULL UNION ALL
        SELECT 'BOOK_CODE',         'BOOK_CODE',          FALSE, 30 UNION ALL
        SELECT 'BOOK',              'BOOK_CODE',          FALSE, NULL UNION ALL
        SELECT 'CURRENCY_CODE',     'CURRENCY_CODE',      FALSE, 40 UNION ALL
        SELECT 'CURRENCY',          'CURRENCY_CODE',      FALSE, NULL UNION ALL
        SELECT 'INSTRUMENT_CODE',   'INSTRUMENT_CODE',    FALSE, 50 UNION ALL
        SELECT 'INSTRUMENT',        'INSTRUMENT_CODE',    FALSE, NULL UNION ALL
        SELECT 'VALUE_USD',         'VALUE_USD',          TRUE,  60 UNION ALL
        SELECT 'VALUE',             'VALUE_USD',          FALSE, NULL UNION ALL
        SELECT 'AMOUNT',            'VALUE_USD',          FALSE, NULL UNION ALL
        SELECT 'ADJUSTMENT_VALUE',  'VALUE_USD',          FALSE, NULL UNION ALL
        SELECT 'REASON',            'REASON',             FALSE, 70 UNION ALL
        SELECT 'TRADE_TYPOLOGY',    'TRADE_TYPOLOGY',     FALSE, 80 UNION ALL
        SELECT 'STRATEGY',          'STRATEGY',           FALSE, NULL UNION ALL
        SELECT 'TRADE_CODE',        'TRADE_CODE',         FALSE, 90 UNION ALL
        SELECT 'TRADE',             'TRADE_CODE',         FALSE, NULL UNION ALL
        SELECT 'SIMULATION_NAME',   'SIMULATION_NAME',    FALSE, 100 UNION ALL
        SELECT 'SIMULATION',        'SIMULATION_NAME',    FALSE, NULL UNION ALL
        SELECT 'SIMULATION_SOURCE', 'SIMULATION_SOURCE',  FALSE, 110 UNION ALL
        SELECT 'PRODUCT_CATEGORY_ATTRIBUTES','PRODUCT_CATEGORY_ATTRIBUTES', FALSE, 120 UNION ALL
        SELECT 'PCA',                'PRODUCT_CATEGORY_ATTRIBUTES', FALSE, NULL
    ) c

    UNION ALL

    -- Sensitivity
    SELECT 'Sensitivity' AS PROCESS_TYPE, c.ACCEPTED_NAME, c.STAGE_COLUMN, c.IS_REQUIRED, c.DISPLAY_ORDER
    FROM (
        SELECT 'ENTITY_CODE' ACCEPTED_NAME,'ENTITY_CODE' STAGE_COLUMN, TRUE  IS_REQUIRED, 10  DISPLAY_ORDER UNION ALL
        SELECT 'ENTITY',            'ENTITY_CODE',        FALSE, NULL UNION ALL
        SELECT 'SOURCE_SYSTEM_CODE','SOURCE_SYSTEM_CODE', FALSE, 20 UNION ALL
        SELECT 'DEPARTMENT_CODE',   'DEPARTMENT_CODE',    FALSE, NULL UNION ALL
        SELECT 'DEPARTMENT',        'DEPARTMENT_CODE',    FALSE, NULL UNION ALL
        SELECT 'BOOK_CODE',         'BOOK_CODE',          FALSE, 30 UNION ALL
        SELECT 'BOOK',              'BOOK_CODE',          FALSE, NULL UNION ALL
        SELECT 'TENOR_CODE',        'TENOR_CODE',         FALSE, 40 UNION ALL
        SELECT 'TENOR',             'TENOR_CODE',         FALSE, NULL UNION ALL
        SELECT 'CURRENCY_CODE',     'CURRENCY_CODE',      FALSE, 50 UNION ALL
        SELECT 'CURRENCY',          'CURRENCY_CODE',      FALSE, NULL UNION ALL
        SELECT 'CURVE_CODE',        'CURVE_CODE',         FALSE, 60 UNION ALL
        SELECT 'CURVE',             'CURVE_CODE',         FALSE, NULL UNION ALL
        SELECT 'INSTRUMENT_CODE',   'INSTRUMENT_CODE',    FALSE, 70 UNION ALL
        SELECT 'INSTRUMENT',        'INSTRUMENT_CODE',    FALSE, NULL UNION ALL
        SELECT 'MEASURE_TYPE_CODE', 'MEASURE_TYPE_CODE',  FALSE, 80 UNION ALL
        SELECT 'MEASURE_TYPE',      'MEASURE_TYPE_CODE',  FALSE, NULL UNION ALL
        SELECT 'VALUE_USD',         'VALUE_USD',          TRUE,  90 UNION ALL
        SELECT 'VALUE',             'VALUE_USD',          FALSE, NULL UNION ALL
        SELECT 'AMOUNT',            'VALUE_USD',          FALSE, NULL UNION ALL
        SELECT 'ADJUSTMENT_VALUE',  'VALUE_USD',          FALSE, NULL UNION ALL
        SELECT 'REASON',            'REASON',             FALSE, 100 UNION ALL
        SELECT 'TRADE_TYPOLOGY',    'TRADE_TYPOLOGY',     FALSE, 110 UNION ALL
        SELECT 'STRATEGY',          'STRATEGY',           FALSE, NULL UNION ALL
        SELECT 'TRADE_CODE',        'TRADE_CODE',         FALSE, 120 UNION ALL
        SELECT 'TRADE',             'TRADE_CODE',         FALSE, NULL UNION ALL
        SELECT 'PRODUCT_CATEGORY_ATTRIBUTES','PRODUCT_CATEGORY_ATTRIBUTES', FALSE, 130 UNION ALL
        SELECT 'PCA',                'PRODUCT_CATEGORY_ATTRIBUTES', FALSE, NULL
    ) c

    UNION ALL

    -- FRTB / FRTBDRC / FRTBRRAO — identical column set (VaR's, plus MEASURE_TYPE_CODE at 95)
    SELECT 'FRTB' AS PROCESS_TYPE, c.ACCEPTED_NAME, c.STAGE_COLUMN, c.IS_REQUIRED, c.DISPLAY_ORDER
    FROM (
        SELECT 'ENTITY_CODE' ACCEPTED_NAME,'ENTITY_CODE' STAGE_COLUMN, TRUE  IS_REQUIRED, 10  DISPLAY_ORDER UNION ALL
        SELECT 'ENTITY',            'ENTITY_CODE',        FALSE, NULL UNION ALL
        SELECT 'SOURCE_SYSTEM_CODE','SOURCE_SYSTEM_CODE', FALSE, 20 UNION ALL
        SELECT 'DEPARTMENT_CODE',   'DEPARTMENT_CODE',    FALSE, 30 UNION ALL
        SELECT 'DEPARTMENT',        'DEPARTMENT_CODE',    FALSE, NULL UNION ALL
        SELECT 'BOOK_CODE',         'BOOK_CODE',          FALSE, 40 UNION ALL
        SELECT 'BOOK',              'BOOK_CODE',          FALSE, NULL UNION ALL
        SELECT 'TRADE_CODE',        'TRADE_CODE',         FALSE, 50 UNION ALL
        SELECT 'TRADE',             'TRADE_CODE',         FALSE, NULL UNION ALL
        SELECT 'TRADE_TYPOLOGY',    'TRADE_TYPOLOGY',     FALSE, 60 UNION ALL
        SELECT 'STRATEGY',          'STRATEGY',           FALSE, 70 UNION ALL
        SELECT 'INSTRUMENT_CODE',   'INSTRUMENT_CODE',    FALSE, 80 UNION ALL
        SELECT 'INSTRUMENT',        'INSTRUMENT_CODE',    FALSE, NULL UNION ALL
        SELECT 'CURRENCY_CODE',     'CURRENCY_CODE',      FALSE, 90 UNION ALL
        SELECT 'CURRENCY',          'CURRENCY_CODE',      FALSE, NULL UNION ALL
        SELECT 'VALUE_USD',         'VALUE_USD',          TRUE,  100 UNION ALL
        SELECT 'VALUE',             'VALUE_USD',          FALSE, NULL UNION ALL
        SELECT 'AMOUNT',            'VALUE_USD',          FALSE, NULL UNION ALL
        SELECT 'ADJUSTMENT_VALUE',  'VALUE_USD',          FALSE, NULL UNION ALL
        SELECT 'REASON',            'REASON',             FALSE, 110 UNION ALL
        SELECT 'MEASURE_TYPE_CODE', 'MEASURE_TYPE_CODE',  FALSE, 95 UNION ALL
        SELECT 'MEASURE_TYPE',      'MEASURE_TYPE_CODE',  FALSE, NULL
    ) c

    UNION ALL

    SELECT 'FRTBDRC' AS PROCESS_TYPE, c.ACCEPTED_NAME, c.STAGE_COLUMN, c.IS_REQUIRED, c.DISPLAY_ORDER
    FROM (
        SELECT 'ENTITY_CODE' ACCEPTED_NAME,'ENTITY_CODE' STAGE_COLUMN, TRUE  IS_REQUIRED, 10  DISPLAY_ORDER UNION ALL
        SELECT 'ENTITY',            'ENTITY_CODE',        FALSE, NULL UNION ALL
        SELECT 'SOURCE_SYSTEM_CODE','SOURCE_SYSTEM_CODE', FALSE, 20 UNION ALL
        SELECT 'DEPARTMENT_CODE',   'DEPARTMENT_CODE',    FALSE, 30 UNION ALL
        SELECT 'DEPARTMENT',        'DEPARTMENT_CODE',    FALSE, NULL UNION ALL
        SELECT 'BOOK_CODE',         'BOOK_CODE',          FALSE, 40 UNION ALL
        SELECT 'BOOK',              'BOOK_CODE',          FALSE, NULL UNION ALL
        SELECT 'TRADE_CODE',        'TRADE_CODE',         FALSE, 50 UNION ALL
        SELECT 'TRADE',             'TRADE_CODE',         FALSE, NULL UNION ALL
        SELECT 'TRADE_TYPOLOGY',    'TRADE_TYPOLOGY',     FALSE, 60 UNION ALL
        SELECT 'STRATEGY',          'STRATEGY',           FALSE, 70 UNION ALL
        SELECT 'INSTRUMENT_CODE',   'INSTRUMENT_CODE',    FALSE, 80 UNION ALL
        SELECT 'INSTRUMENT',        'INSTRUMENT_CODE',    FALSE, NULL UNION ALL
        SELECT 'CURRENCY_CODE',     'CURRENCY_CODE',      FALSE, 90 UNION ALL
        SELECT 'CURRENCY',          'CURRENCY_CODE',      FALSE, NULL UNION ALL
        SELECT 'VALUE_USD',         'VALUE_USD',          TRUE,  100 UNION ALL
        SELECT 'VALUE',             'VALUE_USD',          FALSE, NULL UNION ALL
        SELECT 'AMOUNT',            'VALUE_USD',          FALSE, NULL UNION ALL
        SELECT 'ADJUSTMENT_VALUE',  'VALUE_USD',          FALSE, NULL UNION ALL
        SELECT 'REASON',            'REASON',             FALSE, 110 UNION ALL
        SELECT 'MEASURE_TYPE_CODE', 'MEASURE_TYPE_CODE',  FALSE, 95 UNION ALL
        SELECT 'MEASURE_TYPE',      'MEASURE_TYPE_CODE',  FALSE, NULL
    ) c

    UNION ALL

    SELECT 'FRTBRRAO' AS PROCESS_TYPE, c.ACCEPTED_NAME, c.STAGE_COLUMN, c.IS_REQUIRED, c.DISPLAY_ORDER
    FROM (
        SELECT 'ENTITY_CODE' ACCEPTED_NAME,'ENTITY_CODE' STAGE_COLUMN, TRUE  IS_REQUIRED, 10  DISPLAY_ORDER UNION ALL
        SELECT 'ENTITY',            'ENTITY_CODE',        FALSE, NULL UNION ALL
        SELECT 'SOURCE_SYSTEM_CODE','SOURCE_SYSTEM_CODE', FALSE, 20 UNION ALL
        SELECT 'DEPARTMENT_CODE',   'DEPARTMENT_CODE',    FALSE, 30 UNION ALL
        SELECT 'DEPARTMENT',        'DEPARTMENT_CODE',    FALSE, NULL UNION ALL
        SELECT 'BOOK_CODE',         'BOOK_CODE',          FALSE, 40 UNION ALL
        SELECT 'BOOK',              'BOOK_CODE',          FALSE, NULL UNION ALL
        SELECT 'TRADE_CODE',        'TRADE_CODE',         FALSE, 50 UNION ALL
        SELECT 'TRADE',             'TRADE_CODE',         FALSE, NULL UNION ALL
        SELECT 'TRADE_TYPOLOGY',    'TRADE_TYPOLOGY',     FALSE, 60 UNION ALL
        SELECT 'STRATEGY',          'STRATEGY',           FALSE, 70 UNION ALL
        SELECT 'INSTRUMENT_CODE',   'INSTRUMENT_CODE',    FALSE, 80 UNION ALL
        SELECT 'INSTRUMENT',        'INSTRUMENT_CODE',    FALSE, NULL UNION ALL
        SELECT 'CURRENCY_CODE',     'CURRENCY_CODE',      FALSE, 90 UNION ALL
        SELECT 'CURRENCY',          'CURRENCY_CODE',      FALSE, NULL UNION ALL
        SELECT 'VALUE_USD',         'VALUE_USD',          TRUE,  100 UNION ALL
        SELECT 'VALUE',             'VALUE_USD',          FALSE, NULL UNION ALL
        SELECT 'AMOUNT',            'VALUE_USD',          FALSE, NULL UNION ALL
        SELECT 'ADJUSTMENT_VALUE',  'VALUE_USD',          FALSE, NULL UNION ALL
        SELECT 'REASON',            'REASON',             FALSE, 110 UNION ALL
        SELECT 'MEASURE_TYPE_CODE', 'MEASURE_TYPE_CODE',  FALSE, 95 UNION ALL
        SELECT 'MEASURE_TYPE',      'MEASURE_TYPE_CODE',  FALSE, NULL
    ) c
) src
ON  t.PROCESS_TYPE = src.PROCESS_TYPE AND t.ACCEPTED_NAME = src.ACCEPTED_NAME
WHEN MATCHED THEN UPDATE SET
    t.STAGE_COLUMN = src.STAGE_COLUMN, t.IS_REQUIRED = src.IS_REQUIRED,
    t.IS_ACTIVE = TRUE, t.DISPLAY_ORDER = src.DISPLAY_ORDER
WHEN NOT MATCHED THEN INSERT
    (PROCESS_TYPE, ACCEPTED_NAME, STAGE_COLUMN, IS_REQUIRED, IS_ACTIVE, DISPLAY_ORDER)
VALUES (src.PROCESS_TYPE, src.ACCEPTED_NAME, src.STAGE_COLUMN, src.IS_REQUIRED, TRUE, src.DISPLAY_ORDER);

-- ═══════════════════════════════════════════════════════════════════════════
-- DIRECT_SCOPE_SCHEMA — per-scope Direct Adjustment schema (dev-maintained)
-- Declares how to extract/resolve/map a scope's JSON payload into its fact table.
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR ALTER TABLE ADJUSTMENT_APP.DIRECT_SCOPE_SCHEMA (
    PROCESS_TYPE      VARCHAR(30)  NOT NULL,
    EXPECTED_COLUMNS  VARIANT,      -- [{ "name":..., "type":..., "required":bool }]
    UNPIVOT           VARIANT,      -- nullable {measure_map:{csv_col:measure_value}, measure_name_field, value_field}
    FACT_MAPPING      VARIANT,      -- [{ "payload_field":..., "target_column":..., "type":... }]
    RESOLUTIONS       VARIANT,      -- [{ "source_field":..., "dimension_table":..., "match_column":..., "key_column":..., "target_column":... }]
    METRIC_FIELD      VARCHAR(100),
    METRIC_USD_FIELD  VARCHAR(100),
    WRITER_OVERRIDE   VARCHAR(100),
    IS_ACTIVE         BOOLEAN          DEFAULT TRUE,
    CREATED_DATE      TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_DIRECT_SCOPE_SCHEMA PRIMARY KEY (PROCESS_TYPE)
)
COMMENT = 'Per-scope Direct Adjustment schema: how to extract/resolve/map JSON payload into the scope fact table.';

DELETE FROM ADJUSTMENT_APP.DIRECT_SCOPE_SCHEMA WHERE PROCESS_TYPE = 'VaR';
INSERT INTO ADJUSTMENT_APP.DIRECT_SCOPE_SCHEMA
    (PROCESS_TYPE, EXPECTED_COLUMNS, UNPIVOT, FACT_MAPPING, RESOLUTIONS,
     METRIC_FIELD, METRIC_USD_FIELD, WRITER_OVERRIDE, IS_ACTIVE)
SELECT
    'VaR',
    PARSE_JSON('[
        {"name":"COBId","type":"number","required":true},
        {"name":"EntityCode","type":"string","required":true},
        {"name":"SourceSystemCode","type":"string","required":false},
        {"name":"BookCode","type":"string","required":false},
        {"name":"CurrencyCode","type":"string","required":false},
        {"name":"ScenarioDate","type":"number","required":false},
        {"name":"TradeCode","type":"string","required":false},
        {"name":"AllVaR","type":"number","required":false},
        {"name":"AllVaRSkew","type":"number","required":false},
        {"name":"BasisVaR","type":"number","required":false},
        {"name":"BondAssetSpreadVaR","type":"number","required":false},
        {"name":"CrossEffects","type":"number","required":false},
        {"name":"EquityPriceVaR","type":"number","required":false},
        {"name":"EquityVegaVaR","type":"number","required":false},
        {"name":"FXRateVaR","type":"number","required":false},
        {"name":"FXVolatilityVaR","type":"number","required":false},
        {"name":"IRCapVolVaR","type":"number","required":false},
        {"name":"IRCapVolVaRSkew","type":"number","required":false},
        {"name":"IRSkewVolVaR","type":"number","required":false},
        {"name":"IRSwaptionVolVaR","type":"number","required":false},
        {"name":"IRSwaptionVolVaRSkew","type":"number","required":false},
        {"name":"InflationRateCurveVaR","type":"number","required":false},
        {"name":"InflationVolVaR","type":"number","required":false},
        {"name":"InterestRateCurveVaR","type":"number","required":false},
        {"name":"InterestRateVegaVaR","type":"number","required":false},
        {"name":"MTGSprdVaR","type":"number","required":false},
        {"name":"OASVaR","type":"number","required":false},
        {"name":"ParCreditSpreadVaR","type":"number","required":false},
        {"name":"Category","type":"string","required":false},
        {"name":"Detail","type":"string","required":false}
    ]'),
    PARSE_JSON('{
        "measure_map":{
            "AllVaR":"ALL VAR","AllVaRSkew":"ALL VAR SKEW","BasisVaR":"BASIS VAR",
            "BondAssetSpreadVaR":"BOND ASSET SPREAD VAR","CrossEffects":"CROSS EFFECTS",
            "EquityPriceVaR":"EQUITY PRICE VAR","EquityVegaVaR":"EQUITY VEGA VAR",
            "FXRateVaR":"FX RATE VAR","FXVolatilityVaR":"FX VOLATILITY VAR",
            "IRCapVolVaR":"IR CAP VOL VAR","IRCapVolVaRSkew":"IR CAP VOL VAR SKEW",
            "IRSkewVolVaR":"IR SKEW VOL VAR","IRSwaptionVolVaR":"IR SWAPTION VOL VAR",
            "IRSwaptionVolVaRSkew":"IR SWAPTION VOL VAR SKEW",
            "InflationRateCurveVaR":"INFLATION RATE CURVE VAR","InflationVolVaR":"INFLATION VOL VAR",
            "InterestRateCurveVaR":"INTEREST RATE CURVE VAR","InterestRateVegaVaR":"INTEREST RATE VEGA VAR",
            "MTGSprdVaR":"MTG SPRD VAR","OASVaR":"OAS VAR","ParCreditSpreadVaR":"PAR CREDIT SPREAD VAR"
        },
        "measure_name_field":"VAR_SUB_COMPONENT_NAME",
        "value_field":"ADJ_VALUE"
    }'),
    PARSE_JSON('[
        {"payload_field":"COBId","target_column":"COBID","type":"number"},
        {"payload_field":"EntityCode","target_column":"ENTITY_CODE","type":"string"},
        {"payload_field":"SourceSystemCode","target_column":"SOURCE_SYSTEM_CODE","type":"string"},
        {"payload_field":"CurrencyCode","target_column":"CURRENCY_CODE","type":"string"},
        {"payload_field":"ScenarioDate","target_column":"SCENARIO_DATE_ID","type":"number"}
    ]'),
    PARSE_JSON('[
        {"source_field":"VAR_SUB_COMPONENT_NAME","dimension_table":"DIMENSION.VAR_SUB_COMPONENT",
         "match_column":"VAR_SUB_COMPONENT_NAME","key_column":"VAR_SUB_COMPONENT_ID",
         "target_column":"VAR_SUBCOMPONENT_ID"}
    ]'),
    'ADJ_VALUE', 'ADJ_VALUE', NULL, TRUE;


-- ═══════════════════════════════════════════════════════════════════════════
-- 3. ADJ_STATUS_HISTORY — Full audit trail of every status change
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR ALTER TABLE ADJUSTMENT_APP.ADJ_STATUS_HISTORY (
    HISTORY_ID                  NUMBER(38,0) NOT NULL AUTOINCREMENT,
    ADJ_ID                      VARCHAR(36)  NOT NULL,  -- FK to ADJ_HEADER
    OLD_STATUS                  VARCHAR(30),
    NEW_STATUS                  VARCHAR(30)  NOT NULL,
    CHANGED_BY                  VARCHAR(50)  NOT NULL,
    CHANGED_AT                  TIMESTAMP_NTZ(9) NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    COMMENT                     VARCHAR(2000),

    CONSTRAINT PK_ADJ_STATUS_HISTORY PRIMARY KEY (HISTORY_ID)
)
COMMENT = 'Complete audit trail of every status transition for every adjustment.';


-- ═══════════════════════════════════════════════════════════════════════════
-- 4. ADJUSTMENTS_SETTINGS — Config table (scope → table mapping)
--
-- Drives the processing engine. One row per scope.
-- Adding a new scope = adding a new row. No code changes.
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR ALTER TABLE ADJUSTMENT_APP.ADJUSTMENTS_SETTINGS (
    PROCESS_TYPE                VARCHAR(30)  NOT NULL,
    FACT_TABLE                  VARCHAR(200) NOT NULL,       -- Source fact table to read from
    FACT_ADJUSTED_TABLE         VARCHAR(200),                -- Source for cross-COB (existing adj)
    FACT_TABLE_PK               VARCHAR(500) NOT NULL,       -- PK columns (;-separated)
    ADJUSTMENTS_TABLE           VARCHAR(200) NOT NULL,       -- Target adjustment detail table
    ADJUSTMENTS_SUMMARY_TABLE   VARCHAR(200),                -- Target summary (NULL = no summary)
    METRIC_NAME                 VARCHAR(50)  NOT NULL,       -- Local currency metric
    METRIC_USD_NAME             VARCHAR(50)  NOT NULL,       -- USD metric
    IS_ACTIVE                   BOOLEAN      NOT NULL DEFAULT TRUE,
    CREATED_DATE                TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_ADJ_SETTINGS PRIMARY KEY (PROCESS_TYPE)
)
COMMENT = 'Config: maps each scope to its fact/adjustment tables, metrics, PKs. Adding a new scope = new row, no code change.';


-- ═══════════════════════════════════════════════════════════════════════════
-- 5. ADJ_RECURRING_TEMPLATE — Templates for recurring adjustments
--
-- Admin configures templates here. An external scheduler or the processing
-- task checks for templates whose dependencies are met and auto-creates
-- ADJ_HEADER entries from them.
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR ALTER TABLE ADJUSTMENT_APP.ADJ_RECURRING_TEMPLATE (
    TEMPLATE_ID                 NUMBER(38,0) NOT NULL AUTOINCREMENT,
    TEMPLATE_NAME               VARCHAR(200),
    PROCESS_TYPE                VARCHAR(30)  NOT NULL,
    ADJUSTMENT_TYPE             VARCHAR(20)  NOT NULL,
    ADJUSTMENT_ACTION           VARCHAR(10),
    SCALE_FACTOR                NUMBER(10,4) DEFAULT 1,

    -- Filter dimensions (same as ADJ_HEADER)
    ENTITY_CODE                 VARCHAR(10),
    SOURCE_SYSTEM_CODE          VARCHAR(2),
    DEPARTMENT_CODE             VARCHAR(10),
    BOOK_CODE                   VARCHAR(20),
    CURRENCY_CODE               VARCHAR(3),
    TRADE_TYPOLOGY              VARCHAR(4),
    TRADE_CODE                  VARCHAR(100),
    STRATEGY                    VARCHAR(50),
    INSTRUMENT_CODE             VARCHAR(50),
    SIMULATION_NAME             VARCHAR(250),
    SIMULATION_SOURCE           VARCHAR(30),
    MEASURE_TYPE_CODE           VARCHAR(30),
    TRADER_CODE                 VARCHAR(50),
    GUARANTEED_ENTITY           VARCHAR(10),

    REASON                      VARCHAR(1000),
    DEPENDS_ON                  VARCHAR(1000),              -- External dependency key(s)
    START_COBID                 NUMBER(38,0),               -- First COB date this template applies to
    END_COBID                   NUMBER(38,0),               -- Last COB date this template applies to
    CRON_EXPRESSION             VARCHAR(100),               -- e.g. '0 8 * * MON-FRI'
    IS_ACTIVE                   BOOLEAN      DEFAULT TRUE,
    CREATED_BY                  VARCHAR(50),
    CREATED_DATE                TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_ADJ_RECURRING PRIMARY KEY (TEMPLATE_ID)
)
COMMENT = 'Templates for recurring adjustments. External scheduler creates ADJ_HEADER entries from these when dependencies are met.';


-- ═══════════════════════════════════════════════════════════════════════════
-- 6. SEED DATA — ADJUSTMENTS_SETTINGS
-- ═══════════════════════════════════════════════════════════════════════════

DELETE FROM ADJUSTMENT_APP.ADJUSTMENTS_SETTINGS;

INSERT INTO ADJUSTMENT_APP.ADJUSTMENTS_SETTINGS
    (PROCESS_TYPE, FACT_TABLE, FACT_ADJUSTED_TABLE, FACT_TABLE_PK,
     ADJUSTMENTS_TABLE, ADJUSTMENTS_SUMMARY_TABLE,
     METRIC_NAME, METRIC_USD_NAME, IS_ACTIVE)
VALUES
    ('VaR',
     'FACT.VAR_MEASURES',
     'FACT.VAR_MEASURES_COMBINED',
     'ENTITY_CODE;BOOK_KEY;TRADE_KEY;COMMON_INSTRUMENT_KEY;COMMON_INSTRUMENT_FCD_KEY;CURRENCY_CODE;SOURCE_SYSTEM_CODE;VAR_SUBCOMPONENT_ID;SCENARIO_DATE_ID;IS_OFFICIAL_SOURCE',
     'FACT.VAR_MEASURES_ADJUSTMENT',
     'FACT.VAR_MEASURES_ADJUSTMENT_SUMMARY',
     'PNL_VECTOR_VALUE',
     'PNL_VECTOR_VALUE_IN_USD',
     TRUE),

    ('Stress',
     'FACT.STRESS_MEASURES',
     'FACT.STRESS_MEASURES_COMBINED',
     'ENTITY_KEY;BOOK_KEY;TRADE_KEY;COMMON_INSTRUMENT_KEY;COMMON_INSTRUMENT_FCD_KEY;TRADE_CURRENCY;SOURCE_SYSTEM_CODE;STRESS_SIMULATION_KEY;IS_OFFICIAL_SOURCE',
     'FACT.STRESS_MEASURES_ADJUSTMENT',
     'FACT.STRESS_MEASURES_ADJUSTMENT_SUMMARY',
     'SIMULATION_PL',
     'SIMULATION_PL_IN_USD',
     TRUE),

    ('Sensitivity',
     'FACT.SENSITIVITY_MEASURES',
     'FACT.SENSITIVITY_MEASURES_ADJUSTED',
     'COBID;ENTITY_KEY;SOURCE_SYSTEM_CODE;TRADE_KEY;BOOK_KEY;TENOR_CURRENCY_KEY;UNDERLYING_TENOR_CURRENCY_KEY;CURRENCY_CODE;CURVE_CURRENCY_KEY;COUNTERPARTY_KEY;MEASURE_TYPE_KEY;COMMON_INSTRUMENT_KEY;COMMON_INSTRUMENT_FCD_KEY;CURRENCY_PAIR_KEY;SKEW_KEY;IS_OFFICIAL_SOURCE;CORRELATION_KEY;REGION_AREA_KEY;PRODUCT_CATEGORY_ATTRIBUTES_KEY;TRADE_INDEX_KEY',
     'FACT.SENSITIVITY_MEASURES_ADJUSTMENT',
     'FACT.SENSITIVITY_MEASURES_ADJUSTMENT_SUMMARY',
     'MEASURE_VALUE',
     'MEASURE_VALUE_USD',
     TRUE),

    ('FRTB',
     'FACT.FRTBSA_SENSITIVITY_MEASURES',
     'FACT.FRTBSA_SENSITIVITY_MEASURES_ADJUSTED',
     'FRTBSA_SENSITIVITY_KEY',
     'FACT.FRTBSA_SENSITIVITY_MEASURES_ADJUSTMENT',
     NULL,
     'AMOUNT',
     'AMOUNT_IN_USD',
     TRUE),

    ('FRTBDRC',
     'FACT.FRTBSA_DRC_MEASURES',
     'FACT.FRTBSA_DRC_MEASURES_ADJUSTED',
     'FRTBSA_DRC_KEY',
     'FACT.FRTBSA_DRC_MEASURES_ADJUSTMENT',
     NULL,
     'JTD_LOSS',
     'JTD_LOSS_USD',
     TRUE),

    ('FRTBRRAO',
     'FACT.FRTBSA_RRAO_MEASURES',
     'FACT.FRTBSA_RRAO_MEASURES_ADJUSTED',
     'FRTBSA_RRAO_KEY',
     'FACT.FRTBSA_RRAO_MEASURES_ADJUSTMENT',
     NULL,
     'NOTIONAL_AMOUNT',
     'NOTIONAL_AMOUNT_USD',
     TRUE);


-- ═══════════════════════════════════════════════════════════════════════════
-- 7. ADJ_SIGNOFF_STATUS — COB sign-off lifecycle per scope
--
-- The FIRST sign-off for a COB/scope arrives from the upstream publish system
-- (file feed → BATCH.PUBLISH_VAR_SIGNOFF_STATUS / PUBLISH_SIGNOFF_STATUS_
-- EXCEPTION) and is synced into this table by SP_SYNC_SIGNOFF_STATUS
-- (10_sp_signoff_sync.sql). Re-opening and re-signing-off then happen in the
-- app:
--
--   SIGN_OFF_STATUS lifecycle:
--     (no row / OPEN)   → open: adjustments allowed
--     SIGNED_OFF        → blocked (source EXTERNAL feed, APP re-sign-off, or
--                         ADMIN manual)
--     REOPEN_REQUESTED  → still blocked; a re-open request awaits approval on
--                         the Approval Queue page (4-eyes: approver ≠ requester)
--     REOPENED          → open again; users add adjustments, then sign off
--                         from the app → back to SIGNED_OFF (source APP)
--
-- SP_SUBMIT_ADJUSTMENT blocks on SIGNED_OFF / REOPEN_REQUESTED, and also
-- checks the upstream BATCH tables live so the gate holds even before the
-- sync task has run. Every transition is logged to ADJ_SIGNOFF_HISTORY.
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR ALTER TABLE ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS (
    COBID                       NUMBER(38,0) NOT NULL,
    PROCESS_TYPE                VARCHAR(30)  NOT NULL,
    SIGN_OFF_STATUS             VARCHAR(30)  NOT NULL DEFAULT 'OPEN',   -- OPEN | SIGNED_OFF | REOPEN_REQUESTED | REOPENED
    SIGN_OFF_BY                 VARCHAR(50),
    SIGN_OFF_TIMESTAMP          TIMESTAMP_NTZ(9),
    CREATED_DATE                TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_DATE                TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),

    -- Lifecycle metadata
    SIGNOFF_SOURCE              VARCHAR(20),                 -- EXTERNAL | APP | ADMIN
    REOPEN_REQUESTED_BY         VARCHAR(50),
    REOPEN_REQUESTED_AT         TIMESTAMP_NTZ(9),
    REOPEN_REASON               VARCHAR(500) COLLATE 'en-ci',
    REOPEN_APPROVED_BY          VARCHAR(50),
    REOPEN_APPROVED_AT          TIMESTAMP_NTZ(9),

    -- Sign-off granularity is COBID + ENTITY + scope (matching the upstream
    -- publish feed). '*' = the whole scope at that COB (admin overrides and
    -- pre-migration rows). NOTE: new columns must stay at the END of this
    -- definition — CREATE OR ALTER can only APPEND columns, never reorder.
    ENTITY_CODE                 VARCHAR(50)  NOT NULL DEFAULT '*',

    -- Renamed constraint (was PK_ADJ_SIGNOFF_STATUS on COBID+PROCESS_TYPE):
    -- CREATE OR ALTER cannot change a constraint's columns in place, but it
    -- CAN drop the old-named one and add a new-named one. PKs are
    -- informational in Snowflake, so this is metadata-only.
    CONSTRAINT PK_ADJ_SIGNOFF_STATUS_ENT PRIMARY KEY (COBID, PROCESS_TYPE, ENTITY_CODE)
)
COMMENT = 'Sign-off lifecycle per COB + scope + entity (ENTITY_CODE = ''*'' means the whole scope). SIGNED_OFF / REOPEN_REQUESTED block new adjustments for that entity; REOPENED allows them again until the app re-sign-off. First sign-off synced from the upstream publish feed.';


-- ═══════════════════════════════════════════════════════════════════════════
-- 7a. ADJ_SIGNOFF_HISTORY — append-only audit of sign-off transitions
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR ALTER TABLE ADJUSTMENT_APP.ADJ_SIGNOFF_HISTORY (
    SIGNOFF_HISTORY_ID          NUMBER(38,0) NOT NULL AUTOINCREMENT,
    COBID                       NUMBER(38,0) NOT NULL,
    PROCESS_TYPE                VARCHAR(30)  NOT NULL,
    OLD_STATUS                  VARCHAR(30),
    NEW_STATUS                  VARCHAR(30)  NOT NULL,
    ACTION_BY                   VARCHAR(50),
    ACTION_AT                   TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    COMMENT                     VARCHAR(1000) COLLATE 'en-ci',
    -- At the END on purpose: CREATE OR ALTER can only APPEND columns, and an
    -- earlier revision of this table may already be deployed without it.
    ENTITY_CODE                 VARCHAR(50)  DEFAULT '*',

    CONSTRAINT PK_ADJ_SIGNOFF_HISTORY PRIMARY KEY (SIGNOFF_HISTORY_ID)
)
COMMENT = 'Append-only audit trail of every sign-off status transition (external sync, re-open request/approval/rejection, app re-sign-off, admin overrides).';



-- ═══════════════════════════════════════════════════════════════════════════
-- 7. ADJ_APPROVERS — Authorized approvers
--
-- Users listed here can approve/reject adjustments in the Approval Queue.
-- Scope-level control: NULL PROCESS_TYPE = can approve all scopes.
-- Self-approval is always blocked regardless of this table.
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR ALTER TABLE ADJUSTMENT_APP.ADJ_APPROVERS (
    APPROVER_ID                 NUMBER(38,0) NOT NULL AUTOINCREMENT,
    USERNAME                    VARCHAR(50)  NOT NULL,
    PROCESS_TYPE                VARCHAR(30),            -- NULL = all scopes
    IS_ACTIVE                   BOOLEAN      DEFAULT TRUE,
    ADDED_BY                    VARCHAR(50),
    ADDED_DATE                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_ADJ_APPROVERS PRIMARY KEY (APPROVER_ID)
)
COMMENT = 'Authorized approvers for the Approval Queue. NULL PROCESS_TYPE means the user can approve any scope. Self-approval is always blocked.';


-- ═══════════════════════════════════════════════════════════════════════════
-- 7b. ADJ_ADMINS — Users allowed to use the Admin page
--
-- The Admin page manages approvers, sign-off, and scope config — the controls
-- the 4-eyes workflow depends on — so access to it must itself be controlled.
-- While this table is EMPTY the app runs in bootstrap mode (page open to all,
-- with a prominent warning) so the first admin can be registered.
-- Deliberately NOT seeded and never wiped on redeploy: membership is
-- operational data owned by the admins themselves.
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR ALTER TABLE ADJUSTMENT_APP.ADJ_ADMINS (
    ADMIN_ID                    NUMBER(38,0) NOT NULL AUTOINCREMENT,
    USERNAME                    VARCHAR(50)  NOT NULL,
    IS_ACTIVE                   BOOLEAN      DEFAULT TRUE,
    ADDED_BY                    VARCHAR(50),
    ADDED_DATE                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_ADJ_ADMINS PRIMARY KEY (ADMIN_ID)
)
COMMENT = 'Users authorized to use the Admin page. Empty table = bootstrap mode (open access with warning) until the first admin is added. Not seeded; survives redeploys.';


-- ═══════════════════════════════════════════════════════════════════════════
-- 7c. NOTIFICATIONS — config switch, per-user preferences, send log
--
-- Email notifications are sent via a Snowflake email notification integration
-- (created by the DBA team — see docs/TICKET_email_notification_integration.md)
-- through SP_NOTIFY (11_sp_notify.sql). The whole feature is gated by the
-- NOTIFICATIONS_ENABLED switch below, so this can be deployed BEFORE the
-- integration exists and enabled from the Admin page once it does.
-- ═══════════════════════════════════════════════════════════════════════════

-- App-level key/value config. Seeded ONLY for missing keys — values are
-- admin-managed operational state and must survive redeploys.
CREATE OR ALTER TABLE ADJUSTMENT_APP.ADJ_APP_CONFIG (
    CONFIG_KEY                  VARCHAR(100) NOT NULL,
    CONFIG_VALUE                VARCHAR(500),
    DESCRIPTION                 VARCHAR(500),
    UPDATED_BY                  VARCHAR(50),
    UPDATED_AT                  TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_ADJ_APP_CONFIG PRIMARY KEY (CONFIG_KEY)
)
COMMENT = 'App-level configuration switches, managed on the Admin page. Seed inserts missing keys only — never overwrites values on redeploy.';

MERGE INTO ADJUSTMENT_APP.ADJ_APP_CONFIG t
USING (
    SELECT 'NOTIFICATIONS_ENABLED' AS CONFIG_KEY, 'false' AS CONFIG_VALUE,
           'Master switch for email notifications. Set to true once the DBA-created email notification integration exists.' AS DESCRIPTION
    UNION ALL
    SELECT 'EMAIL_INTEGRATION', 'ADJ_EMAIL_INT',
           'Name of the Snowflake email notification integration SP_NOTIFY sends through.'
    UNION ALL
    SELECT 'SIGNOFF_FEED_TABLE', 'BATCH.PUBLISH_SIGNOFF_STATUS',
           'Unified upstream sign-off feed (COBID, ENTITY_CODE, PROCESS_TYPE, SUB_TYPE, PUBLISH_STATUS, SIGNOFF_UPDATE_TIME). Adjust here if the migration lands under a different name.'
    UNION ALL
    SELECT 'SIGNOFF_FEED_ENABLED', 'true',
           'When true, SP_SUBMIT_ADJUSTMENT and SP_SYNC_SIGNOFF_STATUS consult the upstream feed table live. Set to false only while the unified feed table does not exist yet.'
) s
ON t.CONFIG_KEY = s.CONFIG_KEY
WHEN NOT MATCHED THEN INSERT (CONFIG_KEY, CONFIG_VALUE, DESCRIPTION)
VALUES (s.CONFIG_KEY, s.CONFIG_VALUE, s.DESCRIPTION);

-- One-time correction: SIGNOFF_FEED_TABLE was first seeded with a misspelled
-- default (PUBLISH_SIGNOF_STATUS, single F). The seed MERGE never overwrites
-- existing values, so fix ONLY that known-bad value here — an admin-set
-- custom table name is preserved.
UPDATE ADJUSTMENT_APP.ADJ_APP_CONFIG
SET CONFIG_VALUE = 'BATCH.PUBLISH_SIGNOFF_STATUS',
    UPDATED_BY   = 'DEPLOY',
    UPDATED_AT   = CURRENT_TIMESTAMP()
WHERE CONFIG_KEY = 'SIGNOFF_FEED_TABLE'
  AND CONFIG_VALUE = 'BATCH.PUBLISH_SIGNOF_STATUS';


-- Who receives what. Recipients must be Snowflake users of this account with
-- a VERIFIED profile email — Snowflake refuses delivery otherwise.
CREATE OR ALTER TABLE ADJUSTMENT_APP.ADJ_NOTIFICATION_PREFS (
    PREF_ID                     NUMBER(38,0) NOT NULL AUTOINCREMENT,
    USERNAME                    VARCHAR(50)  NOT NULL,   -- Snowflake username (as in ADJ_HEADER.USERNAME)
    EMAIL                       VARCHAR(320) NOT NULL,   -- verified account-user email
    NOTIFY_MY_OUTCOMES          BOOLEAN      DEFAULT TRUE,   -- my adjustments Processed / Failed
    NOTIFY_APPROVALS            BOOLEAN      DEFAULT FALSE,  -- approver events: pending approvals, COB re-open requests
    IS_ACTIVE                   BOOLEAN      DEFAULT TRUE,
    ADDED_BY                    VARCHAR(50),
    ADDED_DATE                  TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_ADJ_NOTIFICATION_PREFS PRIMARY KEY (PREF_ID)
)
COMMENT = 'Per-user notification opt-ins, managed on the Admin page. Not seeded; survives redeploys.';


-- Every send attempt (including skips while the feature is disabled) — the
-- audit/debug trail for notifications.
CREATE OR ALTER TABLE ADJUSTMENT_APP.ADJ_NOTIFICATION_LOG (
    NOTIFICATION_ID             NUMBER(38,0) NOT NULL AUTOINCREMENT,
    EVENT_TYPE                  VARCHAR(50),
    RECIPIENTS                  VARCHAR(2000),
    SUBJECT                     VARCHAR(500),
    STATUS                      VARCHAR(30),             -- SENT | FAILED | SKIPPED_DISABLED | NO_RECIPIENTS
    ERROR                       VARCHAR(1000),
    CREATED_AT                  TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_ADJ_NOTIFICATION_LOG PRIMARY KEY (NOTIFICATION_ID)
)
COMMENT = 'Append-only log of every notification attempt from SP_NOTIFY.';


-- ═══════════════════════════════════════════════════════════════════════════
-- MIGRATION — add START_DATE to existing ADJ_HEADER
-- ═══════════════════════════════════════════════════════════════════════════
ALTER TABLE ADJUSTMENT_APP.ADJ_HEADER
    ADD COLUMN IF NOT EXISTS START_DATE TIMESTAMP_NTZ(9)
    COMMENT 'Set when the adjustment transitions to Running. Distinct from PROCESS_DATE (end).';

-- Drop the legacy ADJUSTMENT_BASE_TABLE config column: every scope now reads
-- adjustments from ADJUSTMENT_APP.ADJ_HEADER (hardcoded in SP_PROCESS_ADJUSTMENT),
-- so the per-scope base-table pointer is dead config.
ALTER TABLE ADJUSTMENT_APP.ADJUSTMENTS_SETTINGS
    DROP COLUMN IF EXISTS ADJUSTMENT_BASE_TABLE;

-- ═══════════════════════════════════════════════════════════════════════════
-- EROL_PROCESS_LOG — REAL-TIME per-statement diagnostics for engine runs
-- ALL SP_PROCESS_ADJUSTMENT paths (EntityRoll, Direct, Scale/Flatten/Roll)
-- log every dynamically-built statement here: a 'RUNNING' row with the exact
-- SQL_TEXT is written BEFORE each heavy statement and updated to 'DONE' after —
-- so while a slow step runs you can query this table from another session and
-- see which step is in-flight and for how long, and after a failure you can
-- replay the precise command that ran. Each row carries wall-clock duration,
-- rows affected, and the Snowflake QUERY_ID (join to QUERY_HISTORY for
-- partitions scanned / spill). Statements inside explicit transactions become
-- visible only at COMMIT (a failed summary rebuild is re-logged as FAILED
-- after its rollback). Batch-level scale rows leave ENTITY_CODE /
-- ADJUSTMENT_ID NULL. Append-only.
--     SELECT * FROM ADJUSTMENT_APP.VW_EROL_PROCESS_LOG;   -- report view
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR ALTER TABLE ADJUSTMENT_APP.EROL_PROCESS_LOG (
    LOGGED_AT       TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    RUN_LOG_ID      NUMBER(38,0),
    PROCESS_TYPE    VARCHAR,
    COBID           NUMBER(38,0),
    SOURCE_COBID    NUMBER(38,0),
    ENTITY_CODE     VARCHAR,
    ADJUSTMENT_ID   NUMBER(38,0),
    STEP_SEQ        NUMBER(38,0),
    STEP_NAME       VARCHAR,
    STATUS          VARCHAR,            -- RUNNING | DONE | FAILED
    STARTED_AT      TIMESTAMP_NTZ(9),
    ENDED_AT        TIMESTAMP_NTZ(9),
    QUERY_ID        VARCHAR,
    ROWS_AFFECTED   NUMBER(38,0),
    DURATION_SEC    FLOAT,
    SQL_TEXT        VARCHAR
)
COMMENT = 'Real-time per-statement timing/rows/query_id for Entity Roll runs (SP_PROCESS_ADJUSTMENT).';


-- ═══════════════════════════════════════════════════════════════════════════
-- ADJ_CATEGORY — managed list of adjustment categories for the New Adjustment
-- page. Seed-only (edit here + redeploy to change). Seed is idempotent.
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR ALTER TABLE ADJUSTMENT_APP.ADJ_CATEGORY (
    CATEGORY_NAME  VARCHAR(100) NOT NULL,          -- stored value + display label
    IS_ACTIVE      BOOLEAN          DEFAULT TRUE,
    SORT_ORDER     NUMBER(38,0),
    CREATED_DATE   TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_ADJ_CATEGORY PRIMARY KEY (CATEGORY_NAME)
)
COMMENT = 'Managed list of adjustment categories for the New Adjustment page.';

DELETE FROM ADJUSTMENT_APP.ADJ_CATEGORY;
INSERT INTO ADJUSTMENT_APP.ADJ_CATEGORY (CATEGORY_NAME, SORT_ORDER) VALUES
    ('Adjusted by MRM Upload', 10),
    ('Bank Holiday', 20),
    ('Booking Error', 30),
    ('IT-Other', 40),
    ('Late Booking', 50),
    ('Market Data Error', 60),
    ('Missing Trade', 70),
    ('Model Limitation', 80),
    ('Murex System Limitation', 90),
    ('New Business Issue', 100),
    ('PRO Cash Adjustment', 110),
    ('QuantServer System Issue', 120),
    ('QuIC System Limitation', 130),
    ('Raptor Reporting Issue', 140),
    ('Reference Data Error', 150),
    ('Structured Trade Issue', 160),
    ('Time Series Issue', 170),
    ('Valuation Source Issue', 180),
    ('VaR Window Issue', 190);

-- ═══════════════════════════════════════════════════════════════════════════
-- 8. VERIFY
-- ═══════════════════════════════════════════════════════════════════════════
SELECT 'ADJ_HEADER' AS OBJECT, COUNT(*) AS ROW_COUNT FROM ADJUSTMENT_APP.ADJ_HEADER
UNION ALL SELECT 'ADJ_LINE_ITEM', COUNT(*) FROM ADJUSTMENT_APP.ADJ_LINE_ITEM
UNION ALL SELECT 'ADJ_STATUS_HISTORY', COUNT(*) FROM ADJUSTMENT_APP.ADJ_STATUS_HISTORY
UNION ALL SELECT 'ADJUSTMENTS_SETTINGS', COUNT(*) FROM ADJUSTMENT_APP.ADJUSTMENTS_SETTINGS
UNION ALL SELECT 'ADJ_RECURRING_TEMPLATE', COUNT(*) FROM ADJUSTMENT_APP.ADJ_RECURRING_TEMPLATE
UNION ALL SELECT 'ADJ_SIGNOFF_STATUS', COUNT(*) FROM ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS
UNION ALL SELECT 'ADJ_SIGNOFF_HISTORY', COUNT(*) FROM ADJUSTMENT_APP.ADJ_SIGNOFF_HISTORY
UNION ALL SELECT 'ADJ_APPROVERS', COUNT(*) FROM ADJUSTMENT_APP.ADJ_APPROVERS
UNION ALL SELECT 'ADJ_ADMINS', COUNT(*) FROM ADJUSTMENT_APP.ADJ_ADMINS
UNION ALL SELECT 'ADJ_APP_CONFIG', COUNT(*) FROM ADJUSTMENT_APP.ADJ_APP_CONFIG
UNION ALL SELECT 'ADJ_NOTIFICATION_PREFS', COUNT(*) FROM ADJUSTMENT_APP.ADJ_NOTIFICATION_PREFS
UNION ALL SELECT 'ADJ_NOTIFICATION_LOG', COUNT(*) FROM ADJUSTMENT_APP.ADJ_NOTIFICATION_LOG
UNION ALL SELECT 'ADJ_CATEGORY', COUNT(*) FROM ADJUSTMENT_APP.ADJ_CATEGORY;
