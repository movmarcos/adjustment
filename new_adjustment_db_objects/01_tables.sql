-- =============================================================================
-- 01_TABLES.SQL
-- All tables for the unified adjustment process.
-- Source of truth: ADJUSTMENT_APP.ADJ_HEADER is where every adjustment starts.
-- Input: Streamlit on Snowflake (no files, no staging tables).
--
-- All objects live in the ADJUSTMENT_APP schema.
-- =============================================================================

USE DATABASE DVLP_RAPTOR_NEWADJ;
CREATE SCHEMA IF NOT EXISTS ADJUSTMENT_APP;
USE SCHEMA ADJUSTMENT_APP;

-- ═══════════════════════════════════════════════════════════════════════════
-- PREREQUISITE (run once by ACCOUNTADMIN):
--   GRANT READ SESSION ON ACCOUNT TO ROLE DVLP_RAPTOR_OWNER;
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

CREATE OR MODIFY TABLE ADJUSTMENT_APP.ADJ_HEADER (
    -- Identity
    ADJ_ID                      NUMBER(38,0) NOT NULL AUTOINCREMENT START 200000 INCREMENT 1 ORDER,
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

    -- Workflow status
    RUN_STATUS                  VARCHAR(30)  COLLATE 'en-ci' DEFAULT 'Pending',
    IS_POSITIVE_ADJUSTMENT      BOOLEAN      DEFAULT TRUE,       -- FALSE = superseded / inactive
    PROCESS_DATE                TIMESTAMP_NTZ(9),
    RECORD_COUNT                NUMBER(38,0),
    ERRORMESSAGE                VARCHAR(1000) COLLATE 'en-ci',

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

CREATE OR MODIFY TABLE ADJUSTMENT_APP.ADJ_LINE_ITEM (
    LINE_ID                     NUMBER(38,0) NOT NULL AUTOINCREMENT,
    ADJ_ID                      NUMBER(38,0) NOT NULL,  -- FK to ADJ_HEADER

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
-- 3. ADJ_STATUS_HISTORY — Full audit trail of every status change
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR MODIFY TABLE ADJUSTMENT_APP.ADJ_STATUS_HISTORY (
    HISTORY_ID                  NUMBER(38,0) NOT NULL AUTOINCREMENT,
    ADJ_ID                      NUMBER(38,0) NOT NULL,
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

CREATE OR MODIFY TABLE ADJUSTMENT_APP.ADJUSTMENTS_SETTINGS (
    PROCESS_TYPE                VARCHAR(30)  NOT NULL,
    FACT_TABLE                  VARCHAR(200) NOT NULL,       -- Source fact table to read from
    FACT_ADJUSTED_TABLE         VARCHAR(200),                -- Source for cross-COB (existing adj)
    FACT_TABLE_PK               VARCHAR(500) NOT NULL,       -- PK columns (;-separated)
    ADJUSTMENTS_TABLE           VARCHAR(200) NOT NULL,       -- Target adjustment detail table
    ADJUSTMENTS_SUMMARY_TABLE   VARCHAR(200),                -- Target summary (NULL = no summary)
    ADJUSTMENT_BASE_TABLE       VARCHAR(200) NOT NULL DEFAULT 'ADJUSTMENT_APP.ADJ_HEADER',
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

CREATE OR MODIFY TABLE ADJUSTMENT_APP.ADJ_RECURRING_TEMPLATE (
    TEMPLATE_ID                 NUMBER(38,0) NOT NULL AUTOINCREMENT,
    TEMPLATE_NAME               VARCHAR(200) NOT NULL,
    PROCESS_TYPE                VARCHAR(30)  NOT NULL,
    ADJUSTMENT_TYPE             VARCHAR(20)  NOT NULL,
    ADJUSTMENT_ACTION           VARCHAR(10)  NOT NULL,
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
     ADJUSTMENTS_TABLE, ADJUSTMENTS_SUMMARY_TABLE, ADJUSTMENT_BASE_TABLE,
     METRIC_NAME, METRIC_USD_NAME, IS_ACTIVE)
VALUES
    ('VaR',
     'FACT.VAR_MEASURES',
     'FACT.VAR_MEASURES_ADJUSTMENT',
     'ENTITY_CODE;BOOK_KEY;TRADE_KEY;COMMON_INSTRUMENT_KEY;COMMON_INSTRUMENT_FCD_KEY;CURRENCY_CODE;SOURCE_SYSTEM_CODE;VAR_SUBCOMPONENT_ID;SCENARIO_DATE_ID;IS_OFFICIAL_SOURCE',
     'FACT.VAR_MEASURES_ADJUSTMENT',
     'FACT.VAR_MEASURES_ADJUSTMENT_SUMMARY',
     'ADJUSTMENT_APP.ADJ_HEADER',
     'PNL_VECTOR_VALUE',
     'PNL_VECTOR_VALUE_IN_USD',
     TRUE),

    ('Stress',
     'FACT.STRESS_MEASURES',
     'FACT.STRESS_MEASURES_ADJUSTMENT',
     'ENTITY_KEY;BOOK_KEY;TRADE_KEY;COMMON_INSTRUMENT_KEY;COMMON_INSTRUMENT_FCD_KEY;CURRENCY_CODE;SOURCE_SYSTEM_CODE;STRESS_SIMULATION_KEY;IS_OFFICIAL_SOURCE',
     'FACT.STRESS_MEASURES_ADJUSTMENT',
     'FACT.STRESS_MEASURES_ADJUSTMENT_SUMMARY',
     'ADJUSTMENT_APP.ADJ_HEADER',
     'SIMULATION_PL',
     'SIMULATION_PL_IN_USD',
     TRUE),

    ('ES',
     'FACT.ES_MEASURES',
     'FACT.ES_MEASURES_ADJUSTMENT',
     'ENTITY_KEY;BOOK_KEY;TRADE_KEY;COMMON_INSTRUMENT_KEY;COMMON_INSTRUMENT_FCD_KEY;CURRENCY_CODE;SOURCE_SYSTEM_CODE;VAR_SUBCOMPONENT_ID;SCENARIO_DATE_ID;IS_OFFICIAL_SOURCE',
     'FACT.ES_MEASURES_ADJUSTMENT',
     'FACT.ES_MEASURES_ADJUSTMENT_SUMMARY',
     'ADJUSTMENT_APP.ADJ_HEADER',
     'PNL_VECTOR_VALUE',
     'PNL_VECTOR_VALUE_IN_USD',
     FALSE),

    ('Sensitivity',
     'FACT.SENSITIVITY_MEASURES',
     'FACT.SENSITIVITY_MEASURES_ADJUSTED',
     'COBID;ENTITY_KEY;SOURCE_SYSTEM_CODE;TRADE_KEY;BOOK_KEY;TENOR_CURRENCY_KEY;UNDERLYING_TENOR_CURRENCY_KEY;CURRENCY_CODE;CURVE_CURRENCY_KEY;COUNTERPARTY_KEY;MEASURE_TYPE_KEY;COMMON_INSTRUMENT_KEY;COMMON_INSTRUMENT_FCD_KEY;CURRENCY_PAIR_KEY;SKEW_KEY;IS_OFFICIAL_SOURCE;CORRELATION_KEY;REGION_AREA_KEY;PRODUCT_CATEGORY_ATTRIBUTES_KEY;TRADE_INDEX_KEY',
     'FACT.SENSITIVITY_MEASURES_ADJUSTMENT',
     'FACT.SENSITIVITY_MEASURES_ADJUSTMENT_SUMMARY',
     'ADJUSTMENT_APP.ADJUSTMENTS_BASE_SENSITIVITY',
     'MEASURE_VALUE',
     'MEASURE_VALUE_USD',
     TRUE),

    ('FRTB',
     'FACT.FRTBSA_SENSITIVITY_MEASURES',
     'FACT.FRTBSA_SENSITIVITY_MEASURES_ADJUSTED',
     'FRTBSA_SENSITIVITY_KEY',
     'FACT.FRTBSA_SENSITIVITY_MEASURES_ADJUSTMENT',
     NULL,
     'ADJUSTMENT_APP.ADJUSTMENTS_BASE_FRTB',
     'AMOUNT',
     'AMOUNT_IN_USD',
     TRUE),

    ('FRTBDRC',
     'FACT.FRTBSA_DRC_MEASURES',
     'FACT.FRTBSA_DRC_MEASURES_ADJUSTED',
     'FRTBSA_DRC_KEY',
     'FACT.FRTBSA_DRC_MEASURES_ADJUSTMENT',
     NULL,
     'ADJUSTMENT_APP.ADJUSTMENTS_BASE_FRTB',
     'JTD_LOSS',
     'JTD_LOSS_USD',
     TRUE),

    ('FRTBRRAO',
     'FACT.FRTBSA_RRAO_MEASURES',
     'FACT.FRTBSA_RRAO_MEASURES_ADJUSTED',
     'FRTBSA_RRAO_KEY',
     'FACT.FRTBSA_RRAO_MEASURES_ADJUSTMENT',
     NULL,
     'ADJUSTMENT_APP.ADJUSTMENTS_BASE_FRTB',
     'NOTIONAL_AMOUNT',
     'NOTIONAL_AMOUNT_USD',
     TRUE);


-- ═══════════════════════════════════════════════════════════════════════════
-- 7. ADJ_SIGNOFF_STATUS — Self-contained COB sign-off tracking
--
-- Tracks whether a COB/scope is signed off (no more adjustments allowed).
-- Managed via the Admin page in Streamlit.
-- VW_SIGNOFF_STATUS reads from this table.
-- SP_SUBMIT_ADJUSTMENT checks this before allowing submissions.
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR MODIFY TABLE ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS (
    COBID                       NUMBER(38,0) NOT NULL,
    PROCESS_TYPE                VARCHAR(30)  NOT NULL,
    SIGN_OFF_STATUS             VARCHAR(30)  NOT NULL DEFAULT 'OPEN',   -- OPEN or SIGNED_OFF
    SIGN_OFF_BY                 VARCHAR(50),
    SIGN_OFF_TIMESTAMP          TIMESTAMP_NTZ(9),
    CREATED_DATE                TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_DATE                TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_ADJ_SIGNOFF_STATUS PRIMARY KEY (COBID, PROCESS_TYPE)
)
COMMENT = 'COB sign-off status per scope. SIGN_OFF_STATUS = SIGNED_OFF means no new adjustments allowed. Managed via Admin page.';


-- ═══════════════════════════════════════════════════════════════════════════
-- 8. VERIFY
-- ═══════════════════════════════════════════════════════════════════════════
SELECT 'ADJ_HEADER' AS OBJECT, COUNT(*) AS ROW_COUNT FROM ADJUSTMENT_APP.ADJ_HEADER
UNION ALL SELECT 'ADJ_LINE_ITEM', COUNT(*) FROM ADJUSTMENT_APP.ADJ_LINE_ITEM
UNION ALL SELECT 'ADJ_STATUS_HISTORY', COUNT(*) FROM ADJUSTMENT_APP.ADJ_STATUS_HISTORY
UNION ALL SELECT 'ADJUSTMENTS_SETTINGS', COUNT(*) FROM ADJUSTMENT_APP.ADJUSTMENTS_SETTINGS
UNION ALL SELECT 'ADJ_RECURRING_TEMPLATE', COUNT(*) FROM ADJUSTMENT_APP.ADJ_RECURRING_TEMPLATE
UNION ALL SELECT 'ADJ_SIGNOFF_STATUS', COUNT(*) FROM ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS;
