-- =============================================================================
-- 02_TABLES.SQL
-- Core tables: fact table, adjustment header, adjustment line items, audit log
-- =============================================================================

USE DATABASE ADJUSTMENT_DB;
USE WAREHOUSE ADJUSTMENT_WH;

-- ═════════════════════════════════════════════════════════════════════════════
-- 1. FACT TABLE (Source of Truth — Immutable)
-- ═════════════════════════════════════════════════════════════════════════════
-- This represents your existing fact table. Adjust columns to match your schema.
-- The adjustment engine treats this as READ-ONLY.

CREATE TABLE IF NOT EXISTS FACT.FACT_TABLE (
    -- Surrogate key
    FACT_ID             NUMBER AUTOINCREMENT PRIMARY KEY,

    -- Business / Reporting Date
    BUSINESS_DATE       DATE            NOT NULL,

    -- Dimension Keys (add/modify to match your actual dimensions)
    ENTITY_KEY          VARCHAR(50)     NOT NULL,
    PRODUCT_KEY         VARCHAR(50)     NOT NULL,
    ACCOUNT_KEY         VARCHAR(50)     NOT NULL,
    CURRENCY_KEY        VARCHAR(10)     NOT NULL,
    COUNTERPARTY_KEY    VARCHAR(50),
    COST_CENTER_KEY     VARCHAR(50),

    -- Measures (add/modify to match your actual measures)
    AMOUNT              NUMBER(28, 8)   NOT NULL DEFAULT 0,
    QUANTITY            NUMBER(28, 8)   DEFAULT 0,
    NOTIONAL            NUMBER(28, 8)   DEFAULT 0,

    -- Metadata
    SOURCE_SYSTEM       VARCHAR(100),
    LOAD_TIMESTAMP      TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),

    -- Cluster key hint for performance
    CLUSTER BY (BUSINESS_DATE, ENTITY_KEY, PRODUCT_KEY)
)
COMMENT = 'Immutable source fact table — adjustment engine does not modify this table';


-- ═════════════════════════════════════════════════════════════════════════════
-- 2. ADJUSTMENT HEADER (Hybrid Table for OLTP writes from Streamlit)
-- ═════════════════════════════════════════════════════════════════════════════
-- Each adjustment request is one header with 1..N line items.
-- Uses Hybrid Table for row-level ACID transactions during interactive use.

CREATE OR REPLACE HYBRID TABLE CORE.ADJ_HEADER (
    ADJ_ID              NUMBER AUTOINCREMENT PRIMARY KEY,

    -- Adjustment metadata
    ADJ_TYPE            VARCHAR(20)     NOT NULL,  -- 'FLATTEN', 'SCALE', 'ROLL'
    ADJ_STATUS          VARCHAR(20)     NOT NULL DEFAULT 'DRAFT',
        -- DRAFT → PENDING_APPROVAL → APPROVED → APPLIED
        --                          → REJECTED
        --          APPLIED         → REVERSED

    -- Parameters
    SCALE_FACTOR        NUMBER(18, 8)   DEFAULT 1,      -- used by SCALE and ROLL
    ROLL_SOURCE_DATE    DATE,                            -- used by ROLL: where to copy from
    TARGET_DATE         DATE            NOT NULL,        -- business date being adjusted

    -- Filter criteria stored as JSON for auditability & replay
    FILTER_CRITERIA     VARIANT         NOT NULL,
    -- Example: {"entity_key": "US_BANK", "product_key": "FX_SPOT", "currency_key": "USD"}

    -- Affected row count (populated on apply)
    AFFECTED_ROWS       NUMBER          DEFAULT 0,

    -- Business justification (required for approval)
    BUSINESS_REASON     VARCHAR(2000)   NOT NULL,
    TICKET_REFERENCE    VARCHAR(100),       -- JIRA / ServiceNow ticket

    -- User tracking
    CREATED_BY          VARCHAR(256)    NOT NULL DEFAULT CURRENT_USER(),
    CREATED_AT          TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    APPROVED_BY         VARCHAR(256),
    APPROVED_AT         TIMESTAMP_NTZ,
    APPLIED_BY          VARCHAR(256),
    APPLIED_AT          TIMESTAMP_NTZ,
    REVERSED_BY         VARCHAR(256),
    REVERSED_AT         TIMESTAMP_NTZ,

    -- AI-generated summary (populated by Cortex)
    AI_IMPACT_SUMMARY   VARCHAR(4000),

    -- Constraints
    CONSTRAINT CHK_ADJ_TYPE CHECK (ADJ_TYPE IN ('FLATTEN', 'SCALE', 'ROLL')),
    CONSTRAINT CHK_ADJ_STATUS CHECK (ADJ_STATUS IN ('DRAFT', 'PENDING_APPROVAL', 'APPROVED', 'REJECTED', 'APPLIED', 'REVERSED')),

    INDEX IDX_ADJ_STATUS (ADJ_STATUS),
    INDEX IDX_ADJ_TARGET_DATE (TARGET_DATE),
    INDEX IDX_ADJ_CREATED_BY (CREATED_BY)
)
COMMENT = 'Adjustment request headers — one per adjustment action';


-- ═════════════════════════════════════════════════════════════════════════════
-- 3. ADJUSTMENT LINE ITEM (Delta values per dimension key)
-- ═════════════════════════════════════════════════════════════════════════════
-- Stores the calculated delta for each fact row affected by the adjustment.
-- When UNION ALL + SUM with the original fact, produces the adjusted result.

CREATE OR REPLACE HYBRID TABLE CORE.ADJ_LINE_ITEM (
    LINE_ID             NUMBER AUTOINCREMENT PRIMARY KEY,
    ADJ_ID              NUMBER          NOT NULL,

    -- Same dimension keys as fact table
    BUSINESS_DATE       DATE            NOT NULL,
    ENTITY_KEY          VARCHAR(50)     NOT NULL,
    PRODUCT_KEY         VARCHAR(50)     NOT NULL,
    ACCOUNT_KEY         VARCHAR(50)     NOT NULL,
    CURRENCY_KEY        VARCHAR(10)     NOT NULL,
    COUNTERPARTY_KEY    VARCHAR(50),
    COST_CENTER_KEY     VARCHAR(50),

    -- Delta values (these are the OFFSETS, not absolute values)
    AMOUNT_DELTA        NUMBER(28, 8)   NOT NULL DEFAULT 0,
    QUANTITY_DELTA      NUMBER(28, 8)   DEFAULT 0,
    NOTIONAL_DELTA      NUMBER(28, 8)   DEFAULT 0,

    -- Original values snapshot (for audit & reversal)
    AMOUNT_ORIGINAL     NUMBER(28, 8),
    QUANTITY_ORIGINAL   NUMBER(28, 8),
    NOTIONAL_ORIGINAL   NUMBER(28, 8),

    -- For ROLL: the source values copied from
    AMOUNT_SOURCE       NUMBER(28, 8),
    QUANTITY_SOURCE     NUMBER(28, 8),
    NOTIONAL_SOURCE     NUMBER(28, 8),

    -- Reference back to original fact row
    FACT_ID             NUMBER,

    -- Metadata
    CREATED_AT          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),

    -- Foreign key to header
    FOREIGN KEY (ADJ_ID) REFERENCES CORE.ADJ_HEADER(ADJ_ID),

    INDEX IDX_LINE_ADJ_ID (ADJ_ID),
    INDEX IDX_LINE_BIZ_DATE (BUSINESS_DATE),
    INDEX IDX_LINE_ENTITY (ENTITY_KEY)
)
COMMENT = 'Adjustment delta line items — one per affected fact row per adjustment';


-- ═════════════════════════════════════════════════════════════════════════════
-- 4. ADJUSTMENT STATUS HISTORY (Full audit trail of state transitions)
-- ═════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS CORE.ADJ_STATUS_HISTORY (
    HISTORY_ID          NUMBER AUTOINCREMENT,
    ADJ_ID              NUMBER          NOT NULL,
    OLD_STATUS          VARCHAR(20),
    NEW_STATUS          VARCHAR(20)     NOT NULL,
    CHANGED_BY          VARCHAR(256)    NOT NULL DEFAULT CURRENT_USER(),
    CHANGED_AT          TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    COMMENT             VARCHAR(2000),

    CLUSTER BY (ADJ_ID, CHANGED_AT)
)
COMMENT = 'Complete audit trail of every status transition for every adjustment';


-- ═════════════════════════════════════════════════════════════════════════════
-- 5. ADJUSTMENT CONFIGURATION (lookup table for dimension metadata)
-- ═════════════════════════════════════════════════════════════════════════════
-- Drives the Streamlit filter UI — defines which columns are filterable
-- and their display labels.

CREATE TABLE IF NOT EXISTS CORE.ADJ_DIMENSION_CONFIG (
    CONFIG_ID           NUMBER AUTOINCREMENT,
    COLUMN_NAME         VARCHAR(100)    NOT NULL,
    DISPLAY_LABEL       VARCHAR(200)    NOT NULL,
    DATA_TYPE           VARCHAR(50)     NOT NULL DEFAULT 'VARCHAR',
    IS_FILTERABLE       BOOLEAN         NOT NULL DEFAULT TRUE,
    IS_REQUIRED         BOOLEAN         NOT NULL DEFAULT FALSE,
    SORT_ORDER          NUMBER          DEFAULT 0,
    DISTINCT_VALUES_SQL VARCHAR(4000),  -- SQL to fetch distinct values for dropdown
    ACTIVE              BOOLEAN         NOT NULL DEFAULT TRUE
)
COMMENT = 'Configuration table that drives the Streamlit filter UI';

-- Seed dimension config
INSERT INTO CORE.ADJ_DIMENSION_CONFIG (COLUMN_NAME, DISPLAY_LABEL, DATA_TYPE, IS_FILTERABLE, IS_REQUIRED, SORT_ORDER, DISTINCT_VALUES_SQL)
VALUES
    ('BUSINESS_DATE',    'Business Date',    'DATE',    TRUE,  TRUE,  1, 'SELECT DISTINCT BUSINESS_DATE FROM FACT.FACT_TABLE ORDER BY 1 DESC'),
    ('ENTITY_KEY',       'Entity',           'VARCHAR', TRUE,  FALSE, 2, 'SELECT DISTINCT ENTITY_KEY FROM FACT.FACT_TABLE ORDER BY 1'),
    ('PRODUCT_KEY',      'Product',          'VARCHAR', TRUE,  FALSE, 3, 'SELECT DISTINCT PRODUCT_KEY FROM FACT.FACT_TABLE ORDER BY 1'),
    ('ACCOUNT_KEY',      'Account',          'VARCHAR', TRUE,  FALSE, 4, 'SELECT DISTINCT ACCOUNT_KEY FROM FACT.FACT_TABLE ORDER BY 1'),
    ('CURRENCY_KEY',     'Currency',         'VARCHAR', TRUE,  FALSE, 5, 'SELECT DISTINCT CURRENCY_KEY FROM FACT.FACT_TABLE ORDER BY 1'),
    ('COUNTERPARTY_KEY', 'Counterparty',     'VARCHAR', TRUE,  FALSE, 6, 'SELECT DISTINCT COUNTERPARTY_KEY FROM FACT.FACT_TABLE WHERE COUNTERPARTY_KEY IS NOT NULL ORDER BY 1'),
    ('COST_CENTER_KEY',  'Cost Center',      'VARCHAR', TRUE,  FALSE, 7, 'SELECT DISTINCT COST_CENTER_KEY FROM FACT.FACT_TABLE WHERE COST_CENTER_KEY IS NOT NULL ORDER BY 1');


-- ═════════════════════════════════════════════════════════════════════════════
-- 6. MEASURE CONFIGURATION (which measures can be adjusted)
-- ═════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS CORE.ADJ_MEASURE_CONFIG (
    MEASURE_ID          NUMBER AUTOINCREMENT,
    COLUMN_NAME         VARCHAR(100)    NOT NULL,
    DELTA_COLUMN_NAME   VARCHAR(100)    NOT NULL,
    DISPLAY_LABEL       VARCHAR(200)    NOT NULL,
    IS_ADJUSTABLE       BOOLEAN         NOT NULL DEFAULT TRUE,
    SORT_ORDER          NUMBER          DEFAULT 0,
    ACTIVE              BOOLEAN         NOT NULL DEFAULT TRUE
)
COMMENT = 'Configuration table defining which measures can be adjusted';

INSERT INTO CORE.ADJ_MEASURE_CONFIG (COLUMN_NAME, DELTA_COLUMN_NAME, DISPLAY_LABEL, IS_ADJUSTABLE, SORT_ORDER)
VALUES
    ('AMOUNT',   'AMOUNT_DELTA',   'Amount',   TRUE, 1),
    ('QUANTITY', 'QUANTITY_DELTA', 'Quantity', TRUE, 2),
    ('NOTIONAL', 'NOTIONAL_DELTA', 'Notional', TRUE, 3);
