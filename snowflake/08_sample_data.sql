-- =============================================================================
-- 08_SAMPLE_DATA.SQL
-- Sample fact data and test adjustments for development/testing
-- =============================================================================

USE DATABASE ADJUSTMENT_DB;
USE WAREHOUSE ADJUSTMENT_WH;

-- ═════════════════════════════════════════════════════════════════════════════
-- 1. SAMPLE FACT DATA (2 days × multiple dimension combinations)
-- ═════════════════════════════════════════════════════════════════════════════

TRUNCATE TABLE IF EXISTS FACT.FACT_TABLE;

INSERT INTO FACT.FACT_TABLE
    (BUSINESS_DATE, ENTITY_KEY, PRODUCT_KEY, ACCOUNT_KEY, CURRENCY_KEY,
     COUNTERPARTY_KEY, COST_CENTER_KEY, AMOUNT, QUANTITY, NOTIONAL, SOURCE_SYSTEM)
VALUES
    -- Day 1: 2026-02-23
    ('2026-02-23', 'US_BANK',   'FX_SPOT',     'TRADING_PNL',  'USD', 'CPTY_A', 'CC_100', 1500000.00,   100, 15000000.00, 'MUREX'),
    ('2026-02-23', 'US_BANK',   'FX_SPOT',     'TRADING_PNL',  'EUR', 'CPTY_B', 'CC_100', 2300000.00,   200, 23000000.00, 'MUREX'),
    ('2026-02-23', 'US_BANK',   'FX_FORWARD',  'TRADING_PNL',  'USD', 'CPTY_A', 'CC_100',  750000.00,    50,  7500000.00, 'MUREX'),
    ('2026-02-23', 'US_BANK',   'FX_FORWARD',  'TRADING_PNL',  'GBP', 'CPTY_C', 'CC_200',  -320000.00,   30, -3200000.00, 'MUREX'),
    ('2026-02-23', 'US_BANK',   'IR_SWAP',     'HEDGE_PNL',    'USD', 'CPTY_D', 'CC_300', 5000000.00,  1000, 50000000.00, 'CALYPSO'),
    ('2026-02-23', 'EU_BANK',   'FX_SPOT',     'TRADING_PNL',  'EUR', 'CPTY_E', 'CC_400', 1800000.00,   150, 18000000.00, 'MUREX'),
    ('2026-02-23', 'EU_BANK',   'EQUITY_OPT',  'TRADING_PNL',  'EUR', 'CPTY_F', 'CC_400',  920000.00,   500,  9200000.00, 'CALYPSO'),
    ('2026-02-23', 'EU_BANK',   'IR_SWAP',     'HEDGE_PNL',    'EUR', 'CPTY_G', 'CC_500', 3500000.00,   800, 35000000.00, 'CALYPSO'),
    ('2026-02-23', 'APAC_BANK', 'FX_SPOT',     'TRADING_PNL',  'JPY', 'CPTY_H', 'CC_600',  450000.00,  2000,  4500000.00, 'MUREX'),
    ('2026-02-23', 'APAC_BANK', 'FX_FORWARD',  'TRADING_PNL',  'JPY', 'CPTY_H', 'CC_600',  125000.00,   300,  1250000.00, 'MUREX'),

    -- Day 2: 2026-02-24 (slightly different values to test ROLL)
    ('2026-02-24', 'US_BANK',   'FX_SPOT',     'TRADING_PNL',  'USD', 'CPTY_A', 'CC_100', 1520000.00,   105, 15200000.00, 'MUREX'),
    ('2026-02-24', 'US_BANK',   'FX_SPOT',     'TRADING_PNL',  'EUR', 'CPTY_B', 'CC_100', 2280000.00,   198, 22800000.00, 'MUREX'),
    ('2026-02-24', 'US_BANK',   'FX_FORWARD',  'TRADING_PNL',  'USD', 'CPTY_A', 'CC_100',  770000.00,    52,  7700000.00, 'MUREX'),
    ('2026-02-24', 'US_BANK',   'FX_FORWARD',  'TRADING_PNL',  'GBP', 'CPTY_C', 'CC_200', -280000.00,    28, -2800000.00, 'MUREX'),
    ('2026-02-24', 'US_BANK',   'IR_SWAP',     'HEDGE_PNL',    'USD', 'CPTY_D', 'CC_300', 5100000.00,  1020, 51000000.00, 'CALYPSO'),
    ('2026-02-24', 'EU_BANK',   'FX_SPOT',     'TRADING_PNL',  'EUR', 'CPTY_E', 'CC_400', 1850000.00,   155, 18500000.00, 'MUREX'),
    ('2026-02-24', 'EU_BANK',   'EQUITY_OPT',  'TRADING_PNL',  'EUR', 'CPTY_F', 'CC_400',  950000.00,   510,  9500000.00, 'CALYPSO'),
    ('2026-02-24', 'EU_BANK',   'IR_SWAP',     'HEDGE_PNL',    'EUR', 'CPTY_G', 'CC_500', 3600000.00,   820, 36000000.00, 'CALYPSO'),
    ('2026-02-24', 'APAC_BANK', 'FX_SPOT',     'TRADING_PNL',  'JPY', 'CPTY_H', 'CC_600',  460000.00,  2050,  4600000.00, 'MUREX'),
    ('2026-02-24', 'APAC_BANK', 'FX_FORWARD',  'TRADING_PNL',  'JPY', 'CPTY_H', 'CC_600',  130000.00,   310,  1300000.00, 'MUREX');


-- ═════════════════════════════════════════════════════════════════════════════
-- 2. VERIFY SAMPLE DATA
-- ═════════════════════════════════════════════════════════════════════════════

SELECT BUSINESS_DATE, COUNT(*) AS ROW_COUNT, SUM(AMOUNT) AS TOTAL_AMOUNT
FROM FACT.FACT_TABLE
GROUP BY BUSINESS_DATE
ORDER BY BUSINESS_DATE;

-- Expected:
-- 2026-02-23 | 10 | 15,725,000
-- 2026-02-24 | 10 | 16,380,000


-- ═════════════════════════════════════════════════════════════════════════════
-- 3. TEST: Create a FLATTEN adjustment via procedure
-- ═════════════════════════════════════════════════════════════════════════════

-- Preview: Flatten all US_BANK FX_SPOT rows on 2026-02-24
CALL CORE.SP_PREVIEW_ADJUSTMENT(
    'FLATTEN',
    PARSE_JSON('{"entity_key": "US_BANK", "product_key": "FX_SPOT"}'),
    '2026-02-24'::DATE,
    NULL,
    NULL
);

-- Create the adjustment (DRAFT status)
CALL CORE.SP_CREATE_ADJUSTMENT(
    'FLATTEN',
    PARSE_JSON('{"entity_key": "US_BANK", "product_key": "FX_SPOT"}'),
    '2026-02-24'::DATE,
    NULL,
    NULL,
    'Zero out US_BANK FX_SPOT positions for month-end reporting correction',
    'JIRA-12345'
);

-- Check the header
SELECT * FROM CORE.ADJ_HEADER;

-- Check line items
SELECT * FROM CORE.ADJ_LINE_ITEM;


-- ═════════════════════════════════════════════════════════════════════════════
-- 4. TEST: Create a SCALE adjustment
-- ═════════════════════════════════════════════════════════════════════════════

-- Scale EU_BANK EQUITY_OPT by 1.5x on 2026-02-24
CALL CORE.SP_CREATE_ADJUSTMENT(
    'SCALE',
    PARSE_JSON('{"entity_key": "EU_BANK", "product_key": "EQUITY_OPT"}'),
    '2026-02-24'::DATE,
    1.5,
    NULL,
    'Adjust EU_BANK equity options valuation per updated model parameters',
    'JIRA-12346'
);


-- ═════════════════════════════════════════════════════════════════════════════
-- 5. TEST: Create a ROLL adjustment
-- ═════════════════════════════════════════════════════════════════════════════

-- Roll APAC_BANK data from 2026-02-23 → 2026-02-24 with scale 1.0
CALL CORE.SP_CREATE_ADJUSTMENT(
    'ROLL',
    PARSE_JSON('{"entity_key": "APAC_BANK"}'),
    '2026-02-24'::DATE,
    1.0,
    '2026-02-23'::DATE,
    'System outage: roll APAC_BANK from previous day pending feed recovery',
    'INC-98765'
);


-- ═════════════════════════════════════════════════════════════════════════════
-- 6. SEED USER ENTITY ACCESS (for Row Access Policy)
-- ═════════════════════════════════════════════════════════════════════════════

INSERT INTO CORE.USER_ENTITY_ACCESS (USER_NAME, ENTITY_KEY)
VALUES
    -- Replace with actual usernames
    ('ANALYST_US', 'US_BANK'),
    ('ANALYST_EU', 'EU_BANK'),
    ('ANALYST_APAC', 'APAC_BANK'),
    ('MANAGER_GLOBAL', 'US_BANK'),
    ('MANAGER_GLOBAL', 'EU_BANK'),
    ('MANAGER_GLOBAL', 'APAC_BANK');
