-- =============================================================================
-- BUSINESS CHECK — FRTB DRC    ·    run in each DB, export to Excel, compare
-- Easier: use the app's Validation page — just enter the two adjustment IDs.
-- =============================================================================
-- Set the database context first (one):
--   USE DATABASE DVLP_RAPTOR_NEWADJ;   |   USE DATABASE PROD_RAPTOR;
-- ADJ_ID = DIMENSION.ADJUSTMENT.ADJUSTMENT_ID (the number, NOT the hash) — differs per DB
-- COB    = COB date (YYYYMMDD)        ·    USD amount column: JTD_LOSS_USD
-- To narrow an ORIGINAL/FINAL query to one book, add:
--   AND BOOK_KEY IN (SELECT BOOK_KEY FROM DIMENSION.BOOK WHERE BOOK_CODE = 'YOUR-BOOK')
-- =============================================================================
SET ADJ_ID = 100123;     -- <<< the adjustment id IN THIS database
SET COB    = 20260608;   -- <<< the COB date

-- DIMENSION.ADJUSTMENT — the adjustment record: who / what / when. Key: ADJUSTMENT_ID
SELECT * FROM DIMENSION.ADJUSTMENT WHERE ADJUSTMENT_ID = $ADJ_ID;

-- FACT.FRTBSA_DRC_MEASURES_ADJUSTMENT
--   the DELTA this adjustment wrote (what it added/removed). Key: ADJUSTMENT_ID
SELECT COUNT(*) AS ROWS, SUM(JTD_LOSS_USD) AS TOTAL_USD
FROM FACT.FRTBSA_DRC_MEASURES_ADJUSTMENT WHERE ADJUSTMENT_ID = $ADJ_ID;                 -- total
SELECT * FROM FACT.FRTBSA_DRC_MEASURES_ADJUSTMENT WHERE ADJUSTMENT_ID = $ADJ_ID ORDER BY COBID;   -- detail

-- FACT.FRTBSA_DRC_MEASURES
--   ORIGINAL values, before any adjustment. Key: COBID
SELECT COUNT(*) AS ROWS, SUM(JTD_LOSS_USD) AS TOTAL_USD
FROM FACT.FRTBSA_DRC_MEASURES WHERE COBID = $COB;

-- FACT.FRTBSA_DRC_MEASURES_ADJUSTED
--   FINAL values = original + all adjustments (what the reports show). Key: COBID
SELECT COUNT(*) AS ROWS, SUM(JTD_LOSS_USD) AS TOTAL_USD
FROM FACT.FRTBSA_DRC_MEASURES_ADJUSTED WHERE COBID = $COB;
