-- ═══════════════════════════════════════════════════════════════════════════
-- 16_FACT_LOAD_SET_FIX.SQL — widen LOAD_SET on the FRTB adjustment facts
-- ═══════════════════════════════════════════════════════════════════════════
-- The FRTB Direct enriched views stamp the load provenance into LOAD_SET
-- ('DIRECT-SBM' / 'DIRECT-DRC' / 'DIRECT-RRAO'), but the adjustment fact
-- tables inherited tiny widths from the original loader design:
--     FRTBSA_RRAO_MEASURES_ADJUSTMENT.LOAD_SET  VARCHAR(4)   ← 'DIRECT-RRAO' = 11
--     FRTBSA_DRC_MEASURES_ADJUSTMENT.LOAD_SET   VARCHAR(3)   ← 'DIRECT-DRC'  = 10
-- so every RRAO insert failed with a right-truncation error (user-hit live
-- 2026-09-03), and DRC would fail identically on first use. Sensitivity's
-- column is already VARCHAR(16777216) — untouched.
--
-- Widening a VARCHAR is metadata-only in Snowflake: instant, no rewrite, no
-- effect on existing rows or readers. VARCHAR(100) matches the Stress
-- adjustment fact's LOAD_SET width.
-- Re-running is harmless (setting the same width is a no-op).

ALTER TABLE IF EXISTS FACT.FRTBSA_RRAO_MEASURES_ADJUSTMENT
    ALTER COLUMN LOAD_SET SET DATA TYPE VARCHAR(100);

ALTER TABLE IF EXISTS FACT.FRTBSA_DRC_MEASURES_ADJUSTMENT
    ALTER COLUMN LOAD_SET SET DATA TYPE VARCHAR(100);
