-- =============================================================================
-- 02_STREAMS.SQL
-- Streams on the entry-point tables to detect new/changed adjustments.
--
-- The processing task watches these streams and fires when new data arrives.
-- =============================================================================

USE DATABASE DVLP_RAPTOR_NEWADJ;
USE SCHEMA ADJUSTMENT_APP;

-- ═══════════════════════════════════════════════════════════════════════════
-- 1. Stream on ADJ_HEADER — triggers the main processing task
--
-- Detects new adjustments (INSERT) and status changes (UPDATE).
-- Task in 06_tasks.sql uses SYSTEM$STREAM_HAS_DATA('ADJ_HEADER_STREAM').
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE STREAM ADJUSTMENT_APP.ADJ_HEADER_STREAM
    ON TABLE ADJUSTMENT_APP.ADJ_HEADER
    APPEND_ONLY = FALSE          -- Need INSERT + UPDATE to track status changes
    SHOW_INITIAL_ROWS = FALSE    -- Only new data after stream creation
    COMMENT = 'Watches ADJ_HEADER for new submissions and status transitions. Drives the processing task.';


-- ═══════════════════════════════════════════════════════════════════════════
-- 2. Stream on ADJ_LINE_ITEM — optional, for monitoring line-item arrivals
--
-- Useful if we want a separate task that validates line items before
-- marking the header as "ready" (e.g., CSV parsing complete).
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE STREAM ADJUSTMENT_APP.ADJ_LINE_ITEM_STREAM
    ON TABLE ADJUSTMENT_APP.ADJ_LINE_ITEM
    APPEND_ONLY = TRUE           -- Only care about new rows, not updates
    SHOW_INITIAL_ROWS = FALSE
    COMMENT = 'Watches ADJ_LINE_ITEM for new upload data. Can trigger validation before header is marked ready.';


-- ═══════════════════════════════════════════════════════════════════════════
-- VERIFY
-- ═══════════════════════════════════════════════════════════════════════════

SHOW STREAMS LIKE 'ADJ_%' IN SCHEMA ADJUSTMENT_APP;
