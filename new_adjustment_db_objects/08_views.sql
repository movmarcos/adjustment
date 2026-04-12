-- =============================================================================
-- 08_VIEWS.SQL
-- Views for sign-off checks, dashboard KPIs, recent activity, errors,
-- and the processing queue.
--
-- Views are used (not dynamic tables) because:
--   • Sign-off status must be real-time (no lag acceptable)
--   • KPIs are lightweight aggregations
--   • These are read primarily from Streamlit pages
-- =============================================================================

USE DATABASE DVLP_RAPTOR_NEWADJ;
USE SCHEMA ADJUSTMENT_APP;

-- ═══════════════════════════════════════════════════════════════════════════
-- 1. VW_SIGNOFF_STATUS — Unified sign-off check across all scopes
--
-- Streamlit queries this to determine if a COB is editable.
-- The submit procedure also checks this before allowing submissions.
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW ADJUSTMENT_APP.VW_SIGNOFF_STATUS
    COMMENT = 'Unified sign-off status. IS_SIGNED_OFF = TRUE means no new adjustments allowed for that COB/scope. Reads from ADJ_SIGNOFF_STATUS.'
AS
SELECT
    s.COBID,
    s.PROCESS_TYPE,
    s.SIGN_OFF_STATUS,
    CASE
        WHEN UPPER(s.SIGN_OFF_STATUS) = 'SIGNED_OFF' THEN TRUE
        ELSE FALSE
    END AS IS_SIGNED_OFF,
    s.SIGN_OFF_BY,
    s.SIGN_OFF_TIMESTAMP
FROM ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS s;


-- ═══════════════════════════════════════════════════════════════════════════
-- 2. VW_DASHBOARD_KPI — High-level KPIs for the dashboard header
--
-- One row per COB with counts by status, total impact, and timing metrics.
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW ADJUSTMENT_APP.VW_DASHBOARD_KPI
    COMMENT = 'High-level KPIs per COB for the dashboard header cards.'
AS
SELECT
    h.COBID,
    COUNT(DISTINCT h.ADJ_ID)                                           AS TOTAL_ADJUSTMENTS,
    COUNT(DISTINCT CASE WHEN h.RUN_STATUS = 'Pending'            THEN h.ADJ_ID END) AS PENDING_COUNT,
    COUNT(DISTINCT CASE WHEN h.RUN_STATUS = 'Pending Approval'   THEN h.ADJ_ID END) AS PENDING_APPROVAL_COUNT,
    COUNT(DISTINCT CASE WHEN h.RUN_STATUS = 'Approved'           THEN h.ADJ_ID END) AS APPROVED_COUNT,
    COUNT(DISTINCT CASE WHEN h.RUN_STATUS = 'Processed'          THEN h.ADJ_ID END) AS PROCESSED_COUNT,
    COUNT(DISTINCT CASE WHEN h.RUN_STATUS = 'Running'            THEN h.ADJ_ID END) AS RUNNING_COUNT,
    COUNT(DISTINCT CASE WHEN h.RUN_STATUS = 'Failed'             THEN h.ADJ_ID END) AS FAILED_COUNT,
    COUNT(DISTINCT CASE WHEN h.RUN_STATUS LIKE 'Rejected%'       THEN h.ADJ_ID END) AS REJECTED_COUNT,
    SUM(CASE WHEN h.RUN_STATUS = 'Processed' AND NOT h.IS_DELETED
             THEN h.ADJUSTMENT_VALUE_IN_USD ELSE 0 END)               AS TOTAL_PROCESSED_VALUE_USD,
    MIN(h.CREATED_DATE)                                                AS FIRST_SUBMISSION,
    MAX(h.PROCESS_DATE)                                                AS LAST_PROCESSING,
    -- Average processing time in seconds (submitted → processed)
    AVG(CASE WHEN h.RUN_STATUS = 'Processed'
             THEN DATEDIFF('SECOND', h.CREATED_DATE, h.PROCESS_DATE) END) AS AVG_PROCESSING_SECONDS,
    COUNT(DISTINCT h.USERNAME)                                         AS UNIQUE_USERS,
    -- Overlap count for this COB
    (SELECT COUNT(*) FROM ADJUSTMENT_APP.DT_OVERLAP_ALERTS o WHERE o.COBID = h.COBID) AS OVERLAP_ALERTS
FROM ADJUSTMENT_APP.ADJ_HEADER h
WHERE h.IS_DELETED = FALSE
GROUP BY h.COBID;


-- ═══════════════════════════════════════════════════════════════════════════
-- 3. VW_RECENT_ACTIVITY — Activity feed (last 100 events)
--
-- Combines header submissions + status transitions for a timeline view.
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW ADJUSTMENT_APP.VW_RECENT_ACTIVITY
    COMMENT = 'Activity feed combining submissions and status changes. Includes CREATED_DATE, PROCESS_DATE, and DURATION_SECONDS for timing analysis.'
AS
SELECT
    h.ADJ_ID,
    h.COBID,
    h.PROCESS_TYPE,
    h.ADJUSTMENT_TYPE,
    h.ENTITY_CODE,
    h.BOOK_CODE,
    h.USERNAME                                                AS ACTOR,
    'Submitted'                                               AS EVENT_TYPE,
    h.CREATED_DATE                                            AS EVENT_TIME,
    h.REASON                                                  AS EVENT_DETAIL,
    h.RUN_STATUS                                              AS CURRENT_STATUS,
    h.CREATED_DATE,
    h.START_DATE,
    h.PROCESS_DATE,
    DATEDIFF('second', h.START_DATE, h.PROCESS_DATE)          AS DURATION_SECONDS
FROM ADJUSTMENT_APP.ADJ_HEADER h
WHERE h.IS_DELETED = FALSE

UNION ALL

SELECT
    h.ADJ_ID,
    h.COBID,
    h.PROCESS_TYPE,
    h.ADJUSTMENT_TYPE,
    h.ENTITY_CODE,
    h.BOOK_CODE,
    sh.CHANGED_BY                                             AS ACTOR,
    sh.NEW_STATUS                                             AS EVENT_TYPE,
    sh.CHANGED_AT                                             AS EVENT_TIME,
    sh.COMMENT                                                AS EVENT_DETAIL,
    h.RUN_STATUS                                              AS CURRENT_STATUS,
    h.CREATED_DATE,
    h.START_DATE,
    h.PROCESS_DATE,
    DATEDIFF('second', h.START_DATE, h.PROCESS_DATE)          AS DURATION_SECONDS
FROM ADJUSTMENT_APP.ADJ_STATUS_HISTORY sh
-- Cast to VARCHAR so the join survives if ADJ_STATUS_HISTORY.ADJ_ID is still NUMBER
INNER JOIN ADJUSTMENT_APP.ADJ_HEADER h
    ON h.ADJ_ID = sh.ADJ_ID::VARCHAR
WHERE h.IS_DELETED = FALSE;


-- ═══════════════════════════════════════════════════════════════════════════
-- 4. VW_ERRORS — Current errors for the error panel
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW ADJUSTMENT_APP.VW_ERRORS
    COMMENT = 'Adjustments currently in Failed status. Used for the error panel in dashboard.'
AS
SELECT
    h.ADJ_ID,
    h.COBID,
    h.PROCESS_TYPE,
    h.ADJUSTMENT_TYPE,
    h.ENTITY_CODE,
    h.BOOK_CODE,
    h.USERNAME,
    h.CREATED_DATE,
    h.PROCESS_DATE           AS ERROR_TIME,
    h.ERRORMESSAGE,
    h.REASON
FROM ADJUSTMENT_APP.ADJ_HEADER h
WHERE h.RUN_STATUS = 'Failed'
  AND h.IS_DELETED = FALSE;


-- ═══════════════════════════════════════════════════════════════════════════
-- 5. VW_MY_WORK — Adjustments created by the current user
--
-- Filtered in Streamlit by: WHERE SUBMITTED_BY = CURRENT_USER()
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW ADJUSTMENT_APP.VW_MY_WORK
    COMMENT = 'All adjustments with full detail. Streamlit My Work page filters by CURRENT_USER().'
AS
SELECT
    h.ADJ_ID,
    h.COBID,
    h.PROCESS_TYPE,
    h.ADJUSTMENT_TYPE,
    h.ADJUSTMENT_ACTION,
    h.ENTITY_CODE,
    h.DEPARTMENT_CODE,
    h.BOOK_CODE,
    h.CURRENCY_CODE,
    h.SCALE_FACTOR,
    h.SCALE_FACTOR_ADJUSTED,
    h.SOURCE_COBID,
    h.ADJUSTMENT_VALUE_IN_USD,
    h.REASON,
    h.RUN_STATUS,
    h.USERNAME                AS SUBMITTED_BY,
    h.CREATED_DATE            AS SUBMITTED_AT,
    h.START_DATE,
    h.PROCESS_DATE,
    h.RECORD_COUNT,
    h.ERRORMESSAGE,
    h.ADJUSTMENT_OCCURRENCE,
    h.GLOBAL_REFERENCE,
    h.FILE_NAME,
    h.IS_DELETED
FROM ADJUSTMENT_APP.ADJ_HEADER h;


-- ═══════════════════════════════════════════════════════════════════════════
-- 6. VW_PROCESSING_QUEUE — Adjustments currently being processed or pending
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW ADJUSTMENT_APP.VW_PROCESSING_QUEUE
    COMMENT = 'Live view of the processing pipeline. Pending and Running adjustments.'
AS
SELECT
    h.ADJ_ID,
    h.COBID,
    h.PROCESS_TYPE,
    h.ADJUSTMENT_TYPE,
    h.ADJUSTMENT_ACTION,
    h.ENTITY_CODE,
    h.BOOK_CODE,
    h.DEPARTMENT_CODE,
    h.RUN_STATUS,
    h.ADJUSTMENT_OCCURRENCE,
    h.USERNAME                AS SUBMITTED_BY,
    h.CREATED_DATE            AS SUBMITTED_AT,
    -- Estimate queue position
    ROW_NUMBER() OVER (
        PARTITION BY h.PROCESS_TYPE
        ORDER BY
            CASE h.ADJUSTMENT_OCCURRENCE WHEN 'ADHOC' THEN 0 ELSE 1 END,
            h.CREATED_DATE
    ) AS QUEUE_POSITION
FROM ADJUSTMENT_APP.ADJ_HEADER h
WHERE h.RUN_STATUS IN ('Pending', 'Approved', 'Running')
  AND h.IS_DELETED = FALSE;


-- ═══════════════════════════════════════════════════════════════════════════
-- 7. VW_APPROVAL_QUEUE — Adjustments awaiting approval
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW ADJUSTMENT_APP.VW_APPROVAL_QUEUE
    COMMENT = 'Adjustments currently in Pending Approval status. Used by the Approval Queue page.'
AS
SELECT
    h.ADJ_ID,
    h.COBID,
    h.PROCESS_TYPE,
    h.ADJUSTMENT_TYPE,
    h.ADJUSTMENT_ACTION,
    h.ENTITY_CODE,
    h.BOOK_CODE,
    h.DEPARTMENT_CODE,
    h.CURRENCY_CODE,
    h.SCALE_FACTOR,
    h.SCALE_FACTOR_ADJUSTED,
    h.SOURCE_COBID,
    h.ADJUSTMENT_VALUE_IN_USD,
    h.REASON,
    h.RUN_STATUS,
    h.USERNAME                AS SUBMITTED_BY,
    h.CREATED_DATE            AS SUBMITTED_AT,
    h.RECORD_COUNT,
    h.ADJUSTMENT_OCCURRENCE,
    h.GLOBAL_REFERENCE,
    h.APPROVAL_ID
FROM ADJUSTMENT_APP.ADJ_HEADER h
WHERE h.RUN_STATUS = 'Pending Approval'
  AND h.IS_DELETED = FALSE;


-- ═══════════════════════════════════════════════════════════════════════════
-- NOTE: VW_QUEUE_* views are defined in 02_streams.sql (before the streams
-- that depend on them). They are not duplicated here.
-- ═══════════════════════════════════════════════════════════════════════════

-- ═══════════════════════════════════════════════════════════════════════════
-- VERIFY
-- ═══════════════════════════════════════════════════════════════════════════

SHOW VIEWS LIKE 'VW_%' IN SCHEMA ADJUSTMENT_APP;
