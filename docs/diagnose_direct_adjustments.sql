-- ═══════════════════════════════════════════════════════════════════════════
-- DIAGNOSE DIRECT ADJUSTMENTS (incl. FRTB / DRC / RRAO uploads)
-- ═══════════════════════════════════════════════════════════════════════════
-- Run these top to bottom in Snowsight against the environment you're
-- testing (default DVLP):
--     USE DATABASE DVLP_RAPTOR_NEWADJ;   USE SCHEMA ADJUSTMENT_APP;
-- Each query answers ONE question; the comment says what a bad answer means.
-- For "my direct RRAO adjustment is not on the Home grid", run Q1 → Q3 first.
USE DATABASE DVLP_RAPTOR_NEWADJ;
USE SCHEMA ADJUSTMENT_APP;

-- ───────────────────────────────────────────────────────────────────────────
-- Q1. Does the adjustment EXIST at all? Latest FRTB-family headers, no
--     filters. If your RRAO upload is missing here, the submit never created
--     a header (check Q5/Q6 for the staged batch and errors).
-- ───────────────────────────────────────────────────────────────────────────
SELECT ADJ_ID, DIMENSION_ADJ_ID, COBID, PROCESS_TYPE, ADJUSTMENT_TYPE,
       RUN_STATUS, IS_DELETED, GLOBAL_REFERENCE, USERNAME,
       RECORD_COUNT, CREATED_DATE, START_DATE, PROCESS_DATE, ERROR_MESSAGE
FROM ADJ_HEADER
WHERE PROCESS_TYPE IN ('FRTB', 'FRTBDRC', 'FRTBRRAO')
ORDER BY CREATED_DATE DESC
LIMIT 30;

-- ───────────────────────────────────────────────────────────────────────────
-- Q2. WHY is it not on the Home grid? Home shows ADJ_HEADER ordered by
--     CREATED_DATE DESC **LIMIT 50** (plus the COB filter you picked in the
--     UI). This ranks every recent adjustment the way Home does:
--       - POSITION > 50            → pushed out by newer rows (not a bug)
--       - IS_DELETED = TRUE        → hidden by design
--       - COBID not in your filter → change the COB picker on Home
-- ───────────────────────────────────────────────────────────────────────────
SELECT ROW_NUMBER() OVER (ORDER BY CREATED_DATE DESC) AS HOME_POSITION,
       ADJ_ID, COBID, PROCESS_TYPE, RUN_STATUS, IS_DELETED,
       GLOBAL_REFERENCE, USERNAME, CREATED_DATE
FROM ADJ_HEADER
QUALIFY HOME_POSITION <= 80        -- Home cuts at 50; see where yours lands
ORDER BY HOME_POSITION;

-- ───────────────────────────────────────────────────────────────────────────
-- Q3. Full life story of ONE adjustment: every status transition, who and
--     when. Stuck at 'Pending' with no transitions → the pipeline never
--     picked it up (check the task); an error transition shows the message.
--     ⇩ set your ADJ_ID (from Q1) or use the GLOBAL_REFERENCE variant.
-- ───────────────────────────────────────────────────────────────────────────
SELECT h.ADJ_ID, h.PROCESS_TYPE, h.RUN_STATUS AS CURRENT_STATUS,
       s.OLD_STATUS, s.NEW_STATUS, s.CHANGED_BY, s.CHANGED_AT, s.COMMENT
FROM ADJ_HEADER h
LEFT JOIN ADJ_STATUS_HISTORY s ON s.ADJ_ID = h.ADJ_ID
WHERE h.GLOBAL_REFERENCE = '<YOUR_REFERENCE>'      -- ← or: h.ADJ_ID = '<uuid>'
ORDER BY s.CHANGED_AT;

-- ───────────────────────────────────────────────────────────────────────────
-- Q4. Did the upload's DATA rows land? One row per CSV line in
--     ADJ_LINE_ITEM_JSON. RECORD_COUNT on the header should equal N_ROWS
--     here; 0 rows with an existing header = the payload insert failed.
-- ───────────────────────────────────────────────────────────────────────────
SELECT h.ADJ_ID, h.PROCESS_TYPE, h.RUN_STATUS, h.RECORD_COUNT,
       COUNT(j.LINE_ID)  AS N_ROWS,
       MIN(j.RUN_STATUS) AS MIN_ROW_STATUS,
       MAX(j.RUN_STATUS) AS MAX_ROW_STATUS
FROM ADJ_HEADER h
LEFT JOIN ADJ_LINE_ITEM_JSON j ON j.ADJ_ID = h.ADJ_ID AND NOT j.IS_DELETED
WHERE h.PROCESS_TYPE = 'FRTBRRAO'
GROUP BY 1, 2, 3, 4
ORDER BY MAX(h.CREATED_DATE) DESC
LIMIT 20;

-- ───────────────────────────────────────────────────────────────────────────
-- Q5. Inspect the actual RRAO payload rows (raw JSON per CSV line) for one
--     adjustment — verify the values you uploaded are really what's stored.
-- ───────────────────────────────────────────────────────────────────────────
SELECT j.ROW_NUM, j.RUN_STATUS, j.PAYLOAD
FROM ADJ_LINE_ITEM_JSON j
JOIN ADJ_HEADER h ON h.ADJ_ID = j.ADJ_ID
WHERE h.GLOBAL_REFERENCE = '<YOUR_REFERENCE>'
ORDER BY j.ROW_NUM
LIMIT 100;

-- ───────────────────────────────────────────────────────────────────────────
-- Q6. Anything still sitting in the STAGING area? Direct rows live in
--     ADJ_DIRECT_STAGE only between paste/upload and submit; rows older
--     than a few minutes mean a batch was staged but never submitted
--     (validation blocked it, or the user never clicked Submit).
-- ───────────────────────────────────────────────────────────────────────────
SELECT BATCH_ID, USERNAME, COUNT(*) AS ROWS_STAGED,
       MIN(CREATED_DATE) AS STAGED_AT
FROM ADJ_DIRECT_STAGE
GROUP BY 1, 2
ORDER BY STAGED_AT DESC;

-- ───────────────────────────────────────────────────────────────────────────
-- Q7. What the DOWNSTREAM consumers see: the RRAO enriched view (headers
--     joined to their JSON lines in the FRTBSA_RRAO shape). An adjustment
--     that's Processed but absent here usually means the view's filters
--     (PROCESS_TYPE/IS_DELETED/status) exclude it.
-- ───────────────────────────────────────────────────────────────────────────
SELECT *
FROM VW_DIRECT_FRTBRRAO_ENRICHED
WHERE COBID = 20260626                              -- ← your COB
LIMIT 50;

-- ───────────────────────────────────────────────────────────────────────────
-- Q8. Failures overview: every FRTB-family adjustment that errored in the
--     last 7 days, with the message — the fastest "what broke" sweep.
-- ───────────────────────────────────────────────────────────────────────────
SELECT ADJ_ID, COBID, PROCESS_TYPE, GLOBAL_REFERENCE, USERNAME,
       RUN_STATUS, ERROR_MESSAGE, CREATED_DATE
FROM ADJ_HEADER
WHERE PROCESS_TYPE IN ('FRTB', 'FRTBDRC', 'FRTBRRAO')
  AND (RUN_STATUS ILIKE '%fail%' OR ERROR_MESSAGE IS NOT NULL)
  AND CREATED_DATE >= DATEADD(day, -7, CURRENT_TIMESTAMP())
ORDER BY CREATED_DATE DESC;
