# Pre-Go-Live Review — 2026-08-19

Full-solution review (4 parallel passes: DB objects, stored procedures, app
pages, app↔DB cross-consistency) triggered by two questions: (a) is there any
other migration-only code embedded in the repeatable deploy files, and (b) is
the solution ready to go live. Findings verified against the code before
inclusion; each carries file:line.

**Verdict: NOT ready as-is.** 1 CRITICAL + 8 HIGH must be addressed (or
consciously accepted) before go-live. The positive: all proc signatures,
table columns, view columns and status literals between app and DB were
cross-checked with ZERO mismatches, and the queue/claim concurrency protocol
reviewed sound.

---

## A. Migration-only / redeploy-unsafe code in the deploy files

The direct answer to "is there other code like the backfill UPDATEs":

| # | Where | What re-running destroys | Class |
|---|-------|--------------------------|-------|
| A1 | 01_tables.sql:675 `DELETE FROM ADJUSTMENTS_SETTINGS;` + reseed | **The whole engine config** — scopes added in prod, IS_ACTIVE toggles, hotfixed FACT_TABLE_PK. Non-transactional: a mid-window reader sees ZERO config; in-flight adjustments fail. (= July review M7, still open) | HIGH — move to a seed-once/MERGE pattern |
| A2 | 01_tables.sql:1035 `DELETE FROM ADJ_CATEGORY;` + reseed | Categories added/deactivated in prod; empty list during window | LOW |
| A3 | 01_tables.sql:514 + 15_direct_frtb_upload.sql:36/121/204 `DELETE FROM DIRECT_SCOPE_SCHEMA WHERE PROCESS_TYPE=…` + INSERT | Dev-maintained config (reseed intended) but non-transactional: deploy abort between DELETE and INSERT leaves that scope's upload path dead | MEDIUM |
| A4 | 01_tables.sql:488 DIRECT_ACCEPTED_COLUMNS MERGE forces `IS_ACTIVE=TRUE` on matched rows | Admin-deactivated aliases resurrect on redeploy; removed seeds never retire | LOW |
| A5 | 01_tables.sql:873 `UPDATE ADJ_ADMINS SET ADMIN_TYPE='USER' WHERE NULL` | Nothing (idempotent safety net) — fine to keep | OK |
| A6 | 06_tasks.sql `CREATE OR REPLACE TASK` ×5 | Tasks recreated SUSPENDED; only deploy's resume step restarts them. Standalone rerun of 06 silently halts all pipelines; also destroys deliberate operational suspensions | MEDIUM (= July M6) |
| A7 | migrations/2026-08-19_var_component_id_backfill.sql | Correctly isolated — run once at deploy | OK |

Recommendation: convert A1/A2 to MERGE-style seeds (insert-if-missing, never
delete), wrap A3 in explicit transactions, and keep 06 + resume as an atomic
deploy step.

## B. Go-live blockers (CRITICAL/HIGH)

| # | Sev | Where | Finding |
|---|-----|-------|---------|
| B1 | CRITICAL | 1_New_Adjustment.py:2330 (FRTB), ~2045 (VaR Upload) | Clearing the pasted/uploaded CSV leaves `wiz["uploaded_df"]` submittable; the FRTB branch also nulls `_frtb_rule_errs`, turning the rules gate GREEN. A user can see rule errors, clear the box to abort, and Submit the rejected file. |
| B2 | HIGH | 05:1682/2281/2308 via 05c + 4_Adjustment_Pipeline.py:199 | Force-process runs SP_PROCESS in the app's owner's-rights session where `CREATE TEMPORARY TABLE` throws ("Unsupported statement type") — Force is dead on arrival for Scale/Flatten/Roll/EROL and flips the adjustment to Failed. Pipeline-task sessions unaffected. |
| B3 | HIGH | 15_direct_frtb_upload.sql:614-615 (+456) | RRAO metrics parsed with `TRY_TO_NUMBER` without scale → NUMBER(38,0): decimals rounded away, |amount|<0.5 zero-filtered. |
| B4 | HIGH | 15_direct_frtb_upload.sql:546-556 | DRC regulator buckets keyed off optional `REGION` compared against ENTITY codes (bug ported from the FRTB team's original view); SBM correctly uses ENTITY_CODE. MUSI DRC rows land as NEW YORK / NULL bucket. |
| B5 | HIGH | 8_COB_Cockpit.py:222-247 | Cockpit `_resign_off` still signs off by direct UPDATE — bypasses the new approval-gated request flow (pages 1/5 use SP_REQUEST_SIGNOFF_CHANGE). Also its `_request_reopen` duplicates the transition via MERGE. Any user, one checkbox, no 4-eyes. |
| B6 | HIGH | 1_New_Adjustment.py:2287-2294/899-910 | FRTB file flow shows mixed-COB / file-vs-page COB errors but nothing gates Submit on them (the per-row Direct branch has the check; FRTB branch doesn't). |
| B7 | HIGH | systemic: `_esc` in 03/04/05/05b/05c/11/12/14 + app pages 1/3/5/6/8 | Escaping doubles quotes but not backslashes; trailing `\` (e.g. Windows path in FILE_NAME/reason) breaks the literal — hard failure at best, literal-breakout injection at worst. Repo's own convention (backslash first) applied only in `_submit_one`/`_sql_str_literal`/`call_sp_df`. |
| B8 | HIGH | 03:438-463 + 05:677-684 | Upload replacement race: soft-delete of the duplicate has no `RUN_STATUS <> 'Running'` re-check inside the txn, and 05's claim read doesn't check IS_DELETED → pipeline can process a just-Replaced header → double-counted fact rows. |
| B9 | HIGH | 2_Adjustments.py:494, 4_Adjustment_Pipeline.py:592 | `dict(snowpark Row)` raises TypeError inside try/except → Status History timeline permanently renders "No history available" on both pages. Audit trail invisible. |

## C. Mediums (fix soon, not blocking if accepted)

- C1 12_sp_workflow: rejecting a SIGNOFF_REQUESTED that came from REOPENED reverts to OPEN → upstream feed re-blocks; entity stuck (REOPEN refused from OPEN). Store the pre-request status or revert to REOPENED.
- C2 03:331 comment promises Direct GLOBAL_REFERENCE dedup that doesn't exist (Upload-only; 14 does none) → resubmitted Direct reference silently stacks a second adjustment.
- C3 15: dimension joins (TRADE without book/source qualifier; `td.BOOK_CODE = COALESCE(payload,td.BOOK_CODE)` no-op; instrument joins by code) can fan one line item into N rows sharing one FRTBSA_*_KEY. Add QUALIFY dedup.
- C4 8_COB_Cockpit: SIGNOFF_REQUESTED unhandled — pending sign-off shows as OPEN on the COB run sheet.
- C5 1_New_Adjustment: "Submit anyway" override + dup/EROL confirm checkboxes survive file/target changes (stale consent); `_direct_file_cob` survives mode/scope switches (false Submit block).
- C6 6_Admin Schema/SQL reference + 7_Documentation pages table: describe retired streams architecture, the removed Sign-Off Management tab, FRTBALL; misleads admins.
- C7 app.py:446/514: ERRORMESSAGE/ALERT_MESSAGE injected into raw HTML unescaped (Home page).
- C8 01_tables DIRECT_SCOPE_SCHEMA CREATE OR ALTER lacks VALIDATION_RULES/ALIASES that 15 ADDs — rerunning 01 after 15 may fail (CREATE OR ALTER can't drop columns).
- C9 x-check: `_request_reopen` duplicated in pages 1 and 8 via raw MERGE — state machine in three places.

## D. Lows (housekeeping)

08_views VW_ADJUSTMENT_TRACK MIN(actor) decoupled from timestamp; 15 currency-pair
direction differs SBM (CCY2-CCY1) vs DRC/RRAO (CCY1-CCY2) — one family resolves
key -1; 04 cross-COB Roll preview silently wrong without FACT_ADJUSTED_TABLE;
05:2517/12:146 escape-then-truncate can split a quote pair; NaN SCALE_FACTOR
renders "nan"; 3_Approval_Queue recent list colors by current status not
decision; `_frtb_rule_errors` IndexError on malformed 2-element rule;
`call_procedure` (no callers) unescaped; zero-rows submit gate defeated by
touching a filter; COBID stage-column seed points at nonexistent column
(app-side virtual handling only); ADJ_SIGNOFF_STATUS comment omits
SIGNOFF_REQUESTED.

## E. Verified clean

Proc signatures (11 procs, all call sites), 17 tables, 13 views, 15 status
literals: zero mismatches. SP_DECIDE_REOPEN and VW_PROCESSING_QUEUE: no stale
references. BEGIN/COMMIT/ROLLBACK balanced everywhere; DDL kept outside
transactions; claim-token protocol sound; 14's deterministic-UUID retry design
sound; summary rebuilds self-healing under concurrency; `UNDERYLING_TENOR_CODE`
(sic) spelling matches the real dimension column.

## F. Go-live grant/deploy checklist

Owner role needs — SELECT: DIMENSION.{ENTITY, BOOK, TRADE, VAR_SUB_COMPONENT,
STRESS_SIMULATION, MEASURE_TYPE, COMMON_INSTRUMENT(_FCD), PRODUCT_CATEGORY_
ATTRIBUTES, TENOR_CURRENCY, UNDERLYING_TENOR_CURRENCY, CURVE_CURRENCY,
CURRENCY_PAIR, FRTB_INSTRUMENT}; FACT.{VAR|STRESS|SENSITIVITY|FRTBSA_*}
_MEASURES(+_COMBINED/_ADJUSTED); BATCH.RUN_LOG; BATCH.PUBLISH_SIGNOFF_STATUS;
RAVEN.LOG_STAGE_ME_STATUS; STATIC_STAGING.TICKER_ISIN_MAP.
INSERT/UPDATE/DELETE: DIMENSION.ADJUSTMENT, FACT.*_ADJUSTMENT(+_SUMMARY),
RAVEN.LOG_STAGE_ME_STATUS (dbt trigger row).
USAGE/EXECUTE: FACT.UPDATE_POWERBI_FOR_ADJUSTMENTS, BATCH.LOAD_RUN_LOG(_END_
WITH_DETAIL), BATCH.SEQ_RUN_LOG, email integration ADJ_EMAIL_INT +
SYSTEM$SEND_EMAIL, EXECUTE TASK + EXECUTE MANAGED TASK, warehouses (app +
{{DT_WH}}).
Verify: `SHOW GRANTS OF ROLE BI_DEVELOPER` runnable by owner role (admin
role-gate); `SHOW TASKS` from app; tasks RESUMED after deploy (06 leaves them
suspended); run migrations/2026-08-19_var_component_id_backfill.sql once;
run SP_SYNC_SIGNOFF_STATUS once to materialise open COBs.

## G. Still-open items carried from the 2026-07-28 review

SCD2 key-fix not batch-scoped; NULL-key supersede gap; sign-off feed/entity
granularity; 10-day VaR bypass; VaR -1 key resolution; M8 timezone skew in
VW_REPORT_REFRESH_STATUS; M9 DT_OVERLAP_ALERTS quadratic growth; M13 stored-
markup rendering via unsafe_allow_html (overlaps C7). Recurring adjustments
end-to-end remains future work.
