# Direct Adjustment vs VaR Upload — Split Design

**Date:** 2026-08-05
**Status:** Approved (design review with Marcos, 2026-08-05)
**Supersedes the "Direct" semantics of** `2026-06-03-direct-adjustment-scope-schema-design.md` — the declarative CSV engine built there is kept, but re-labelled as the **Upload** flow (VaR only). "Direct" gets a new, simpler meaning.

## Problem

The current app category "Direct Adjustment" is a CSV upload driven by
`DIRECT_SCOPE_SCHEMA` + `ADJ_LINE_ITEM_JSON`: one submission = one
`ADJ_HEADER`/`DIMENSION.ADJUSTMENT` entry whose CSV lines all become fact rows
under a single adjustment id. That is the semantics of the **legacy VaR
Upload** (one file → one adjustment entry; see
`_OLD/context/codes/VaR_Upload/` — the legacy dimension rows even carry
`ADJUSTMENT_TYPE='Upload'`), not of a legacy **Direct** adjustment, where each
entered row is its own adjustment (own `ADJUSTMENT_ID`, one fact row — see the
Direct handling in `FACT.LOAD_STRESS_ADJUSTMENT_UPLOAD`). The two concepts were
conflated when the feature was first built.

## Decisions (agreed)

1. **Direct Adjustment (new meaning):** CSV upload *or* copy-paste where each
   row carries dimension **codes + a USD value**. **Each row becomes its own
   `ADJ_HEADER` + `DIMENSION.ADJUSTMENT` entry and exactly one row in the
   scope's `FACT.*_ADJUSTMENT` table** — individually approvable, deletable,
   reconcilable, exactly as if the scaling form had been submitted once per
   row. Keys: `ADJUSTMENT_TYPE='Direct'`, `ADJUSTMENT_ACTION='Direct'`.
2. **VaR Upload (rename of today's flow):** the existing declarative CSV
   engine (`ADJ_LINE_ITEM_JSON` + `DIRECT_SCOPE_SCHEMA`, unpivot of the 21
   legacy VaR measure columns) is offered **only for VaR**, under a new app
   category label "VaR Upload". One file → one adjustment entry (many fact
   rows). Keys change to `ADJUSTMENT_TYPE='Upload'`,
   `ADJUSTMENT_ACTION='Upload'` (matches legacy naming).
3. **Scopes:** Direct is offered for ALL scopes including VaR (amended
   2026-08-05 after implementation — user decision: what makes VaR special is
   only its Upload feed, not an exclusion from Direct). VaR Upload remains
   VaR-only.
4. **Column order/case never matter** for either paste/upload: columns are
   matched by header *name* through a per-scope accepted-columns config.
5. **Per-scope validation views** own row validation for Direct
   (`VW_DIRECT_VALIDATE_<SCOPE>`); generic rules now, FRTB cross-field rules
   to be added later by editing only the FRTB view(s).
6. Existing dev rows with `ADJUSTMENT_TYPE='Direct'` (CSV tests) are left
   untouched; no data migration.

## Architecture

### A. App — category and forms (`1_New_Adjustment.py`)

- Category picker: "Direct Adjustment" (Stress/Sens/FRTB scopes) and
  "VaR Upload" (VaR only). The current Direct wizard branch becomes the VaR
  Upload branch with its label/scope gate changed; behaviour unchanged.
- Direct branch (new): scope pills → paste area / file upload → parse by
  header name (order/case-free, aliases from config) → write rows to the
  staging table → query the scope's validation view → show a preview grid
  with per-row ✓/✗ + messages → on submit, create one header per **valid**
  row via the existing submit path (codes land on the header's filter
  columns, value on `ADJUSTMENT_VALUE_IN_USD`); invalid rows are never
  submitted (downloadable rejects, as the upload validation does today).
  A single-row "form mode" is the same thing with one staged row.

### B. Staging + validation (new DB objects)

- **`ADJUSTMENT_APP.ADJ_DIRECT_STAGE`** — transient parse target: `BATCH_ID`
  (UUID per paste), `ROW_NUM`, one column per accepted code field
  (ENTITY_CODE, DEPARTMENT_CODE, BOOK_CODE, TRADE_CODE, TRADE_TYPOLOGY,
  STRATEGY, INSTRUMENT_CODE, SIMULATION_NAME, SIMULATION_SOURCE,
  MEASURE_TYPE_CODE, CURRENCY_CODE, SOURCE_SYSTEM_CODE, …), `VALUE_USD`,
  `CREATED_DATE`, `USERNAME`. Rows are deleted after submit/cancel; a
  cleanup task or age-based purge keeps it small.
- **`ADJUSTMENT_APP.DIRECT_ACCEPTED_COLUMNS`** — per scope: accepted header
  name/alias → stage column, `IS_REQUIRED`. Seeds: generic per scope
  (entity + value required everywhere; simulation fields only for Stress;
  measure type for Sens/FRTB; etc.). The app parser and the validation
  views both read it.
- **`ADJUSTMENT_APP.VW_DIRECT_VALIDATE_<SCOPE>`** (STRESS, SENSITIVITY,
  FRTB, FRTBDRC, FRTBRRAO) — one view per scope over `ADJ_DIRECT_STAGE`,
  generated from one generic template. Output: `BATCH_ID, ROW_NUM,
  IS_VALID, VALIDATION_ERRORS (ARRAY)`. Generic rules v1:
  - required fields present (per config), `VALUE_USD` numeric and ≠ 0;
  - every supplied code exists in its dimension (ENTITY, BOOK — and its
    DEPARTMENT consistency, TRADE, COMMON_INSTRUMENT, STRESS_SIMULATION,
    MEASURE_TYPE), case-insensitive;
  - **Extension point:** FRTB cross-field rules will be added to the three
    FRTB views only, when the requirements arrive; the per-scope-view
    structure exists precisely so those edits stay local.

### C. Engine — `SP_PROCESS_ADJUSTMENT`

- **Direct branch rewritten** (the declarative engine code moves under the
  Upload action): for each claimed Direct header, build **one** fact row —
  resolve each supplied code to its dimension key (case-insensitive
  lookup), default **−1 for every `*_KEY`/`*_ID` with no code or no
  match** (legacy convention, incl. e.g. `INFLATION_INDEX_ID = -1`),
  legacy value defaults (`CURRENCY_CODE → 'N/A'`, `SOURCE_SYSTEM_CODE →
  'QP'` where applicable), `ADJUSTMENT_VALUE_IN_USD` into the USD measure
  column (and the native measure column when the table has both — no FX).
  Keeps: retry cleanup keyed by prior `DIMENSION_ADJ_ID`, cross-batch
  supersede, per-adjustment `RECORD_COUNT` (=1), zero-match warning if the
  row could not be written, `_erlog` SQL debug logging, run log, PowerBI /
  dbt hand-off.
- **Summary rebuild added for both Direct and Upload paths** (the current
  Direct branch never rebuilds `*_ADJUSTMENT_SUMMARY`; the Scale path and
  the legacy procs both do). Same atomic delete+insert as the Scale path.
- **Upload branch:** the existing declarative engine, now gated on
  `ADJUSTMENT_ACTION='Upload'`; content unchanged apart from the summary
  rebuild above.

### D. Dispatch and periphery

- `SP_RUN_PIPELINE` / queue combos: 'Upload' action added, 'Direct'
  combos now dispatch to the new branch. Approval flow, sign-off gate,
  claim tokens, notifications: unchanged (headers are ordinary headers).
- Recon query v2 (`docs/adjustment_recon_v2.sql`) gains a trivial Direct
  rule: a Direct adjustment reconciles when its fact-table sum equals
  `ADJ_HEADER.ADJUSTMENT_VALUE_IN_USD` (no original/source lookup).
- `VW_REPORT_REFRESH_STATUS`, unified grid, pipeline pages: unchanged —
  they key on scope/status, not adjustment type. Grid/status label maps
  gain the 'Upload' type where labels are hardcoded.

## Non-goals

- No FRTB cross-field validation rules yet (structure only).
- No VaR Direct.
- No renaming of `DIRECT_SCOPE_SCHEMA` / `ADJ_LINE_ITEM_JSON` tables.
- No migration of existing `ADJUSTMENT_TYPE='Direct'` dev rows.
- No change to Scaling/EROL paths.

## Acceptance

1. VaR: category shows "VaR Upload"; a pasted legacy-layout file produces
   one header + one dimension entry + unpivoted fact rows, type/action
   'Upload' (parity with today's behaviour).
2. Stress (representative Direct scope): paste 3 rows with shuffled,
   mixed-case columns → preview shows per-row validation from
   `VW_DIRECT_VALIDATE_STRESS`; a bad book code marks only that row
   invalid; submitting creates 2 headers; processing writes exactly one
   `FACT.STRESS_MEASURES_ADJUSTMENT` row per header with resolved keys,
   −1 defaults, the pasted USD value, and rebuilds the summary; both rows
   reconcile in recon v2.
3. FRTB scopes: same generic behaviour via their own views.
4. Zero-match warning appears if a Direct row fails to insert.
