# Direct Adjustment — Grid Entry, Templates, and Field Coverage

**Date:** 2026-08-06
**Status:** Approved (design review with Marcos, 2026-08-06)
**Builds on:** `2026-08-05-direct-vs-upload-design.md` (Direct = one row → one
adjustment; staging + per-scope validation views + per-row submit all exist).

## Problem

CSV paste/upload suits users who already have a file. Users who are keying a
handful of direct values have no guided way to do it: they must hand-craft a
CSV, and the field set per scope is undocumented in the UI. Marcos supplied
the per-scope field lists (Sensitivity, Stress) that users expect to report.

## Decisions (agreed 2026-08-06)

1. **Third input mode: "Enter in grid"** — an editable `st.data_editor`
   grid inside the Direct Adjustment category, per-scope columns, add/delete
   rows, dimension-backed columns rendered as in-grid dropdowns. Underneath
   it reuses the existing pipeline unchanged: stage batch →
   `VW_DIRECT_VALIDATE_<scope>` verdicts → one `SP_SUBMIT_ADJUSTMENT` call
   per valid row.
2. **Per-scope CSV template download** — one click, canonical headers in
   display order + one example row; serves the paste/upload route.
3. **Field coverage grows to Marcos's lists**, with corrections:
   - **Dropped:** `RECORD_COUNT` (system-stamped) and `SCALE_FACTOR`
     (belongs to Scaling; a Direct row IS the value).
   - **COBID stays batch-level** (the wizard's COB; not a grid column).
   - **REASON becomes an optional per-row column** overriding the batch
     reason for that row; batch reason remains the default.
   - **New filter fields:** `TENOR_CODE`, `CURVE_CODE` (Sensitivity) and
     `PRODUCT_CATEGORY_ATTRIBUTES` (Sensitivity + Stress) — full chain:
     staging column, accepted-columns seed, validation rule, engine key
     resolution.
4. Resulting per-scope field sets (display order):
   - **Sensitivity:** ENTITY_CODE, SOURCE_SYSTEM_CODE, BOOK_CODE,
     TENOR_CODE, CURRENCY_CODE, CURVE_CODE, INSTRUMENT_CODE,
     MEASURE_TYPE_CODE, VALUE_USD, REASON, TRADE_TYPOLOGY, TRADE_CODE,
     PRODUCT_CATEGORY_ATTRIBUTES.
   - **Stress:** ENTITY_CODE, SOURCE_SYSTEM_CODE, BOOK_CODE, CURRENCY_CODE,
     INSTRUMENT_CODE, VALUE_USD, REASON, TRADE_TYPOLOGY, TRADE_CODE,
     SIMULATION_NAME, SIMULATION_SOURCE, PRODUCT_CATEGORY_ATTRIBUTES.
   - **VaR / FRTB scopes:** unchanged sets for now (shared columns +
     REASON), same grid/template treatment.

## Architecture

### A. Config (`DIRECT_ACCEPTED_COLUMNS`)

Add `DISPLAY_ORDER NUMBER` (CREATE OR ALTER append). The grid's column
order and the template's header order both come from the canonical rows
(`ACCEPTED_NAME = STAGE_COLUMN`) sorted by `DISPLAY_ORDER`. Seed the
orders per scope from the lists above; aliases keep `DISPLAY_ORDER NULL`.
New seeds: TENOR_CODE (+alias TENOR), CURVE_CODE (+alias CURVE) for
Sensitivity; PRODUCT_CATEGORY_ATTRIBUTES (+alias PCA) for Sensitivity and
Stress; REASON for every Direct scope.

### B. Staging (`ADJ_DIRECT_STAGE`)

Append columns (CREATE OR ALTER): `TENOR_CODE VARCHAR(10)`,
`CURVE_CODE VARCHAR(50)`, `PRODUCT_CATEGORY_ATTRIBUTES VARCHAR(255)`,
`REASON VARCHAR(1000)` — widths mirror `ADJ_HEADER`.

### C. Validation views

- Sensitivity view adds: TENOR_CODE exists in `DIMENSION.TENOR_CURRENCY`
  where `TENOR_CURRENCY_CODE = CONCAT(TENOR_CODE, '_',
  COALESCE(CURRENCY_CODE, 'USD'))` (the dim's composite convention);
  CURVE_CODE exists in `DIMENSION.CURVE_CURRENCY`; PCA matches
  `DIMENSION.PRODUCT_CATEGORY_ATTRIBUTES` on space-insensitive
  `PCA_CONCAT_KEY`.
- Stress view adds the same PCA rule.
- REASON: no validation (free text).

### D. Engine (`_direct_expr` additions, mirroring the Scale path lookups)

- `TENOR_CURRENCY_KEY` → lookup by `CONCAT(h.TENOR_CODE, '_',
  COALESCE(h.CURRENCY_CODE, 'USD'))`, −1 default.
- `CURVE_CURRENCY_KEY` → lookup by `h.CURVE_CODE`, −1 default.
- `PRODUCT_CATEGORY_ATTRIBUTES_KEY` → lookup by space-insensitive
  `PCA_CONCAT_KEY = REPLACE(h.PRODUCT_CATEGORY_ATTRIBUTES, ' ', '')`,
  −1 default.
(Submit SP already accepts tenor/curve/PCA payload keys and stores them on
the header — no submit change.)

### E. App — grid mode

- Third radio/pill in the Direct branch: Paste / Upload file / **Enter in
  grid**.
- Grid = `st.data_editor(num_rows="dynamic")`, columns from config order.
  Column rendering by stage column: `SelectboxColumn` for ENTITY_CODE,
  BOOK_CODE (narrowed by chosen entity where feasible), MEASURE_TYPE_CODE,
  SIMULATION_NAME, SIMULATION_SOURCE (reusing the existing option
  helpers); `NumberColumn` for VALUE_USD; `TextColumn` otherwise. REASON
  placeholder shows "blank = batch reason".
- "Validate rows" button stages the grid content (same `_stage_direct_batch`
  path, fresh BATCH_ID) and shows the standard verdict preview; Submit is
  the existing per-row loop. Editing the grid invalidates prior verdicts
  (signature covers grid content).
- Per-row REASON: `_direct_row_payload` uses the row's REASON when
  non-blank, else the batch reason.

### F. App — template download

`st.download_button` "Download CSV template" in Paste/Upload modes:
headers = canonical columns in display order, plus one commented example
row of plausible values. Filename `direct_<scope>_template.csv`.

## Non-goals

- No per-row COBID; no SCALE_FACTOR/RECORD_COUNT entry.
- No "start from existing data" prefill (possible later phase).
- No change to VaR Upload, Scaling, EROL.
- FRTB cross-field rules remain pending (unchanged extension point).

## Acceptance

1. Sensitivity grid shows the 13 columns in the specified order; entity,
   book, measure type are dropdowns; a row with tenor `1Y`, currency USD
   validates against `TENOR_CURRENCY` and processes with a real
   `TENOR_CURRENCY_KEY` (not −1) on the fact row.
2. Stress grid shows its 12 columns; simulation name+source dropdowns;
   PCA value with spaces matches its dimension row.
3. A grid batch with one bad code shows ✗ on that row only; submit creates
   headers only for ✓ rows; per-row REASON lands on those headers, blank
   rows inherit the batch reason.
4. Template download for each scope opens in Excel with the right headers;
   pasting it back (with values) validates green.
5. CSV route accepts the new TENOR/CURVE/PCA/REASON columns by name.
