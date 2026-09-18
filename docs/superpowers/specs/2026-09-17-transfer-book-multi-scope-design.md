# Scaling: type-before-scope, multi-scope submission, Transfer Book — design

Date: 2026-09-17. Author: Marcos Magri (decisions), spec written with Claude.
Status: approved in conversation; awaiting spec review.

## 1. Goals

1. In the Scaling category the user picks the **Adjustment Type before the Data
   Scope**.
2. Data Scope becomes a **multi-select** over the six real scopes (VaR, Stress,
   Sensitivity, FRTBSBM, FRTBDRC, FRTBRRAO) for **Scaling** and **Entity Roll**.
   One adjustment (one `ADJ_HEADER` row) is created **per selected scope**.
   The client-side "FRTBALL" pseudo-scope is removed.
3. With several scopes selected the filter form shows the eight main fields
   (Entity Code, Source System, Department Code, Book Code, Instrument Code,
   Strategy, Trade Typology, Trade Code) and **More filters** shows only the
   fields the engine can apply to *every* selected scope.
4. New Scaling adjustment type **Transfer Book**: at one COB, the source
   book's adjusted values (× a scale factor) are **added** to the target book
   — *amended 2026-09-18 by Marcos; this originally read "the target book is
   replaced by the source book's adjusted values"*. Source book untouched, and
   the target book keeps everything it already has. Optional trade codes
   restrict the transfer; **one adjustment per trade code** (and per scope).

Decisions taken by Marcos during design:

| Question | Decision |
|---|---|
| Transfer semantics | ~~**Copy, like Roll**: target book's rows in scope are flattened and replaced by the source book's adjusted values; source untouched.~~ **Amended 2026-09-18 (Marcos) — APPEND, not replace**: the source book's adjusted values × the scale factor are *added* to the target book. Nothing already on the target (its originals or its own adjustments) is flattened or superseded; source untouched. `combined(target) = whatever the target already had + factor × adjusted(source)`. |
| Trade with no version under the target book at the COB | Copied rows use the target book's `'<BOOK_CODE>/Adjustment'` trade (same fallback as the Direct path). |
| Engine placement | Extend the Scale path (netted UNION) rather than a new action or Direct line items. |
| Multi-scope | Client-side fan-out (generalised FRTBALL loop), no new server contract. |
| Filters for several scopes | Per-scope applicability table in the page; More filters = intersection. |

Non-goals: recurring Transfer Book; a "move" semantic (source flattened);
multi-scope for Direct Adjustment / VaR Upload; server-side batch submit.

## 2. Current state (what the change touches)

- `streamlit_app/pages/1_New_Adjustment.py` — single-screen order ticket.
  Scaling sections: 1 Category → 2 Data Scope (`_render_scope_pills`, l.1036)
  → 3 Adjustment Type (l.1522) → 4 Date & Schedule → 5 Dimension Filters
  (`_render_main_filters` l.1402, `_render_extra_filters` l.1459) → 6 Business
  Context. `wiz["process_type"]` is one string. FRTBALL fan-out in
  `_do_submit` l.561–583, preview fan-out `_run_preview` l.3027–3041, sign-off
  fan-out `_signoff_scopes` l.3087. `SCOPE_FIELDS` l.625 / `ALL_EXTRA_FIELDS`
  l.641 hard-code the filter lists.
- `03_sp_submit_adjustment.sql` — `ACTION_MAP` l.40 validates
  `ADJUSTMENT_TYPE`; `compute_scale_factor_adjusted` l.123; factor-1 (Scale
  only) and same-COB guards l.315–335; `col_map` l.567 writes flat filter
  columns.
- `05_sp_process_adjustment.sql` Scale path l.1329–1990: legs ①②③ (l.1786),
  `_dim_filters` l.1540, roll leg l.1723, SCD2 remap l.1869, supersede by
  filter after it.
- `04_sp_preview_adjustment.sql` — mirrors the filter builder; `is_roll` l.86.
- `01_tables.sql` — `ADJ_HEADER` l.30–125 (append-only), `ADJUSTMENTS_SETTINGS`.
- `utils/styles.py` — `TYPE_CONFIG` l.291, `render_filter_chips` l.1654.

## 3. Wizard flow (page)

### 3.1 Section order for Scaling

1 Category → **2 Adjustment Type** (Flatten / Scale / Roll / **Transfer Book**)
→ **3 Data Scope** (multi) → 4 Date & Schedule → 5 Filters *(or 5 Transfer
Details for Transfer Book)* → 6 Business Context.

`wiz["adjustment_type"]` values: `Flatten | Scale | Roll | Transfer`.
`TYPE_CONFIG` gains `"Transfer": {label "Transfer Book", desc, formula
"target book = adjusted(source book) at the COB"}`.

### 3.2 Scope multi-select

- New `wiz["process_types"]: list[str]` replaces `wiz["process_type"]` for
  Scaling and Entity Roll. `wiz["process_type"]` stays for Direct / VaR
  Upload (single). A helper `_selected_scopes()` returns the list for any
  category (single-scope categories return `[process_type]`).
- Widget: `st.pills(options, selection_mode="multi", format_func=scope_label)`
  over `ALL_SCOPES` (`styles.py` l.159, already excludes FRTBALL). Runtime is
  pinned to Streamlit 1.50 (`st.pills` needs ≥ 1.40); keep the existing
  `_st_version()` guard pattern and fall back to a `st.multiselect` below 1.40.
- Default: none selected; ticket checklist item "Select at least one scope".
- Changing the selection purges filter values no longer applicable
  (extends the existing purge at l.1046–1065 to the intersection rule).
- `FRTB_SUBTYPES`/`FRTBALL` label (l.616–621) and the FRTB sub-row of pills
  are deleted; the six scopes are one flat pill group. `_render_scope_pills`
  keeps its name; `include_frtball` parameter removed.

### 3.3 Filter applicability

New page-level constant `SCOPE_FILTER_FIELDS: dict[scope, set[filter_key]]`,
derived from the engine's lookup rules in `_dim_filters` (05 l.1540–1665), the
direct-column equality joins (05 l.1691) and the base fact columns
(`fact_schema.csv`). Rule: a filter is offered for a scope only if the engine
applies it there.

| filter key | VaR | Stress | Sens | FRTBSBM | FRTBDRC | FRTBRRAO | how applied |
|---|---|---|---|---|---|---|---|
| entity_code | Y | Y | Y | Y | Y | Y | direct or ENTITY_KEY lookup |
| source_system_code | Y | Y | Y | Y | Y | Y | direct |
| department_code, book_code, trader_code, guaranteed_entity, region_key | Y | Y | Y | Y | Y | Y | BOOK_KEY lookup |
| trade_code, strategy, trade_typology | Y | Y | Y | Y | Y | Y | TRADE_KEY lookup |
| instrument_code | Y | Y | Y | Y | Y | Y | COMMON_INSTRUMENT_KEY lookup (SBM: direct) |
| currency_code | Y | . | Y | Y | Y | Y | direct (Stress has TRADE_CURRENCY only) |
| measure_type_code | . | . | Y | Y | Y | Y | MEASURE_TYPE_KEY lookup |
| simulation_name | . | Y | . | Y | Y | Y | Stress: lookup; FRTB*: direct column |
| simulation_source | . | Y | . | . | . | . | STRESS_SIMULATION lookup |
| var_component_name, var_sub_component_name, day_type | Y | . | . | . | . | . | VAR_SUB_COMPONENT lookup |
| scenario_date_id | Y | . | . | . | . | . | direct |
| tenor_code | . | . | Y | Y | . | . | TENOR_CURRENCY lookup |
| underlying_tenor_code | . | . | Y | . | . | . | lookup |
| curve_code | . | . | Y | Y | . | . | lookup |
| product_category_attributes | . | Y | Y | Y | Y | Y | PCA lookup |
| batch_region_area, murex_family, murex_group | . | . | . | . | . | . | **never applied** (no fact column) |

Consequences: the three never-applied fields disappear from the form (they
remain on `ADJ_HEADER` for old rows). Nothing else a user can pick today is
removed for a single scope except where it was already ignored by the engine.

Rendering:

- **One scope**: main row = the four defaults (+ VaR row as today); More
  filters = `SCOPE_FILTER_FIELDS[scope]` minus main fields, ordered as
  `SCOPE_FIELDS` then `ALL_EXTRA_FIELDS` (existing tiering kept).
- **Several scopes**: main row = Entity Code, Source System, Department Code,
  Book Code, Instrument Code, Strategy, Trade Typology, Trade Code (two rows of
  four); More filters = intersection of the selected scopes' sets minus the
  main fields. If the intersection is empty the expander is not rendered.
- Completion rule unchanged: Entity required + at least one of Department /
  Book (/ VaR Component for VaR-only selections).

### 3.4 Multi-scope submission and preview

- `_do_submit`: replace the FRTBALL branch with a loop over
  `_selected_scopes()`; one `SP_SUBMIT_ADJUSTMENT` call per scope (× per trade
  for Transfer, §5). Result aggregation as today: all accepted → success
  screen listing the created ids per scope; any failure → `status: "Error"`
  naming accepted scopes and failures, no rollback.
- `_run_preview`: loop over scopes, sum numeric summary columns (existing
  code, generalised); per-row breakdown/sample shown only for a single scope
  (existing rule). For Transfer with several trades the preview runs per
  scope with all selected trades in one call (the preview accepts a list,
  §6) and reports one row per trade in `breakdown` mode.
- `_signoff_scopes` returns the selected list; the sign-off panel already
  loops per scope.
- Ticket summary: "Scopes: VaR, Stress — 2 adjustments will be created"
  (× trades for Transfer).

### 3.5 Entity Roll

Only §3.2 applies (scope pills become multi). Roll Details and the wipe
preview run per scope (the recon counts are shown per scope in one table).
Submission loops per scope; each scope's serverless task processes its own
EROL (one roll per call, others requeued — unchanged).

## 4. Transfer Book form (section 5 for type = Transfer)

Two cards side by side.

**Source**
- COB Date (the shared `cobid` from section 4; no Source COB field).
- Book Code: searchable `selectbox` over `DIMENSION.BOOK` current rows
  (`IS_CURRENT_ROW = TRUE`), label `BOOK_CODE — DEPARTMENT_CODE`. Caption
  under it: "Entity: {ENTITY_CODE}".
- Trade Codes (optional): `multiselect` over `DIMENSION.TRADE` current rows
  where `BOOK_CODE = source book`, sorted, with a count caption ("312 trades in
  this book"). Empty = whole book.

**Target**
- Book Code: same dropdown, excluding the source book. Caption "Entity:
  {ENTITY_CODE}". If the target entity differs from the source entity show an
  info line "Rows will be reported under entity {target entity}".

Ticket checklist for Transfer: scope(s) selected, COB, source book, target
book ≠ source, reason. Recurring occurrence is not offered for Transfer
(`occurrence` forced to ADHOC). A **Scale Factor** applies exactly as for
Roll — 1 copies the source book as-is, 1.10 adds 10% (amended 2026-09-18,
Marcos; the original design fixed it at 1).

Wizard state: `wiz["source_book_code"]`, `wiz["target_book_code"]`,
`wiz["transfer_trade_codes"]: list[str]`. Reset on type change.

## 5. Storage and submit

### 5.1 `ADJ_HEADER`

Append one column at the end of the `CREATE OR ALTER TABLE` (append-only):
`SOURCE_BOOK_CODE VARCHAR(20) COLLATE 'en-ci'` — "Transfer Book only: the
book whose adjusted values replace BOOK_CODE at the COB". Same for
`ADJ_RECURRING_TEMPLATE` is **not** needed (no recurring transfers).

### 5.2 Payload (page → `SP_SUBMIT_ADJUSTMENT`)

One call per scope per trade:

```json
{"cobid": 20260916, "process_type": "VaR", "adjustment_type": "Transfer",
 "username": "...", "source_cobid": 20260916, "scale_factor": 1,
 "book_code": "<TARGET>", "source_book_code": "<SOURCE>",
 "trade_code": "<TRADE or absent>", "reason": "...",
 "adjustment_occurrence": "ADHOC", "requires_approval": false,
 "adjustment_category": "...", "global_reference": "..."}
```

No other filter keys are sent for a Transfer.

### 5.3 `SP_SUBMIT_ADJUSTMENT` (03)

- `ACTION_MAP["transfer"] = "Scale"`.
- `compute_scale_factor_adjusted`: `transfer` → `float(scale_factor)` (full
  factor, i.e. 1.0 — like cross-COB), never `sf − 1`.
- Guards: factor-1 rejection applies to `scale` only (unchanged); the
  same-COB guard for `scale`/`flatten` unchanged; new Transfer guards:
  `source_book_code` required, `book_code` required, `book_code ≠
  source_book_code`, `source_cobid == cobid`, both books exist in
  `DIMENSION.BOOK` current rows (else `status: "Error"` with a plain message).
- **Entity derivation**: for Transfer, `entity_code` is set server-side to the
  target book's `DIMENSION.BOOK.ENTITY_CODE` (ignore any client value). The
  sign-off check, blocking/overlap check (`dim_vals`), notifications and every
  list page then work unchanged.
- `col_map` gains `"source_book_code": "SOURCE_BOOK_CODE"`.
- Overlap check: Transfer participates like Scale (dims: entity, book,
  trade_code) — a pending Flatten on the target book blocks it, as intended.

### 5.4 Display

`render_filter_chips` (`styles.py` l.1654) gains `SOURCE_BOOK_CODE` labelled
"From book". Adjustments grid / ticket: type label "Transfer Book"; the
existing "Roll from COB" line pattern becomes "From book {SOURCE_BOOK_CODE}"
for transfers.

## 6. Engine — `SP_PROCESS_ADJUSTMENT` Scale path (05)

Definitions per `adjust` row: `is_transfer := adjust.SOURCE_BOOK_CODE IS NOT
NULL`; `is_cross_cob := adjust.COBID <> adjust.SOURCE_COBID`.

### 6.1 Legs

```
① same-COB Scale/Flatten:  fact.COBID = SOURCE_COBID AND COBID = SOURCE_COBID
                           AND SOURCE_BOOK_CODE IS NULL                 (changed)
② roll (cross-COB):        unchanged
②T transfer (new):         see below
③ flatten target:          fact.COBID = COBID AND COBID <> SOURCE_COBID
```

> **Amended 2026-09-18 by Marcos — append semantics.** Leg ③ is reverted to
> the cross-COB Roll condition only (`COBID <> SOURCE_COBID`). A Transfer Book
> gets **leg ②T and nothing else**: no leg ①, no flatten leg ③. It only ADDS
> rows, so the target book keeps its originals and its own adjustments.

Batch flags: `has_cross_cob` (existing) and new `has_transfer` (any claimed
row with `SOURCE_BOOK_CODE`). Leg ②T is emitted only when `has_transfer`.
Like Roll, Transfer requires a distinct `FACT_ADJUSTED_TABLE`; raise the same
loud error otherwise.

### 6.2 Leg ②T — source rows re-keyed to the target book

Reads `FACT_ADJUSTED_TABLE fact` at `fact.COBID = adjust.SOURCE_COBID` (=
COBID). Predicates: **only** the source book and the optional trade code —
*not* the header's ENTITY_CODE / DEPARTMENT_CODE (they describe the target):

```
AND EXISTS (SELECT 1 FROM DIMENSION.BOOK sb
            WHERE sb.BOOK_KEY = COALESCE(fact.BOOK_KEY,-1)
              AND UPPER(sb.BOOK_CODE) = UPPER(adjust.SOURCE_BOOK_CODE))
AND (adjust.TRADE_CODE IS NULL OR EXISTS (SELECT 1 FROM DIMENSION.TRADE st
            WHERE st.TRADE_KEY = COALESCE(fact.TRADE_KEY,-1)
              AND UPPER(st.TRADE_CODE) = UPPER(adjust.TRADE_CODE)))
```

Select list = leg ②'s `select_scale_adj` with these columns **overridden**
(only when the column exists on the `_ADJUSTMENT` table):

| column | value |
|---|---|
| `BOOK_KEY` | `tb.BOOK_KEY` — target book, `DIMENSION.BOOK` current row by `adjust.BOOK_CODE` |
| `BOOK_CODE` (FRTB tables) | `adjust.BOOK_CODE` |
| `ENTITY_KEY` | `te.ENTITY_KEY` of `tb.ENTITY_CODE` |
| `ENTITY_CODE` | `tb.ENTITY_CODE` |
| `TRADE_KEY` | `COALESCE(tt.TRADE_KEY, ta.TRADE_KEY)` where `tt` = `DIMENSION.TRADE` row with the source row's `TRADE_CODE` under `adjust.BOOK_CODE`, effective at the COB date; `ta` = the target book's `'<BOOK_CODE>/Adjustment'` trade effective at the COB |

`tb`, `te`, `tt`, `ta` are LEFT JOINs added to leg ②T's FROM (joined on
`adjust`, and `tt` additionally on the source row's trade via `DIMENSION.TRADE
st ON st.TRADE_KEY = fact.TRADE_KEY`). If `tb` resolves to NULL the leg
yields no rows; the submit guard makes this unreachable, and the run log
records the row count.

Metrics: `× adjust.SCALE_FACTOR_ADJUSTED` (the full factor, as in leg ②).

### 6.3 Why re-key inside the leg

`fact_key` computes the surrogate key from the emitted columns (BOOK_KEY,
TRADE_KEY, ENTITY…). ~~Leg ③'s flatten rows carry the target book's keys; leg
②T must carry the **same** keys for the same position so `netted` cancels
them (`combined(target) = adjusted(source)`).~~ Re-keying after netting (as the
SCD2 UPDATE does for Roll) would leave source-keyed and target-keyed rows
un-netted.

> **Amended 2026-09-18 by Marcos — append semantics.** There is no leg ③ for a
> transfer, so nothing has to cancel. The rows are still re-keyed inside the
> leg so the ADDED rows sit on the **target book's** positions (book, entity,
> the trade's version under the target book) and are reported there. Two
> consequences of append:
>
> - **Supersede**: `supersede_sql`'s inner `EXISTS` gains
>   `AND adjust.SOURCE_BOOK_CODE IS NULL` — a transfer deletes no earlier
>   adjustment row in the target scope. Non-transfer headers in the same batch
>   supersede exactly as before.
> - **`ranked`**: when the batch carries transfers, the DENSE_RANK partition
>   becomes `PARTITION BY {key_name}, CASE WHEN ADJUSTMENT_ID IN
>   (<transfer dim ids>) THEN ADJUSTMENT_ID ELSE -1 END`, so no other row in
>   the batch can displace a transfer's row (two transfers into one target
>   book, a Scale on the target, two fallback-trade transfers). They all
>   survive and SUM. The old v1 fallback-collapse limitation (UAT TRF-05) is
>   gone. With no transfer in the batch the original clause is emitted
>   verbatim. Single-column-PK (FRTB) keys also carry
>   `|| '-' || adjust.DIMENSION_ADJ_ID` so two transfers never write one key.

### 6.4 Steps after the UNION

- Rounding residual, staging, `perm_insert`: unchanged.
- SCD2 remap (l.1869): its `adj_cte` filter `ad.COBID <> ad.SOURCE_COBID`
  already excludes transfers (same COB). No change; add a comment.
- Supersede by filter: ~~unchanged — the header's filters (target book, trade,
  entity) describe the target scope, so earlier adjustments in the target
  book (for that trade) are removed.~~ **Amended 2026-09-18 by Marcos**: a
  transfer supersedes **nothing** (see §6.3). Source-book adjustments are
  untouched, and so are the target book's.
- Summary rebuild, downstream hand-off: unchanged.
- `_erlog` step labels: `stage_build` SQL contains the new leg; add a
  `transfer_leg` note in the batch log ctx for post-mortem.

### 6.5 Preview (04)

`is_transfer = adjustment_type == "transfer"`. Modes:

- `summary`: ~~`PROJECTED = Σ adjusted(source book, COB, trade)`~~, `EXISTING =
  Σ original(target book, COB, trade)`, `EXISTING_ADJ = Σ adjustment rows in
  the target scope`, plus the source split lines (`SOURCE_ORIGINAL_VALUE`,
  `SOURCE_ADJUSTMENTS_VALUE`, `SOURCE_ADJUSTED_VALUE`) as Roll reports.

  > **Amended 2026-09-18 by Marcos — append maths.**
  > `TOTAL_ADJUSTMENT_DELTA = factor × SOURCE_ADJUSTED_VALUE` (what is added)
  > and `TOTAL_PROJECTED_VALUE = TOTAL_CURRENT_VALUE + factor ×
  > SOURCE_ADJUSTED_VALUE`. `ROWS_AFFECTED`, `NONZERO_ROWS` and the source
  > split are unchanged.
  >
  > `TOTAL_CURRENT_VALUE` now reads **`FACT_ADJUSTED_TABLE`** for the target
  > side (the `tgt` CTE; `tgt_where` itself is unchanged), so "current" is
  > `adjusted(target)` — the book's originals **plus the adjustments already on
  > it**. That is the one place the transfer preview differs from Roll's: a
  > transfer supersedes none of those adjustments, so reading the base fact
  > would understate the book by exactly their value and make "projected" wrong
  > by the same amount. **Roll's branch is unchanged** and still reads the base
  > fact for its target side, because a Roll *does* flatten the target's
  > originals and supersede its adjustments.
  >
  > The per-trade `breakdown` is unchanged — its `PROJECTED_VALUE` was always
  > the factored amount added per trade, and the page now labels that column
  > "Value added".
- `breakdown`: one row per trade code (accepts `trade_codes: [..]` in the
  payload for the multi-trade preview; the header still stores one trade).
- `sql`: the generated text, as today.

Source predicate = §6.2's two EXISTS with the payload's `source_book_code` /
`trade_code(s)`; target predicate = the normal filter builder.

## 7. Approval, sign-off, lifecycle

- Transfer is a Scaling adjustment: approval optional (form checkbox), no
  forced approval. Sign-off gate uses the derived (target) entity.
- Retry / Delete / Recall / supersede: unchanged behaviour.
- `TYPE_CONFIG` label drives the pills in Adjustments/Home grids; status
  badges unchanged.

## 8. Testing

Automated (tests/, fake COB 20991231, bots `UAT_BOT_*`):

- TRF-01 submit: Transfer accepted; header has `ADJUSTMENT_TYPE='Transfer'`,
  `ADJUSTMENT_ACTION='Scale'`, `SCALE_FACTOR_ADJUSTED`= the submitted factor,
  `BOOK_CODE`=target,
  `SOURCE_BOOK_CODE`=source, `ENTITY_CODE` = target book's entity from
  `DIMENSION.BOOK`.
- TRF-02 guards: same book, unknown book, missing source book → `Error`.
- TRF-03 preview `mode='sql'` for a Transfer contains the source-book EXISTS,
  the target BOOK_KEY override and the `/Adjustment` COALESCE.
- TRF-04 overlap: a pending Flatten on the target book blocks a Transfer.
- MSC-01 page-level (AppTest with fake session): three scopes selected →
  three submit calls with the scope swapped; FRTBALL absent from pills.
- MSC-02 filter intersection (pure function over `SCOPE_FILTER_FIELDS`):
  VaR+Stress → main eight + More = {trader_code, guaranteed_entity,
  region_key}; Stress+Sensitivity → main eight + More = {trader_code,
  guaranteed_entity, region_key, product_category_attributes}; all six →
  main eight + More = {trader_code, guaranteed_entity, region_key}.

**Open item (needs Marcos):** an end-to-end processing test requires fact
rows at the fake COB in `FACT.*_MEASURES` and the adjusted view. The suite
does not write to FACT today. Options: (a) DBA-provided fixture rows at
20991231 in DVLP only; (b) run the existing `SP_DEBUG_ENTITY_ROLL`-style
read-only SQL dump for Transfer (`SP_DEBUG_TRANSFER`) and assert its text.
Default if undecided: (b).

## 9. Deploy

1. `01_tables.sql` (`ADJ_HEADER` + `SOURCE_BOOK_CODE`).
2. `03`, `04`, `05` procedures.
3. App. Old headers without `SOURCE_BOOK_CODE` are unaffected (NULL = not a
   transfer).

## 10. Risks

- Volume: a whole-book transfer on VaR scans the combined view for one book
  at one COB — far smaller than Entity Roll; no new staging needed.
- Trade fallback hides data-quality gaps (trade missing under the target
  book). Mitigation: preview `breakdown` shows per-trade "target trade
  found / fallback" so the user sees it before submitting.
- Multi-scope partial failure leaves siblings created (existing FRTBALL
  behaviour); the success screen names them so the user can delete.
