# Entity Roll v2 (EROL) — flatten + copy via offset rows

Date: 2026-06-11
Status: Implemented (2026-06-12) — SP branch, EROL rename, UI wording, test harness

## Problem

The current Entity Roll (`SP_PROCESS_ADJUSTMENT`, EntityRoll branch) physically
deletes the target COB + entity slice from `FACT_TABLE` and
`FACT_ADJUSTED_TABLE`, then physically copies the source COB's rows in. Two
problems:

1. **Destructive and irreversible.** The target's original data is gone;
   deleting the adjustment afterwards cannot restore it. It also performs
   direct DML on the combined/adjusted tables, which every other adjustment
   type treats as read-only.
2. **`DIMENSION.ADJUSTMENT.ADJUSTMENT_TYPE` is VARCHAR(4).** The type value
   `Entity_Roll` (11 chars) cannot land there.

## Decisions (confirmed with Marcos, 2026-06-11)

- Flatten = **offset rows, no deletes** (the same meaning "flatten" already
  has in the scaling path's leg ③).
- Performance matters: entity rolls move large volumes, so the flow must be
  fully set-based.
- New type name: **`EROL`, end-to-end** (UI payload, ADJ_HEADER,
  DIMENSION.ADJUSTMENT). Human-facing labels stay "Entity Roll".
- Implementation: **dedicated offset legs in the EntityRoll branch** (not
  routed through the generic Scaling Roll machinery) — leaner SQL for an
  entity-wide operation and clean reversibility.

## Design

### 1. Naming — EROL end-to-end

- `1_New_Adjustment.py:132,541` — payload sends `adjustment_type: "EROL"`.
- `03_sp_submit_adjustment.sql:44` — `ACTION_MAP` key becomes `"erol": "EntityRoll"`.
- `ADJ_HEADER.ADJUSTMENT_TYPE` and `DIMENSION.ADJUSTMENT.ADJUSTMENT_TYPE`
  both store `EROL` (fits VARCHAR(4)).
- `compute_scale_factor_adjusted` unchanged (falls to `else` → 0.0; the new
  flow does not use scale factor).
- Display labels ("Entity Roll") unchanged in `styles.py`, wizard,
  Documentation page.

### 2. Processing semantics

The EntityRoll branch keeps its validations (single adjustment at a time,
`ENTITY_CODE` required, `SOURCE_COBID ≠ COBID`, DIMENSION.ADJUSTMENT insert
must succeed before any write) and replaces the delete+copy transaction with
**one set-based INSERT into the ADJUSTMENTS_TABLE** (`FACT.*_ADJUSTMENT`)
built from two legs over the combined view (`FACT_ADJUSTED_TABLE`):

- **Leg 1 — flatten target:** `−metrics` from combined view
  `WHERE COBID = target AND <entity predicate>` — cancels everything the
  entity shows at the target COB (original + existing adjustments).
- **Leg 2 — copy source:** `+metrics` from combined view
  `WHERE COBID = source AND <entity predicate>` — the source entity's full
  adjusted values.
- **Netting:** GROUP BY the surrogate key from `FACT_TABLE_PK` config, SUM
  metrics, `HAVING SUM(metric_usd) <> 0`; all rows carry the new
  `ADJUSTMENT_ID` and `COBID = target`.

Net effect: `combined(target, entity) = adjusted(source, entity)`.
`FACT_TABLE` and `FACT_ADJUSTED_TABLE` receive **zero DML**. Columns the
combined view lacks default to `-1` (KEY/ID) or `NULL` (same convention as the
Roll leg, `05_sp_process_adjustment.sql:817`). Entity predicate: direct match
on text `ENTITY_CODE`, or resolve through `DIMENSION.ENTITY` for numeric
`ENTITY_KEY` (existing `_entity_pred` logic).

### 3. Performance

One INSERT…SELECT scanning two COBID partitions of the combined view filtered
by entity. No fact↔adjust join machinery, no DENSE_RANK. Positions identical
in source and target net to zero and are never written, so row volume is only
actual differences + source-only positions — strictly less data than the old
full physical copy. Re-run idempotency: delete this `ADJUSTMENT_ID`'s own
previous rows first, inside the same transaction as the insert.

### 4. SCD2, summary, bookkeeping

- **SCD2 key fix:** copied rows carry source-COB `TRADE_KEY` /
  `COMMON_INSTRUMENT_KEY` / `COMMON_INSTRUMENT_FCD_KEY`. Reuse the existing
  cross-COB SCD2 UPDATE (line 929) scoped to this adjustment.
- **Summary:** rebuild `ADJUSTMENTS_SUMMARY_TABLE` for the target COB exactly
  like the scaling path (delete COB rows + `GROUP BY ALL` reinsert, line 1022).
- **RECORD_COUNT**, header/dimension status, `ADJ_STATUS_HISTORY`, run log,
  PowerBI trigger: unchanged pattern.
- **No supersede-delete:** the target's earlier adjustment rows stay
  physically present, numerically cancelled by leg 1 — deliberate, see §5.

### 5. Deleting the roll restores the entity

The delete handler (2_Adjustments.py) removes rows by `ADJUSTMENT_ID` from the
adjustments + summary tables. Since the roll never touched base data or other
adjustments' rows, deleting it returns the entity to its exact pre-roll
adjusted state.

### 6. Error handling

Same hard-stops as today, plus: fail before any write when the scope has no
`FACT_ADJUSTED_TABLE` configured — the legs read it, and falling back to the
raw fact table would silently ignore existing adjustments. The error message
names the missing config column.

### 7. Testing

SQL test alongside `tests/test_scaling_adjustment.sql`: seed base rows + an
existing adjustment at both COBs, run EROL, assert:

- (a) combined(target) equals adjusted(source) per position;
- (b) `FACT_TABLE` row count at target unchanged;
- (c) deleting the adjustment restores the original combined values;
- (d) `DIMENSION.ADJUSTMENT.ADJUSTMENT_TYPE = 'EROL'`.

## Out of scope

- Preview support for Entity Roll in `SP_PREVIEW_ADJUSTMENT` (none exists
  today; unchanged).
- Migration of historical `Entity_Roll` header rows (none processed in
  production).
- Any change to the Scaling/Direct paths.
