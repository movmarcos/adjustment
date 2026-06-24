# Entity Roll — destructive replace + consent

**Date:** 2026-06-23
**Status:** Approved — implemented
**Supersedes behaviour in:** 2026-06-11-entity-roll-flatten-design.md (the "no physical delete / fully reversible" promise)

## Problem

Entity Roll (EROL) makes an entity's adjusted figures at a target COB equal the
source COB's adjusted figures. Today it does this purely with offset rows under a
new `ADJUSTMENT_ID` — prior adjustments for the entity stay in the table (their
effect cancelled in aggregate by the flatten leg) and their `ADJ_HEADER` rows
remain active. The desired behaviour is a clean destructive replace: prior
entity-scoped adjustments at the target COB are flagged deleted and their data
removed, then the roll is applied. Users must be warned and consent before submit.

## Decisions (from brainstorming + real-data review)

- **Wipe scope:** ALL adjustments for the entity at the target COB. A roll means
  the target COB's data for the entity is wrong/missing (e.g. bank holiday), so
  the whole entity is rebuilt from source. Adjustments can be loaded by EXTERNAL
  systems that bypass `ADJ_HEADER`, so `DIMENSION.ADJUSTMENT` and the fact table
  can hold more adjustments than `ADJ_HEADER` (observed: 1 header vs 44 fact for
  one COB+entity). We flag every `ADJ_HEADER` + `DIMENSION.ADJUSTMENT` row for the
  COB+entity deleted and physically delete every entity row at the COB from the
  fact `ADJUSTMENTS_TABLE`.
- **Reconciliation:** report distinct adjustment counts across the three sources
  (header / dimension / fact) and whether dimension == fact — surfaced in the UI
  preview and logged/returned by the SP.
- **Timing:** at processing, after approval, inside the existing EROL transaction.
  A rejected roll deletes nothing.
- **UI:** destructive banner, the count to be removed (the dimension/fact count,
  not the header count), the 3-way reconciliation, and an agreement checkbox
  gating Submit.
- **Summary:** recalculated after the roll for the rolled entity only.

## Backend — `SP_PROCESS_ADJUSTMENT`, EROL path (`05_sp_process_adjustment.sql`)

Sequence (reconciliation first, then the wipe + roll inside `BEGIN/COMMIT`):

1. **Entity predicate** for the fact `ADJUSTMENTS_TABLE` — `(ENTITY_KEY IN (…) OR
   ENTITY_CODE = entity)` over whichever columns the table exposes, so both
   external rows (real `ENTITY_KEY`) and roll rows (may default `ENTITY_KEY=-1`
   but carry `ENTITY_CODE`) are caught.
2. **Reconciliation** — count distinct adjustments in `ADJ_HEADER`,
   `DIMENSION.ADJUSTMENT` (both by `COBID+ENTITY_CODE`), and the fact table (by
   the entity predicate); `print` + `result["reconcile"]` with `match = (dim ==
   fact)`.
3. **Flag deleted** — `DIMENSION.ADJUSTMENT WHERE COBID=target AND
   ENTITY_CODE=entity AND ADJUSTMENT_ID <> <roll>` → `IS_DELETED=TRUE,
   RUN_STATUS='Superseded'` (catches external rows); `ADJ_HEADER WHERE
   COBID=target AND ENTITY_CODE=entity AND ADJ_ID NOT IN (<roll>)` →
   `IS_DELETED, RUN_STATUS='Superseded', DELETED_BY, DELETED_DATE, ERRORMESSAGE`.
4. **Wipe rows** — `DELETE FROM <ADJUSTMENTS_TABLE> WHERE COBID=target AND
   <entity predicate>` (single predicate, all adjustments — no per-ID list).
5. **Flatten + roll** — **two straight INSERTs** under the new `ADJUSTMENT_ID`
   (not a `UNION ALL` + surrogate-key `GROUP BY` net):
   - leg ① flatten: `SELECT … metric * -1 FROM combined WHERE COBID = target AND
     <entity>` — negates the entity's current combined at the target (= its base,
     since its adjustments were just wiped), so the base cancels;
   - leg ② roll: `SELECT … metric FROM combined WHERE COBID = source AND <entity>`
     — copies the source COB's combined.
   The combined view SUMs both legs per position ⇒ `combined(target) =
   combined(source)`. See Performance for why the net was dropped.
6. **Summary** — entity-scoped rebuild (see Performance).

### Correctness — why wipe-first is safe here

The result must stay `combined(target)=adjusted(source)` while physically removing
all the entity's prior rows. That holds because leg ① reads the *post-wipe*
combined view. The existing code already deletes adjustment rows and then reads
the combined view within the same `BEGIN/COMMIT`, so the codebase already relies
on the combined view reflecting in-transaction deletes (i.e. the `*_COMBINED` /
`*_ADJUSTED` objects behave as synchronous views). We reuse exactly that
mechanism — no change to the netting SQL.

> Verification: the test asserts `combined(target)=adjusted(source)` directly, so
> if a `*_COMBINED` object were ever a lagging dynamic table the test would fail
> loudly rather than silently corrupting numbers.

## UI — `1_New_Adjustment.py`

- **Banner** (`render_entity_roll_form`) rewritten from "reversible / no data
  deleted" to a red destructive warning.
- **Impact preview + reconciliation** — after Roll Details, when COB + entity +
  scope are set, query the three sources (`ADJ_HEADER`, `DIMENSION.ADJUSTMENT` by
  `COBID+ENTITY_CODE`; fact table resolved from `ADJUSTMENTS_SETTINGS`, counted by
  `ENTITY_KEY`). Show "N existing adjustment(s) will be permanently removed" (N =
  max of dimension/fact) plus a "header X · dimension Y · fact Z · match/⚠"
  reconciliation line. Stored in `wiz["_eroll_remove_count"]`.
- **Agreement checkbox** near Submit (Entity Roll only): "I understand this Entity
  Roll will permanently remove N existing adjustment(s) … (including data from
  other systems)".
- **Gate** — Submit `disabled` gains `or not eroll_ok`.

## Performance — the big wins

Three things made the roll run for an hour and time out; all are fixed:

1. **No surrogate-key netting.** The old INSERT did `UNION ALL` of both legs, built
   a surrogate key per row, and `GROUP BY`'d the whole combined set with `HAVING
   SUM <> 0`. That GROUP BY over the entire entity's combined rows was the
   hour-long step. Replaced by two plain `INSERT … SELECT` scans (flatten + roll).
   Trade-off: both legs' rows are stored instead of one netted row per position —
   the combined view/summary still sum correctly. Manual benchmark of the
   equivalent copy: ~1 min.
2. **No sub-selects in scans.** Entity filters are literal (`ENTITY_CODE = …`, or a
   one-time-resolved `ENTITY_KEY IN (…)` list) instead of a correlated
   `DIMENSION.ENTITY` sub-select embedded in every scan/DELETE.
3. **The wipe DELETE is a single direct predicate** (`COBID + ENTITY_CODE`), and
   the reconciliation no longer full-scans the fact table during processing.

## Performance — entity-scoped summary rebuild

The EROL summary rebuild previously did `DELETE … WHERE COBID` + `INSERT … SELECT
… WHERE COBID GROUP BY ALL` — re-aggregating **the whole COB's adjustment table
across all entities** on every roll. That cost scaled with total adjustments at
the COB and was the likely timeout cause.

Since a roll only changes one entity's rows, the rebuild is now scoped to that
entity: both the `DELETE` and the re-`INSERT` add `AND <entity_pred>`. The filter
uses `er_entity_col` — the entity column the roll rows are reliably keyed by (the
combined view's column, `ENTITY_KEY` for the affected scopes) — and is applied
only when that column exists in **both** the adjustment and summary tables;
otherwise it falls back to the original whole-COB rebuild. Cost drops from
*O(all adjustments at the COB)* to *O(this entity's adjustments)*.

Correctness: other entities' summary rows are untouched by the roll, so rebuilding
only this entity's slice is exact. Removing superseded prior rows (the destructive
step) also keeps the adjustment table lean, which further reduces both the
combined-view scan and this rebuild over repeated rolls.

## Scope / error handling

- Everything for the COB+entity is wiped (matched by `ENTITY_CODE` for the header/
  dimension flags, and by the `ENTITY_KEY`/`ENTITY_CODE` predicate for the fact
  rows). Other entities and other COBs are never touched.
- The roll's own new `DIMENSION.ADJUSTMENT` row (`new_dim_adj_id`) and `ADJ_HEADER`
  rows are excluded from the flag so the roll isn't self-superseded.
- All destructive DML is inside the EROL transaction; failure rolls back the
  flags, the wipe, and the roll together.
- Empty prior set ⇒ behaves as a plain roll.
- Efficiency: the wipe is a single predicate `DELETE` (no per-ID `IN` list), and
  leg ① is cheaper because the entity's adjustments are gone before it reads the
  combined view.

## Testing — `tests/test_entity_roll_adjustment.sql`

Captures the entity's prior adjustments at baseline from the **fact table**
(authoritative — includes external rows) and prints the 3-way reconciliation,
then asserts after processing: (a) `adjusted(target)=adjusted(source)`,
(b) FACT_TABLE untouched, (c) DIMENSION type EROL/Processed, and (e) every prior
is wiped — `E1` no prior fact rows remain, `E2` `DIMENSION.ADJUSTMENT` for the
COB+entity is superseded, `E3` `ADJ_HEADER` for the COB+entity is superseded.
Seed prior adjustments before running to exercise (e); with none it is vacuously
PASS.
