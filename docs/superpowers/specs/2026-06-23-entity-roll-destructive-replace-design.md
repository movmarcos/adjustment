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

## Decisions (from brainstorming)

- **Wipe scope:** entity-scoped only — adjustments where `ADJ_HEADER.ENTITY_CODE =`
  the rolled entity (any type: prior EROL, Scale, Flatten, Direct). Globals
  (`ENTITY_CODE = NULL`) are untouched.
- **Timing:** at processing, after approval, inside the existing EROL transaction.
  A rejected roll deletes nothing.
- **UI:** rewrite the banner as destructive, show the count of what will be
  removed, and gate Submit behind an agreement checkbox.
- **Summary:** recalculated after the roll for scopes that configure one.

## Backend — `SP_PROCESS_ADJUSTMENT`, EROL path (`05_sp_process_adjustment.sql`)

New sequence inside the existing `BEGIN/COMMIT`:

1. **Find priors** — `ADJ_HEADER WHERE COBID=target AND ENTITY_CODE=entity AND
   IS_DELETED=FALSE AND ADJ_ID NOT IN (<this roll>)`. Collect `ADJ_ID`s and
   non-null `DIMENSION_ADJ_ID`s.
2. **Flag deleted** — `ADJ_HEADER` → `IS_DELETED=TRUE, RUN_STATUS='Superseded',
   DELETED_BY='ENTITY_ROLL', DELETED_DATE, ERRORMESSAGE='Superseded by Entity Roll
   <adj_id>'`; `DIMENSION.ADJUSTMENT` → `IS_DELETED=TRUE, RUN_STATUS='Superseded'`.
3. **Delete their rows** — the priors' `DIMENSION_ADJ_ID`s are added to the
   existing `clear_ids` set, so the existing `DELETE FROM <ADJUSTMENTS_TABLE>
   WHERE COBID=target AND ADJUSTMENT_ID IN (clear_ids)` removes them.
4. **Flatten + copy** — the existing `insert_sql` runs unchanged. Because it reads
   the combined view *after* the delete (same transaction), leg ① flattens only
   the entity's original+global base ⇒ `combined(target) = adjusted(source)` with
   no leftover rows.
5. **Summary** — the existing guarded rebuild (`if fact_adj_summary_name`) runs
   last, re-aggregating the final adjustment table for the COB. FRTB scopes
   (NULL summary) are skipped. Robust to the combined-table type — it reads the
   adjustment table, not the combined view.
6. **Audit** — one `ADJ_STATUS_HISTORY` row per superseded adjustment.

### Correctness — why delete-first is safe here

The result must stay `combined(target)=adjusted(source)` while physically removing
priors. That only holds if leg ① reads the *post-delete* combined view. The
existing code already deletes adjustment rows (its own prior failed attempt) and
then reads the combined view within the same `BEGIN/COMMIT`, so the codebase
already relies on the combined view reflecting in-transaction deletes (i.e. the
`*_COMBINED` / `*_ADJUSTED` objects behave as synchronous views). We reuse exactly
that mechanism — no change to the netting SQL, no fragile add-back leg.

> Verification: the test asserts `combined(target)=adjusted(source)` directly, so
> if a `*_COMBINED` object were ever a lagging dynamic table the test would fail
> loudly rather than silently corrupting numbers.

## UI — `1_New_Adjustment.py`

- **Banner** (`render_entity_roll_form`) rewritten from "reversible / no data
  deleted" to a red destructive warning.
- **Impact preview** — after Roll Details, when COB + entity are set, query
  `ADJ_HEADER` for prior entity-scoped non-deleted adjustments into
  `wiz["_eroll_supersede_ids"]` and show "N existing adjustment(s) will be
  permanently removed" (mirrors the Direct-Adjustment dup pattern).
- **Agreement checkbox** near Submit (Entity Roll only): "I understand this Entity
  Roll will permanently remove N existing adjustment(s) …".
- **Gate** — Submit `disabled` gains `or not eroll_ok`.

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

- Only entity-scoped headers (`ENTITY_CODE = entity`) are touched; globals intact.
- Unprocessed priors (no `DIMENSION_ADJ_ID`) have no rows to drop but are still
  flagged so they cannot later overwrite the roll.
- All destructive DML is inside the EROL transaction; failure rolls back the flag,
  the delete, and the roll together.
- Empty prior set ⇒ behaves exactly as before.

## Testing — `tests/test_entity_roll_adjustment.sql`

Captures prior entity-scoped adjustments at baseline, then asserts after
processing: (a) `adjusted(target)=adjusted(source)`, (b) FACT_TABLE untouched,
(c) DIMENSION type EROL/Processed, (e) every captured prior is
`IS_DELETED`+`Superseded` and its rows are gone from the adjustment table. Seed a
prior entity-scoped adjustment before running to exercise (e); with none it is
vacuously PASS.
