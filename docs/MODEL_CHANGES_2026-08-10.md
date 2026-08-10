# RaptorReporting — Model Changes, 2026-08-10

**Model:** `RaptorReporting.bim` (compatibility level 1601)
**Scope:** metadata-only hygiene changes; no partitions, no M expressions, no DAX, no relationships were modified.
**Motivation:** reduce processing (refresh) cost and enforce modelling best practices ahead of the incremental-window review.
**Tooling:** changes applied programmatically to the .bim; reviewable as a normal git diff. All edits verified with a semantic (object-level) diff against the original file — only the four changes below are present.

---

## 1. `IsAvailableInMDX = false` on hidden columns — 856 columns

Every hidden column (or column of a hidden table) now has `isAvailableInMdx: false`, **except** 42 columns that are referenced by another column's `SortByColumn` or used as a hierarchy level — those keep their attribute hierarchies, as disabling them would break sorting/hierarchies.

**Why:** Analysis Services builds an attribute hierarchy for every column that allows it, during the Process Recalc phase of every refresh. Hidden columns (surrogate keys, technical flags) are never used from MDX clients, so these structures are pure processing and memory overhead. This is the standard Best Practice Analyzer rule and the one change in this batch that directly reduces refresh time.

**Impact on users/reports:** none. Hidden columns were already invisible in Power BI and in Analyze-in-Excel field lists. DAX, relationships and RLS are unaffected (`IsAvailableInMDX` only controls MDX attribute-hierarchy generation).

## 2. `SummarizeBy = None` on numeric key columns — 179 columns

All numeric columns whose name ends in `Key`/`ID` had their default aggregation set to **Don't summarize**. They were previously defaulting to Sum or Count, e.g.:

| Table | Column | Before | After |
|---|---|---|---|
| Book | BOOK_KEY | Sum | None |
| Instrument | INSTRUMENT_KEY | Sum | None |
| VaR Adjustment Summary Import | ADJUSTMENT_ID | Count | None |
| Fact Percentile Calc | WINDOWENDDATEID | Count | None |

**Why:** dragging a key column into a visual should never produce a meaningless "Sum of BOOK_KEY".

**Impact on users/reports:** existing visuals are unaffected — a visual stores its own aggregation choice at the time the field is added. Only the *default* for newly added fields changes.

## 3. `DiscourageImplicitMeasures = true` (model level)

**Why:** the model has disciplined dedicated measure tables (VaR Measures, Stress Measures, Market Risk Measure Summary, …). This setting stops report authors from creating implicit measures (drag-a-column aggregations) that bypass them, and it is a prerequisite for calculation groups if we add them later.

**Impact on users/reports:** existing visuals keep working. Report authors can no longer create *new* implicit aggregations from bare columns — they must use the published measures (which is the intent).

## 4. `Reporting Date` marked as the model's date table

- `Reporting Date` table: `dataCategory = "Time"`.
- The table key (`IsKey`) moved from `COBID` (int) to the `Reporting Date` column (datetime), as required for a marked date table.

**Why:** the model uses time-intelligence DAX (`DATESINPERIOD` ×1, `PREVIOUSDAY` ×2). A marked date table is the supported way to guarantee correct filter behaviour for those functions.

**⚠ Deployment risk — please verify:**

1. **Uniqueness:** `IsKey` enforces uniqueness at process time. If the `Reporting Date` column contains duplicate or blank values (e.g. a placeholder row — some calculated tables in the model guard against `ISBLANK(COBID)`, which suggests one may exist), processing will fail with a duplicate/blank key error. **Fallback if that happens:** move `IsKey` back to `COBID` and remove `dataCategory` from the table; all other changes stand.
2. **Time-intelligence measures:** re-check the three measures using `DATESINPERIOD` / `PREVIOUSDAY` against a known-good report day. Marking the date table changes how filters on *other* `Reporting Date` columns interact with these functions — results should be identical or more correct, but confirm.

---

## Deployment notes

- Deploy metadata from Tabular Editor 3 as usual. **No data reload is required**, but the `IsAvailableInMDX` changes invalidate calculated structures — run a **Process Recalc** on the database after deployment (TE3 will normally prompt for it).
- Expect the first post-deployment refresh to be *faster*, not slower (fewer attribute hierarchies to build).

## Validation checklist

- [ ] Model deploys without errors.
- [ ] Process Recalc completes; check `Reporting Date` key uniqueness error (see §4).
- [ ] Spot-check one report page per scope (VaR / Stress / Sensitivity) against pre-change numbers.
- [ ] Verify the three time-intelligence measures (§4.2).
- [ ] Column sorting still works where `SortByColumn` is used (Month names, ordered dimensions) — the 42 exclusions in §1 protect this, but confirm one example.

## Explicitly NOT changed (open items)

| Item | Status |
|---|---|
| Incremental-refresh windows (65/32/15/8-day hot windows equal to the rolling windows) | On hold — pending business decision on retained days |
| 9 bi-directional relationships | To review — candidates for `CROSSFILTER` in measures instead |
| 30 inactive relationships | To review — confirm each has a `USERELATIONSHIP` consumer |
| ~300 measures without format strings, ~530 without descriptions | Backlog — needs business input on formats |
| Visible key columns (e.g. COBID) not hidden | Deliberate — possibly used in slicers/filters |

## Rollback

All changes are in a single commit-able diff of `RaptorReporting.bim`. Full rollback = `git revert` of that commit. Partial rollback: each change above is independent; §4 has its own targeted fallback described inline.
