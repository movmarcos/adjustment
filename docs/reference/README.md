# Reference material (kept from the retired `_OLD/` tree, 2026-09-17)

- `legacy_dbt/` — the dbt adjustment models the processing engine mirrors
  (`adjustment__adjustments_base*.sql`, sign-off models, `_adjustment__model.yml`,
  the legacy `ADJUSTMENTS_SETTINGS` seed). SP_PROCESS_ADJUSTMENT's dimension
  lookups follow `adjustment__adjustments_base.sql`.
- `fact_columns.csv` — legacy per-fact-table column inventory (the live dump is
  `fact_schema.csv` at the repo root).
- `dimension.adjustment.sql` — DDL of `DIMENSION.ADJUSTMENT`.
- `adjustment_metadata.md` — original adjustment metadata notes.

Everything else that lived under `_OLD/` (prototype apps, business checks,
sample upload files, legacy loader tasks) was removed from the tree and is
available in git history before commit "chore: clean the repository".
