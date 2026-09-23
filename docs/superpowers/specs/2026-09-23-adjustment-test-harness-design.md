# Adjustment Test Harness — design

**Date:** 2026-09-23
**Status:** approved by Marcos, 2026-09-23
**Goal:** a Snowflake Notebook that runs any adjustment scenario end to end —
preview, submit, wait for the pipeline, verify the numbers actually landed,
clean up — and can run the whole legal matrix unattended.

---

## 1. Why this exists

The repo already has a live-Snowflake UAT suite at `tests/`. It does **not**
test the thing that matters most:

- `tests/test_approvals.py:59-61` deletes an approved header immediately,
  with the comment *"Keep the pipeline poller away from an approved row on a
  fake COB."*
- `tests/README.md:106-115` lists "dataset-dependent end-to-end processing"
  under **What stays manual**.

So today nothing verifies that a submitted adjustment is picked up by the
pipeline, processed, and lands correct numbers in the fact adjustment table.
Every regression in `SP_PROCESS_ADJUSTMENT` is caught by eye, in the app,
after deploy. That is the gap this harness closes.

A second problem blocks the obvious fix. The reserved UAT COB (`20991231`)
holds no fact rows, so a Scale or Flatten there previews zero rows and
verifies nothing. The existing suite skips those tests rather than pass
falsely (`tests/README.md:64-78`). The harness therefore **seeds** the test
COB by cloning a pinned slice of real fact rows, changing only the COB.

## 2. Decisions taken

| Question | Decision |
|---|---|
| Runtime | Snowflake Notebook, Python cells, `get_active_session()` |
| Test data | Isolated COB `20991231` by default; real COB opt-in via config |
| Cleanup | Auto-delete after verify; `keep=True` per scenario to retain |
| Run modes | single, smoke, full matrix, negative — all four |
| Extras | pre-flight, timing, pruning guard, leak detector, concurrency — all in |

## 3. Files

| Path | Responsibility |
|---|---|
| `notebooks/adjustment_test_kit.py` | All harness logic. Pure functions plus a `Harness` class taking a session. No notebook imports, so it unit-tests locally. |
| `notebooks/adjustment_test_harness.ipynb` | Snowflake Notebook. Config cell, optional widget cell, one cell per run mode. Thin: it calls the kit. |
| `notebooks/tests/test_adjustment_test_kit.py` | Local pytest against a fake session, same pattern as `streamlit_app/tests`. |
| `deploy.py` (modified) | New `deploy_notebooks(session)`: stage the kit and the notebook, `CREATE OR REPLACE NOTEBOOK`. |
| `deploy_all.ps1` (modified) | `-Mode notebooks` and inclusion in `-Mode all`. |

The kit is deployed **next to the notebook on its stage**, so the notebook's
first code cell is `import adjustment_test_kit as kit`. Snowflake puts the
notebook's stage directory on `sys.path`.

## 4. Configuration is read, not hardcoded

Everything scope-specific comes from `ADJUSTMENT_APP.ADJUSTMENTS_SETTINGS`
at runtime: `FACT_TABLE`, `FACT_ADJUSTED_TABLE`, `ADJUSTMENTS_TABLE`,
`ADJUSTMENTS_SUMMARY_TABLE`, `FACT_TABLE_PK`, `METRIC_NAME`,
`METRIC_USD_NAME`, `IS_ACTIVE`. Adding a seventh scope requires no notebook
change.

`kit.load_scopes(session)` returns `{process_type: ScopeConfig}`.

## 5. The scenario model

```python
@dataclass
class Scenario:
    id: str                      # "SCL-VAR-01"
    scope: str                   # PROCESS_TYPE, e.g. "VaR"
    adjustment_type: str         # Flatten | Scale | Roll | Transfer | EROL | Direct | Upload
    params: dict                 # merged into the submit payload
    expect: str = "processed"    # "processed" | "reject" | "signoff_reject"
    expect_message: str = ""     # substring match, negative scenarios only
    keep: bool = False
    seed: bool = True
```

`kit.build_matrix(scopes, mode)` generates scenarios. It encodes the legality
rules found in `03_sp_submit_adjustment.sql`:

- **Flatten** — `source_cobid == cobid`; scale factor ignored (engine forces
  `-1.0`).
- **Scale** — `scale_factor != 1.0` (hard reject at `:323-327`);
  `source_cobid == cobid`.
- **Roll** — `source_cobid != cobid`; factor 1.0 legal.
- **Transfer** — source and target book both required, must differ,
  `source_cobid == cobid`; entity derived server-side; all other filters
  stripped.
- **EROL** — approval forced on (`:420-422`); target and source COB differ;
  entity required.
- **Direct / Upload** — skip overlap checking; file flows dedupe on
  `global_reference`.

Filter applicability per scope comes from `streamlit_app/utils/scope_filters.py`
(`SCOPE_FILTER_FIELDS`), vendored into the kit as a constant with a unit test
asserting it still matches the app's copy.

## 6. The seven phases

`kit.run_scenario(session, scenario, cfg)` executes, in order:

1. **seed** — clone fact rows into the test COB. Idempotent: skipped when the
   target COB already holds rows for that scope and book. Skipped entirely
   when `cfg.cob` is a real COB.
   ```sql
   INSERT INTO <FACT_TABLE>
   SELECT * REPLACE (<TEST_COB> AS COBID) FROM <FACT_TABLE>
   WHERE COBID = <SEED_SOURCE_COB> AND BOOK_KEY IN (<pinned keys>)
   LIMIT <cfg.seed_rows>
   ```
   Book keys are resolved once from `DIMENSION.BOOK` by `BOOK_CODE`, pinned
   as literals. `DIMENSION.TRADE` is never queried.

2. **preview** — `CALL SP_PREVIEW_ADJUSTMENT(<payload>)`, `mode=summary`.
   Captures `ROWS_AFFECTED`, `NONZERO_ROWS`, `TOTAL_CURRENT_VALUE`,
   `TOTAL_ADJUSTMENT_DELTA`, `TOTAL_PROJECTED_VALUE`. Rendered as a table.
   A zero-row preview fails the scenario immediately rather than proceeding,
   mirroring the app's own submit gate.

3. **submit** — `CALL SP_SUBMIT_ADJUSTMENT(<payload>)`, returns `adj_id`.
   For a negative scenario this is the terminal phase: assert `status` is
   `Error` and the message contains `expect_message`.

4. **approve** — only when the header came back `Pending Approval` (EROL
   always). `CALL SP_DECIDE_ADJUSTMENT(adj_id, 'Approved', comment, approver)`
   as `UAT_BOT_APPROVER`, which differs from the submitter because
   self-approval is refused.

5. **wait** — poll `ADJ_HEADER.RUN_STATUS` until terminal
   (`Processed`, `Failed`, `Superseded`, `Replaced`, `Deleted`,
   `Rejected - SignedOff`). Default interval 10 s, timeout 900 s. Tasks fire
   every minute (`06_tasks.sql`). With `cfg.force=True` the harness instead
   calls `SP_FORCE_PROCESS_ADJUSTMENT(adj_id)` and polls only briefly, which
   trades queue coverage for speed.

6. **verify** — see below.

7. **cleanup** — replays the app's own delete transaction
   (`streamlit_app/pages/2_Adjustments.py:756-812`): soft-delete the header,
   write status history, soft-delete the `DIMENSION.ADJUSTMENT` row, delete
   the fact adjustment and summary rows, all inside one `BEGIN`/`COMMIT`.
   Using the app's path means cleanup is itself a test of delete.
   Skipped when `keep=True` or when the scenario failed and
   `cfg.keep_failures=True` (the default, so evidence survives).

## 7. Verification

**The uniform check**, applied to every processed scenario:

> the measure total written to the scope's `ADJUSTMENTS_TABLE` for this
> `ADJUSTMENT_ID` equals the `TOTAL_ADJUSTMENT_DELTA` the preview projected.

Compared at a relative tolerance of 1e-6, with an absolute floor of 1e-6 to
match the engine's own scale-10 rounding.

Alongside it:

- header `RUN_STATUS = 'Processed'` and `IS_DELETED = FALSE`
- a `DIMENSION.ADJUSTMENT` row exists with `RECORD_COUNT > 0`
- fact adjustment row count equals the header `RECORD_COUNT`
- the adjusted or combined view total equals base total plus adjustment total

**Per-type assertions:**

| Type | Assertion |
|---|---|
| Flatten | adjustment sum equals the exact negative of the base sum for the filter |
| Scale | adjustment sum equals base sum times (factor minus 1) |
| Roll | adjustment sum equals factor times the source COB's adjusted total |
| Transfer | target gains rows equal to the source row count; the source book's own totals are byte-identical before and after |
| EROL | the entity's prior adjustment rows at the target COB are gone |
| Direct | adjustment sum equals the sum of the submitted values |

## 8. Run modes

| Mode | What it does |
|---|---|
| `single` | one scenario built from the config cell |
| `smoke` | one representative Scale per scope, six scenarios |
| `full` | every legal scope by type combination |
| `negative` | the eight hard rejections from `SP_SUBMIT_ADJUSTMENT` |

`kit.run_suite(session, scenarios, cfg)` runs them in order, collects
`ScenarioResult` records, and returns a results frame. Scenarios are run
sequentially: the engine serialises combos writing the same table, so
parallel submission would only queue.

## 9. The extras

**Pre-flight** (`kit.preflight`) — runs before any submit and fails loudly:
the four scope tasks exist and are `started` (`SHOW TASKS`), the warehouse
is resumable, every scope under test is `IS_ACTIVE`, the test COB is not
signed off for the entities under test, and both bot users are present in
`ADJ_APPROVERS`. Without this a suspended task turns into a silent 15 minute
wait.

**Timing** — each phase is wall-clocked into `ScenarioResult.timings`, plus
rows per second for the process phase. The report prints them so a
regression in Entity Roll or Transfer shows as a number, not a feeling.

**Pruning guard** (`kit.check_pruning`) — after processing, reads
`SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY` (falling back to
`INFORMATION_SCHEMA.QUERY_HISTORY` for freshness) for queries tagged with
this run, and fails the scenario when any query scanned more than
`cfg.max_scan_ratio` of a table's partitions. The engine's queries are
tagged by setting `QUERY_TAG` to the `adj_id` around the run. This turns the
standing rule — never scan `DIMENSION.TRADE` unpruned — into a test.

**Leak detector** (`kit.check_leaks`) — after a full run, asserts nothing
remains at the test COB across `ADJ_HEADER`, `DIMENSION.ADJUSTMENT`, every
scope's adjustment and summary table, and the seeded fact rows.

**Concurrency check** (`kit.check_serialisation`) — submits two scenarios in
the same scope and COB that write the same adjustment table, then asserts
from `ADJ_STATUS_HISTORY` that their `Running` windows do not overlap. This
closes one of the three live-warehouse checks still outstanding from the
2026-09 audit.

**Report** — `kit.render_report(results)` produces markdown in the same
shape as `docs/UAT_AUTOMATION_REPORT.md`, displayed in the notebook and
optionally written to `ADJUSTMENT_APP.ADJ_TEST_RUNS` for history.

## 10. Safety

- Default COB is `20991231`. Pointing at a real COB requires setting
  `cfg.cob` **and** `cfg.allow_real_cob = True`; the kit raises otherwise.
- Seeding only ever inserts into the test COB. The kit refuses to seed when
  `cfg.cob` is not the configured test COB.
- Cleanup deletes only rows it can tie to an `adj_id` this run created, or
  fact rows at the test COB.
- The harness never touches `DIMENSION.TRADE`, per the standing rule.
- Bot identities `UAT_BOT_SUBMITTER` and `UAT_BOT_APPROVER` carry no dot or
  domain, so they cannot collide with a real employee.

## 11. Out of scope

- Testing the Streamlit UI itself. `streamlit_app/tests` already does that.
- Replacing `tests/`. The pytest suite stays as the fast synchronous check;
  this harness is the slow end-to-end one.
- Scheduling. The notebook is run by a human or by a task the user adds
  later.
