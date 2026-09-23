# Adjustment test harness

A Snowflake Notebook that runs any adjustment scenario end to end: preview,
submit, wait for the pipeline, verify the numbers actually landed, clean up.

| File | What it is |
|---|---|
| `adjustment_test_harness.ipynb` | The notebook. Thin — it calls the kit. |
| `adjustment_test_kit.py` | All the logic. Staged next to the notebook so it can `import adjustment_test_kit`. |
| `tests/` | Local pytest against a fake session. No Snowflake needed. |

Design: `docs/superpowers/specs/2026-09-23-adjustment-test-harness-design.md`

## Why it exists

The pytest suite in `tests/` runs against live Snowflake but deliberately keeps
the pipeline away from what it creates — it deletes an approved header
immediately so the poller cannot pick it up, and `tests/README.md` lists
end-to-end processing under "what stays manual". Nothing verified that a
submitted adjustment is picked up, processed, and lands correct numbers.

This notebook is that missing check. The pytest suite stays as the fast
synchronous one.

## Running it

1. Deploy it: `.\deploy_all.ps1 -Mode notebooks` (or `-Mode all`).
2. Open **ADJUSTMENT_TEST_HARNESS** in Snowsight.
3. Run `setup`, edit and run `config`, then run `preflight`.
4. Run `seed` once for the environment.
5. Run whichever mode you want.

### Seeding is not optional

The reserved test COB (`20991231`) holds no fact data. Without seeding, every
preview returns zero rows and verifies nothing, which is why several existing
pytest cases skip rather than pass falsely. The seed cell clones a pinned slice
of real fact rows into the test COB, changing only the COB, so keys and
measures stay consistent with real data.

Set `seed_source_cob` to a COB that actually holds rows for your books.

## Run modes

| Mode | Scenarios | What it is for |
|---|---|---|
| `single` | 1 | The everyday case. Pick a scope, a type, and parameters. |
| `smoke` | one per active scope | Quick post-deploy sanity check. |
| `full` | every legal scope by type | Pre-deploy regression gate. Long. |
| `negative` | 8 | Asserts the illegal combinations are actually refused. |

## What gets verified

The uniform check is that the measure total written to the scope's adjustment
table equals the delta the preview projected, compared at the engine's own
scale-10 precision. On top of that:

- the header reached `Processed` and is not deleted
- a dimension row was minted with a non-zero record count
- the fact row count matches the header's record count
- a per-type assertion: Flatten writes the exact negative of the base, Scale
  writes the base times the factor minus one, Transfer leaves the source book
  untouched, Entity Roll clears the entity's prior adjustments

## The extras

- **Pre-flight** confirms the pipeline tasks are started before anything is
  submitted. A suspended task otherwise turns into a silent wait to timeout.
- **Timings** are captured per phase, so a slowdown in the engine shows up as a
  number you can compare between runs.
- **Pruning guard** reads partitions scanned from query history and fails a
  scenario when a query scanned most of a table. Most reliable with
  `force=True`, because the escape hatch runs the engine in your own session;
  a task-run adjustment executes in the task's session, which your role may not
  be able to see. When history is unavailable it reports that rather than
  failing.
- **Leak detector** asserts the test COB is empty after a full run.
- **Concurrency check** submits two adjustments that write the same table and
  asserts their processing windows did not overlap.

## Safety

- Default COB is `20991231`. Pointing at a real COB needs `cob` **and**
  `allow_real_cob = True`; the kit raises otherwise.
- `seed()` refuses to write anywhere but the test COB.
- Cleanup replays the app's own delete transaction rather than raw deletes, so
  it exercises the same path your users do, and a failed scenario is kept by
  default so you still have the evidence.
- The harness never queries `DIMENSION.TRADE`.
- Bot identities `UAT_BOT_SUBMITTER` and `UAT_BOT_APPROVER` carry no dot or
  domain, so they cannot collide with a real employee.

## Local tests

```bash
pytest notebooks/tests -q
```

No Snowflake connection is used. One of these tests asserts that the kit's
vendored copy of `SCOPE_FILTER_FIELDS` still matches the app's, so a filter
added to the app cannot silently go untested here.
