# UAT Automation Suite

Automated execution of the go-live UAT test plan against the environment in
`config.py`. Each test drives the real stored procedures with distinct
`p_caller` identities (the same channel the app uses), verifies results with
SQL, and records that SQL + its output as evidence.

**Every run writes `docs/UAT_AUTOMATION_REPORT.md`** — a results table mapped
to the UAT plan IDs plus the full evidence per test. That file is the
documentation deliverable; attach it to the go-live sign-off.

## Run

```bash
pip install pytest              # plus the mufg_snowflakeconn wheel (repo root)
pytest tests -v
```

Connection matches `test_process.py`/`deploy.py`: `mufg_snowflakeconn` with
env `dvlp` and the deployment user. Override with:

| Env var | Default | Purpose |
|---|---|---|
| `UAT_SF_ENV` | `dvlp` | MufgSnowflakeConn environment |
| `UAT_SF_USER` | `apd_raptor_sfk_depl@mufgsecurities.com` | connection user |
| `UAT_FAKE_COB` | `20991231` | isolation COB all tests use |
| `TEST_DIR_ENTITY` / `TEST_DIR_SOURCE_SYSTEM` / `TEST_DIR_DEPARTMENT` / `TEST_DIR_BOOK` / `TEST_DIR_MEASURE_TYPE` | `MUSI` / `QP` / `TRD` / `UATBOOK` / `PL` | DIR-07 stage values — set these to codes that pass your validation views if defaults are rejected |

## Safety & isolation

- All mutating tests use the far-future isolation COB (`20991231`) and
  `ZUAT*` entities — they never touch real COBs.
- Approved test adjustments are deleted immediately so the pipeline poller
  cannot pick them up; everything else is deleted in the session teardown
  (headers, stage rows, sign-off rows/history, feed rows, bot approvers).
- The suite INSERTs rows into the sign-off feed table
  (`BATCH.PUBLISH_SIGNOFF_STATUS` or the configured override) on the
  isolation COB and deletes them afterwards. The connection role needs
  insert/delete there; if delete is not granted, cleanup logs a skip and the
  rows stay (harmless — 2099 COB — but tell the feed owner).
- Run against **UAT only**. Nothing stops you pointing `config.py` at prod,
  so don't.

## Coverage map

| Suite file | UAT plan cases | Notes |
|---|---|---|
| `test_signoff_lifecycle.py` | SGN-01 … SGN-09 | ordered scenario; feed propagation asserted both directions |
| `test_approvals.py` | APP-01 … APP-04 (+ self-approval) | APP-04 automated as reject→resubmit→approve |
| `test_direct_batch.py` | DIR-07 | 120 rows, timing, deterministic UUID ids |
| `test_views_admin_config.py` | FRT-01/02/03 (compile), ADM-00/02, VIEW-01, CFG-01 | read-only |

## What stays manual (by design)

UI-only cases from the plan: checkbox defaults and locking on the Sign-Off
cards, cockpit tiles/grids rendering, timezone picker display, filter-tag
styling, Logs tabs, page load feel (SGN-10 visual half, UXD-01..04, APP-05
header boxes), plus cross-team checks (DWN-01/02) and dataset-dependent
end-to-end processing (SCL-01/02, DIR-08, PIP, FRT-04 with real trade data).
The worst historical bugs (st.cache_data, collect_nowait, temp tables) only
reproduce inside the SiS runtime — **this suite cannot replace the deploy-day
manual smoke test** in the plan.

## First run expectations

SGN/APP suites depend only on seeded config and should pass on a correctly
deployed environment. DIR-07 depends on your validation views accepting the
`TEST_DIR_*` codes — if it fails with `created=0`, the report's evidence
shows the SP message; adjust the env vars to valid reference codes and rerun.
