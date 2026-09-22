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
pip install pytest ./mufg_snowflakeconn-2.5.2-py3-none-any.whl   # wheel is vendored at the repo root
pytest tests -v
```

Connection matches `deploy.py`: `mufg_snowflakeconn` with
env `dvlp` and the deployment user. Override with:

| Env var | Default | Purpose |
|---|---|---|
| `UAT_SF_ENV` | `dvlp` | MufgSnowflakeConn environment |
| `UAT_SF_USER` | `apd_raptor_sfk_depl@mufgsecurities.com` | connection user |
| `UAT_FAKE_COB` | `20991231` | isolation COB all tests use |
| `TEST_DIR_ENTITY` / `TEST_DIR_SOURCE_SYSTEM` / `TEST_DIR_DEPARTMENT` / `TEST_DIR_BOOK` / `TEST_DIR_MEASURE_TYPE` | `MUSI` / `QP` / `TRD` / `UATBOOK` / `PL` | DIR-07 stage values — set these to codes that pass your validation views if defaults are rejected |
| `TEST_TRF_SRC_BOOK` | `UATBOOK` | Transfer Book (TRF suite) source book code |
| `TEST_TRF_TGT_BOOK` | `UATBOOK2` | Transfer Book (TRF suite) target book code |
| `TEST_TRF_FALLBACK_TRADES` | `UAT-NO-SUCH-TRADE-1,UAT-NO-SUCH-TRADE-2` | TRF-05 trade codes to check for the "fallback" flag |

**`TEST_TRF_SRC_BOOK` / `TEST_TRF_TGT_BOOK` must name two real, current
`DIMENSION.BOOK` codes that both carry actual fact rows at the COB you test
against.** The built-in `UATBOOK`/`UATBOOK2` placeholders are very unlikely
to carry data on a default DVLP run — see "Vacuous-pass risk in the Transfer
Book suite" below. Do not generate a UAT report for sign-off with the
placeholder defaults still in effect for TRF-05/06/08.

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
| `test_transfer_book.py` | TRF-01 … TRF-08 | Transfer Book (append semantics); TRF-05/06/08 (all P1/P2) need real book data — see "Vacuous-pass risk" below |

### Vacuous-pass risk in the Transfer Book suite

TRF-05, TRF-06 and TRF-08 each verify something that only exists when the
fake COB actually carries fact rows for `TEST_TRF_SRC_BOOK` /
`TEST_TRF_TGT_BOOK` (a discriminating value: a non-empty breakdown, a
non-zero target-book total, a readable adjusted-view). When that data isn't
there, each case now calls `pytest.skip(...)` with the reason instead of
silently falling through to a check that would pass on empty/zero data
without actually exercising anything — the summary table shows
**⏭ SKIPPED**, not ✅ PASSED, so a green run can't be mistaken for real
coverage. Before generating a report for go-live sign-off, point
`TEST_TRF_SRC_BOOK` / `TEST_TRF_TGT_BOOK` (and, for TRF-05,
`TEST_TRF_FALLBACK_TRADES`) at real books/trades with data at the COB you're
testing, rerun, and confirm TRF-05/06/08 show PASSED with the underlying
figures reported in the evidence section — not SKIPPED.

## Streamlit widget/form test suite (`streamlit_app/tests/`)

A separate pytest suite covers the app's Python logic and widget/form
behaviour using Streamlit's `AppTest` harness (mocked Streamlit — no
Snowflake connection, no live app). It complements this SQL/SP-integration
suite; run both before a deploy:

```bash
pytest streamlit_app/tests -v
```

| Suite file | Covers |
|---|---|
| `test_activity_grid.py` | `build_activity_grid_df` / `resolve_selected_adjustment` (Home/Adjustments grid row shaping) |
| `test_mgrid_html.py` | The three grid renderers (`render_activity_grid`, `render_df_table`, `render_grid`) and the legacy→native row shape conversion |
| `test_new_adjustment_page.py` | New Adjustment page: wizard state, preview/submit call sequencing (async job lifecycle) |
| `test_scope_filters.py` | `utils/scope_filters.py`: per-scope filter field catalogue and multi-scope layout |
| `test_submit_fanout.py` | `utils/submit_fanout.py`: pure fan-out/branch logic for multi-scope submit, no Streamlit or Snowflake |
| `test_transfer_form.py` | Transfer Book fan-out helpers (`utils/transfer_book.py`) and the New Adjustment page's Transfer form/submit routing |

This suite cannot exercise anything that only happens inside the real
Streamlit-in-Snowflake sandbox (CSS test-id quirks, SiS runtime version
behaviour) — a manual click-through in the deployed app is still required
before go-live (see the Deploy checklist in the project's deploy-readiness
report).

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
TRF-05/06/08 depend on `TEST_TRF_SRC_BOOK` / `TEST_TRF_TGT_BOOK` naming
books with real fact data at your test COB — on a default run against the
`UATBOOK`/`UATBOOK2` placeholders they report SKIPPED (see "Vacuous-pass
risk" above), not a failure.
