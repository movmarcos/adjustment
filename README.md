# MUFG Risk Adjustment Engine

A Snowflake-native platform that lets MUFG's risk team correct/override
published risk numbers — VaR, Stress, Sensitivity, FRTB (with sub-types
FRTB/FRTBSBM, FRTBDRC, FRTBRRAO, one shared pipeline) — with full
auditability, an approval workflow, and scheduled processing that hands the
adjusted numbers to downstream reporting. Base fact data is **never**
modified; adjustments live in separate delta tables and reports read a
combined view.

**End users are non-technical risk analysts.** The UI (`streamlit_app/`)
prioritises error prevention and plain-English messaging over feature
count; anything ambiguous in the UI is treated as a bug. See `AGENTS.md`
for the full set of engineering conventions and invariants — this README
covers the solution as built, for a human reader.

---

## Architecture in five lines

1. The UI calls `SP_SUBMIT_ADJUSTMENT(json)` → validates, inserts
   `ADJ_HEADER` (status `Pending` or `Pending Approval`), logs
   `ADJ_STATUS_HISTORY`.
2. A scheduled task **polls** the queue every minute (no streams) and calls
   `SP_RUN_PIPELINE` → `SP_PROCESS_ADJUSTMENT` per scope.
3. Processing is config-driven by `ADJUSTMENT_APP.ADJUSTMENTS_SETTINGS`
   (one row per scope: `FACT_TABLE`, `FACT_ADJUSTED_TABLE` = the combined
   adjusted view, `ADJUSTMENTS_TABLE` = delta rows, `FACT_TABLE_PK` =
   composite surrogate key spec, metric columns).
4. The Scale-family path writes **delta rows** into the scope's
   `ADJUSTMENTS_TABLE`, netted per surrogate key; the adjusted view = fact +
   deltas. Every adjustment gets a numeric `DIMENSION.ADJUSTMENT.ADJUSTMENT_ID`
   (shown to users as `#id`; the internal `ADJ_ID` UUID is never surfaced).
5. Statuses: `Pending → (Pending Approval → Approved) → Running →
   Processed | Failed`, plus `Rejected`, `Rejected - SignedOff`, `Deleted`.

---

## Pages (`streamlit_app/pages/`)

| Page | Purpose |
|---|---|
| Home (`app.py`) | Dashboard: KPI cards, System Status, acknowledge failures |
| 1 — New Adjustment | Create/submit an adjustment ("order ticket" wizard) |
| 2 — Adjustments | Browse/manage: pipeline status boxes, stage board, full list with Retry/Delete/Recall |
| 3 — Approval Queue | Approve/reject pending adjustments (4-eyes; approvers never approve their own requests) |
| 5 — Sign-Off | Sign off a COB/entity/scope; request re-open (always needs approval); sync with the upstream feed |
| 6 — Admin | Scope config, authorized sign-off users, authorized approvers, admins |
| 7 — Documentation | In-app AI assistant (Cortex-backed Q&A) plus this how-it-works knowledge |
| 8 — FRTB Explore | Browse the official FRTB fact tables (COB / entity / risk class / sensitivity type / book / trade), with a CSV download in the FRTB upload template shape for round-tripping into a Direct adjustment |
| 9 — Logs | Runs, activity, errors, sign-off audit |
| 10 — Tasks & Cost | Pipeline task health + serverless compute cost |

(There is no page numbered `4` — the pipeline is monitored from page 2's
status boxes, not a separate page.)

---

## Adjustment categories

- **Scaling** — Flatten (zero the scope), Scale (multiply by a factor),
  Roll (carry another COB's *adjusted* values forward). Scaling and Entity
  Roll accept **several scopes at once**: the app creates one adjustment
  per selected scope, and the filter form offers only the fields every
  selected scope supports (each scope measures a different quantity, so
  the preview never sums figures across scopes — only row counts are
  totalled across scopes).
  - **Transfer Book** (a Scaling type): at one COB, the *source* book's
    adjusted values are **APPENDED** onto the *target* book (source
    untouched), multiplied by a scale factor exactly like Roll (`1` adds a
    copy as-is, `1.10` adds 10% more). It is a pure append — nothing
    already on the target book, its own base values or its own
    adjustments, is flattened or superseded; submitting the same transfer
    twice adds it twice (delete one to undo). A later Scale/Flatten/Roll on
    that book at the same COB *does* replace the transferred rows along
    with everything else in its filter. Optional trade codes scope it to
    specific trades; a trade with no version under the target book lands on
    the target's `<BOOK>/Adjustment` trade.
- **Direct** — exact values. For VaR/Stress/Sensitivity it is **per row**
  (paste or upload a CSV; each row is its own adjustment). For
  FRTB/FRTBDRC/FRTBRRAO it is **per file** (one uploaded file = one Direct
  adjustment; the COB comes from the file's `COBID` column; re-uploading
  the same COB + Reference replaces the previous submission).
- **VaR Upload** — one CSV in the VaR legacy layout = one adjustment;
  re-upload with the same COB + Reference replaces the previous one.
- **Entity Roll** — destructive replace of an entity's figures at a COB;
  always needs approval.

Overlapping adjustments at the same COB/filter are serialised and the
**newest wins**: when it processes, every earlier adjustment row inside its
filter at that COB is removed (superseded by filter scope). See
`docs/superpowers/specs/2026-09-17-transfer-book-multi-scope-design.md` for
the Transfer Book / multi-scope design in full.

---

## Pipeline

Four scope tasks (`TASK_PROCESS_VAR/STRESS/FRTB/SENSITIVITY`) run every
minute (serverless) and call `SP_RUN_PIPELINE`, which polls `ADJ_HEADER`:
reap dead runs (stuck > 4h → `Failed`), claim eligible rows with a token,
process each via `SP_PROCESS_ADJUSTMENT`, release rows that were blocked by
an earlier row that has since resolved. A fifth task,
`TASK_SYNC_SIGNOFF`, syncs sign-off state from the upstream publish feed
every 30 minutes (also checked live at submit time).

Sign-off is owned by the upstream publish feed, granularity
COB + entity + scope (+ optional sub-type). Once signed off, no new
adjustment can be submitted for that entity. Sign-off can apply immediately
(approval optional); re-opening a sign-off always needs 4-eyes approval.

**Reports hand-off**: VaR/Stress → Power BI refresh (~5 min); Sensitivity/
FRTB → dbt rebuild trigger via Control-M. If hand-off fails, the numbers
are still applied but downstream reports may be stale.

---

## Repository layout

```
adjustment/
├── README.md                        # this file
├── AGENTS.md                        # engineering conventions, invariants (source of truth)
├── config.py                        # environment-derived Snowflake names (DVLP/TEST/RLSE/PROD)
├── deploy.py                        # the only supported deploy entry point
├── deploy_all.ps1                   # Windows wrapper: git pull + auto-scoped deploy.py call
├── environment.yml                  # local-dev conda env (mirrors streamlit_app/environment.yml)
├── mufg_snowflakeconn-2.5.2-py3-none-any.whl   # vendored private connector wheel
├── drivers/                         # JDBC driver, split into git-trackable parts (see drivers/README.md)
├── new_adjustment_db_objects/       # Snowflake DDL + stored procedures, deploy-ordered
│   ├── 01_tables.sql … 08_views.sql          # core schema, SPs, tasks, views
│   ├── 05b_sp_run_pipeline.sql, 05c_sp_force_process.sql
│   ├── 09_sp_debug_entity_roll.sql … 15_direct_frtb_upload.sql
│   └── tests/                       # SQL-level integration tests (test_entity_roll_adjustment.sql etc.)
├── streamlit_app/                   # Streamlit-in-Snowflake (SiS) application
│   ├── app.py                       # Home dashboard
│   ├── pages/                       # 1, 2, 3, 5, 6, 7, 8, 9, 10 — see Pages above
│   ├── utils/                       # styles.py (design system), snowflake_conn.py, scope_filters.py, transfer_book.py, submit_fanout.py
│   ├── environment.yml              # authoritative SiS runtime pin (streamlit=1.50.0)
│   ├── requirements.txt             # local-dev pip reference only — SiS does not install from it
│   └── tests/                       # widget/form pytest suite (mocked Streamlit)
├── notebooks/                       # Snowflake Notebook test harness
│   ├── adjustment_test_kit.py       # harness logic (pure, takes an explicit session)
│   ├── adjustment_test_harness.ipynb   # the Snowflake Notebook itself (thin, calls the kit)
│   └── tests/                       # local pytest against a fake session
├── tests/                           # UAT automation pytest suite (live Snowflake, isolation COB)
└── docs/                            # reviews/audits, specs (docs/superpowers/specs/), reference SQL
```

Old prototype trees (an earlier design with different page numbering and a
`snowflake/`-rooted DDL layout) were removed from the working copy on
2026-09-17 and remain in git history only.

---

## Deploy

The **only** supported deploy path is `python deploy.py`, run against the
environment `config.py` currently targets (`ADJ_ENV` env var, default
`DVLP`; valid values `DVLP | TEST | RLSE | PROD`).

```bash
# mufg_snowflakeconn is a private package with no reachable index — install
# the vendored wheel at the repo root; snowflake-snowpark-python comes from PyPI.
pip install ./mufg_snowflakeconn-2.5.2-py3-none-any.whl snowflake-snowpark-python

python deploy.py                  # full deploy: DB objects + Streamlit app + Notebooks
python deploy.py --db-only        # DB objects only
python deploy.py --streamlit-only # Streamlit app only
python deploy.py --notebooks-only # Snowflake Notebooks only
python deploy.py --test-adj       # (opt-in) also submit one test VaR Flatten adjustment
python deploy.py --validate-only  # run only the post-deploy schema validation, no deploy
```

A full run:

1. **PHASE 1 — Database Objects**: deploys every file under
   `new_adjustment_db_objects/*.sql` in **filename-sorted order**
   (`01` → `15`; `05b`/`05c` sort correctly between `05` and `06`). All
   tables use `CREATE OR ALTER TABLE`, so redeploys do not drop data.
2. **PHASE 1b — Resume pipeline tasks**: `CREATE OR REPLACE TASK` always
   leaves a task `Suspended`; this step un-suspends all five.
3. **PHASE 1c — Validate schema**: checks `ADJUSTMENTS_SETTINGS`-configured
   objects (metric columns, `FACT_TABLE_PK`) against the live fact/adjusted
   tables. Warnings about Roll defaulting missing combined-view columns to
   `-1`/`NULL` are expected; any ❌ is a hard block.
4. **PHASE 2 — Streamlit Application**: uploads `streamlit_app/` to the SiS
   stage and reports what stale files (if any) it removed.
5. **PHASE 2b — Snowflake Notebooks**: uploads `notebooks/` (the harness kit
   + the `.ipynb`) to their own stage and creates/activates the
   `ADJUSTMENT_TEST_HARNESS` notebook.
6. **PHASE 3 — Test adjustment** (only with `--test-adj`; off by default):
   submits one test VaR Flatten adjustment.

**Never deploy a single `new_adjustment_db_objects/*.sql` file
individually, out of that numeric order.** Later files reference columns
added by earlier ones unconditionally — e.g. `05_sp_process_adjustment.sql`
and `08_views.sql` both reference `ADJ_HEADER.SOURCE_BOOK_CODE`, added in
`01_tables.sql`. Snowflake does not validate a stored procedure body
against live table schemas at `CREATE OR ALTER PROCEDURE` time, only at
first invocation — a hand-pasted single-file "hotfix" against an
environment that hasn't received an earlier file's change will succeed
silently and only fail later, deep inside a live pipeline run. Full-
directory deploys via `python deploy.py` only (`--db-only` still deploys
every DB file, not one). If a hotfix to a single procedure is operationally
necessary, re-run `01_tables.sql` first — it's `CREATE OR ALTER`, so
idempotent/safe.

### `deploy_all.ps1` (Windows convenience wrapper)

Pulls the target branch, diffs it against the last successfully-deployed
commit (`.last_deploy_commit`, gitignored/local) to auto-scope the deploy,
and calls `deploy.py` for you.

```powershell
.\deploy_all.ps1                                   # auto: only what changed since last deploy
.\deploy_all.ps1 -Mode all                          # force full deploy (DB + Streamlit + Notebooks)
.\deploy_all.ps1 -Mode db                           # force DB objects only
.\deploy_all.ps1 -Mode streamlit                    # force Streamlit app only
.\deploy_all.ps1 -Mode notebooks                    # force Snowflake Notebooks only
.\deploy_all.ps1 -Branch my-branch                  # deploy from a feature branch (default: main)
.\deploy_all.ps1 -PythonExe C:/path/to/python.exe   # override the interpreter (default: "python" on PATH, or $env:ADJ_DEPLOY_PYTHON)
```

The script's `-Branch` default currently tracks the active feature branch
under test and must be flipped back to `main` as the last commit before
that branch merges — always pass `-Branch` explicitly rather than relying
on the default while a feature branch is in flight.

### Post-deploy smoke test

There is no automated Streamlit harness gate in CI — after any deploy,
manually click through: New Adjustment submit, Approval queue, Sign-Off,
and anything specifically touched by the change (see Tests below for what
the automated suites do and do not cover).

---

## Tests

Two independent pytest suites; run both before trusting a change.

### `streamlit_app/tests/` — widget/form logic (mocked Streamlit)

No Snowflake connection; uses `streamlit.testing.v1.AppTest` plus direct
unit tests of pure helper modules.

```bash
pytest streamlit_app/tests -v
```

Covers: activity-grid row shaping, the three grid renderers, the New
Adjustment page's wizard/preview/submit call sequencing, per-scope filter
field catalogues, multi-scope submit fan-out logic, and the Transfer Book
form/submit routing. Cannot exercise anything that only happens inside the
real SiS sandbox (CSS test-id quirks, runtime-version-specific behaviour).

### `tests/` — UAT automation (live Snowflake integration)

Drives the real stored procedures against the environment in `config.py`,
under distinct `p_caller` identities (the same channel the app uses), with
a far-future isolation COB so nothing touches real data. Every run writes
`docs/UAT_AUTOMATION_REPORT.md` — the documentation deliverable attached to
go-live sign-off.

```bash
pip install pytest ./mufg_snowflakeconn-2.5.2-py3-none-any.whl
pytest tests -v
```

| Env var | Default | Purpose |
|---|---|---|
| `UAT_SF_ENV` | `dvlp` | MufgSnowflakeConn environment |
| `UAT_SF_USER` | `apd_raptor_sfk_depl@mufgsecurities.com` | connection user |
| `UAT_FAKE_COB` | `20991231` | isolation COB all tests use |
| `TEST_DIR_ENTITY` | `MUSI` | DIR-07 stage value |
| `TEST_DIR_SOURCE_SYSTEM` | `QP` | DIR-07 stage value |
| `TEST_DIR_DEPARTMENT` | `TRD` | DIR-07 stage value |
| `TEST_DIR_BOOK` | `UATBOOK` | DIR-07 stage value |
| `TEST_DIR_MEASURE_TYPE` | `PL` | DIR-07 stage value |
| `TEST_TRF_SRC_BOOK` | `UATBOOK` | Transfer Book suite source book code |
| `TEST_TRF_TGT_BOOK` | `UATBOOK2` | Transfer Book suite target book code |
| `TEST_TRF_FALLBACK_TRADES` | `UAT-NO-SUCH-TRADE-1,UAT-NO-SUCH-TRADE-2` | trade codes for the "fallback" flag case |

**`TEST_TRF_SRC_BOOK`/`TEST_TRF_TGT_BOOK` must name two real, current books
that both carry actual fact data at the COB under test before a report is
generated for sign-off** — otherwise the Transfer Book P1 cases (TRF-05,
TRF-06, TRF-08) report `SKIPPED` rather than exercising the append-vs-
replace behaviour they exist to prove. See `tests/README.md` for the full
coverage map, safety/isolation notes, and what stays manual by design.

---

## Specs and reviews

- Design specs: `docs/superpowers/specs/` (e.g. Entity Roll v2,
  Transfer Book / multi-scope).
- Reviews/audits: `docs/REVIEW_*.md`, `docs/AUDIT_*.md` — point-in-time
  findings, not always kept current after a fix ships; check the code
  before trusting an "open" marker.
- `docs/reference/` — retired-prototype migration notes and reference SQL.
