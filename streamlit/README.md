# MRM Adjustment Engine — deployable package

**Copy this folder and you have the whole solution.** It carries the deploy
engine, the environment map and every app, each with its own database
objects. Nothing in here reads git, and nothing reaches outside this folder
(the one exception is the optional test-harness notebook, described at the
end).

```
streamlit/
├── deploy.ps1                 run this — the only entry point you need
├── deploy.py                  the deploy engine (PowerShell calls it)
├── config.py                  environment map: database → warehouse, roles, user
└── adjustment_engine/         one folder = one app
    ├── app.toml               Snowflake object name + the title Snowsight shows
    ├── app.py                 the app's entry point
    ├── pages/                 one file per page
    ├── utils/                 shared code (styles, session, validation helpers)
    ├── sql/                   the app's database objects, deployed in filename order
    ├── environment.yml        packages Snowflake installs for the app
    └── requirements.txt
```

## Deploy

```powershell
cd streamlit
.\deploy.ps1 -DbTarget TEST_RAPTOR_NEWADJ_4
```

**The first four letters of the database are the environment.** Everything
else follows from them, so there is nothing else to configure:

| Database passed | Environment | Warehouse | Owner role | Deploy account |
|---|---|---|---|---|
| `DVLP_RAPTOR_NEWADJ_4` | DVLP | `DVLP_RAPTOR_WH` | `DVLP_RAPTOR_OWNER` | `apd_…` |
| `TEST_RAPTOR_NEWADJ_4` | TEST | `TEST_RAPTOR_WH` | `TEST_RAPTOR_OWNER` | `apt_…` |
| `RLSE_RAPTOR_NEWADJ_4` | RLSE | `RLSE_RAPTOR_WH` | `RLSE_RAPTOR_OWNER` | `apr_…` |
| `PROD_RAPTOR_NEWADJ_4` | PROD | `PROD_RAPTOR_WH` | `PROD_RAPTOR_OWNER` | `app_…` |

A database whose first four letters are none of those is refused before
anything connects, so a typo cannot create objects in the wrong place.
RLSE and PROD ask you to type the database name to confirm; `-Yes` skips
that for an automated pipeline.

### Options

| Command | What it does |
|---|---|
| `.\deploy.ps1 -DbTarget <db>` | everything: database objects, then the app |
| `.\deploy.ps1 -DbTarget <db> -Mode db` | the `sql/` objects only |
| `.\deploy.ps1 -DbTarget <db> -Mode streamlit` | the app only |
| `.\deploy.ps1 -DbTarget <db> -Mode notebooks` | the test-harness notebook only |
| `-PythonExe C:\path\to\python.exe` | pick the interpreter (default: `python` on PATH, or `$env:ADJ_DEPLOY_PYTHON`) |

### What it needs

Python 3.9+ with `snowflake-snowpark-python` and the MUFG Snowflake
connector (`mufg_snowflakeconn`). The deploy authenticates as the
environment's `…_raptor_sfk_depl` service account through that connector —
no credentials live in these files.

## What a deploy does

1. **Database objects** — every `adjustment_engine/sql/*.sql` in filename
   order (`01_…` first, `16_grants.sql` last), statement by statement.
   `{{DATABASE}}`, `{{WAREHOUSE}}`, `{{ROLE_OWNER}}` and friends are
   substituted from the target before each file runs. A failed statement is
   counted and reported, and the deploy exits non-zero.
2. **Pipeline tasks** are resumed, then the schema is validated against the
   scope configuration.
3. **The app** — its files are uploaded to `ADJUSTMENT_APP.STREAMLIT_ADJUSTMENT_STAGE`
   (unchanged files are skipped by MD5, so a redeploy is quick) and the
   `ADJUSTMENT_APP.ADJUSTMENT_ENGINE` object is recreated, listed in
   Snowsight as **MRM Adjustment Engine**.

Two generated files travel with the app and are not in this folder:
`utils/build_info.py` (which commit is live, shown in the sidebar) and
`deploy_target.py` (which environment the app was deployed into). The second
one matters: Streamlit in Snowflake sets no environment variables, so
without it an app deployed to TEST would read the default in `config.py` and
query the DVLP database by name.

## Changing the displayed name

Edit `title` in `adjustment_engine/app.toml` and redeploy. Leave `name`
alone unless you mean to create a second app: it is the Snowflake object
name, so changing it produces a new app at a new URL and orphans the old
one, its grants and its bookmarks.

## Adding another app

Create a folder beside `adjustment_engine/` with an `app.py`, an `app.toml`
naming it, and a `sql/` folder if it has database objects of its own.
`deploy.ps1` picks it up with no change to any script.

## What this folder deliberately does not do

- **No git.** No fetch, no branch, no marker file. It deploys what is on
  disk, which is what makes it safe to copy elsewhere.
- **No tests.** The app's ~540 unit tests live in the source repository
  under `tests/app/`; they need no Snowflake connection and are not needed
  to deploy.
- **`-Mode notebooks`** deploys the Snowflake test-harness notebook, which
  lives in `notebooks/` in the source repository, beside this folder's
  parent. A copied package has no such folder, and the deploy says so and
  moves on instead of failing.

In the source repository there is a second script, `..\deploy_all.ps1`. That
one is for development: it pulls from git and deploys only what changed.
This one is the one to use everywhere else.
