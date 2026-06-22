# Central environment config — design

**Date:** 2026-06-22
**Status:** Approved (Approach A)

## Problem

Environment-specific Snowflake identifiers (database, schema, warehouses, roles,
the prod compare-DB) are hardcoded across **7 files** outside `_OLD`. Deploying the
solution to a different database means hand-editing every one of them, which is
error-prone and easy to get partially wrong (e.g. changing `get_session()` but
leaving the banner print or the docs page stale).

### Current hardcoded identifiers

| Identifier        | Value                  | Files |
|-------------------|------------------------|-------|
| App database      | `DVLP_RAPTOR_NEWADJ`   | deploy.py (×3), test_process.py, utils/snowflake_conn.py, pages/9_Validation.py, pages/6_Documentation.py, tests/*.sql (×2) |
| Prod compare DB   | `PROD_RAPTOR`          | pages/9_Validation.py |
| Schema            | `ADJUSTMENT_APP`       | utils/snowflake_conn.py (default) + every DB-object SQL via session-relative `USE SCHEMA` |
| Owner role        | `DVLP_RAPTOR_OWNER`    | deploy.py, test_process.py |
| RO role           | `DVLP_RAPTOR_RO`       | deploy.py |
| Deploy/app WH     | `DVLP_RAVEN_WH_M`      | deploy.py, utils/snowflake_conn.py |
| Dynamic-table WH  | `DVLP_RAPTOR_WH_XS`    | 07_dynamic_tables.sql (×2), test_process.py |

The DB-object SQL files (`01`–`08`) are already **session-relative** — they use
`USE SCHEMA ADJUSTMENT_APP` and unqualified object names, inheriting the database
from the deploy session. They bake in no database. The only literals in SQL are
the two warehouse names in `07_dynamic_tables.sql` and the `USE DATABASE` in the
two test scripts.

## Approach A — single `config.py` + deploy-time propagation

**Single source of truth:** a new `config.py` at the repo root holding all seven
identifiers as plain Python constants. This is the one file you edit to retarget a
new environment.

```python
# config.py — single source of truth for environment-specific names.
DATABASE   = "DVLP_RAPTOR_NEWADJ"   # app database (deploy + app + tests)
SCHEMA     = "ADJUSTMENT_APP"       # app schema (stable across environments)
WAREHOUSE  = "DVLP_RAVEN_WH_M"      # deploy session + Streamlit QUERY_WAREHOUSE
DT_WH      = "DVLP_RAPTOR_WH_XS"    # dynamic-table refresh warehouse
ROLE_OWNER = "DVLP_RAPTOR_OWNER"    # owning role (grants, deploy)
ROLE_RO    = "DVLP_RAPTOR_RO"       # read-only role (grants)
PROD_DB    = "PROD_RAPTOR"          # cross-DB validation compare target
```

### How each consumer reads it

1. **`deploy.py` / `test_process.py`** (repo-root Python) — `import config` and use
   `config.DATABASE`, `config.WAREHOUSE`, `config.ROLE_OWNER`, etc. for session
   setup, grants, the banner print, and the `CREATE STREAMLIT … QUERY_WAREHOUSE`.

2. **SQL files** — `deploy.py` substitutes placeholders before executing:
   - `07_dynamic_tables.sql`: `WAREHOUSE = {{DT_WH}}` → `config.DT_WH`
   - `tests/*.sql`: `USE DATABASE {{DATABASE}}` → `config.DATABASE`
   Substitution happens in the existing read loop (`deploy_db_objects`) right after
   `f.read()`, via a small `render(sql_text)` helper. Files without placeholders are
   unaffected.

3. **Streamlit app** — `deploy.py` uploads `config.py` into the Streamlit stage root
   (added to `files_to_upload`), so it ships alongside `app.py`. The stage root is
   already on `sys.path` (existing `from utils.x import …` proves it), so the app does
   `import config`:
   - `utils/snowflake_conn.py`: local-dev defaults + drops the redundant
     `USE WAREHOUSE` (SiS already runs the app on its `QUERY_WAREHOUSE`); falls back
     to literals if the import fails (defensive, for pre-deploy local runs).
   - `pages/9_Validation.py`: `DEV_DB = config.DATABASE`, `PROD_DB_DEFAULT = config.PROD_DB`.
   - `pages/6_Documentation.py`: the prose DB name interpolated from `config.DATABASE`.

### Scope note — schema

`SCHEMA` is included in `config.py` and consumed at the connection/session layer
(snowflake_conn default; `9_Validation` qualified reads). The ~30 internal
`ADJUSTMENT_APP.<object>` qualifiers inside `deploy.py` stay as literals: the schema
is environment-invariant (the app's own internal schema is `ADJUSTMENT_APP` in every
environment), and rewiring 30 string literals adds churn and risk for no real
deployment benefit. Documented here so it's a conscious decision, not an oversight.

## Why not B/C

- **B (config table):** `deploy.py` still needs the DB name to connect, so the table
  can't be the only source; changing config means running SQL per environment.
- **C (TOML):** adds a parser and the app can't `import` it — you'd still generate a
  `.py` for the app. Same work as A plus a format.

## Out of scope

- No change to the DB-object SQL logic — only the two warehouse literals + test `USE DATABASE`.
- No refactor of `deploy.py`'s internal `ADJUSTMENT_APP.` object qualifiers (see scope note).
- `_OLD/` is untouched.

## Verification

- `python -c "import config"` parses.
- `python -m py_compile` on every edited `.py`.
- Grep shows zero `DVLP_RAPTOR_NEWADJ` / `PROD_RAPTOR` / `DVLP_RAVEN_WH_M` /
  `DVLP_RAPTOR_WH_XS` / `DVLP_RAPTOR_OWNER` / `DVLP_RAPTOR_RO` literals remaining
  outside `config.py` and `_OLD/` (except the defensive fallbacks in snowflake_conn.py).
