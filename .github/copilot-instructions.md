# MUFG Adjustment Engine — Agent Instructions

Guidance for AI assistants (GitHub Copilot, Claude Opus 4.x, or any coding
agent) working in this repository. Read this fully before changing code.
The same content may be copied to `AGENTS.md` or `CLAUDE.md` for other tools.

## What this is

A risk-adjustment engine for MUFG: non-technical risk analysts submit
adjustments (scale / flatten / roll / direct upload / entity roll) against
fact tables (VaR, Stress, FRTB, Sensitivity), with approval workflow,
scheduled processing, and PowerBI refresh. Two halves:

- `streamlit_app/` — Streamlit-in-Snowflake (SiS) UI. Entry `app.py`
  (dashboard), pages under `pages/`, shared design system in
  `utils/styles.py`, connection helpers in `utils/snowflake_conn.py`.
- `new_adjustment_db_objects/` — Snowflake DDL + stored procedures, numbered
  in deploy order (`01_tables.sql` … `08_views.sql`). `deploy.py` deploys.
  `_OLD/` is dead code — never edit it.

**End users are non-technical.** Error prevention and clear messages beat
features. Anything ambiguous in the UI is a bug.

## Architecture in five lines

1. UI calls `SP_SUBMIT_ADJUSTMENT(json)` → validates, inserts `ADJ_HEADER`
   (status `Pending` or `Pending Approval`), logs `ADJ_STATUS_HISTORY`.
2. A scheduled task **polls** the queue (no streams) and calls
   `SP_RUN_PIPELINE` → `SP_PROCESS_ADJUSTMENT` per scope.
3. Processing is config-driven by `ADJUSTMENT_APP.ADJUSTMENTS_SETTINGS`
   (one row per scope: `FACT_TABLE`, `FACT_ADJUSTED_TABLE` = combined
   adjusted view, `ADJUSTMENTS_TABLE` = delta rows, `ADJUSTMENTS_SUMMARY_TABLE`,
   `FACT_TABLE_PK` = composite surrogate key spec, metric columns).
4. Scale path writes **delta rows** into the ADJUSTMENTS_TABLE, netted per
   surrogate key; the adjusted view = FACT + deltas. Each adjustment gets a
   numeric `DIMENSION.ADJUSTMENT.ADJUSTMENT_ID` (shown to users as `#id`,
   never the internal `ADJ_ID` UUID).
5. Statuses: `Pending → (Pending Approval → Approved) → Running →
   Processed | Failed`, plus `Rejected`, `Rejected - SignedOff`, `Deleted`.

## Critical invariants — violating these causes production bugs

### SQL from Python
- **Escape order matters**: when embedding JSON into a SQL literal, double
  backslashes FIRST, then single quotes:
  `json.dumps(p).replace("\\", "\\\\").replace("'", "''")`.
  Snowflake string literals interpret `\n`/`\t`, so unescaped backslashes
  arrive as raw control characters and `json.loads` inside the SP fails with
  "Invalid control character". Prefer `session.call()` / bind params for new
  code — they avoid the whole class.
- Snowflake escapes quotes by **doubling** (`''`), never backslash.

### Audit trail
- `ADJ_STATUS_HISTORY` has **no trigger** — every status transition must
  INSERT its own row explicitly (UI buttons do it; `SP_PROCESS_ADJUSTMENT`
  uses `log_status_history`). A new transition without an INSERT silently
  loses audit.

### Submit result handling
- A sign-off rejection returns status `Rejected - SignedOff`, **not**
  `Error`. Success = status in `("Pending", "Pending Approval", "Approved")`
  (see `_SUBMIT_SUCCESS_STATUSES` in `pages/1_New_Adjustment.py`). Checking
  `status != "Error"` wrongly shows success.

### Data/config
- `FACT_TABLE_PK` in settings must match live fact table columns exactly
  (surrogate key is built from it). Stress uses `TRADE_CURRENCY`, not
  `CURRENCY_CODE`.
- `DIMENSION.ADJUSTMENT.ADJUSTMENT_TYPE` is **VARCHAR(4)** — long type names
  overflow. Entity Roll v2 renames the type to `EROL` (see Active work).
- Deletes are soft (`IS_DELETED`, `RUN_STATUS='Deleted'`); fact delta rows
  for the adjustment are physically removed. Never physically delete from
  `ADJ_HEADER`.
- Filter resolution: adjustment filters are codes resolved to keys via
  EXISTS lookups against dimensions (see `SP_PROCESS_ADJUSTMENT` join
  builder); blank filter = all values.

### pandas / preview
- `NaN` is truthy: `int(value or 0)` crashes on NaN. Use the `_safe_int()`
  pattern (`pd.isna` guard) for anything coming back from
  `SP_PREVIEW_ADJUSTMENT`.
- Entity-wide scaling adjustments (entity set, no book/department) **skip**
  the preview — it would time out on millions of rows.
- Submit is blocked when a current (non-stale) preview matches 0 rows.

## Streamlit-in-Snowflake (SiS) rules

- **Sandbox blocks external origins**: no Google Fonts, no CDN icons. The
  design system uses a system font stack and inline SVG icons via
  `icon(name, size, color)` from `utils/styles.py` (Lucide-style paths in
  `_ICON_PATHS`). **Never use emoji as icons** in HTML.
- Native widgets (`st.button`, `st.tabs`, `st.expander` labels) render text
  only — no SVG. Buttons may use Material shortcodes
  (`st.button(..., icon=":material/send:")`) guarded by try/except
  TypeError for older runtimes (see `_btn()` in `1_New_Adjustment.py`).
- **Selection controls**: do NOT use `st.radio`/`st.pills` for one-click
  selection — passing a default each rerun fights widget state and the
  first click reverts. Use the `_pill_row()` pattern: a row of small
  buttons, selected state driven purely by app state (`type="primary"`),
  click → update state → `safe_rerun()`.
- Widget keys are versioned via `_k(name)`; `reset_wizard()` bumps the
  version so widgets recreate fresh. Always use `_k()` for wizard widgets.
- **CSS caution**: Streamlit wrapper test-ids differ across versions. CSS on
  `[data-testid="stVerticalBlockBorderWrapper"]` etc. may silently not
  apply in SiS. For spacing/dividers, emit explicit elements in the page
  flow (see `_card()` context manager and `_sec()` header in
  `1_New_Adjustment.py`). To style ONE specific widget, use its key class
  `div[class*="st-key-<key>"]` (Streamlit ≥1.39) with a graceful fallback.
- `current_user_name()` relies on `CURRENT_USER()` + the owner role having
  `READ SESSION` grant.

## Design system (utils/styles.py)

- Tokens: MUFG red `#D50032` (brand), slate neutrals (`#0F172A` ink,
  `#475569` secondary, `#E2E5EA` border, `#F6F7F9` bg), radius 6/10/14,
  three shadow levels, tabular numerals for numbers.
- **Red = action only.** Solid red is reserved for the primary CTA (Submit),
  the sidebar brand stripe, and errors/destructive actions. Selected states
  are quiet: light tint + red border + red text. Informational emphasis
  (ticket values, progress) is dark ink/slate. Do not reintroduce solid red
  fills for selections — in a risk app red reads as "Failed".
- Config dicts (`STATUS_ICONS`, `SCOPE_CONFIG`, `TYPE_CONFIG`,
  `STAGE_CONFIG`, `CATEGORY_CONFIG`) store **icon names**, not glyphs.
  Always render with `icon(cfg["icon"], ...)` — embedding the raw value
  prints the word ("bar-chart"). Never put these in native widget labels.
- New Adjustment page = "Order Ticket" layout: numbered section cards
  (`_card()` + `_sec(n, title, helper)`) on the left, live "Ticket Summary"
  panel (`_ticket_html`) with completion checklist + Submit on the right.

## Active work / recent state

- **Entity Roll v2 (EROL)** — approved spec at
  `docs/superpowers/specs/2026-06-11-entity-roll-flatten-design.md`,
  implementation pending. Summary: replace the physical delete+copy in the
  EntityRoll branch of `SP_PROCESS_ADJUSTMENT` with set-based offset legs
  (−adjusted(target COB+entity) and +adjusted(source COB+entity) from
  `FACT_ADJUSTED_TABLE`, netted per surrogate key into the
  ADJUSTMENTS_TABLE under one new ADJUSTMENT_ID). No DML on FACT or the
  combined view; deleting the adjustment restores the entity. Rename type
  `Entity_Roll` → `EROL` end-to-end (UI payload, `ACTION_MAP` in
  `03_sp_submit_adjustment.sql`, ADJ_HEADER, DIMENSION.ADJUSTMENT). Read the
  spec before implementing — it covers SCD2 key fixes, summary rebuild,
  RECORD_COUNT, and the test plan. **The UI wording for Entity Roll still
  describes the old delete+copy behaviour and must be updated when the SP
  changes.**
- A concurrency redesign for the pipeline was audited and is pending
  approval — do not assume single-writer.

## Conventions

- DDL uses `CREATE OR ALTER` where possible; deployments should not destroy
  data. Test adjustments are opt-in, never seeded by default.
- Commits: conventional prefixes (`fix(submit):`, `ux(wizard):`,
  `docs:`), imperative subject, body explains the why.
- Verification: there is **no Python test harness for the Streamlit app**.
  Minimum bar: `python3 -m py_compile` every touched file, then deploy to
  SiS and click through the affected flow. SQL-side tests live in
  `new_adjustment_db_objects/tests/`. Snowflake behaviour (literal
  escaping, SP results) cannot be fully verified locally — say so in the
  commit/PR instead of claiming verification.
- When the user reports a bug: find the root cause before patching
  (read the code path end-to-end; the bug class here is usually an
  escaping/NaN/widget-state/test-id-mismatch issue, see invariants above).
- UI text: plain English for non-technical users; error messages must say
  what happened AND what to do next. Reference adjustments by report number
  (`fmt_adj_id` / `DIMENSION_ADJ_ID`), never raw UUID hashes.

## Quick file map

| Area | File |
|---|---|
| Dashboard / KPIs | `streamlit_app/app.py` |
| New Adjustment (order ticket) | `streamlit_app/pages/1_New_Adjustment.py` |
| Browse/manage + delete + history | `streamlit_app/pages/2_Adjustments.py` |
| Approval queue | `streamlit_app/pages/3_Approval_Queue.py` |
| Pipeline monitor / force process | `streamlit_app/pages/4_Adjustment_Pipeline.py` |
| Admin (settings, sign-off, approvers) | `streamlit_app/pages/5_Admin.py` |
| Design tokens, icons, components | `streamlit_app/utils/styles.py` |
| Session/queries | `streamlit_app/utils/snowflake_conn.py` |
| Tables + settings seed | `new_adjustment_db_objects/01_tables.sql` |
| Submit SP (validation, ACTION_MAP, blocking) | `new_adjustment_db_objects/03_sp_submit_adjustment.sql` |
| Preview SP (summary/breakdown/sample modes) | `new_adjustment_db_objects/04_sp_preview_adjustment.sql` |
| Process SP (Direct/Scale/EntityRoll paths) | `new_adjustment_db_objects/05_sp_process_adjustment.sql` |
| Entity Roll v2 spec | `docs/superpowers/specs/2026-06-11-entity-roll-flatten-design.md` |
