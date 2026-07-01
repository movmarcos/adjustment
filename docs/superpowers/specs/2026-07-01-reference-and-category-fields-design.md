# New Adjustment: Reference + Adjustment Category — Design

**Date:** 2026-07-01
**Status:** Approved (design) — pending spec review

## Problem

The New Adjustment wizard needs two new inputs on every adjustment type:
1. **Reference** — free text stored in the existing `ADJ_HEADER.GLOBAL_REFERENCE`
   column. Today only the Direct form captures it; Scaling and Entity Roll do not.
2. **Adjustment Category** — a **required** value chosen from a managed list,
   stored in a **new** `ADJ_HEADER.ADJUSTMENT_CATEGORY` column and backed by a
   **new** reference table. When the adjustment is written to
   `DIMENSION.ADJUSTMENT`, its `REASON` must be stored as
   `"<category> | <reason>"`.

## Decisions (from brainstorming)

- Category is **required** (enforced in the UI; Submit disabled until picked).
- Category list is **seed-only** for now: a table seeded on deploy; changes are
  made by editing the seed + redeploying (no admin UI).
- Reference is **optional free text**.
- The `"<category> | <reason>"` format is applied **only when inserting into
  `DIMENSION.ADJUSTMENT`**. `ADJ_HEADER` keeps `ADJUSTMENT_CATEGORY` and `REASON`
  as separate columns.
- The existing `GLOBAL_REFERENCE` **dedup** (soft-delete prior rows with the same
  `COBID + GLOBAL_REFERENCE`) is **scoped to Direct only**, so adding Reference to
  Scaling/Entity Roll is a purely informational tag with no supersede surprise.
- **No** server-side category validation in the submit proc — the UI dropdown is
  the gate.

## Non-Goals

- No admin screen to manage categories (seed-only).
- No `ADJUSTMENT_CATEGORY` column on `DIMENSION.ADJUSTMENT` (it's a shared dim we
  don't own) — the category is folded into that table's `REASON` instead.
- No change to how existing adjustments (already processed) are displayed.

## Design

### 1. New table `ADJUSTMENT_APP.ADJ_CATEGORY` (`01_tables.sql`)

Mirror the `ADJUSTMENTS_SETTINGS` seed pattern (idempotent `DELETE` + `INSERT`):

```sql
CREATE OR ALTER TABLE ADJUSTMENT_APP.ADJ_CATEGORY (
    CATEGORY_NAME  VARCHAR(100) NOT NULL,   -- stored value + display label
    IS_ACTIVE      BOOLEAN          DEFAULT TRUE,
    SORT_ORDER     NUMBER(38,0),
    CREATED_DATE   TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_ADJ_CATEGORY PRIMARY KEY (CATEGORY_NAME)
)
COMMENT = 'Managed list of adjustment categories for the New Adjustment page.';

DELETE FROM ADJUSTMENT_APP.ADJ_CATEGORY;
INSERT INTO ADJUSTMENT_APP.ADJ_CATEGORY (CATEGORY_NAME, SORT_ORDER) VALUES
  ('Adjusted by MRM Upload', 10), ('Bank Holiday', 20), ('Booking Error', 30),
  ('IT-Other', 40), ('Late Booking', 50), ('Market Data Error', 60),
  ('Missing Trade', 70), ('Model Limitation', 80),
  ('Murex System Limitation', 90), ('New Business Issue', 100),
  ('PRO Cash Adjustment', 110), ('QuantServer System Issue', 120),
  ('QuIC System Limitation', 130), ('Raptor Reporting Issue', 140),
  ('Reference Data Error', 150), ('Structured Trade Issue', 160),
  ('Time Series Issue', 170), ('Valuation Source Issue', 180),
  ('VaR Window Issue', 190);
```
Also: add it to the file's `VERIFY` `UNION ALL` block, and to the `--rebuild`
drop list in `deploy.py`.

### 2. New column on `ADJ_HEADER` (`01_tables.sql`)

Add next to `REASON` in the `CREATE OR ALTER TABLE ADJ_HEADER` definition:
```sql
ADJUSTMENT_CATEGORY  VARCHAR(100) COLLATE 'en-ci',
```
(`CREATE OR ALTER` re-applies cleanly, adding the column to existing tables.)

### 3. New Adjustment page (`streamlit_app/pages/1_New_Adjustment.py`)

- Add wizard defaults: `"adjustment_category": None`. (`"global_reference"`
  already exists.)
- Add a cached reader:
  ```python
  def _category_options():
      rows = _ref_rows(
          "SELECT CATEGORY_NAME FROM ADJUSTMENT_APP.ADJ_CATEGORY "
          "WHERE IS_ACTIVE = TRUE ORDER BY SORT_ORDER, CATEGORY_NAME",
          "_ref_categories")
      return [str(r[0]) for r in rows if r[0] is not None]
  ```
- In each of the three forms (Scaling ~746, Direct ~843/853, Entity Roll ~986),
  near the Reason box:
  - **Adjustment Category** `st.selectbox` (required) → `wiz["adjustment_category"]`,
    options from `_category_options()`, with a blank/placeholder first entry so
    "nothing selected" is detectable.
  - **Reference** `st.text_input` → `wiz["global_reference"]` on **Scaling** and
    **Entity Roll** (Direct already has its "Reference *" input — leave as-is).
- `_build_payload()` (each of the 3 branches): set
  `payload["adjustment_category"] = wiz.get("adjustment_category")` and, for
  Scaling/Entity Roll, `payload["global_reference"] = wiz.get("global_reference")`.
  `reason` stays the plain justification.
- Completion checks (`_completion_checks` / `_missing_fields`): add
  "Adjustment Category" as a required check so Submit is disabled until chosen.

### 4. Submit stored proc (`03_sp_submit_adjustment.sql`)

- Add to `col_map`: `"ADJUSTMENT_CATEGORY": adj.get("adjustment_category")`.
  (Dynamic INSERT already picks up any non-null `col_map` key — no other change.)
- **Dedup scoping:** the `GLOBAL_REFERENCE` soft-delete/dedup block
  (~lines 286-356) must only run when `ADJUSTMENT_ACTION = 'Direct'` (guard the
  block on the action), so a reference on Scaling/Entity Roll never supersedes a
  prior adjustment.
- Update the docstring's expected-keys list to include `adjustment_category`.

### 5. Process stored proc (`05_sp_process_adjustment.sql`)

In `insert_to_dimension_and_get_ids` (the single `DIMENSION.ADJUSTMENT` insert,
shared by Direct/Scale/EntityRoll), change the `REASON` value in the `SELECT`
from `REASON` to:
```sql
IFF(ADJUSTMENT_CATEGORY IS NOT NULL AND ADJUSTMENT_CATEGORY <> '',
    ADJUSTMENT_CATEGORY || ' | ' || REASON, REASON)  AS REASON
```
(Category is required going forward, but the `IFF` keeps older rows that predate
the column — with a NULL category — rendering as plain `REASON`.) This is the
only place the combined format is produced; all three paths flow through it.

## Data Flow

```
New Adjustment form
  ├─ category (required)  → wiz.adjustment_category ─┐
  ├─ reference (optional) → wiz.global_reference ───┐│
  └─ reason (required)    → wiz.reason ────────────┐││
                                                   ▼▼▼
        _build_payload → JSON → SP_SUBMIT_ADJUSTMENT
                                   └─ INSERT ADJ_HEADER
                                        (ADJUSTMENT_CATEGORY, GLOBAL_REFERENCE, REASON separate)
                                        (dedup only if ADJUSTMENT_ACTION='Direct')
                                   ▼
        SP_PROCESS_ADJUSTMENT → insert_to_dimension_and_get_ids
                                   └─ INSERT DIMENSION.ADJUSTMENT
                                        REASON = category || ' | ' || reason
```

## Error Handling

- Empty category → Submit disabled (UI); if the proc is somehow called without
  one, `ADJUSTMENT_CATEGORY` is NULL and the dim `REASON` falls back to plain
  `REASON` (no crash).
- `_category_options()` on a query failure returns `[]` (existing `_ref_rows`
  try/except) — the selectbox shows no options; Submit stays blocked (a
  pre-existing failure mode of the DB-backed dropdowns).

## Testing

Streamlit-on-Snowflake — verification is manual after deploy:
1. `ADJ_CATEGORY` exists and holds the 19 seeded rows.
2. `ADJ_HEADER` has `ADJUSTMENT_CATEGORY`.
3. New Adjustment: category dropdown lists the 19; Submit blocked until chosen;
   Reference visible on all three forms.
4. Submit one of each type → `ADJ_HEADER` has category + reference in their own
   columns, `REASON` plain.
5. Process it → `DIMENSION.ADJUSTMENT.REASON` reads `"<category> | <reason>"`.
6. A Scaling adjustment reusing a Reference at the same COB does **not** supersede
   the earlier one (dedup Direct-only); re-uploading a Direct with the same
   reference still does.

## Files Touched

- `new_adjustment_db_objects/01_tables.sql` — new `ADJ_CATEGORY` table + seed;
  `ADJUSTMENT_CATEGORY` column on `ADJ_HEADER`; VERIFY block.
- `deploy.py` — add `ADJ_CATEGORY` to the `--rebuild` drop list.
- `streamlit_app/pages/1_New_Adjustment.py` — category dropdown + reference input,
  wizard defaults, payload, completion checks.
- `new_adjustment_db_objects/03_sp_submit_adjustment.sql` — `col_map` entry;
  scope dedup to Direct; docstring.
- `new_adjustment_db_objects/05_sp_process_adjustment.sql` — combined `REASON` in
  the `DIMENSION.ADJUSTMENT` insert.
