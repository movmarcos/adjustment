# Reference + Adjustment Category Fields — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a required Adjustment Category dropdown (backed by a new seeded table + new `ADJ_HEADER` column) and a Reference field (→ `GLOBAL_REFERENCE`) to all three New Adjustment forms, and store `DIMENSION.ADJUSTMENT.REASON` as `"<category> | <reason>"`.

**Architecture:** `ADJ_HEADER` keeps `ADJUSTMENT_CATEGORY` and `REASON` as separate columns; the combined format is produced in exactly one place — the shared `DIMENSION.ADJUSTMENT` insert. The category list is a new seeded reference table the Streamlit page reads into a dropdown. The `GLOBAL_REFERENCE` dedup is scoped to Direct so Reference is a safe informational tag elsewhere.

**Tech Stack:** Snowflake SQL (tables + Python stored procs), Streamlit-in-Snowflake (Python), deploy.py (globs `*.sql` in numeric order).

## Global Constraints

- `DIMENSION.ADJUSTMENT.REASON` must be `"<category> | <reason>"` for new adjustments; older rows (NULL category) stay plain `REASON`.
- Category is REQUIRED in the UI (Submit disabled until chosen). No server-side category validation.
- Category list is seed-only (edit seed + redeploy to change). Seed the 19 exact values, in the given order.
- `ADJUSTMENT_CATEGORY` and `REASON` stay separate columns on `ADJ_HEADER`; concatenation happens only at the `DIMENSION.ADJUSTMENT` insert.
- The `GLOBAL_REFERENCE` dedup/soft-delete block runs ONLY for Direct adjustments.
- Follow existing patterns: `CREATE OR ALTER TABLE`, idempotent `DELETE`+`INSERT` seed, `_ref_rows()` cached dropdown reader, dynamic `col_map` insert.
- The 19 category values (exact, in order): Adjusted by MRM Upload; Bank Holiday; Booking Error; IT-Other; Late Booking; Market Data Error; Missing Trade; Model Limitation; Murex System Limitation; New Business Issue; PRO Cash Adjustment; QuantServer System Issue; QuIC System Limitation; Raptor Reporting Issue; Reference Data Error; Structured Trade Issue; Time Series Issue; Valuation Source Issue; VaR Window Issue.

---

## File Structure

- `new_adjustment_db_objects/01_tables.sql` — new `ADJ_CATEGORY` table + seed; `ADJUSTMENT_CATEGORY` column on `ADJ_HEADER`; VERIFY row.
- `deploy.py` — add `ADJ_CATEGORY` to the `--rebuild` drop list.
- `new_adjustment_db_objects/05_sp_process_adjustment.sql` — combined `REASON` in the `DIMENSION.ADJUSTMENT` insert (line ~240).
- `new_adjustment_db_objects/03_sp_submit_adjustment.sql` — `col_map` entry; scope dedup to Direct; docstring.
- `streamlit_app/pages/1_New_Adjustment.py` — wizard default, category reader, dropdown + reference on the 3 forms, payload, completion checks.

---

### Task 1: Database — ADJ_CATEGORY table, seed, and ADJ_HEADER column

**Files:**
- Modify: `new_adjustment_db_objects/01_tables.sql`
- Modify: `deploy.py` (the `--rebuild` drop list)

**Interfaces:**
- Produces: table `ADJUSTMENT_APP.ADJ_CATEGORY(CATEGORY_NAME, IS_ACTIVE, SORT_ORDER, CREATED_DATE)`; column `ADJUSTMENT_APP.ADJ_HEADER.ADJUSTMENT_CATEGORY VARCHAR(100)`.

- [ ] **Step 1: Add the `ADJUSTMENT_CATEGORY` column to `ADJ_HEADER`**

In `01_tables.sql`, in the `CREATE OR ALTER TABLE ADJUSTMENT_APP.ADJ_HEADER` block, find the `-- Business context` section (currently line 75-76):

```sql
    -- Business context
    REASON                      VARCHAR(1000) COLLATE 'en-ci',
```

Replace with:

```sql
    -- Business context
    ADJUSTMENT_CATEGORY         VARCHAR(100)  COLLATE 'en-ci',   -- from ADJ_CATEGORY; required in UI
    REASON                      VARCHAR(1000) COLLATE 'en-ci',
```

- [ ] **Step 2: Add the `ADJ_CATEGORY` table + seed**

In `01_tables.sql`, immediately BEFORE the `-- 8. VERIFY` section (the block that starts with `SELECT 'ADJ_HEADER' AS OBJECT, COUNT(*) ...`), insert:

```sql
-- ═══════════════════════════════════════════════════════════════════════════
-- ADJ_CATEGORY — managed list of adjustment categories for the New Adjustment
-- page. Seed-only (edit here + redeploy to change). Seed is idempotent.
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR ALTER TABLE ADJUSTMENT_APP.ADJ_CATEGORY (
    CATEGORY_NAME  VARCHAR(100) NOT NULL,          -- stored value + display label
    IS_ACTIVE      BOOLEAN          DEFAULT TRUE,
    SORT_ORDER     NUMBER(38,0),
    CREATED_DATE   TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_ADJ_CATEGORY PRIMARY KEY (CATEGORY_NAME)
)
COMMENT = 'Managed list of adjustment categories for the New Adjustment page.';

DELETE FROM ADJUSTMENT_APP.ADJ_CATEGORY;
INSERT INTO ADJUSTMENT_APP.ADJ_CATEGORY (CATEGORY_NAME, SORT_ORDER) VALUES
    ('Adjusted by MRM Upload', 10),
    ('Bank Holiday', 20),
    ('Booking Error', 30),
    ('IT-Other', 40),
    ('Late Booking', 50),
    ('Market Data Error', 60),
    ('Missing Trade', 70),
    ('Model Limitation', 80),
    ('Murex System Limitation', 90),
    ('New Business Issue', 100),
    ('PRO Cash Adjustment', 110),
    ('QuantServer System Issue', 120),
    ('QuIC System Limitation', 130),
    ('Raptor Reporting Issue', 140),
    ('Reference Data Error', 150),
    ('Structured Trade Issue', 160),
    ('Time Series Issue', 170),
    ('Valuation Source Issue', 180),
    ('VaR Window Issue', 190);
```

- [ ] **Step 3: Add `ADJ_CATEGORY` to the VERIFY block**

In `01_tables.sql`, the VERIFY `UNION ALL` block ends with:

```sql
UNION ALL SELECT 'ADJ_APPROVERS', COUNT(*) FROM ADJUSTMENT_APP.ADJ_APPROVERS;
```

Change it to add a row (keep the semicolon on the last line):

```sql
UNION ALL SELECT 'ADJ_APPROVERS', COUNT(*) FROM ADJUSTMENT_APP.ADJ_APPROVERS
UNION ALL SELECT 'ADJ_CATEGORY', COUNT(*) FROM ADJUSTMENT_APP.ADJ_CATEGORY;
```

- [ ] **Step 4: Add `ADJ_CATEGORY` to the `--rebuild` drop list in `deploy.py`**

Find the list of tables dropped on `--rebuild` (near `def` handling `--rebuild`, the `DROP ... TABLE` set for repo-managed tables). Add `ADJUSTMENT_APP.ADJ_CATEGORY` to that list, alongside the other `ADJUSTMENT_APP.*` tables. (Locate it with: `grep -n "ADJ_APPROVERS\|rebuild\|DROP TABLE" deploy.py`.) If the list is a Python list of names, append `"ADJUSTMENT_APP.ADJ_CATEGORY"` in the same style as its neighbors.

- [ ] **Step 5: Verify the SQL renders and parses**

Run:
```bash
cd /Users/marcosmagri/Documents/MUFG/adjustment
python3 -c "import config; s=config.render(open('new_adjustment_db_objects/01_tables.sql').read()); assert 'ADJ_CATEGORY' in s and 'ADJUSTMENT_CATEGORY' in s; print('ok, no leftover tokens:', '{{' not in s)"
```
Expected: `ok, no leftover tokens: True`.

- [ ] **Step 6: Commit**

```bash
git add new_adjustment_db_objects/01_tables.sql deploy.py
git commit -m "feat(db): ADJ_CATEGORY table + seed; ADJUSTMENT_CATEGORY column on ADJ_HEADER"
```

---

### Task 2: Process SP — combined REASON into DIMENSION.ADJUSTMENT

**Files:**
- Modify: `new_adjustment_db_objects/05_sp_process_adjustment.sql` (the `INSERT INTO DIMENSION.ADJUSTMENT ... SELECT`, ~line 240)

**Interfaces:**
- Consumes: `ADJ_HEADER.ADJUSTMENT_CATEGORY` (Task 1).
- Produces: `DIMENSION.ADJUSTMENT.REASON = "<category> | <reason>"`.

- [ ] **Step 1: Change the REASON value in the SELECT**

In `05_sp_process_adjustment.sql`, inside `insert_to_dimension_and_get_ids`, the `SELECT` feeding the insert has this line (currently line 240):

```sql
            CREATED_DATE, CURRENT_TIMESTAMP(), USERNAME, 'Running', REASON,
```

Replace `REASON,` on THAT line with the category-prefixed expression (leave the `INSERT` column list at line ~226 unchanged — it still lists `REASON`):

```sql
            CREATED_DATE, CURRENT_TIMESTAMP(), USERNAME, 'Running',
            IFF(ADJUSTMENT_CATEGORY IS NOT NULL AND ADJUSTMENT_CATEGORY <> '',
                ADJUSTMENT_CATEGORY || ' | ' || REASON, REASON),
```

(The other `REASON` in the `INSERT (... REASON ...)` column list stays as-is — this only changes the value expression in the `SELECT`.)

- [ ] **Step 2: Verify the embedded Python still parses**

Run:
```bash
cd /Users/marcosmagri/Documents/MUFG/adjustment
python3 - <<'PY'
import ast
lines = open("new_adjustment_db_objects/05_sp_process_adjustment.sql").read().splitlines()
b = next(i for i,l in enumerate(lines) if l.strip()=="$$")
e = next(i for i,l in enumerate(lines) if l.strip()=="$$;")
ast.parse("\n".join(lines[b+1:e])); print("ok")
PY
```
Expected: `ok`.

- [ ] **Step 3: Commit**

```bash
git add new_adjustment_db_objects/05_sp_process_adjustment.sql
git commit -m "feat(process): write DIMENSION.ADJUSTMENT.REASON as '<category> | <reason>'"
```

---

### Task 3: Submit SP — category column + Direct-only dedup

**Files:**
- Modify: `new_adjustment_db_objects/03_sp_submit_adjustment.sql`

**Interfaces:**
- Consumes: payload keys `adjustment_category`, `global_reference` (Task 4).
- Produces: `ADJ_HEADER.ADJUSTMENT_CATEGORY` persisted; dedup only for Direct.

- [ ] **Step 1: Add `ADJUSTMENT_CATEGORY` to `col_map`**

In `03_sp_submit_adjustment.sql`, the `col_map` dict has (line ~394):

```python
            "REASON":                      adj.get("reason"),
```

Insert a line immediately before it:

```python
            "ADJUSTMENT_CATEGORY":         adj.get("adjustment_category"),
            "REASON":                      adj.get("reason"),
```

(The dynamic INSERT drops NULLs and picks up any non-null `col_map` key — no other insert change needed.)

- [ ] **Step 2: Scope the GLOBAL_REFERENCE dedup to Direct only**

`adjustment_type` is defined at line ~198 (before the dedup block). The dedup guard is currently (line ~288):

```python
        if global_ref and str(global_ref).strip():
```

Replace with (Direct-only):

```python
        if str(adjustment_type).lower() == 'direct' and global_ref and str(global_ref).strip():
```

- [ ] **Step 3: Document the new key in the docstring**

In the SP docstring's expected-keys list (near line 153-186), add a line next to `reason` / `global_reference`:

```
      adjustment_category (optional)  str     Category label; stored in ADJ_HEADER.ADJUSTMENT_CATEGORY
```

- [ ] **Step 4: Verify the embedded Python parses**

Run:
```bash
cd /Users/marcosmagri/Documents/MUFG/adjustment
python3 - <<'PY'
import ast
lines = open("new_adjustment_db_objects/03_sp_submit_adjustment.sql").read().splitlines()
b = next(i for i,l in enumerate(lines) if l.strip()=="$$")
e = next(i for i,l in enumerate(lines) if l.strip()=="$$;")
ast.parse("\n".join(lines[b+1:e])); print("ok")
PY
```
Expected: `ok`.

- [ ] **Step 5: Commit**

```bash
git add new_adjustment_db_objects/03_sp_submit_adjustment.sql
git commit -m "feat(submit): persist ADJUSTMENT_CATEGORY; scope GLOBAL_REFERENCE dedup to Direct"
```

---

### Task 4: New Adjustment page — category dropdown + reference

**Files:**
- Modify: `streamlit_app/pages/1_New_Adjustment.py`

**Interfaces:**
- Consumes: `ADJ_CATEGORY` table (Task 1).
- Produces: payload keys `adjustment_category` and `global_reference` consumed by Task 3.

- [ ] **Step 1: Add the wizard default**

In `_WIZ_DEFAULTS` (line ~36), find:

```python
    "reason":                 "",
    "requires_approval":      False,
```

Replace with:

```python
    "reason":                 "",
    "adjustment_category":    None,
    "requires_approval":      False,
```

- [ ] **Step 2: Add the category options reader**

After `_entity_options()` (line ~617), add a new reader that mirrors it:

```python
def _category_options():
    rows = _ref_rows(
        "SELECT CATEGORY_NAME FROM ADJUSTMENT_APP.ADJ_CATEGORY "
        "WHERE IS_ACTIVE = TRUE ORDER BY SORT_ORDER, CATEGORY_NAME", "_ref_categories")
    return [str(r[0]) for r in rows if r[0] is not None]
```

- [ ] **Step 3: Add category + reference to the SCALING form's Business Context**

In the Scaling form, the Business Context card (line ~743-748) is:

```python
    # ── Reason ───────────────────────────────────────────────────────────
    with _card():
        _sec(6, "Business Context", "Why is this adjustment needed?")
        wiz["reason"] = st.text_area("Reason / Business Justification *",
                                     value=wiz.get("reason", ""), height=70,
                                     key=_k("reason"))
```

Replace with:

```python
    # ── Reason ───────────────────────────────────────────────────────────
    with _card():
        _sec(6, "Business Context", "Why is this adjustment needed?")
        wiz["adjustment_category"] = _code_select(
            "Adjustment Category *", _k("adj_category"),
            wiz.get("adjustment_category"), _category_options()) or None
        wiz["global_reference"] = st.text_input(
            "Reference", key=_k("scale_ref"),
            value=wiz.get("global_reference") or "",
            help="Optional free-text reference for this adjustment.").strip() or None
        wiz["reason"] = st.text_area("Reason / Business Justification *",
                                     value=wiz.get("reason", ""), height=70,
                                     key=_k("reason"))
```

- [ ] **Step 4: Add category to the DIRECT form's Business Context (Reference already exists)**

In the Direct form, the Business Context card (line ~851-855) is:

```python
    with _card():
        _sec(5, "Business Context", "Why is this adjustment needed?")
        wiz["reason"] = st.text_area("Reason / Business Justification *",
                                     value=wiz.get("reason", ""), height=70,
                                     key=_k("var_reason"))
```

Replace with:

```python
    with _card():
        _sec(5, "Business Context", "Why is this adjustment needed?")
        wiz["adjustment_category"] = _code_select(
            "Adjustment Category *", _k("var_adj_category"),
            wiz.get("adjustment_category"), _category_options()) or None
        wiz["reason"] = st.text_area("Reason / Business Justification *",
                                     value=wiz.get("reason", ""), height=70,
                                     key=_k("var_reason"))
```

- [ ] **Step 5: Add category + reference to the ENTITY ROLL form's Business Context**

In the Entity Roll form, the Business Context card (line ~984-988) is:

```python
    with _card():
        _sec(4, "Business Context", "Why is this roll needed?")
        wiz["reason"] = st.text_area("Reason / Business Justification *",
                                     value=wiz.get("reason", ""), height=70, key=_k("er_reason"),
                                     placeholder="e.g. Rolling MUSE VaR from previous business day")
```

Replace with:

```python
    with _card():
        _sec(4, "Business Context", "Why is this roll needed?")
        wiz["adjustment_category"] = _code_select(
            "Adjustment Category *", _k("er_adj_category"),
            wiz.get("adjustment_category"), _category_options()) or None
        wiz["global_reference"] = st.text_input(
            "Reference", key=_k("er_ref"),
            value=wiz.get("global_reference") or "",
            help="Optional free-text reference for this roll.").strip() or None
        wiz["reason"] = st.text_area("Reason / Business Justification *",
                                     value=wiz.get("reason", ""), height=70, key=_k("er_reason"),
                                     placeholder="e.g. Rolling MUSE VaR from previous business day")
```

- [ ] **Step 6: Add `adjustment_category` (and reference for EROL) to `_build_payload`**

In `_build_payload()` (line ~107), all three branches build a dict. Add `adjustment_category` to each, and `global_reference` to the Entity Roll and Scaling branches (Direct already has it).

Direct branch (line ~111-123) — add the category key:
```python
            "global_reference":      wiz.get("global_reference", ""),
            "adjustment_category":   wiz.get("adjustment_category"),
        }
```

Entity Roll branch (line ~126-136) — add category + reference before the closing `}`:
```python
            "adjustment_occurrence": "ADHOC",
            "global_reference":      wiz.get("global_reference"),
            "adjustment_category":   wiz.get("adjustment_category"),
        }
```

Scaling branch — after the `payload = { ... }` literal (line ~139-149), add two lines right after the dict is created (before the `if wiz.get("occurrence") == "RECURRING":`):
```python
    payload["adjustment_category"] = wiz.get("adjustment_category")
    if wiz.get("global_reference"):
        payload["global_reference"] = wiz.get("global_reference")
```

- [ ] **Step 7: Require category in the completion checklist**

In `_completion_checks()` (line ~521), add an Adjustment Category check to each of the three branches so Submit stays disabled until chosen. In the Direct branch (after line 534 `("Reason", ...)`), the Entity Roll branch (after line 542), and the Scaling branch (after line 563), add this tuple to each `checks += [ ... ]` list:

```python
            ("Adjustment Category", bool((wiz.get("adjustment_category") or "").strip())),
```

(Add it inside each of the three `checks += [...]` blocks. `_missing_fields()` derives from these checks, so this both shows the item in the checklist and gates the Submit button.)

- [ ] **Step 8: Verify the page compiles**

Run:
```bash
cd /Users/marcosmagri/Documents/MUFG/adjustment
/opt/homebrew/bin/python3.11 -m py_compile streamlit_app/pages/1_New_Adjustment.py && echo ok
```
Expected: `ok`.

- [ ] **Step 9: Commit**

```bash
git add streamlit_app/pages/1_New_Adjustment.py
git commit -m "feat(new-adjustment): required Adjustment Category dropdown + Reference field on all forms"
```

---

### Task 5: End-to-end verification (deploy + manual)

**Files:** none (verification only).

- [ ] **Step 1: Deploy** — `python deploy.py`. Confirm it reports creating `ADJ_CATEGORY` and updating `ADJ_HEADER`, `SP_SUBMIT_ADJUSTMENT`, `SP_PROCESS_ADJUSTMENT`, and the Streamlit app, with no errors.

- [ ] **Step 2: Data checks:**
```sql
SELECT COUNT(*) FROM ADJUSTMENT_APP.ADJ_CATEGORY;                 -- expect 19
DESC TABLE ADJUSTMENT_APP.ADJ_HEADER;                            -- has ADJUSTMENT_CATEGORY
```

- [ ] **Step 3: New Adjustment UI** — on each of Scaling, Direct, Entity Roll: the **Adjustment Category** dropdown lists the 19 values; Submit is disabled until a category is chosen; a **Reference** field is present (Direct's existing one; new on Scaling/EROL).

- [ ] **Step 4: Submit one Scaling adjustment** with a category, a reference, and a reason. Verify:
```sql
SELECT ADJUSTMENT_CATEGORY, GLOBAL_REFERENCE, REASON
FROM ADJUSTMENT_APP.ADJ_HEADER ORDER BY CREATED_DATE DESC LIMIT 1;
-- category + reference in their own columns; REASON is the plain justification (no prefix)
```

- [ ] **Step 5: Process it** (approve/force as needed) and verify the dim reason format:
```sql
SELECT REASON FROM DIMENSION.ADJUSTMENT ORDER BY ADJUSTMENT_ID DESC LIMIT 1;
-- expect: '<category> | <reason>'
```

- [ ] **Step 6: Dedup scoping** — submit two Scaling adjustments at the same COB with the same Reference; confirm the first is NOT superseded (both remain, not `Replaced`). Then re-submit a Direct upload with the same COB + Reference and confirm the prior Direct IS replaced (existing behavior preserved).

---

## Self-Review

**Spec coverage:**
- New `ADJ_CATEGORY` table + seed → Task 1. ✓
- `ADJUSTMENT_CATEGORY` column on `ADJ_HEADER` → Task 1. ✓
- Category dropdown (required, DB-backed) on all 3 forms → Task 4 (Steps 2-5, 7). ✓
- Reference field on all 3 forms → Task 4 (Direct existing; Scaling Step 3; EROL Step 5). ✓
- `col_map` category entry → Task 3 Step 1. ✓
- Dedup scoped to Direct → Task 3 Step 2. ✓
- `DIMENSION.ADJUSTMENT.REASON = "<category> | <reason>"` → Task 2. ✓
- deploy `--rebuild` list → Task 1 Step 4. ✓

**Placeholder scan:** No TBD/TODO; every code step shows exact before/after. ✓

**Type consistency:** payload key `adjustment_category` (Task 4 Step 6) == `col_map` `adj.get("adjustment_category")` (Task 3 Step 1). `global_reference` matches the existing payload/col_map key. Reader `_category_options()` name consistent across Task 4 Steps 2-5. Column `ADJUSTMENT_CATEGORY` consistent across Tasks 1/2/3. ✓

**Risk note:** Manual verification (Task 5) is the primary test surface (Streamlit-on-SiS + SQL procs; no unit-test harness for these). Python `ast.parse`/`py_compile` and `config.render` checks guard against syntax/token errors before deploy.
