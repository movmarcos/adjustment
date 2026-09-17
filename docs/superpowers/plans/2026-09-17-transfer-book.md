# Transfer Book Implementation Plan (Part B of 2)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A new Scaling adjustment type "Transfer Book": at one COB the target book's positions in scope are replaced by the source book's adjusted values (source untouched), one adjustment per selected scope per trade code.

**Architecture:** Roll with the book swapped for the COB. `ADJ_HEADER` gains `SOURCE_BOOK_CODE`; the header's `BOOK_CODE` is the TARGET book and `ENTITY_CODE` is derived server-side from the target book, so sign-off, overlap checks, supersede-by-filter and every grid work unchanged. The Scale path of `SP_PROCESS_ADJUSTMENT` gets a fourth UNION leg (②T) that reads the source book's rows from the adjusted view and re-keys them to the target book inside the SELECT, so netting cancels the target's flattened rows position by position. `SP_PREVIEW_ADJUSTMENT` mirrors it. The page adds the type pill, a Source/Target form and per-trade fan-out on top of Part A's `_submit_fanout`.

**Tech Stack:** Snowflake Python stored procedures (Snowpark), Streamlit 1.50, pytest (UAT suite in `tests/` needs Snowflake and is NOT runnable here; local unit tests use the scratchpad venv `VENV=/private/tmp/claude-501/-Users-marcosmagri-Documents-MUFG-adjustment/202b9189-fe88-405b-a79e-040561cbbd73/scratchpad/venv/bin/python`).

**Spec:** `docs/superpowers/specs/2026-09-17-transfer-book-multi-scope-design.md` §4–§10. Requires Part A (`docs/superpowers/plans/2026-09-17-multi-scope-scaling.md`) merged first: `_selected_scopes()`, `_submit_fanout()`, `FIELD_LABELS`, type-before-scope order.

## Global Constraints

- `ADJ_HEADER` is `CREATE OR ALTER` and append-only: new columns go LAST (after `VAR_SUB_COMPONENT_NAME`, `01_tables.sql` l.127).
- `ADJUSTMENT_TYPE` value is `Transfer` (stored exactly so, VARCHAR(20)); `ADJUSTMENT_ACTION` is `Scale`. UI label "Transfer Book".
- All user strings into SQL go through the file's own `_esc` (SPs) or `.replace("\\", "\\\\").replace("'", "''")` (page).
- Every temp object in `SP_PROCESS_ADJUSTMENT` is per-run unique (`_{run_log_id}`) — do not add fixed-name objects.
- Engine step SQL must be logged through `_erlog(session, _sqlog, label, sql)` like its neighbours.
- No push; commit per task with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`.
- **Deviation from spec §8 open item, for Marcos to accept:** no `SP_DEBUG_TRANSFER`. The engine already logs the exact executed SQL per run in `ADJUSTMENT_APP.EROL_PROCESS_LOG` (`_erlog`, step `stage_build (netted temp)`); end-to-end verification is a manual DVLP run + that log. Automated coverage is at submit/preview level (Tasks 2, 4).

## File Structure

- Modify `new_adjustment_db_objects/01_tables.sql` — `ADJ_HEADER` + `SOURCE_BOOK_CODE`.
- Modify `new_adjustment_db_objects/08_views.sql` — `VW_MY_WORK`, `VW_APPROVAL_QUEUE` column lists + `SOURCE_BOOK_CODE`.
- Modify `new_adjustment_db_objects/03_sp_submit_adjustment.sql` — type map, factor, guards, entity derivation, `col_map`.
- Modify `new_adjustment_db_objects/05_sp_process_adjustment.sql` — Scale path legs.
- Modify `new_adjustment_db_objects/04_sp_preview_adjustment.sql` — transfer branch.
- Modify `streamlit_app/utils/styles.py` — `TYPE_CONFIG`, `render_filter_chips`.
- Modify `streamlit_app/pages/1_New_Adjustment.py` — type pill, Transfer form, payload, fan-out, checklist, ticket, preview payload.
- Modify `streamlit_app/pages/2_Adjustments.py`, `3_Approval_Queue.py` — "From book" detail row.
- Modify `streamlit_app/pages/7_Documentation.py` — knowledge text.
- Create `tests/test_transfer_book.py` — UAT tests TRF-01..04 (Snowflake).
- Create `streamlit_app/tests/test_transfer_form.py` — local unit tests for the page's pure helpers.

---

### Task 1: Storage — `SOURCE_BOOK_CODE` on `ADJ_HEADER` and the two header views

**Files:**
- Modify: `new_adjustment_db_objects/01_tables.sql:125-128`
- Modify: `new_adjustment_db_objects/08_views.sql` (`VW_MY_WORK` l.185–246, `VW_APPROVAL_QUEUE` l.259–300)

- [ ] **Step 1: Append the column**

In `01_tables.sql`, after `    VAR_SUB_COMPONENT_NAME      VARCHAR(200) COLLATE 'en-ci',` add:
```sql
    -- Transfer Book (2026-09-17): the SOURCE book whose adjusted rows replace
    -- BOOK_CODE (the TARGET book) at COBID. NULL = not a transfer.
    SOURCE_BOOK_CODE            VARCHAR(20)  COLLATE 'en-ci',
```

- [ ] **Step 2: Expose it in the views**

In `08_views.sql`, in both `VW_MY_WORK` and `VW_APPROVAL_QUEUE`, after the line `    h.VAR_SUB_COMPONENT_NAME,` add `    h.SOURCE_BOOK_CODE,`. Check `VW_ADJUSTMENT_TRACK` (l.462+) — if it lists header columns explicitly (grep `h.VAR_SUB_COMPONENT_NAME` inside it), add the column there too; if it uses `h.*` nothing is needed.

- [ ] **Step 3: Verify the SQL is well-formed**

Run: `grep -n "SOURCE_BOOK_CODE" new_adjustment_db_objects/01_tables.sql new_adjustment_db_objects/08_views.sql`
Expected: 1 hit in 01 (inside `ADJ_HEADER`, before `CONSTRAINT PK_ADJ_HEADER`), 2–3 hits in 08.

- [ ] **Step 4: Commit**

```bash
git add new_adjustment_db_objects/01_tables.sql new_adjustment_db_objects/08_views.sql
git commit -m "feat(transfer): ADJ_HEADER.SOURCE_BOOK_CODE (+ header views)

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 2: Submit procedure — accept `Transfer`, validate, derive the entity

**Files:**
- Modify: `new_adjustment_db_objects/03_sp_submit_adjustment.sql` — `ACTION_MAP` (l.40), `compute_scale_factor_adjusted` (l.123), guards (l.315–335), `col_map` (l.567)
- Create: `tests/test_transfer_book.py`
- Modify: `tests/conftest.py` — cleanup list (l.~140)

**Interfaces:**
- Consumes payload keys: `adjustment_type="Transfer"`, `book_code` (target), `source_book_code`, optional `trade_code`, `source_cobid == cobid`, `scale_factor` 1.
- Produces: header row with `ADJUSTMENT_TYPE='Transfer'`, `ADJUSTMENT_ACTION='Scale'`, `SCALE_FACTOR_ADJUSTED=1.0`, `ENTITY_CODE` = target book's entity, `SOURCE_BOOK_CODE`.

- [ ] **Step 1: Write the failing UAT tests**

```python
# tests/test_transfer_book.py
"""
TRF — Transfer Book (spec docs/superpowers/specs/2026-09-17-transfer-book-multi-scope-design.md).
Needs two real CURRENT books in DIMENSION.BOOK (env TEST_TRF_SRC_BOOK / TEST_TRF_TGT_BOOK,
same or different entity). Headers land on the fake COB and are cleaned up by conftest.
"""
import json, os
import pytest

AREA = "Transfer Book"

from conftest import FAKE_COB, U_SUBMIT, call_sp, rows

SP_SUBMIT  = "ADJUSTMENT_APP.SP_SUBMIT_ADJUSTMENT"
SP_PREVIEW = "ADJUSTMENT_APP.SP_PREVIEW_ADJUSTMENT"
SRC = os.environ.get("TEST_TRF_SRC_BOOK", "UATBOOK")
TGT = os.environ.get("TEST_TRF_TGT_BOOK", "UATBOOK2")


def _submit(session, **over):
    payload = {"cobid": FAKE_COB, "process_type": "VaR", "adjustment_type": "Transfer",
               "source_cobid": FAKE_COB, "scale_factor": 1, "username": U_SUBMIT,
               "book_code": TGT, "source_book_code": SRC,
               "reason": "UAT automation — transfer", "requires_approval": False,
               "adjustment_category": "Booking Error"}
    payload.update(over)
    return call_sp(session, SP_SUBMIT, json.dumps(payload))


@pytest.mark.uat("TRF-01", title="Transfer accepted; header stores target/source book and derived entity", priority="P1")
def test_trf01_submit(session, ev):
    res = _submit(session, trade_code="UAT-TRADE-1")
    ev.note("SP result", str(res)[:300])
    ev.check("accepted", isinstance(res, dict) and res.get("status") in ("Pending", "Pending Approval"))
    h = ev.sql("Header", f"""SELECT ADJUSTMENT_TYPE, ADJUSTMENT_ACTION, SCALE_FACTOR_ADJUSTED,
                                    BOOK_CODE, SOURCE_BOOK_CODE, TRADE_CODE, ENTITY_CODE
                             FROM ADJUSTMENT_APP.ADJ_HEADER WHERE ADJ_ID = '{res.get("adj_id")}'""")
    ent = ev.sql("Target book entity", f"""SELECT MAX(ENTITY_CODE) AS E FROM DIMENSION.BOOK
                                           WHERE UPPER(BOOK_CODE) = UPPER('{TGT}') AND IS_CURRENT_ROW = TRUE""")
    ev.check("type/action", h and h[0]["ADJUSTMENT_TYPE"] == "Transfer" and h[0]["ADJUSTMENT_ACTION"] == "Scale")
    ev.check("factor adjusted is 1 (not 0)", h and float(h[0]["SCALE_FACTOR_ADJUSTED"]) == 1.0)
    ev.check("target book in BOOK_CODE, source in SOURCE_BOOK_CODE",
             h and h[0]["BOOK_CODE"].upper() == TGT.upper() and h[0]["SOURCE_BOOK_CODE"].upper() == SRC.upper())
    ev.check("entity derived from the target book",
             h and ent and (h[0]["ENTITY_CODE"] or "").upper() == (ent[0]["E"] or "").upper())


@pytest.mark.uat("TRF-02", title="Transfer guards: same book, unknown book, missing source, cross-COB", priority="P1")
def test_trf02_guards(session, ev):
    r1 = _submit(session, source_book_code=TGT)
    ev.check("same book refused", isinstance(r1, dict) and r1.get("status") == "Error")
    r2 = _submit(session, book_code="ZZ_NO_SUCH_BOOK")
    ev.check("unknown target refused", isinstance(r2, dict) and r2.get("status") == "Error")
    r3 = _submit(session, source_book_code="")
    ev.check("missing source refused", isinstance(r3, dict) and r3.get("status") == "Error")
    r4 = _submit(session, source_cobid=FAKE_COB - 1)
    ev.check("cross-COB refused", isinstance(r4, dict) and r4.get("status") == "Error")


@pytest.mark.uat("TRF-04", title="A pending Flatten on the target book blocks a Transfer", priority="P2")
def test_trf04_overlap_blocks(session, ev):
    ent = rows(session, f"""SELECT MAX(ENTITY_CODE) AS E FROM DIMENSION.BOOK
                            WHERE UPPER(BOOK_CODE) = UPPER('{TGT}') AND IS_CURRENT_ROW = TRUE""")[0]["E"]
    flat = call_sp(session, SP_SUBMIT, json.dumps({
        "cobid": FAKE_COB, "process_type": "Stress", "adjustment_type": "Flatten",
        "username": U_SUBMIT, "entity_code": ent, "book_code": TGT,
        "reason": "UAT automation — blocker", "adjustment_category": "Booking Error"}))
    res = _submit(session, process_type="Stress")
    h = ev.sql("Transfer header", f"""SELECT BLOCKED_BY_ADJ_ID FROM ADJUSTMENT_APP.ADJ_HEADER
                                      WHERE ADJ_ID = '{res.get("adj_id")}'""")
    ev.check("transfer is blocked by the earlier Flatten",
             h and h[0]["BLOCKED_BY_ADJ_ID"] == flat.get("adj_id"))
```

Add to `tests/conftest.py` `_cleanup` `stmts` (headers on FAKE_COB are already deleted by `DELETE FROM ADJUSTMENT_APP.ADJ_HEADER WHERE COBID = {FAKE_COB}` — nothing extra needed; confirm and leave a comment `# Transfer Book headers (tests/test_transfer_book.py) are on FAKE_COB too.`).

- [ ] **Step 2: Note the expected failure (cannot run here)**

These run on the Windows/DVLP box: `pytest tests/test_transfer_book.py -q`. Before Task 2's SP change they fail with `Unknown adjustment_type: Transfer`. Record that expectation in the commit message; do not claim a run.

- [ ] **Step 3: Implement in `03_sp_submit_adjustment.sql`**

`ACTION_MAP`: add `"transfer":     "Scale",` after `"roll": "Scale",`.

`compute_scale_factor_adjusted`: add before `elif t in ("scale", "roll"):`
```python
    elif t == "transfer":
        return float(scale_factor)               # book swap: full factor (1.0), never sf-1
```

Guards: keep the factor-1 rejection as is (it checks `== "scale"` only). After the same-COB guard block (`if adjustment_type.lower() in ("scale", "flatten") ...`), add:

```python
        # ── Transfer Book: source book → target book at ONE COB ──────────
        # BOOK_CODE is the TARGET (the scope being replaced — like Roll's
        # filters describe the target COB); SOURCE_BOOK_CODE is where the
        # values come from. The entity is DERIVED from the target book so the
        # sign-off check, overlap check and every grid work unchanged.
        if adjustment_type.lower() == "transfer":
            src_book = str(adj.get("source_book_code") or "").strip()
            tgt_book = str(adj.get("book_code") or "").strip()
            if not src_book or not tgt_book:
                return {"adj_id": None, "status": "Error",
                        "message": "Transfer Book needs both a source book and a target book."}
            if src_book.upper() == tgt_book.upper():
                return {"adj_id": None, "status": "Error",
                        "message": "Source and target book must differ."}
            if source_cobid is not None and int(source_cobid) != int(cobid):
                return {"adj_id": None, "status": "Error",
                        "message": "Transfer Book applies within one COB: source_cobid must equal cobid."}
            def _book_entity(code):
                r = session.sql(f"""
                    SELECT MAX(ENTITY_CODE) AS E, COUNT(*) AS N FROM DIMENSION.BOOK
                    WHERE UPPER(BOOK_CODE) = UPPER('{_esc(code)}') AND IS_CURRENT_ROW = TRUE
                """).collect()
                return (r[0]["E"], int(r[0]["N"])) if r else (None, 0)
            src_ent, src_n = _book_entity(src_book)
            tgt_ent, tgt_n = _book_entity(tgt_book)
            if src_n == 0:
                return {"adj_id": None, "status": "Error",
                        "message": f"Source book '{src_book}' is not a current book in DIMENSION.BOOK."}
            if tgt_n == 0:
                return {"adj_id": None, "status": "Error",
                        "message": f"Target book '{tgt_book}' is not a current book in DIMENSION.BOOK."}
            adj["entity_code"] = tgt_ent
            adj["book_code"] = tgt_book
            adj["source_book_code"] = src_book
            for _k in ("department_code", "source_system_code", "currency_code", "trade_typology",
                       "strategy", "instrument_code", "simulation_name", "simulation_source",
                       "measure_type_code", "trader_code", "guaranteed_entity", "region_key",
                       "scenario_date_id", "tenor_code", "underlying_tenor_code", "curve_code",
                       "product_category_attributes", "var_component_name",
                       "var_sub_component_name", "day_type"):
                adj.pop(_k, None)          # a transfer carries no other filters
            source_cobid = cobid
```

`col_map`: add `"SOURCE_BOOK_CODE": adj.get("source_book_code"),` after `"BOOK_CODE": adj.get("book_code"),`.

Also update the docstring list of allowed types (l.~244) to include `Transfer`.

- [ ] **Step 4: Sanity-check the Python inside the SP**

Extract the Python body and compile it:
```bash
python3 - <<'EOF'
import re
s = open("new_adjustment_db_objects/03_sp_submit_adjustment.sql").read()
body = s.split("$$")[1]
compile(body, "03_body", "exec"); print("ok")
EOF
```
Expected: `ok`.

- [ ] **Step 5: Commit**

```bash
git add new_adjustment_db_objects/03_sp_submit_adjustment.sql tests/test_transfer_book.py tests/conftest.py
git commit -m "feat(transfer): SP_SUBMIT accepts Transfer — guards, target-book entity derivation, SOURCE_BOOK_CODE (UAT TRF-01/02/04, run on DVLP)

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 3: Engine — leg ②T in the Scale path

**Files:**
- Modify: `new_adjustment_db_objects/05_sp_process_adjustment.sql` — `has_cross_cob` block (l.1348–1350), roll-leg block (l.1723–1770), UNION assembly (l.1786–1803), SCD2 comment (l.~1920).

**Interfaces:**
- Consumes header columns `SOURCE_BOOK_CODE`, `BOOK_CODE`, `TRADE_CODE`, `SCALE_FACTOR_ADJUSTED`; variables in scope at that point of the file: `fact_tbl_name`, `fact_adjusted_tbl_name`, `adj_base_tbl_name`, `adj_ids_str`, `cobid`, `fact_non_metric_matched`, `select_measure`, `select_non_metric`, `select_scale`, `_view_cols` (defined inside the roll block — hoist it, see Step 2), `_adj_default`, `_erlog`, `_sqlog`.

- [ ] **Step 1: Batch flag**

After `has_cross_cob = ...count() > 0` add:
```python
            # Transfer Book rows (SOURCE_BOOK_CODE set): same COB, book swapped.
            # They use the adjusted view like a cross-COB Roll (leg ②T) and the
            # flatten leg ③ on the TARGET book; never leg ①.
            has_transfer = df_adj_scale.filter(
                col('SOURCE_BOOK_CODE').isNotNull()
            ).count() > 0
```

- [ ] **Step 2: Hoist the adjusted-view column discovery and build leg ②T**

Replace the roll-leg block (from `roll_leg = ""` down to the closing `{join_cond}"""` of `roll_leg`) with:

```python
            roll_leg = ""
            transfer_leg = ""
            needs_adjusted_view = has_cross_cob or has_transfer
            if needs_adjusted_view and (not fact_adjusted_tbl_name
                                        or fact_adjusted_tbl_name == fact_tbl_name):
                raise Exception(
                    f"Cross-COB Roll / Transfer Book for {process_type} requires "
                    f"FACT_ADJUSTED_TABLE to be configured in ADJUSTMENTS_SETTINGS "
                    f"(distinct from FACT_TABLE). Refusing to process — without the "
                    f"adjusted view the target would be flattened instead of replaced.")
            _view_cols = set()
            if needs_adjusted_view:
                try:
                    _view_cols = set(session.table(fact_adjusted_tbl_name).columns)
                except Exception:
                    _view_cols = set()

            def _adj_default(c):
                return "-1" if c.split('_')[-1].upper() in ('KEY', 'ID') else "NULL"

            if has_cross_cob:
                select_non_metric_adj = ', '.join(
                    (f"fact.{c}" if c in _view_cols else f"{_adj_default(c)} AS {c}")
                    for c in fact_non_metric_matched
                )
                select_scale_adj = (
                    select_scale.replace(select_non_metric, select_non_metric_adj, 1)
                    if select_non_metric else select_scale
                )
                roll_leg = f"""
                UNION ALL
                -- ② Roll cross-COB: source COB's ADJUSTED value from FACT_ADJUSTED_TABLE
                --    (columns the combined view lacks default to -1 / NULL)
                {select_scale_adj} {from_where_adj}
                AND fact.COBID = adjust.SOURCE_COBID
                AND adjust.COBID <> adjust.SOURCE_COBID
                AND adjust.SOURCE_BOOK_CODE IS NULL
                {join_cond}"""

            if has_transfer:
                # ②T Transfer Book — the SOURCE book's adjusted rows at the COB,
                # RE-KEYED to the target book INSIDE the select: the surrogate
                # key (fact_key) is built from these columns, so leg ③'s
                # flattened target rows and these rows share a key per position
                # and `netted` cancels them → combined(target) = adjusted(source).
                # Only the source book + optional trade code filter the source
                # (the header's ENTITY/BOOK/DEPT describe the TARGET).
                # TRADE_KEY: the same trade code's version under the target
                # book at the COB (tt), else the target book's
                # '<BOOK>/Adjustment' trade (ta), else the source key.
                def _transfer_col(c):
                    cu = c.upper()
                    if cu == "BOOK_KEY":        return "tb.BOOK_KEY AS BOOK_KEY"
                    if cu == "BOOK_CODE":       return "adjust.BOOK_CODE AS BOOK_CODE"
                    if cu == "DEPARTMENT_CODE": return "tb.DEPARTMENT_CODE AS DEPARTMENT_CODE"
                    if cu == "ENTITY_KEY":      return "te.ENTITY_KEY AS ENTITY_KEY"
                    if cu == "ENTITY_CODE":     return "tb.ENTITY_CODE AS ENTITY_CODE"
                    if cu == "TRADE_KEY":
                        return "COALESCE(tt.TRADE_KEY, ta.TRADE_KEY, fact.TRADE_KEY) AS TRADE_KEY"
                    return f"fact.{c}" if c in _view_cols else f"{_adj_default(c)} AS {c}"
                select_non_metric_trf = ', '.join(_transfer_col(c) for c in fact_non_metric_matched)
                _cob_date = f"TO_DATE('{int(cobid)}', 'YYYYMMDD')"
                transfer_leg = f"""
                UNION ALL
                -- ②T Transfer Book: source book's ADJUSTED rows at the COB, re-keyed to the target book
                SELECT adjust.COBID, adjust.DIMENSION_ADJ_ID AS ADJUSTMENT_ID,
                       adjust.CREATED_DATE AS ADJUSTMENT_CREATED_TIMESTAMP,
                       {select_non_metric_trf}, {select_measure}
                FROM {fact_adjusted_tbl_name} fact
                INNER JOIN {adj_base_tbl_name} adjust
                    ON  adjust.COBID = {cobid}
                    AND adjust.ADJ_ID IN ({adj_ids_str})
                    AND adjust.IS_DELETED = FALSE
                    AND adjust.RUN_STATUS = 'Running'
                    AND adjust.SOURCE_BOOK_CODE IS NOT NULL
                    AND fact.COBID = adjust.SOURCE_COBID
                LEFT JOIN DIMENSION.BOOK tb
                    ON  UPPER(tb.BOOK_CODE) = UPPER(adjust.BOOK_CODE)
                    AND tb.IS_CURRENT_ROW = TRUE
                LEFT JOIN DIMENSION.ENTITY te
                    ON  UPPER(te.ENTITY_CODE) = UPPER(tb.ENTITY_CODE)
                LEFT JOIN DIMENSION.TRADE st
                    ON  st.TRADE_KEY = COALESCE(fact.TRADE_KEY, -1)
                LEFT JOIN DIMENSION.TRADE tt
                    ON  UPPER(tt.TRADE_CODE) = UPPER(st.TRADE_CODE)
                    AND UPPER(tt.BOOK_CODE)  = UPPER(adjust.BOOK_CODE)
                    AND {_cob_date} BETWEEN tt.EFFECTIVE_START_DATE AND tt.EFFECTIVE_END_DATE
                LEFT JOIN DIMENSION.TRADE ta
                    ON  UPPER(ta.TRADE_CODE) = UPPER(adjust.BOOK_CODE || '/Adjustment')
                    AND UPPER(ta.BOOK_CODE)  = UPPER(adjust.BOOK_CODE)
                    AND {_cob_date} BETWEEN ta.EFFECTIVE_START_DATE AND ta.EFFECTIVE_END_DATE
                WHERE fact.{metric_usd_name} IS NOT NULL
                  AND EXISTS (SELECT 1 FROM DIMENSION.BOOK sb
                              WHERE sb.BOOK_KEY = COALESCE(fact.BOOK_KEY, -1)
                                AND UPPER(sb.BOOK_CODE) = UPPER(adjust.SOURCE_BOOK_CODE))
                  AND (adjust.TRADE_CODE IS NULL
                       OR UPPER(st.TRADE_CODE) = UPPER(adjust.TRADE_CODE))"""
```

Notes for the implementer:
- `fact_non_metric_matched` may not contain some of the overridden columns for a scope (e.g. Stress has `ENTITY_KEY` but no `ENTITY_CODE`); `_transfer_col` only rewrites columns that are in the list, so the UNION column list stays aligned with legs ① and ③.
- `metric_usd_name` is the variable the file already uses in `from_where` (`AND {metric_usd_name} IS NOT NULL`). Use the same name.
- The tt/ta/st joins are on `DIMENSION.TRADE` keyed like the existing SCD2 remap (l.1869–1938: `TRADE_CODE` + `BOOK_CODE` + effective dates). If `DIMENSION.TRADE` lacks `EFFECTIVE_START_DATE`/`EFFECTIVE_END_DATE` in this environment the existing remap would already be broken; do not invent other column names.

- [ ] **Step 3: Leg ① and ③ conditions, UNION assembly**

In the `insert_sql` CTE:
- Leg ①: after `AND adjust.COBID = adjust.SOURCE_COBID` add a line `AND adjust.SOURCE_BOOK_CODE IS NULL`.
- Replace `{join_cond}{roll_leg}` with `{join_cond}{roll_leg}{transfer_leg}`.
- Leg ③: replace `AND adjust.COBID <> adjust.SOURCE_COBID` with `AND (adjust.COBID <> adjust.SOURCE_COBID OR adjust.SOURCE_BOOK_CODE IS NOT NULL)` and update its comment to `-- ③ Flatten current COB (offsets existing values at target COB for cross-COB roll, or in the TARGET book for a transfer)`.

Leg ③ keeps `{from_where}` + `{join_cond}`: for a transfer row the header's filters (target book, trade, target entity) select exactly the target book's rows to flatten.

- [ ] **Step 4: SCD2 remap comment; batch log note**

At `if has_cross_cob:` before `_erlog(session, _sqlog, "scd2_key_fix", scd2_update)` add the comment `# Transfer rows are re-keyed inside leg ②T and are same-COB, so adj_cte (COBID <> SOURCE_COBID) already excludes them.` Right after `_erlog(session, _sqlog, "stage_build (netted temp)", insert_sql)` add:
```python
            if has_transfer:
                _erlog(session, _sqlog, "transfer_leg (note)",
                       "-- batch contains Transfer Book rows: leg ②T re-keys source-book rows to the target book")
```
(`_erlog` with a comment-only statement — check its signature at l.~500: if it executes the SQL, a bare comment is a no-op statement in Snowflake and is fine; if it requires a result, skip this note.)

- [ ] **Step 5: Compile the SP body**

```bash
python3 - <<'EOF'
s = open("new_adjustment_db_objects/05_sp_process_adjustment.sql").read()
body = s.split("$$")[1]
compile(body, "05_body", "exec"); print("ok")
EOF
grep -n "has_transfer\|transfer_leg\|SOURCE_BOOK_CODE" new_adjustment_db_objects/05_sp_process_adjustment.sql | head -20
```
Expected: `ok`; the grep shows the flag, the leg, and the leg ①/③ conditions.

- [ ] **Step 6: Commit**

```bash
git add new_adjustment_db_objects/05_sp_process_adjustment.sql
git commit -m "feat(transfer): Scale path leg ②T — source book's adjusted rows re-keyed to the target book; legs ①/③ conditions

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 4: Preview procedure — transfer summary, per-trade breakdown, SQL mode

**Files:**
- Modify: `new_adjustment_db_objects/04_sp_preview_adjustment.sql` — after `is_roll = ...` (l.86), the factor block (l.120–127), and before the `if is_roll and not (...)` guard (l.~415).
- Modify: `tests/test_transfer_book.py` — add TRF-03.

- [ ] **Step 1: Add TRF-03 (Snowflake, cannot run here)**

```python
@pytest.mark.uat("TRF-03", title="Transfer preview SQL reads the source book and re-keys to the target", priority="P1")
def test_trf03_preview_sql(session, ev):
    from conftest import call_sp
    payload = json.dumps({"cobid": FAKE_COB, "process_type": "VaR", "adjustment_type": "Transfer",
                          "source_cobid": FAKE_COB, "scale_factor": 1,
                          "book_code": TGT, "source_book_code": SRC,
                          "trade_codes": ["UAT-TRADE-1", "UAT-TRADE-2"], "mode": "sql"})
    r = session.sql(f"CALL {SP_PREVIEW}('{payload}')").collect()
    txt = str(r[0][0]) if r else ""
    ev.note("SQL", txt[:600])
    ev.check("source book predicate", f"UPPER(sb.BOOK_CODE) = UPPER('{SRC}')".upper() in txt.upper())
    ev.check("target book predicate", TGT.upper() in txt.upper())
    ev.check("trade list", "UAT-TRADE-1" in txt and "UAT-TRADE-2" in txt)
    ev.check("fallback trade", "/Adjustment" in txt)
```
(`SP_PREVIEW_ADJUSTMENT` returns a TABLE; `session.sql("CALL ...")` yields its rows — the first column of the first row is `PREVIEW_SQL` in `mode='sql'`.)

- [ ] **Step 2: Parse transfer inputs**

After `is_roll = (...)` add:
```python
    is_transfer = (adjustment_type == "transfer")
    src_book = str(adj.get("source_book_code") or "").strip()
    tgt_book = str(adj.get("book_code") or "").strip()
    trade_codes = adj.get("trade_codes") or ([adj["trade_code"]] if adj.get("trade_code") else [])
    trade_codes = [str(t).strip() for t in trade_codes if str(t).strip()]
    if is_transfer and (not src_book or not tgt_book or src_book.upper() == tgt_book.upper()):
        return session.sql("SELECT 'Error: Transfer Book preview needs a source book and a "
                           "different target book' AS MESSAGE")
```
In the factor block add `elif adjustment_type == "transfer": sf_adjusted = scale_factor` before the `else:` (Direct/Upload) branch. Also make the same-COB guard not fire for transfer (it checks `("scale", "flatten")` only — no change needed; confirm).

For the TARGET predicates the normal builder must see `book_code` = target and the trade list: when `is_transfer`, set `adj["trade_code"] = None` before the WHERE builder runs and instead append, after the builder (before `where_sql = ...`), if `trade_codes`:
```python
    if is_transfer and trade_codes and has_trade_key:
        _tl = ", ".join(f"'{_esc(t)}'" for t in trade_codes)
        where_clauses.append(f"EXISTS (SELECT 1 FROM DIMENSION.TRADE td "
                             f"WHERE td.TRADE_KEY = fact.TRADE_KEY AND UPPER(td.TRADE_CODE) IN ({_tl.upper()}))")
```
(Read the builder's existing TRADE EXISTS first — l.~215–240 — and match its alias/shape.)

- [ ] **Step 3: Transfer branch (summary / breakdown / sql)**

Insert immediately before `if is_roll and not (fact_adj_tbl and fact_adj_tbl != fact_tbl):`:

```python
    if is_transfer and not (fact_adj_tbl and fact_adj_tbl != fact_tbl):
        return session.sql(
            "SELECT 'Error: Transfer Book preview needs FACT_ADJUSTED_TABLE configured "
            "for this scope (ADJUSTMENTS_SETTINGS)' AS MESSAGE")

    if is_transfer:
        dim_filters = where_clauses[1:]          # target: book/trade predicates
        tgt_dim_sql = ("\n      AND " + "\n      AND ".join(dim_filters)) if dim_filters else ""
        src_preds = [f"EXISTS (SELECT 1 FROM DIMENSION.BOOK sb WHERE sb.BOOK_KEY = fact.BOOK_KEY "
                     f"AND UPPER(sb.BOOK_CODE) = UPPER('{_esc(src_book)}'))"]
        if trade_codes and has_trade_key:
            _tl = ", ".join(f"'{_esc(t).upper()}'" for t in trade_codes)
            src_preds.append(f"EXISTS (SELECT 1 FROM DIMENSION.TRADE st WHERE st.TRADE_KEY = fact.TRADE_KEY "
                             f"AND UPPER(st.TRADE_CODE) IN ({_tl}))")
        src_where = (f"WHERE fact.COBID = {int(cobid)}\n      AND " + "\n      AND ".join(src_preds)
                     + f"\n      AND fact.{primary_metric} IS NOT NULL")
        tgt_where = f"WHERE fact.COBID = {int(cobid)}{tgt_dim_sql}\n      AND fact.{primary_metric} IS NOT NULL"
        _cob_date = f"TO_DATE('{int(cobid)}', 'YYYYMMDD')"

        transfer_summary = f"""
        WITH src_adj AS (
            SELECT COUNT(*)                                 AS ROWS_AFFECTED,
                   COUNT_IF(fact.{primary_metric} != 0)     AS NONZERO_ROWS,
                   COALESCE(SUM(fact.{primary_metric}), 0)  AS SOURCE_ADJUSTED_VALUE
            FROM {fact_adj_tbl} fact
            {src_where}
        ),
        src_orig AS (
            SELECT COALESCE(SUM(fact.{primary_metric}), 0)  AS SOURCE_ORIGINAL_VALUE
            FROM {fact_tbl} fact
            {src_where}
        ),
        tgt AS (
            SELECT COALESCE(SUM(fact.{primary_metric}), 0)  AS TOTAL_CURRENT_VALUE
            FROM {fact_tbl} fact
            {tgt_where}
        )
        SELECT
            src_adj.ROWS_AFFECTED,
            src_adj.NONZERO_ROWS,
            src_orig.SOURCE_ORIGINAL_VALUE,
            src_adj.SOURCE_ADJUSTED_VALUE - src_orig.SOURCE_ORIGINAL_VALUE      AS SOURCE_ADJUSTMENTS_VALUE,
            src_adj.SOURCE_ADJUSTED_VALUE,
            tgt.TOTAL_CURRENT_VALUE,
            {scale_factor} * src_adj.SOURCE_ADJUSTED_VALUE - tgt.TOTAL_CURRENT_VALUE AS TOTAL_ADJUSTMENT_DELTA,
            {scale_factor} * src_adj.SOURCE_ADJUSTED_VALUE                           AS TOTAL_PROJECTED_VALUE,
            {overlap_cols}
        FROM src_adj, src_orig, tgt
        """
        transfer_breakdown = f"""
        SELECT st.TRADE_CODE                                            AS TRADE_CODE,
               MAX(CASE WHEN tt.TRADE_KEY IS NOT NULL THEN 'target trade found'
                        ELSE 'fallback: {_esc(tgt_book)}/Adjustment' END) AS TARGET_TRADE,
               COUNT(*)                                                 AS ROWS_AFFECTED,
               COALESCE(SUM(fact.{primary_metric}), 0)                  AS PROJECTED_VALUE
        FROM {fact_adj_tbl} fact
        LEFT JOIN DIMENSION.TRADE st ON st.TRADE_KEY = fact.TRADE_KEY
        LEFT JOIN DIMENSION.TRADE tt
               ON  UPPER(tt.TRADE_CODE) = UPPER(st.TRADE_CODE)
               AND UPPER(tt.BOOK_CODE)  = UPPER('{_esc(tgt_book)}')
               AND {_cob_date} BETWEEN tt.EFFECTIVE_START_DATE AND tt.EFFECTIVE_END_DATE
        {src_where}
        GROUP BY 1
        ORDER BY 1
        """
        if mode == "sql":
            return _as_sql_row(_with_overlap_sql(transfer_summary)
                               + "\n\n-- Per-trade breakdown\n" + transfer_breakdown)
        if mode == "breakdown":
            return session.sql(transfer_breakdown)
        return session.sql(transfer_summary)
```

`_as_sql_row`, `_with_overlap_sql`, `overlap_cols`, `has_trade_key`, `primary_metric`, `_esc` all exist above this point in the file. `mode == "sample"` for a transfer falls through to the generic sample (target rows) — acceptable.

- [ ] **Step 4: Compile the SP body**

```bash
python3 - <<'EOF'
s = open("new_adjustment_db_objects/04_sp_preview_adjustment.sql").read()
body = s.split("$$")[1]
compile(body, "04_body", "exec"); print("ok")
EOF
```
Expected: `ok`.

- [ ] **Step 5: Commit**

```bash
git add new_adjustment_db_objects/04_sp_preview_adjustment.sql tests/test_transfer_book.py
git commit -m "feat(transfer): preview — source-book summary with split, per-trade breakdown with fallback flag, sql mode (UAT TRF-03)

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 5: Page — Transfer Book type, Source/Target form, per-trade fan-out, ticket

**Files:**
- Modify: `streamlit_app/utils/styles.py` — `TYPE_CONFIG` (l.291)
- Modify: `streamlit_app/pages/1_New_Adjustment.py` — `TYPE_BTN_ICONS` (l.760), `_WIZ_DEFAULTS`, `render_scaling_form` (type pill branch + section 5), `_render_schedule_fields`, `_build_payload`, `_do_submit`, `_preview_payload`, `_completion_checks`, `_ticket_html`, preview-detail block (l.~3683)
- Create: `streamlit_app/tests/test_transfer_form.py`

**Interfaces:**
- Consumes (Part A): `_selected_scopes()`, `_submit_fanout(payload, scopes)`, `_submit_one`, `_is_submit_success`, `_book_dept_rows()`, `_ref_rows()`, `_code_select`, `_card`, `_sec`, `_k`, `wiz`.
- Produces: `wiz["source_book_code"]`, `wiz["target_book_code"]`, `wiz["transfer_trade_codes"]`; pure helper `transfer_jobs(scopes, trade_codes) -> list[tuple[scope, trade_or_None]]` in `streamlit_app/utils/transfer_book.py`; `book_entity(rows, code) -> str|None` there too.

- [ ] **Step 1: Pure helpers + unit tests first**

```python
# streamlit_app/utils/transfer_book.py
"""Pure helpers for the Transfer Book form (no Streamlit)."""

def transfer_jobs(scopes, trade_codes):
    """One submission per scope per trade code; whole book when no trade."""
    scopes = [s for s in (scopes or []) if s]
    trades = [t.strip() for t in (trade_codes or []) if t and t.strip()]
    if not trades:
        return [(s, None) for s in scopes]
    return [(s, t) for s in scopes for t in trades]


def book_entity(book_rows, code):
    """ENTITY_CODE of a book from DIMENSION.BOOK rows shaped
    (BOOK_CODE, DEPARTMENT_CODE, ENTITY_CODE); None when unknown."""
    code = (code or "").strip().upper()
    for r in book_rows or []:
        if r[0] is not None and str(r[0]).strip().upper() == code:
            return str(r[2]) if r[2] is not None else None
    return None
```

```python
# streamlit_app/tests/test_transfer_form.py
from utils.transfer_book import transfer_jobs, book_entity

def test_jobs_whole_book_per_scope():
    assert transfer_jobs(["VaR", "Stress"], []) == [("VaR", None), ("Stress", None)]

def test_jobs_per_scope_per_trade():
    assert transfer_jobs(["VaR"], ["T1", " T2 "]) == [("VaR", "T1"), ("VaR", "T2")]
    assert len(transfer_jobs(["VaR", "FRTB"], ["T1", "T2", "T3"])) == 6

def test_book_entity():
    rows = [("B1", "D1", "MUSI"), ("B2", "D1", None)]
    assert book_entity(rows, "b1") == "MUSI"
    assert book_entity(rows, "B2") is None
    assert book_entity(rows, "ZZ") is None
```

Run: `$VENV -m pytest streamlit_app/tests -q` → the two new files fail with ImportError, then pass after creating the module. Expected after: all pass.

- [ ] **Step 2: Type config and icon**

`utils/styles.py` `TYPE_CONFIG` add:
```python
    "Transfer": {"icon": "shuffle", "desc": "Replace a target book with a source book's values",
                 "formula": "target book = adjusted(source book) at the COB"},
```
Also add a label map used by grids: in `styles.py` near `TYPE_CONFIG` add `TYPE_LABELS = {"Transfer": "Transfer Book"}` and a helper
```python
def type_label(code):
    return TYPE_LABELS.get(str(code or ""), str(code or ""))
```
Page `TYPE_BTN_ICONS` add `"Transfer": ":material/swap_horiz:",`. In `_pill_row(list(TYPE_CONFIG.keys()), ...)` inside `render_scaling_form` pass `fmt=type_label` (import it from `utils.styles`).

- [ ] **Step 3: Wizard state**

`_WIZ_DEFAULTS` add under `"scale_factor": 1.0,`:
```python
    "source_book_code":       None,   # Transfer Book
    "target_book_code":       None,
    "transfer_trade_codes":   [],
```
In the type-pill branch of `render_scaling_form`, after `if tsel != "Roll": wiz["source_cobid"] = None` add:
```python
            if tsel != "Transfer":
                wiz["source_book_code"] = None
                wiz["target_book_code"] = None
                wiz["transfer_trade_codes"] = []
            else:
                wiz["occurrence"] = "ADHOC"
```

- [ ] **Step 4: Schedule + form section for Transfer**

In `render_scaling_form`, the Date & Schedule card: when `wiz.get("adjustment_type") == "Transfer"` do not render the ADHOC/RECURRING pill row (only `_render_schedule_fields()`), and in `_render_schedule_fields` the factor column condition `in ("Scale", "Roll")` stays (no factor for Transfer).

Replace the Dimension Filters card with:
```python
    if wiz.get("adjustment_type") == "Transfer":
        with _card():
            _sec(5, "Transfer Details",
                 "The target book's positions at this COB are replaced by the source "
                 "book's values. Choose trade codes to transfer only those trades — "
                 "one adjustment is created per trade (and per scope).")
            _render_transfer_fields()
    else:
        with _card():
            _sec(5, "Dimension Filters", ...unchanged...)
            _render_main_filters()
            _render_extra_filters()
```
and add above `render_scaling_form`:

```python
def _book_trade_options(book_code):
    code = (book_code or "").strip().replace("\\", "\\\\").replace("'", "''")
    if not code:
        return []
    rows = _ref_rows(
        f"SELECT DISTINCT TRADE_CODE FROM DIMENSION.TRADE "
        f"WHERE UPPER(BOOK_CODE) = UPPER('{code}') AND IS_CURRENT_ROW = TRUE "
        f"AND TRADE_CODE IS NOT NULL AND TRADE_CODE NOT ILIKE '%/Adjustment' "
        f"ORDER BY TRADE_CODE", f"_ref_trades_{code.upper()}")
    return [str(r[0]) for r in rows if r[0] is not None]


def _render_transfer_fields() -> None:
    books = _book_options(None, None)                    # every current book
    rows_ = _book_dept_rows()
    s_col, t_col = st.columns(2)
    with s_col:
        st.markdown("**Source** — values come from")
        wiz["source_book_code"] = _code_select(
            "Source Book Code *", _k("trf_src_book"), wiz.get("source_book_code"),
            books, placeholder="— select book —")
        src_ent = book_entity(rows_, wiz.get("source_book_code"))
        if wiz.get("source_book_code"):
            st.caption(f"Entity: {src_ent or '—'}")
        opts = _book_trade_options(wiz.get("source_book_code"))
        picked = st.multiselect(
            "Trade Codes (optional — blank = whole book)", opts,
            default=[t for t in (wiz.get("transfer_trade_codes") or []) if t in opts],
            key=_k(f"trf_trades_{(wiz.get('source_book_code') or '').upper()}"),
            help="One adjustment is created per selected trade.")
        wiz["transfer_trade_codes"] = list(picked)
        if wiz.get("source_book_code"):
            st.caption(f"{len(opts):,} trades in this book")
    with t_col:
        st.markdown("**Target** — book that receives the values")
        tgt_opts = [b for b in books if b != (wiz.get("source_book_code") or "")]
        wiz["target_book_code"] = _code_select(
            "Target Book Code *", _k("trf_tgt_book"), wiz.get("target_book_code"),
            tgt_opts, placeholder="— select book —")
        tgt_ent = book_entity(rows_, wiz.get("target_book_code"))
        if wiz.get("target_book_code"):
            st.caption(f"Entity: {tgt_ent or '—'}")
            if src_ent and tgt_ent and src_ent != tgt_ent:
                st.info(f"Rows will be reported under entity {tgt_ent}.")
    # Keep the generic filter keys consistent with the ticket / sign-off gate.
    wiz["book_code"] = wiz.get("target_book_code")
    wiz["entity_code"] = tgt_ent
    n_jobs = len(transfer_jobs(_selected_scopes(), wiz.get("transfer_trade_codes")))
    if n_jobs > 1:
        st.caption(f"**{n_jobs} adjustments** will be created "
                   f"({len(_selected_scopes())} scope(s) × "
                   f"{max(1, len(wiz.get('transfer_trade_codes') or []))} trade(s)).")
```
Import at the top of the page: `from utils.transfer_book import transfer_jobs, book_entity`.

- [ ] **Step 5: Payload, preview payload, submit fan-out**

`_build_payload` Scaling branch: after the `payload = {...}` literal add:
```python
    if wiz.get("adjustment_type") == "Transfer":
        payload.update({
            "source_cobid":     wiz["cobid"],
            "scale_factor":     1,
            "book_code":        wiz.get("target_book_code"),
            "source_book_code": wiz.get("source_book_code"),
            "adjustment_occurrence": "ADHOC",
        })
        payload["adjustment_category"] = wiz.get("adjustment_category")
        if wiz.get("global_reference"):
            payload["global_reference"] = wiz.get("global_reference")
        return payload            # no other filter keys for a transfer
```

`_preview_payload`: after building `pj`, add:
```python
    if wiz.get("adjustment_type") == "Transfer":
        pj = {"cobid": wiz["cobid"], "process_type": wiz["process_type"],
              "adjustment_type": "Transfer", "source_cobid": wiz["cobid"], "scale_factor": 1,
              "book_code": wiz.get("target_book_code"),
              "source_book_code": wiz.get("source_book_code"),
              "trade_codes": list(wiz.get("transfer_trade_codes") or [])}
    return pj
```

`_do_submit`: replace the Part A line `if wiz.get("category") in (...) and len(scopes) > 1: return _submit_fanout(payload, scopes)` with:
```python
        scopes = _selected_scopes()
        if wiz.get("adjustment_type") == "Transfer":
            jobs = transfer_jobs(scopes, wiz.get("transfer_trade_codes"))
            if len(jobs) > 1:
                return _submit_jobs(payload, jobs)
            if jobs and jobs[0][1]:
                payload["trade_code"] = jobs[0][1]
        elif wiz.get("category") in ("Scaling Adjustment", "Entity Roll") and len(scopes) > 1:
            return _submit_fanout(payload, scopes)
```
and add next to `_submit_fanout`:
```python
def _submit_jobs(payload: dict, jobs: list) -> dict:
    """Transfer Book: one call per (scope, trade). Partial failures are named."""
    created, failures, statuses = [], [], []
    for sc, trade in jobs:
        p = {**payload, "process_type": sc}
        if trade:
            p["trade_code"] = trade
        res = _submit_one(p)
        label = f"{scope_label(sc)}{' / ' + trade if trade else ''}"
        if _is_submit_success(res):
            created.append(label); statuses.append(res.get("status"))
        else:
            failures.append(f"{label}: {res.get('message', 'not accepted')}")
    if not failures:
        return {"status": statuses[0],
                "message": f"Created {len(created)} Transfer Book adjustments ({', '.join(created)})."}
    partial = (f" Already created: {', '.join(created)} — delete them from the Adjustments "
               f"page if they are no longer wanted." if created else "")
    return {"status": "Error", "message": "Not every transfer was accepted. " + " | ".join(failures) + partial}
```

- [ ] **Step 6: Checklist and ticket**

`_completion_checks` Scaling branch: when `wiz.get("adjustment_type") == "Transfer"` replace the Entity/narrow-label pair with:
```python
        if wiz.get("adjustment_type") == "Transfer":
            checks += [
                ("Source book", bool(wiz.get("source_book_code"))),
                ("Target book", bool(wiz.get("target_book_code"))),
                ("Target differs from source",
                 (wiz.get("source_book_code") or "") != (wiz.get("target_book_code") or "")
                 if wiz.get("source_book_code") and wiz.get("target_book_code") else True),
            ]
        else:
            checks += [("Entity code", ...), (_narrow_label, ...)]   # existing pair
```
`_ticket_html` Scaling branch: when Transfer, replace the `Filters` row with
```python
            kv += _ticket_row("Transfer",
                              f'{wiz.get("source_book_code")} → {wiz.get("target_book_code")}'
                              if wiz.get("source_book_code") and wiz.get("target_book_code") else None)
            _tc = wiz.get("transfer_trade_codes") or []
            kv += _ticket_row("Trades", f"{len(_tc)} selected" if _tc else "Whole book", True)
```
and skip the applied-filter chips for Transfer.

Preview-detail block (l.~3683): for Transfer show the per-trade breakdown expander (`mode: "breakdown"`) with the columns renamed `TRADE_CODE→Trade, TARGET_TRADE→Target trade, PROJECTED_VALUE→Projected`, and the caption "Transfer preview: **current** = the target book's total in scope (flattened), **projected** = the source book's adjusted total."

- [ ] **Step 7: Run local tests and compile**

```bash
python3 -m py_compile streamlit_app/pages/1_New_Adjustment.py streamlit_app/utils/styles.py
$VENV -m pytest streamlit_app/tests -q
```
Expected: compiles; all tests pass (Part A's page smoke tests included).

- [ ] **Step 8: Commit**

```bash
git add streamlit_app/utils/transfer_book.py streamlit_app/utils/styles.py streamlit_app/pages/1_New_Adjustment.py streamlit_app/tests/test_transfer_form.py
git commit -m "feat(transfer): Transfer Book type in the Scaling form — source/target books, optional trades, one adjustment per scope × trade

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 6: Display and docs — "From book" everywhere a ticket is shown

**Files:**
- Modify: `streamlit_app/utils/styles.py` — `render_filter_chips` `dim_labels` (l.1654)
- Modify: `streamlit_app/pages/2_Adjustments.py:626`, `streamlit_app/pages/3_Approval_Queue.py:309`
- Modify: `streamlit_app/pages/7_Documentation.py` — `_KNOWLEDGE`

- [ ] **Step 1: Chips and detail rows**

`dim_labels`: add `"SOURCE_BOOK_CODE": "From book",` right after `"BOOK_CODE": "Book",`.

`2_Adjustments.py` after the `("Source COB", ...)` tuple add:
```python
                ("From book",    str(row.get("SOURCE_BOOK_CODE")) if row.get("SOURCE_BOOK_CODE") else "—"),
```
`3_Approval_Queue.py` after `("Source COB", _txt(row.get("SOURCE_COBID"))),` add `("From book", _txt(row.get("SOURCE_BOOK_CODE"))),`.

Wherever these pages print the adjustment type raw (grep `ADJUSTMENT_TYPE` in both files and in `utils/styles.py` `build_activity_grid_df`), wrap with `type_label(...)` from Task 5 so `Transfer` reads "Transfer Book".

- [ ] **Step 2: Documentation knowledge**

In `7_Documentation.py` `_KNOWLEDGE`, Scaling bullet, add:
```
  Transfer Book (Scaling type): at one COB the TARGET book's positions are
  replaced by the SOURCE book's adjusted values (source untouched); optional
  trade codes limit it, one adjustment per trade and per scope. The ticket's
  entity is the target book's entity. Trades with no version under the
  target book land on the target's '<BOOK>/Adjustment' trade.
```

- [ ] **Step 3: Compile and commit**

```bash
python3 -m py_compile streamlit_app/pages/2_Adjustments.py streamlit_app/pages/3_Approval_Queue.py streamlit_app/pages/7_Documentation.py streamlit_app/utils/styles.py
git add streamlit_app/utils/styles.py streamlit_app/pages/2_Adjustments.py streamlit_app/pages/3_Approval_Queue.py streamlit_app/pages/7_Documentation.py
git commit -m "feat(transfer): 'From book' in chips and detail panels; Transfer Book label; docs

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

## Deploy (Marcos)

**Order matters.** `01_tables.sql` must be deployed before `05` — the Scale
path references `ADJ_HEADER.SOURCE_BOOK_CODE` on **every** run, transfer or
not, so a 05 deployed against the old table breaks Scale, Flatten and Roll
too. `deploy_all.ps1` does this automatically (files run in name order);
never hand-deploy 05 alone.

**FRTB scopes:** transferred rows get a NEW `FRTBSA_*_KEY` (MD5 of source
key + target book + resolved trade), like Direct FRTB rows; DVLP check: no
duplicate (COBID, `FRTBSA_*_KEY`) in the FRTB `_ADJUSTMENT` table after a
transfer.

1. Push; on the Windows box `.\deploy_all.ps1` (auto picks DB + Streamlit).
2. Run `pytest tests/test_transfer_book.py -q` with `TEST_TRF_SRC_BOOK` / `TEST_TRF_TGT_BOOK` set to two real current books.
3. Submit one small Transfer (one trade) in DVLP, let the task process it, and read `ADJUSTMENT_APP.VW_EROL_PROCESS_LOG` for the run's `stage_build (netted temp)` SQL to confirm leg ②T.

### DVLP checks after that first processed Transfer

Run these against a **populated** target book (one that actually has rows at
the COB) — all three must hold:

1. **Leg ③ produced rows.** The flatten of the target book is not empty:
   ```sql
   SELECT COUNT(*) FROM <scope>_ADJUSTMENT
   WHERE COBID = <cob> AND ADJUSTMENT_ID = '<dimension_adj_id>'
     AND <metric> < 0;          -- leg ③'s negated originals
   ```
   0 here means the target predicates (entity + book + trade) matched
   nothing — the transfer then just adds the source on top instead of
   replacing.
2. **Leg ②T row count = the preview's `ROWS_AFFECTED`.** The preview counts
   the source rows it will carry over; the engine must have written the same
   number (per adjustment):
   ```sql
   SELECT COUNT(*) FROM <scope>_ADJUSTMENT
   WHERE COBID = <cob> AND ADJUSTMENT_ID = '<dimension_adj_id>'
     AND <metric> > 0;
   ```
   A count that is a clean multiple of the preview's is the SCD2 fan-out
   signature (a duplicate BOOK / ENTITY / TRADE row defeating a dedup).
3. **Target book's combined total = the preview's projected value.** The
   whole point of the feature:
   ```sql
   SELECT SUM(<metric>) FROM <combined view>
   WHERE COBID = <cob> AND BOOK_KEY = (target book's key);
   ```
   must equal `TOTAL_PROJECTED_VALUE` from the preview summary (= the source
   book's adjusted total). A difference of exactly the target's original
   total means legs ②T and ③ did not net — check the surrogate key columns.

## Self-review (done while writing)

- Spec §4 form → Task 5. §5.1 column → Task 1. §5.2 payload → Task 5 Step 5. §5.3 submit → Task 2. §5.4 display → Task 6. §6.1–6.4 engine → Task 3. §6.5 preview → Task 4. §7 lifecycle → no change needed (Scale action). §8 tests → Tasks 2, 4, 5 (processing test deliberately replaced by the run log, flagged in Global Constraints).
- Names: `transfer_jobs`, `book_entity` (Task 5 Step 1) used in Step 4/5; `_submit_jobs` defined and used in Task 5; `type_label` defined in Task 5 Step 2, used in Task 6; `SOURCE_BOOK_CODE` spelled identically in Tasks 1–6.
