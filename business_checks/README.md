# Business Number Checks — DVLP vs PROD tie-out

> **Easier option:** the app's **Validation** page does this automatically —
> enter the **two adjustment IDs** (dev + prod) and it shows the header diff plus
> `DEV` / `PROD` / `DIFF` with ✅/❌ per dimension. No Excel. Use these SQL files
> only when you need to query the DB directly (e.g. single-DB access).

## How to use

1. Connect to one database; set its context at the top of the file
   (`USE DATABASE DVLP_RAPTOR_NEWADJ;` **or** `USE DATABASE PROD_RAPTOR;`).
2. Set the two parameters:
   - `ADJ_ID` — the adjustment number (`DIMENSION.ADJUSTMENT.ADJUSTMENT_ID`, **not**
     the long hash). ⚠️ Each DB has its **own** id for the same adjustment.
   - `COB` — the COB date (YYYYMMDD).
3. Run a query, export to Excel. Repeat in the other DB and compare.
   The one that should match is **FINAL** (`*_COMBINED` / `*_ADJUSTED`).

## One query for everything

`00_ALL_scopes_dev_vs_prod.sql` (needs both DBs in the same session) returns
**every processed app adjustment** with its delta total + row count in **dev and
prod** — one row per source. It reads the prod id from the **Reason** field (put
the prod adjustment id there). Export and pivot: rows = `COBID / ENTITY /
PROCESS_TYPE / PROD_ADJ_ID`, columns = `SOURCE`, values = `TOTAL_ADJUSTMENT`.

## What each per-scope file gives you

One file per scope (`01_VaR_check.sql` … `06_FRTB_RRAO_check.sql`), each with a
ready query per table:

| Table | What it is | Keyed by |
|-------|-----------|----------|
| `DIMENSION.ADJUSTMENT` | the adjustment record (who / what / when) | `ADJUSTMENT_ID` |
| `*_MEASURES_ADJUSTMENT` | the **delta** the adjustment wrote | `ADJUSTMENT_ID` |
| `*_MEASURES` | **original** values, before any adjustment | `COBID` |
| `*_MEASURES_COMBINED` / `*_ADJUSTED` | **final** values = original + adjustments (what reports show) | `COBID` |
| `*_MEASURES_ADJUSTMENT_SUMMARY` | per-COB summary of the delta (VaR / Stress / Sensitivity only) | `COBID` |

Tip: to narrow an original/final query to one book, add
`AND BOOK_KEY IN (SELECT BOOK_KEY FROM DIMENSION.BOOK WHERE BOOK_CODE = 'YOUR-BOOK')`.
