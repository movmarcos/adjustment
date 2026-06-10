# Business Number Checks — DVLP vs PROD tie-out

> **Easier option:** the app's **Validation** page (`9_Validation.py`) does the
> dev‑vs‑prod comparison automatically — enter the **two adjustment IDs** (the
> dev one and the prod one) and it shows the header diff plus `DEV`, `PROD`,
> `DIFF` and a ✅/❌ flag per dimension, no Excel needed. It requires a session
> that can read **both** databases. These per‑DB SQL files remain as a fallback
> for users who only have single‑DB access.

---


These queries let the business **check an adjustment's numbers** by pulling the
same information from both databases and comparing the two exports in Excel:

- `DVLP_RAPTOR_NEWADJ` — the new adjustment engine
- `PROD_RAPTOR` — production

Because role restrictions usually mean you can only reach **one** database per
connection, the workflow is: run the file in one DB, export to Excel, then run
the same file in the other DB, export, and compare side by side.

## The adjustment ID

You supply the **numeric `ADJUSTMENT_ID`** — the one stored in
`DIMENSION.ADJUSTMENT` (it starts at 100000 and counts up). This is **not** the
long hash / UUID (`ADJ_ID`) used internally by the app.

> ⚠️ The two databases assign their **own** `ADJUSTMENT_ID`s, so the same
> adjustment usually has a **different** number in DVLP and in PROD. Set the
> correct id for whichever database you are connected to before you run.

## One file per scope

| Scope            | File                              | Adjustment table                          |
|------------------|-----------------------------------|-------------------------------------------|
| VaR              | `01_VaR_check.sql`                | `FACT.VAR_MEASURES_ADJUSTMENT`            |
| Stress           | `02_Stress_check.sql`             | `FACT.STRESS_MEASURES_ADJUSTMENT`         |
| Sensitivity      | `03_Sensitivity_check.sql`        | `FACT.SENSITIVITY_MEASURES_ADJUSTMENT`    |
| FRTB Sensitivity | `04_FRTB_Sensitivity_check.sql`   | `FACT.FRTBSA_SENSITIVITY_MEASURES_ADJUSTMENT` |
| FRTB DRC         | `05_FRTB_DRC_check.sql`           | `FACT.FRTBSA_DRC_MEASURES_ADJUSTMENT`     |
| FRTB RRAO        | `06_FRTB_RRAO_check.sql`          | `FACT.FRTBSA_RRAO_MEASURES_ADJUSTMENT`    |

## What each file returns

1. **Header** — one row from `DIMENSION.ADJUSTMENT`: scope, COB, scale factor,
   filters, the total USD value, status, who/when.
2. **Impact summary** — row count + total USD impact the adjustment wrote.
3. **Impact detail** — every position-level row the adjustment wrote.

## How to run

1. Open a worksheet connected to the database you want to check.
2. Set the database context at the top of the file (uncomment one `USE DATABASE`).
3. Set the id for **that** database: `SET ADJ_ID = <number>;`
4. Run each query and export the result grid to Excel.
5. Repeat in the other database and compare.
</content>
</invoke>
