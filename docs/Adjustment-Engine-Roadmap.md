# Adjustment Engine — Status & Roadmap

**Author:** Marcos Magri
**Date:** 24 June 2026
**Key dates:** Marcos on leave **6–17 July** (back **20 July**)

---

## 1. Summary

The Adjustment Engine (Streamlit-on-Snowflake app + Snowflake processing pipeline)
lets risk users create, approve, process and track adjustments across the risk
measures. It currently supports **VaR, Stress and Sensitivity** end-to-end, with
**FRTB** (FRTB / FRTBDRC / FRTBRRAO) partially in place and to be completed.

This document covers: what is done, known issues, the FRTB work, user testing,
and the path to deploy from **DEV → PROD**.

---

## 2. Recently delivered (this cycle)

- **Entity Roll redesign** — destructive replace: wipes all of an entity's
  adjustments at the target COB (including those loaded by external systems),
  flags them deleted in `ADJ_HEADER` + `DIMENSION.ADJUSTMENT`, then rebuilds the
  entity from a source COB. UI shows a clear warning, an impact count and a
  mandatory consent checkbox.
- **Entity Roll performance** — reduced a >1-hour / timing-out run to ~minutes:
  removed the costly netting, read the combined view once (outside the
  transaction via temp tables), flatten from the base table, single-predicate
  delete, entity-scoped summary rebuild.
- **UX improvements** — Adjustments page is now a scannable **grid** (instead of
  blocks); **Entity / Department / Book are dropdowns**, with Book filtered by
  Department, to stop free-text mistakes.
- **Environment config centralised** — all DB / warehouse / role names live in a
  single `config.py`, so re-pointing to another database is a one-file change.
- **PowerBI trigger** aligned to the original reporting procedure for VaR / Stress
  / Sensitivity.

---

## 3. Known issues / open items

| # | Item | Detail | Severity | Status / dependency |
|---|------|--------|----------|---------------------|
| 1 | PowerBI action not created for older COBs | `POWERBI_ACTION` is only generated when the COB is within ~3 business days (gated by the external `VW_POWERBI_ACTION_INSERT_SOURCE`). Publish-info / publish-detail are written correctly. | Low–Med | **Decision needed**: is "recent COB only" acceptable, or must historical COBs publish? Latter = change to the reporting-side view (not this app). |
| 2 | PowerBI for FRTB | The reporting metadata view only recognises `var/stress/sensitivity`. FRTB refresh is currently **skipped** (logged). | Med | **Reporting team** must register FRTB data group/sources before FRTB can publish. |
| 3 | Entity Roll performance — confirm in DEV | The fixes are in; needs a clean timed run per scope to confirm it stays within target (~3 min). | Med | Validate during UAT. |
| 4 | Config → different DB deploy | Centralised config is untested against a second database; the PROD external objects (FACT views, DIMENSION, BATCH, METADATA) must exist. | Med | Verify as part of PROD deploy. |
| 5 | More user testing | Several flows (destructive Entity Roll, grid, dropdowns, PowerBI) need real user sign-off and edge-case feedback. | Med | UAT (Section 5). |

---

## 4. FRTB implementation

FRTB covers three sub-scopes — **FRTB** (FRTBSA Sensitivity), **FRTBDRC**,
**FRTBRRAO** — which differ from VaR/Stress/Sensitivity (single surrogate key,
no summary table, different metric columns).

**Status:** the core processing and Entity Roll logic are written to be
scope-generic (config-driven) and were verified against the FRTB schemas. Still
required:

- [ ] End-to-end processing test per FRTB sub-scope (Scale, Direct, Entity Roll).
- [ ] Confirm fact / combined-view / dimension objects exist and are readable.
- [ ] PowerBI: align with the reporting team so FRTB data groups publish (item #2).
- [ ] FRTB-specific edge cases from users (filters, keys, FRTBALL handling).

**Estimate:** ~1 week of focused work once dependencies (esp. PowerBI metadata)
are confirmed — targeted for **after** the leave.

---

## 5. User testing (UAT)

Hand to users **before leave** so testing can continue while Marcos is away.

| Area | What to test |
|------|--------------|
| Entity Roll (destructive) | Wipe + rebuild correct; reconciliation counts match; consent gate works |
| Performance | Entity Roll completes in minutes on a real COB/entity |
| Adjustments grid | Filtering, selecting a row, actions (delete / force / retry) |
| Dropdowns | Entity/Department/Book values correct; Book filters by Department |
| Approvals & pipeline | Submit → approve → process → status/PowerBI tracking |
| PowerBI | Refresh appears for a **recent** COB (VaR/Stress/Sensitivity) |

UAT feedback to be logged and triaged on return.

---

## 6. Deployment: DEV → PROD

**Approach:** identical codebase; only `config.py` changes between environments
(database, warehouses, roles, prod compare-DB). Deploy via `deploy.py`.

**Pre-requisites (confirm with platform / reporting):**
- PROD database + schema (`ADJUSTMENT_APP`) and roles/warehouses provisioned.
- PROD external objects present & readable: `FACT.*_MEASURES`, `*_COMBINED/_ADJUSTED`,
  `*_ADJUSTMENT(_SUMMARY)`, `DIMENSION.*`, `BATCH.RUN_LOG`, `METADATA.POWERBI_*`.
- `ADJUSTMENTS_SETTINGS` seeded for all active scopes in PROD.

**Steps:**
1. Set PROD values in `config.py` (database, warehouses, roles, prod-DB).
2. `python deploy.py --db-only` → verify objects, run the SQL test harnesses.
3. `python deploy.py --streamlit-only` → smoke-test the app, grant access.
4. Targeted UAT on PROD with one COB per scope.
5. Sign-off → enable the pipeline tasks (`ALTER TASK … RESUME`).

**Rollback:** procedures are `CREATE OR ALTER`; redeploy the prior tagged commit.
Tasks can be `SUSPEND`ed immediately to halt processing.

**Recommended:** keep PROD deploy until **after the leave** so the developer is
present for the go-live and any issues.

---

## 7. Timeline (with leave 6–17 July)

| Window | Plan |
|--------|------|
| **24 Jun – 3 Jul** (pre-leave) | Stabilise current work; deploy latest to DEV; confirm Entity Roll timing (#3); hand UAT pack to users; open reporting-team request for FRTB PowerBI (#2) and the historical-COB decision (#1). |
| **6 – 17 Jul** (Marcos on leave) | Users run UAT and log feedback; reporting team progresses FRTB PowerBI metadata; **no PROD deploy / no FRTB code changes** during this period. |
| **20 – 31 Jul** (return) | Triage UAT feedback; implement FRTB (Section 4); finalise PROD pre-reqs. |
| **Aug** | PROD deployment + go-live UAT (Section 6); FRTB to PROD once signed off. |

---

## 8. Risks & dependencies

- **Reporting-team dependency** — FRTB PowerBI publishing (#2) and the
  historical-COB behaviour (#1) are owned outside this app; FRTB go-live depends
  on them.
- **External objects in PROD** — go-live assumes the FACT/DIMENSION/BATCH/METADATA
  objects exist and are readable by the app role.
- **Coverage during leave** — no developer changes 6–17 July; scope that window to
  UAT and external dependencies only.

---

## 9. One-line asks for the manager

1. Approve **PROD deploy after the leave** (developer present for go-live).
2. Chase the **reporting team** on FRTB PowerBI metadata + the historical-COB decision.
3. Nominate **UAT users** so testing runs during the leave.
