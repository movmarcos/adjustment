# Adjustment Engine — Delivery Summary & Roadmap

**Prepared by:** Marcos Magri  ·  **Date:** 24 June 2026
**Note:** Marcos on leave **6–17 July** (back **20 July**)

---

## What we delivered

- **New "Entity Roll" capability** — roll an entity's figures for a given day from
  a reference day. Now reliable and **fast** (previously timed out; now runs in
  minutes), with clear warnings and a user confirmation step before any change.
- **Easier, less error-prone interface** — adjustments shown in a searchable grid
  (was hard to scan), and entity / department / book are now **dropdowns** to stop
  manual input mistakes.
- **Reporting refresh** wired so dashboards update after adjustments for **VaR,
  Stress and Sensitivity**.
- **Verification & reconciliation** — provided a ready-to-use query that
  cross-checks adjustment records across the system, so the team can
  independently confirm an adjustment is complete and consistent.
- **Safer deployments** — moving between environments (development → production)
  is now a single, controlled configuration change.

---

## Open issues

- **FRTB not yet live** — the FRTB measures still need to be implemented and
  switched on (depends on a reporting-team setup step).
- **Reporting refresh for older dates** — by design of the reporting layer, the
  dashboard refresh only triggers for recent business days. A decision is needed
  on whether older dates must also refresh.
- **User sign-off pending** — the new capability and interface changes need user
  acceptance testing before go-live.

---

## Test plan

Users validate the key flows — creating, approving and processing adjustments, the
new Entity Roll, the updated interface, and the reporting refresh — on real data.
Testing starts before the leave so it continues while Marcos is away; feedback is
logged and addressed on return.

---

## Timeline

| When | Focus |
|------|-------|
| **Now – 3 Jul** | Finalise current work, deploy to development, hand over user testing |
| **6 – 17 Jul** (leave) | Users test; reporting team prepares FRTB; no changes / no production deploy |
| **From 20 Jul** | Address user feedback; build FRTB |
| **August** | Production go-live (with developer present) and FRTB once signed off |

---

## Decisions needed from management

1. Approve **production go-live after the leave** (so the developer is present).
2. **Reporting team** to enable FRTB and decide on refresh for older dates.
3. Nominate **users for testing** during the leave.
