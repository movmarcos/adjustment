Subject: Adjustment Application — Development Status Update

---

Hi [Manager],

I wanted to share a progress update on the Adjustment Application rebuild.

**Overall Status: On Track**

The core platform is built and running end-to-end on Snowflake. The main pipeline has been replicated and tested for **Scaling adjustments** (flatten, scale, and roll), which is the most complex adjustment type as it involves cross-COB delta netting and overlap resolution.

---

## What Is Done

### Processing Pipeline
- Adjustments are submitted to Snowflake (`ADJ_HEADER`) and processed asynchronously via Snowflake Tasks polling every minute — one task per scope (VaR, Stress, FRTB, Sensitivity).
- The pipeline handles overlap detection: if two adjustments target the same entity/book/COB, the second is blocked until the first completes, preventing double-counting.
- Roll adjustments now produce a single net delta row per position (e.g., −1 + 1.5 → 0.5) rather than two separate rows, significantly reducing fact table volume for large roll batches.

### Approval Workflow
- Adjustments can be submitted directly to processing (Pending) or routed through an approval step (Pending Approval → Approved → Processing).
- Approvers can approve, reject, or sign off with comments. Submitters can recall adjustments before they are approved.
- Full status history is recorded for audit purposes.

### User Interface (Streamlit)
- **Command Center (Home):** KPI dashboard with live counts, COB-level bar charts, overlap alerts panel, error panel, and recent activity feed with Created / Started / Ended timestamps and processing duration.
- **New Adjustment:** Guided form for Scaling, Upload, and Global adjustment types with scope selection, filter chips, and overlap pre-check before submission.
- **My Work:** Personal workspace showing all adjustments by status, with full metadata, status timeline, and available actions (delete, submit for approval, recall, retry).
- **Approval Queue:** Dedicated view for approvers with approve/reject actions and overlap warnings.
- **Processing Queue:** Live pipeline monitor showing queue position, Snowflake Task schedule, and recently processed items with duration metrics.

### Data & Infrastructure
- Snowflake schema with clean separation: `ADJ_HEADER` (orchestration), `DIMENSION.ADJUSTMENT` (dimension SCD2), fact tables per scope.
- Three clear timestamps per adjustment: **Created** (submitted), **Started** (pipeline begins), **Ended** (processed/failed).
- Soft-delete propagated across header, dimension, and fact tables.
- COB sign-off status prevents new submissions after a COB is signed off.
- Dynamic Table (`DT_OVERLAP_ALERTS`) for real-time overlap detection across running and pending adjustments.

---

## Next Steps

### Short Term (Testing & Stabilisation)
1. **Test VaR Upload and Global Adjustments** — validate end-to-end flow for Upload (file-based) and Global (broad-scope) adjustment types across VaR scope.
2. **Fix `DIMENSION.ADJUSTMENT` start timestamp** — currently the dimension table records `PROCESS_DATE` as the processing start time; this should be `START_DATE` now that the two timestamps are split.
3. **Stuck Running row safeguard** — add a cleanup mechanism (scheduled task or manual query) to recover adjustments that remain in `Running` status if a Snowflake Task fails mid-execution without updating status.

### Medium Term (Features)
4. **PowerBI partition action** — add a post-processing step that triggers the PowerBI partition refresh for the affected scope and COB after a successful adjustment, so reports reflect changes without manual refresh.
5. **Book-to-book transfer adjustment type** — new adjustment type that moves values from one book to another within the same COB: debit source book, credit target book, net to zero at entity level.
6. **Dynamic filter fields by scope** — the filter dimensions available when creating an adjustment (entity, book, department, currency, etc.) should be driven by scope configuration, not hardcoded, so new scopes can be onboarded without code changes.

### Longer Term (Quality & Operations)
7. **Test plan and automation** — define a formal test plan covering happy path, overlap scenarios, approval routing, roll netting, error/retry, and sign-off lockout; automate regression tests against a dev Snowflake environment.
8. **User roles and permissions** — formalise who can submit, approve, and sign off per scope. Currently the application uses Snowflake user identity but does not enforce role-based access control at the UI level.
9. **Email/notification integration** — notify approvers when an adjustment is submitted for approval, and notify submitters when their adjustment is approved, rejected, or fails processing.
10. **Environment promotion pipeline** — formalise DEV → UAT → PROD promotion path for DB objects (stored procedures, tasks, views) so changes can be validated before hitting production.
11. **End-user documentation** — user guide covering the submission workflow, approval process, overlap rules, and what to do when an adjustment fails.

---

Happy to walk through any of the above in more detail. Let me know if you have questions or want to adjust priorities.

Best regards,
[Your name]
