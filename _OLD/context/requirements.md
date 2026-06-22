# Unified Adjustment Process — Requirements

> **Created:** 2026-03-28
>
> **Purpose:** Capture all functional and technical requirements for the redesigned
> unified adjustment process. This document drives the architecture design and
> Snowflake object creation.
>
> **Context files:** [adjustment_metadata.md](adjustment_metadata.md) | [var_upload.md](var_upload.md) | [global_adj.md](global_adj.md) | [scaling_adjustment.md](scaling_adjustment.md)

---

## 1. Business Objectives

| # | Objective |
|---|---|
| O1 | **Simplify** — Replace 3 separate pipelines (48+ objects) with one unified process |
| O2 | **Understandable** — Any team member can follow the data flow without deep SQL knowledge |
| O3 | **Fast** — Adjustments processed in seconds, not minutes of polling |
| O4 | **Visible** — Users can see the status of every adjustment at every stage |
| O5 | **Auditable** — Full history of who did what, when, and why |
| O6 | **Safe** — Overlap detection, sign-off enforcement, self-approval prevention |

---

## 2. Adjustment Scopes

The system must support **four processing scopes**, each with different fact tables and measures:

| Scope | Fact Source Table | Adjustment Detail Table | Adjustment Summary Table | Metric (Local) | Metric (USD) |
|---|---|---|---|---|---|
| **VaR** | `FACT.VAR_MEASURES` | `FACT.VAR_MEASURES_ADJUSTMENT` | `FACT.VAR_MEASURES_ADJUSTMENT_SUMMARY` | `PNL_VECTOR_VALUE` | `PNL_VECTOR_VALUE_IN_USD` |
| **Stress** | `FACT.STRESS_MEASURES` | `FACT.STRESS_MEASURES_ADJUSTMENT` | `FACT.STRESS_MEASURES_ADJUSTMENT_SUMMARY` | `SIMULATION_PL` | `SIMULATION_PL_IN_USD` |
| **FRTB** | TBD (via config) | TBD | TBD (may not have summary) | TBD | TBD |
| **Sensitivity** | TBD (via config) | TBD | TBD (may not have summary) | TBD | TBD |

> **Key:** The scope determines which tables to read from and write to. This must be
> **configuration-driven** (not hard-coded), so adding a new scope = adding a row to a
> config table, not changing code.

---

## 3. Adjustment Types (Actions)

| Type | Scale Factor Logic | Description |
|---|---|---|
| **Flatten** | `scale_factor_adjusted = -1` | Zero out fact values for the filtered scope. Insert negated values. |
| **Scale (Same COB)** | `scale_factor_adjusted = scale_factor - 1` | Multiply fact values by scale_factor. Delta = `fact × (sf - 1)`. |
| **Scale (Cross COB)** | `scale_factor_adjusted = scale_factor` + flatten current | Roll data from source COB to target COB, then scale. 3 steps: flatten target, insert source × sf, insert source adjustments × sf. |
| **Direct / Upload** | N/A — values provided directly | User provides explicit adjustment values (e.g., VaR_Upload CSVs with actual USD amounts per scenario date). No scaling from fact. |

---

## 4. Functional Requirements

### FR1 — Streamlit Input (replaces file-based ingestion)

| # | Requirement | Priority |
|---|---|---|
| FR1.1 | Users create adjustments via **Streamlit on Snowflake** — no CSV files as primary interface | Must |
| FR1.2 | Streamlit calls `SP_SUBMIT_ADJUSTMENT(JSON)` which validates and inserts into `ADJUSTMENT.ADJ_HEADER` | Must |
| FR1.3 | For VaR Upload: user uploads CSV via `st.file_uploader()`, Streamlit parses + UNPIVOTs, saves to `ADJ_LINE_ITEM` | Must |
| FR1.4 | For Scale/Flatten: user fills form (scope, entity, book, scale factor) → header only, no line items | Must |
| FR1.5 | All adjustments enter as rows in `ADJUSTMENT.ADJ_HEADER` — this is the single point of entry | Must |
| FR1.6 | Legacy file-based pipelines remain operational during migration but are not extended | Should |

### FR2 — Adjustment Header (ADJ_HEADER)

| # | Requirement | Priority |
|---|---|---|
| FR2.1 | `ADJUSTMENT.ADJ_HEADER` is the single source of truth for all adjustments | Must |
| FR2.2 | Support soft-delete via `IS_DELETED` flag + `DELETED_BY`, `DELETED_DATE` | Must |
| FR2.3 | `PROCESS_TYPE` and `ADJUSTMENT_TYPE` specified by user at submission time | Must |
| FR2.4 | `ADJUSTMENT_ACTION` (Direct/Scale) derived from type by `SP_SUBMIT_ADJUSTMENT` | Must |
| FR2.5 | `SCALE_FACTOR_ADJUSTED` computed at submission: -1 (Flatten), sf-1 (same-COB), sf (cross-COB) | Must |
| FR2.6 | All filter dimensions (entity, book, dept, currency, etc.) stored as columns matching legacy format | Must |
| FR2.7 | Full audit trail via `ADJ_STATUS_HISTORY` (every status transition recorded) | Must |

### FR3 — Processing Queue

| # | Requirement | Priority |
|---|---|---|
| FR3.1 | Adjustments with `RUN_STATUS = 'Pending'` enter the processing queue | Must |
| FR3.2 | Processing must be **scope-aware**: VaR adjustments only touch VaR fact tables | Must |
| FR3.3 | Processing must be **config-driven**: table names, metrics, PKs from a settings table | Must |
| FR3.4 | For **Direct** adjustments: insert provided values directly into fact adjustment tables | Must |
| FR3.5 | For **Scale/Flatten** adjustments: read from fact source, apply scale factor, insert into fact adjustment tables | Must |
| FR3.6 | For **Cross-COB** (Roll): flatten target COB + insert source COB × scale factor | Must |
| FR3.7 | After detail rows are inserted, re-calculate summary table if one exists | Must |
| FR3.8 | Update `RUN_STATUS` to `'Processed'` on success, `'Error'` on failure | Must |

### FR4 — Ad-Hoc vs. Recurring Adjustments

| # | Requirement | Priority |
|---|---|---|
| FR4.1 | **Ad-hoc** adjustments: processed immediately when user presses "Save" (real-time via task or procedure call) | Must |
| FR4.2 | **Recurring** adjustments: configured once, applied daily when all file dependencies are met | Must |
| FR4.3 | Recurring adjustments wait for an external signal (all dependent files loaded) before processing | Must |
| FR4.4 | The system distinguishes ad-hoc vs. recurring via an `ADJUSTMENT_MODE` field (`'ADHOC'` / `'RECURRING'`) | Should |
| FR4.5 | Ad-hoc adjustments skip the file dependency check | Must |

### FR5 — Sign-Off Enforcement

| # | Requirement | Priority |
|---|---|---|
| FR5.1 | No adjustment can be processed after sign-off for that `(COBID, ENTITY_CODE, SCOPE)` | Must |
| FR5.2 | VaR scope checks `BATCH.PUBLISH_VAR_SIGNOFF_STATUS` | Must |
| FR5.3 | Sensitivity/FRTB scopes check `BATCH.PUBLISH_SIGNOFF_STATUS_EXCEPTION` | Must |
| FR5.4 | 10-day VaR adjustments bypass the sign-off gate | Must |
| FR5.5 | Signed-off adjustments get `RUN_STATUS = 'Rejected - SignedOff'` — recorded but not processed | Must |
| FR5.6 | Sign-off status is evaluated at submit time (in `SP_SUBMIT_ADJUSTMENT`) | Must |

### FR6 — Overlap Detection

| # | Requirement | Priority |
|---|---|---|
| FR6.1 | Detect when two or more adjustments target the same fact rows (overlapping filters) | Must |
| FR6.2 | When overlap exists, keep the **most recently posted** adjustment (by `CREATED_DATE DESC, ADJUSTMENT_ID DESC`) | Must |
| FR6.3 | Superseded adjustments are not deleted — they remain in `DIMENSION.ADJUSTMENT` but are excluded from processing | Must |
| FR6.4 | A **view** (`VW_OVERLAP_ALERTS`) surfaces all current overlaps for monitoring | Must |
| FR6.5 | Overlap detection considers: `COBID`, `ENTITY_CODE`, `PROCESS_TYPE`, `BOOK_CODE`, `DEPARTMENT_CODE`, `TRADE_CODE`, `TRADE_TYPOLOGY`, `INSTRUMENT_CODE`, `CURRENCY_CODE` | Must |
| FR6.6 | Future: GUI alert when user is about to create an overlapping adjustment | Should |

### FR7 — Visibility & Monitoring

| # | Requirement | Priority |
|---|---|---|
| FR7.1 | Users can see all adjustments and their current status at any time | Must |
| FR7.2 | A **dashboard view** shows: pending count, running count, processed count, error count, rejected count | Must |
| FR7.3 | Each adjustment shows: who created it, when, what filters, what scope, what type, current status | Must |
| FR7.4 | Error details are captured in `ERRORMESSAGE` field | Must |
| FR7.5 | Processing history/audit trail available via `BATCH.RUN_LOG` | Must |
| FR7.6 | PowerBI metadata updated after processing to trigger downstream refresh | Should |

### FR8 — Dimension Key Resolution

| # | Requirement | Priority |
|---|---|---|
| FR8.1 | Resolve `ENTITY_CODE` → `ENTITY_KEY` via `DIMENSION.ENTITY` | Must |
| FR8.2 | Resolve `BOOK_CODE + DEPARTMENT_CODE + TRADER_CODE + GUARANTEED_ENTITY + REGION_KEY` → `BOOK_KEY` via `DIMENSION.BOOK` | Must |
| FR8.3 | Resolve `TRADE_CODE + TRADE_TYPOLOGY + STRATEGY` → `TRADE_KEY` via `DIMENSION.TRADE` | Must |
| FR8.4 | Resolve `INSTRUMENT_CODE` → `COMMON_INSTRUMENT_KEY` via `DIMENSION.COMMON_INSTRUMENT` | Must |
| FR8.5 | Resolve `VAR_COMPONENT_ID + VAR_SUB_COMPONENT_ID + DAY_TYPE` → `VAR_SUBCOMPONENT_ID` via `DIMENSION.VAR_SUB_COMPONENT` | Must |
| FR8.6 | Resolve `SIMULATION_NAME + SIMULATION_SOURCE` → `STRESS_SIMULATION_KEY` via `DIMENSION.STRESS_SIMULATION` | Must (Stress scope) |
| FR8.7 | For cross-COB adjustments, update `TRADE_KEY`, `COMMON_INSTRUMENT_KEY`, `COMMON_INSTRUMENT_FCD_KEY` using SCD2 effective dates | Must |
| FR8.8 | Dimension resolution should use NULL-safe joins (`OR adjust.column IS NULL` = no filter) | Must |

---

## 5. Non-Functional Requirements

| # | Requirement | Priority |
|---|---|---|
| NF1 | **Config-driven:** Adding a new scope requires only a new row in `ADJUSTMENT.ADJUSTMENTS_SETTINGS`, no code changes | Must |
| NF2 | **Idempotent:** Re-processing the same adjustment produces the same result (DELETE + re-INSERT pattern) | Must |
| NF3 | **Concurrency safe:** Only one session can process a given scope at a time | Should |
| NF4 | **Error isolation:** One failed adjustment does not block others in the same batch | Should |
| NF5 | **Performance:** Process 1000+ adjustments within 60 seconds | Must |
| NF6 | **Language:** Python 3.11 + Snowpark for stored procedures (not JavaScript) | Must |
| NF7 | **Execution mode:** `EXECUTE AS CALLER` for proper RBAC enforcement | Must |
| NF8 | **Zero-value filtering:** Adjustment rows where metric = 0 are excluded from fact tables | Must |
| NF9 | **Summary tables:** When a summary table is configured, auto-rebuild after detail inserts (`GROUP BY ALL + SUM`) | Must |
| NF10 | **Observability:** Every processing run logged with query IDs, row counts, timing | Should |

---

## 6. Data Flow — Target State

```
 ┌──────────────────────────────────────────────────────────────┐
 │                 STREAMLIT ON SNOWFLAKE                       │
 │                                                              │
 │  User creates adjustment:                                    │
 │  • Scale/Flatten: fill form → Preview → Save                │
 │  • Upload (VaR): upload CSV → parse → Save                  │
 │  • Recurring: admin configures template                     │
 └──────────────────────────┬───────────────────────────────────┘
                            │
                            │ CALL SP_SUBMIT_ADJUSTMENT(JSON)
                            ▼
 ┌──────────────────────────────────────────────────────────────┐
 │  ADJUSTMENT.ADJ_HEADER  (single point of entry)             │
 │  + ADJUSTMENT.ADJ_LINE_ITEM  (for Upload values)            │
 │  + ADJUSTMENT.ADJ_STATUS_HISTORY  (audit)                   │
 └──────────────────────────┬───────────────────────────────────┘
                            │
           ┌────────────────┼───────────────────┐
           │                │                   │
           ▼ Ad-hoc         ▼ Stream            ▼ Templates
     (immediate)     ADJ_HEADER_STREAM    INSTANTIATE_
     SP_PROCESS_            │             RECURRING_TASK
     ADJUSTMENT             ▼                   │
           │        PROCESS_PENDING_TASK         │
           │         (1-min, recurring)  ◄───────┘
           │                │
           └────────┬───────┘
                    ▼
 ┌──────────────────────────────────────────────────────────────┐
 │  SP_PROCESS_ADJUSTMENT(scope, action, cobid)                │
 │                                                              │
 │  1. Read ADJUSTMENTS_SETTINGS → table names, metrics        │
 │  2. Filter ADJ_HEADER → Pending + scope match               │
 │  3. Direct: read ADJ_LINE_ITEM → INSERT to fact adj         │
 │  4. Scale: 3-way UNION ALL + DENSE_RANK overlap             │
 │  5. Cross-COB: SCD2 dimension key fix                       │
 │  6. Summary rebuild if configured                           │
 │  7. Update ADJ_HEADER.RUN_STATUS → Processed/Error          │
 │  8. Log to ADJ_STATUS_HISTORY                               │
 └──────────────────────────┬───────────────────────────────────┘
                            │
                            ▼
 ┌──────────────────────────────────────────────────────────────┐
 │  FACT.*_MEASURES_ADJUSTMENT  (+ _SUMMARY)                   │
 └──────────────────────────────────────────────────────────────┘
                            │
                            ▼
 ┌──────────────────────────────────────────────────────────────┐
 │  VISIBILITY LAYER (auto-refresh)                            │
 │  • DT_DASHBOARD — status counts per scope/COB               │
 │  • DT_OVERLAP_ALERTS — overlapping adjustments              │
 │  • VW_SIGNOFF_STATUS — sign-off gate                        │
 │  • VW_DASHBOARD_KPI — KPI cards                             │
 │  • VW_APPROVAL_QUEUE — pending approvals                    │
 │  • VW_MY_WORK — user's adjustments                          │
 └──────────────────────────────────────────────────────────────┘
```

---

## 7. Snowflake Object Strategy

| Object Type | Use Case | Why |
|---|---|---|
| **Tables** | `ADJ_HEADER` (entry point), `ADJ_LINE_ITEM` (upload values), `ADJ_STATUS_HISTORY` (audit), `ADJUSTMENTS_SETTINGS` (config), `ADJ_RECURRING_TEMPLATE` | Persistent state — Streamlit writes here directly |
| **Streams** | On `ADJ_HEADER` and `ADJ_LINE_ITEM` | CDC capture — trigger processing task when new data arrives |
| **Views** | Sign-off status, KPIs, approval queue, my work, processing queue, errors, activity | Real-time reads for Streamlit pages |
| **Stored Procedures** (Python) | `SP_SUBMIT_ADJUSTMENT`, `SP_PREVIEW_ADJUSTMENT`, `SP_PROCESS_ADJUSTMENT` | Business logic, validation, config-driven processing |
| **Tasks** | `PROCESS_PENDING_TASK` (recurring), `INSTANTIATE_RECURRING_TASK` (templates) | Automated processing with stream guards |
| **Dynamic Tables** | `DT_DASHBOARD`, `DT_OVERLAP_ALERTS` | Auto-refresh, zero maintenance, fast dashboard reads |
| **Sequences** | `SEQ_RUN_LOG` (existing in BATCH) | ID generation for run logging |

---

## 8. Object Inventory — Target State

All new objects under `ADJUSTMENT` schema (except `BATCH.RUN_LOG` which already exists):

| # | Object | Type | Purpose |
|---|---|---|---|
| 1 | `ADJ_HEADER` | Table | Single point of entry — Streamlit writes here |
| 2 | `ADJ_LINE_ITEM` | Table | Upload/Direct detail rows (VaR CSV values, etc.) |
| 3 | `ADJ_STATUS_HISTORY` | Table | Complete audit trail of every status transition |
| 4 | `ADJUSTMENTS_SETTINGS` | Table | Config: scope → table mapping, metrics, PKs |
| 5 | `ADJ_RECURRING_TEMPLATE` | Table | Templates for recurring adjustments |
| 6 | `ADJ_HEADER_STREAM` | Stream | CDC on ADJ_HEADER — guards the processing task |
| 7 | `ADJ_LINE_ITEM_STREAM` | Stream | CDC on ADJ_LINE_ITEM — optional validation |
| 8 | `SP_SUBMIT_ADJUSTMENT` | Procedure | Streamlit entry: validate, insert, trigger processing |
| 9 | `SP_PREVIEW_ADJUSTMENT` | Procedure | Preview impact without applying (read-only) |
| 10 | `SP_PROCESS_ADJUSTMENT` | Procedure | Core engine: Direct + Scale, overlap resolution |
| 11 | `PROCESS_PENDING_TASK` | Task | 1-min, stream-guarded — processes recurring adjustments |
| 12 | `INSTANTIATE_RECURRING_TASK` | Task | 5-min — creates ADJ_HEADER from templates |
| 13 | `DT_DASHBOARD` | Dynamic Table | Aggregated status summary (1-min refresh) |
| 14 | `DT_OVERLAP_ALERTS` | Dynamic Table | Overlapping adjustment detection (1-min refresh) |
| 15 | `VW_SIGNOFF_STATUS` | View | Unified sign-off check (real-time) |
| 16 | `VW_DASHBOARD_KPI` | View | KPI cards for dashboard header |
| 17 | `VW_RECENT_ACTIVITY` | View | Activity feed / timeline |
| 18 | `VW_ERRORS` | View | Current errors panel |
| 19 | `VW_APPROVAL_QUEUE` | View | Pending approval items with context |
| 20 | `VW_MY_WORK` | View | User's adjustments |
| 21 | `VW_PROCESSING_QUEUE` | View | Live pipeline view with queue position |
| — | `BATCH.RUN_LOG` | Table | Processing log (already exists, not modified) |

---

## 9. Legacy Object Retirement Plan

Once the unified process is validated, these legacy objects can be retired:

| Legacy Object | Replaced By | Phase |
|---|---|---|
| `FACT.PROCESS_ADJUSTMENTS` (JavaScript) | `ADJUSTMENT.SP_PROCESS_ADJUSTMENT` (Python) | 5 |
| `FACT.PROCESS_ADJUSTMENT_STEP` (JavaScript) | Built into `SP_PROCESS_ADJUSTMENT` | 5 |
| `FACT.UPDATE_POWERBI_FOR_ADJUSTMENTS` | Streamlit dashboard (`DT_DASHBOARD`) or removed | 6 |
| `STAGING.PUBLISH_SCALING_ADJUSTMENT_TASK` | `ADJUSTMENT.PROCESS_PENDING_TASK` | 5 |
| `STAGING.LOAD_VAR_SCALING_ADJUSTMENT_TASK` | `ADJUSTMENT.PROCESS_PENDING_TASK` | 5 |
| `STAGING.LOAD_STRESS_SCALING_ADJUSTMENT_TASK` | `ADJUSTMENT.PROCESS_PENDING_TASK` | 5 |
| `STAGING.LOAD_FRTB_SENSITIVITY_SCALING_ADJUSTMENT_TASK` | `ADJUSTMENT.PROCESS_PENDING_TASK` | 5 |
| `STAGING.PUBLISH_VAR_UPLOAD_TASK` | User uploads via Streamlit → `ADJ_LINE_ITEM` | 4 |
| `STAGING.LOAD_VAR_ADJUSTMENT_UPLOAD_TASK` | `ADJUSTMENT.PROCESS_PENDING_TASK` | 4 |
| `STAGING.GLOBAL_ADJUSTMENT_SF_TASK` | User creates via Streamlit → `ADJ_HEADER` | 4 |
| `FACT.LOAD_VAR_ADJUSTMENT_UPLOAD` (JavaScript) | `ADJUSTMENT.SP_PROCESS_ADJUSTMENT` (Python) | 4 |
| `STAGING.IDM_*_STREAM` views | `ADJUSTMENT.ADJ_HEADER_STREAM` | 6 |
| CSV file drops on network share | Streamlit file uploader → `ADJ_LINE_ITEM` | 4 |

---

## 10. Sample File Reference

| File Type | Sample Location | Rows | Key Columns |
|---|---|---|---|
| VaR_Upload | `context/codes/VaR_Upload/sample_file_received/` | 755 per file | COBId, EntityCode, BookCode, 21 VaR metric columns, Category, Detail |
| Global_Adj | `context/codes/global_adj/sample_file_received/` | 1–3 per file | COBId, AdjustmentId, AdjustmentType, Reason, EntityCode |
| Scaling_Adjustment | `context/codes/scaling_adjustment/sample_file_received/` | 287 | COBID, ProcessType, AdjustmentType, ScaleFactor, EntityCode, 30+ filter columns |

---

## 11. Open Questions

| # | Question | Status | Impact |
|---|---|---|---|
| Q1 | What are the exact FRTB/Sensitivity fact table names and metric columns? | **Open** | Needed for `ADJUSTMENTS_SETTINGS` seed data (marked TODO in 01_tables.sql) |
| Q2 | How does the external "file dependency check" work for recurring adjustments? | **Open** | Impacts `INSTANTIATE_RECURRING_TASK` dependency logic |
| Q3 | Should Global_Adj also write to `DIMENSION.ADJUSTMENT` and fact tables, or stay separate? | **Open** | Impacts scope of unification |
| Q4 | Is there a GUI/Streamlit for ad-hoc adjustments? | **Answered: YES** | Streamlit on Snowflake is the primary UI. `SP_SUBMIT_ADJUSTMENT` is the entry point. |
| Q5 | Should the unified process also handle the VaR_Upload UNPIVOT? | **Answered: Streamlit** | Streamlit Python code parses + UNPIVOTs CSV before writing to `ADJ_LINE_ITEM` |
| Q6 | What PowerBI refresh mechanism is needed going forward? | **Open** | May not be needed if Streamlit dashboard replaces PowerBI |
| Q7 | What warehouse name should the tasks and dynamic tables use? | **Open** | Currently set to `COMPUTE_WH` as placeholder |
