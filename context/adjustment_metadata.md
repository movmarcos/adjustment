# Adjustment Metadata — Index

> **Purpose:** Context file for GitHub Copilot and developers describing the upstream
> adjustment file types. Each adjustment type has its own detailed context file.
>
> **Last updated:** 2026-03-28

---

## Adjustment Types

| # | Type | Staging Table | Merge Target | Rows | Context File |
|---|---|---|---|---|---|
| 1 | **VaR_Upload** | `STAGING.IDM_VAR_UPLOAD` | `ADJUSTMENT.IDM_VAR_UPLOAD` | 700+ | [var_upload.md](var_upload.md) |
| 2 | **Global_Adj** | `STAGING.IDM_GLOBAL_ADJUSTMENT_SF` | `ADJUSTMENT.GLOBAL_ADJUSTMENT_SF` | <5 | [global_adj.md](global_adj.md) |
| 3 | **Scaling_Adjustment** | `STAGING.IDM_SCALING_ADJUSTMENT` | `DIMENSION.ADJUSTMENT` | 1000+ | [scaling_adjustment.md](scaling_adjustment.md) |

---

## Redesign Documents

| Document | Purpose |
|---|---|
| [requirements.md](requirements.md) | All functional and technical requirements for the unified process |
| [unified_adjustment_design.md](unified_adjustment_design.md) | Target-state architecture, object inventory, data flow diagrams |

Scripts: `new_adjustment_db_objects/` (01–10 SQL files)

---

## Common Patterns

- All file paths are on the network share `\\mfil\proddfs\fsroot\sfs-prod\raptor\extracts\general\`
- File name date tokens follow the pattern `|:YYYYMMDD:|` (e.g. `VaR_Upload_|:20260328:|_*.csv`)
- `VaR_Upload` and `Scaling_Adjustment` both land in the same folder (`VaRUpload`)
- All pipelines use the **stream + task + MERGE** CDC pattern with a 1-minute schedule
- These staging tables (`STAGING.IDM_*`) are distinct from the app's internal
  `ADJUSTMENT_DB.CORE` tables (`ADJ_HEADER`, `ADJ_LINE_ITEM`)
- These file-based adjustment types represent **source feed inputs**, whereas the app
  models adjustment operations as FLATTEN / SCALE / ROLL
