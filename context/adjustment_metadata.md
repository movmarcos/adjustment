# Adjustment Metadata — Source File Reference

> **Purpose:** Context file for GitHub Copilot and developers describing the upstream
> adjustment file types, their naming conventions, file layouts, staging destinations,
> and row-volume expectations.
>
> **Last updated:** 2026-03-28
>
> ⚠️ Fields marked `[INCOMPLETE]` were not fully visible in the source document.
> Please fill them in with the correct values.

---

## Adjustment Types

### 1. VaR_Upload

| Property | Value |
|---|---|
| **Description** | Users upload the adjustment file direct to the folder |
| **File Name** | `VaR_Upload_\|:YYYYMMDD:\|_*.csv` |
| **Path** | `\\mfil\proddfs\fsroot\sfs-prod\raptor\extracts\general\VaRUpload` |
| **Table Destination** | `STAGING.IDM_VAR_UPLOAD` |
| **Rows Per File** | Can be more than 700 |

**Layout (columns in order):**

```
COBId, EntityCode, SourceSystemCode, BookCode, CurrencyCode, ScenarioDate, TradeCode,
AllVaR, AllVaRSkew, BasisVaR, BondAssetSpreadVaR, CrossEffects, EquityPriceVaR,
EquityVegaVaR, FXRateVaR, FXVolatilityVaR, IRCapVolVaR, IRCapVolVaRSkew, IRSkewVolVaR,
IRSwaptionVolVaR, IRSwaptionVolVaRSkew, InflationRateCurveVaR, InflationVolVaR,
InterestRateCurveVaR, InterestRateVegaVaR, MTGSprdVaR, OASVaR, ParCreditSpreadVaR,
Category, Detail
```

---

### 2. Global_Adj

| Property | Value |
|---|---|
| **Description** | Not sure how it is created |
| **File Name** | `Global_Adj_SF_\|:YYYYMMDD:\|_*.csv` |
| **Path** | `\\mfil\proddfs\fsroot\sfs-prod\raptor\extracts\general\GlobalAdjustmentSF` |
| **Table Destination** | `STAGING.IDM_GLOBAL_ADJUSTMENT_SF` |
| **Rows Per File** | Less than 5 usually |

**Layout (columns in order):**

```
COBId, AdjustmentId, SourceCOBId, ApprovalId, AdjustmentType, Reason, FormData,
AdjustedBy, AdjustmentCreated, AdjustmentCompleted, Status, EntityCode, EntityKey,
DepartmentCode, IsDeleted, DeletedBy, AdjustmentDeleted, TempId
```

---

### 3. Scaling_Adjustment

| Property | Value |
|---|---|
| **Description** | Most used adjustment during the day. Comes from SSRS reporting; Python code identifies new adjustments and exports the file |
| **File Name** | `Scaling_Adjustment_\|:YYYYMMDD:\|.csv` |
| **Path** | `\\mfil\proddfs\fsroot\sfs-prod\raptor\extracts\general\VaRUpload` |
| **Table Destination** | `STAGING.IDM_SCALING_ADJUSTMENT` |
| **Rows Per File** | Can be more than 1000 |

**Layout (columns in order):**

```
ID, COBID, ProcessType, AdjustmentType, SourceCOBID, EntityCode, SourceSystemCode,
DepartmentCode, BookCode, CurrencyCode, TradeTypology, TradeCode, Strategy, ScaleFactor,
TraderCode, VaRComponentId, VaRSubComponentId, GuaranteedEntity, RegionKey, ScenarioDateId,
AdjustmentValueInUSD, ErrorMessage, ApprovalId, Reason, ActiveStatus, GlobalId, UserName,
CreatedDate, ExtractDate, InstrumentCode, SimulationName, ProductCategoryAttributes,
Simulation_Source, DayType, CurveCode, MeasureTypeCode, TenorCode, UnderlyingTenorCode
```

---

## Notes

- All file paths are on the network share `\\mfil\proddfs\fsroot\sfs-prod\raptor\extracts\general\`
- File name date tokens follow the pattern `|:YYYYMMDD:|` (e.g. `VaR_Upload_|:20260328:|_*.csv`)
- `VaR_Upload` and `Scaling_Adjustment` both land in the same folder (`VaRUpload`)
- These staging tables (`STAGING.IDM_*`) are distinct from the app's internal
  `ADJUSTMENT_DB.CORE` tables (`ADJ_HEADER`, `ADJ_LINE_ITEM`)
- These file-based adjustment types (VaR_Upload, Global_Adj, Scaling_Adjustment)
  represent **source feed inputs**, whereas the app models adjustment operations as FLATTEN / SCALE / ROLL
- `Scaling_Adjustment` is the highest-volume feed (1000+ rows); `Global_Adj` is the lowest (<5 rows)
