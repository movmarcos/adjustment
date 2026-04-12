-- =============================================================================
-- 03_SP_SUBMIT_ADJUSTMENT.SQL
-- Entry point for Streamlit. Creates the adjustment header, validates,
-- and optionally triggers immediate processing for ad-hoc adjustments.
--
-- Streamlit calls:
--   CALL ADJUSTMENT_APP.SP_SUBMIT_ADJUSTMENT('{ ... JSON ... }');
-- =============================================================================

USE DATABASE DVLP_RAPTOR_NEWADJ;
USE SCHEMA ADJUSTMENT_APP;

CREATE OR REPLACE PROCEDURE ADJUSTMENT_APP.SP_SUBMIT_ADJUSTMENT(p_adjustment VARCHAR)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
COMMENT = 'Entry point from Streamlit. Validates input, checks for Running blockers, inserts into ADJ_HEADER with BLOCKED_BY_ADJ_ID if overlapping. Processing handled by scope pipeline tasks.'
EXECUTE AS CALLER
AS
$$
import json
import uuid
from snowflake.snowpark.functions import col, lit, upper, max as sf_max
from datetime import datetime

# ─── Status constants ────────────────────────────────────────────────────────
STATUS_PENDING        = "Pending"
STATUS_PENDING_APPROV = "Pending Approval"
STATUS_REJECTED_SO    = "Rejected - SignedOff"

# ─── SQL escape helper ───────────────────────────────────────────────────────
def _esc(val):
    """Escape a value for safe inclusion in a SQL string literal."""
    if val is None:
        return None
    return str(val).replace("'", "''")

# ─── Adjustment type → action mapping ────────────────────────────────────────
ACTION_MAP = {
    "flatten":      "Scale",
    "scale":        "Scale",
    "roll":         "Scale",
    "entity_roll":  "EntityRoll",
    "upload":       "Direct",
    "direct":       "Direct",
}

# Pipeline groupings — used for blocking checks at submit time
PIPELINE_TYPES = {
    'VAR':         ['VaR'],
    'STRESS':      ['Stress'],
    'SENSITIVITY': ['Sensitivity'],
    'FRTB':        ['FRTB', 'FRTBDRC', 'FRTBRRAO', 'FRTBALL'],
    'FRTBDRC':     ['FRTB', 'FRTBDRC', 'FRTBRRAO', 'FRTBALL'],
    'FRTBRRAO':    ['FRTB', 'FRTBDRC', 'FRTBRRAO', 'FRTBALL'],
    'FRTBALL':     ['FRTB', 'FRTBDRC', 'FRTBRRAO', 'FRTBALL'],
}

OVERLAP_DIMS_SUBMIT = [
    'ENTITY_CODE', 'SOURCE_SYSTEM_CODE', 'DEPARTMENT_CODE',
    'BOOK_CODE', 'CURRENCY_CODE', 'TRADE_TYPOLOGY', 'TRADE_CODE',
    'STRATEGY', 'TRADER_CODE', 'INSTRUMENT_CODE',
    'SIMULATION_NAME', 'SIMULATION_SOURCE',
    'TENOR_CODE', 'UNDERLYING_TENOR_CODE', 'CURVE_CODE',
    'MEASURE_TYPE_CODE', 'PRODUCT_CATEGORY_ATTRIBUTES',
    'BATCH_REGION_AREA', 'MUREX_FAMILY', 'MUREX_GROUP',
    'GUARANTEED_ENTITY',
]


def find_blocking_adj(session, process_type, cobid, adj_values):
    """
    Return ADJ_ID of an adjustment in the same pipeline that blocks the new one,
    or None if no blocker found.

    adj_values: dict with optional dimension values (lowercase keys) for the new adjustment.

    Checks against Pending, Approved, AND Running adjustments — not just Running.
    This is consistent with SP_RUN_PIPELINE's _block_pending_overlaps which also
    blocks Pending-vs-Pending overlaps. The user sees the blocking at submit time
    rather than being surprised when the pipeline silently blocks it later.

    Overlap rule: same COBID + each dimension matches (or one side is NULL/wildcard).
    ADJUSTMENT_TYPE and ADJUSTMENT_ACTION are NOT considered — a Flatten and a
    Scale targeting the same data DO overlap.
    """
    pipeline = PIPELINE_TYPES.get(process_type.upper(), [process_type])
    pipeline_in = ", ".join(f"'{t}'" for t in pipeline)

    dim_conditions = []
    for dim in OVERLAP_DIMS_SUBMIT:
        new_val = adj_values.get(dim.lower())
        if new_val is None:
            # New adj is wildcard (NULL) → matches any value on this dimension
            dim_conditions.append("TRUE")
        else:
            escaped = str(new_val).replace("'", "''")
            dim_conditions.append(
                f"(r.{dim} IS NULL OR UPPER(r.{dim}) = UPPER('{escaped}'))"
            )

    where_dims = " AND ".join(dim_conditions)
    sql = f"""
        SELECT ADJ_ID FROM ADJUSTMENT_APP.ADJ_HEADER r
        WHERE r.COBID = {int(cobid)}
          AND r.PROCESS_TYPE IN ({pipeline_in})
          AND r.RUN_STATUS IN ('Pending', 'Approved', 'Running')
          AND r.IS_DELETED = FALSE
          AND {where_dims}
        ORDER BY r.CREATED_DATE ASC
        LIMIT 1
    """

    rows = session.sql(sql).collect()
    return rows[0]["ADJ_ID"] if rows else None


def compute_scale_factor_adjusted(adj_type, scale_factor, cobid, source_cobid):
    """Derive the effective scale factor the processing engine multiplies by."""
    t = adj_type.lower()
    if t == "flatten":
        return -1.0
    elif t in ("scale", "roll"):
        if source_cobid and int(source_cobid) != int(cobid):
            return float(scale_factor)           # cross-COB → full factor
        else:
            return float(scale_factor) - 1.0     # same-COB  → incremental
    else:
        return 0.0                               # Direct / Upload: not used

def check_signoff(session, process_type, cobid):
    """Return TRUE if the COB is signed off for this scope (no new adjustments allowed)."""
    sql = f"""
        SELECT COUNT(*) AS cnt
        FROM ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS
        WHERE COBID  = {int(cobid)}
          AND UPPER(PROCESS_TYPE) = '{_esc(process_type).upper()}'
          AND UPPER(SIGN_OFF_STATUS) = 'SIGNED_OFF'
    """
    result = session.sql(sql).collect()
    return result[0]["CNT"] > 0 if result else False


def main(session, p_adjustment):
    """
    Submit a new adjustment from Streamlit.

    Expected JSON keys (all optional except marked required):
      cobid              (required)  int     COB date as YYYYMMDD
      process_type       (required)  str     VaR | Stress | FRTB | FRTBDRC | FRTBRRAO | Sensitivity | ES
      adjustment_type    (required)  str     Flatten | Scale | Roll | Upload | Direct
      username           (required)  str     Streamlit user
      source_cobid                   int     Defaults to cobid
      scale_factor                   float   Defaults to 1.0
      reason                         str
      entity_code                    str
      source_system_code             str
      department_code                str
      book_code                      str
      currency_code                  str
      trade_typology                 str
      trade_code                     str
      strategy                       str
      trader_code                    str
      var_component_id               int
      var_sub_component_id           int
      guaranteed_entity              str
      region_key                     int
      scenario_date_id               int
      instrument_code                str
      simulation_name                str
      simulation_source              str
      tenor_code                     str
      underlying_tenor_code          str
      curve_code                     str
      measure_type_code              str
      day_type                       int
      product_category_attributes    str
      batch_region_area              str
      murex_family                   str
      murex_group                    str
      adjustment_value_in_usd        float   For Direct only
      adjustment_occurrence          str     ADHOC (default) | RECURRING
      global_reference               str
      file_name                      str     Original CSV filename (Upload)

    Returns VARIANT:
      { "adj_id": "a1b2c3d4-...", "status": "Pending", "message": "..." }
    """
    try:
        adj = json.loads(p_adjustment) if isinstance(p_adjustment, str) else p_adjustment

        # ── Required fields ──────────────────────────────────────────────
        cobid          = adj["cobid"]
        process_type   = adj["process_type"]
        adjustment_type = adj["adjustment_type"]
        username       = adj.get("username")
        if not username or not str(username).strip():
            # Fallback: resolve from session context (works with READ SESSION grant)
            try:
                row = session.sql("SELECT CURRENT_USER() AS U").collect()
                username = row[0]["U"] if row and row[0]["U"] else None
            except Exception:
                pass
        if not username or not str(username).strip():
            return {"adj_id": None, "status": "Error",
                    "message": "username could not be resolved. Ensure READ SESSION is granted to the app owner role."}

        # ── Derived / defaulted ──────────────────────────────────────────
        source_cobid    = adj.get("source_cobid", cobid)
        scale_factor    = float(adj.get("scale_factor", 1.0))
        adj_action      = ACTION_MAP.get(adjustment_type.lower())
        if adj_action is None:
            return {"adj_id": None, "status": "Error",
                    "message": f"Unknown adjustment_type: {adjustment_type}"}

        sf_adjusted = compute_scale_factor_adjusted(
            adjustment_type, scale_factor, cobid, source_cobid)

        occurrence  = adj.get("adjustment_occurrence", "ADHOC").upper()

        # ── Validate scope is active ─────────────────────────────────────
        settings = session.table("ADJUSTMENT_APP.ADJUSTMENTS_SETTINGS").filter(
            (upper(col("PROCESS_TYPE")) == process_type.upper()) &
            (col("IS_ACTIVE") == True)
        ).collect()
        if not settings:
            return {"adj_id": None, "status": "Error",
                    "message": f"Scope '{process_type}' is not active or not configured."}

        # ── Check sign-off ───────────────────────────────────────────────
        if check_signoff(session, process_type, cobid):
            # Still insert the header (for audit) but mark as rejected
            initial_status = STATUS_REJECTED_SO
        else:
            # Entity Roll always requires approval (destructive operation)
            requires_approval = adj.get("requires_approval", False)
            if adj_action == "EntityRoll":
                requires_approval = True
            if requires_approval:
                initial_status = STATUS_PENDING_APPROV
            else:
                initial_status = STATUS_PENDING

        # ── Blocking check — set before INSERT ──────────────────────────
        blocked_by_adj_id = None
        if initial_status == STATUS_PENDING:
            dim_vals = {
                "entity_code":                 adj.get("entity_code"),
                "source_system_code":          adj.get("source_system_code"),
                "department_code":             adj.get("department_code"),
                "book_code":                   adj.get("book_code"),
                "currency_code":               adj.get("currency_code"),
                "trade_typology":              adj.get("trade_typology"),
                "trade_code":                  adj.get("trade_code"),
                "strategy":                    adj.get("strategy"),
                "trader_code":                 adj.get("trader_code"),
                "instrument_code":             adj.get("instrument_code"),
                "simulation_name":             adj.get("simulation_name"),
                "simulation_source":           adj.get("simulation_source"),
                "tenor_code":                  adj.get("tenor_code"),
                "underlying_tenor_code":       adj.get("underlying_tenor_code"),
                "curve_code":                  adj.get("curve_code"),
                "measure_type_code":           adj.get("measure_type_code"),
                "product_category_attributes": adj.get("product_category_attributes"),
                "batch_region_area":           adj.get("batch_region_area"),
                "murex_family":                adj.get("murex_family"),
                "murex_group":                 adj.get("murex_group"),
                "guaranteed_entity":           adj.get("guaranteed_entity"),
            }
            blocked_by_adj_id = find_blocking_adj(session, process_type, cobid, dim_vals)

        # ── Build the INSERT ─────────────────────────────────────────────
        # Generate a UUID for this adjustment — unambiguous vs DIMENSION.ADJUSTMENT.ADJUSTMENT_ID
        adj_id = str(uuid.uuid4())

        # Map JSON keys → column names (only include non-null values)
        col_map = {
            "ADJ_ID":                      adj_id,
            "COBID":                       cobid,
            "PROCESS_TYPE":                process_type,
            "ADJUSTMENT_TYPE":             adjustment_type,
            "ADJUSTMENT_ACTION":           adj_action,
            "SOURCE_COBID":                source_cobid,
            "SCALE_FACTOR":                scale_factor,
            "SCALE_FACTOR_ADJUSTED":       sf_adjusted,
            "ENTITY_CODE":                 adj.get("entity_code"),
            "SOURCE_SYSTEM_CODE":          adj.get("source_system_code"),
            "DEPARTMENT_CODE":             adj.get("department_code"),
            "BOOK_CODE":                   adj.get("book_code"),
            "CURRENCY_CODE":               adj.get("currency_code"),
            "TRADE_TYPOLOGY":              adj.get("trade_typology"),
            "TRADE_CODE":                  adj.get("trade_code"),
            "STRATEGY":                    adj.get("strategy"),
            "TRADER_CODE":                 adj.get("trader_code"),
            "VAR_COMPONENT_ID":            adj.get("var_component_id"),
            "VAR_SUB_COMPONENT_ID":        adj.get("var_sub_component_id"),
            "GUARANTEED_ENTITY":           adj.get("guaranteed_entity"),
            "REGION_KEY":                  adj.get("region_key"),
            "SCENARIO_DATE_ID":            adj.get("scenario_date_id"),
            "INSTRUMENT_CODE":             adj.get("instrument_code"),
            "SIMULATION_NAME":             adj.get("simulation_name"),
            "SIMULATION_SOURCE":           adj.get("simulation_source"),
            "TENOR_CODE":                  adj.get("tenor_code"),
            "UNDERLYING_TENOR_CODE":       adj.get("underlying_tenor_code"),
            "CURVE_CODE":                  adj.get("curve_code"),
            "MEASURE_TYPE_CODE":           adj.get("measure_type_code"),
            "DAY_TYPE":                    adj.get("day_type"),
            "PRODUCT_CATEGORY_ATTRIBUTES": adj.get("product_category_attributes"),
            "BATCH_REGION_AREA":           adj.get("batch_region_area"),
            "MUREX_FAMILY":                adj.get("murex_family"),
            "MUREX_GROUP":                 adj.get("murex_group"),
            "ADJUSTMENT_VALUE_IN_USD":     adj.get("adjustment_value_in_usd"),
            "REASON":                      adj.get("reason"),
            "RUN_STATUS":                  initial_status,
            "IS_POSITIVE_ADJUSTMENT":      True,
            "IS_DELETED":                  False,
            "USERNAME":                    username,
            "ADJUSTMENT_OCCURRENCE":       occurrence,
            "GLOBAL_REFERENCE":            adj.get("global_reference"),
            "FILE_NAME":                   adj.get("file_name"),
            "APPROVAL_ID":                 adj.get("approval_id"),
            "BLOCKED_BY_ADJ_ID":           blocked_by_adj_id,
        }

        # Only include non-None values (let NULLs default in the table)
        cols_to_insert = {k: v for k, v in col_map.items() if v is not None}
        col_names  = ", ".join(cols_to_insert.keys())
        col_values = ", ".join([
            ("TRUE" if v else "FALSE") if isinstance(v, bool) else
            f"'{_esc(v)}'" if isinstance(v, str) else
            f"{v}"   if isinstance(v, (int, float)) else
            "NULL"
            for v in cols_to_insert.values()
        ])

        insert_sql = f"""
            INSERT INTO ADJUSTMENT_APP.ADJ_HEADER ({col_names})
            VALUES ({col_values})
        """
        session.sql(insert_sql).collect()

        # ── Audit: status history ────────────────────────────────────────
        session.sql(f"""
            INSERT INTO ADJUSTMENT_APP.ADJ_STATUS_HISTORY
                (ADJ_ID, OLD_STATUS, NEW_STATUS, CHANGED_BY, COMMENT)
            VALUES
                ('{_esc(adj_id)}', NULL, '{_esc(initial_status)}', '{_esc(username)}',
                 'Submitted via Streamlit — {_esc(adjustment_type)} / {_esc(process_type)}')
        """).collect()

        return {
            "adj_id":  adj_id,
            "status":  initial_status,
            "blocked_by": blocked_by_adj_id,
            "message": f"Adjustment {adj_id} created with status '{initial_status}'."
                       + (f" Blocked by ADJ #{blocked_by_adj_id}." if blocked_by_adj_id else "")
        }

    except KeyError as ke:
        return {"adj_id": None, "status": "Error",
                "message": f"Missing required field: {ke}"}
    except Exception as e:
        return {"adj_id": None, "status": "Error",
                "message": f"Submission failed: {str(e)}"}
$$;
