-- =============================================================================
-- 03_SP_SUBMIT_ADJUSTMENT.SQL
-- Entry point for Streamlit. Creates the adjustment header, validates,
-- and optionally triggers immediate processing for ad-hoc adjustments.
--
-- Streamlit calls:
--   CALL ADJUSTMENT_APP.SP_SUBMIT_ADJUSTMENT('{ ... JSON ... }');
-- =============================================================================

USE SCHEMA ADJUSTMENT_APP;

CREATE OR ALTER PROCEDURE ADJUSTMENT_APP.SP_SUBMIT_ADJUSTMENT(p_adjustment VARCHAR)
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
    "erol":         "EntityRoll",
    "entity_roll":  "EntityRoll",   # legacy alias (pre-EROL rename)
    "upload":       "Upload",
    "direct":       "Direct",
}

# Pipeline groupings — used for blocking checks at submit time.
# (FRTBALL is retired: the app submits one adjustment per real FRTB sub-type.)
PIPELINE_TYPES = {
    'VAR':         ['VaR'],
    'STRESS':      ['Stress'],
    'SENSITIVITY': ['Sensitivity'],
    'FRTB':        ['FRTB', 'FRTBDRC', 'FRTBRRAO'],
    'FRTBDRC':     ['FRTB', 'FRTBDRC', 'FRTBRRAO'],
    'FRTBRRAO':    ['FRTB', 'FRTBDRC', 'FRTBRRAO'],
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

def _app_cfg(session, key, default=""):
    rows = session.sql(f"""
        SELECT CONFIG_VALUE FROM ADJUSTMENT_APP.ADJ_APP_CONFIG
        WHERE CONFIG_KEY = '{_esc(key)}'
    """).collect()
    v = rows[0]["CONFIG_VALUE"] if rows else None
    return str(v) if v is not None else default


def check_signoff(session, process_type, cobid, entity_code=None):
    """Return TRUE if the COB is signed off for this scope+entity (blocked).

    Sign-off granularity is COBID + ENTITY + scope, matching the upstream
    publish feed. ENTITY_CODE '*' in the app table = the whole scope.

    App lifecycle rules (ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS), exact-entity row
    taking precedence over the '*' row:
      REOPENED          → open — an approved in-app re-open overrides the
                          upstream feed until the entity is signed off again
      SIGNED_OFF        → blocked
      REOPEN_REQUESTED  → blocked (re-open request awaiting approval)
      OPEN / no row     → fall through to the upstream feed

    A BROAD adjustment (no entity filter) touches every entity, so ANY
    blocked row — entity-level or '*' — blocks it.

    Upstream: the unified publish feed (ADJ_APP_CONFIG.SIGNOFF_FEED_TABLE,
    default BATCH.PUBLISH_SIGNOFF_STATUS) keyed by COBID + ENTITY_CODE +
    PROCESS_TYPE. An upstream 'FRTB' row covers FRTB, FRTBDRC and FRTBRRAO
    (no separate entries exist for DRC/RRAO). SUB_TYPE NULL/'' (VaR-style
    rows) and 'NonCVA' count; 'CVA' rows do not. An upstream sign-off is
    overridden by an app REOPENED row for the same entity (or '*').
    The live feed check can be paused during the feed migration via
    ADJ_APP_CONFIG.SIGNOFF_FEED_ENABLED = 'false' (the synced app rows keep
    governing). A failing feed query RAISES — fail closed, never silently
    wave a control gate through.
    """
    pt_esc = _esc(process_type).upper()
    ent = str(entity_code).strip() if entity_code and str(entity_code).strip() else None
    BLOCKED = ("SIGNED_OFF", "REOPEN_REQUESTED")

    app_rows = session.sql(f"""
        SELECT UPPER(COALESCE(ENTITY_CODE, '*')) AS E, UPPER(SIGN_OFF_STATUS) AS S
        FROM ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS
        WHERE COBID = {int(cobid)} AND UPPER(PROCESS_TYPE) = '{pt_esc}'
    """).collect()
    app = {str(r["E"]): str(r["S"]) for r in app_rows}

    if ent is not None:
        s = app.get(ent.upper())
        if s in (None, "OPEN"):          # no entity-level statement → wildcard
            s = app.get("*")
        if s == "REOPENED":
            return False
        if s in BLOCKED:
            return True
    else:
        if any(s in BLOCKED for s in app.values()):
            return True

    # ── Upstream unified feed (live) ─────────────────────────────────────
    if _app_cfg(session, "SIGNOFF_FEED_ENABLED", "true").strip().lower() != "true":
        return False
    feed = _app_cfg(session, "SIGNOFF_FEED_TABLE",
                    "BATCH.PUBLISH_SIGNOFF_STATUS").strip()
    if pt_esc in ("FRTB", "FRTBDRC", "FRTBRRAO"):
        pt_match = f"UPPER(u.PROCESS_TYPE) IN ('FRTB', '{pt_esc}')"
    else:
        pt_match = f"UPPER(u.PROCESS_TYPE) = '{pt_esc}'"
    ent_pred = (f"AND UPPER(u.ENTITY_CODE) = UPPER('{_esc(ent)}')"
                if ent else "")
    upstream = session.sql(f"""
        SELECT 1
        FROM {feed} u
        WHERE u.COBID = {int(cobid)}
          AND {pt_match}
          AND (u.SUB_TYPE IS NULL OR TRIM(u.SUB_TYPE) = ''
               OR UPPER(u.SUB_TYPE) = 'NONCVA')
          AND UPPER(u.PUBLISH_STATUS) = 'SIGNEDOFF'
          {ent_pred}
          AND NOT EXISTS (
              SELECT 1 FROM ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS a
              WHERE a.COBID = u.COBID
                AND UPPER(a.PROCESS_TYPE) = '{pt_esc}'
                AND UPPER(a.SIGN_OFF_STATUS) = 'REOPENED'
                AND (UPPER(a.ENTITY_CODE) = UPPER(u.ENTITY_CODE)
                     OR a.ENTITY_CODE = '*')
          )
        LIMIT 1
    """).collect()
    return bool(upstream)


def main(session, p_adjustment):
    """
    Submit a new adjustment from Streamlit.

    Expected JSON keys (all optional except marked required):
      cobid              (required)  int     COB date as YYYYMMDD
      process_type       (required)  str     VaR | Stress | FRTB | FRTBDRC | FRTBRRAO | Sensitivity | ES
      adjustment_type    (required)  str     Flatten | Scale | Roll | Upload | Direct | EROL
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
      var_component_id               int     Legacy — prefer var_component_name
      var_sub_component_id           int     Legacy — prefer var_sub_component_name
      var_component_name             str     VaR component (matched by name)
      var_sub_component_name         str     VaR sub-component (matched by name)
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
      adjustment_category (optional)  str     Category label; stored in ADJ_HEADER.ADJUSTMENT_CATEGORY
      global_reference               str
      file_name                      str     Original CSV filename (Upload)

    Returns VARIANT:
      { "adj_id": "a1b2c3d4-...", "status": "Pending", "message": "..." }
    """
    txn_open = False
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

        # ── Check sign-off (COB + entity + scope granularity) ────────────
        if check_signoff(session, process_type, cobid, adj.get("entity_code")):
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
        # Direct adjustments skip overlap checking — they are explicit value
        # insertions. Duplicate detection is handled via GLOBAL_REFERENCE.
        blocked_by_adj_id = None
        if initial_status == STATUS_PENDING and adj_action not in ("Direct", "Upload"):
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
        # Use pre-generated ADJ_ID if provided (VaR Upload writes line items first),
        # otherwise generate a new UUID.
        adj_id = adj.get("adj_id") or str(uuid.uuid4())

        # Map JSON keys → column names (only include non-null values)
        # ── Duplicate reference cleanup ──────────────────────────────
        # If GLOBAL_REFERENCE is provided, soft-delete any existing adjustments
        # with the same COBID + GLOBAL_REFERENCE and remove their FACT data.
        #
        # DIMENSION.ADJUSTMENT and the FACT adjustment tables are keyed by the
        # numeric DIMENSION_ADJ_ID assigned at processing time — never by the
        # ADJ_HEADER UUID. A duplicate that was never processed has no
        # dimension/fact rows, so only the header is soft-deleted.
        #
        # Skipped entirely when the submission is rejected by sign-off: a
        # rejected submission must not destroy the adjustment it would have
        # replaced. The whole cleanup + new-header insert runs in ONE explicit
        # transaction so a failure anywhere rolls the replacement back.
        global_ref = adj.get("global_reference")
        replaced_adj_ids = []
        dup_rows = []
        if (str(adjustment_type).lower() == 'upload'
                and global_ref and str(global_ref).strip()
                and initial_status != STATUS_REJECTED_SO):
            dup_rows = session.sql(f"""
                SELECT ADJ_ID, PROCESS_TYPE, RUN_STATUS, DIMENSION_ADJ_ID
                FROM ADJUSTMENT_APP.ADJ_HEADER
                WHERE COBID = {int(cobid)}
                  AND UPPER(GLOBAL_REFERENCE) = UPPER('{_esc(global_ref)}')
                  AND UPPER(ADJUSTMENT_TYPE) = 'UPLOAD'
                  AND IS_DELETED = FALSE
            """).collect()

            # Never replace an adjustment that is mid-processing: the delete
            # would race the pipeline's fact writes.
            running = [str(d["ADJ_ID"]) for d in dup_rows
                       if str(d["RUN_STATUS"]) == "Running"]
            if running:
                return {"adj_id": None, "status": "Error",
                        "message": (f"An adjustment with reference "
                                    f"'{global_ref}' is currently processing. "
                                    f"Wait for it to finish before replacing it.")}

        session.sql("BEGIN").collect()
        txn_open = True
        try:
            for dup in dup_rows:
                dup_adj_id = str(dup["ADJ_ID"])
                dup_pt     = str(dup["PROCESS_TYPE"])
                old_status = str(dup["RUN_STATUS"]) if dup["RUN_STATUS"] else "Unknown"
                dup_dim_id = dup["DIMENSION_ADJ_ID"]     # NULL if never processed
                replaced_adj_ids.append(dup_adj_id)

                # Soft-delete in ADJ_HEADER
                session.sql(f"""
                    UPDATE ADJUSTMENT_APP.ADJ_HEADER
                    SET IS_DELETED = TRUE,
                        RUN_STATUS = 'Replaced',
                        ERRORMESSAGE = 'Replaced by new upload with same reference'
                    WHERE ADJ_ID = '{_esc(dup_adj_id)}'
                """).collect()

                if dup_dim_id is not None:
                    # Soft-delete in DIMENSION.ADJUSTMENT (numeric key)
                    session.sql(f"""
                        UPDATE DIMENSION.ADJUSTMENT
                        SET IS_DELETED = TRUE
                        WHERE ADJUSTMENT_ID = {int(dup_dim_id)}
                    """).collect()

                    # Delete from FACT adjustment tables (lookup from settings).
                    # No IS_ACTIVE filter: a deactivated scope's fact rows must
                    # still be cleaned. Failures here raise → transaction rolls
                    # back → nothing is destroyed (no silent partial replace).
                    settings_dup = session.sql(f"""
                        SELECT ADJUSTMENTS_TABLE, ADJUSTMENTS_SUMMARY_TABLE
                        FROM ADJUSTMENT_APP.ADJUSTMENTS_SETTINGS
                        WHERE UPPER(PROCESS_TYPE) = UPPER('{_esc(dup_pt)}')
                    """).collect()
                    if not settings_dup:
                        raise Exception(
                            f"Cannot replace ADJ {dup_adj_id}: no "
                            f"ADJUSTMENTS_SETTINGS row for scope '{dup_pt}' "
                            f"to locate its fact adjustment tables.")
                    adj_tbl = settings_dup[0]["ADJUSTMENTS_TABLE"]
                    adj_sum = settings_dup[0]["ADJUSTMENTS_SUMMARY_TABLE"]
                    session.sql(f"""
                        DELETE FROM {adj_tbl}
                        WHERE ADJUSTMENT_ID = {int(dup_dim_id)}
                    """).collect()
                    if adj_sum:
                        session.sql(f"""
                            DELETE FROM {adj_sum}
                            WHERE ADJUSTMENT_ID = {int(dup_dim_id)}
                        """).collect()

                # Audit trail
                session.sql(f"""
                    INSERT INTO ADJUSTMENT_APP.ADJ_STATUS_HISTORY
                        (ADJ_ID, OLD_STATUS, NEW_STATUS, CHANGED_BY, COMMENT)
                    VALUES
                        ('{_esc(dup_adj_id)}', '{_esc(old_status)}', 'Replaced',
                         '{_esc(username)}',
                         'Replaced by new upload ADJ_ID={_esc(adj_id)}')
                """).collect()
        except Exception:
            session.sql("ROLLBACK").collect()
            txn_open = False
            raise

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
            "VAR_COMPONENT_NAME":          adj.get("var_component_name"),
            "VAR_SUB_COMPONENT_NAME":      adj.get("var_sub_component_name"),
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
            "ADJUSTMENT_CATEGORY":         adj.get("adjustment_category"),
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

        # Replacement cleanup + new header + audit all committed together —
        # a failure anywhere above rolled everything back.
        session.sql("COMMIT").collect()
        txn_open = False

        # ── Best-effort notification (never fails the submission) ────────
        # New Pending Approval item → tell the scope's approvers.
        if initial_status == STATUS_PENDING_APPROV:
            try:
                _np = json.dumps({
                    "process_type":    process_type,
                    "cobid":           cobid,
                    "adjustment_type": adjustment_type,
                    "submitted_by":    username,
                }).replace("'", "''")
                session.sql(
                    f"CALL ADJUSTMENT_APP.SP_NOTIFY('approval_pending', '{_np}')"
                ).collect()
            except Exception as notify_err:
                print(f"Approval notification skipped (non-fatal): {notify_err}")

        # Refer to the blocking adjustment by its report number (DIMENSION_ADJ_ID)
        # — the integer users recognise — and only fall back to the internal hash
        # when the blocker hasn't been processed yet (no report number assigned).
        blocked_label = None
        if blocked_by_adj_id:
            try:
                _brow = session.sql(
                    f"SELECT DIMENSION_ADJ_ID FROM ADJUSTMENT_APP.ADJ_HEADER "
                    f"WHERE ADJ_ID = '{_esc(blocked_by_adj_id)}'"
                ).collect()
                _bdim = _brow[0]["DIMENSION_ADJ_ID"] if _brow else None
            except Exception:
                _bdim = None
            blocked_label = f"#{int(_bdim)}" if _bdim is not None else f"#{blocked_by_adj_id}"

        return {
            "adj_id":  adj_id,
            "status":  initial_status,
            "blocked_by": blocked_by_adj_id,
            "replaced": replaced_adj_ids,
            "message": f"Created with status '{initial_status}'."
                       + (f" Blocked by ADJ {blocked_label}." if blocked_by_adj_id else "")
                       + (f" Replaced {len(replaced_adj_ids)} previous adjustment(s)." if replaced_adj_ids else "")
        }

    except KeyError as ke:
        if txn_open:
            try:
                session.sql("ROLLBACK").collect()
            except Exception:
                pass
        return {"adj_id": None, "status": "Error",
                "message": f"Missing required field: {ke}"}
    except Exception as e:
        if txn_open:
            try:
                session.sql("ROLLBACK").collect()
            except Exception:
                pass
        return {"adj_id": None, "status": "Error",
                "message": f"Submission failed: {str(e)}"}
$$;
