"""
New Adjustment — Order Ticket
==============================
Single dense screen (no wizard steps): segmented pills select Category /
Scope / Type in one click, a compact field grid holds dates and factors,
and a live Ticket panel on the right mirrors the selections, runs the
impact preview, and carries the Submit button.

Widget-state note: widget keys are versioned via _k(); reset_wizard()
bumps the version so every control is recreated fresh after a submit.
"""
import streamlit as st
import json
import pandas as pd

st.set_page_config(
    page_title="New Adjustment · MUFG", page_icon="✏️",
    layout="wide", initial_sidebar_state="expanded",
)

from utils.styles import (
    inject_css, render_sidebar,
    P, SCOPE_CONFIG, TYPE_CONFIG, CATEGORY_CONFIG, render_df_table,
    fmt_adj_id, icon, bordered_container,
)
from utils.snowflake_conn import (run_query, call_sp_df, current_user_name,
                                  safe_rerun, friendly_error)

inject_css()
render_sidebar()


# ══════════════════════════════════════════════════════════════════════════════
# WIZARD STATE
# ══════════════════════════════════════════════════════════════════════════════

_WIZ_DEFAULTS: dict = {
    "step": 1,
    "category": None,
    # Scaling
    "process_type":           None,
    "adjustment_type":        None,
    "occurrence":             "ADHOC",
    "source_cobid":           None,
    "recurring_start_cobid":  None,
    "recurring_end_cobid":    None,
    "scale_factor":           1.0,
    # Shared
    "cobid":                  None,
    "entity_code":            None,
    "source_system_code":     None,
    "department_code":        None,
    "book_code":              None,
    "currency_code":          None,
    "trade_typology":         None,
    "trade_code":             None,
    "strategy":               None,
    "instrument_code":        None,
    "simulation_name":        None,
    "simulation_source":      None,
    "measure_type_code":      None,
    "trader_code":            None,
    "var_component_name":     None,
    "var_sub_component_name": None,
    "guaranteed_entity":      None,
    "region_key":              None,
    "scenario_date_id":       None,
    "tenor_code":             None,
    "underlying_tenor_code":  None,
    "curve_code":             None,
    "day_type":               None,
    "product_category_attributes": None,
    "batch_region_area":      None,
    "murex_family":           None,
    "murex_group":            None,
    "reason":                 "",
    "adjustment_category":    None,
    "requires_approval":      False,
    # VaR Upload
    "global_reference":       None,
    "uploaded_file_name":     None,
    "uploaded_df":            None,
    # Direct Adjustment (order-free CSV → stage → per-row submit)
    "direct_batch_id":        None,
    "direct_ndf":             None,
    "direct_verdicts":        None,
    "direct_grid_df":         None,
    "direct_grid_ver":        0,
    "_direct_in_mode":        None,
    # Internal
    "result":                 None,
}


def _k(name: str) -> str:
    """Versioned widget key — prevents stale widget state after wizard reset."""
    return f"{name}_{st.session_state.get('_wiz_v', 0)}"


def reset_wizard() -> None:
    """Reset all wizard fields and bump key version to clear widget state."""
    st.session_state["_wiz_v"] = st.session_state.get("_wiz_v", 0) + 1
    st.session_state["wiz"] = dict(_WIZ_DEFAULTS)


if "wiz" not in st.session_state:
    reset_wizard()

wiz: dict = st.session_state["wiz"]


# ══════════════════════════════════════════════════════════════════════════════
# SUBMISSION LOGIC
# ══════════════════════════════════════════════════════════════════════════════

def _build_payload() -> dict:
    cat = wiz.get("category")

    if cat == "VaR Upload":
        return {
            "cobid":                 wiz["cobid"],
            "process_type":          wiz["process_type"],
            "adjustment_type":       "Upload",
            "username":              current_user_name(),
            "source_cobid":          wiz["cobid"],
            "reason":                wiz.get("reason", ""),
            "entity_code":           wiz.get("entity_code", ""),
            "requires_approval":     wiz.get("requires_approval", False),
            "adjustment_occurrence": "ADHOC",
            "file_name":             wiz.get("uploaded_file_name", ""),
            "global_reference":      wiz.get("global_reference", ""),
            "adjustment_category":   wiz.get("adjustment_category"),
        }

    if cat == "Entity Roll":
        return {
            "cobid":                 wiz["cobid"],
            "process_type":          wiz["process_type"],
            "adjustment_type":       "EROL",
            "username":              current_user_name(),
            "source_cobid":          wiz.get("source_cobid") or wiz["cobid"],
            "reason":                wiz.get("reason", ""),
            "entity_code":           wiz.get("entity_code", ""),
            "requires_approval":     True,
            "adjustment_occurrence": "ADHOC",
            "global_reference":      wiz.get("global_reference"),
            "adjustment_category":   wiz.get("adjustment_category"),
        }

    # Scaling Adjustment
    payload = {
        "cobid":                 wiz["cobid"],
        "process_type":          wiz["process_type"],
        "adjustment_type":       wiz["adjustment_type"],
        "username":              current_user_name(),
        "source_cobid":          wiz.get("source_cobid") or wiz["cobid"],
        "scale_factor":          wiz.get("scale_factor", 1.0),
        "reason":                wiz.get("reason", ""),
        "adjustment_occurrence": wiz.get("occurrence", "ADHOC"),
        "requires_approval":     wiz.get("requires_approval", False),
    }
    payload["adjustment_category"] = wiz.get("adjustment_category")
    if wiz.get("global_reference"):
        payload["global_reference"] = wiz.get("global_reference")
    if wiz.get("occurrence") == "RECURRING":
        payload["recurring_start_cobid"] = wiz.get("recurring_start_cobid")
        payload["recurring_end_cobid"]   = wiz.get("recurring_end_cobid")
    for key in ["entity_code", "source_system_code", "department_code",
                "book_code", "currency_code", "trade_typology",
                "strategy", "instrument_code", "simulation_name",
                "simulation_source", "measure_type_code", "trade_code",
                "trader_code", "var_component_name", "var_sub_component_name",
                "guaranteed_entity", "region_key", "scenario_date_id",
                "tenor_code", "underlying_tenor_code", "curve_code",
                "day_type", "product_category_attributes",
                "batch_region_area", "murex_family", "murex_group"]:
        val = wiz.get(key)
        if val and str(val).strip():
            payload[key] = str(val).strip()
    return payload


def _direct_row_payload(row: dict) -> dict:
    """One staged Direct Adjustment row → one SP_SUBMIT_ADJUSTMENT payload.
    Called per row by the submit loop in _do_submit (Direct never touches
    ADJ_LINE_ITEM_JSON — each valid row becomes its own adjustment)."""
    p = {
        "cobid":                 wiz["cobid"],
        "process_type":          wiz["process_type"],
        "adjustment_type":       "Direct",
        "username":              current_user_name(),
        "source_cobid":          wiz["cobid"],
        "reason":                wiz.get("reason", ""),
        "requires_approval":     wiz.get("requires_approval", False),
        "adjustment_occurrence": "ADHOC",
        "adjustment_category":   wiz.get("adjustment_category"),
        "adjustment_value_in_usd": float(row["VALUE_USD"]),
    }
    for stage_col, payload_key in [
            ("ENTITY_CODE", "entity_code"), ("SOURCE_SYSTEM_CODE", "source_system_code"),
            ("DEPARTMENT_CODE", "department_code"), ("BOOK_CODE", "book_code"),
            ("TRADE_CODE", "trade_code"), ("TRADE_TYPOLOGY", "trade_typology"),
            ("STRATEGY", "strategy"), ("INSTRUMENT_CODE", "instrument_code"),
            ("SIMULATION_NAME", "simulation_name"), ("SIMULATION_SOURCE", "simulation_source"),
            ("MEASURE_TYPE_CODE", "measure_type_code"), ("CURRENCY_CODE", "currency_code"),
            ("TENOR_CODE", "tenor_code"), ("CURVE_CODE", "curve_code"),
            ("PRODUCT_CATEGORY_ATTRIBUTES", "product_category_attributes")]:
        v = row.get(stage_col)
        if not _direct_cell_blank(v):
            p[payload_key] = str(v).strip()
    row_reason = row.get("REASON")
    if not _direct_cell_blank(row_reason):
        p["reason"] = str(row_reason).strip()
    return p


# ── VaR Upload: config-driven schema + JSON upload writer ─────────────────────
def _direct_schema(scope: str) -> dict:
    """Full DIRECT_SCOPE_SCHEMA config for a scope (cached per session)."""
    cache_key = f"_ref_direct_schema_{scope}"
    if cache_key in st.session_state:
        return st.session_state[cache_key]
    cfg = {"expected": [], "unpivot": None, "fact_mapping": [], "resolutions": []}
    try:
        rows = run_query(f"""
            SELECT EXPECTED_COLUMNS, UNPIVOT, FACT_MAPPING, RESOLUTIONS
            FROM ADJUSTMENT_APP.DIRECT_SCOPE_SCHEMA
            WHERE UPPER(PROCESS_TYPE) = UPPER('{scope.replace("'", "''")}')
              AND IS_ACTIVE = TRUE
        """)
        if rows:
            def _j(v):
                return json.loads(v) if isinstance(v, str) else v
            r = rows[0]
            cfg["expected"]     = _j(r["EXPECTED_COLUMNS"]) or []
            cfg["unpivot"]      = _j(r["UNPIVOT"])
            cfg["fact_mapping"] = _j(r["FACT_MAPPING"]) or []
            cfg["resolutions"]  = _j(r["RESOLUTIONS"]) or []
    except Exception:
        pass
    st.session_state[cache_key] = cfg
    return cfg


def _direct_expected_columns(scope: str) -> list:
    """Ordered expected CSV column names (empty if no config for the scope)."""
    return [c.get("name") for c in _direct_schema(scope)["expected"] if c.get("name")]


def _validate_direct_upload(df: pd.DataFrame, schema: dict) -> dict:
    """Server-side pre-validation of a Direct CSV, BEFORE anything is written.

    Checks, mirroring exactly what the processing engine will do:
      • required columns present (hard block)
      • every coded CSV field resolves against its dimension table — the
        engine would otherwise map unknown codes to key -1 silently
      • unpivot measure names resolve (config-level: a measure that does not
        resolve loses ALL its values)
      • ambiguous codes (one code matching several dimension keys → the
        engine's join would duplicate those rows)
      • numeric measure cells actually numeric (non-numeric → row skipped)

    Returns a dict consumed by _render_upload_validation / _completion_checks.
    """
    out = {"missing_required": [], "missing_optional": [],
           "unknown_row_codes": {},   # csv_col → {"values": [...], "rows": n}
           "unknown_measures": [],    # unpivot measure names with no dim match
           "ambiguous": {},           # field → [codes matching >1 key]
           "nonnumeric": {},          # csv_col → n cells
           "reject_df": None, "checked_fields": []}

    expected = schema.get("expected") or []
    for c in expected:
        name = c.get("name")
        if name and name not in df.columns:
            (out["missing_required"] if c.get("required")
             else out["missing_optional"]).append(name)

    unpivot      = schema.get("unpivot") or {}
    measure_map  = (unpivot.get("measure_map") or {}) if unpivot else {}
    name_field   = unpivot.get("measure_name_field") if unpivot else None
    fact_by_tgt  = {m.get("target_column"): m.get("payload_field")
                    for m in (schema.get("fact_mapping") or [])}

    def _dim_lookup(dim_table, match_col, key_col, values):
        """Return (known_upper_set, ambiguous_list) for the given values."""
        vals = sorted({str(v).strip().upper() for v in values
                       if v is not None and str(v).strip() != ""})[:2000]
        if not vals:
            return set(), []
        in_list = ", ".join("'" + v.replace("'", "''") + "'" for v in vals)
        known = run_query(f"""
            SELECT DISTINCT UPPER({match_col}) AS V
            FROM {dim_table}
            WHERE UPPER({match_col}) IN ({in_list})
        """)
        known_set = {str(r["V"]) for r in known} if known else set()
        ambi = run_query(f"""
            SELECT UPPER({match_col}) AS V
            FROM {dim_table}
            WHERE UPPER({match_col}) IN ({in_list})
            GROUP BY UPPER({match_col})
            HAVING COUNT(DISTINCT {key_col}) > 1
        """)
        ambi_list = sorted({str(r["V"]) for r in ambi}) if ambi else []
        return known_set, ambi_list

    reject_reasons = pd.Series("", index=df.index)

    for res in (schema.get("resolutions") or []):
        src   = res.get("source_field")
        dim   = res.get("dimension_table")
        match = res.get("match_column")
        keyc  = res.get("key_column")
        if not (src and dim and match and keyc):
            continue
        try:
            if name_field and src == name_field:
                # Config-level: the coded values are the unpivot MEASURE NAMES
                # for the CSV columns actually present in this upload.
                measures = {measure_map[k]: k for k in measure_map
                            if k in df.columns}
                known, ambi = _dim_lookup(dim, match, keyc, measures.keys())
                out["checked_fields"].append(f"{src} (measure names)")
                unknown = [m for m in measures
                           if str(m).strip().upper() not in known]
                if unknown:
                    out["unknown_measures"] = sorted(
                        f"{m} (column {measures[m]})" for m in unknown)
                if ambi:
                    out["ambiguous"][src] = ambi
            else:
                csv_col = fact_by_tgt.get(src)
                if not csv_col or csv_col not in df.columns:
                    continue
                known, ambi = _dim_lookup(dim, match, keyc, df[csv_col].dropna())
                out["checked_fields"].append(csv_col)
                up = df[csv_col].astype(str).str.strip().str.upper()
                filled = df[csv_col].notna() & (up != "") & (up != "NAN")
                bad = filled & ~up.isin(known)
                if bad.any():
                    out["unknown_row_codes"][csv_col] = {
                        "values": sorted(up[bad].unique().tolist())[:25],
                        "rows": int(bad.sum()),
                    }
                    reject_reasons[bad] = (
                        reject_reasons[bad] + f"unknown {csv_col}; ")
                if ambi:
                    out["ambiguous"][csv_col] = ambi
        except Exception:
            # A validation query failing must never block the workflow — the
            # engine remains the authority; just note the field as unchecked.
            out["checked_fields"].append(f"{src} (check unavailable)")

    # Numeric checks: unpivot measure columns, or the flat metric field.
    numeric_cols = ([k for k in measure_map if k in df.columns] if measure_map
                    else [c.get("name") for c in expected
                          if c.get("type") == "number" and c.get("name") in df.columns])
    for col in numeric_cols:
        raw = df[col]
        filled = raw.notna() & (raw.astype(str).str.strip() != "")
        bad = filled & pd.to_numeric(raw, errors="coerce").isna()
        if bad.any():
            out["nonnumeric"][col] = int(bad.sum())

    mask = reject_reasons != ""
    if mask.any():
        rej = df[mask].copy()
        rej["REJECT_REASON"] = reject_reasons[mask].str.rstrip("; ")
        out["reject_df"] = rej
    return out


def _render_upload_validation(v: dict, row_count: int) -> None:
    """Render the validation verdict + rejects download + override control."""
    problems = bool(v["missing_required"] or v["unknown_row_codes"]
                    or v["unknown_measures"])
    if v["missing_required"]:
        st.error(f"Missing REQUIRED columns: {', '.join(v['missing_required'])} "
                 f"— submission is blocked until the file includes them.")
    for col, info in v["unknown_row_codes"].items():
        st.error(f"**{col}**: {info['rows']} row(s) carry codes that don't "
                 f"exist in the reference data — e.g. "
                 f"{', '.join(info['values'][:8])}. Unresolved codes would be "
                 f"stored under the fallback key (-1) and lose attribution.")
    if v["unknown_measures"]:
        st.error("These measures don't resolve in the reference data — ALL "
                 "their values would land under the fallback key (-1): "
                 + ", ".join(v["unknown_measures"]))
    for field, codes in v["ambiguous"].items():
        st.warning(f"**{field}**: {', '.join(codes[:8])} match MORE THAN ONE "
                   f"reference row — affected rows would be duplicated when "
                   f"processed. Check the dimension data before submitting.")
    for col, n in v["nonnumeric"].items():
        st.warning(f"**{col}**: {n} non-numeric cell(s) — those values will "
                   f"be skipped when processed.")
    if v["reject_df"] is not None:
        st.download_button(
            f"⬇ Download {len(v['reject_df'])} rejected row(s) (CSV)",
            v["reject_df"].to_csv(index=False).encode("utf-8-sig"),
            file_name="upload_rejects.csv", mime="text/csv",
            key=_k("rejects_dl"))
    if problems:
        if not v["missing_required"]:
            wiz["_upval_override"] = st.checkbox(
                "Submit anyway — I understand unresolved codes will be stored "
                "under the fallback key (-1)",
                key=_k("upval_override"), value=False)
    else:
        wiz["_upval_override"] = False
        checked = ", ".join(v["checked_fields"]) if v["checked_fields"] else "columns"
        st.success(f"Validated {row_count} row(s) — {checked} all resolve "
                   f"against the reference data.")


def _write_direct_json_rows(adj_id: str, df_csv: pd.DataFrame) -> int:
    """Store each CSV row verbatim as a JSON object in ADJ_LINE_ITEM_JSON.
    Returns the number of rows written."""
    from utils.snowflake_conn import get_session
    session = get_session()
    if df_csv is None or len(df_csv) == 0:
        return 0

    payloads = pd.DataFrame({
        "ADJ_ID":  adj_id,
        "ROW_NUM": range(1, len(df_csv) + 1),
        # one JSON string per row; NaN → None so PARSE_JSON yields null
        "PAYLOAD_TEXT": [json.dumps({k: (None if pd.isna(v) else v) for k, v in rec.items()})
                         for rec in df_csv.to_dict(orient="records")],
    })

    session.write_pandas(
        payloads, table_name="TEMP_DIRECT_JSON_ROWS", schema="ADJUSTMENT_APP",
        auto_create_table=True, overwrite=True, table_type="temporary")
    session.sql("""
        INSERT INTO ADJUSTMENT_APP.ADJ_LINE_ITEM_JSON (ADJ_ID, ROW_NUM, PAYLOAD)
        SELECT ADJ_ID, ROW_NUM, PARSE_JSON(PAYLOAD_TEXT)
        FROM ADJUSTMENT_APP.TEMP_DIRECT_JSON_ROWS
    """).collect()
    return len(payloads)


def _delete_direct_json_rows(adj_id: str) -> None:
    """Remove pre-written Direct line items for an adj_id. Used to roll back when
    the header submission is rejected/fails, so line items aren't left orphaned."""
    from utils.snowflake_conn import get_session
    safe = str(adj_id).replace("'", "''")
    get_session().sql(
        f"DELETE FROM ADJUSTMENT_APP.ADJ_LINE_ITEM_JSON WHERE ADJ_ID = '{safe}'"
    ).collect()


# Statuses SP_SUBMIT_ADJUSTMENT returns when the adjustment was actually accepted
# into the workflow. NB: a sign-off rejection returns "Rejected - SignedOff"
# (not "Error"), so checking `status != "Error"` would wrongly show success.
_SUBMIT_SUCCESS_STATUSES = ("Pending", "Pending Approval", "Approved")


def _is_submit_success(result: dict) -> bool:
    return (result or {}).get("status") in _SUBMIT_SUCCESS_STATUSES


def _submit_one(payload: dict) -> dict:
    """One CALL to SP_SUBMIT_ADJUSTMENT; returns the parsed result dict.

    Snowflake escapes a single quote by DOUBLING it (''), not with a
    backslash. A backslash leaves the quote active → broken/injectable CALL.
    Backslashes must be doubled FIRST: Snowflake literals interpret \\n, \\t
    etc., so json.dumps's "\\n" would arrive as a raw newline inside the
    JSON and json.loads in the SP fails with "Invalid control character"."""
    json_str = json.dumps(payload).replace("\\", "\\\\").replace("'", "''")
    rows = run_query(f"CALL ADJUSTMENT_APP.SP_SUBMIT_ADJUSTMENT('{json_str}')")
    if not rows:
        return {"status": "Error", "message": "No response from stored procedure"}
    raw = rows[0][0]
    return json.loads(str(raw)) if isinstance(raw, str) else raw


def _do_submit() -> dict:
    """Call SP_SUBMIT_ADJUSTMENT. Returns result dict (never raises)."""
    import uuid as _uuid
    wrote_line_items_for = None
    try:
        # Direct Adjustment: one row = one adjustment, never a JSON line item.
        # Modelled on the FRTBALL fan-out loop below — submit N, collect
        # ok/fail per unit, report a summary. Invalid rows are never submitted.
        if wiz.get("category") == "Direct Adjustment":
            batch_id = wiz.get("direct_batch_id")
            ndf      = wiz.get("direct_ndf")
            verdicts = wiz.get("direct_verdicts") or []
            valid_rows = {v["ROW_NUM"] for v in verdicts if v["IS_VALID"]}
            if not batch_id or ndf is None or ndf.empty or not valid_rows:
                return {"status": "Error",
                        "message": "No valid staged rows to submit — parse a CSV "
                                   "with at least one valid row first."}
            n_total = len(valid_rows)
            created, failures, statuses = [], [], []
            for i, (_, row) in enumerate(ndf.iterrows(), start=1):
                if i not in valid_rows:
                    continue
                # Never let one row's exception abort the loop — a network hiccup
                # on row 5 of 500 must not lose track of rows 1-4's created headers.
                try:
                    row_res = _submit_one(_direct_row_payload(row.to_dict()))
                except Exception as exc:
                    failures.append(f"row {i}: {exc}")
                    continue
                if _is_submit_success(row_res):
                    created.append(i)
                    statuses.append(row_res.get("status"))
                else:
                    failures.append(f"row {i}: {row_res.get('message', 'not accepted')}")
            try:
                _delete_direct_batch(batch_id)
            except Exception:
                pass
            wiz["direct_batch_id"] = None
            wiz["direct_ndf"]      = None
            wiz["direct_verdicts"] = None
            wiz["_direct_sig"]     = None
            wiz["direct_grid_df"]  = None
            wiz["direct_grid_ver"] = wiz.get("direct_grid_ver", 0) + 1
            # Partial failure must never read as success: any failed row makes
            # the overall result an Error, even though the successful rows'
            # headers already exist (mirrors the FRTBALL fan-out's semantics).
            if failures:
                shown = "; ".join(failures[:10])
                more  = f" (+{len(failures) - 10} more)" if len(failures) > 10 else ""
                return {"status": "Error",
                        "message": f"Created {len(created)} of {n_total} Direct "
                                   f"adjustments. Failures: {shown}{more}"}
            return {"status": statuses[0],
                    "message": f"Created {len(created)} Direct adjustment(s)."}

        payload = _build_payload()

        # "All FRTB": FRTBALL is not a processable scope — submit one sibling
        # adjustment per real FRTB sub-type instead. The pipeline serialises
        # them (same data scope, same pipeline), so they apply in sequence.
        if payload.get("process_type") == "FRTBALL":
            created, failures, statuses = [], [], []
            for sub in ("FRTB", "FRTBDRC", "FRTBRRAO"):
                sub_res = _submit_one({**payload, "process_type": sub})
                if _is_submit_success(sub_res):
                    created.append(sub)
                    statuses.append(sub_res.get("status"))
                else:
                    failures.append(f"{sub}: {sub_res.get('message', 'not accepted')}")
            if not failures:
                return {"status": statuses[0],
                        "message": ("Created 3 adjustments — one per FRTB sub-type "
                                    "(FRTB, FRTBDRC, FRTBRRAO). They are queued and "
                                    "will be processed in sequence.")}
            partial = (f" Already created: {', '.join(created)} — delete them from "
                       f"the Adjustments page if they are no longer wanted."
                       if created else "")
            return {"status": "Error",
                    "message": ("Not all FRTB sub-types were accepted. "
                                + " | ".join(failures) + partial)}

        # For VaR Upload: write line items BEFORE the SP call so that
        # navigating away can't interrupt the write. Pre-generate the
        # ADJ_ID so both line items and header share the same ID.
        if wiz.get("category") == "VaR Upload" and wiz.get("uploaded_df") is not None:
            adj_id = str(_uuid.uuid4())
            payload["adj_id"] = adj_id
            n = _write_direct_json_rows(adj_id, wiz["uploaded_df"])
            if n == 0:
                return {"status": "Error",
                        "message": "No rows found in CSV data"}
            wrote_line_items_for = adj_id

        result = _submit_one(payload)
    except Exception as exc:
        result = {"status": "Error", "message": str(exc)}

    # Roll back pre-written Direct line items if the header was NOT accepted,
    # so a rejection/failure doesn't leave orphaned rows in ADJ_LINE_ITEM_JSON.
    if wrote_line_items_for and not _is_submit_success(result):
        try:
            _delete_direct_json_rows(wrote_line_items_for)
        except Exception:
            pass
    return result


# ══════════════════════════════════════════════════════════════════════════════
# UI HELPERS
# ══════════════════════════════════════════════════════════════════════════════

FRTB_SUBTYPES = ["FRTB", "FRTBDRC", "FRTBRRAO", "FRTBALL"]
FRTB_SUBTYPE_CONFIG = {
    "FRTB":     "Standard FRTB",
    "FRTBDRC":  "Default Risk Charge",
    "FRTBRRAO": "Residual Risk Add-On",
    "FRTBALL":  "All FRTB — submits one adjustment per sub-type",
}

# Scope-specific filter fields (tier 2)
SCOPE_FIELDS = {
    "VaR":         [],
    "Stress":      [("simulation_name",   "Simulation Name",   "e.g. MRM_GLB_Std_EQ_M_PriceDnVolUp"),
                    ("trade_typology",    "Trade Typology",    "e.g. EQTT"),
                    ("instrument_code",   "Instrument Code",   "e.g. US4642872422 US")],
    "Sensitivity": [("measure_type_code", "Measure Type Code", "e.g. FxDeltaExp"),
                    ("strategy",          "Strategy",          "e.g. SSA00306"),
                    ("trade_typology",    "Trade Typology",    "e.g. FEXF"),
                    ("instrument_code",   "Instrument Code",   "e.g. US46090E1038 US")],
    "FRTB":        [("measure_type_code", "Measure Type Code", "e.g. FRTBCSRDelta"),
                    ("instrument_code",   "Instrument Code",   "e.g. US92826C8394 US"),
                    ("strategy",          "Strategy",          "e.g. SSU00332")],
}
for _fst in FRTB_SUBTYPES:
    SCOPE_FIELDS.setdefault(_fst, SCOPE_FIELDS["FRTB"])

ALL_EXTRA_FIELDS = [
    ("currency_code",                "Currency Code",                "e.g. USD"),
    ("trade_typology",               "Trade Typology",               "e.g. FEXF"),
    ("trade_code",                   "Trade Code",                   ""),
    ("strategy",                     "Strategy",                     ""),
    ("instrument_code",              "Instrument Code",              ""),
    ("simulation_name",              "Simulation Name",              ""),
    ("simulation_source",            "Simulation Source",            ""),
    ("measure_type_code",            "Measure Type Code",            ""),
    ("trader_code",                  "Trader Code",                  ""),
    ("guaranteed_entity",            "Guaranteed Entity",            ""),
    ("region_key",                   "Region Key",                   ""),
    ("scenario_date_id",             "Scenario Date ID",             ""),
    ("tenor_code",                   "Tenor Code",                   ""),
    ("underlying_tenor_code",        "Underlying Tenor Code",        ""),
    ("curve_code",                   "Curve Code",                   ""),
    ("product_category_attributes",  "Product Category Attributes",  ""),
    ("batch_region_area",            "Batch Region Area",            ""),
    ("murex_family",                 "Murex Family",                 ""),
    ("murex_group",                  "Murex Group",                  ""),
]

# Every filter key (used for the ticket's applied-filter chips)
FILTER_KEYS = ["entity_code", "source_system_code", "department_code", "book_code",
               "var_component_name", "var_sub_component_name", "day_type"] + \
              [k for k, _, _ in ALL_EXTRA_FIELDS]


def _btn(label, *, icon_name=None, **kwargs):
    """st.button with a Material icon when the runtime supports the icon param."""
    if icon_name:
        try:
            return st.button(label, icon=icon_name, **kwargs)
        except TypeError:
            pass
    return st.button(label, **kwargs)


def _pill_row(options, selected, key_prefix, fmt=None, icons=None, descs=None):
    """One-click segmented selector built from small buttons.

    Buttons are used instead of st.radio/st.pills: the selection is driven
    purely by wiz state (selected → primary red), so there is no widget
    default-vs-state fight and a single click always takes effect.
    `descs` adds a small caption under each option (two-line card look).
    Returns the clicked option, or None."""
    cols = st.columns(len(options))
    clicked = None
    for i, opt in enumerate(options):
        with cols[i]:
            if _btn(fmt(opt) if fmt else str(opt),
                    icon_name=(icons or {}).get(opt),
                    key=_k(f"{key_prefix}_{opt}"),
                    use_container_width=True,
                    type="primary" if opt == selected else "secondary"):
                clicked = opt
            if descs and descs.get(opt):
                st.caption(descs[opt])
    return clicked


def _sec(num: int, title: str, helper: str = "") -> None:
    """Numbered section header inside a section card, followed by a hairline."""
    help_html = f'<div class="sec-help">{helper}</div>' if helper else ""
    st.markdown(
        f'<div class="sec-head"><span class="sec-num">{num}</span>'
        f'<div><div class="sec-title">{title}</div>{help_html}</div></div>'
        f'<div style="height:1px;background:{P["border"]};margin:2px 0 10px 0"></div>',
        unsafe_allow_html=True)


from contextlib import contextmanager


@contextmanager
def _card():
    """Section card followed by a spacer, so cards never hug each other (the
    gap is emitted in the page flow — version-proof, unlike CSS on Streamlit's
    wrapper test-ids). bordered_container() draws a real border on 1.29+ AND
    on SiS 1.26 via the .sec-card-flag marker + :has() CSS."""
    with bordered_container():
        yield
    st.markdown('<div style="height:0.7rem"></div>', unsafe_allow_html=True)


def _safe_int(v) -> int:
    """int() that survives None and NaN (NaN is truthy, so `v or 0` won't)."""
    try:
        if v is None or pd.isna(v):
            return 0
        return int(v)
    except (TypeError, ValueError):
        return 0


# Material icon shortcodes for the pill buttons (ignored on older Streamlit)
CATEGORY_BTN_ICONS = {
    "Scaling Adjustment": ":material/balance:",
    "VaR Upload":         ":material/upload_file:",
    "Direct Adjustment":  ":material/edit_note:",
    "Entity Roll":        ":material/autorenew:",
}

# Category picker copy — local to this page. CATEGORY_CONFIG in utils/styles.py
# already reflects the post-split "Direct Adjustment"/"VaR Upload" concepts;
# this dict overrides two of its descriptions with picker-specific wording
# while reusing CATEGORY_CONFIG's for the other two.
CATEGORY_UI_DESCS = {
    "Scaling Adjustment": CATEGORY_CONFIG["Scaling Adjustment"]["desc"],
    "Direct Adjustment":   "Paste/upload a CSV of exact values — one adjustment per row (all scopes)",
    "VaR Upload":          "Upload one file of exact VaR adjustment values (CSV, whole file = one adjustment)",
    "Entity Roll":         CATEGORY_CONFIG["Entity Roll"]["desc"],
}
SCOPE_BTN_ICONS = {
    "VaR":         ":material/bar_chart:",
    "Stress":      ":material/show_chart:",
    "FRTB":        ":material/account_balance:",
    "Sensitivity": ":material/adjust:",
}
TYPE_BTN_ICONS = {
    "Scale":   ":material/bar_chart:",
    "Flatten": ":material/remove_circle:",
    "Roll":    ":material/autorenew:",
}
OCC_BTN_ICONS = {
    "ADHOC":     ":material/event:",
    "RECURRING": ":material/event_repeat:",
}


def _int_input(label, key, value, placeholder="e.g. 20260328"):
    """Compact YYYYMMDD date input returning int, or None when empty/invalid.

    Strict validation: a transposed digit used to pass as a "valid" COB
    (e.g. 2026328) and flow into previews and submission targeting a
    nonexistent date — now it surfaces an inline error and blocks the ticket
    (the completion checklist treats None as missing)."""
    from datetime import datetime as _dt
    raw = st.text_input(label, key=_k(key), value=str(value or ""),
                        placeholder=placeholder).strip()
    if not raw:
        return None
    if not raw.isdigit() or len(raw) != 8:
        st.error(f"“{raw}” is not a valid COB — use exactly 8 digits, "
                 f"YYYYMMDD (e.g. 20260328).")
        return None
    try:
        _dt.strptime(raw, "%Y%m%d")
    except ValueError:
        st.error(f"“{raw}” is not a real calendar date — check the month/day.")
        return None
    return int(raw)


def _float_input(label, key, value, min_v=-10.0, max_v=100.0,
                 help=None, placeholder="e.g. 1.05"):
    """Type-only numeric input (no +/- steppers) returning a clamped float.

    Unlike st.number_input, st.text_input has no stepper buttons, so it avoids
    the per-click rerun lag that makes +/- unusable in Snowsight. Invalid or
    out-of-range entries surface an inline error and fall back to the last value.
    """
    default = float(value if value is not None else 1.0)
    shown = ("%.4f" % default).rstrip("0").rstrip(".")
    raw = st.text_input(label, key=_k(key), value=shown,
                        help=help, placeholder=placeholder)
    s = raw.strip()
    if not s:
        return default
    try:
        v = float(s)
    except ValueError:
        st.error(f"“{raw}” is not a number — keeping {default:g}")
        return default
    if v < min_v or v > max_v:
        st.error(f"Scale Factor must be between {min_v:g} and {max_v:g}")
        return max(min_v, min(max_v, v))
    return v


def _info_banner(text: str) -> None:
    st.markdown(
        f'<div style="background:{P["info_lt"]};border:1px solid #BFDBFE;'
        f'border-radius:8px;padding:0.7rem 1rem;margin-bottom:1rem;font-size:0.85rem">'
        f'{text}</div>', unsafe_allow_html=True)


def _is_entity_only(wiz: dict) -> bool:
    """True when a Scaling Adjustment is broad-scope (entity set, no book or department).

    Entity+currency is treated as broad-scope because it still targets millions of
    rows and the preview times out. The pipeline blocking logic already handles it
    correctly — only book_code, department_code and (for VaR) the VaR component
    filters narrow the scope meaningfully.
    """
    if wiz.get("category") != "Scaling Adjustment":
        return False
    if not (wiz.get("entity_code") or "").strip():
        return False
    has_book = bool((wiz.get("book_code") or "").strip())
    has_dept = bool((wiz.get("department_code") or "").strip())
    has_var  = bool((wiz.get("var_component_name") or "").strip()) \
        or bool((wiz.get("var_sub_component_name") or "").strip())
    return not (has_book or has_dept or has_var)


def _preview_payload() -> dict:
    pj = {
        "cobid":           wiz["cobid"],
        "process_type":    wiz["process_type"],
        "adjustment_type": wiz["adjustment_type"],
        "source_cobid":    wiz.get("source_cobid") or wiz["cobid"],
        "scale_factor":    wiz.get("scale_factor", 1.0),
    }
    for key in FILTER_KEYS:
        val = wiz.get(key)
        if val and str(val).strip():
            pj[key] = str(val).strip()
    return pj


def _fmt_money(v):
    """Compact money formatting (K/M/B/T) so big totals fit the ticket rows."""
    try:
        if v is None or pd.isna(v):
            return "—"
        n = float(v)
    except (TypeError, ValueError):
        return "—"
    a = abs(n)
    for div, suf in ((1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "K")):
        if a >= div:
            return f"{n / div:,.2f}{suf}"
    return f"{n:,.0f}"


# ══════════════════════════════════════════════════════════════════════════════
# VALIDATION
# ══════════════════════════════════════════════════════════════════════════════

def _upload_validation_ok() -> bool:
    """True when the uploaded CSV passed pre-validation. Unknown codes can be
    explicitly overridden (stored under key -1); missing REQUIRED columns
    never can. Scopes without a schema have nothing to validate."""
    _v = wiz.get("_upval")
    v = _v.get("result") if _v else None
    if v is None:
        return True
    if v["missing_required"]:
        return False
    if ((v["unknown_row_codes"] or v["unknown_measures"])
            and not wiz.get("_upval_override")):
        return False
    return True


def _completion_checks() -> list:
    """Ordered (label, done) pairs driving the ticket's completion checklist."""
    cat = wiz.get("category")
    checks = [("Category", bool(cat))]
    if not cat:
        return checks
    if cat == "VaR Upload":
        checks += [
            ("Data scope",          bool(wiz.get("process_type"))),
            ("CSV data",            wiz.get("uploaded_df") is not None),
            ("CSV validated",       wiz.get("uploaded_df") is not None
                                    and _upload_validation_ok()),
            ("COB date",            bool(wiz.get("cobid"))),
            ("Entity code",         bool((wiz.get("entity_code") or "").strip())),
            ("Reference",           bool((wiz.get("global_reference") or "").strip())),
            ("Adjustment Category", bool((wiz.get("adjustment_category") or "").strip())),
            ("Reason",              bool((wiz.get("reason") or "").strip())),
        ]
    elif cat == "Direct Adjustment":
        _n_valid = sum(1 for v in (wiz.get("direct_verdicts") or []) if v["IS_VALID"])
        checks += [
            ("Data scope",          bool(wiz.get("process_type"))),
            ("CSV parsed",          wiz.get("direct_ndf") is not None
                                    and len(wiz["direct_ndf"]) > 0),
            ("At least 1 valid row", _n_valid > 0),
            ("COB date",            bool(wiz.get("cobid"))),
            ("Adjustment Category", bool((wiz.get("adjustment_category") or "").strip())),
            ("Reason",              bool((wiz.get("reason") or "").strip())),
        ]
    elif cat == "Entity Roll":
        checks += [
            ("Data scope",          bool(wiz.get("process_type"))),
            ("Target COB",          bool(wiz.get("cobid"))),
            ("Source COB",          bool(wiz.get("source_cobid"))),
            ("Entity code",         bool((wiz.get("entity_code") or "").strip())),
            ("Adjustment Category", bool((wiz.get("adjustment_category") or "").strip())),
            ("Reason",              bool((wiz.get("reason") or "").strip())),
        ]
        if wiz.get("cobid") and wiz.get("source_cobid") \
                and wiz["cobid"] == wiz["source_cobid"]:
            checks.append(("Source COB differs from target", False))
    else:  # Scaling
        checks += [
            ("Data scope",      bool(wiz.get("process_type"))),
            ("Adjustment type", bool(wiz.get("adjustment_type"))),
            ("COB date",        bool(wiz.get("cobid"))),
        ]
        if wiz.get("adjustment_type") == "Roll":
            checks.append(("Source COB", bool(wiz.get("source_cobid"))))
        if wiz.get("occurrence") == "RECURRING":
            checks += [("Start COBID", bool(wiz.get("recurring_start_cobid"))),
                       ("End COBID",   bool(wiz.get("recurring_end_cobid")))]
        _has_var_comp = wiz.get("process_type") == "VaR" and (
            bool((wiz.get("var_component_name") or "").strip())
            or bool((wiz.get("var_sub_component_name") or "").strip()))
        _narrow_label = ("Department, Book or VaR Component"
                         if wiz.get("process_type") == "VaR"
                         else "Department or Book code")
        checks += [
            ("Entity code", bool((wiz.get("entity_code") or "").strip())),
            (_narrow_label,
             bool((wiz.get("department_code") or "").strip())
             or bool((wiz.get("book_code") or "").strip())
             or _has_var_comp),
            ("Adjustment Category", bool((wiz.get("adjustment_category") or "").strip())),
            ("Reason", bool((wiz.get("reason") or "").strip())),
        ]
    return checks


def _missing_fields() -> list:
    return [label for label, done in _completion_checks() if not done]


# ══════════════════════════════════════════════════════════════════════════════
# LEFT COLUMN — FORM SECTIONS
# ══════════════════════════════════════════════════════════════════════════════

def _render_scope_pills(include_frtball: bool = True) -> None:
    """Scope pill buttons + FRTB sub-type row. Sets wiz['process_type']."""
    current_group = "FRTB" if wiz.get("process_type") in FRTB_SUBTYPES \
                    else wiz.get("process_type")
    clicked = _pill_row(list(SCOPE_CONFIG.keys()), current_group,
                        f"scope_{wiz.get('category')}", icons=SCOPE_BTN_ICONS)
    if clicked and clicked != current_group:
        wiz["process_type"] = clicked  # FRTB group starts on the plain FRTB sub-type
        wiz["_preview_sum"] = None
        safe_rerun()

    if wiz.get("process_type") in FRTB_SUBTYPES:
        subtypes = [k for k in FRTB_SUBTYPE_CONFIG
                    if include_frtball or k != "FRTBALL"]
        sub = _pill_row(subtypes, wiz["process_type"],
                        f"frtbsub_{wiz.get('category')}")
        if sub and sub != wiz["process_type"]:
            wiz["process_type"] = sub
            wiz["_preview_sum"] = None
            safe_rerun()
        st.caption(FRTB_SUBTYPE_CONFIG.get(wiz["process_type"], ""))


# ── Reference-data dropdowns (entity / department / book) ───────────────────
# Codes are validated against DIMENSION.ENTITY and DIMENSION.BOOK so users pick
# from real values instead of free-typing (and Book is filtered by Department).
# Lists are loaded once per session; if a dimension can't be read, the field
# falls back to free text so the form still works.
def _ref_rows(sql: str, cache_key: str):
    if cache_key not in st.session_state:
        try:
            st.session_state[cache_key] = run_query(sql)
        except Exception:
            st.session_state[cache_key] = []
    return st.session_state[cache_key]


def _entity_options():
    rows = _ref_rows(
        "SELECT DISTINCT ENTITY_CODE FROM DIMENSION.ENTITY "
        "WHERE ENTITY_CODE IS NOT NULL ORDER BY ENTITY_CODE", "_ref_entities")
    return [str(r[0]) for r in rows if r[0] is not None]


def _category_options():
    rows = _ref_rows(
        "SELECT CATEGORY_NAME FROM ADJUSTMENT_APP.ADJ_CATEGORY "
        "WHERE IS_ACTIVE = TRUE ORDER BY SORT_ORDER, CATEGORY_NAME", "_ref_categories")
    return [str(r[0]) for r in rows if r[0] is not None]


def _book_dept_rows():
    return _ref_rows(
        "SELECT DISTINCT BOOK_CODE, DEPARTMENT_CODE, ENTITY_CODE "
        "FROM DIMENSION.BOOK WHERE BOOK_CODE IS NOT NULL", "_ref_books_v2")


def _dept_options(entity=None):
    entity = (entity or "").strip()
    return sorted({str(r[1]) for r in _book_dept_rows()
                   if r[1] is not None
                   and (not entity or str(r[2]) == entity)})


def _book_options(dept, entity=None):
    dept = (dept or "").strip()
    entity = (entity or "").strip()
    return sorted({str(r[0]) for r in _book_dept_rows()
                   if r[0] is not None
                   and (not dept or str(r[1]) == dept)
                   and (not entity or str(r[2]) == entity)})


def _var_comp_rows():
    return _ref_rows(
        "SELECT DISTINCT VAR_COMPONENT_NAME, VAR_SUB_COMPONENT_NAME "
        "FROM DIMENSION.VAR_SUB_COMPONENT "
        "WHERE VAR_COMPONENT_NAME IS NOT NULL", "_ref_var_comps")


def _var_comp_options():
    return sorted({str(r[0]) for r in _var_comp_rows() if r[0] is not None})


def _var_sub_options(comp):
    comp = (comp or "").strip()
    return sorted({str(r[1]) for r in _var_comp_rows()
                   if r[1] is not None and (not comp or str(r[0]) == comp)})


def _sim_source_options():
    rows = _ref_rows(
        "SELECT DISTINCT SIMULATION_SOURCE FROM DIMENSION.STRESS_SIMULATION "
        "WHERE SIMULATION_SOURCE IS NOT NULL AND TRIM(SIMULATION_SOURCE) <> '' "
        "ORDER BY SIMULATION_SOURCE",
        "_ref_sim_sources")
    return sorted({str(r[0]).strip() for r in rows
                   if r[0] is not None and str(r[0]).strip()})


# ── Direct Adjustment: order-free CSV → stage → per-scope validation view ────
_DIRECT_STAGE_COLS = [
    "ENTITY_CODE", "SOURCE_SYSTEM_CODE", "DEPARTMENT_CODE", "BOOK_CODE",
    "TRADE_CODE", "TRADE_TYPOLOGY", "STRATEGY", "INSTRUMENT_CODE",
    "SIMULATION_NAME", "SIMULATION_SOURCE", "MEASURE_TYPE_CODE",
    "CURRENCY_CODE", "VALUE_USD",
    "TENOR_CODE", "CURVE_CODE", "PRODUCT_CATEGORY_ATTRIBUTES", "REASON",
]

_DIRECT_STAGE_CHUNK = 500  # rows per INSERT — keeps the generated SQL bounded


def _st_version():
    try:
        return tuple(int(p) for p in st.__version__.split(".")[:2])
    except Exception:
        return (0, 0)


# SiS's 1.26 front-end crashes the data_editor component (minified React
# errors, e.g. #186) when column_config is applied to an empty/dynamic
# editor — the rich configs are only safe on ≥ 1.29. Below that the grid
# renders plain (no in-cell dropdowns; the validation views still own
# correctness). The grid mode itself needs st.data_editor to exist at all.
_GRID_FULL_CONFIG = _st_version() >= (1, 29)
_GRID_AVAILABLE = hasattr(st, "data_editor")


def _direct_cell_blank(v) -> bool:
    """True when a staged/normalized Direct cell carries no real value —
    None, float NaN (pandas' empty-cell marker), or whitespace-only text.
    Shared by _stage_direct_batch (what gets written) and _direct_row_payload
    (what gets submitted) so a blank cell never round-trips as the string
    "nan"."""
    return v is None or (isinstance(v, float) and pd.isna(v)) or str(v).strip() == ""


def _sql_str_literal(v) -> str:
    """Snowflake string literal: backslashes doubled FIRST, then quotes —
    see _submit_one's docstring for why the order matters (Snowflake
    literals interpret \\n, \\t, etc., so a lone backslash before an escaped
    quote would misparse)."""
    return "'" + str(v).replace("\\", "\\\\").replace("'", "''") + "'"


def _accepted_columns(scope: str):
    """{ACCEPTED_NAME (upper): STAGE_COLUMN} + set of required stage columns
    + ordered canonical [(stage_col, display_order), ...] (DISPLAY_ORDER not
    NULL, ascending) — the grid/template column order."""
    rows = _ref_rows(
        f"SELECT ACCEPTED_NAME, STAGE_COLUMN, IS_REQUIRED, DISPLAY_ORDER "
        f"FROM ADJUSTMENT_APP.DIRECT_ACCEPTED_COLUMNS "
        f"WHERE UPPER(PROCESS_TYPE) = UPPER('{scope}') AND IS_ACTIVE = TRUE",
        f"_ref_direct_cols_v2_{scope}")
    alias_map = {str(r[0]).strip().upper(): str(r[1]).strip().upper() for r in rows}
    required  = {str(r[1]).strip().upper() for r in rows if r[2]}
    ordered_cols = sorted(
        ((str(r[1]).strip().upper(), r[3]) for r in rows if r[3] is not None),
        key=lambda t: t[1])
    return alias_map, required, ordered_cols


def _parse_direct_df(df, scope: str):
    """Map pasted columns to stage columns by header name (order/case-free).
    Returns (normalized_df, unknown_cols, missing_required)."""
    alias_map, required, _ = _accepted_columns(scope)
    out, seen, first_col_used = {}, set(), {}
    unknown = []
    for c in df.columns:
        key = str(c).strip().upper()
        tgt = alias_map.get(key)
        if tgt is None:
            unknown.append(str(c))
        elif tgt not in seen:
            seen.add(tgt)
            first_col_used[tgt] = str(c)
            out[tgt] = df[c]
        else:
            # duplicate CSV header mapping to an already-claimed stage column —
            # first one wins; surface the drop instead of silently discarding it.
            unknown.append(f"{c} (duplicate of {first_col_used[tgt]})")
    ndf = pd.DataFrame(out)
    missing_required = sorted(required - seen)
    return ndf, unknown, missing_required


def _stage_direct_batch(batch_id: str, ndf) -> int:
    """Write normalized rows to ADJ_DIRECT_STAGE. Returns row count.

    Escaping: backslashes doubled BEFORE quotes (Snowflake convention — see
    _sql_str_literal). Chunked into _DIRECT_STAGE_CHUNK-row INSERTs so a large
    paste doesn't generate one unbounded VALUES list."""
    user = _sql_str_literal(current_user_name() or "")
    batch_lit = _sql_str_literal(batch_id)
    values = []
    for i, (_, row) in enumerate(ndf.iterrows(), start=1):
        cells = []
        for col_name in _DIRECT_STAGE_COLS:
            v = row.get(col_name)
            if _direct_cell_blank(v):
                cells.append("NULL")
            elif col_name == "VALUE_USD" and not isinstance(v, str):
                # Grid cells arrive as numpy floats/ints, not str (unlike the
                # CSV path, which reads everything with dtype=str) — str() on
                # extreme magnitudes yields exponent notation ('1e+16',
                # '1e-05') that Snowflake's TRY_TO_NUMBER rejects, so format
                # as a fixed-point decimal instead. Two branches: normal
                # magnitudes (and exact zero) round to 6dp as usual; a
                # sub-cent-of-a-cent value (0 < |v| < 1e-6) would otherwise
                # silently become "0.000000" at 6dp, so it's staged at 15dp
                # instead — still no exponent, and TRY_TO_NUMBER(38,6)
                # downstream will round it to 0, which the validation view's
                # non-zero rule then correctly flags rather than the app
                # silently pretending it staged a real value.
                _fv = float(v)
                if 0 < abs(_fv) < 1e-6:
                    cells.append(_sql_str_literal(f"{_fv:.15f}"))
                else:
                    cells.append(_sql_str_literal(f"{_fv:.6f}"))
            else:
                cells.append(_sql_str_literal(str(v).strip()))
        values.append(f"({batch_lit}, {i}, {', '.join(cells)}, {user})")
    if not values:
        return 0
    cols_sql = f"(BATCH_ID, ROW_NUM, {', '.join(_DIRECT_STAGE_COLS)}, USERNAME)"
    for start in range(0, len(values), _DIRECT_STAGE_CHUNK):
        chunk = values[start:start + _DIRECT_STAGE_CHUNK]
        run_query(
            f"INSERT INTO ADJUSTMENT_APP.ADJ_DIRECT_STAGE {cols_sql} "
            f"VALUES {', '.join(chunk)}")
    return len(values)


def _direct_validation(batch_id: str, scope: str):
    """Per-row verdicts from the scope's validation view."""
    view = f"ADJUSTMENT_APP.VW_DIRECT_VALIDATE_{scope.upper()}"
    return run_query(
        f"SELECT ROW_NUM, IS_VALID, VALIDATION_ERRORS FROM {view} "
        f"WHERE BATCH_ID = '{batch_id}' ORDER BY ROW_NUM")


def _delete_direct_batch(batch_id: str) -> None:
    run_query(f"DELETE FROM ADJUSTMENT_APP.ADJ_DIRECT_STAGE "
              f"WHERE BATCH_ID = '{batch_id}'")


def _sim_name_options(source=None):
    """Simulation names from DIMENSION.STRESS_SIMULATION, narrowed to the
    selected Simulation Source when one is set. Free-typed names that don't
    exist in the dimension match no fact rows and process as a silent no-op —
    hence a dropdown."""
    rows = _ref_rows(
        "SELECT DISTINCT STRESS_SIMULATION_NAME, SIMULATION_SOURCE "
        "FROM DIMENSION.STRESS_SIMULATION "
        "WHERE STRESS_SIMULATION_NAME IS NOT NULL "
        "AND TRIM(STRESS_SIMULATION_NAME) <> ''",
        "_ref_sim_names")
    src = (source or "").strip()
    return sorted({str(r[0]).strip() for r in rows
                   if r[0] is not None and str(r[0]).strip()
                   and (not src or (r[1] is not None and str(r[1]).strip() == src))})


def _measure_type_options(process_type=None):
    """Measure type codes from DIMENSION.MEASURE_TYPE — the engine matches
    filters against these exact codes (e.g. 'FRTBFXDelta', not 'FRTB Fx
    Delta'). For FRTB scopes the FRTB-prefixed codes are listed first."""
    rows = _ref_rows(
        "SELECT DISTINCT MEASURE_TYPE_CODE FROM DIMENSION.MEASURE_TYPE "
        "WHERE MEASURE_TYPE_CODE IS NOT NULL AND TRIM(MEASURE_TYPE_CODE) <> '' "
        "ORDER BY MEASURE_TYPE_CODE",
        "_ref_measure_types")
    codes = sorted({str(r[0]).strip() for r in rows
                    if r[0] is not None and str(r[0]).strip()})
    if (process_type or "").upper().startswith("FRTB"):
        codes = ([c for c in codes if c.upper().startswith("FRTB")]
                 + [c for c in codes if not c.upper().startswith("FRTB")])
    return codes


def _code_select(label, key, value, options, help=None, placeholder="— select —"):
    """Dropdown over reference codes; free-text fallback when none are available."""
    if not options:
        return st.text_input(label, value=value or "", key=key + "_txt",
                             help=help).strip()
    cur = (value or "").strip()
    opts = [""] + list(options)
    if cur and cur not in opts:          # keep an already-set value selectable
        opts.insert(1, cur)
    idx = opts.index(cur) if cur in opts else 0
    return st.selectbox(label, opts, index=idx, key=key, help=help,
                        format_func=lambda x: placeholder if x == "" else x)


_DAY_TYPE_LABELS = {"": "— both —", "1": "1 — 1-day VaR", "10": "10 — 10-day VaR"}


def _render_day_type(slot: str) -> None:
    """Day Type dropdown (VaR only) — shown in BOTH the main filter row and
    More filters. Each slot has its own widget key; both are driven from and
    write back wiz['day_type'], so they always mirror each other."""
    k = _k(f"day_type_{slot}")
    opts = list(_DAY_TYPE_LABELS.keys())
    cur = str(wiz.get("day_type") or "")
    st.session_state[k] = cur if cur in opts else ""

    def _sync():
        wiz["day_type"] = st.session_state.get(k) or None

    st.selectbox("Day Type", opts, key=k,
                 format_func=lambda v: _DAY_TYPE_LABELS.get(v, v),
                 on_change=_sync,
                 help="VaR horizon: 1 = 1-day VaR, 10 = 10-day VaR. "
                      "Blank applies the adjustment to both horizons.")


def _render_main_filters() -> None:
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        wiz["entity_code"] = _code_select(
            "Entity Code *", _k("entity_dd"), wiz.get("entity_code"),
            _entity_options(), placeholder="— select entity —")
    with c2:
        wiz["source_system_code"] = st.text_input("Source System", key=_k("src_sys"),
                                                  value=wiz.get("source_system_code") or "",
                                                  placeholder="e.g. MS")
    with c3:
        wiz["department_code"] = _code_select(
            "Department Code †", _k("dept_dd"), wiz.get("department_code"),
            _dept_options(wiz.get("entity_code")), placeholder="— any —",
            help="Filtered by the selected Entity")
    with c4:
        wiz["book_code"] = _code_select(
            "Book Code †", _k("book_dd"), wiz.get("book_code"),
            _book_options(wiz.get("department_code"), wiz.get("entity_code")),
            placeholder="— any —",
            help="Filtered by the selected Entity and Department")
    if wiz.get("process_type") == "VaR":
        c5, c6, c7, _ = st.columns(4)
        with c5:
            wiz["var_component_name"] = _code_select(
                "VaR Component †", _k("var_comp_dd"),
                wiz.get("var_component_name"),
                _var_comp_options(), placeholder="— any —")
        with c6:
            wiz["var_sub_component_name"] = _code_select(
                "VaR Sub-Component", _k("var_sub_dd"),
                wiz.get("var_sub_component_name"),
                _var_sub_options(wiz.get("var_component_name")),
                placeholder="— any —",
                help="Filtered by the selected VaR Component")
        with c7:
            _render_day_type("main")
        st.caption("Blank = all values for that dimension · "
                   "† at least one of Department, Book or VaR Component")
    else:
        st.caption("Blank = all values for that dimension · "
                   "† at least one of Department or Book")


def _render_extra_filters() -> None:
    """Scope-specific (tier 2) + rarely-used (tier 3) filters, collapsed."""
    pt = wiz.get("process_type", "")
    custom_fields = SCOPE_FIELDS.get(pt, [])
    applied = sum(1 for k in FILTER_KEYS
                  if k not in ("entity_code", "source_system_code",
                               "department_code", "book_code",
                               "var_component_name", "var_sub_component_name")
                  and (wiz.get(k) or "").strip())
    label = f"More filters ({applied} applied)" if applied else "More filters"

    def _filter_widget(fk, fl, ph):
        """Text input by default; reference-backed fields get a dropdown."""
        if fk == "simulation_source":
            wiz[fk] = _code_select(fl, _k(fk), wiz.get(fk),
                                   _sim_source_options(), placeholder="— any —")
        elif fk == "simulation_name":
            wiz[fk] = _code_select(fl, _k(fk), wiz.get(fk),
                                   _sim_name_options(wiz.get("simulation_source")),
                                   placeholder="— any —",
                                   help="Narrowed by Simulation Source when one is selected")
        elif fk == "measure_type_code":
            wiz[fk] = _code_select(fl, _k(fk), wiz.get(fk),
                                   _measure_type_options(wiz.get("process_type")),
                                   placeholder="— any —")
        else:
            wiz[fk] = st.text_input(fl, key=_k(fk),
                                    value=wiz.get(fk) or "", placeholder=ph)

    with st.expander(label, expanded=False):
        if custom_fields or pt == "VaR":
            st.markdown(
                f'<div style="font-size:0.72rem;font-weight:700;text-transform:uppercase;'
                f'letter-spacing:0.05em;color:{P["info"]};margin-bottom:0.2rem">'
                f'{pt} filters</div>', unsafe_allow_html=True)
            _n_scope = len(custom_fields) + (1 if pt == "VaR" else 0)
            ccols = st.columns(min(max(_n_scope, 1), 4))
            for i, (fk, fl, ph) in enumerate(custom_fields):
                with ccols[i % len(ccols)]:
                    _filter_widget(fk, fl, ph)
            if pt == "VaR":
                with ccols[len(custom_fields) % len(ccols)]:
                    _render_day_type("more")
        shown = {"entity_code", "source_system_code", "department_code", "book_code",
                 "var_component_name", "var_sub_component_name", "day_type"}
        shown.update(fk for fk, _, _ in custom_fields)
        extra = [(k, l, p) for k, l, p in ALL_EXTRA_FIELDS if k not in shown]
        ecols = st.columns(3)
        for i, (fk, fl, ph) in enumerate(extra):
            with ecols[i % 3]:
                _filter_widget(fk, fl, ph)


def render_scaling_form() -> None:
    with _card():
        _sec(2, "Data Scope", "Select the data scope for this adjustment.")
        _render_scope_pills()
    if not wiz.get("process_type"):
        st.info("Select a data scope to continue.")
        return

    # ── Type ─────────────────────────────────────────────────────────────
    with _card():
        _sec(3, "Adjustment Type", "How should the figures change?")
        tsel = _pill_row(list(TYPE_CONFIG.keys()), wiz.get("adjustment_type"),
                         "type", icons=TYPE_BTN_ICONS,
                         descs={k: v["desc"] for k, v in TYPE_CONFIG.items()})
        if tsel and tsel != wiz.get("adjustment_type"):
            wiz["adjustment_type"] = tsel
            wiz["_preview_sum"] = None
            safe_rerun()
        if wiz.get("adjustment_type"):
            st.caption(f"Formula: {TYPE_CONFIG[wiz['adjustment_type']]['formula']}")

    # ── Dates & factor ───────────────────────────────────────────────────
    with _card():
        _sec(4, "Date & Schedule", "When should this adjustment be applied?")
        occ = _pill_row(["ADHOC", "RECURRING"], wiz.get("occurrence", "ADHOC"),
                        "freq", icons=OCC_BTN_ICONS,
                        fmt=lambda k: "Ad hoc" if k == "ADHOC" else "Recurring (daily)",
                        descs={"ADHOC": "Apply once to a single COB",
                               "RECURRING": "Apply daily between start and end COB"})
        if occ and occ != wiz.get("occurrence"):
            wiz["occurrence"] = occ
            safe_rerun()

        _render_schedule_fields()

    # ── Filters ──────────────────────────────────────────────────────────
    with _card():
        _sec(5, "Dimension Filters", "Optional — blank means all values for that dimension.")
        _render_main_filters()
        _render_extra_filters()

    # ── Reason ───────────────────────────────────────────────────────────
    with _card():
        _sec(6, "Business Context", "Why is this adjustment needed?")
        wiz["adjustment_category"] = _code_select(
            "Adjustment Category *", _k("adj_category"),
            wiz.get("adjustment_category"), _category_options()) or None
        wiz["global_reference"] = st.text_input(
            "Reference", key=_k("scale_ref"),
            value=wiz.get("global_reference") or "",
            help="Optional free-text reference for this adjustment.").strip() or None
        wiz["reason"] = st.text_area("Reason / Business Justification *",
                                     value=wiz.get("reason", ""), height=70,
                                     key=_k("reason"))


def _render_schedule_fields() -> None:
    d1, d2, d3 = st.columns(3)
    with d1:
        wiz["cobid"] = _int_input(
            "First COB Date (YYYYMMDD) *" if wiz["occurrence"] == "RECURRING"
            else "COB Date (YYYYMMDD) *", "cobid", wiz.get("cobid"))
    with d2:
        if wiz.get("adjustment_type") == "Roll":
            wiz["source_cobid"] = _int_input("Source COB (roll from) *",
                                             "src_cobid", wiz.get("source_cobid"))
        elif wiz["occurrence"] == "RECURRING":
            wiz["recurring_start_cobid"] = _int_input("Start COBID *", "rec_start",
                                                      wiz.get("recurring_start_cobid"))
    with d3:
        if wiz.get("adjustment_type") in ("Scale", "Roll"):
            wiz["scale_factor"] = _float_input(
                "Scale Factor", "sf", wiz.get("scale_factor", 1.0),
                min_v=-10.0, max_v=100.0,
                help="1.05 = +5%,  0.95 = −5%")

    if wiz["occurrence"] == "RECURRING":
        r1, r2, _ = st.columns(3)
        if wiz.get("adjustment_type") == "Roll":
            with r1:
                wiz["recurring_start_cobid"] = _int_input("Start COBID *", "rec_start",
                                                          wiz.get("recurring_start_cobid"))
            with r2:
                wiz["recurring_end_cobid"] = _int_input("End COBID *", "rec_end",
                                                        wiz.get("recurring_end_cobid"))
        else:
            with r1:
                wiz["recurring_end_cobid"] = _int_input("End COBID *", "rec_end",
                                                        wiz.get("recurring_end_cobid"))


DIRECT_SCOPES = ["VaR", "Stress", "Sensitivity", "FRTB", "FRTBDRC", "FRTBRRAO"]
DIRECT_SCOPE_ICONS = {
    "Stress":      SCOPE_BTN_ICONS["Stress"],
    "Sensitivity": SCOPE_BTN_ICONS["Sensitivity"],
    "FRTB":        SCOPE_BTN_ICONS["FRTB"],
    "FRTBDRC":     SCOPE_BTN_ICONS["FRTB"],
    "FRTBRRAO":    SCOPE_BTN_ICONS["FRTB"],
}


def _fmt_direct_errors(raw) -> list:
    """VALIDATION_ERRORS comes back as a Snowflake ARRAY — normalize whatever
    shape the driver hands us (native list or a JSON-text string) to a
    list[str], defensively (mirrors _direct_schema's _j() helper)."""
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(x) for x in raw]
    try:
        parsed = json.loads(str(raw))
        return [str(x) for x in parsed] if isinstance(parsed, list) else [str(parsed)]
    except Exception:
        return [str(raw)]


def _purge_abandoned_direct_batches() -> None:
    """Best-effort cleanup: stage rows nobody submitted or re-parsed within 2
    days (e.g. a browser closed mid-paste) are abandoned and safe to drop."""
    try:
        run_query("DELETE FROM ADJUSTMENT_APP.ADJ_DIRECT_STAGE "
                  "WHERE CREATED_DATE < DATEADD('day', -2, CURRENT_TIMESTAMP())")
    except Exception:
        pass


def _render_direct_verdict_preview() -> None:
    """✓/✗ + Errors preview table (+ reject download) for whatever batch is
    currently staged in wiz['direct_ndf']/['direct_verdicts'] — identical
    rendering regardless of which input mode staged it, so CSV and grid look
    the same once rows are staged."""
    verdicts = wiz.get("direct_verdicts")
    ndf_staged = wiz.get("direct_ndf")
    if verdicts is not None and ndf_staged is not None and len(ndf_staged):
        # Row objects don't support .get() — normalize to plain dicts.
        v_by_row = {v["ROW_NUM"]: v.asDict() for v in verdicts}
        preview = ndf_staged.copy()
        preview.insert(0, "✓/✗", [
            "✓" if v_by_row.get(i + 1, {}).get("IS_VALID") else "✗"
            for i in range(len(preview))])
        preview["Errors"] = [
            "; ".join(_fmt_direct_errors(v_by_row.get(i + 1, {}).get("VALIDATION_ERRORS")))
            for i in range(len(preview))]
        n_valid = sum(1 for v in verdicts if v["IS_VALID"])
        n_bad   = len(verdicts) - n_valid
        if n_bad:
            st.warning(f"**{n_valid} valid row(s)**, **{n_bad} invalid row(s)** — "
                       f"invalid rows are excluded from submission (see the "
                       f"Errors column below).")
            bad_rows = preview[preview["✓/✗"] == "✗"]
            st.download_button(
                f"⬇ Download {len(bad_rows)} invalid row(s) (CSV)",
                bad_rows.to_csv(index=False).encode("utf-8-sig"),
                file_name="direct_adjustment_rejects.csv", mime="text/csv",
                key=_k("dadj_rejects_dl"))
        else:
            st.success(f"All {n_valid} row(s) validated — ready to submit.")
        render_df_table(preview, max_rows=20, height=220)


def _stage_and_validate(ndf, scope: str):
    """Drop any previously staged batch, stage ndf fresh, validate it. Shared
    by the CSV path and the grid path so both feed the exact same
    stage → validate → verdict-preview → submit chain. Returns
    (batch_id, verdicts)."""
    old_batch = wiz.get("direct_batch_id")
    if old_batch:
        try:
            _delete_direct_batch(old_batch)
        except Exception:
            pass
    _purge_abandoned_direct_batches()
    import uuid
    batch_id = str(uuid.uuid4())
    with st.spinner("Staging and validating rows…"):
        _stage_direct_batch(batch_id, ndf)
        verdicts = _direct_validation(batch_id, scope)
    return batch_id, verdicts


def render_direct_form() -> None:
    with _card():
        _sec(2, "Data Scope", "Select the data scope for this Direct Adjustment.")
        dsel = _pill_row(DIRECT_SCOPES, wiz.get("process_type"),
                         "direct_scope", icons=DIRECT_SCOPE_ICONS)
        if dsel and dsel != wiz.get("process_type"):
            old_batch = wiz.get("direct_batch_id")
            if old_batch:
                try:
                    _delete_direct_batch(old_batch)
                except Exception:
                    pass
            wiz["process_type"]    = dsel
            wiz["direct_batch_id"] = None
            wiz["direct_ndf"]      = None
            wiz["direct_verdicts"] = None
            wiz["_direct_sig"]     = None
            wiz["direct_grid_df"]  = None
            wiz["direct_grid_ver"] = wiz.get("direct_grid_ver", 0) + 1
            safe_rerun()
    if not wiz.get("process_type"):
        st.info("Select a data scope to continue.")
        return

    scope = wiz["process_type"]
    accepted_map, required, _ = _accepted_columns(scope)
    accepted_names = sorted(set(accepted_map.keys()))

    with _card():
        # Read the radio's widget state (if any) before instantiating it so the
        # heading/caption below can be mode-aware on the very same render —
        # defaults to the CSV wording until the widget has a value.
        _dadj_mode_key = _k("dadj_mode")
        _cur_in_mode = st.session_state.get(_dadj_mode_key, "Paste content")
        if _cur_in_mode == "Enter in grid":
            _sec(3, f"Grid Entry — {scope}",
                 "Add rows below — each valid row becomes its own adjustment.")
        else:
            _sec(3, f"CSV Data — {scope}",
                 "Paste or upload rows — one row = one adjustment. Column order and case don't matter.")
        if accepted_names:
            _info_banner('Provide a CSV of exact adjustment values, one row per '
                         'adjustment — paste the content or upload the file. '
                         'REQUIRED columns: <code>' + ', '.join(sorted(required)) +
                         '</code>. All accepted headers (any order, any case): '
                         '<code>' + ', '.join(accepted_names) + '</code>.')
        else:
            _info_banner(f'No accepted-columns configuration found for the '
                         f'<b>{scope}</b> scope yet — contact an administrator.')

        _in_mode = st.radio(
            "How do you want to provide the data?",
            ["Paste content", "Upload file", "Enter in grid"],
            horizontal=True, key=_dadj_mode_key,
            label_visibility="collapsed")

        # Switching input mode must not leave a validated batch from the
        # PREVIOUS mode submittable behind the newly-shown (unvalidated) UI —
        # e.g. a validated CSV batch must not still submit while the grid,
        # now empty, is on screen. Clear the staged batch + verdicts and
        # force a fresh grid widget (bump direct_grid_ver) whenever the mode
        # actually changes.
        if wiz.get("_direct_in_mode") is not None and wiz["_direct_in_mode"] != _in_mode:
            old_batch = wiz.get("direct_batch_id")
            if old_batch:
                try:
                    _delete_direct_batch(old_batch)
                except Exception:
                    pass
            wiz["direct_batch_id"] = None
            wiz["direct_ndf"]      = None
            wiz["direct_verdicts"] = None
            wiz["_direct_sig"]     = None
            wiz["direct_grid_df"]  = None
            wiz["direct_grid_ver"] = wiz.get("direct_grid_ver", 0) + 1
        wiz["_direct_in_mode"] = _in_mode

        if _in_mode in ("Paste content", "Upload file"):
            _tmpl_cols = [c for c, _ in _accepted_columns(scope)[2]]
            if _tmpl_cols:
                st.download_button(
                    "Download CSV template", data=",".join(_tmpl_cols) + "\n",
                    file_name=f"direct_{scope.lower()}_template.csv",
                    mime="text/csv", key=_k("direct_tmpl"))

            _DELIMS = {"Auto-detect": None, "Comma ( , )": ",", "Semicolon ( ; )": ";",
                       "Tab": "\t", "Pipe ( | )": "|"}
            delim_choice = st.selectbox(
                "Delimiter", list(_DELIMS.keys()), index=0, key=_k("dadj_delim"),
                help="Auto-detect works for most files; pick one explicitly if the "
                     "columns come out wrong.")

            def _read_csv(buf):
                """Parse with the chosen delimiter (or sniff it) — dtype=str so
                numeric-looking codes (e.g. trade/instrument codes) don't get
                silently coerced to float ('12345' → '12345.0')."""
                sep = _DELIMS[delim_choice]
                if sep is None:
                    return pd.read_csv(buf, sep=None, engine="python", dtype=str)
                return pd.read_csv(buf, sep=sep, dtype=str)

            df, _src_token, _parse_err = None, None, None
            if _in_mode == "Upload file":
                up_file = st.file_uploader(
                    "Upload CSV file", type=["csv", "txt"], key=_k("dadj_file"),
                    help="First row must be the header.")
                if up_file is not None:
                    try:
                        _raw = up_file.getvalue()
                        from io import StringIO
                        try:
                            _text = _raw.decode("utf-8-sig")
                        except UnicodeDecodeError:
                            _text = _raw.decode("latin-1")
                        df = _read_csv(StringIO(_text))
                        _src_token = f"file:{up_file.name}:{len(_raw)}:{delim_choice}"
                    except Exception as exc:
                        _parse_err = exc
            else:
                csv_text = st.text_area(
                    "Paste CSV Data Here", value="", height=160, key=_k("dadj_csv"),
                    help="Paste the full CSV content including the header row.")
                if csv_text.strip():
                    try:
                        from io import StringIO
                        df = _read_csv(StringIO(csv_text.strip()))
                        _src_token = f"paste:{hash(csv_text)}:{delim_choice}"
                    except Exception as exc:
                        _parse_err = exc

            if _parse_err is not None:
                # A parse failure must not leave a previously validated batch
                # submittable behind the error message (same pattern as the
                # grid's sig-mismatch branch — see "edited since last
                # validation" below).
                if wiz.get("direct_batch_id"):
                    try:
                        _delete_direct_batch(wiz["direct_batch_id"])
                    except Exception:
                        pass
                wiz["direct_batch_id"] = None
                wiz["direct_ndf"]      = None
                wiz["direct_verdicts"] = None
                wiz["_direct_sig"]     = None
                st.error(f"Failed to read the CSV: {_parse_err}. If the columns look "
                         f"wrong, try selecting the delimiter explicitly above.")
            elif df is not None:
                ndf, unknown_cols, missing_required = _parse_direct_df(df, scope)

                if unknown_cols:
                    st.warning("Column(s) not recognized for this scope (ignored): "
                               + ", ".join(unknown_cols))
                if missing_required:
                    st.error("Missing REQUIRED column(s): " + ", ".join(missing_required)
                             + " — staging is blocked until the CSV includes them.")

                if missing_required or not len(ndf):
                    # Can't stage this parse — drop any previously staged batch so a
                    # stale/valid batch is never left submittable behind a now-broken CSV.
                    if wiz.get("direct_batch_id"):
                        try:
                            _delete_direct_batch(wiz["direct_batch_id"])
                        except Exception:
                            pass
                    wiz["direct_batch_id"] = None
                    wiz["direct_ndf"]      = None
                    wiz["direct_verdicts"] = None
                    wiz["_direct_sig"]     = None
                else:
                    _sig = f'{scope}|{len(df)}|{",".join(map(str, df.columns))}|{_src_token}'
                    if wiz.get("_direct_sig") != _sig:
                        batch_id, verdicts = _stage_and_validate(ndf, scope)
                        wiz["direct_batch_id"] = batch_id
                        wiz["direct_ndf"]      = ndf
                        wiz["direct_verdicts"] = verdicts
                        wiz["_direct_sig"]     = _sig

                _render_direct_verdict_preview()
            elif wiz.get("direct_batch_id"):
                # No content currently shown (cleared paste box / removed
                # uploaded file) but a batch from a previous validation is
                # still on record — same invalidation as the parse-error and
                # missing-required-columns cases above.
                try:
                    _delete_direct_batch(wiz["direct_batch_id"])
                except Exception:
                    pass
                wiz["direct_batch_id"] = None
                wiz["direct_ndf"]      = None
                wiz["direct_verdicts"] = None
                wiz["_direct_sig"]     = None

        elif not _GRID_AVAILABLE:  # Enter in grid, but no data_editor in runtime
            st.info("Grid entry needs a newer Streamlit runtime — use "
                    "Paste CSV or Upload file (a template is available there).")
        else:  # Enter in grid
            ordered = _accepted_columns(scope)[2]     # [(stage_col, ord), ...]
            grid_cols = [c for c, _ in ordered]
            col_cfg = {}
            for c in grid_cols:
                if c == "VALUE_USD":
                    col_cfg[c] = st.column_config.NumberColumn("VALUE_USD (required)", format="%.6f")
                elif c == "ENTITY_CODE":
                    col_cfg[c] = st.column_config.SelectboxColumn("ENTITY_CODE (required)", options=_entity_options())
                elif c == "BOOK_CODE":
                    # Not a SelectboxColumn: the unscoped (no entity/dept
                    # filter) options list can be huge and would happily
                    # offer books that fail entity/department validation
                    # anyway — the validation view is the source of truth
                    # for correctness, this is just free text.
                    col_cfg[c] = st.column_config.TextColumn(
                        "BOOK_CODE", help="Validated against the entity's books")
                elif c == "MEASURE_TYPE_CODE":
                    col_cfg[c] = st.column_config.SelectboxColumn("MEASURE_TYPE_CODE", options=_measure_type_options(scope))
                elif c == "SIMULATION_NAME":
                    col_cfg[c] = st.column_config.SelectboxColumn("SIMULATION_NAME", options=_sim_name_options(None))
                elif c == "SIMULATION_SOURCE":
                    col_cfg[c] = st.column_config.SelectboxColumn("SIMULATION_SOURCE", options=_sim_source_options())
                elif c == "REASON":
                    col_cfg[c] = st.column_config.TextColumn("REASON", help="Blank = use the batch reason below")
                else:
                    col_cfg[c] = st.column_config.TextColumn(c)
            # Seed ONE blank row rather than a zero-row frame: the empty
            # dynamic editor is the combination most often behind SiS 1.26's
            # front-end (React) crashes, and a visible starter row is also
            # friendlier. Dtypes stay explicit: VALUE_USD float64 (NaN seed),
            # the rest object (None seed). The all-blank seed row is dropped
            # by the dropna(how="all") in the validate step.
            _seed_grid = pd.DataFrame(
                [{c: (float("nan") if c == "VALUE_USD" else None)
                  for c in grid_cols}])
            if not _GRID_FULL_CONFIG:
                # Rich column_config crashes the 1.26 front-end — render a
                # plain editor there; codes are still checked on Validate.
                col_cfg = None
                st.caption("This runtime shows plain cells (no in-cell "
                           "dropdowns) — values are checked when you "
                           "validate the rows.")
            gdf = st.data_editor(
                wiz.get("direct_grid_df") if wiz.get("direct_grid_df") is not None
                else _seed_grid,
                num_rows="dynamic", column_config=col_cfg,
                use_container_width=True,
                key=_k(f"direct_grid_{scope}_{wiz.get('direct_grid_ver', 0)}"))
            wiz["direct_grid_df"] = gdf

            _grid_sig = (f'grid|{scope}|{len(gdf)}|'
                         f'{pd.util.hash_pandas_object(gdf).sum() if len(gdf) else 0}')

            if st.button("Validate rows", key=_k("direct_grid_validate")):
                gndf = gdf.dropna(how="all").reset_index(drop=True)
                if not len(gndf):
                    if wiz.get("direct_batch_id"):
                        try:
                            _delete_direct_batch(wiz["direct_batch_id"])
                        except Exception:
                            pass
                    wiz["direct_batch_id"] = None
                    wiz["direct_ndf"]      = None
                    wiz["direct_verdicts"] = None
                    wiz["_direct_sig"]     = None
                    st.warning("Add at least one row to the grid before validating.")
                else:
                    batch_id, verdicts = _stage_and_validate(gndf, scope)
                    wiz["direct_batch_id"] = batch_id
                    wiz["direct_ndf"]      = gndf
                    wiz["direct_verdicts"] = verdicts
                    wiz["_direct_sig"]     = _grid_sig

            if wiz.get("direct_verdicts") is not None:
                if wiz.get("_direct_sig") == _grid_sig:
                    _render_direct_verdict_preview()
                else:
                    # Grid edited since the last validation — the staged
                    # batch/verdicts are for the PRE-EDIT snapshot and must
                    # not remain submittable (completion checks + _do_submit
                    # both key off direct_batch_id/direct_ndf/direct_verdicts
                    # alone, with no signature check of their own).
                    old_batch = wiz.get("direct_batch_id")
                    if old_batch:
                        try:
                            _delete_direct_batch(old_batch)
                        except Exception:
                            pass
                    wiz["direct_batch_id"] = None
                    wiz["direct_ndf"]      = None
                    wiz["direct_verdicts"] = None
                    wiz["_direct_sig"]     = None
                    st.info("The grid has changed since the last validation — "
                            "click **Validate rows** to refresh before submitting.")

    with _card():
        _sec(4, "Batch Details", "COB applies to every row submitted from this CSV.")
        wiz["cobid"] = _int_input("COB Date (YYYYMMDD) *", "dadj_cobid", wiz.get("cobid"))

    with _card():
        _sec(5, "Business Context", "Why is this adjustment needed?")
        wiz["adjustment_category"] = _code_select(
            "Adjustment Category *", _k("dadj_adj_category"),
            wiz.get("adjustment_category"), _category_options()) or None
        wiz["reason"] = st.text_area("Reason / Business Justification *",
                                     value=wiz.get("reason", ""), height=70,
                                     key=_k("dadj_reason"))


def render_var_upload_form() -> None:
    # VaR Upload is restricted to the VaR scope — no scope pills, pinned directly.
    wiz["process_type"] = "VaR"

    expected_cols = _direct_expected_columns(wiz["process_type"])
    _csv_card = _card()
    _csv_card.__enter__()
    _sec(2, f"CSV Upload — {wiz['process_type']}", "Paste exact adjustment values.")
    if expected_cols:
        _info_banner('Provide a CSV of exact adjustment values — paste the '
                     'content or upload the file. Expected columns: '
                     '<code>' + ', '.join(expected_cols) + '</code>.')
    else:
        _info_banner(f'No upload schema is configured for the <b>{wiz["process_type"]}</b> '
                     'scope yet. Provide a CSV (paste or upload); columns will '
                     'be stored as-is.')

    # ── Input mode: paste the content OR upload the file ─────────────────
    _in_mode = st.radio(
        "How do you want to provide the data?",
        ["Paste content", "Upload file"],
        horizontal=True, key=_k("direct_mode"),
        label_visibility="collapsed")

    _DELIMS = {"Auto-detect": None, "Comma ( , )": ",", "Semicolon ( ; )": ";",
               "Tab": "\t", "Pipe ( | )": "|"}
    delim_choice = st.selectbox(
        "Delimiter", list(_DELIMS.keys()), index=0, key=_k("direct_delim"),
        help="Auto-detect works for most files; pick one explicitly if the "
             "columns come out wrong.")

    def _read_csv(buf):
        """Parse with the chosen delimiter (or sniff it) — same for both modes."""
        sep = _DELIMS[delim_choice]
        if sep is None:
            return pd.read_csv(buf, sep=None, engine="python")
        return pd.read_csv(buf, sep=sep)

    df, _src_token, _parse_err = None, None, None
    if _in_mode == "Upload file":
        up_file = st.file_uploader(
            "Upload CSV file", type=["csv", "txt"], key=_k("direct_file"),
            help="First row must be the header.")
        if up_file is not None:
            try:
                _raw = up_file.getvalue()
                from io import StringIO
                try:
                    _text = _raw.decode("utf-8-sig")
                except UnicodeDecodeError:
                    _text = _raw.decode("latin-1")
                df = _read_csv(StringIO(_text))
                wiz["uploaded_file_name"] = up_file.name
                _src_token = f"file:{up_file.name}:{len(_raw)}:{delim_choice}"
            except Exception as exc:
                _parse_err = exc
    else:
        csv_text = st.text_area(
            "Paste CSV Data Here", value="", height=160, key=_k("direct_csv"),
            help="Paste the full CSV content including the header row.")
        if csv_text.strip():
            try:
                from io import StringIO
                df = _read_csv(StringIO(csv_text.strip()))
                wiz["uploaded_file_name"] = f"CSV_Pasted_{len(df)}_rows.csv"
                _src_token = f"paste:{hash(csv_text)}:{delim_choice}"
            except Exception as exc:
                _parse_err = exc

    if _parse_err is not None:
        st.error(f"Failed to read the CSV: {_parse_err}. If the columns look "
                 f"wrong, try selecting the delimiter explicitly above.")
    elif df is not None:
        if len(df.columns) == 1 and expected_cols and len(expected_cols) > 1:
            st.warning(f"Only ONE column was detected ({df.columns[0]}) but "
                       f"{len(expected_cols)} are expected — the delimiter is "
                       f"probably wrong. Pick it explicitly above.")
        wiz["uploaded_df"] = df

        extra_cols = [c for c in df.columns if c not in expected_cols]
        if extra_cols:
            st.info(f"Columns outside the expected schema (stored with the "
                    f"upload, not processed): {', '.join(extra_cols)}")

        # ── Pre-validation: catch what the engine would silently mangle ──
        schema = _direct_schema(wiz["process_type"])
        if schema["expected"] or schema["resolutions"]:
            _sig = (f'{wiz["process_type"]}|{len(df)}|'
                    f'{",".join(map(str, df.columns))}|{_src_token}')
            if (wiz.get("_upval") or {}).get("sig") != _sig:
                with st.spinner("Validating codes against reference data…"):
                    wiz["_upval"] = {
                        "sig": _sig,
                        "result": _validate_direct_upload(df, schema)}
            _render_upload_validation(wiz["_upval"]["result"], len(df))
        else:
            wiz["_upval"] = None

        render_df_table(df, max_rows=20, height=200)

        if "COBId" in df.columns and len(df):
            try:
                wiz["cobid"] = int(df["COBId"].iloc[0])
            except (TypeError, ValueError):
                pass
        if "EntityCode" in df.columns and len(df):
            wiz["entity_code"] = str(df["EntityCode"].iloc[0])

    _csv_card.__exit__(None, None, None)

    with _card():
        _sec(3, "Upload Details", "COB and entity are auto-detected from the CSV when present.")
        g1, g2, g3 = st.columns(3)
        with g1:
            wiz["cobid"] = _int_input("COB Date (auto-detected) *", "var_cobid",
                                      wiz.get("cobid"), placeholder="")
        with g2:
            ev = st.text_input("Entity Code (auto-detected) *", key=_k("var_entity"),
                               value=wiz.get("entity_code") or "")
            wiz["entity_code"] = ev.strip() or wiz.get("entity_code")
        with g3:
            rv = st.text_input("Reference *", key=_k("var_ref"),
                               value=wiz.get("global_reference") or "",
                               placeholder="e.g. CTN FX VaR",
                               help="Unique reference for this upload. Re-submitting the same "
                                    "COB + Reference replaces the previous adjustment.")
            wiz["global_reference"] = rv.strip() or None

    with _card():
        _sec(4, "Business Context", "Why is this adjustment needed?")
        wiz["adjustment_category"] = _code_select(
            "Adjustment Category *", _k("var_adj_category"),
            wiz.get("adjustment_category"), _category_options()) or None
        wiz["reason"] = st.text_area("Reason / Business Justification *",
                                     value=wiz.get("reason", ""), height=70,
                                     key=_k("var_reason"))

    # ── Duplicate reference check ────────────────────────────────────────
    if wiz.get("cobid") and wiz.get("global_reference"):
        try:
            dup_rows = run_query(f"""
                SELECT ADJ_ID, ENTITY_CODE, RUN_STATUS, USERNAME, CREATED_DATE, DIMENSION_ADJ_ID
                FROM ADJUSTMENT_APP.ADJ_HEADER
                WHERE COBID = {wiz['cobid']}
                  AND UPPER(GLOBAL_REFERENCE) = UPPER('{wiz["global_reference"].replace("'","''")}')
                  AND IS_DELETED = FALSE
                ORDER BY CREATED_DATE DESC
            """)
        except Exception:
            dup_rows = []
        if dup_rows:
            dup_info = dup_rows[0]
            st.markdown(
                f'<div style="background:{P["warning_lt"]};border:1px solid #FDE68A;border-radius:10px;'
                f'padding:1rem;margin:0.8rem 0">'
                f'<div style="font-weight:700;font-size:0.92rem;color:{P["warning"]};margin-bottom:0.4rem">'
                f'{icon("alert-triangle", size=14, color="#B45309")} Existing adjustment found with the same Reference</div>'
                f'<div style="font-size:0.83rem;color:{P["warning"]}">'
                f'<strong>Adj ID:</strong> {fmt_adj_id(dup_info[5])} &nbsp;·&nbsp; '
                f'<strong>Entity:</strong> {dup_info[1]} &nbsp;·&nbsp; '
                f'<strong>Status:</strong> {dup_info[2]} &nbsp;·&nbsp; '
                f'<strong>User:</strong> {dup_info[3]}<br/>'
                f'If you submit, the previous adjustment will be <strong>soft-deleted</strong> '
                f'and its data removed from the adjustment tables.</div></div>',
                unsafe_allow_html=True)
            wiz["_dup_adj_ids"] = [r[0] for r in dup_rows]
        else:
            wiz["_dup_adj_ids"] = []
    else:
        wiz["_dup_adj_ids"] = []


def render_entity_roll_form() -> None:
    st.markdown(
        f'<div style="background:{P["danger_lt"]};border:1px solid #FECACA;border-radius:10px;'
        f'padding:0.8rem 1rem;margin-bottom:0.6rem">'
        f'<div style="font-weight:700;font-size:0.9rem;color:{P["danger"]};margin-bottom:0.3rem">'
        f'{icon("alert-triangle", size=14, color="#B91C1C")} Entity Roll is destructive — approval required</div>'
        f'<div style="font-size:0.82rem;color:{P["danger"]}">'
        f'The entity\'s adjusted figures at the target COB are rebuilt from the source '
        f'COB. <strong>Any existing adjustments for this entity at the target COB are '
        f'permanently removed first</strong> (flagged deleted and their data deleted) — '
        f'this cannot be undone.</div></div>',
        unsafe_allow_html=True)

    with _card():
        _sec(2, "Data Scope", "Select the data scope to roll.")
        _render_scope_pills(include_frtball=False)
    if not wiz.get("process_type"):
        st.info("Select a scope to continue.")
        return

    with _card():
        _sec(3, "Roll Details", "Copy the full entity from the source COB to the target COB.")
        g1, g2, g3 = st.columns(3)
        with g1:
            wiz["cobid"] = _int_input("Target COB (YYYYMMDD) *", "er_cobid", wiz.get("cobid"))
        with g2:
            wiz["source_cobid"] = _int_input("Source COB (YYYYMMDD) *", "er_src_cobid",
                                             wiz.get("source_cobid"), placeholder="e.g. 20260327")
        with g3:
            wiz["entity_code"] = _code_select(
                "Entity Code *", _k("er_entity_dd"), wiz.get("entity_code"),
                _entity_options(), placeholder="— select entity —")

    # ── Wipe preview + reconciliation across header / dimension / fact ──────
    # Adjustments for an entity/COB can be loaded by external systems that
    # bypass ADJ_HEADER, so DIMENSION.ADJUSTMENT and the fact table can hold
    # more than ADJ_HEADER. Show all three counts and whether they match; the
    # roll removes every one of them.
    wiz["_eroll_remove_count"] = 0
    if wiz.get("cobid") and (wiz.get("entity_code") or "").strip() and wiz.get("process_type"):
        _cob = int(wiz["cobid"])
        _ent = wiz["entity_code"].strip().replace("'", "''")
        _pt  = wiz["process_type"].replace("'", "''")
        h_cnt = d_cnt = f_cnt = None
        try:
            _hd = run_query(f"""
                SELECT
                  (SELECT COUNT(*) FROM ADJUSTMENT_APP.ADJ_HEADER
                    WHERE COBID = {_cob} AND ENTITY_CODE = '{_ent}'
                      AND UPPER(PROCESS_TYPE) = UPPER('{_pt}') AND IS_DELETED = FALSE) AS H,
                  (SELECT COUNT(*) FROM DIMENSION.ADJUSTMENT
                    WHERE COBID = {_cob} AND ENTITY_CODE = '{_ent}'
                      AND UPPER(PROCESS_TYPE) = UPPER('{_pt}') AND IS_DELETED = FALSE) AS D
            """)
            if _hd:
                h_cnt, d_cnt = int(_hd[0][0]), int(_hd[0][1])
        except Exception:
            pass
        try:
            _ft = run_query(f"""SELECT ADJUSTMENTS_TABLE FROM ADJUSTMENT_APP.ADJUSTMENTS_SETTINGS
                                WHERE UPPER(PROCESS_TYPE) = UPPER('{_pt}')""")
            _fact_tbl = _ft[0][0] if _ft else None
            if _fact_tbl:
                _fc = run_query(f"""
                    SELECT COUNT(DISTINCT ADJUSTMENT_ID) FROM {_fact_tbl}
                    WHERE COBID = {_cob} AND ENTITY_CODE = '{_ent}'
                """)
                f_cnt = int(_fc[0][0]) if _fc else None
        except Exception:
            f_cnt = None
        wiz["_eroll_remove_count"] = max(d_cnt or 0, f_cnt or 0)
        if wiz["_eroll_remove_count"] > 0:
            _match = (d_cnt is not None and f_cnt is not None and d_cnt == f_cnt)
            _fact_txt = "n/a" if f_cnt is None else str(f_cnt)
            _recon = (f"sources — header {h_cnt} · dimension {d_cnt} · fact {_fact_txt}"
                      + ("&nbsp; · match ✓" if _match
                         else ("&nbsp; · ⚠ mismatch" if f_cnt is not None else "")))
            st.markdown(
                f'<div style="background:{P["danger_lt"]};border:1px solid #FECACA;border-radius:10px;'
                f'padding:0.8rem 1rem;margin:0.2rem 0 0.6rem">'
                f'<div style="font-weight:700;font-size:0.88rem;color:{P["danger"]};margin-bottom:0.25rem">'
                f'{icon("alert-triangle", size=14, color="#B91C1C")} '
                f'{wiz["_eroll_remove_count"]} existing adjustment(s) will be permanently removed</div>'
                f'<div style="font-size:0.82rem;color:{P["danger"]}">'
                f'Processing this roll deletes <strong>all</strong> adjustment data for '
                f'<strong>{wiz["entity_code"].strip()}</strong> at COB '
                f'<strong>{wiz["cobid"]}</strong> (including any loaded by other systems) '
                f'and rebuilds the entity from the source COB.<br/>'
                f'<span style="font-size:0.76rem;opacity:0.85">{_recon}</span>'
                f'</div></div>',
                unsafe_allow_html=True)

    with _card():
        _sec(4, "Business Context", "Why is this roll needed?")
        wiz["adjustment_category"] = _code_select(
            "Adjustment Category *", _k("er_adj_category"),
            wiz.get("adjustment_category"), _category_options()) or None
        wiz["global_reference"] = st.text_input(
            "Reference", key=_k("er_ref"),
            value=wiz.get("global_reference") or "",
            help="Optional free-text reference for this roll.").strip() or None
        wiz["reason"] = st.text_area("Reason / Business Justification *",
                                     value=wiz.get("reason", ""), height=70, key=_k("er_reason"),
                                     placeholder="e.g. Rolling MUSE VaR from previous business day")
    wiz["adjustment_type"]   = "EROL"
    wiz["requires_approval"] = True


# ══════════════════════════════════════════════════════════════════════════════
# RIGHT COLUMN — LIVE TICKET
# ══════════════════════════════════════════════════════════════════════════════

def _ticket_row(label: str, value, is_set=None) -> str:
    """One Ticket Summary row: set values bold red, unset show 'Not set'."""
    setflag = bool(value) if is_set is None else is_set
    cls  = "v-set" if setflag else "v-unset"
    disp = value if setflag else "Not set"
    return (f'<div class="kv"><span class="k">{label}</span>'
            f'<span class="v {cls}">{disp}</span></div>')


def _ticket_html(missing: list) -> str:
    cat = wiz.get("category")
    kv = _ticket_row("Category", cat)
    applied = []

    if cat == "Entity Roll":
        roll_set = bool(wiz.get("source_cobid") and wiz.get("cobid"))
        kv += _ticket_row("Scope",  wiz.get("process_type"))
        kv += _ticket_row("Entity", wiz.get("entity_code"))
        kv += _ticket_row("Roll",
                          f'{wiz.get("source_cobid")} → {wiz.get("cobid")}'
                          if roll_set else None, roll_set)
    elif cat == "VaR Upload":
        df_up = wiz.get("uploaded_df")
        kv += _ticket_row("Scope",     wiz.get("process_type"))
        kv += _ticket_row("COB",       wiz.get("cobid"))
        kv += _ticket_row("Reference", wiz.get("global_reference"))
        kv += _ticket_row("CSV rows",
                          f"{len(df_up):,}" if df_up is not None else None,
                          df_up is not None)
    elif cat == "Direct Adjustment":
        d_ndf      = wiz.get("direct_ndf")
        d_verdicts = wiz.get("direct_verdicts") or []
        n_valid    = sum(1 for v in d_verdicts if v["IS_VALID"])
        n_bad      = len(d_verdicts) - n_valid
        kv += _ticket_row("Scope", wiz.get("process_type"))
        kv += _ticket_row("COB",   wiz.get("cobid"))
        kv += _ticket_row("CSV rows",
                          f"{len(d_ndf):,}" if d_ndf is not None else None,
                          d_ndf is not None)
        kv += _ticket_row("Valid / Invalid",
                          f"{n_valid:,} valid / {n_bad:,} invalid" if d_verdicts else None,
                          bool(d_verdicts) and n_valid > 0)
    elif cat == "Scaling Adjustment":
        type_txt = wiz.get("adjustment_type")
        if type_txt and type_txt in ("Scale", "Roll"):
            type_txt += f' ×{wiz.get("scale_factor", 1.0):g}'
        cob_txt = wiz.get("cobid")
        if wiz.get("adjustment_type") == "Roll" and wiz.get("source_cobid") and cob_txt:
            cob_txt = f'{wiz.get("source_cobid")} → {cob_txt}'
        kv += _ticket_row("Scope", wiz.get("process_type"))
        kv += _ticket_row("Type",  type_txt)
        kv += _ticket_row("Schedule",
                          "Ad hoc" if wiz.get("occurrence", "ADHOC") == "ADHOC"
                          else "Recurring (daily)", True)
        kv += _ticket_row("COB", cob_txt)
        if wiz.get("occurrence") == "RECURRING":
            rec_set = bool(wiz.get("recurring_start_cobid") and wiz.get("recurring_end_cobid"))
            kv += _ticket_row("Recurring",
                              f'{wiz.get("recurring_start_cobid")} → '
                              f'{wiz.get("recurring_end_cobid")}' if rec_set else None,
                              rec_set)
        applied = [(k, str(wiz.get(k)).strip()) for k in FILTER_KEYS
                   if (wiz.get(k) or "") and str(wiz.get(k)).strip()]
        kv += _ticket_row("Filters",
                          f"{len(applied)} applied" if applied else "All records", True)

    if cat:
        kv += _ticket_row("Approval",
                          "Required" if wiz.get("requires_approval") else "Not required",
                          True)

    # Applied-filter chips (Scaling only) — mirrors the form selections
    if cat == "Scaling Adjustment":
        label_map = {"entity_code": "Entity", "source_system_code": "Source",
                     "department_code": "Dept", "book_code": "Book",
                     "var_component_name": "VaR Comp",
                     "var_sub_component_name": "VaR Sub",
                     "day_type": "Day Type"}
        label_map.update({k: l for k, l, _ in ALL_EXTRA_FIELDS})
        chips = "".join(
            f'<span class="filter-chip">{label_map.get(k, k)}: {v}</span>'
            for k, v in applied)
        if chips:
            kv += f'<div class="adj-filters" style="margin-top:6px">{chips}</div>'

    # ── Impact block ─────────────────────────────────────────────────────
    imp = ""
    if cat == "Scaling Adjustment" and _is_entity_only(wiz) and not missing:
        imp = (f'<div class="t-warn">{icon("alert-triangle", size=12, color="#B45309")} '
               f'<strong>Entity-wide adjustment</strong> — applies to every book and '
               f'department of {wiz.get("entity_code")}. Preview is skipped for '
               f'broad-scope adjustments; processing waits until the scope is idle.</div>')
    elif wiz.get("_preview_sum") is not None:
        s = wiz["_preview_sum"]
        stale = (wiz.get("_preview_for") != json.dumps(_preview_payload(), sort_keys=True,
                                                       default=str)) \
                if cat == "Scaling Adjustment" else False
        note = ('<div style="font-size:0.68rem;color:#B45309;margin-top:3px">'
                'Filters changed — re-run the preview.</div>') if stale else ""
        imp = (f'<div class="t-imp">'
               f'<div style="font-size:0.66rem;font-weight:700;text-transform:uppercase;'
               f'letter-spacing:.07em;color:{P["grey_700"]};margin-bottom:3px">Impact preview</div>'
               f'<div class="kv"><span class="k">Rows affected</span>'
               f'<span class="v">{_safe_int(s.get("ROWS_AFFECTED")):,}</span></div>'
               f'<div class="kv"><span class="k">Non-zero rows</span>'
               f'<span class="v">{_safe_int(s.get("NONZERO_ROWS")):,}</span></div>'
               f'<div class="kv"><span class="k">Original</span>'
               f'<span class="v">{_fmt_money(s.get("TOTAL_CURRENT_VALUE"))}</span></div>'
               f'<div class="kv"><span class="k">Adjustment</span>'
               f'<span class="v">{_fmt_money(s.get("TOTAL_ADJUSTMENT_DELTA"))}</span></div>'
               f'<div class="kv"><span class="k">Projected</span>'
               f'<span class="v">{_fmt_money(s.get("TOTAL_PROJECTED_VALUE"))}</span></div>'
               f'{note}</div>')

    # ── Completion progress ──────────────────────────────────────────────
    checks = _completion_checks()
    done   = sum(1 for _, ok in checks if ok)
    pct    = int(done / len(checks) * 100) if checks else 0
    items  = "".join(
        f'<div class="ck-item {"done" if ok else ""}">'
        f'<span class="ck-dot {"done" if ok else ""}">'
        f'{icon("check", size=9, color="white", valign="0") if ok else ""}</span>'
        f'{label}</div>'
        for label, ok in checks)
    prog = (f'<div class="t-prog">'
            f'<div class="prog-head">Completion progress<b>{pct}%</b></div>'
            f'<div class="prog-bar"><i style="width:{pct}%"></i></div>'
            f'{items}</div>')

    return (f'<div class="ticket">'
            f'<div class="t-head">{icon("file-text", size=14, color=P["primary"])} Ticket Summary</div>'
            f'<div class="t-body">{kv}{imp}{prog}</div>'
            f'</div>')


def _run_preview() -> None:
    """Run the summary-mode preview SP and stash the single aggregate row.

    "All FRTB" (FRTBALL) previews each real sub-type and sums the numeric
    totals — FRTBALL itself is not a previewable/processable scope."""
    payload = _preview_payload()
    subtypes = (["FRTB", "FRTBDRC", "FRTBRRAO"]
                if payload.get("process_type") == "FRTBALL"
                else [payload.get("process_type")])
    try:
        agg = None
        for sub in subtypes:
            df_sum = call_sp_df("ADJUSTMENT_APP.SP_PREVIEW_ADJUSTMENT",
                                json.dumps({**payload, "process_type": sub,
                                            "mode": "summary"}))
            if df_sum.empty or "ROWS_AFFECTED" not in df_sum.columns:
                msg_col = next((c for c in df_sum.columns if "MESSAGE" in c.upper()), None)
                wiz["_preview_err"] = (str(df_sum.iloc[0][msg_col]) if msg_col and not df_sum.empty
                                       else "Couldn't calculate a preview for these filters.")
                wiz["_preview_sum"] = None
                return
            row = df_sum.iloc[0].to_dict()
            if agg is None:
                agg = row
            else:
                for k, v in row.items():
                    try:
                        agg[k] = (agg.get(k) or 0) + (v or 0)
                    except TypeError:
                        pass    # non-numeric column — keep the first value
        wiz["_preview_sum"] = agg
        wiz["_preview_err"] = None
        wiz["_preview_for"] = json.dumps(payload, sort_keys=True, default=str)
    except Exception as exc:
        wiz["_preview_err"] = str(exc)
        wiz["_preview_sum"] = None


# ══════════════════════════════════════════════════════════════════════════════
# SIGN-OFF LIFECYCLE
# The first sign-off for a COB/scope comes from the upstream publish system
# (synced into ADJ_SIGNOFF_STATUS; SP_SUBMIT also checks the feed live).
# From here users can request a re-open (goes to the Approval Queue) and,
# once re-opened and done adjusting, sign the COB off again from the app.
# ══════════════════════════════════════════════════════════════════════════════

def _signoff_scopes() -> list:
    pt = wiz.get("process_type")
    if pt == "FRTBALL":
        return ["FRTB", "FRTBDRC", "FRTBRRAO"]
    return [pt] if pt else []


_SIGNOFF_BLOCKED = ("SIGNED_OFF", "REOPEN_REQUESTED")


def _signoff_feed_cfg():
    """Cached upstream-feed config from ADJ_APP_CONFIG."""
    if "_signoff_feed_cfg" not in st.session_state:
        cfg = {"enabled": True, "table": "BATCH.PUBLISH_SIGNOFF_STATUS"}
        try:
            rows = run_query("""
                SELECT CONFIG_KEY, CONFIG_VALUE FROM ADJUSTMENT_APP.ADJ_APP_CONFIG
                WHERE CONFIG_KEY IN ('SIGNOFF_FEED_TABLE', 'SIGNOFF_FEED_ENABLED')
            """)
            for r in rows or []:
                if str(r["CONFIG_KEY"]) == "SIGNOFF_FEED_TABLE" and r["CONFIG_VALUE"]:
                    cfg["table"] = str(r["CONFIG_VALUE"]).strip()
                if str(r["CONFIG_KEY"]) == "SIGNOFF_FEED_ENABLED":
                    cfg["enabled"] = (str(r["CONFIG_VALUE"] or "")
                                      .strip().lower() == "true")
        except Exception:
            pass
        st.session_state["_signoff_feed_cfg"] = cfg
    return st.session_state["_signoff_feed_cfg"]


def _signoff_state(scope, cobid, entity=None):
    """Sign-off lifecycle state for (scope, cobid, entity).

    Granularity is COB + ENTITY + scope; an app row with ENTITY_CODE '*'
    covers the whole scope. Returns None when open, else a dict with the
    governing row's status/metadata and 'entity' (None + 'blocked_entities'
    when a BROAD adjustment is blocked by several entity-level rows).
    App rows govern first (exact entity beats '*'); otherwise the unified
    upstream feed is checked live (an upstream FRTB row covers FRTBDRC and
    FRTBRRAO; SUB_TYPE NULL/'' or NonCVA counts; app REOPENED overrides)."""
    esc_scope = str(scope).replace("'", "''").upper()
    ent = str(entity).strip() if entity and str(entity).strip() else None
    try:
        rows = run_query(f"""
            SELECT ENTITY_CODE, SIGN_OFF_STATUS, SIGNOFF_SOURCE, SIGN_OFF_BY,
                   REOPEN_REQUESTED_BY, REOPEN_REQUESTED_AT, REOPEN_REASON,
                   REOPEN_APPROVED_BY, REOPEN_APPROVED_AT
            FROM ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS
            WHERE COBID = {int(cobid)} AND UPPER(PROCESS_TYPE) = '{esc_scope}'
        """) or []
    except Exception:
        return None    # can't read sign-off state — submit still enforces it

    def _mk(r):
        d = {k: r[k] for k in ("SIGNOFF_SOURCE", "SIGN_OFF_BY",
                               "REOPEN_REQUESTED_BY", "REOPEN_REQUESTED_AT",
                               "REOPEN_REASON", "REOPEN_APPROVED_BY",
                               "REOPEN_APPROVED_AT")}
        d["status"] = str(r["SIGN_OFF_STATUS"]).upper()
        d["entity"] = str(r["ENTITY_CODE"] or "*")
        return d

    app = {str(r["ENTITY_CODE"] or "*").upper(): r for r in rows}

    if ent is not None:
        gov = app.get(ent.upper())
        if gov is None or str(gov["SIGN_OFF_STATUS"]).upper() == "OPEN":
            gov = app.get("*")
        if gov is not None and str(gov["SIGN_OFF_STATUS"]).upper() != "OPEN":
            return _mk(gov)
    else:
        blocked = [r for r in rows
                   if str(r["SIGN_OFF_STATUS"]).upper() in _SIGNOFF_BLOCKED]
        if blocked:
            wc = next((r for r in blocked
                       if str(r["ENTITY_CODE"] or "*") == "*"), None)
            if wc is not None:
                return _mk(wc)
            d = _mk(blocked[0])
            d["entity"] = None
            d["blocked_entities"] = sorted(str(r["ENTITY_CODE"]) for r in blocked)
            return d

    # ── Upstream unified feed (live) ─────────────────────────────────────
    cfg = _signoff_feed_cfg()
    if not cfg["enabled"]:
        return None
    try:
        if esc_scope in ("FRTB", "FRTBDRC", "FRTBRRAO"):
            pt_match = f"UPPER(u.PROCESS_TYPE) IN ('FRTB', '{esc_scope}')"
        else:
            pt_match = f"UPPER(u.PROCESS_TYPE) = '{esc_scope}'"
        ent_pred = ("AND UPPER(u.ENTITY_CODE) = UPPER('"
                    + ent.replace("'", "''") + "')") if ent else ""
        up = run_query(f"""
            SELECT DISTINCT UPPER(TRIM(u.ENTITY_CODE)) AS E
            FROM {cfg['table']} u
            WHERE u.COBID = {int(cobid)} AND {pt_match}
              AND (u.SUB_TYPE IS NULL OR TRIM(u.SUB_TYPE) = ''
                   OR UPPER(u.SUB_TYPE) = 'NONCVA')
              AND UPPER(u.PUBLISH_STATUS) = 'SIGNEDOFF'
              {ent_pred}
              AND NOT EXISTS (
                  SELECT 1 FROM ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS a
                  WHERE a.COBID = u.COBID
                    AND UPPER(a.PROCESS_TYPE) = '{esc_scope}'
                    AND UPPER(a.SIGN_OFF_STATUS) = 'REOPENED'
                    AND (UPPER(a.ENTITY_CODE) = UPPER(TRIM(u.ENTITY_CODE))
                         OR a.ENTITY_CODE = '*')
              )
            LIMIT 20
        """)
        if up:
            ents = sorted({str(r["E"]) for r in up if r["E"]})
            d = {"status": "SIGNED_OFF", "SIGNOFF_SOURCE": "EXTERNAL",
                 "SIGN_OFF_BY": "EXTERNAL FEED",
                 "REOPEN_REQUESTED_BY": None, "REOPEN_REQUESTED_AT": None,
                 "REOPEN_REASON": None, "REOPEN_APPROVED_BY": None,
                 "REOPEN_APPROVED_AT": None,
                 "entity": ent if ent else (ents[0] if len(ents) == 1 else None)}
            if not ent and len(ents) > 1:
                d["blocked_entities"] = ents
            return d
    except Exception:
        pass    # feed not readable from the app — submit-side check still holds
    return None


def _signoff_history(scope, cobid, entity, old_status, new_status, comment):
    run_query(f"""
        INSERT INTO ADJUSTMENT_APP.ADJ_SIGNOFF_HISTORY
            (COBID, PROCESS_TYPE, ENTITY_CODE, OLD_STATUS, NEW_STATUS, ACTION_BY, COMMENT)
        VALUES ({int(cobid)}, '{str(scope).replace("'", "''")}',
                '{str(entity or '*').replace("'", "''")}',
                {"NULL" if old_status is None else "'" + str(old_status).replace("'", "''") + "'"},
                '{str(new_status).replace("'", "''")}',
                '{current_user_name().replace("'", "''")}',
                '{str(comment).replace("'", "''")}')
    """)


def _request_reopen(scope, cobid, entity, reason):
    """SIGNED_OFF → REOPEN_REQUESTED for one entity ('*' = whole scope).
    Creates the row when the sign-off only exists upstream so far."""
    esc_scope  = str(scope).replace("'", "''")
    esc_ent    = str(entity or "*").replace("'", "''")
    esc_reason = str(reason).replace("'", "''")[:490]
    usr        = current_user_name().replace("'", "''")
    run_query(f"""
        MERGE INTO ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS t
        USING (SELECT {int(cobid)} AS COBID) u
        ON t.COBID = u.COBID AND UPPER(t.PROCESS_TYPE) = '{esc_scope.upper()}'
           AND UPPER(t.ENTITY_CODE) = UPPER('{esc_ent}')
        WHEN MATCHED AND UPPER(t.SIGN_OFF_STATUS) = 'SIGNED_OFF' THEN UPDATE SET
            t.SIGN_OFF_STATUS     = 'REOPEN_REQUESTED',
            t.REOPEN_REQUESTED_BY = '{usr}',
            t.REOPEN_REQUESTED_AT = CURRENT_TIMESTAMP(),
            t.REOPEN_REASON       = '{esc_reason}',
            t.REOPEN_APPROVED_BY  = NULL,
            t.REOPEN_APPROVED_AT  = NULL,
            t.UPDATED_DATE        = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT
            (COBID, PROCESS_TYPE, ENTITY_CODE, SIGN_OFF_STATUS, SIGN_OFF_BY,
             SIGN_OFF_TIMESTAMP, SIGNOFF_SOURCE, REOPEN_REQUESTED_BY,
             REOPEN_REQUESTED_AT, REOPEN_REASON)
        VALUES
            (u.COBID, '{esc_scope}', '{esc_ent}', 'REOPEN_REQUESTED',
             'EXTERNAL FEED', CURRENT_TIMESTAMP(), 'EXTERNAL', '{usr}',
             CURRENT_TIMESTAMP(), '{esc_reason}')
    """)
    rows = run_query(f"""
        SELECT SIGN_OFF_STATUS, REOPEN_REQUESTED_BY
        FROM ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS
        WHERE COBID = {int(cobid)} AND UPPER(PROCESS_TYPE) = '{esc_scope.upper()}'
          AND UPPER(ENTITY_CODE) = UPPER('{esc_ent}')
    """)
    if (rows and str(rows[0]["SIGN_OFF_STATUS"]).upper() == "REOPEN_REQUESTED"
            and str(rows[0]["REOPEN_REQUESTED_BY"] or "") == current_user_name()):
        _signoff_history(scope, cobid, entity, "SIGNED_OFF", "REOPEN_REQUESTED",
                         f"Re-open requested: {reason}"[:990])
        # Best-effort: notify the scope's approvers (gated by the app's
        # notification switch; never blocks the request).
        try:
            _np = json.dumps({
                "process_type": scope,
                "cobid":        int(cobid),
                "requested_by": current_user_name(),
                "reason":       f"[entity {entity or '*'}] " + str(reason)[:280],
            }).replace("'", "''")
            run_query(f"CALL ADJUSTMENT_APP.SP_NOTIFY('reopen_requested', '{_np}')")
        except Exception:
            pass
        _ent_txt = "all entities" if (entity or "*") == "*" else entity
        return True, (f"Re-open request for COB {cobid} / {scope} ({_ent_txt}) "
                      f"submitted — an approver can action it on the Approval "
                      f"Queue page.")
    return False, ("The sign-off state changed before the request could be "
                   "recorded — check the current status below.")


def _resign_off(scope, cobid, entity):
    """REOPENED → SIGNED_OFF (source APP) for one entity ('*' = whole scope)."""
    esc_scope = str(scope).replace("'", "''")
    esc_ent   = str(entity or "*").replace("'", "''")
    usr       = current_user_name().replace("'", "''")
    rows = run_query(f"""
        UPDATE ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS
        SET SIGN_OFF_STATUS    = 'SIGNED_OFF',
            SIGN_OFF_BY        = '{usr}',
            SIGN_OFF_TIMESTAMP = CURRENT_TIMESTAMP(),
            SIGNOFF_SOURCE     = 'APP',
            UPDATED_DATE       = CURRENT_TIMESTAMP()
        WHERE COBID = {int(cobid)}
          AND UPPER(PROCESS_TYPE) = '{esc_scope.upper()}'
          AND UPPER(ENTITY_CODE) = UPPER('{esc_ent}')
          AND UPPER(SIGN_OFF_STATUS) = 'REOPENED'
    """)
    try:
        n = int(rows[0][0]) if rows else 0
    except (TypeError, ValueError, IndexError):
        n = 0
    if n == 0:
        return False, ("Nothing changed — this entity is no longer in a "
                       "re-opened state (someone may have signed it off already).")
    _signoff_history(scope, cobid, entity, "REOPENED", "SIGNED_OFF",
                     "Signed off from the app after re-open cycle")
    _ent_txt = "all entities" if (entity or "*") == "*" else entity
    return True, (f"COB {cobid} / {scope} ({_ent_txt}) signed off — new "
                  f"adjustments are blocked again.")


def _render_signoff_panel() -> bool:
    """Sign-off banner + actions for the selected scope/COB/entity. Returns
    True when submissions are currently blocked by sign-off."""
    cobid = wiz.get("cobid")
    scopes = _signoff_scopes()
    if not cobid or not scopes:
        return False
    entity = (wiz.get("entity_code") or "").strip() or None

    flash = st.session_state.pop("na_signoff_flash", None)
    if flash:
        (st.success if flash[0] == "success" else st.warning)(flash[1])

    blocked = False
    for scope in scopes:
        state = _signoff_state(scope, cobid, entity)
        if not state:
            continue
        status  = state["status"]
        gov_ent = state.get("entity")
        ent_txt = ("all entities" if gov_ent == "*"
                   else gov_ent if gov_ent else "several entities")
        label = f"COB {cobid} · {scope} · {ent_txt}"
        if status in _SIGNOFF_BLOCKED:
            blocked = True

        if status == "SIGNED_OFF" and state.get("blocked_entities"):
            # Broad adjustment blocked by several entity-level sign-offs —
            # there is no single row to act on from here.
            st.error(f"**{label} is signed off** for: "
                     f"{', '.join(state['blocked_entities'][:10])}. This "
                     f"adjustment has no entity filter, so it touches those "
                     f"entities and is blocked. Set the **Entity Code** field "
                     f"to work on one entity (and request its re-open), or "
                     f"manage sign-offs per entity on the COB Cockpit page.")
        elif status == "SIGNED_OFF":
            src = ("the upstream publish system"
                   if (state.get("SIGNOFF_SOURCE") or "EXTERNAL") == "EXTERNAL"
                   else f"{state.get('SIGN_OFF_BY') or 'the app'}")
            st.error(f"**{label} is signed off** (by {src}). "
                     f"New adjustments are blocked. If the business needs to "
                     f"adjust it, request a re-open below — it goes to the "
                     f"Approval Queue.")
            reason = st.text_input(
                "Reason for re-opening", key=_k(f"reopen_reason_{scope}"),
                placeholder="Why does this need to be re-opened?")
            if st.button(f"Request re-open — {scope} ({ent_txt})",
                         key=_k(f"reopen_btn_{scope}"), use_container_width=True,
                         disabled=not reason.strip()):
                try:
                    ok, msg = _request_reopen(scope, cobid, gov_ent,
                                              reason.strip())
                    st.session_state["na_signoff_flash"] = (
                        "success" if ok else "warning", msg)
                except Exception as ex:
                    st.session_state["na_signoff_flash"] = (
                        "warning", f"Could not submit the re-open request. "
                                   f"{friendly_error(ex)}")
                safe_rerun()
        elif status == "REOPEN_REQUESTED":
            st.warning(f"**{label}: re-open requested** by "
                       f"{state.get('REOPEN_REQUESTED_BY') or '—'} — awaiting "
                       f"approval on the Approval Queue page. Submissions stay "
                       f"blocked until it is approved.")
        elif status == "REOPENED":
            st.info(f"**{label} is re-opened** (approved by "
                    f"{state.get('REOPEN_APPROVED_BY') or '—'}). Submit the "
                    f"adjustments needed, then sign off again below.")
            confirm = st.checkbox(
                f"Confirm — all adjustments for {label} are done; sign off "
                f"and block new submissions again",
                key=_k(f"resign_confirm_{scope}"), value=False)
            if st.button(f"Sign off {scope} ({ent_txt}) again",
                         key=_k(f"resign_btn_{scope}"), use_container_width=True,
                         disabled=not confirm):
                try:
                    ok, msg = _resign_off(scope, cobid, gov_ent)
                    st.session_state["na_signoff_flash"] = (
                        "success" if ok else "warning", msg)
                except Exception as ex:
                    st.session_state["na_signoff_flash"] = (
                        "warning", f"Sign-off failed. {friendly_error(ex)}")
                safe_rerun()
    return blocked


# ══════════════════════════════════════════════════════════════════════════════
# PAGE
# ══════════════════════════════════════════════════════════════════════════════

# ── Success screen ────────────────────────────────────────────────────────────
if wiz["step"] == 3:
    result = wiz.get("result") or {}
    msg    = result.get("message", "Adjustment created successfully")

    # The SP references the blocking adjustment by its internal hash. The blocker
    # is still unprocessed (so it has no report number yet); users don't recognise
    # the big hash, so shorten it to a compact id for display.
    def _shorten_blocker(text):
        marker = "Blocked by ADJ #"
        i = text.find(marker)
        if i == -1:
            return text
        j = i + len(marker)
        k = j
        while k < len(text) and text[k] not in " .":
            k += 1
        ident = text[j:k]
        return text[:j] + ident[:8] + "…" + text[k:] if len(ident) > 8 else text

    msg = _shorten_blocker(msg)
    blocked_msg = msg if "Blocked by ADJ #" in msg else ""

    st.markdown(
        f'<div style="background:{P["success_lt"]};border:2px solid #BBF7D0;'
        f'border-radius:12px;padding:2.5rem;text-align:center;margin:1rem 0">'
        f'<div>{icon("check-circle", size=44, color=P["success"], valign="0")}</div>'
        f'<div style="font-size:1.4rem;font-weight:700;color:{P["success"]};margin-top:0.5rem">'
        f'Adjustment Submitted Successfully</div>'
        f'<div style="font-size:0.9rem;color:{P["success"]};margin-top:0.4rem">{msg}</div>'
        f'<div style="font-size:0.82rem;color:{P["info"]};margin-top:0.8rem">'
        f'Your adjustment is queued and will be processed automatically by the scope pipeline. '
        f'Track it on the Adjustment Pipeline page; once processed it is assigned a <strong>report ID</strong> '
        f'(the number shown in Adjustments, the Adjustment Pipeline, and your reports).</div>'
        f'</div>', unsafe_allow_html=True)

    if blocked_msg:
        st.markdown(
            f'<div style="background:{P["warning_lt"]};border:1px solid #FDE68A;border-radius:8px;'
            f'padding:0.75rem 1rem;margin-top:0.5rem;font-size:0.83rem;color:{P["warning"]}">'
            f'{icon("clock", size=13, color="#B45309")} <strong>Processing is queued behind another adjustment.</strong> '
            f'{blocked_msg} It will be picked up automatically once that run completes.</div>',
            unsafe_allow_html=True)

    st.markdown("<br/>", unsafe_allow_html=True)
    if st.button("Create Another Adjustment", use_container_width=True,
                 type="secondary", key="new_adj"):
        reset_wizard()
        safe_rerun()
    st.stop()


# ── Order ticket layout ───────────────────────────────────────────────────────
st.markdown("## New Adjustment")
st.markdown(
    f"<span style='color:{P['grey_700']};font-size:0.9rem'>"
    f"Create and submit an adjustment ticket.</span>", unsafe_allow_html=True)
st.markdown("<div style='height:0.4rem'></div>", unsafe_allow_html=True)

left, right = st.columns([1.85, 1], gap="large")

with left:
    with _card():
        _sec(1, "Category", "Select the adjustment category.")
        # Descriptions are NOT shown per-button (four captions crowd the row
        # for users who already know the categories) — only the selected
        # category's one-liner renders below the buttons.
        cat = _pill_row(list(CATEGORY_UI_DESCS.keys()), wiz.get("category"),
                        "cat", icons=CATEGORY_BTN_ICONS)
        if cat and cat != wiz.get("category"):
            if wiz.get("direct_batch_id"):  # leaving Direct Adjustment — drop its stage rows
                try:
                    _delete_direct_batch(wiz["direct_batch_id"])
                except Exception:
                    pass
            wiz.update({"category": cat, "process_type": None, "adjustment_type": None,
                        "uploaded_df": None, "uploaded_file_name": None,
                        "_preview_sum": None, "_preview_err": None,
                        "direct_batch_id": None, "direct_ndf": None,
                        "direct_verdicts": None, "_direct_sig": None,
                        "direct_grid_df": None,
                        "direct_grid_ver": wiz.get("direct_grid_ver", 0) + 1})
            safe_rerun()
        if wiz.get("category"):
            st.caption(CATEGORY_UI_DESCS.get(wiz["category"], ""))

    if not wiz.get("category"):
        st.info("Select an adjustment category to continue.")
    elif wiz["category"] == "Direct Adjustment":
        render_direct_form()
    elif wiz["category"] == "VaR Upload":
        render_var_upload_form()
    elif wiz["category"] == "Entity Roll":
        render_entity_roll_form()
    else:
        render_scaling_form()

missing = _missing_fields()

with right:
    st.markdown(_ticket_html(missing), unsafe_allow_html=True)

    cat = wiz.get("category")

    # ── Sign-off lifecycle (blocks submission while signed off) ─────────
    signoff_blocked = _render_signoff_panel()

    # ── Impact preview trigger (Scaling, narrow scope only) ─────────────
    zero_rows = False
    if cat == "Scaling Adjustment" and not missing and not _is_entity_only(wiz):
        if _btn("Run impact preview", icon_name=":material/visibility:",
                use_container_width=True, key=_k("run_preview")):
            with st.spinner("Calculating impact…"):
                _run_preview()
            safe_rerun()
        if wiz.get("_preview_err"):
            st.warning(f"Preview not available: {wiz['_preview_err']}")
        s = wiz.get("_preview_sum")
        preview_current = (s is not None and wiz.get("_preview_for")
                           == json.dumps(_preview_payload(), sort_keys=True, default=str))
        if preview_current and _safe_int(s.get("ROWS_AFFECTED")) == 0:
            zero_rows = True
            st.warning(
                "These filters match **0 rows**, so this adjustment would change "
                "nothing. One of the filter values probably doesn't exist for this "
                "COB — double-check entity, book, and measure-type codes. "
                "Submission is blocked until the preview finds matching rows.")

    # ── VaR Upload: replacement confirmation ──────────────────────────────
    dup_ok = True
    if cat == "VaR Upload" and wiz.get("_dup_adj_ids"):
        dup_ok = st.checkbox(
            f"Replace {len(wiz['_dup_adj_ids'])} existing adjustment(s) with this upload",
            key=_k("dup_confirm"), value=False)

    # ── Entity Roll: destructive-replace agreement ───────────────────────
    eroll_ok = True
    if cat == "Entity Roll":
        _n_sup = wiz.get("_eroll_remove_count") or 0
        _sup_txt = (f"{_n_sup} existing adjustment(s) " if _n_sup
                    else "all existing adjustments ")
        eroll_ok = st.checkbox(
            f"I understand this Entity Roll will permanently remove {_sup_txt}"
            f"for this entity at the target COB (including data from other systems).",
            key=_k("eroll_confirm"), value=False)

    # ── Previous submit error ─────────────────────────────────────────────
    _res = wiz.get("result") or {}
    if _res and not _is_submit_success(_res):
        _status = _res.get("status", "")
        _msg = _res.get("message", "Submission was not accepted.")
        st.error(f"Not submitted — {_status}: {_msg}" if _status and _status != "Error"
                 else _msg)

    # ── Submit ────────────────────────────────────────────────────────────
    if _btn("Submit Adjustment", icon_name=":material/send:", type="primary",
            use_container_width=True, key=_k("submit"),
            disabled=bool(missing) or not dup_ok or not eroll_ok or zero_rows
                     or signoff_blocked):
        wiz["result"] = None
        with st.spinner("Submitting adjustment…"):
            result = _do_submit()
        wiz["result"] = result
        wiz["step"]   = 3 if _is_submit_success(result) else 1
        safe_rerun()
    if missing:
        st.caption("Submit unlocks when the ticket is complete.")
    elif signoff_blocked:
        st.caption("Submit is blocked: this COB is signed off for the "
                   "selected scope (see the sign-off panel above).")
    elif zero_rows:
        st.caption("Submit is blocked: the current filters match no data.")

    # ── Approval flag (Entity Roll is always locked on) ───────────────────
    if cat == "Entity Roll":
        st.checkbox("Requires Approval", value=True, disabled=True,
                    key=_k("er_approval"),
                    help="Entity Roll always goes through the approval queue.")
    elif cat:
        wiz["requires_approval"] = st.checkbox(
            "Requires Approval", value=wiz.get("requires_approval", False),
            key=_k("approval"),
            help="Send this adjustment to the Approval Queue before processing.")


# ── Full-width preview detail (breakdown / sample) ───────────────────────────
# (Not for "All FRTB": the summary above is an aggregate over three sub-types;
#  per-row breakdown/sample only makes sense per sub-type.)
if wiz.get("category") == "Scaling Adjustment" and wiz.get("_preview_sum") \
        and wiz.get("process_type") != "FRTBALL" \
        and not missing and not _is_entity_only(wiz):
    s = wiz["_preview_sum"]
    total_rows = _safe_int(s.get("ROWS_AFFECTED"))
    _is_roll = (wiz.get("adjustment_type") == "Roll"
                and wiz.get("source_cobid")
                and int(wiz.get("source_cobid")) != int(wiz["cobid"]))
    if _is_roll:
        st.caption(
            "Roll preview shows the net impact: **current** = the target COB's "
            "original total (flattened), **projected** = the source COB's adjusted "
            "total rolled forward. Per-row breakdown is not shown for cross-COB rolls.")
    elif total_rows > 0:
        with st.expander("Breakdown by book / department / entity", expanded=False):
            try:
                df_grp = call_sp_df("ADJUSTMENT_APP.SP_PREVIEW_ADJUSTMENT",
                                    json.dumps({**_preview_payload(), "mode": "breakdown"}))
                if not df_grp.empty:
                    df_grp = df_grp.rename(columns={"CURRENT_VALUE": "Original",
                                                    "ADJUSTMENT_DELTA": "Adjustment",
                                                    "PROJECTED_VALUE": "Projected"})
                    render_df_table(df_grp, max_rows=100, height=300)
            except Exception as exc:
                st.warning(f"Breakdown not available: {exc}")
        with st.expander(f"Sample rows (up to 1,000 of {total_rows:,})", expanded=False):
            try:
                df_sample = call_sp_df("ADJUSTMENT_APP.SP_PREVIEW_ADJUSTMENT",
                                       json.dumps({**_preview_payload(), "mode": "sample"}))
                render_df_table(df_sample, max_rows=200, height=300)
            except Exception as exc:
                st.warning(f"Sample not available: {exc}")
