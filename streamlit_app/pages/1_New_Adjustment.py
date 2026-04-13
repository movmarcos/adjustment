"""
New Adjustment — 2-Step Wizard
================================
Step 1 : Category & Details
Step 2 : Preview & Submit
Step 3 : Success  (inline — no fragile switch_page after submit)

Root cause of "stuck after second submit":
  Streamlit widget state persists by key across reruns.  After reset_wizard()
  the text_input widgets (key="wiz_cobid", etc.) still hold their old values,
  which then overwrite the freshly-reset wiz dict on the very next render.
  Fix: _k() appends a version counter to every widget key; reset_wizard()
  bumps the counter so all widgets are created fresh.
"""
import streamlit as st
import json
import pandas as pd

st.set_page_config(
    page_title="New Adjustment · MUFG", page_icon="✏️",
    layout="wide", initial_sidebar_state="expanded",
)

from utils.styles import (
    inject_css, render_sidebar, render_step_bar, render_filter_chips,
    section_title, P, SCOPE_CONFIG, TYPE_CONFIG, CATEGORY_CONFIG,
)
from utils.snowflake_conn import run_query, call_sp_df, current_user_name, safe_rerun

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
    "var_component_id":       None,
    "var_sub_component_id":   None,
    "guaranteed_entity":      None,
    "region_key":             None,
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
    "requires_approval":      False,
    # VaR Upload
    "global_reference":       None,
    "uploaded_file_name":     None,
    "uploaded_df":            None,
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
            "process_type":          "VaR",
            "adjustment_type":       "Upload",
            "username":              current_user_name(),
            "source_cobid":          wiz["cobid"],
            "reason":                wiz.get("reason", ""),
            "entity_code":           wiz.get("entity_code", ""),
            "requires_approval":     wiz.get("requires_approval", False),
            "adjustment_occurrence": "ADHOC",
            "file_name":             wiz.get("uploaded_file_name", ""),
            "global_reference":      wiz.get("global_reference", ""),
        }

    if cat == "Entity Roll":
        return {
            "cobid":                 wiz["cobid"],
            "process_type":          wiz["process_type"],
            "adjustment_type":       "Entity_Roll",
            "username":              current_user_name(),
            "source_cobid":          wiz.get("source_cobid") or wiz["cobid"],
            "reason":                wiz.get("reason", ""),
            "entity_code":           wiz.get("entity_code", ""),
            "requires_approval":     True,
            "adjustment_occurrence": "ADHOC",
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
    if wiz.get("occurrence") == "RECURRING":
        payload["recurring_start_cobid"] = wiz.get("recurring_start_cobid")
        payload["recurring_end_cobid"]   = wiz.get("recurring_end_cobid")
    for key in ["entity_code", "source_system_code", "department_code",
                "book_code", "currency_code", "trade_typology",
                "strategy", "instrument_code", "simulation_name",
                "simulation_source", "measure_type_code", "trade_code",
                "trader_code", "var_component_id", "var_sub_component_id",
                "guaranteed_entity", "region_key", "scenario_date_id",
                "tenor_code", "underlying_tenor_code", "curve_code",
                "day_type", "product_category_attributes",
                "batch_region_area", "murex_family", "murex_group"]:
        val = wiz.get(key)
        if val and str(val).strip():
            payload[key] = str(val).strip()
    return payload


# ── VaR CSV column → Snowflake UNPIVOT name ──────────────────────────────────
_VAR_COL_MAP = {
    "AllVaR": "ALL_VAR", "AllVaRSkew": "ALL_VAR_SKEW",
    "BasisVaR": "BASIS_VAR", "BondAssetSpreadVaR": "BOND_ASSET_SPREAD_VAR",
    "CrossEffects": "CROSS_EFFECTS", "EquityPriceVaR": "EQUITY_PRICE_VAR",
    "EquityVegaVaR": "EQUITY_VEGA_VAR", "FXRateVaR": "FX_RATE_VAR",
    "FXVolatilityVaR": "FX_VOLATILITY_VAR", "IRCapVolVaR": "IR_CAP_VOL_VAR",
    "IRCapVolVaRSkew": "IR_CAP_VOL_VAR_SKEW", "IRSkewVolVaR": "IR_SKEW_VOL_VAR",
    "IRSwaptionVolVaR": "IR_SWAPTION_VOL_VAR",
    "IRSwaptionVolVaRSkew": "IR_SWAPTION_VOL_VAR_SKEW",
    "InflationRateCurveVaR": "INFLATION_RATE_CURVE_VAR",
    "InflationVolVaR": "INFLATION_VOL_VAR",
    "InterestRateCurveVaR": "INTEREST_RATE_CURVE_VAR",
    "InterestRateVegaVaR": "INTEREST_RATE_VEGA_VAR",
    "MTGSprdVaR": "MTG_SPRD_VAR", "OASVaR": "OAS_VAR",
    "ParCreditSpreadVaR": "PAR_CREDIT_SPREAD_VAR",
}


def _write_var_upload_line_items(adj_id: str, df_csv: pd.DataFrame) -> int:
    """UNPIVOT the VaR CSV and write line items to ADJ_LINE_ITEM.

    Each CSV row has 21 VaR measure columns. We melt them into individual
    rows with VAR_SUB_COMPONENT_ID resolved via DIMENSION.VAR_SUB_COMPONENT.
    Returns the number of line items written.
    """
    from utils.snowflake_conn import get_session
    session = get_session()

    # Resolve VAR_SUB_COMPONENT_NAME → VAR_SUB_COMPONENT_ID
    vsc_rows = session.sql("""
        SELECT VAR_SUB_COMPONENT_ID, UPPER(VAR_SUB_COMPONENT_NAME) AS NAME
        FROM DIMENSION.VAR_SUB_COMPONENT
    """).collect()
    vsc_map = {r["NAME"]: r["VAR_SUB_COMPONENT_ID"] for r in vsc_rows}

    # Identify which VaR measure columns exist in the CSV
    present_measures = {csv_col: db_col for csv_col, db_col in _VAR_COL_MAP.items()
                        if csv_col in df_csv.columns}
    if not present_measures:
        return 0

    # Dimension columns from the CSV
    dim_map = {
        "COBId": "COBID", "EntityCode": "ENTITY_CODE",
        "SourceSystemCode": "SOURCE_SYSTEM_CODE", "BookCode": "BOOK_CODE",
        "CurrencyCode": "CURRENCY_CODE", "ScenarioDate": "SCENARIO_DATE_ID",
        "TradeCode": "TRADE_CODE", "Category": "CATEGORY", "Detail": "DETAIL",
    }

    rows = []
    for _, csv_row in df_csv.iterrows():
        base = {db_col: csv_row.get(csv_col) for csv_col, db_col in dim_map.items()
                if csv_col in df_csv.columns}
        base["ADJ_ID"] = adj_id

        for csv_col, db_col in present_measures.items():
            val = csv_row.get(csv_col)
            if pd.isna(val) or val == 0:
                continue
            row = dict(base)
            row["VAR_SUB_COMPONENT_ID"] = vsc_map.get(db_col.upper())
            row["ADJUSTMENT_VALUE"] = float(val)
            row["ADJUSTMENT_VALUE_IN_USD"] = float(val)
            row["IS_DELETED"] = False
            rows.append(row)

    if not rows:
        return 0

    df_lines = pd.DataFrame(rows)
    session.write_pandas(
        df_lines,
        table_name="ADJ_LINE_ITEM",
        schema="ADJUSTMENT_APP",
        auto_create_table=False,
    )
    return len(df_lines)


def _do_submit() -> dict:
    """Call SP_SUBMIT_ADJUSTMENT. Returns result dict (never raises)."""
    try:
        json_str = json.dumps(_build_payload()).replace("'", "\\'")
        rows = run_query(f"CALL ADJUSTMENT_APP.SP_SUBMIT_ADJUSTMENT('{json_str}')")
        if not rows:
            return {"status": "Error", "message": "No response from stored procedure"}
        raw = rows[0][0]
        result = json.loads(str(raw)) if isinstance(raw, str) else raw

        # For VaR Upload: write CSV data as line items to ADJ_LINE_ITEM
        if (wiz.get("category") == "VaR Upload"
                and result.get("status") != "Error"
                and wiz.get("uploaded_df") is not None):
            try:
                n = _write_var_upload_line_items(result["adj_id"], wiz["uploaded_df"])
                result["line_items"] = n
            except Exception as li_err:
                result["status"] = "Error"
                result["message"] = f"Header created but line items failed: {li_err}"

        return result
    except Exception as exc:
        return {"status": "Error", "message": str(exc)}




# ══════════════════════════════════════════════════════════════════════════════
# STEP 1 FORM RENDERERS
# ══════════════════════════════════════════════════════════════════════════════

def _info_banner(text: str) -> None:
    st.markdown(
        f'<div style="background:{P["info_lt"]};border:1px solid #90CAF9;'
        f'border-radius:8px;padding:0.7rem 1rem;margin-bottom:1rem;font-size:0.85rem">'
        f'{text}</div>', unsafe_allow_html=True)


def _missing_info(fields: list) -> None:
    if fields:
        st.info(f"Complete required fields: **{', '.join(fields)}**")


# ── VaR Upload ────────────────────────────────────────────────────────────────

VAR_MEASURE_COLS = [
    "AllVaR", "AllVaRSkew", "BasisVaR", "BondAssetSpreadVaR",
    "CrossEffects", "EquityPriceVaR", "EquityVegaVaR", "FXRateVaR",
    "FXVolatilityVaR", "IRCapVolVaR", "IRCapVolVaRSkew", "IRSkewVolVaR",
    "IRSwaptionVolVaR", "IRSwaptionVolVaRSkew", "InflationRateCurveVaR",
    "InflationVolVaR", "InterestRateCurveVaR", "InterestRateVegaVaR",
    "MTGSprdVaR", "OASVaR", "ParCreditSpreadVaR",
]
EXPECTED_VAR_COLS = [
    "COBId", "EntityCode", "SourceSystemCode", "BookCode", "CurrencyCode",
    "ScenarioDate", "TradeCode",
] + VAR_MEASURE_COLS + ["Category", "Detail"]


def render_var_upload_form() -> None:
    section_title("VaR Upload — CSV File", "📤")
    _info_banner(
        'Upload a VaR adjustment CSV containing the 21 VaR measure columns. '
        'Expected columns: <code>COBId, EntityCode, SourceSystemCode, BookCode, '
        'CurrencyCode, ScenarioDate, TradeCode, AllVaR … ParCreditSpreadVaR, '
        'Category, Detail</code>.')

    csv_text = st.text_area(
        "Paste VaR CSV Data Here", value="", height=180, key=_k("var_csv"),
        help="Paste the full CSV content including header row.")

    if csv_text.strip():
        try:
            from io import StringIO
            df = pd.read_csv(StringIO(csv_text.strip()))
            wiz["uploaded_file_name"] = f"CSV_Pasted_{len(df)}_rows.csv"
            wiz["uploaded_df"]        = df

            missing_cols = [c for c in EXPECTED_VAR_COLS if c not in df.columns]
            extra_cols   = [c for c in df.columns   if c not in EXPECTED_VAR_COLS]
            if missing_cols:
                st.warning(f"Missing expected columns: {', '.join(missing_cols)}")
            if extra_cols:
                st.info(f"Extra columns (will be ignored): {', '.join(extra_cols)}")

            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Rows",     f"{len(df):,}")
            m2.metric("Columns",  len(df.columns))
            if "EntityCode" in df.columns:
                m3.metric("Entities", df["EntityCode"].nunique())
            if "BookCode" in df.columns:
                m4.metric("Books", df["BookCode"].nunique())

            st.dataframe(df.head(20), use_container_width=True, height=200)

            if "COBId" in df.columns and len(df):
                wiz["cobid"] = int(df["COBId"].iloc[0])
            if "EntityCode" in df.columns and len(df):
                wiz["entity_code"] = str(df["EntityCode"].iloc[0])
        except Exception as exc:
            st.error(f"Failed to read CSV: {exc}")

    st.divider()
    g1, g2 = st.columns(2)
    with g1:
        cobid_val = st.text_input("COB Date (auto-detected)", key=_k("var_cobid"),
                                   value=str(wiz.get("cobid") or ""))
        if cobid_val.strip().isdigit():
            wiz["cobid"] = int(cobid_val.strip())
    with g2:
        entity_val = st.text_input("Entity Code (auto-detected)", key=_k("var_entity"),
                                    value=wiz.get("entity_code") or "")
        if entity_val.strip():
            wiz["entity_code"] = entity_val.strip()

    ref_val = st.text_input(
        "Reference *", key=_k("var_ref"),
        value=wiz.get("global_reference") or "",
        placeholder="e.g. CTN FX VaR",
        help="Unique reference for this upload. If you submit again with the same COB + Reference, "
             "the previous adjustment will be replaced.")
    if ref_val.strip():
        wiz["global_reference"] = ref_val.strip()

    wiz["reason"] = st.text_area(
        "Reason / Business Justification", value=wiz.get("reason", ""),
        height=60, key=_k("var_reason"))
    wiz["requires_approval"] = st.checkbox(
        "🔐 Requires Approval", value=wiz.get("requires_approval", False),
        key=_k("var_approval"))

    # ── Duplicate reference check ────────────────────────────────────────
    if wiz.get("cobid") and wiz.get("global_reference"):
        try:
            dup_rows = run_query(f"""
                SELECT ADJ_ID, ENTITY_CODE, RUN_STATUS, USERNAME, CREATED_DATE
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
                f'<div style="background:#FFF3E0;border:2px solid #FFB74D;border-radius:10px;'
                f'padding:1rem;margin:0.8rem 0">'
                f'<div style="font-weight:700;font-size:0.92rem;color:#E65100;margin-bottom:0.4rem">'
                f'⚠️ Existing adjustment found with the same Reference</div>'
                f'<div style="font-size:0.83rem;color:#BF360C">'
                f'<strong>ADJ ID:</strong> {dup_info[0]} &nbsp;·&nbsp; '
                f'<strong>Entity:</strong> {dup_info[1]} &nbsp;·&nbsp; '
                f'<strong>Status:</strong> {dup_info[2]} &nbsp;·&nbsp; '
                f'<strong>User:</strong> {dup_info[3]}<br/>'
                f'If you continue, the previous adjustment will be <strong>soft-deleted</strong> '
                f'and its data removed from the adjustment tables.</div></div>',
                unsafe_allow_html=True)
            wiz["_dup_adj_ids"] = [r[0] for r in dup_rows]
        else:
            wiz["_dup_adj_ids"] = []
    else:
        wiz["_dup_adj_ids"] = []

    st.markdown("<br/>", unsafe_allow_html=True)
    _checks = [
        ("CSV Data",     wiz.get("uploaded_df") is not None),
        ("COB Date",     bool(wiz.get("cobid"))),
        ("Entity Code",  bool(wiz.get("entity_code"))),
        ("Reference",    bool(wiz.get("global_reference"))),
    ]
    missing = [f for f, present in _checks if not present]
    if missing:
        _missing_info(missing)
    else:
        if wiz.get("_dup_adj_ids"):
            confirmed = st.checkbox(
                "I confirm I want to replace the existing adjustment(s) with this upload",
                key=_k("var_dup_confirm"), value=False)
        else:
            confirmed = True

        if confirmed:
            if st.button("Continue → Preview", type="primary",
                         use_container_width=True, key=_k("var_continue")):
                wiz["process_type"]    = "VaR"
                wiz["adjustment_type"] = "Upload"
                wiz["step"] = 2
                safe_rerun()


# ── Entity Roll ──────────────────────────────────────────────────────────────

def render_entity_roll_form() -> None:
    section_title("Entity Roll — Full Entity Copy", "🔄")

    st.markdown(
        f'<div style="background:#FFF3E0;border:2px solid #FFB74D;border-radius:10px;'
        f'padding:1rem;margin-bottom:1rem">'
        f'<div style="font-weight:700;font-size:0.95rem;color:#E65100;margin-bottom:0.4rem">'
        f'⚠️ Destructive Operation — Approval Required</div>'
        f'<div style="font-size:0.84rem;color:#BF360C">'
        f'This operation will <strong>delete all existing data</strong> for the target COB + Entity '
        f'in both the FACT table and FACT ADJUSTED table, then <strong>copy all data</strong> from '
        f'the source COB + Entity. No delta calculation is performed.<br/><br/>'
        f'All source adjustment records will be consolidated under a single new Adjustment ID.<br/><br/>'
        f'<strong>This adjustment always requires approval before processing.</strong>'
        f'</div></div>',
        unsafe_allow_html=True)

    # Scope selection
    section_title("Data Scope", "🔍")
    scope_cols = st.columns(len(SCOPE_CONFIG))
    for i, (sk, cfg) in enumerate(SCOPE_CONFIG.items()):
        with scope_cols[i]:
            is_sel = wiz.get("process_type") == sk
            st.markdown(
                f'<div style="background:{"#E3F2FD" if is_sel else P["white"]};'
                f'border:2px solid {P["primary"] if is_sel else P["border"]};'
                f'border-radius:10px;padding:0.6rem;text-align:center">'
                f'<div style="font-size:1.3rem">{cfg.get("icon","📊")}</div>'
                f'<div style="font-weight:600;font-size:0.82rem">{sk}</div></div>',
                unsafe_allow_html=True)
            if st.button(sk, key=_k(f"er_scope_{sk}"), use_container_width=True,
                         type="primary" if is_sel else "secondary"):
                wiz["process_type"] = sk
                safe_rerun()

    if not wiz.get("process_type"):
        st.info("👆 Select a scope to continue.")
        return

    st.divider()
    g1, g2, g3 = st.columns(3)
    with g1:
        cobid_val = st.text_input("Target COB Date (YYYYMMDD) *", key=_k("er_cobid"),
                                   value=str(wiz.get("cobid") or ""),
                                   placeholder="e.g. 20260328")
        if cobid_val.strip().isdigit():
            wiz["cobid"] = int(cobid_val.strip())
    with g2:
        src_val = st.text_input("Source COB Date (YYYYMMDD) *", key=_k("er_src_cobid"),
                                 value=str(wiz.get("source_cobid") or ""),
                                 placeholder="e.g. 20260327")
        if src_val.strip().isdigit():
            wiz["source_cobid"] = int(src_val.strip())
    with g3:
        wiz["entity_code"] = st.text_input(
            "Entity Code *", key=_k("er_entity"),
            value=wiz.get("entity_code") or "", placeholder="e.g. MUSE")

    st.divider()
    wiz["reason"] = st.text_area(
        "Reason / Business Justification *", value=wiz.get("reason", ""),
        height=60, key=_k("er_reason"), placeholder="e.g. Rolling MUSE VaR from previous business day")

    # Approval is always required — show locked checkbox
    st.checkbox("🔐 Requires Approval", value=True, disabled=True, key=_k("er_approval"))

    st.markdown("<br/>", unsafe_allow_html=True)
    missing = [f for f, v in [("Scope", wiz.get("process_type")),
                               ("Target COB", wiz.get("cobid")),
                               ("Source COB", wiz.get("source_cobid")),
                               ("Entity Code", (wiz.get("entity_code") or "").strip())] if not v]
    if wiz.get("cobid") and wiz.get("source_cobid") and wiz["cobid"] == wiz["source_cobid"]:
        st.error("Source COB and Target COB must be different for an Entity Roll.")
    elif missing:
        _missing_info(missing)
    else:
        if st.button("Continue → Preview", type="primary",
                     use_container_width=True, key=_k("er_continue")):
            wiz["adjustment_type"] = "Entity_Roll"
            wiz["requires_approval"] = True
            wiz["step"] = 2
            safe_rerun()


# ── Scaling Adjustment ────────────────────────────────────────────────────────

FRTB_SUBTYPES = ["FRTB", "FRTBDRC", "FRTBRRAO", "FRTBALL"]
FRTB_SUBTYPE_CONFIG = {
    "FRTB":     "Standard FRTB",
    "FRTBDRC":  "Default Risk Charge",
    "FRTBRRAO": "Residual Risk Add-On",
    "FRTBALL":  "All FRTB (combined)",
}


def render_scaling_form() -> None:
    # ── Scope cards ───────────────────────────────────────────────────────
    section_title("Data Scope", "🔍")
    scope_cols = st.columns(len(SCOPE_CONFIG))
    for i, (sk, cfg) in enumerate(SCOPE_CONFIG.items()):
        with scope_cols[i]:
            is_sel = (wiz["process_type"] in FRTB_SUBTYPES) if sk == "FRTB" \
                     else (wiz["process_type"] == sk)
            st.markdown(
                f'<div style="background:{cfg["bg"] if is_sel else P["white"]};'
                f'border:2px solid {P["primary"] if is_sel else P["border"]};'
                f'border-radius:10px;padding:0.6rem 0.4rem;text-align:center">'
                f'<div style="font-size:1.5rem">{cfg["icon"]}</div>'
                f'<div style="font-weight:700;font-size:0.8rem;margin-top:0.2rem">'
                f'{cfg["label"]}</div></div>', unsafe_allow_html=True)
            if st.button(f'{"✓ " if is_sel else ""}{cfg["label"]}',
                         key=_k(f"scope_{sk}"), use_container_width=True,
                         type="primary" if is_sel else "secondary"):
                wiz["process_type"] = sk
                safe_rerun()

    # FRTB sub-type selector
    if wiz["process_type"] in FRTB_SUBTYPES:
        st.markdown(
            f'<div style="background:{P["success_lt"]};border:1px solid #A5D6A7;'
            f'border-radius:8px;padding:0.5rem 1rem;margin:0.5rem 0 0.3rem;'
            f'font-size:0.82rem;color:{P["success"]}">'
            f'🏛️ <strong>FRTB selected</strong> — choose a sub-type</div>',
            unsafe_allow_html=True)
        sub_cols = st.columns(len(FRTB_SUBTYPE_CONFIG))
        for i, (stk, stdesc) in enumerate(FRTB_SUBTYPE_CONFIG.items()):
            with sub_cols[i]:
                is_sub = wiz["process_type"] == stk
                st.markdown(
                    f'<div style="background:{P["success_lt"] if is_sub else P["white"]};'
                    f'border:2px solid {P["success"] if is_sub else P["border"]};'
                    f'border-radius:8px;padding:0.5rem 0.3rem;text-align:center">'
                    f'<div style="font-weight:700;font-size:0.82rem">{stk}</div>'
                    f'<div style="font-size:0.68rem;color:{P["grey_700"]};margin-top:2px">'
                    f'{stdesc}</div></div>', unsafe_allow_html=True)
                if st.button(f'{"✓ " if is_sub else ""}{stk}',
                             key=_k(f"frtb_{stk}"), use_container_width=True,
                             type="primary" if is_sub else "secondary"):
                    wiz["process_type"] = stk
                    safe_rerun()

    st.divider()
    if not wiz["process_type"]:
        st.info("👆 Select a data scope to continue.")
        return

    # ── Adjustment Type  +  Date & Schedule ──────────────────────────────
    left_col, right_col = st.columns([3, 2], gap="large")

    with left_col:
        section_title("Adjustment Type", "⚡")
        type_cols = st.columns(len(TYPE_CONFIG))
        for i, (tk, tcfg) in enumerate(TYPE_CONFIG.items()):
            with type_cols[i]:
                is_sel = wiz["adjustment_type"] == tk
                st.markdown(
                    f'<div style="background:{"#FFF0F3" if is_sel else P["white"]};'
                    f'border:2px solid {P["primary"] if is_sel else P["border"]};'
                    f'border-radius:10px;padding:0.6rem 0.4rem;text-align:center">'
                    f'<div style="font-size:1.3rem">{tcfg["icon"]}</div>'
                    f'<div style="font-weight:700;font-size:0.78rem;margin:0.2rem 0">{tk}</div>'
                    f'<div style="font-size:0.68rem;color:{P["grey_700"]}">{tcfg["desc"]}</div>'
                    f'</div>', unsafe_allow_html=True)
                if st.button(f'{"✓ " if is_sel else ""}{tk}',
                             key=_k(f"type_{tk}"), use_container_width=True,
                             type="primary" if is_sel else "secondary"):
                    wiz["adjustment_type"] = tk
                    safe_rerun()

        if wiz["adjustment_type"]:
            st.markdown(
                f'<div style="background:{P["grey_100"]};border-radius:6px;'
                f'padding:0.5rem 0.8rem;font-size:0.78rem;color:{P["grey_700"]};'
                f'margin-top:0.5rem;font-family:monospace">'
                f'Formula: {TYPE_CONFIG[wiz["adjustment_type"]]["formula"]}</div>',
                unsafe_allow_html=True)

    with right_col:
        section_title("Date & Schedule", "📅")
        wiz["occurrence"] = st.radio(
            "Frequency", ["ADHOC", "RECURRING"],
            index=0 if wiz.get("occurrence", "ADHOC") == "ADHOC" else 1,
            horizontal=True, key=_k("freq"))

        if wiz["occurrence"] == "ADHOC":
            cobid_val = st.text_input(
                "COB Date (YYYYMMDD) *", key=_k("cobid"),
                value=str(wiz.get("cobid") or ""), placeholder="e.g. 20260328")
            if cobid_val.strip().isdigit():
                wiz["cobid"] = int(cobid_val.strip())
        else:
            st.info("Recurring adjustments repeat daily between start and end COB.")
            cobid_val = st.text_input(
                "First COB Date (YYYYMMDD) *", key=_k("cobid_r"),
                value=str(wiz.get("cobid") or ""), placeholder="e.g. 20260328")
            if cobid_val.strip().isdigit():
                wiz["cobid"] = int(cobid_val.strip())
            rc1, rc2 = st.columns(2)
            with rc1:
                rs = st.text_input("Start COBID *", key=_k("rec_start"),
                                    value=str(wiz.get("recurring_start_cobid") or ""))
                if rs.strip().isdigit():
                    wiz["recurring_start_cobid"] = int(rs.strip())
            with rc2:
                re_ = st.text_input("End COBID *", key=_k("rec_end"),
                                     value=str(wiz.get("recurring_end_cobid") or ""))
                if re_.strip().isdigit():
                    wiz["recurring_end_cobid"] = int(re_.strip())

        if wiz["adjustment_type"] == "Roll":
            src_val = st.text_input(
                "Source COB (roll from) *", key=_k("src_cobid"),
                value=str(wiz.get("source_cobid") or ""))
            if src_val.strip().isdigit():
                wiz["source_cobid"] = int(src_val.strip())

        if wiz["adjustment_type"] in ("Scale", "Roll"):
            section_title("Scale Factor", "🔧")
            wiz["scale_factor"] = st.number_input(
                "Scale Factor", value=float(wiz.get("scale_factor", 1.0)),
                min_value=-10.0, max_value=100.0, step=0.01, format="%.4f",
                help="1.05 = +5%,  0.95 = −5%", key=_k("sf"))

    st.divider()

    # ── Dimension Filters ────────────────────────────────────────────────
    # Fields are organized into 3 tiers:
    #   1. Main (always visible): common across all process types
    #   2. Scope-specific (visible when process type selected): varies by scope
    #   3. Additional (collapsed): rarely used, available if needed

    SCOPE_FIELDS = {
        "VaR": {
            "custom": [],
        },
        "Stress": {
            "custom": [
                ("simulation_name",   "Simulation Name",   "e.g. MRM_GLB_Std_EQ_M_PriceDnVolUp"),
                ("trade_typology",    "Trade Typology",    "e.g. EQTT"),
                ("instrument_code",   "Instrument Code",   "e.g. US4642872422 US"),
            ],
        },
        "Sensitivity": {
            "custom": [
                ("measure_type_code", "Measure Type Code", "e.g. FxDeltaExp"),
                ("strategy",          "Strategy",          "e.g. SSA00306"),
                ("trade_typology",    "Trade Typology",    "e.g. FEXF"),
                ("instrument_code",   "Instrument Code",   "e.g. US46090E1038 US"),
            ],
        },
        "FRTB": {
            "custom": [
                ("measure_type_code", "Measure Type Code", "e.g. FRTBCSRDelta"),
                ("instrument_code",   "Instrument Code",   "e.g. US92826C8394 US"),
                ("strategy",          "Strategy",          "e.g. SSU00332"),
            ],
        },
    }
    # FRTB sub-types share the same fields
    for fst in FRTB_SUBTYPES:
        if fst not in SCOPE_FIELDS:
            SCOPE_FIELDS[fst] = SCOPE_FIELDS["FRTB"]

    pt = wiz.get("process_type", "")

    section_title("Dimension Filters", "🎯")
    st.caption("Leave blank to include all values for that dimension.")

    # ── Tier 1: Main fields (always visible) ─────────────────────────────
    st.markdown(
        f'<div style="font-size:0.78rem;font-weight:700;text-transform:uppercase;'
        f'letter-spacing:0.05em;color:{P["primary"]};margin-bottom:0.3rem">'
        f'Main Filters</div>', unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        wiz["entity_code"]     = st.text_input("Entity Code",      key=_k("entity"),
                                                value=wiz.get("entity_code") or "",
                                                placeholder="e.g. MUSI")
    with c2:
        wiz["source_system_code"] = st.text_input("Source System",  key=_k("src_sys"),
                                                    value=wiz.get("source_system_code") or "",
                                                    placeholder="e.g. MS")
    with c3:
        wiz["department_code"] = st.text_input("Department Code",   key=_k("dept"),
                                                value=wiz.get("department_code") or "")
    with c4:
        wiz["book_code"]       = st.text_input("Book Code",         key=_k("book"),
                                                value=wiz.get("book_code") or "")

    # ── Tier 2: Scope-specific fields ────────────────────────────────────
    custom_fields = SCOPE_FIELDS.get(pt, {}).get("custom", [])
    if custom_fields:
        st.markdown(
            f'<div style="font-size:0.78rem;font-weight:700;text-transform:uppercase;'
            f'letter-spacing:0.05em;color:{P["info"]};margin:0.6rem 0 0.3rem">'
            f'{pt} Filters</div>', unsafe_allow_html=True)
        custom_cols = st.columns(min(len(custom_fields), 4))
        for i, (field_key, field_label, placeholder) in enumerate(custom_fields):
            with custom_cols[i % len(custom_cols)]:
                wiz[field_key] = st.text_input(field_label, key=_k(field_key),
                                                value=wiz.get(field_key) or "",
                                                placeholder=placeholder)

    # ── Tier 3: Additional fields (collapsed) ────────────────────────────
    # Collect keys already shown in tier 1 + tier 2
    shown_keys = {"entity_code", "source_system_code", "department_code", "book_code"}
    shown_keys.update(fk for fk, _, _ in custom_fields)

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
        ("var_component_id",             "VaR Component ID",             ""),
        ("var_sub_component_id",         "VaR Sub-Component ID",         ""),
        ("guaranteed_entity",            "Guaranteed Entity",            ""),
        ("region_key",                   "Region Key",                   ""),
        ("scenario_date_id",             "Scenario Date ID",             ""),
        ("tenor_code",                   "Tenor Code",                   ""),
        ("underlying_tenor_code",        "Underlying Tenor Code",        ""),
        ("curve_code",                   "Curve Code",                   ""),
        ("day_type",                     "Day Type",                     ""),
        ("product_category_attributes",  "Product Category Attributes",  ""),
        ("batch_region_area",            "Batch Region Area",            ""),
        ("murex_family",                 "Murex Family",                 ""),
        ("murex_group",                  "Murex Group",                  ""),
    ]
    extra_fields = [(k, l, p) for k, l, p in ALL_EXTRA_FIELDS if k not in shown_keys]

    if extra_fields:
        with st.expander("Additional Filters", expanded=False):
            extra_cols = st.columns(3)
            for i, (field_key, field_label, placeholder) in enumerate(extra_fields):
                with extra_cols[i % 3]:
                    wiz[field_key] = st.text_input(field_label, key=_k(field_key),
                                                    value=wiz.get(field_key) or "",
                                                    placeholder=placeholder)

    st.divider()

    # ── Business Context ─────────────────────────────────────────────────
    wiz["reason"] = st.text_area(
        "Reason / Business Justification", value=wiz.get("reason", ""),
        height=80, key=_k("reason"))
    wiz["requires_approval"] = st.checkbox(
        "🔐 Requires Approval", value=wiz.get("requires_approval", False),
        key=_k("approval"))

    st.markdown("<br/>", unsafe_allow_html=True)
    missing = [f for f, v in [("Adjustment Type", wiz.get("adjustment_type")),
                               ("COB Date",        wiz.get("cobid"))] if not v]
    if wiz.get("occurrence") == "RECURRING":
        for f, v in [("Start COBID", wiz.get("recurring_start_cobid")),
                     ("End COBID",   wiz.get("recurring_end_cobid"))]:
            if not v:
                missing.append(f)

    if missing:
        _missing_info(missing)
    else:
        if st.button("Continue → Preview", type="primary",
                     use_container_width=True, key=_k("scale_continue")):
            wiz["step"] = 2
            safe_rerun()


# ══════════════════════════════════════════════════════════════════════════════
# PAGE LAYOUT — STEP BAR + ROUTER
# ══════════════════════════════════════════════════════════════════════════════

def _is_entity_only(wiz: dict) -> bool:
    """True when a Scaling Adjustment is broad-scope (entity set, no book or department).

    Entity+currency is treated as broad-scope because it still targets millions of
    rows and the preview times out. The pipeline blocking logic already handles it
    correctly — only book_code and department_code narrow the scope meaningfully.
    """
    if wiz.get("category") != "Scaling Adjustment":
        return False
    if not (wiz.get("entity_code") or "").strip():
        return False
    # Broad-scope: no book_code and no department_code (currency alone doesn't narrow enough)
    has_book = bool((wiz.get("book_code") or "").strip())
    has_dept = bool((wiz.get("department_code") or "").strip())
    return not has_book and not has_dept


STEPS = ["Category & Details", "Preview & Submit"]

if wiz["step"] in (1, 2):
    render_step_bar(wiz["step"], STEPS)
    st.markdown("<br/>", unsafe_allow_html=True)


# ── STEP 1 : Category & Details ───────────────────────────────────────────────

if wiz["step"] == 1:
    section_title("Adjustment Category", "📂")
    cat_cols = st.columns(len(CATEGORY_CONFIG))
    for i, (ck, ccfg) in enumerate(CATEGORY_CONFIG.items()):
        with cat_cols[i]:
            is_sel = wiz["category"] == ck
            st.markdown(
                f'<div style="background:{ccfg["bg"] if is_sel else P["white"]};'
                f'border:2px solid {P["primary"] if is_sel else P["border"]};'
                f'border-radius:10px;padding:0.8rem 0.5rem;'
                f'text-align:center;min-height:130px">'
                f'<div style="font-size:1.8rem">{ccfg["icon"]}</div>'
                f'<div style="font-weight:700;font-size:0.85rem;margin-top:0.3rem">{ck}</div>'
                f'<div style="font-size:0.68rem;color:{P["grey_700"]};margin-top:0.2rem">'
                f'{ccfg["desc"]}</div></div>', unsafe_allow_html=True)
            if st.button(f'{"✓ " if is_sel else ""}{ck}',
                         key=_k(f"cat_{ck}"), use_container_width=True,
                         type="primary" if is_sel else "secondary"):
                wiz.update({"category": ck, "process_type": None,
                             "adjustment_type": None,
                             "uploaded_df": None, "uploaded_file_name": None})
                safe_rerun()

    st.divider()

    if not wiz["category"]:
        st.info("👆 Select an adjustment category to continue.")
    elif wiz["category"] == "VaR Upload":
        render_var_upload_form()
    elif wiz["category"] == "Entity Roll":
        render_entity_roll_form()
    else:
        render_scaling_form()


# ── STEP 2 : Preview & Submit ─────────────────────────────────────────────────

elif wiz["step"] == 2:
    cat       = wiz.get("category") or "Scaling Adjustment"
    scope_cfg = SCOPE_CONFIG.get(wiz.get("process_type", ""), {})

    section_title("Adjustment Summary", "📋")

    # ── Summary banner ────────────────────────────────────────────────────
    if cat == "VaR Upload":
        df_up     = wiz.get("uploaded_df")
        row_count = len(df_up) if df_up is not None else 0
        st.markdown(
            f'<div class="mcard">'
            f'<div style="display:flex;gap:16px;align-items:center">'
            f'<span style="font-size:2rem">📤</span>'
            f'<div><div style="font-weight:700;font-size:1.1rem">VaR Upload</div>'
            f'<div style="font-size:0.85rem;color:{P["grey_700"]}">'
            f'Ref: {wiz.get("global_reference","?")} · '
            f'File: {wiz.get("uploaded_file_name","?")} · '
            f'{row_count:,} rows · COB: {wiz["cobid"]}'
            f'</div></div></div></div>', unsafe_allow_html=True)
        if df_up is not None:
            section_title(f"Data Preview ({row_count:,} rows)", "📊")
            st.dataframe(df_up.head(50), use_container_width=True, height=300)

        if wiz.get("_dup_adj_ids"):
            dup_count = len(wiz["_dup_adj_ids"])
            st.markdown(
                f'<div style="background:#FFF3E0;border:2px solid #FFB74D;border-radius:10px;'
                f'padding:1rem;margin:0.8rem 0">'
                f'<div style="font-weight:700;font-size:0.92rem;color:#E65100">'
                f'⚠️ Replacing {dup_count} existing adjustment(s) with reference '
                f'"{wiz.get("global_reference","")}" on COB {wiz.get("cobid","")}</div>'
                f'<div style="font-size:0.83rem;color:#BF360C;margin-top:0.3rem">'
                f'Previous adjustment data will be soft-deleted from ADJ_HEADER and '
                f'DIMENSION.ADJUSTMENT, and removed from FACT adjustment tables on submission.</div>'
                f'</div>', unsafe_allow_html=True)

    elif cat == "Entity Roll":
        st.markdown(
            f'<div class="mcard" style="border-left:4px solid #E65100">'
            f'<div style="display:flex;gap:16px;align-items:center">'
            f'<span style="font-size:2rem">🔄</span>'
            f'<div><div style="font-weight:700;font-size:1.1rem">'
            f'{wiz.get("process_type","")} — Entity Roll</div>'
            f'<div style="font-size:0.85rem;color:{P["grey_700"]}">'
            f'Entity: {wiz.get("entity_code","")} · '
            f'Source COB: {wiz.get("source_cobid","")} → Target COB: {wiz.get("cobid","")}'
            f'</div></div></div></div>', unsafe_allow_html=True)

        st.markdown(
            f'<div style="background:#FFF3E0;border:1px solid #FFB74D;border-radius:8px;'
            f'padding:0.75rem 1rem;margin-top:0.8rem;font-size:0.83rem;color:#E65100">'
            f'⚠️ <strong>This will delete all {wiz.get("process_type","")} data for '
            f'COB {wiz.get("cobid","")} / Entity {wiz.get("entity_code","")}</strong> '
            f'in both FACT and FACT ADJUSTED tables, then copy from '
            f'COB {wiz.get("source_cobid","")}. '
            f'This operation requires approval before it is processed.</div>',
            unsafe_allow_html=True)

        if wiz.get("reason"):
            st.markdown(
                f'<div class="mcard" style="margin-top:0.8rem">'
                f'<strong>Reason:</strong> {wiz.get("reason","")}</div>',
                unsafe_allow_html=True)

    else:  # Scaling Adjustment
        scale_info = (f' · Scale: {wiz.get("scale_factor", 1.0)}×'
                      if wiz.get("adjustment_type") in ("Scale", "Roll") else "")
        rec_info   = (f' · Recurring: {wiz.get("recurring_start_cobid")} → '
                      f'{wiz.get("recurring_end_cobid")}'
                      if wiz.get("occurrence") == "RECURRING" else "")
        st.markdown(
            f'<div class="mcard">'
            f'<div style="display:flex;gap:16px;align-items:center">'
            f'<span style="font-size:2rem">{scope_cfg.get("icon","📊")}</span>'
            f'<div><div style="font-weight:700;font-size:1.1rem">'
            f'{wiz.get("process_type","")} — {wiz.get("adjustment_type","")}</div>'
            f'<div style="font-size:0.85rem;color:{P["grey_700"]}">'
            f'COB: {wiz["cobid"]} · {wiz.get("occurrence","ADHOC")}'
            f'{scale_info}{rec_info}</div></div></div></div>',
            unsafe_allow_html=True)

        render_filter_chips(wiz)

        # ── Impact Preview ─────────────────────────────────────────────────
        section_title("Impact Preview", "👁️")

        if _is_entity_only(wiz):
            st.info(
                f"**Entity-wide adjustment** — this applies to all books and departments "
                f"for entity **{wiz.get('entity_code')}** on COB **{wiz['cobid']}**. "
                f"Data preview is skipped for broad-scope adjustments to avoid timeouts. "
                f"The adjustment will be queued and will only be processed once no other "
                f"adjustments are active in the **{wiz.get('process_type','')}** scope."
            )
        else:
            preview_json = {
                "cobid":           wiz["cobid"],
                "process_type":    wiz["process_type"],
                "adjustment_type": wiz["adjustment_type"],
                "source_cobid":    wiz.get("source_cobid") or wiz["cobid"],
                "scale_factor":    wiz.get("scale_factor", 1.0),
            }
            for key in ["entity_code", "source_system_code", "department_code",
                        "book_code", "currency_code", "trade_typology",
                        "strategy", "instrument_code", "simulation_name",
                        "simulation_source", "measure_type_code", "trade_code",
                        "trader_code", "var_component_id", "var_sub_component_id",
                        "guaranteed_entity", "region_key", "scenario_date_id",
                        "tenor_code", "underlying_tenor_code", "curve_code",
                        "day_type", "product_category_attributes",
                        "batch_region_area", "murex_family", "murex_group"]:
                val = wiz.get(key)
                if val and str(val).strip():
                    preview_json[key] = str(val).strip()

            try:
                df_preview = call_sp_df("ADJUSTMENT_APP.SP_PREVIEW_ADJUSTMENT",
                                         json.dumps(preview_json))
                if not df_preview.empty:
                    total_rows = len(df_preview)
                    col_cv  = next((c for c in df_preview.columns
                                    if "CURRENT_VALUE"    in c and "LOCAL" not in c), None)
                    col_del = next((c for c in df_preview.columns
                                    if "ADJUSTMENT_DELTA" in c and "LOCAL" not in c), None)
                    col_pv  = next((c for c in df_preview.columns
                                    if "PROJECTED_VALUE"  in c and "LOCAL" not in c), None)

                    def _fmt(v):
                        try:    return f"{float(v):,.0f}"
                        except: return "—"

                    m1, m2, m3, m4, m5 = st.columns(5)
                    m1.metric("Rows Affected",    f"{total_rows:,}")
                    m2.metric("Non-zero Rows",    f"{int((df_preview[col_cv] != 0).sum()):,}"
                                                   if col_cv else "—")
                    m3.metric("Total Original",   _fmt(df_preview[col_cv].sum())  if col_cv  else "—")
                    m4.metric("Total Adjustment", _fmt(df_preview[col_del].sum()) if col_del else "—")
                    m5.metric("Total Projected",  _fmt(df_preview[col_pv].sum())  if col_pv  else "—")

                    st.markdown("<br/>", unsafe_allow_html=True)

                    grp_cols = [c for c in ["BOOK_CODE", "DEPARTMENT_CODE", "ENTITY_CODE"]
                                if c in df_preview.columns]
                    val_cols = [c for c in [col_cv, col_del, col_pv] if c]

                    btn1, btn2, _ = st.columns([1, 1, 3])
                    with btn1:
                        if st.button("📊 Show Breakdown", key=_k("show_breakdown"), type="secondary", use_container_width=True):
                            wiz["show_breakdown"] = True
                    with btn2:
                        if st.button("🔍 Show Sample Rows", key=_k("show_sample"), type="secondary", use_container_width=True):
                            wiz["show_sample"] = True

                    if wiz.get("show_breakdown") and grp_cols and val_cols:
                        df_grp = (df_preview.groupby(grp_cols)[val_cols]
                                  .sum().reset_index().sort_values(grp_cols))
                        df_grp.rename(columns={col_cv: "Original", col_del: "Adjustment",
                                               col_pv: "Projected"}, inplace=True)
                        st.markdown(f"**Breakdown by {' / '.join(grp_cols)}**")
                        st.dataframe(df_grp, use_container_width=True,
                                     height=min(300, 38 + 35 * len(df_grp)))

                    if wiz.get("show_sample"):
                        st.markdown(f"**Sample rows (up to 1,000 of {total_rows:,})**")
                        st.dataframe(df_preview.head(1000), use_container_width=True,
                                     height=300)
                else:
                    st.info("No matching rows found for this filter combination.")

                with st.expander("🔍 Debug — request params", expanded=df_preview.empty):
                    st.code(json.dumps(preview_json, indent=2), language="json")

            except Exception as exc:
                st.warning(f"Preview not available: {exc}")

    # ── Error from previous attempt ────────────────────────────────────────
    if (wiz.get("result") or {}).get("status") == "Error":
        st.error(f"❌ {wiz['result'].get('message', 'Submission failed')}")

    # ── Navigation ─────────────────────────────────────────────────────────
    st.divider()
    nav1, nav2 = st.columns(2)
    with nav1:
        if st.button("← Back", use_container_width=True, key=_k("back")):
            wiz["result"]         = None
            wiz["step"]           = 1
            wiz["show_breakdown"] = False
            wiz["show_sample"]    = False
            safe_rerun()
    with nav2:
        if st.button("🚀 Submit Adjustment", type="primary",
                     use_container_width=True, key=_k("submit")):
            wiz["result"] = None
            with st.spinner("Submitting adjustment…"):
                result = _do_submit()
            wiz["result"] = result
            wiz["step"]   = 3 if result.get("status") != "Error" else 2
            safe_rerun()


# ── STEP 3 : Success ──────────────────────────────────────────────────────────

elif wiz["step"] == 3:
    result = wiz.get("result") or {}
    msg    = result.get("message", "Adjustment created successfully")

    blocked_msg = ""
    if "Blocked by ADJ #" in msg:
        blocked_msg = msg

    st.markdown(
        f'<div style="background:#E8F5E9;border:2px solid #A5D6A7;'
        f'border-radius:12px;padding:2.5rem;text-align:center;margin:1rem 0">'
        f'<div style="font-size:3rem">✅</div>'
        f'<div style="font-size:1.4rem;font-weight:700;color:#2E7D32;margin-top:0.5rem">'
        f'Adjustment Submitted Successfully</div>'
        f'<div style="font-size:0.9rem;color:#388E3C;margin-top:0.4rem">{msg}</div>'
        f'<div style="font-size:0.82rem;color:{P["info"]};margin-top:0.8rem">'
        f'⚡ Your adjustment is queued and will be processed automatically by the scope pipeline. '
        f'You can monitor its progress in the Processing Queue.</div>'
        f'</div>', unsafe_allow_html=True)

    if blocked_msg:
        st.markdown(
            f'<div style="background:#FFF3E0;border:1px solid #FFB74D;border-radius:8px;'
            f'padding:0.75rem 1rem;margin-top:0.5rem;font-size:0.83rem;color:#E65100">'
            f'⏸ <strong>Processing is queued behind another adjustment.</strong> '
            f'{blocked_msg} It will be picked up automatically once that run completes.</div>',
            unsafe_allow_html=True)

    st.markdown("<br/>", unsafe_allow_html=True)
    if st.button("➕ Create Another Adjustment", use_container_width=True,
                 type="secondary", key="new_adj"):
        reset_wizard()
        safe_rerun()
