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
    "measure_type_code":      None,
    "reason":                 "",
    "requires_approval":      False,
    # Global Adjustment
    "ga_adjustment_type":     "inserted",
    "ga_form_data":           "",
    "ga_source_cobid":        None,
    # VaR Upload
    "uploaded_file_name":     None,
    "uploaded_df":            None,
    # Internal
    "submitting":             False,
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

    if cat == "Global Adjustment":
        return {
            "cobid":                 wiz["cobid"],
            "process_type":          wiz.get("process_type", "VaR"),
            "adjustment_type":       "Direct",
            "username":              current_user_name(),
            "source_cobid":          wiz.get("ga_source_cobid") or wiz["cobid"],
            "reason":                wiz.get("reason", ""),
            "entity_code":           wiz.get("entity_code", ""),
            "department_code":       wiz.get("department_code", ""),
            "requires_approval":     wiz.get("requires_approval", False),
            "adjustment_occurrence": "ADHOC",
            "global_reference":      f"GA_{wiz['cobid']}_{wiz.get('entity_code','')}",
        }

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
                "measure_type_code", "trade_code"]:
        val = wiz.get(key)
        if val and str(val).strip():
            payload[key] = str(val).strip()
    return payload


def _do_submit() -> dict:
    """Call SP_SUBMIT_ADJUSTMENT. Returns result dict (never raises)."""
    try:
        json_str = json.dumps(_build_payload()).replace("'", "\\'")
        rows = run_query(f"CALL ADJUSTMENT_APP.SP_SUBMIT_ADJUSTMENT('{json_str}')")
        if not rows:
            return {"status": "Error", "message": "No response from stored procedure"}
        raw = rows[0][0]
        return json.loads(str(raw)) if isinstance(raw, str) else raw
    except Exception as exc:
        return {"status": "Error", "message": str(exc)}


# Handle deferred submission: button sets submitting=True + reruns,
# then this block runs the SP inside the spinner on the NEXT pass.
if wiz.get("submitting"):
    with st.spinner("Submitting adjustment…"):
        result = _do_submit()
    wiz["submitting"] = False
    wiz["result"]     = result
    wiz["step"]       = 3 if result.get("status") != "Error" else 2
    safe_rerun()


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


# ── Global Adjustment ─────────────────────────────────────────────────────────

def render_global_adj_form() -> None:
    section_title("Global Adjustment Details", "🌐")
    _info_banner(
        'Global Adjustments are metadata records tracked in '
        '<code>ADJUSTMENT.GLOBAL_ADJUSTMENT_SF</code>. They do <strong>not</strong> '
        'write to FACT tables — they capture an adjustment decision for audit.')

    g1, g2 = st.columns(2)
    with g1:
        cobid_val = st.text_input("COB Date (YYYYMMDD) *", key=_k("ga_cobid"),
                                   value=str(wiz.get("cobid") or ""),
                                   placeholder="e.g. 20260328")
        if cobid_val.strip().isdigit():
            wiz["cobid"] = int(cobid_val.strip())

        src_val = st.text_input("Source COB Date (optional)", key=_k("ga_src_cobid"),
                                 value=str(wiz.get("ga_source_cobid") or ""),
                                 placeholder="Leave blank if same as COB")
        if src_val.strip().isdigit():
            wiz["ga_source_cobid"] = int(src_val.strip())

        opts = ["inserted", "updated", "deleted", "other"]
        wiz["ga_adjustment_type"] = st.selectbox(
            "Adjustment Type", opts,
            index=opts.index(wiz.get("ga_adjustment_type") or "inserted"),
            key=_k("ga_adj_type"))

    with g2:
        wiz["entity_code"] = st.text_input(
            "Entity Code *", key=_k("ga_entity"),
            value=wiz.get("entity_code") or "", placeholder="e.g. MUSI")
        wiz["department_code"] = st.text_input(
            "Department Code (optional)", key=_k("ga_dept"),
            value=wiz.get("department_code") or "")

    st.divider()
    wiz["reason"] = st.text_area(
        "Reason / Business Justification", value=wiz.get("reason", ""),
        height=80, key=_k("ga_reason"), placeholder="e.g. EFE FX VaR correction")
    wiz["ga_form_data"] = st.text_area(
        "Form Data (optional — JSON or free text)", key=_k("ga_form_data"),
        value=wiz.get("ga_form_data", ""), height=60)
    wiz["requires_approval"] = st.checkbox(
        "🔐 Requires Approval", value=wiz.get("requires_approval", False),
        key=_k("ga_approval"))

    st.markdown("<br/>", unsafe_allow_html=True)
    missing = [f for f, v in [("COB Date", wiz.get("cobid")),
                               ("Entity Code", wiz.get("entity_code"))] if not v]
    if missing:
        _missing_info(missing)
    else:
        if st.button("Continue → Preview", type="primary",
                     use_container_width=True, key=_k("ga_continue")):
            wiz["process_type"]    = "VaR"
            wiz["adjustment_type"] = "Direct"
            wiz["step"] = 2
            safe_rerun()


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

    wiz["reason"] = st.text_area(
        "Reason / Business Justification", value=wiz.get("reason", ""),
        height=60, key=_k("var_reason"))
    wiz["requires_approval"] = st.checkbox(
        "🔐 Requires Approval", value=wiz.get("requires_approval", False),
        key=_k("var_approval"))

    st.markdown("<br/>", unsafe_allow_html=True)
    _checks = [
        ("CSV Data",     wiz.get("uploaded_df") is not None),
        ("COB Date",     bool(wiz.get("cobid"))),
        ("Entity Code",  bool(wiz.get("entity_code"))),
    ]
    missing = [f for f, present in _checks if not present]
    if missing:
        _missing_info(missing)
    else:
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
    section_title("Dimension Filters", "🎯")
    st.caption("Leave blank to include all values for that dimension.")

    c1, c2, c3 = st.columns(3)
    with c1:
        wiz["entity_code"]    = st.text_input("Entity Code",    key=_k("entity"),
                                               value=wiz.get("entity_code") or "",
                                               placeholder="e.g. MUSI")
        wiz["department_code"] = st.text_input("Department Code", key=_k("dept"),
                                                value=wiz.get("department_code") or "")
        wiz["book_code"]      = st.text_input("Book Code",      key=_k("book"),
                                               value=wiz.get("book_code") or "")
    with c2:
        wiz["source_system_code"] = st.text_input("Source System Code", key=_k("src_sys"),
                                                    value=wiz.get("source_system_code") or "")
        wiz["currency_code"]  = st.text_input("Currency Code",  key=_k("ccy"),
                                               value=wiz.get("currency_code") or "",
                                               placeholder="e.g. USD")
        wiz["trade_typology"] = st.text_input("Trade Typology", key=_k("typology"),
                                               value=wiz.get("trade_typology") or "")
    with c3:
        wiz["strategy"]        = st.text_input("Strategy",        key=_k("strategy"),
                                                value=wiz.get("strategy") or "")
        wiz["instrument_code"] = st.text_input("Instrument Code", key=_k("instrument"),
                                                value=wiz.get("instrument_code") or "")
        if wiz["process_type"] == "Stress":
            wiz["simulation_name"] = st.text_input("Simulation Name", key=_k("sim"),
                                                     value=wiz.get("simulation_name") or "")

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
    elif wiz["category"] == "Global Adjustment":
        render_global_adj_form()
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
    if cat == "Global Adjustment":
        st.markdown(
            f'<div class="mcard">'
            f'<div style="display:flex;gap:16px;align-items:center">'
            f'<span style="font-size:2rem">🌐</span>'
            f'<div><div style="font-weight:700;font-size:1.1rem">Global Adjustment</div>'
            f'<div style="font-size:0.85rem;color:{P["grey_700"]}">'
            f'COB: {wiz["cobid"]} · Entity: {wiz.get("entity_code","—")} · '
            f'Type: {wiz.get("ga_adjustment_type","—")}'
            f'</div></div></div></div>', unsafe_allow_html=True)
        rows_html = "".join(
            f'<tr>'
            f'<td style="color:{P["grey_700"]};padding:4px 12px 4px 0;font-size:0.85rem">{k}</td>'
            f'<td style="font-weight:600;font-size:0.85rem">{v}</td>'
            f'</tr>'
            for k, v in {
                "COB Date":        wiz.get("cobid"),
                "Source COB":      wiz.get("ga_source_cobid") or "—",
                "Entity":          wiz.get("entity_code"),
                "Department":      wiz.get("department_code") or "—",
                "Adjustment Type": wiz.get("ga_adjustment_type"),
                "Reason":          wiz.get("reason") or "—",
            }.items() if v and str(v) not in ("", "—")
        )
        st.markdown(
            f'<div class="mcard" style="margin-top:0.8rem">'
            f'<table style="width:100%;border-collapse:collapse">'
            f'{rows_html}</table></div>', unsafe_allow_html=True)

    elif cat == "VaR Upload":
        df_up     = wiz.get("uploaded_df")
        row_count = len(df_up) if df_up is not None else 0
        st.markdown(
            f'<div class="mcard">'
            f'<div style="display:flex;gap:16px;align-items:center">'
            f'<span style="font-size:2rem">📤</span>'
            f'<div><div style="font-weight:700;font-size:1.1rem">VaR Upload</div>'
            f'<div style="font-size:0.85rem;color:{P["grey_700"]}">'
            f'File: {wiz.get("uploaded_file_name","?")} · '
            f'{row_count:,} rows · COB: {wiz["cobid"]}'
            f'</div></div></div></div>', unsafe_allow_html=True)
        if df_up is not None:
            section_title(f"Data Preview ({row_count:,} rows)", "📊")
            st.dataframe(df_up.head(50), use_container_width=True, height=300)

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
                        "strategy", "instrument_code", "simulation_name"]:
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
                     use_container_width=True, key=_k("submit"),
                     disabled=wiz.get("submitting", False)):
            wiz["result"]     = None   # clear any previous error
            wiz["submitting"] = True
            safe_rerun()               # spinner block at top handles actual call


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
    c1, c2 = st.columns(2)
    with c1:
        if st.button("➕ Create Another Adjustment", use_container_width=True,
                     type="secondary", key="new_adj"):
            reset_wizard()
            safe_rerun()
    with c2:
        if st.button("📋 View Processing Queue", use_container_width=True,
                     type="primary", key="go_queue"):
            reset_wizard()
            try:
                st.switch_page("pages/4_Processing_Queue.py")
            except Exception:
                try:
                    st.switch_page("4_Processing_Queue.py")
                except Exception:
                    safe_rerun()
