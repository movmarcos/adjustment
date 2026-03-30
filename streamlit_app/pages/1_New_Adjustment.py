"""
New Adjustment — 3-Step Wizard
================================
Step 1  : Category  →  category-specific form
Step 2  : Preview
Step 3  : Submit

Categories
  • Global Adjustment   – metadata form (fields from Global_Adj CSV)
  • VaR Upload          – upload a CSV with 21 VaR columns
  • Scaling Adjustment  – Scope (VaR/Stress/…) + Type (Flatten/Scale/Roll) + dim filters
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
from utils.snowflake_conn import run_query, run_query_df, call_sp_df, current_user_name, safe_rerun

# ── Auto-navigate to Processing Queue after successful submission ─────────────
if st.session_state.get("_nav_to_queue"):
    del st.session_state["_nav_to_queue"]
    try:
        st.switch_page("pages/4_Processing_Queue.py")
    except Exception:
        pass  # Older Streamlit: fall through, success flash is shown instead

inject_css()
render_sidebar()

# ── Success flash from previous submission ────────────────────────────────────
if st.session_state.get("_submit_success"):
    st.success(f"✅ {st.session_state.pop('_submit_success')}")
    st.info("Your adjustment is in the Processing Queue. Open it from the sidebar.")


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  WIZARD STATE                                                              ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

def init_wizard():
    if "wiz" not in st.session_state:
        reset_wizard()


def reset_wizard():
    st.session_state["wiz"] = {
        "step": 1,
        # Category (first choice)
        "category": None,
        # Scaling-specific
        "process_type": None,
        "adjustment_type": None,
        "occurrence": "ADHOC",
        "source_cobid": None,
        "recurring_start_cobid": None,
        "recurring_end_cobid": None,
        "scale_factor": 1.0,
        # Shared
        "cobid": None,
        "entity_code": None,
        "source_system_code": None,
        "department_code": None,
        "book_code": None,
        "currency_code": None,
        "trade_typology": None,
        "trade_code": None,
        "strategy": None,
        "instrument_code": None,
        "simulation_name": None,
        "measure_type_code": None,
        "reason": "",
        "requires_approval": False,
        # Global Adjustment fields
        "ga_adjustment_type": "",
        "ga_form_data": "",
        "ga_source_cobid": None,
        # VaR Upload fields
        "uploaded_file_name": None,
        "uploaded_df": None,
        # Results
        "preview_data": None,
        "result": None,
    }


init_wizard()
wiz = st.session_state["wiz"]


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  HELPER RENDERERS (defined before use)                                     ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

# ── Global Adjustment ────────────────────────────────────────────────────────

def render_global_adj_form():
    """
    Fields based on the Global_Adj_SF CSV layout:
    COBId, SourceCOBId, AdjustmentType, Reason, FormData,
    EntityCode, EntityKey, DepartmentCode
    """
    section_title("Global Adjustment Details", "🌐")

    st.markdown(
        f'<div style="background:{P["info_lt"]};border:1px solid #90CAF9;border-radius:8px;'
        f'padding:0.7rem 1rem;margin-bottom:1rem;font-size:0.85rem">'
        f'Global Adjustments are metadata records tracked in '
        f'<code>ADJUSTMENT.GLOBAL_ADJUSTMENT_SF</code>. They do <strong>not</strong> '
        f'write to FACT tables — they capture an adjustment decision for audit.'
        f'</div>', unsafe_allow_html=True)

    g1, g2 = st.columns(2)
    with g1:
        cobid_input = st.text_input(
            "COB Date (YYYYMMDD)", key="ga_cobid",
            value=str(wiz.get("cobid") or ""),
            placeholder="e.g. 20260328")
        if cobid_input.strip():
            wiz["cobid"] = int(cobid_input.strip())

        src_cobid_input = st.text_input(
            "Source COB Date (optional)", key="ga_src_cobid",
            value=str(wiz.get("ga_source_cobid") or ""),
            placeholder="Leave blank if same as COB")
        if src_cobid_input.strip():
            wiz["ga_source_cobid"] = int(src_cobid_input.strip())

        wiz["ga_adjustment_type"] = st.selectbox(
            "Adjustment Type", ["inserted", "updated", "deleted", "other"],
            index=["inserted", "updated", "deleted", "other"].index(
                wiz.get("ga_adjustment_type") or "inserted"),
            key="ga_adj_type")

    with g2:
        wiz["entity_code"] = st.text_input(
            "Entity Code", key="ga_entity",
            value=wiz.get("entity_code") or "",
            placeholder="e.g. MUSI")

        wiz["department_code"] = st.text_input(
            "Department Code (optional)", key="ga_dept",
            value=wiz.get("department_code") or "")

    st.markdown("<hr style='margin:0.8rem 0;border-color:#e0e0e0'/>",
                unsafe_allow_html=True)

    wiz["reason"] = st.text_area(
        "Reason / Business Justification", value=wiz.get("reason", ""),
        height=80, key="ga_reason",
        placeholder="e.g. VaR_Upload_20260326_EFE FX VaR")

    wiz["ga_form_data"] = st.text_area(
        "Form Data (optional — JSON or free text)", key="ga_form_data_input",
        value=wiz.get("ga_form_data", ""), height=60,
        placeholder="Optional structured data")

    # Approval
    wiz["requires_approval"] = st.checkbox(
        "🔐 Requires Approval", value=wiz.get("requires_approval", False),
        key="ga_approval")

    # Continue
    st.markdown("<br/>", unsafe_allow_html=True)
    can_continue = wiz.get("cobid") and wiz.get("entity_code")
    if can_continue:
        if st.button("Continue → Preview", type="primary", use_container_width=True):
            # Map Global Adj to the ADJ_HEADER model
            wiz["process_type"] = "VaR"            # default scope
            wiz["adjustment_type"] = "Direct"       # metadata-only
            wiz["step"] = 2
            safe_rerun()
    else:
        missing = []
        if not wiz.get("cobid"):        missing.append("COB Date")
        if not wiz.get("entity_code"):  missing.append("Entity Code")
        st.markdown(
            f'<div style="font-size:0.82rem;color:{P["grey_700"]};text-align:center">'
            f'Complete required fields: <strong>{", ".join(missing)}</strong></div>',
            unsafe_allow_html=True)


# ── VaR Upload ───────────────────────────────────────────────────────────────

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


def render_var_upload_form():
    section_title("VaR Upload — CSV File", "📤")

    st.markdown(
        f'<div style="background:{P["info_lt"]};border:1px solid #90CAF9;border-radius:8px;'
        f'padding:0.7rem 1rem;margin-bottom:1rem;font-size:0.85rem">'
        f'Upload a VaR adjustment CSV file containing the 21 VaR measure columns. '
        f'Expected columns: <code>COBId, EntityCode, SourceSystemCode, BookCode, '
        f'CurrencyCode, ScenarioDate, TradeCode, AllVaR … ParCreditSpreadVaR, '
        f'Category, Detail</code>.'
        f'</div>', unsafe_allow_html=True)


    csv_text = st.text_area(
        "Paste VaR CSV Data Here",
        value="",
        height=180,
        key="var_csv_textarea",
        help="Paste the full CSV content including header row. The CSV must have the standard VaR Upload column layout.")

    if csv_text.strip():
        try:
            from io import StringIO
            df = pd.read_csv(StringIO(csv_text.strip()))
            wiz["uploaded_file_name"] = f"CSV_Pasted_{len(csv_text)}_chars.csv"
            wiz["uploaded_df"] = df

            # Validate columns
            missing_cols = [c for c in EXPECTED_VAR_COLS if c not in df.columns]
            extra_cols   = [c for c in df.columns if c not in EXPECTED_VAR_COLS]

            if missing_cols:
                st.warning(f"⚠️ Missing expected columns: {', '.join(missing_cols)}")
            if extra_cols:
                st.info(f"Extra columns (will be ignored): {', '.join(extra_cols)}")

            # Stats
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Rows", f"{len(df):,}")
            c2.metric("Columns", len(df.columns))
            if "EntityCode" in df.columns:
                c3.metric("Entities", df["EntityCode"].nunique())
            if "BookCode" in df.columns:
                c4.metric("Books", df["BookCode"].nunique())

            # Preview
            st.dataframe(df.head(20), use_container_width=True, height=250)

            # Auto-extract COBId and EntityCode from data
            if "COBId" in df.columns and not df["COBId"].empty:
                wiz["cobid"] = int(df["COBId"].iloc[0])
            if "EntityCode" in df.columns and not df["EntityCode"].empty:
                wiz["entity_code"] = str(df["EntityCode"].iloc[0])

        except Exception as e:
            st.error(f"Failed to read CSV: {e}")

    st.markdown("<hr style='margin:0.8rem 0;border-color:#e0e0e0'/>",
                unsafe_allow_html=True)

    # Manual overrides
    g1, g2 = st.columns(2)
    with g1:
        cobid_val = st.text_input(
            "COB Date (auto-detected)", key="var_cobid",
            value=str(wiz.get("cobid") or ""))
        if cobid_val.strip():
            wiz["cobid"] = int(cobid_val.strip())
    with g2:
        entity_val = st.text_input(
            "Entity Code (auto-detected)", key="var_entity",
            value=wiz.get("entity_code") or "")
        if entity_val.strip():
            wiz["entity_code"] = entity_val.strip()

    wiz["reason"] = st.text_area(
        "Reason / Business Justification", value=wiz.get("reason", ""),
        height=60, key="var_reason")

    wiz["requires_approval"] = st.checkbox(
        "🔐 Requires Approval", value=wiz.get("requires_approval", False),
        key="var_approval")

    # Continue
    st.markdown("<br/>", unsafe_allow_html=True)
    can_continue = (wiz.get("cobid") and wiz.get("uploaded_df") is not None
                    and wiz.get("entity_code"))
    if can_continue:
        if st.button("Continue → Preview", type="primary", use_container_width=True):
            wiz["process_type"] = "VaR"
            wiz["adjustment_type"] = "Upload"
            wiz["step"] = 2
            safe_rerun()
    else:
        missing = []
        if wiz.get("uploaded_df") is None: missing.append("CSV File")
        if not wiz.get("cobid"):           missing.append("COB Date")
        if not wiz.get("entity_code"):     missing.append("Entity Code")
        st.markdown(
            f'<div style="font-size:0.82rem;color:{P["grey_700"]};text-align:center">'
            f'Complete required fields: <strong>{", ".join(missing)}</strong></div>',
            unsafe_allow_html=True)


# ── Scaling Adjustment ───────────────────────────────────────────────────────

def render_scaling_form():
    """The existing Scope + Type + Dimension filters form."""

    # ── Scope cards ───────────────────────────────────────────────────────
    FRTB_SUBTYPES = ["FRTB", "FRTBDRC", "FRTBRRAO", "FRTBALL"]

    section_title("Data Scope", "🔍")
    scope_cols = st.columns(len(SCOPE_CONFIG))
    for i, (sk, cfg) in enumerate(SCOPE_CONFIG.items()):
        with scope_cols[i]:
            if sk == "FRTB":
                is_sel = wiz["process_type"] in FRTB_SUBTYPES
            else:
                is_sel = wiz["process_type"] == sk
            border = P["primary"] if is_sel else P["border"]
            bg     = cfg["bg"]    if is_sel else P["white"]
            st.markdown(
                f'<div style="background:{bg};border:2px solid {border};border-radius:10px;'
                f'padding:0.6rem 0.4rem;text-align:center">'
                f'<div style="font-size:1.5rem">{cfg["icon"]}</div>'
                f'<div style="font-weight:700;font-size:0.8rem;margin-top:0.2rem">{cfg["label"]}</div>'
                f'</div>', unsafe_allow_html=True)
            if st.button(f'{"✓ " if is_sel else ""}{cfg["label"]}',
                         key=f"scope_{sk}", use_container_width=True,
                         type="primary" if is_sel else "secondary"):
                wiz["process_type"] = sk  # FRTB sets to "FRTB" as default subtype
                safe_rerun()

    # FRTB subtype selector — appears when any FRTB variant is active
    if wiz["process_type"] in FRTB_SUBTYPES:
        FRTB_SUBTYPE_CONFIG = {
            "FRTB":     "Standard FRTB",
            "FRTBDRC":  "Default Risk Charge",
            "FRTBRRAO": "Residual Risk Add-On",
            "FRTBALL":  "All FRTB (combined)",
        }
        st.markdown(
            f'<div style="background:{P["success_lt"]};border:1px solid #A5D6A7;border-radius:8px;'
            f'padding:0.5rem 1rem;margin:0.5rem 0 0.3rem;font-size:0.82rem;color:{P["success"]}">'
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
                    f'<div style="font-size:0.68rem;color:{P["grey_700"]};margin-top:2px">{stdesc}</div>'
                    f'</div>', unsafe_allow_html=True)
                if st.button(f'{"✓ " if is_sub else ""}{stk}',
                             key=f"frtb_sub_{stk}", use_container_width=True,
                             type="primary" if is_sub else "secondary"):
                    wiz["process_type"] = stk
                    safe_rerun()

    st.markdown("<hr style='margin:0.8rem 0;border-color:#e0e0e0'/>",
                unsafe_allow_html=True)

    if not wiz["process_type"]:
        st.info("👆 Select a data scope to continue.")
        return

    # ── Adjustment Type cards ────────────────────────────────────────────
    left_col, right_col = st.columns([3, 2], gap="large")

    with left_col:
        section_title("Adjustment Type", "⚡")
        type_cols = st.columns(len(TYPE_CONFIG))
        for i, (tk, tcfg) in enumerate(TYPE_CONFIG.items()):
            with type_cols[i]:
                is_sel = wiz["adjustment_type"] == tk
                border = P["primary"] if is_sel else P["border"]
                bg     = "#FFF0F3"   if is_sel else P["white"]
                st.markdown(
                    f'<div style="background:{bg};border:2px solid {border};border-radius:10px;'
                    f'padding:0.6rem 0.4rem;text-align:center">'
                    f'<div style="font-size:1.3rem">{tcfg["icon"]}</div>'
                    f'<div style="font-weight:700;font-size:0.78rem;margin:0.2rem 0">{tk}</div>'
                    f'<div style="font-size:0.68rem;color:{P["grey_700"]}">{tcfg["desc"]}</div>'
                    f'</div>', unsafe_allow_html=True)
                if st.button(f'{"✓ " if is_sel else ""}{tk}',
                             key=f"type_{tk}", use_container_width=True,
                             type="primary" if is_sel else "secondary"):
                    wiz["adjustment_type"] = tk
                    safe_rerun()

        if wiz["adjustment_type"]:
            st.markdown(
                f'<div style="background:{P["grey_100"]};border-radius:6px;padding:0.5rem 0.8rem;'
                f'font-size:0.78rem;color:{P["grey_700"]};margin-top:0.5rem;font-family:monospace">'
                f'Formula: {TYPE_CONFIG[wiz["adjustment_type"]]["formula"]}</div>',
                unsafe_allow_html=True)

    with right_col:
        section_title("Date & Schedule", "📅")

        wiz["occurrence"] = st.radio(
            "Frequency", ["ADHOC", "RECURRING"],
            index=0 if wiz["occurrence"] == "ADHOC" else 1,
            horizontal=True, key="wiz_freq")

        if wiz["occurrence"] == "ADHOC":
            cobid_input = st.text_input(
                "COB Date (YYYYMMDD)", key="wiz_cobid",
                value=str(wiz.get("cobid") or ""),
                placeholder="e.g. 20260328")
            if cobid_input.strip():
                wiz["cobid"] = int(cobid_input.strip())
        else:
            st.markdown(
                f'<div style="background:{P["info_lt"]};border:1px solid #90CAF9;'
                f'border-radius:6px;padding:0.5rem 0.8rem;font-size:0.8rem;margin-bottom:0.5rem">'
                f'Recurring adjustments repeat daily between start and end COB.</div>',
                unsafe_allow_html=True)
            cobid_input = st.text_input(
                "First COB Date (YYYYMMDD)", key="wiz_cobid_r",
                value=str(wiz.get("cobid") or ""),
                placeholder="e.g. 20260328")
            if cobid_input.strip():
                wiz["cobid"] = int(cobid_input.strip())

            rc1, rc2 = st.columns(2)
            with rc1:
                rs = st.text_input("Start COBID", key="wiz_rec_start",
                                   value=str(wiz.get("recurring_start_cobid") or ""))
                if rs.strip():
                    wiz["recurring_start_cobid"] = int(rs.strip())
            with rc2:
                re = st.text_input("End COBID", key="wiz_rec_end",
                                   value=str(wiz.get("recurring_end_cobid") or ""))
                if re.strip():
                    wiz["recurring_end_cobid"] = int(re.strip())

        if wiz["adjustment_type"] == "Roll":
            source_input = st.text_input(
                "Source COB (roll from)", key="wiz_src_cobid",
                value=str(wiz.get("source_cobid") or ""))
            if source_input.strip():
                wiz["source_cobid"] = int(source_input.strip())

        if wiz["adjustment_type"] in ("Scale", "Roll"):
            section_title("Scale Factor", "🔧")
            wiz["scale_factor"] = st.number_input(
                "Scale Factor", value=float(wiz["scale_factor"]),
                min_value=-10.0, max_value=100.0, step=0.01, format="%.4f",
                help="1.05 = +5%, 0.95 = -5%", key="wiz_sf")

    st.markdown("<hr style='margin:0.8rem 0;border-color:#e0e0e0'/>",
                unsafe_allow_html=True)

    # ── Dimension Filters ────────────────────────────────────────────────
    section_title("Dimension Filters", "🎯")
    st.markdown(
        f"<span style='font-size:0.82rem;color:{P['grey_700']}'>"
        f"Leave blank to include all values for that dimension.</span>",
        unsafe_allow_html=True)

    c1, c2, c3 = st.columns(3)
    with c1:
        wiz["entity_code"] = st.text_input(
            "Entity Code", value=wiz.get("entity_code") or "",
            placeholder="e.g. MUSE", key="wiz_entity")
        wiz["department_code"] = st.text_input(
            "Department Code", value=wiz.get("department_code") or "", key="wiz_dept")
        wiz["book_code"] = st.text_input(
            "Book Code", value=wiz.get("book_code") or "", key="wiz_book")
    with c2:
        wiz["source_system_code"] = st.text_input(
            "Source System Code", value=wiz.get("source_system_code") or "", key="wiz_src_sys")
        wiz["currency_code"] = st.text_input(
            "Currency Code", value=wiz.get("currency_code") or "",
            placeholder="e.g. USD", key="wiz_ccy")
        wiz["trade_typology"] = st.text_input(
            "Trade Typology", value=wiz.get("trade_typology") or "", key="wiz_typology")
    with c3:
        wiz["strategy"] = st.text_input(
            "Strategy", value=wiz.get("strategy") or "", key="wiz_strategy")
        wiz["instrument_code"] = st.text_input(
            "Instrument Code", value=wiz.get("instrument_code") or "", key="wiz_instrument")
        if wiz["process_type"] == "Stress":
            wiz["simulation_name"] = st.text_input(
                "Simulation Name", value=wiz.get("simulation_name") or "", key="wiz_sim")

    st.markdown("<hr style='margin:0.8rem 0;border-color:#e0e0e0'/>",
                unsafe_allow_html=True)

    # ── Business context ─────────────────────────────────────────────────
    wiz["reason"] = st.text_area(
        "Reason / Business Justification", value=wiz["reason"],
        height=80, key="wiz_reason")

    wiz["requires_approval"] = st.checkbox(
        "🔐 Requires Approval", value=wiz.get("requires_approval", False),
        key="wiz_requires_approval")

    # Continue
    st.markdown("<br/>", unsafe_allow_html=True)
    can_continue = wiz.get("cobid") and wiz.get("adjustment_type")
    if wiz["occurrence"] == "RECURRING":
        can_continue = can_continue and wiz.get("recurring_start_cobid") and wiz.get("recurring_end_cobid")

    if can_continue:
        if st.button("Continue → Preview", type="primary", use_container_width=True):
            wiz["step"] = 2
            safe_rerun()
    else:
        missing = []
        if not wiz.get("adjustment_type"): missing.append("Adjustment Type")
        if not wiz.get("cobid"):           missing.append("COB Date")
        if wiz["occurrence"] == "RECURRING":
            if not wiz.get("recurring_start_cobid"): missing.append("Start COBID")
            if not wiz.get("recurring_end_cobid"):   missing.append("End COBID")
        st.markdown(
            f'<div style="font-size:0.82rem;color:{P["grey_700"]};text-align:center">'
            f'Complete required fields: <strong>{", ".join(missing)}</strong></div>',
            unsafe_allow_html=True)


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  SUBMIT LOGIC  (defined before step routing)                               ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

def _do_submit():
    cat = wiz.get("category", "Scaling Adjustment")

    if cat == "Global Adjustment":
        submit_json = {
            "cobid":              wiz["cobid"],
            "process_type":       wiz.get("process_type", "VaR"),
            "adjustment_type":    "Direct",
            "username":           current_user_name(),
            "source_cobid":       wiz.get("ga_source_cobid") or wiz["cobid"],
            "reason":             wiz.get("reason", ""),
            "entity_code":        wiz.get("entity_code", ""),
            "department_code":    wiz.get("department_code", ""),
            "requires_approval":  wiz.get("requires_approval", False),
            "adjustment_occurrence": "ADHOC",
            "global_reference":   f"GA_{wiz['cobid']}_{wiz.get('entity_code','')}",
        }

    elif cat == "VaR Upload":
        submit_json = {
            "cobid":              wiz["cobid"],
            "process_type":       "VaR",
            "adjustment_type":    "Upload",
            "username":           current_user_name(),
            "source_cobid":       wiz["cobid"],
            "reason":             wiz.get("reason", ""),
            "entity_code":        wiz.get("entity_code", ""),
            "requires_approval":  wiz.get("requires_approval", False),
            "adjustment_occurrence": "ADHOC",
            "file_name":          wiz.get("uploaded_file_name", ""),
        }

    else:  # Scaling Adjustment
        submit_json = {
            "cobid":              wiz["cobid"],
            "process_type":       wiz["process_type"],
            "adjustment_type":    wiz["adjustment_type"],
            "username":           current_user_name(),
            "source_cobid":       wiz.get("source_cobid") or wiz["cobid"],
            "scale_factor":       wiz.get("scale_factor", 1.0),
            "reason":             wiz.get("reason", ""),
            "adjustment_occurrence": wiz.get("occurrence", "ADHOC"),
            "requires_approval":  wiz.get("requires_approval", False),
        }
        if wiz.get("occurrence") == "RECURRING":
            submit_json["recurring_start_cobid"] = wiz.get("recurring_start_cobid")
            submit_json["recurring_end_cobid"]   = wiz.get("recurring_end_cobid")

        for key in ["entity_code", "source_system_code", "department_code",
                     "book_code", "currency_code", "trade_typology",
                     "strategy", "instrument_code", "simulation_name",
                     "measure_type_code", "trade_code"]:
            val = wiz.get(key)
            if val and str(val).strip():
                submit_json[key] = str(val).strip()

    try:
        json_str = json.dumps(submit_json).replace("'", "\\'")
        result_rows = run_query(
            f"CALL ADJUSTMENT_APP.SP_SUBMIT_ADJUSTMENT('{json_str}')")
        if result_rows:
            wiz["result"] = (json.loads(str(result_rows[0][0]))
                             if isinstance(result_rows[0][0], str)
                             else result_rows[0][0])
        else:
            wiz["result"] = {"status": "Error",
                             "message": "No response from procedure"}
    except Exception as e:
        wiz["result"] = {"status": "Error", "message": str(e)}


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  STEP 1 — MAIN ROUTER                                                     ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

STEPS = ["Category & Details", "Preview & Submit"]
render_step_bar(wiz["step"], STEPS)
st.markdown("<br/>", unsafe_allow_html=True)


if wiz["step"] == 1:

    # ── Category selector ─────────────────────────────────────────────────
    section_title("Adjustment Category", "📂")
    cat_cols = st.columns(len(CATEGORY_CONFIG))
    for i, (ck, ccfg) in enumerate(CATEGORY_CONFIG.items()):
        with cat_cols[i]:
            is_sel = wiz["category"] == ck
            border = P["primary"] if is_sel else P["border"]
            bg     = ccfg["bg"]   if is_sel else P["white"]
            st.markdown(
                f'<div style="background:{bg};border:2px solid {border};border-radius:10px;'
                f'padding:0.8rem 0.5rem;text-align:center;min-height:100px">'
                f'<div style="font-size:1.8rem">{ccfg["icon"]}</div>'
                f'<div style="font-weight:700;font-size:0.85rem;margin-top:0.3rem">{ck}</div>'
                f'<div style="font-size:0.68rem;color:{P["grey_700"]};margin-top:0.2rem">'
                f'{ccfg["desc"]}</div>'
                f'</div>', unsafe_allow_html=True)
            if st.button(f'{"✓ " if is_sel else ""}{ck}',
                         key=f"cat_{ck}", use_container_width=True,
                         type="primary" if is_sel else "secondary"):
                wiz["category"] = ck
                wiz["process_type"] = None
                wiz["adjustment_type"] = None
                wiz["uploaded_df"] = None
                wiz["uploaded_file_name"] = None
                safe_rerun()

    st.markdown("<hr style='margin:1rem 0;border-color:#e0e0e0'/>",
                unsafe_allow_html=True)

    if not wiz["category"]:
        st.info("👆 Select an adjustment category to continue.")
    elif wiz["category"] == "Global Adjustment":
        render_global_adj_form()
    elif wiz["category"] == "VaR Upload":
        render_var_upload_form()
    elif wiz["category"] == "Scaling Adjustment":
        render_scaling_form()


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  STEP 2 — PREVIEW                                                         ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

elif wiz["step"] == 2:
    section_title("Adjustment Summary", "📋")

    cat       = wiz.get("category", "Scaling Adjustment")
    cat_cfg   = CATEGORY_CONFIG.get(cat, {})
    scope_cfg = SCOPE_CONFIG.get(wiz.get("process_type", ""), {})

    # ── Summary card ─────────────────────────────────────────────────────
    if cat == "Global Adjustment":
        st.markdown(
            f'<div class="mcard">'
            f'<div style="display:flex;gap:16px;align-items:center;margin-bottom:0.8rem">'
            f'<span style="font-size:2rem">🌐</span>'
            f'<div>'
            f'<div style="font-weight:700;font-size:1.1rem">Global Adjustment</div>'
            f'<div style="font-size:0.85rem;color:{P["grey_700"]}">'
            f'COB: {wiz["cobid"]} · Entity: {wiz.get("entity_code","—")} · '
            f'Type: {wiz.get("ga_adjustment_type","—")}</div>'
            f'</div></div></div>',
            unsafe_allow_html=True)

        section_title("Details", "📝")
        details = {
            "COB Date": wiz.get("cobid"),
            "Source COB": wiz.get("ga_source_cobid") or "—",
            "Entity": wiz.get("entity_code"),
            "Department": wiz.get("department_code") or "—",
            "Adjustment Type": wiz.get("ga_adjustment_type"),
            "Reason": wiz.get("reason") or "—",
            "Form Data": wiz.get("ga_form_data") or "—",
        }
        rows_html = "".join(
            f'<tr><td style="color:{P["grey_700"]};padding:4px 12px 4px 0;font-size:0.85rem">'
            f'{k}</td><td style="font-weight:600;font-size:0.85rem">{v}</td></tr>'
            for k, v in details.items() if v and str(v) != "—"
        )
        st.markdown(
            f'<div class="mcard"><table style="width:100%;border-collapse:collapse">'
            f'{rows_html}</table></div>',
            unsafe_allow_html=True)

    elif cat == "VaR Upload":
        df_upload = wiz.get("uploaded_df")
        row_count = len(df_upload) if df_upload is not None else 0
        st.markdown(
            f'<div class="mcard">'
            f'<div style="display:flex;gap:16px;align-items:center;margin-bottom:0.8rem">'
            f'<span style="font-size:2rem">📤</span>'
            f'<div>'
            f'<div style="font-weight:700;font-size:1.1rem">VaR Upload</div>'
            f'<div style="font-size:0.85rem;color:{P["grey_700"]}">'
            f'File: {wiz.get("uploaded_file_name","?")} · {row_count:,} rows · '
            f'COB: {wiz["cobid"]}</div>'
            f'</div></div></div>',
            unsafe_allow_html=True)

        if df_upload is not None:
            section_title(f"Data Preview ({row_count:,} rows)", "📊")
            st.dataframe(df_upload.head(50), use_container_width=True, height=300)

    else:  # Scaling Adjustment
        scale_info = (f' · Scale: {wiz["scale_factor"]}x'
                      if wiz.get("adjustment_type") in ("Scale", "Roll") else "")
        rec_info   = (f' · Recurring: {wiz.get("recurring_start_cobid")} → '
                      f'{wiz.get("recurring_end_cobid")}'
                      if wiz.get("occurrence") == "RECURRING" else "")
        st.markdown(
            f'<div class="mcard">'
            f'<div style="display:flex;gap:16px;align-items:center;margin-bottom:0.8rem">'
            f'<span style="font-size:2rem">{scope_cfg.get("icon","📊")}</span>'
            f'<div>'
            f'<div style="font-weight:700;font-size:1.1rem">'
            f'{wiz.get("process_type","")} — {wiz.get("adjustment_type","")}</div>'
            f'<div style="font-size:0.85rem;color:{P["grey_700"]}">'
            f'COB: {wiz["cobid"]} · {wiz.get("occurrence","ADHOC")}{scale_info}{rec_info}'
            f'</div></div></div></div>',
            unsafe_allow_html=True)

        render_filter_chips(wiz)

        section_title("Impact Preview", "👁️")

        preview_json = {
            "cobid": wiz["cobid"],
            "process_type": wiz["process_type"],
            "adjustment_type": wiz["adjustment_type"],
            "source_cobid": wiz.get("source_cobid") or wiz["cobid"],
            "scale_factor": wiz.get("scale_factor", 1.0),
        }
        for key in ["entity_code", "source_system_code", "department_code",
                     "book_code", "currency_code", "trade_typology",
                     "strategy", "instrument_code", "simulation_name"]:
            val = wiz.get(key)
            if val and str(val).strip():
                preview_json[key] = str(val).strip()

        try:
            # session.call() correctly handles RETURNS TABLE() procedures
            df_preview = call_sp_df(
                "ADJUSTMENT_APP.SP_PREVIEW_ADJUSTMENT",
                json.dumps(preview_json),
            )

            if not df_preview.empty:
                total_rows = len(df_preview)

                # ── KPI metrics ────────────────────────────────────────────
                def _fmt(v):
                    try:
                        return f"{float(v):,.0f}"
                    except Exception:
                        return "—"

                col_cv  = next((c for c in df_preview.columns if "CURRENT_VALUE"    in c and "LOCAL" not in c), None)
                col_del = next((c for c in df_preview.columns if "ADJUSTMENT_DELTA" in c and "LOCAL" not in c), None)
                col_pv  = next((c for c in df_preview.columns if "PROJECTED_VALUE"  in c and "LOCAL" not in c), None)

                sum_cv  = df_preview[col_cv].sum()  if col_cv  else None
                sum_del = df_preview[col_del].sum() if col_del else None
                sum_pv  = df_preview[col_pv].sum()  if col_pv  else None

                nonzero_rows = int((df_preview[col_cv] != 0).sum()) if col_cv else total_rows

                m1, m2, m3, m4, m5 = st.columns(5)
                m1.metric("Rows Affected",      f"{total_rows:,}")
                m2.metric("Non-zero Rows",      f"{nonzero_rows:,}")
                m3.metric("Total Original",     _fmt(sum_cv)  if sum_cv  is not None else "—")
                m4.metric("Total Adjustment",   _fmt(sum_del) if sum_del is not None else "—")
                m5.metric("Total Projected",    _fmt(sum_pv)  if sum_pv  is not None else "—")

                st.markdown("<br/>", unsafe_allow_html=True)

                # ── Breakdown by BOOK_CODE / DEPARTMENT_CODE ───────────────
                grp_cols = [c for c in ["BOOK_CODE", "DEPARTMENT_CODE", "ENTITY_CODE"] if c in df_preview.columns]
                val_cols = [c for c in [col_cv, col_del, col_pv] if c]
                if grp_cols and val_cols:
                    df_grp = (
                        df_preview.groupby(grp_cols)[val_cols]
                        .sum()
                        .reset_index()
                        .sort_values(grp_cols)
                    )
                    rename_map = {}
                    if col_cv:  rename_map[col_cv]  = "Original"
                    if col_del: rename_map[col_del] = "Adjustment"
                    if col_pv:  rename_map[col_pv]  = "Projected"
                    df_grp = df_grp.rename(columns=rename_map)
                    st.markdown("**Breakdown by " + " / ".join(grp_cols) + "**")
                    st.dataframe(df_grp, use_container_width=True, height=min(300, 38 + 35 * len(df_grp)))

                # ── Full detail (collapsed, capped at 1 000 rows for display) ─
                with st.expander(f"View sample rows (up to 1,000 of {total_rows:,})", expanded=False):
                    st.dataframe(df_preview.head(1000), use_container_width=True, height=300)

            else:
                st.info("No matching rows found for this filter combination.")

            with st.expander("🔍 Debug — View request params", expanded=df_preview.empty):
                st.code(json.dumps(preview_json, indent=2), language="json")

        except Exception as e:
            st.warning(f"Preview not available: {e}")

    # ── Submit section ───────────────────────────────────────────────────
    st.markdown("<hr style='margin:1rem 0;border-color:#e0e0e0'/>", unsafe_allow_html=True)

    nav1, nav2 = st.columns(2)
    with nav1:
        if st.button("← Back", use_container_width=True):
            wiz["step"] = 1
            safe_rerun()
    with nav2:
        if st.button("🚀 Submit Adjustment", type="primary", use_container_width=True,
                     disabled=st.session_state.get("_submitting", False)):
            st.session_state["_submitting"] = True
            with st.spinner("Submitting adjustment…"):
                _do_submit()
            st.session_state["_submitting"] = False
            result = (wiz.get("result") or {})
            if result.get("status") != "Error":
                msg = result.get("message", "Adjustment created successfully")
                st.session_state["_submit_success"] = msg
                st.session_state["_nav_to_queue"] = True
                reset_wizard()
            safe_rerun()

    if (wiz.get("result") or {}).get("status") == "Error":
        st.error(f"❌ {wiz['result'].get('message', 'Submission failed')}")
        if st.button("← Back to Edit", key="back_err", use_container_width=True):
            wiz["result"] = None
            wiz["step"] = 1
            safe_rerun()




