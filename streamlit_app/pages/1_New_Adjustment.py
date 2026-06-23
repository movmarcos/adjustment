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
    P, SCOPE_CONFIG, TYPE_CONFIG, CATEGORY_CONFIG,
    fmt_adj_id, icon,
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
    "requires_approval":      False,
    # Direct Adjustment upload
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

    if cat == "Direct Adjustment":
        return {
            "cobid":                 wiz["cobid"],
            "process_type":          wiz["process_type"],
            "adjustment_type":       "Direct",
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
            "adjustment_type":       "EROL",
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


# ── Direct Adjustment: config-driven schema + JSON upload writer ──────────────
def _direct_expected_columns(scope: str) -> list:
    """Return the ordered expected CSV column names for a scope from DIRECT_SCOPE_SCHEMA.
    Empty list if the scope has no config row yet."""
    try:
        rows = run_query(f"""
            SELECT EXPECTED_COLUMNS
            FROM ADJUSTMENT_APP.DIRECT_SCOPE_SCHEMA
            WHERE UPPER(PROCESS_TYPE) = UPPER('{scope.replace("'", "''")}')
              AND IS_ACTIVE = TRUE
        """)
        if not rows or rows[0][0] is None:
            return []
        spec = rows[0][0]
        spec = json.loads(spec) if isinstance(spec, str) else spec
        return [c["name"] for c in spec]
    except Exception:
        return []


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


def _do_submit() -> dict:
    """Call SP_SUBMIT_ADJUSTMENT. Returns result dict (never raises)."""
    import uuid as _uuid
    wrote_line_items_for = None
    try:
        payload = _build_payload()

        # For Direct Adjustment: write line items BEFORE the SP call so that
        # navigating away can't interrupt the write. Pre-generate the
        # ADJ_ID so both line items and header share the same ID.
        if wiz.get("category") == "Direct Adjustment" and wiz.get("uploaded_df") is not None:
            adj_id = str(_uuid.uuid4())
            payload["adj_id"] = adj_id
            n = _write_direct_json_rows(adj_id, wiz["uploaded_df"])
            if n == 0:
                return {"status": "Error",
                        "message": "No rows found in CSV data"}
            wrote_line_items_for = adj_id

        # Snowflake escapes a single quote by DOUBLING it (''), not with a
        # backslash. A backslash leaves the quote active → broken/injectable CALL.
        # Backslashes must be doubled FIRST: Snowflake literals interpret \n, \t
        # etc., so json.dumps's "\n" would arrive as a raw newline inside the
        # JSON and json.loads in the SP fails with "Invalid control character".
        json_str = json.dumps(payload).replace("\\", "\\\\").replace("'", "''")
        rows = run_query(f"CALL ADJUSTMENT_APP.SP_SUBMIT_ADJUSTMENT('{json_str}')")
        if not rows:
            result = {"status": "Error", "message": "No response from stored procedure"}
        else:
            raw = rows[0][0]
            result = json.loads(str(raw)) if isinstance(raw, str) else raw
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
    "FRTBALL":  "All FRTB (combined)",
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

# Every filter key (used for the ticket's applied-filter chips)
FILTER_KEYS = ["entity_code", "source_system_code", "department_code", "book_code"] + \
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
    """Bordered container (section card) followed by a spacer, so cards never
    hug each other (the gap is emitted in the page flow — version-proof,
    unlike CSS on Streamlit's wrapper test-ids)."""
    try:
        c = st.container(border=True)
    except TypeError:
        c = st.container()
    with c:
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
    "Direct Adjustment":  ":material/upload_file:",
    "Entity Roll":        ":material/autorenew:",
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
    """Compact YYYYMMDD text input returning int or None."""
    raw = st.text_input(label, key=_k(key), value=str(value or ""),
                        placeholder=placeholder)
    return int(raw.strip()) if raw.strip().isdigit() else None


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
    correctly — only book_code and department_code narrow the scope meaningfully.
    """
    if wiz.get("category") != "Scaling Adjustment":
        return False
    if not (wiz.get("entity_code") or "").strip():
        return False
    has_book = bool((wiz.get("book_code") or "").strip())
    has_dept = bool((wiz.get("department_code") or "").strip())
    return not (has_book or has_dept)


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

def _completion_checks() -> list:
    """Ordered (label, done) pairs driving the ticket's completion checklist."""
    cat = wiz.get("category")
    checks = [("Category", bool(cat))]
    if not cat:
        return checks
    if cat == "Direct Adjustment":
        checks += [
            ("Data scope",  bool(wiz.get("process_type"))),
            ("CSV data",    wiz.get("uploaded_df") is not None),
            ("COB date",    bool(wiz.get("cobid"))),
            ("Entity code", bool((wiz.get("entity_code") or "").strip())),
            ("Reference",   bool((wiz.get("global_reference") or "").strip())),
            ("Reason",      bool((wiz.get("reason") or "").strip())),
        ]
    elif cat == "Entity Roll":
        checks += [
            ("Data scope",  bool(wiz.get("process_type"))),
            ("Target COB",  bool(wiz.get("cobid"))),
            ("Source COB",  bool(wiz.get("source_cobid"))),
            ("Entity code", bool((wiz.get("entity_code") or "").strip())),
            ("Reason",      bool((wiz.get("reason") or "").strip())),
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
        checks += [
            ("Entity code", bool((wiz.get("entity_code") or "").strip())),
            ("Department or Book code",
             bool((wiz.get("department_code") or "").strip())
             or bool((wiz.get("book_code") or "").strip())),
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


def _render_main_filters() -> None:
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        wiz["entity_code"] = st.text_input("Entity Code *", key=_k("entity"),
                                           value=wiz.get("entity_code") or "",
                                           placeholder="e.g. MUSI")
    with c2:
        wiz["source_system_code"] = st.text_input("Source System", key=_k("src_sys"),
                                                  value=wiz.get("source_system_code") or "",
                                                  placeholder="e.g. MS")
    with c3:
        wiz["department_code"] = st.text_input("Department Code †", key=_k("dept"),
                                               value=wiz.get("department_code") or "")
    with c4:
        wiz["book_code"] = st.text_input("Book Code †", key=_k("book"),
                                         value=wiz.get("book_code") or "")
    st.caption("Blank = all values for that dimension · † at least one of Department or Book")


def _render_extra_filters() -> None:
    """Scope-specific (tier 2) + rarely-used (tier 3) filters, collapsed."""
    pt = wiz.get("process_type", "")
    custom_fields = SCOPE_FIELDS.get(pt, [])
    applied = sum(1 for k in FILTER_KEYS
                  if k not in ("entity_code", "source_system_code",
                               "department_code", "book_code")
                  and (wiz.get(k) or "").strip())
    label = f"More filters ({applied} applied)" if applied else "More filters"
    with st.expander(label, expanded=False):
        if custom_fields:
            st.markdown(
                f'<div style="font-size:0.72rem;font-weight:700;text-transform:uppercase;'
                f'letter-spacing:0.05em;color:{P["info"]};margin-bottom:0.2rem">'
                f'{pt} filters</div>', unsafe_allow_html=True)
            ccols = st.columns(min(len(custom_fields), 4))
            for i, (fk, fl, ph) in enumerate(custom_fields):
                with ccols[i % len(ccols)]:
                    wiz[fk] = st.text_input(fl, key=_k(fk),
                                            value=wiz.get(fk) or "", placeholder=ph)
        shown = {"entity_code", "source_system_code", "department_code", "book_code"}
        shown.update(fk for fk, _, _ in custom_fields)
        extra = [(k, l, p) for k, l, p in ALL_EXTRA_FIELDS if k not in shown]
        ecols = st.columns(3)
        for i, (fk, fl, ph) in enumerate(extra):
            with ecols[i % 3]:
                wiz[fk] = st.text_input(fl, key=_k(fk),
                                        value=wiz.get(fk) or "", placeholder=ph)


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


def render_direct_form() -> None:
    with _card():
        _sec(2, "Data Scope", "Select the data scope for this upload.")
        _render_scope_pills(include_frtball=False)
    if not wiz.get("process_type"):
        st.info("Select a data scope to continue.")
        return

    expected_cols = _direct_expected_columns(wiz["process_type"])
    _csv_card = _card()
    _csv_card.__enter__()
    _sec(3, f"CSV Upload — {wiz['process_type']}", "Paste exact adjustment values.")
    if expected_cols:
        _info_banner('Paste a CSV of exact adjustment values. Expected columns: '
                     '<code>' + ', '.join(expected_cols) + '</code>.')
    else:
        _info_banner(f'No upload schema is configured for the <b>{wiz["process_type"]}</b> '
                     'scope yet. Paste a CSV; columns will be stored as-is.')

    csv_text = st.text_area("Paste CSV Data Here", value="", height=160,
                            key=_k("direct_csv"),
                            help="Paste the full CSV content including the header row.")
    if csv_text.strip():
        try:
            from io import StringIO
            df = pd.read_csv(StringIO(csv_text.strip()))
            wiz["uploaded_file_name"] = f"CSV_Pasted_{len(df)}_rows.csv"
            wiz["uploaded_df"]        = df

            missing_cols = [c for c in expected_cols if c not in df.columns]
            extra_cols   = [c for c in df.columns    if c not in expected_cols]
            if missing_cols:
                st.warning(f"Missing expected columns: {', '.join(missing_cols)}")
            if extra_cols:
                st.info(f"Extra columns (will be ignored): {', '.join(extra_cols)}")

            st.dataframe(df.head(20), use_container_width=True, height=200)

            if "COBId" in df.columns and len(df):
                wiz["cobid"] = int(df["COBId"].iloc[0])
            if "EntityCode" in df.columns and len(df):
                wiz["entity_code"] = str(df["EntityCode"].iloc[0])
        except Exception as exc:
            st.error(f"Failed to read CSV: {exc}")

    _csv_card.__exit__(None, None, None)

    with _card():
        _sec(4, "Upload Details", "COB and entity are auto-detected from the CSV when present.")
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
        _sec(5, "Business Context", "Why is this adjustment needed?")
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
            wiz["entity_code"] = st.text_input("Entity Code *", key=_k("er_entity"),
                                               value=wiz.get("entity_code") or "",
                                               placeholder="e.g. MUSE")

    # ── Supersede preview: prior entity-scoped adjustments at the target COB ──
    wiz["_eroll_supersede_ids"] = []
    if wiz.get("cobid") and (wiz.get("entity_code") or "").strip():
        try:
            sup_rows = run_query(f"""
                SELECT ADJ_ID, DIMENSION_ADJ_ID
                FROM ADJUSTMENT_APP.ADJ_HEADER
                WHERE COBID = {int(wiz['cobid'])}
                  AND ENTITY_CODE = '{wiz["entity_code"].strip().replace("'", "''")}'
                  AND IS_DELETED = FALSE
                ORDER BY CREATED_DATE DESC
            """)
        except Exception:
            sup_rows = []
        wiz["_eroll_supersede_ids"] = [r[0] for r in sup_rows]
        if sup_rows:
            st.markdown(
                f'<div style="background:{P["danger_lt"]};border:1px solid #FECACA;border-radius:10px;'
                f'padding:0.8rem 1rem;margin:0.2rem 0 0.6rem">'
                f'<div style="font-weight:700;font-size:0.88rem;color:{P["danger"]};margin-bottom:0.25rem">'
                f'{icon("alert-triangle", size=14, color="#B91C1C")} '
                f'{len(sup_rows)} existing adjustment(s) will be permanently removed</div>'
                f'<div style="font-size:0.82rem;color:{P["danger"]}">'
                f'Processing this roll flags those adjustments deleted and removes their '
                f'data for <strong>{wiz["entity_code"].strip()}</strong> at COB '
                f'<strong>{wiz["cobid"]}</strong>. Global adjustments are not affected.'
                f'</div></div>',
                unsafe_allow_html=True)

    with _card():
        _sec(4, "Business Context", "Why is this roll needed?")
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
    elif cat == "Direct Adjustment":
        df_up = wiz.get("uploaded_df")
        kv += _ticket_row("Scope",     wiz.get("process_type"))
        kv += _ticket_row("COB",       wiz.get("cobid"))
        kv += _ticket_row("Reference", wiz.get("global_reference"))
        kv += _ticket_row("CSV rows",
                          f"{len(df_up):,}" if df_up is not None else None,
                          df_up is not None)
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
                     "department_code": "Dept", "book_code": "Book"}
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
    """Run the summary-mode preview SP and stash the single aggregate row."""
    payload = _preview_payload()
    try:
        df_sum = call_sp_df("ADJUSTMENT_APP.SP_PREVIEW_ADJUSTMENT",
                            json.dumps({**payload, "mode": "summary"}))
        if df_sum.empty or "ROWS_AFFECTED" not in df_sum.columns:
            msg_col = next((c for c in df_sum.columns if "MESSAGE" in c.upper()), None)
            wiz["_preview_err"] = (str(df_sum.iloc[0][msg_col]) if msg_col and not df_sum.empty
                                   else "Couldn't calculate a preview for these filters.")
            wiz["_preview_sum"] = None
        else:
            wiz["_preview_sum"] = df_sum.iloc[0].to_dict()
            wiz["_preview_err"] = None
            wiz["_preview_for"] = json.dumps(payload, sort_keys=True, default=str)
    except Exception as exc:
        wiz["_preview_err"] = str(exc)
        wiz["_preview_sum"] = None


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
        cat = _pill_row(list(CATEGORY_CONFIG.keys()), wiz.get("category"),
                        "cat", icons=CATEGORY_BTN_ICONS,
                        descs={k: v["desc"] for k, v in CATEGORY_CONFIG.items()})
        if cat and cat != wiz.get("category"):
            wiz.update({"category": cat, "process_type": None, "adjustment_type": None,
                        "uploaded_df": None, "uploaded_file_name": None,
                        "_preview_sum": None, "_preview_err": None})
            safe_rerun()

    if not wiz.get("category"):
        st.info("Select an adjustment category to continue.")
    elif wiz["category"] == "Direct Adjustment":
        render_direct_form()
    elif wiz["category"] == "Entity Roll":
        render_entity_roll_form()
    else:
        render_scaling_form()

missing = _missing_fields()

with right:
    st.markdown(_ticket_html(missing), unsafe_allow_html=True)

    cat = wiz.get("category")

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

    # ── Direct: replacement confirmation ─────────────────────────────────
    dup_ok = True
    if cat == "Direct Adjustment" and wiz.get("_dup_adj_ids"):
        dup_ok = st.checkbox(
            f"Replace {len(wiz['_dup_adj_ids'])} existing adjustment(s) with this upload",
            key=_k("dup_confirm"), value=False)

    # ── Entity Roll: destructive-replace agreement ───────────────────────
    eroll_ok = True
    if cat == "Entity Roll":
        _n_sup = len(wiz.get("_eroll_supersede_ids") or [])
        _sup_txt = (f"{_n_sup} existing adjustment(s) " if _n_sup
                    else "any existing adjustments ")
        eroll_ok = st.checkbox(
            f"I understand this Entity Roll will permanently remove {_sup_txt}"
            f"for this entity at the target COB.",
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
            disabled=bool(missing) or not dup_ok or not eroll_ok or zero_rows):
        wiz["result"] = None
        with st.spinner("Submitting adjustment…"):
            result = _do_submit()
        wiz["result"] = result
        wiz["step"]   = 3 if _is_submit_success(result) else 1
        safe_rerun()
    if missing:
        st.caption("Submit unlocks when the ticket is complete.")
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
if wiz.get("category") == "Scaling Adjustment" and wiz.get("_preview_sum") \
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
                    st.dataframe(df_grp, use_container_width=True,
                                 height=min(300, 38 + 35 * len(df_grp)))
            except Exception as exc:
                st.warning(f"Breakdown not available: {exc}")
        with st.expander(f"Sample rows (up to 1,000 of {total_rows:,})", expanded=False):
            try:
                df_sample = call_sp_df("ADJUSTMENT_APP.SP_PREVIEW_ADJUSTMENT",
                                       json.dumps({**_preview_payload(), "mode": "sample"}))
                st.dataframe(df_sample, use_container_width=True, height=300)
            except Exception as exc:
                st.warning(f"Sample not available: {exc}")
