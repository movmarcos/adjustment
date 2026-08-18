# FRTB Direct Adjustment uploader: input (CSV upload / paste) -> save to direct table -> validate mandatory fields -> check derived view & validation proc.
# Co-authored with CoCo
import io
import os
import re
from datetime import datetime

import numpy as np
import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------
conn = st.connection("snowflake", ttl=os.getenv("SNOWFLAKE_CONNECTION_TTL"))
session = conn.session()

DATABASE = "DVLP_RAPTOR_EM_20260410"
SCHEMA = "ADJUSTMENT"

# ---------------------------------------------------------------------------
# Per-adjustment-type configuration.
#
# conditional_rules format:
#   Each rule is a dict with keys:
#     field: column to validate
#     conditions: list of (column, pattern, negate) tuples that must ALL match
#     error: error message
# ---------------------------------------------------------------------------
TYPE_CONFIG = {
    "RRAO": {
        "table": "ADJUSTMENTS_DIRECT_RRAO",
        "view": "VW_ADJUSTMENTS_DIRECT_RRAO",
        "view_dataset_col": "DIRECT_RRAO_DATASET_NAME",
        "validation_proc": "ADJUSTMENTS_DIRECT_RRAO_VALIDATION",
        "validation_table": "ADJUSTMENTS_DIRECT_RRAO_VALIDATION",
        "validation_view": "VW_ADJUSTMENTS_DIRECT_RRAO_VALIDATION",
        "filename_col": "DIRECT_RRAO_FILENAME",
        "file_row_col": "DIRECT_RRAO_FILE_ROW_NUMBER",
        "stage_ts_col": "DIRECT_RRAO_STAGE_TIMESTAMP",
        "dataset_col": "DIRECT_RRAO_DATASET_NAME",
        "date_columns": [],
        "mandatory_fields": [
            "COBID",
            "ENTITY_CODE",
            "TRADE_CODE",
            "BOOK_CODE",
            "MUFG_PRODUCT_CODE",
            "BUSINESS_PRODUCT_CODE4",
            "TRADING_DESK",
            "CCY1",
            "CCY_AMT",
            "SA_RRAO_PRODUCT_TYPE",
            "NOTIONAL_AMOUNT",
            "NOTIONAL_AMOUNT_USD",
        ],
        "conditional_rules": [],
        "mandatory_confirmed": True,
        "aliases": {
            "EVALUATION_DATE": "COBID",
            "TRADE_ID": "TRADE_CODE",
            "BUSINESS_ORGANIZATION_CODE": "BOOK_CODE",
        },
    },
    "DRC": {
        "table": "ADJUSTMENTS_DIRECT_DRC",
        "view": "VW_ADJUSTMENTS_DIRECT_DRC",
        "view_dataset_col": "DIRECT_DRC_DATASET_NAME",
        "validation_proc": "ADJUSTMENTS_DIRECT_DRC_VALIDATION",
        "validation_table": "ADJUSTMENTS_DIRECT_DRC_VALIDATION",
        "validation_view": "VW_ADJUSTMENTS_DIRECT_DRC_VALIDATION",
        "filename_col": "DIRECT_DRC_FILENAME",
        "file_row_col": "DIRECT_DRC_FILE_ROW_NUMBER",
        "stage_ts_col": "DIRECT_DRC_STAGE_TIMESTAMP",
        "dataset_col": "DIRECT_DRC_DATASET_NAME",
        "date_columns": ["MATURITY_DATE"],
        "mandatory_fields": [
            "COBID",
            "ENTITY_CODE",
            "TRADE_CODE",
            "BOOK_CODE",
            "MUFG_PRODUCT_CODE",
            "BUSINESS_PRODUCT_CODE4",
            "TRADING_DESK",
            "CCY1",
            "CCY_AMT",
            "SECURITY_CODE_TYPE",
            "SECURITY_CODE",
            "ISSUER_CODE",
            "MATURITY_DATE",
            "JTD_RISK_DIRECTION",
            "RISK_CLASS",
            "BUCKET",
            "JTD_LOSS",
            "JTD_LOSS_USD",
        ],
        "conditional_rules": [
            {"field": "ISSUER_NAME", "conditions": [("RISK_CLASS", "NON.*SEC.*CREDIT", False)], "error": "Error: ISSUER_NAME is required for Non-Sec (Credit)"},
            {"field": "ISSUER_NAME", "conditions": [("RISK_CLASS", "NON.*SEC.*EQUITY", False)], "error": "Error: ISSUER_NAME is required for Non-Sec (Equity)"},
            {"field": "DEFAULT_RISK_WEIGHT", "conditions": [("RISK_CLASS", "NON.*SEC.*EQUITY", False)], "error": "Error: DEFAULT_RISK_WEIGHT is required for Non-Sec (Equity)"},
            {"field": "DEFAULT_RISK_WEIGHT", "conditions": [("RISK_CLASS", "SEC", False), ("RISK_CLASS", "NON.*SEC", True)], "error": "Error: DEFAULT_RISK_WEIGHT is required for Sec"},
            {"field": "LGD", "conditions": [("RISK_CLASS", "NON.*SEC.*CREDIT", False)], "error": "Error: LGD is required for Non-Sec (Credit)"},
        ],
        "mandatory_confirmed": True,
        "aliases": {
            "EVALUATION_DATE": "COBID",
            "TRADE_ID": "TRADE_CODE",
            "BUSINESS_ORGANIZATION_CODE": "BOOK_CODE",
        },
    },
    "SBM": {
        "table": "ADJUSTMENTS_DIRECT_SBM",
        "view": "VW_ADJUSTMENTS_DIRECT_SBM",
        "view_dataset_col": "DIRECT_SBM_DATASET_NAME",
        "validation_proc": "ADJUSTMENTS_DIRECT_SBM_VALIDATION",
        "validation_table": "ADJUSTMENTS_DIRECT_SBM_VALIDATION",
        "validation_view": "VW_ADJUSTMENTS_DIRECT_SBM_VALIDATION",
        "filename_col": "DIRECT_SBM_FILENAME",
        "file_row_col": "DIRECT_SBM_FILE_ROW_NUMBER",
        "stage_ts_col": "DIRECT_SBM_STAGE_TIMESTAMP",
        "dataset_col": "DIRECT_SBM_DATASET_NAME",
        "date_columns": [],
        "mandatory_fields": [
            # SBM mandatory fields (from ADJUSTMENTS_DIRECT_SBM_VALIDATION proc)
            "COBID",
            "ENTITY_CODE",
            "MUFG_PRODUCT_CODE",
            "BUSINESS_PRODUCT_CODE4",
            "TRADING_DESK",
            "SENSITIVITY_TYPE",
            "RISK_CLASS",
            "AMOUNT",
            "AMOUNT_IN_USD",
        ],
        "conditional_rules": [
            # CCY1 required for FX and GIRR
            {"field": "CCY1", "conditions": [("RISK_CLASS", "FX|GIRR", False)], "error": "Error: CCY1 is required for FX and GIRR positions"},
            # CCY_AMT required for FX and GIRR
            {"field": "CCY_AMT", "conditions": [("RISK_CLASS", "FX|GIRR", False)], "error": "Error: CCY_AMT is required for FX and GIRR positions"},
            # CCY2 required for FX
            {"field": "CCY2", "conditions": [("RISK_CLASS", "FX", False)], "error": "Error: CCY2 is required for FX positions"},
            # MATURITY_OF_THE_OPTION required for FX Vega
            {"field": "MATURITY_OF_THE_OPTION", "conditions": [("RISK_CLASS", "FX", False), ("SENSITIVITY_TYPE", "VEGA", False)], "error": "Error: MATURITY_OF_THE_OPTION is required for FX Vega positions"},
            # UNDERLYING_TENOR_CODE required for FX Vega
            {"field": "UNDERLYING_TENOR_CODE", "conditions": [("RISK_CLASS", "FX", False), ("SENSITIVITY_TYPE", "VEGA", False)], "error": "Error: UNDERLYING_TENOR_CODE (VERTEX_UNDERLYING) is required for FX Vega positions"},
            # CURVE_TYPE required for GIRR Curvature
            {"field": "CURVE_TYPE", "conditions": [("RISK_CLASS", "GIRR", False), ("SENSITIVITY_TYPE", "CURVATURE", False)], "error": "Error: CURVE_TYPE is required for GIRR Curvature positions"},
            # VERTEX required for GIRR Delta and CSR Delta
            {"field": "VERTEX", "conditions": [("RISK_CLASS", "GIRR", False), ("SENSITIVITY_TYPE", "DELTA", False)], "error": "Error: VERTEX is required for GIRR Delta positions"},
            {"field": "VERTEX", "conditions": [("RISK_CLASS", "CSR", False), ("SENSITIVITY_TYPE", "DELTA", False)], "error": "Error: VERTEX is required for CSR Delta positions"},
            # CURVATURE_SCENARIO required for all Curvature positions
            {"field": "CURVATURE_SCENARIO", "conditions": [("SENSITIVITY_TYPE", "CURVATURE", False)], "error": "Error: CURVATURE_SCENARIO is required for Curvature positions"},
            # BUCKET required for Equity and CSR
            {"field": "BUCKET", "conditions": [("RISK_CLASS", "EQUIT|CSR", False)], "error": "Error: BUCKET is required for Equity and CSR positions"},
            # SECURITY_CODE_TYPE required for Equity and CSR
            {"field": "SECURITY_CODE_TYPE", "conditions": [("RISK_CLASS", "EQUIT|CSR", False)], "error": "Error: SECURITY_CODE_TYPE is required for Equity and CSR positions"},
            # SECURITY_CODE required for Equity and CSR
            {"field": "SECURITY_CODE", "conditions": [("RISK_CLASS", "EQUIT|CSR", False)], "error": "Error: SECURITY_CODE is required for Equity and CSR positions"},
            # SECURITY_INFORMATION1 required for Equity and CSR
            {"field": "SECURITY_INFORMATION1", "conditions": [("RISK_CLASS", "EQUIT|CSR", False)], "error": "Error: SECURITY_INFORMATION1 is required for Equity and CSR positions"},
            # SECURITY_INFORMATION2 required for Equity and CSR
            {"field": "SECURITY_INFORMATION2", "conditions": [("RISK_CLASS", "EQUIT|CSR", False)], "error": "Error: SECURITY_INFORMATION2 is required for Equity and CSR positions"},
            # ISSUER_CODE required for Equity and CSR
            {"field": "ISSUER_CODE", "conditions": [("RISK_CLASS", "EQUIT|CSR", False)], "error": "Error: ISSUER_CODE is required for Equity and CSR positions"},
            # ISSUER_NAME required for Equity and CSR
            {"field": "ISSUER_NAME", "conditions": [("RISK_CLASS", "EQUIT|CSR", False)], "error": "Error: ISSUER_NAME is required for Equity and CSR positions"},
            # PRA_BUCKET required for Equity and CSR
            {"field": "PRA_BUCKET", "conditions": [("RISK_CLASS", "EQUIT|CSR", False)], "error": "Error: PRA_BUCKET is required for Equity and CSR positions"},
            # SECURITY_INFORMATION3 required for CSR
            {"field": "SECURITY_INFORMATION3", "conditions": [("RISK_CLASS", "CSR", False)], "error": "Error: SECURITY_INFORMATION3 (Sector) is required for CSR positions"},
        ],
        "mandatory_confirmed": True,
        "aliases": {
            "EVALUATION_DATE": "COBID",
            "VERTEX_UNDERLYING": "UNDERLYING_TENOR_CODE",
        },
    },
}


def auto_columns(cfg):
    """Columns the app fills automatically - users do not supply these in the file."""
    return {
        cfg["filename_col"],
        cfg["file_row_col"],
        cfg["stage_ts_col"],
        cfg["dataset_col"],
        "FILE_ROW_NUMBER",
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
@st.cache_data(show_spinner=False)
def get_table_columns(table: str):
    """Return ordered list of column names for a table from INFORMATION_SCHEMA."""
    df = session.sql(
        f"""
        SELECT COLUMN_NAME
        FROM {DATABASE}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
        """,
        params=[SCHEMA, table],
    ).to_pandas()
    return df["COLUMN_NAME"].tolist()


def object_exists(kind: str, name: str) -> bool:
    """kind in {'TABLE','VIEW'}. Checks INFORMATION_SCHEMA for existence."""
    view = "TABLES" if kind == "TABLE" else "VIEWS"
    df = session.sql(
        f"""
        SELECT COUNT(*) AS C
        FROM {DATABASE}.INFORMATION_SCHEMA.{view}
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        """,
        params=[SCHEMA, name],
    ).to_pandas()
    return int(df["C"].iloc[0]) > 0


def parse_input(uploaded_file, pasted_text, delimiter):
    """Return a pandas DataFrame from either an uploaded file or pasted text."""
    sep = {"Comma (,)": ",", "Tab": "\t", "Semicolon (;)": ";", "Pipe (|)": "|"}[delimiter]
    if uploaded_file is not None:
        raw = uploaded_file.getvalue().decode("utf-8-sig")
        source_name = uploaded_file.name
    elif pasted_text and pasted_text.strip():
        raw = pasted_text
        source_name = None
    else:
        return None, None
    df = pd.read_csv(io.StringIO(raw), sep=sep, dtype=str, keep_default_na=False)
    df.columns = [c.strip().upper() for c in df.columns]
    return df, source_name


def apply_aliases(df, cfg):
    """Rename accepted business header names to their physical column names."""
    aliases = cfg.get("aliases", {})
    if not aliases:
        return df, []
    rename = {c: aliases[c] for c in df.columns if c in aliases}
    applied = [f"{src} -> {dst}" for src, dst in rename.items()]
    return df.rename(columns=rename), applied


def coerce_date_columns(df, date_columns):
    """Convert date columns from various formats (DD/MM/YYYY, MM/DD/YYYY, etc.) to YYYY-MM-DD."""
    for col in date_columns:
        if col not in df.columns:
            continue
        converted = pd.to_datetime(df[col], dayfirst=True, errors="coerce")
        df[col] = converted.dt.strftime("%Y-%m-%d")
        df[col] = df[col].replace({"NaT": None, "nan": None})
    return df


def normalize_to_table(df, table_cols, cfg, source_name, dataset_name):
    """Keep only columns that exist in the target table; add auto metadata columns."""
    table_set = set(table_cols)
    keep = [c for c in df.columns if c in table_set]
    out = df[keep].copy()

    n = len(out)
    file_name = source_name or f"PASTED_INPUT_{datetime.utcnow():%Y%m%d_%H%M%S}"
    stage_ts = datetime.utcnow()
    row_numbers = list(range(1, n + 1))

    autos = {
        cfg["filename_col"]: [file_name] * n,
        cfg["file_row_col"]: row_numbers,
        "FILE_ROW_NUMBER": row_numbers,
        cfg["stage_ts_col"]: [stage_ts] * n,
        cfg["dataset_col"]: [dataset_name] * n,
    }
    for col, vals in autos.items():
        if col in table_set:
            out[col] = vals

    out = coerce_date_columns(out, cfg.get("date_columns", []))
    out = out.replace({"": None})
    out = out.replace({np.nan: None})

    return out


def _is_blank(series):
    """Return boolean mask where values are blank/null-like."""
    col = series.astype(str).str.strip()
    return col.eq("") | col.str.lower().eq("nan") | col.str.lower().eq("none")


def validate_mandatory(df, mandatory_fields, conditional_rules=None):
    """Return error_rows_df with row-level error details for mandatory and conditional rules."""
    present = set(df.columns)
    errors = []

    # Unconditional mandatory field checks
    for f in mandatory_fields:
        if f not in present:
            for row_num in range(1, len(df) + 1):
                errors.append({
                    "ROW_NUMBER": row_num,
                    "COLUMN_NAME": f,
                    "ERROR": f"Error: {f} is required (column missing from input)",
                })
        else:
            blank_mask = _is_blank(df[f])
            for idx in blank_mask[blank_mask].index:
                errors.append({
                    "ROW_NUMBER": idx + 1,
                    "COLUMN_NAME": f,
                    "ERROR": f"Error: {f} is required",
                })

    # Conditional rules: each rule has conditions on one or more columns
    if conditional_rules:
        # Pre-compute uppercased columns for condition matching
        col_cache = {}
        for rule in conditional_rules:
            for cond_col, _, _ in rule["conditions"]:
                if cond_col not in col_cache and cond_col in present:
                    col_cache[cond_col] = df[cond_col].astype(str).str.strip().str.upper()

        for rule in conditional_rules:
            field = rule["field"]
            conditions = rule["conditions"]
            error_msg = rule["error"]

            # Build combined condition mask (all conditions must be true)
            combined_mask = pd.Series(True, index=df.index)
            skip_rule = False
            for cond_col, pattern, negate in conditions:
                if cond_col not in present:
                    skip_rule = True
                    break
                match_mask = col_cache[cond_col].str.contains(pattern, flags=re.IGNORECASE, na=False)
                if negate:
                    match_mask = ~match_mask
                combined_mask = combined_mask & match_mask

            if skip_rule:
                continue

            # Check field is blank where conditions match
            if field not in present:
                for idx in combined_mask[combined_mask].index:
                    errors.append({
                        "ROW_NUMBER": idx + 1,
                        "COLUMN_NAME": field,
                        "ERROR": f"{error_msg} (column missing from input)",
                    })
            else:
                field_blank = _is_blank(df[field])
                error_mask = combined_mask & field_blank
                for idx in error_mask[error_mask].index:
                    errors.append({
                        "ROW_NUMBER": idx + 1,
                        "COLUMN_NAME": field,
                        "ERROR": error_msg,
                    })

    error_df = pd.DataFrame(errors, columns=["ROW_NUMBER", "COLUMN_NAME", "ERROR"])
    return error_df


def step_header(num, title, icon, done=False):
    """Render a consistent step heading with a status badge."""
    badge = ":green-badge[Done]" if done else ":gray-badge[Pending]"
    st.markdown(f"#### :material/{icon}: {num}. {title} &nbsp; {badge}")


# ---------------------------------------------------------------------------
# Page setup
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="FRTB Direct Adjustment",
    page_icon=":material/tune:",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Header + adjustment type selection (on-page)
# ---------------------------------------------------------------------------
st.title("FRTB direct adjustment uploader")
st.caption("Upload or paste data, validate it, and load it into a direct adjustment table.")

adj_type = st.segmented_control(
    "Adjustment type",
    options=list(TYPE_CONFIG.keys()),
    default="RRAO",
    help="Determines the target direct table, derived view and validation objects.",
)
if not adj_type:
    adj_type = "RRAO"
cfg = TYPE_CONFIG[adj_type]

table_fqn = f"{DATABASE}.{SCHEMA}.{cfg['table']}"
table_cols = get_table_columns(cfg["table"])
table_set = set(table_cols)
user_input_cols = [c for c in table_cols if c not in auto_columns(cfg)]

with st.container(border=True):
    oc1, oc2, oc3 = st.columns(3)
    oc1.markdown(f":material/table_chart: **Table**\n\n`{cfg['table']}`")
    oc2.markdown(f":material/visibility: **Derived view**\n\n`{cfg['view']}`")
    oc3.markdown(f":material/rule: **Validation proc**\n\n`{cfg['validation_proc']}`")
    st.caption(f"Schema: `{DATABASE}.{SCHEMA}`")

# ---------------------------------------------------------------------------
# Step 1 - provide data
# ---------------------------------------------------------------------------
with st.container(border=True):
    step_header(1, "Provide data", "upload")

    dc1, dc2 = st.columns(2)
    dataset_name = dc1.text_input(
        "Dataset name",
        value=f"DIRECT_{adj_type}_{datetime.utcnow():%Y%m%d}",
        help="Stored in the dataset-name metadata column and used to identify this load.",
    )
    delimiter = dc2.selectbox("Delimiter", ["Comma (,)", "Tab", "Semicolon (;)", "Pipe (|)"])

    with st.expander(f"Expected layout for {adj_type}", icon=":material/info:"):
        st.caption(
            "File / paste headers must match these column names (case-insensitive). "
            "Unlisted columns are ignored. Metadata columns "
            f"({', '.join(sorted(auto_columns(cfg)))}) are filled automatically."
        )
        st.code(", ".join(user_input_cols), language="text")
        if cfg.get("aliases"):
            st.caption("Accepted business-name aliases (auto-renamed):")
            st.code(
                "\n".join(f"{src} -> {dst}" for src, dst in cfg["aliases"].items()),
                language="text",
            )

    tab_upload, tab_paste = st.tabs([":material/upload_file: Upload file", ":material/content_paste: Paste content"])
    with tab_upload:
        uploaded_file = st.file_uploader("CSV / text file", type=["csv", "txt", "tsv"])
    with tab_paste:
        pasted_text = st.text_area(
            "Rows (first line = header)",
            height=180,
            placeholder="COBID,BOOK_CODE,ENTITY_CODE,...\n20260618,BOOK1,ENT1,...",
        )

df, source_name = parse_input(uploaded_file, pasted_text, delimiter)

if df is None:
    st.caption(":material/arrow_upward: Upload a file or paste content above to continue.")
    st.stop()

# Rename accepted business header names to physical column names.
df, applied_aliases = apply_aliases(df, cfg)

with st.container(border=True):
    pc1, pc2 = st.columns(2)
    pc1.metric("Rows parsed", len(df))
    pc2.metric("Columns parsed", len(df.columns))
    if applied_aliases:
        st.caption(":material/swap_horiz: Renamed business headers: " + ", ".join(applied_aliases))
    st.dataframe(df.head(50), use_container_width=True, hide_index=True)

# ---------------------------------------------------------------------------
# Step 2 - layout validation
# ---------------------------------------------------------------------------
matched = [c for c in df.columns if c in table_set]
unknown = [c for c in df.columns if c not in table_set]

with st.container(border=True):
    step_header(2, "Layout validation", "fact_check", done=bool(matched))
    lc1, lc2 = st.columns(2)
    with lc1:
        st.markdown(f":green-badge[:material/check: Matched: {len(matched)}]")
        st.code(", ".join(matched) or "(none)", language="text")
    with lc2:
        st.markdown(f":orange-badge[:material/block: Ignored: {len(unknown)}]")
        st.code(", ".join(unknown) or "(none)", language="text")

    if not matched:
        st.error("No columns match the target table layout. Fix the headers and try again.", icon=":material/error:")
        st.stop()
    if unknown:
        st.warning("Unknown columns will not be loaded. Confirm the headers are correct.", icon=":material/warning:")

# ---------------------------------------------------------------------------
# Step 3 - mandatory field validation
# ---------------------------------------------------------------------------
validation_errors_df = validate_mandatory(df, cfg["mandatory_fields"], cfg.get("conditional_rules", []))
mandatory_ok = len(validation_errors_df) == 0

with st.container(border=True):
    step_header(3, "Mandatory field validation", "rule", done=mandatory_ok)
    if cfg["mandatory_confirmed"]:
        st.caption(f"Mandatory rules confirmed for {adj_type}.")
    else:
        st.caption(f"Starter rules for {adj_type} - to be replaced by the official spec.")

    if mandatory_ok:
        st.success("All mandatory fields are present and populated.", icon=":material/check_circle:")
    else:
        st.error(
            f"Mandatory validation failed - {len(validation_errors_df)} error(s) found.",
            icon=":material/error:",
        )
        st.dataframe(
            validation_errors_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "ROW_NUMBER": st.column_config.NumberColumn("Row #", width="small"),
                "COLUMN_NAME": st.column_config.TextColumn("Column", width="medium"),
                "ERROR": st.column_config.TextColumn("Error Details", width="large"),
            },
        )

    override = st.toggle("Override and save anyway (prototype only)", value=False)

# ---------------------------------------------------------------------------
# Step 4 - save
# ---------------------------------------------------------------------------
prepared = normalize_to_table(df, table_cols, cfg, source_name, dataset_name)
saved = st.session_state.get("last_dataset") == dataset_name and st.session_state.get("last_type") == adj_type

with st.container(border=True):
    step_header(4, "Save to direct table", "save", done=saved)
    st.caption(f"Loads {len(prepared)} row(s) into `{cfg['table']}` (metadata columns filled automatically).")

    can_save = mandatory_ok or override
    if not can_save:
        st.caption(":material/lock: Resolve mandatory validation or enable override to save.")

    if st.button(
        f"Save {len(prepared)} row(s)",
        type="primary",
        icon=":material/save:",
        disabled=not can_save,
    ):
        try:
            with st.spinner("Writing to Snowflake..."):
                session.write_pandas(
                    prepared,
                    table_name=cfg["table"],
                    database=DATABASE,
                    schema=SCHEMA,
                    quote_identifiers=False,
                    auto_create_table=False,
                )
            nrows = len(prepared)
            st.session_state["last_dataset"] = dataset_name
            st.session_state["last_type"] = adj_type
            st.toast(f"Inserted {nrows} row(s) into {cfg['table']}", icon=":material/check_circle:")
            st.success(f"Inserted {nrows} row(s) into `{table_fqn}`.", icon=":material/check_circle:")
        except Exception as exc:  # noqa: BLE001
            st.error(f"Save failed: {exc}", icon=":material/error:")

# ---------------------------------------------------------------------------
# Step 5 - check derived view & run validation stored procedure
# ---------------------------------------------------------------------------
with st.container(border=True):
    saved_now = st.session_state.get("last_dataset") and st.session_state.get("last_type") == adj_type
    step_header(5, "Derived view & validation", "query_stats", done=bool(saved_now))

    if not saved_now:
        st.caption(":material/info: Save a dataset above to preview the derived view and run validation.")
    else:
        ds = st.session_state["last_dataset"]
        st.caption(f"Showing results for dataset `{ds}`.")

        view_tab, val_tab = st.tabs([":material/visibility: Derived view", ":material/rule: Validation"])

        with view_tab:
            try:
                view_dataset_col = cfg.get("view_dataset_col")
                if view_dataset_col:
                    view_df = session.sql(
                        f"SELECT * FROM {DATABASE}.{SCHEMA}.{cfg['view']} WHERE {view_dataset_col} = ? LIMIT 200",
                        params=[ds],
                    ).to_pandas()
                else:
                    view_df = session.sql(
                        f"SELECT * FROM {DATABASE}.{SCHEMA}.{cfg['view']} LIMIT 200"
                    ).to_pandas()
                st.caption(f"`{cfg['view']}` - {len(view_df)} row(s)")
                if not view_dataset_col:
                    st.caption(":material/info: View does not contain dataset column - showing latest 200 rows.")
                st.dataframe(view_df, use_container_width=True, hide_index=True)
            except Exception as exc:  # noqa: BLE001
                st.warning(f"Could not query derived view: {exc}", icon=":material/warning:")

        with val_tab:
            proc_name = cfg["validation_proc"]
            st.caption(f"Stored procedure: `{SCHEMA}.{proc_name}()`")

            if st.button(
                f"Run {proc_name}()",
                type="secondary",
                icon=":material/play_arrow:",
                key="run_validation_proc",
            ):
                try:
                    with st.spinner(f"Calling {SCHEMA}.{proc_name}()..."):
                        result_df = session.sql(
                            f"CALL {DATABASE}.{SCHEMA}.{proc_name}()"
                        ).to_pandas()
                    st.session_state["validation_proc_result"] = result_df
                    st.session_state["validation_proc_ran"] = True
                    st.toast(f"{proc_name}() executed successfully", icon=":material/check_circle:")
                    st.rerun()
                except Exception as exc:  # noqa: BLE001
                    st.error(f"Validation procedure failed: {exc}", icon=":material/error:")

            if st.session_state.get("validation_proc_ran"):
                result_df = st.session_state.get("validation_proc_result")
                if result_df is not None and not result_df.empty:
                    st.warning(
                        f"Validation returned {len(result_df)} row(s) - review issues below.",
                        icon=":material/warning:",
                    )
                    st.dataframe(result_df, use_container_width=True, hide_index=True)
                elif result_df is not None and result_df.empty:
                    st.success("Validation passed - no issues found.", icon=":material/check_circle:")
                else:
                    st.info("Procedure returned no result set.", icon=":material/info:")
