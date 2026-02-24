"""
State manager — acts as the "database" for the prototype.
All tables live in st.session_state as pandas DataFrames.
Every mutation goes through this module so behaviour mirrors
what the Snowflake Hybrid Tables + Dynamic Tables would do.
"""

import json
import streamlit as st
import pandas as pd
from datetime import datetime, date
from data.mock_data import (
    generate_fact_table,
    generate_dimension_config,
    generate_measure_config,
    empty_adj_header,
    empty_adj_line_items,
    empty_status_history,
    generate_sample_adjustments,
)


# ------------------------------------------------------------------ #
#  Initialisation                                                      #
# ------------------------------------------------------------------ #

def init_state():
    """Call once at app startup. Populates session_state with tables."""
    if "initialised" in st.session_state:
        return

    # Source fact data (immutable)
    st.session_state["fact_table"] = generate_fact_table()
    st.session_state["dim_config"] = generate_dimension_config()
    st.session_state["measure_config"] = generate_measure_config()

    # Adjustment tables (mutable)
    sample_headers, sample_lines, sample_history = generate_sample_adjustments(
        st.session_state["fact_table"]
    )
    st.session_state["adj_header"] = sample_headers
    st.session_state["adj_line_items"] = sample_lines
    st.session_state["status_history"] = sample_history

    # Sequence counters
    st.session_state["next_adj_id"] = int(sample_headers["ADJ_ID"].max()) + 1
    st.session_state["next_line_id"] = int(sample_lines["LINE_ID"].max()) + 1
    st.session_state["next_hist_id"] = int(sample_history["HISTORY_ID"].max()) + 1

    # Current user (switchable in sidebar for demo)
    st.session_state["current_user"] = "sarah.chen"

    st.session_state["initialised"] = True


# ------------------------------------------------------------------ #
#  Read helpers                                                        #
# ------------------------------------------------------------------ #

def get_fact_table() -> pd.DataFrame:
    return st.session_state["fact_table"]


def get_dim_config() -> pd.DataFrame:
    return st.session_state["dim_config"]


def get_measure_config() -> pd.DataFrame:
    return st.session_state["measure_config"]


def get_adj_headers() -> pd.DataFrame:
    return st.session_state["adj_header"]


def get_adj_line_items() -> pd.DataFrame:
    return st.session_state["adj_line_items"]


def get_status_history() -> pd.DataFrame:
    return st.session_state["status_history"]


def get_current_user() -> str:
    return st.session_state.get("current_user", "sarah.chen")


# ------------------------------------------------------------------ #
#  Computed view: FACT_ADJUSTED  (mirrors the Dynamic Table)           #
# ------------------------------------------------------------------ #

def get_fact_adjusted() -> pd.DataFrame:
    """
    Recomputes the adjusted fact view on every call.
    In Snowflake this is a Dynamic Table with 1-min lag.
    """
    fact = get_fact_table().copy()
    headers = get_adj_headers()
    lines = get_adj_line_items()

    applied_ids = headers[headers["ADJ_STATUS"] == "APPLIED"]["ADJ_ID"].tolist()
    applied_lines = lines[lines["ADJ_ID"].isin(applied_ids)]

    if applied_lines.empty:
        fact["IS_ADJUSTED"] = False
        return fact

    # Sum deltas per FACT_ID
    delta_agg = (
        applied_lines
        .groupby("FACT_ID")[["AMOUNT_DELTA", "QUANTITY_DELTA", "NOTIONAL_DELTA"]]
        .sum()
        .reset_index()
    )

    merged = fact.merge(delta_agg, on="FACT_ID", how="left")
    merged["AMOUNT_DELTA"] = merged["AMOUNT_DELTA"].fillna(0)
    merged["QUANTITY_DELTA"] = merged["QUANTITY_DELTA"].fillna(0)
    merged["NOTIONAL_DELTA"] = merged["NOTIONAL_DELTA"].fillna(0)

    merged["IS_ADJUSTED"] = merged["AMOUNT_DELTA"] != 0
    merged["AMOUNT"] = merged["AMOUNT"] + merged["AMOUNT_DELTA"]
    merged["QUANTITY"] = merged["QUANTITY"] + merged["QUANTITY_DELTA"]
    merged["NOTIONAL"] = merged["NOTIONAL"] + merged["NOTIONAL_DELTA"]

    merged.drop(columns=["AMOUNT_DELTA", "QUANTITY_DELTA", "NOTIONAL_DELTA"], inplace=True)
    return merged


# ------------------------------------------------------------------ #
#  Write helpers                                                       #
# ------------------------------------------------------------------ #

def _next_adj_id() -> int:
    aid = st.session_state["next_adj_id"]
    st.session_state["next_adj_id"] = aid + 1
    return aid


def _next_line_id() -> int:
    lid = st.session_state["next_line_id"]
    st.session_state["next_line_id"] = lid + 1
    return lid


def _next_hist_id() -> int:
    hid = st.session_state["next_hist_id"]
    st.session_state["next_hist_id"] = hid + 1
    return hid


def _add_history(adj_id: int, old_status, new_status: str, comment: str = ""):
    row = {
        "HISTORY_ID": _next_hist_id(),
        "ADJ_ID": adj_id,
        "OLD_STATUS": old_status,
        "NEW_STATUS": new_status,
        "CHANGED_BY": get_current_user(),
        "CHANGED_AT": datetime.now(),
        "COMMENT": comment,
    }
    st.session_state["status_history"] = pd.concat(
        [st.session_state["status_history"], pd.DataFrame([row])],
        ignore_index=True,
    )


# ------------------------------------------------------------------ #
#  Preview adjustment (read-only, no writes)                           #
# ------------------------------------------------------------------ #

def preview_adjustment(
    adj_type: str,
    target_date: date,
    filters: dict,
    scale_factor: float = 1.0,
    roll_source_date: date = None,
) -> pd.DataFrame:
    """
    Returns a DataFrame with ORIGINAL / DELTA / NEW for each matched row.
    Mirrors SP_PREVIEW_ADJUSTMENT in Snowflake.
    """
    fact = get_fact_table()
    mask = fact["BUSINESS_DATE"] == target_date
    for col, val in filters.items():
        if val:
            mask = mask & (fact[col] == val)

    matched = fact[mask].copy()
    if matched.empty:
        return pd.DataFrame()

    measures = ["AMOUNT", "QUANTITY", "NOTIONAL"]

    if adj_type == "FLATTEN":
        for m in measures:
            matched[f"{m}_DELTA"] = -matched[m]
            matched[f"{m}_NEW"] = 0.0

    elif adj_type == "SCALE":
        for m in measures:
            matched[f"{m}_DELTA"] = round(matched[m] * (scale_factor - 1), 2)
            matched[f"{m}_NEW"] = round(matched[m] * scale_factor, 2)

    elif adj_type == "ROLL":
        source = get_fact_table()
        src_mask = source["BUSINESS_DATE"] == roll_source_date
        for col, val in filters.items():
            if val:
                src_mask = src_mask & (source[col] == val)
        source_rows = source[src_mask]

        # Join on dimension keys
        dim_keys = ["ENTITY_KEY", "PRODUCT_KEY", "ACCOUNT_KEY"]
        matched = matched.merge(
            source_rows[dim_keys + measures].rename(
                columns={m: f"{m}_SRC" for m in measures}
            ),
            on=dim_keys,
            how="left",
        )
        for m in measures:
            src_col = f"{m}_SRC"
            matched[src_col] = matched[src_col].fillna(matched[m])
            matched[f"{m}_DELTA"] = round(
                matched[src_col] * scale_factor - matched[m], 2
            )
            matched[f"{m}_NEW"] = round(matched[src_col] * scale_factor, 2)
            matched.drop(columns=[src_col], inplace=True)

    return matched


# ------------------------------------------------------------------ #
#  Create adjustment                                                   #
# ------------------------------------------------------------------ #

def create_adjustment(
    adj_type: str,
    target_date: date,
    filters: dict,
    scale_factor: float,
    roll_source_date: date,
    business_reason: str,
    ticket_reference: str,
    submit_for_approval: bool,
) -> int:
    """Create header + line items. Returns the new ADJ_ID."""
    preview = preview_adjustment(adj_type, target_date, filters, scale_factor, roll_source_date)
    if preview.empty:
        return -1

    adj_id = _next_adj_id()
    status = "PENDING_APPROVAL" if submit_for_approval else "DRAFT"

    total_delta = round(preview["AMOUNT_DELTA"].sum(), 2)

    header = {
        "ADJ_ID": adj_id,
        "ADJ_TYPE": adj_type,
        "ADJ_STATUS": status,
        "TARGET_DATE": target_date,
        "SCALE_FACTOR": scale_factor if adj_type in ("SCALE", "ROLL") else None,
        "ROLL_SOURCE_DATE": roll_source_date if adj_type == "ROLL" else None,
        "FILTER_CRITERIA": json.dumps({k: v for k, v in filters.items() if v}),
        "AFFECTED_ROWS": len(preview),
        "TOTAL_DELTA_AMOUNT": total_delta,
        "BUSINESS_REASON": business_reason,
        "TICKET_REFERENCE": ticket_reference,
        "CREATED_BY": get_current_user(),
        "CREATED_AT": datetime.now(),
        "APPROVED_BY": None,
        "APPROVED_AT": None,
        "APPLIED_AT": None,
        "AI_SUMMARY": _generate_mock_summary(adj_type, filters, target_date, total_delta, len(preview), business_reason),
    }

    # Line items
    lines = []
    for _, r in preview.iterrows():
        lines.append({
            "LINE_ID": _next_line_id(),
            "ADJ_ID": adj_id,
            "FACT_ID": r["FACT_ID"],
            "BUSINESS_DATE": r["BUSINESS_DATE"],
            "ENTITY_KEY": r["ENTITY_KEY"],
            "PRODUCT_KEY": r["PRODUCT_KEY"],
            "ACCOUNT_KEY": r["ACCOUNT_KEY"],
            "CURRENCY_KEY": r["CURRENCY_KEY"],
            "COUNTERPARTY_KEY": r["COUNTERPARTY_KEY"],
            "COST_CENTER_KEY": r["COST_CENTER_KEY"],
            "AMOUNT_DELTA": r["AMOUNT_DELTA"],
            "QUANTITY_DELTA": r["QUANTITY_DELTA"],
            "NOTIONAL_DELTA": r["NOTIONAL_DELTA"],
            "AMOUNT_ORIGINAL": r["AMOUNT"],
            "QUANTITY_ORIGINAL": r["QUANTITY"],
            "NOTIONAL_ORIGINAL": r["NOTIONAL"],
        })

    st.session_state["adj_header"] = pd.concat(
        [st.session_state["adj_header"], pd.DataFrame([header])],
        ignore_index=True,
    )
    st.session_state["adj_line_items"] = pd.concat(
        [st.session_state["adj_line_items"], pd.DataFrame(lines)],
        ignore_index=True,
    )

    _add_history(adj_id, None, "DRAFT", "Created")
    if submit_for_approval:
        _add_history(adj_id, "DRAFT", "PENDING_APPROVAL", "Submitted for approval")

    return adj_id


# ------------------------------------------------------------------ #
#  Status transitions                                                  #
# ------------------------------------------------------------------ #

VALID_TRANSITIONS = {
    "DRAFT": ["PENDING_APPROVAL"],
    "PENDING_APPROVAL": ["APPROVED", "REJECTED"],
    "APPROVED": ["APPLIED"],
    "APPLIED": ["REVERSED"],
    "REJECTED": ["DRAFT"],
    "REVERSED": [],
}


def update_status(adj_id: int, new_status: str, comment: str = "") -> tuple[bool, str]:
    """
    Attempt a status transition. Returns (success, message).
    Mirrors SP_UPDATE_ADJUSTMENT_STATUS.
    """
    headers = st.session_state["adj_header"]
    idx = headers.index[headers["ADJ_ID"] == adj_id]
    if len(idx) == 0:
        return False, f"Adjustment {adj_id} not found."

    i = idx[0]
    current = headers.at[i, "ADJ_STATUS"]
    user = get_current_user()

    # Validate transition
    if new_status not in VALID_TRANSITIONS.get(current, []):
        return False, f"Cannot move from {current} → {new_status}."

    # Self-approval guard
    if new_status == "APPROVED" and user == headers.at[i, "CREATED_BY"]:
        return False, "Self-approval is not allowed."

    # Update
    st.session_state["adj_header"].at[i, "ADJ_STATUS"] = new_status
    now = datetime.now()

    if new_status == "APPROVED":
        st.session_state["adj_header"].at[i, "APPROVED_BY"] = user
        st.session_state["adj_header"].at[i, "APPROVED_AT"] = now
    elif new_status == "APPLIED":
        st.session_state["adj_header"].at[i, "APPLIED_AT"] = now

    _add_history(adj_id, current, new_status, comment)

    # Generate AI summary on approval
    if new_status in ("APPROVED", "APPLIED") and pd.isna(headers.at[i, "AI_SUMMARY"]):
        row = st.session_state["adj_header"].loc[i]
        summary = _generate_mock_summary(
            row["ADJ_TYPE"], {}, row["TARGET_DATE"],
            row["TOTAL_DELTA_AMOUNT"], row["AFFECTED_ROWS"],
            row["BUSINESS_REASON"],
        )
        st.session_state["adj_header"].at[i, "AI_SUMMARY"] = summary

    return True, f"Adjustment {adj_id} moved to {new_status}."


def reverse_adjustment(adj_id: int) -> tuple[bool, str]:
    """Create a reversal adjustment (negated deltas) and mark original as REVERSED."""
    headers = st.session_state["adj_header"]
    idx = headers.index[headers["ADJ_ID"] == adj_id]
    if len(idx) == 0:
        return False, "Not found."
    i = idx[0]
    if headers.at[i, "ADJ_STATUS"] != "APPLIED":
        return False, "Only APPLIED adjustments can be reversed."

    lines = get_adj_line_items()
    orig_lines = lines[lines["ADJ_ID"] == adj_id]

    # New reversal adjustment
    rev_id = _next_adj_id()
    rev_header = {
        "ADJ_ID": rev_id,
        "ADJ_TYPE": headers.at[i, "ADJ_TYPE"],
        "ADJ_STATUS": "APPLIED",
        "TARGET_DATE": headers.at[i, "TARGET_DATE"],
        "SCALE_FACTOR": None,
        "ROLL_SOURCE_DATE": None,
        "FILTER_CRITERIA": headers.at[i, "FILTER_CRITERIA"],
        "AFFECTED_ROWS": len(orig_lines),
        "TOTAL_DELTA_AMOUNT": round(-headers.at[i, "TOTAL_DELTA_AMOUNT"], 2),
        "BUSINESS_REASON": f"Reversal of ADJ-{adj_id}",
        "TICKET_REFERENCE": headers.at[i, "TICKET_REFERENCE"],
        "CREATED_BY": get_current_user(),
        "CREATED_AT": datetime.now(),
        "APPROVED_BY": get_current_user(),
        "APPROVED_AT": datetime.now(),
        "APPLIED_AT": datetime.now(),
        "AI_SUMMARY": f"Automatic reversal of adjustment ADJ-{adj_id}. All deltas negated.",
    }

    rev_lines = []
    for _, r in orig_lines.iterrows():
        rev_lines.append({
            "LINE_ID": _next_line_id(),
            "ADJ_ID": rev_id,
            "FACT_ID": r["FACT_ID"],
            "BUSINESS_DATE": r["BUSINESS_DATE"],
            "ENTITY_KEY": r["ENTITY_KEY"],
            "PRODUCT_KEY": r["PRODUCT_KEY"],
            "ACCOUNT_KEY": r["ACCOUNT_KEY"],
            "CURRENCY_KEY": r["CURRENCY_KEY"],
            "COUNTERPARTY_KEY": r["COUNTERPARTY_KEY"],
            "COST_CENTER_KEY": r["COST_CENTER_KEY"],
            "AMOUNT_DELTA": -r["AMOUNT_DELTA"],
            "QUANTITY_DELTA": -r["QUANTITY_DELTA"],
            "NOTIONAL_DELTA": -r["NOTIONAL_DELTA"],
            "AMOUNT_ORIGINAL": r["AMOUNT_ORIGINAL"],
            "QUANTITY_ORIGINAL": r["QUANTITY_ORIGINAL"],
            "NOTIONAL_ORIGINAL": r["NOTIONAL_ORIGINAL"],
        })

    st.session_state["adj_header"] = pd.concat(
        [st.session_state["adj_header"], pd.DataFrame([rev_header])],
        ignore_index=True,
    )
    st.session_state["adj_line_items"] = pd.concat(
        [st.session_state["adj_line_items"], pd.DataFrame(rev_lines)],
        ignore_index=True,
    )

    # Mark original as reversed
    st.session_state["adj_header"].at[i, "ADJ_STATUS"] = "REVERSED"
    _add_history(adj_id, "APPLIED", "REVERSED", f"Reversed by ADJ-{rev_id}")
    _add_history(rev_id, None, "APPLIED", f"Reversal of ADJ-{adj_id}")

    return True, f"ADJ-{adj_id} reversed. Reversal: ADJ-{rev_id}."


# ------------------------------------------------------------------ #
#  Mock AI summary (simulates Cortex COMPLETE)                         #
# ------------------------------------------------------------------ #

def _generate_mock_summary(adj_type, filters, target_date, total_delta, rows, reason):
    """Generate a realistic-sounding summary without calling an LLM."""
    type_desc = {
        "FLATTEN": "zeroing out",
        "SCALE": "scaling",
        "ROLL": "rolling forward",
    }.get(adj_type, adj_type.lower())

    filter_desc = ", ".join(f"{k}={v}" for k, v in filters.items() if v) if filters else "all entities"

    return (
        f"{adj_type.title()} adjustment {type_desc} {rows} row(s) "
        f"for {filter_desc} on {target_date}. "
        f"Net impact: ${total_delta:+,.2f}. "
        f"Reason: {reason}"
    )
