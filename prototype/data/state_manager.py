"""
State Manager — in-memory database backed by st.session_state
==============================================================
Every operation is scope-aware.
"""
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
from data.mock_data import SCOPES, generate_fact_table, generate_sample_adjustments, DEFAULT_SCOPE

VALID_TRANSITIONS = {
    "DRAFT":            ["PENDING_APPROVAL"],
    "PENDING_APPROVAL": ["APPROVED", "REJECTED"],
    "APPROVED":         ["APPLIED"],
    "APPLIED":          ["REVERSED"],
    "REJECTED":         [],
    "REVERSED":         [],
}

STATUS_COLORS = {
    "DRAFT":            "#78909C",
    "PENDING_APPROVAL": "#FFA726",
    "APPROVED":         "#29B6F6",
    "APPLIED":          "#66BB6A",
    "REJECTED":         "#EF5350",
    "REVERSED":         "#AB47BC",
}


# ─────────────────────────────────────────────────────────────────────
# INIT
# ─────────────────────────────────────────────────────────────────────
def init_state():
    """Bootstrap session state with all scopes' data."""
    if "initialized" in st.session_state:
        return
    st.session_state["current_scope"] = DEFAULT_SCOPE
    st.session_state["current_user"] = "Sarah"

    for scope_id, cfg in SCOPES.items():
        pfx = f"scope_{scope_id}"
        st.session_state[f"{pfx}_fact"] = generate_fact_table(scope_id)
        h, l, hist = generate_sample_adjustments(scope_id)
        st.session_state[f"{pfx}_headers"] = h
        st.session_state[f"{pfx}_lines"] = l
        st.session_state[f"{pfx}_history"] = hist

    st.session_state["initialized"] = True


def _pfx():
    return f"scope_{st.session_state['current_scope']}"


def current_scope_cfg():
    return SCOPES[st.session_state["current_scope"]]


# ─────────────────────────────────────────────────────────────────────
# GETTERS
# ─────────────────────────────────────────────────────────────────────
def get_fact_table() -> pd.DataFrame:
    return st.session_state[f"{_pfx()}_fact"]


def get_headers() -> pd.DataFrame:
    return st.session_state[f"{_pfx()}_headers"]


def get_lines() -> pd.DataFrame:
    return st.session_state[f"{_pfx()}_lines"]


def get_history() -> pd.DataFrame:
    return st.session_state[f"{_pfx()}_history"]


def get_fact_adjusted() -> pd.DataFrame:
    """Compute adjusted fact view: original + applied deltas."""
    fact = get_fact_table().copy()
    headers = get_headers()
    lines = get_lines()
    cfg = current_scope_cfg()
    meas_keys = [m["key"] for m in cfg["measures"]]

    applied = headers[headers["STATUS"] == "APPLIED"]
    if applied.empty:
        fact["_SOURCE"] = "ORIGINAL"
        return fact

    applied_lines = lines[lines["ADJ_ID"].isin(applied["ADJ_ID"])]

    fact["_SOURCE"] = "ORIGINAL"
    # Build delta rows (simplified: apply sum of deltas per measure to matching rows)
    for _, ln in applied_lines.iterrows():
        meas = ln.get("MEASURE")
        if meas and meas in meas_keys:
            delta_val = ln.get("DELTA_VALUE", 0)
            # Apply to all matching rows (simplified)
            mask = fact[cfg["dimensions"][0]["key"]] == ln.get(cfg["dimensions"][0]["key"])
            if mask.any():
                fact.loc[mask, meas] = fact.loc[mask, meas] + delta_val

    return fact


# ─────────────────────────────────────────────────────────────────────
# ADJUSTMENT OPERATIONS
# ─────────────────────────────────────────────────────────────────────
def preview_adjustment(filters: dict, adj_type: str, params: dict) -> pd.DataFrame:
    """Preview what the adjustment would look like."""
    fact = get_fact_table()
    cfg = current_scope_cfg()
    meas_keys = [m["key"] for m in cfg["measures"]]

    mask = pd.Series(True, index=fact.index)
    for k, v in filters.items():
        if v and k in fact.columns:
            mask &= fact[k].isin(v) if isinstance(v, list) else fact[k] == v

    matched = fact[mask].copy()
    if matched.empty:
        return matched

    preview = matched.copy()
    for m in meas_keys:
        preview[f"ORIG_{m}"] = preview[m]
        if adj_type == "FLATTEN":
            preview[f"DELTA_{m}"] = -preview[m]
            preview[f"NEW_{m}"] = 0
        elif adj_type == "SCALE":
            factor = params.get("factor", 1.0)
            preview[f"DELTA_{m}"] = preview[m] * (factor - 1)
            preview[f"NEW_{m}"] = preview[m] * factor
        elif adj_type == "ROLL":
            source_date = params.get("source_date", "")
            source = fact[(fact["AS_OF_DATE"] == source_date) & mask]
            if not source.empty:
                scale = params.get("scale", 1.0)
                avg_source = source[m].mean()
                preview[f"DELTA_{m}"] = avg_source * scale - preview[m]
                preview[f"NEW_{m}"] = avg_source * scale
            else:
                preview[f"DELTA_{m}"] = 0
                preview[f"NEW_{m}"] = preview[m]
    return preview


def create_adjustment(filters: dict, adj_type: str, params: dict, justification: str, business_date: str) -> str:
    """Create a new adjustment and return its ID."""
    scope_id = st.session_state["current_scope"]
    user = st.session_state["current_user"]
    now = datetime.now().isoformat()

    existing = get_headers()
    n = len(existing) + 1
    adj_id = f"ADJ-{n:03d}"

    # Preview to build lines
    preview = preview_adjustment(filters, adj_type, params)
    cfg = current_scope_cfg()
    meas_keys = [m["key"] for m in cfg["measures"]]

    new_lines = []
    line_id = 1
    for _, row in preview.iterrows():
        for m in meas_keys:
            if f"DELTA_{m}" in row and row[f"DELTA_{m}"] != 0:
                ln = {
                    "ADJ_ID": adj_id,
                    "LINE_ID": line_id,
                    cfg["dimensions"][0]["key"]: row.get(cfg["dimensions"][0]["key"]),
                    "MEASURE": m,
                    "ORIGINAL_VALUE": row.get(f"ORIG_{m}", 0),
                    "DELTA_VALUE": row.get(f"DELTA_{m}", 0),
                    "NEW_VALUE": row.get(f"NEW_{m}", 0),
                }
                new_lines.append(ln)
                line_id += 1

    import json
    header = {
        "ADJ_ID": adj_id,
        "SCOPE": scope_id,
        "ADJ_TYPE": adj_type,
        "STATUS": "DRAFT",
        "JUSTIFICATION": justification,
        "BUSINESS_DATE": business_date,
        "CREATED_BY": user,
        "CREATED_AT": now,
        "FILTER_JSON": json.dumps(filters),
    }

    pfx = _pfx()
    st.session_state[f"{pfx}_headers"] = pd.concat(
        [get_headers(), pd.DataFrame([header])], ignore_index=True
    )
    if new_lines:
        st.session_state[f"{pfx}_lines"] = pd.concat(
            [get_lines(), pd.DataFrame(new_lines)], ignore_index=True
        )
    hist = {
        "ADJ_ID": adj_id,
        "FROM_STATUS": "",
        "TO_STATUS": "DRAFT",
        "CHANGED_BY": user,
        "CHANGED_AT": now,
        "COMMENT": "Created",
    }
    st.session_state[f"{pfx}_history"] = pd.concat(
        [get_history(), pd.DataFrame([hist])], ignore_index=True
    )
    return adj_id


def update_status(adj_id: str, new_status: str, comment: str = "") -> bool:
    """Transition an adjustment to a new status."""
    pfx = _pfx()
    headers = get_headers()
    idx = headers.index[headers["ADJ_ID"] == adj_id]
    if idx.empty:
        return False
    current = headers.loc[idx[0], "STATUS"]
    if new_status not in VALID_TRANSITIONS.get(current, []):
        return False

    st.session_state[f"{pfx}_headers"].loc[idx[0], "STATUS"] = new_status

    hist = {
        "ADJ_ID": adj_id,
        "FROM_STATUS": current,
        "TO_STATUS": new_status,
        "CHANGED_BY": st.session_state["current_user"],
        "CHANGED_AT": datetime.now().isoformat(),
        "COMMENT": comment or f"{current} → {new_status}",
    }
    st.session_state[f"{pfx}_history"] = pd.concat(
        [get_history(), pd.DataFrame([hist])], ignore_index=True
    )
    return True


def reverse_adjustment(adj_id: str, comment: str = "") -> bool:
    return update_status(adj_id, "REVERSED", comment or "Reversed by user")
