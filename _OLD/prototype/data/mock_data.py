"""
Scope Configuration & Mock Data Generator
==========================================
Each scope represents a different fact source table with its own
dimension keys and measures.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# ─────────────────────────────────────────────────────────────────────
# SCOPE DEFINITIONS
# ─────────────────────────────────────────────────────────────────────
SCOPES = {
    "pnl": {
        "id": "pnl",
        "name": "Profit & Loss",
        "icon": "💰",
        "table": "FACT_PNL",
        "description": "Daily P&L by entity, desk and product",
        "dimensions": [
            {"key": "ENTITY_KEY",   "label": "Entity",   "values": ["ENT_US", "ENT_UK", "ENT_JP", "ENT_SG"]},
            {"key": "DESK_KEY",     "label": "Desk",     "values": ["RATES", "FX", "CREDIT", "EQUITY"]},
            {"key": "PRODUCT_KEY",  "label": "Product",  "values": ["SWAP", "OPTION", "FUTURE", "BOND", "FRA"]},
            {"key": "BOOK_KEY",     "label": "Book",     "values": ["BOOK_A", "BOOK_B", "BOOK_C"]},
        ],
        "measures": [
            {"key": "MTM",       "label": "Mark-to-Market", "unit": "USD"},
            {"key": "DELTA_PNL", "label": "Delta P&L",      "unit": "USD"},
            {"key": "VEGA_PNL",  "label": "Vega P&L",       "unit": "USD"},
        ],
    },
    "risk": {
        "id": "risk",
        "name": "Risk Sensitivities",
        "icon": "📊",
        "table": "FACT_RISK",
        "description": "Daily risk sensitivities by entity and risk type",
        "dimensions": [
            {"key": "ENTITY_KEY",    "label": "Entity",    "values": ["ENT_US", "ENT_UK", "ENT_JP"]},
            {"key": "RISK_TYPE_KEY", "label": "Risk Type", "values": ["IR_DELTA", "IR_VEGA", "FX_DELTA", "EQ_DELTA", "CR_SPREAD"]},
            {"key": "TENOR_KEY",     "label": "Tenor",     "values": ["1M", "3M", "6M", "1Y", "2Y", "5Y", "10Y"]},
            {"key": "SCENARIO_KEY",  "label": "Scenario",  "values": ["BASE", "STRESS_UP", "STRESS_DOWN"]},
        ],
        "measures": [
            {"key": "SENSITIVITY",  "label": "Sensitivity",  "unit": "USD"},
            {"key": "VAR_99",       "label": "VaR 99%",      "unit": "USD"},
        ],
    },
    "fx_position": {
        "id": "fx_position",
        "name": "FX Positions",
        "icon": "💱",
        "table": "FACT_FX_POSITION",
        "description": "FX positions by currency pair and settlement",
        "dimensions": [
            {"key": "ENTITY_KEY",     "label": "Entity",        "values": ["ENT_US", "ENT_UK", "ENT_JP", "ENT_SG"]},
            {"key": "CCY_PAIR_KEY",   "label": "Currency Pair",  "values": ["USD/JPY", "EUR/USD", "GBP/USD", "USD/SGD", "EUR/GBP"]},
            {"key": "SETTLE_KEY",     "label": "Settlement",     "values": ["SPOT", "1W", "1M", "3M"]},
        ],
        "measures": [
            {"key": "NOTIONAL",   "label": "Notional",   "unit": "USD"},
            {"key": "LOCAL_AMT",  "label": "Local Amt",  "unit": "LCL"},
        ],
    },
}

DEFAULT_SCOPE = "pnl"


# ─────────────────────────────────────────────────────────────────────
# DATA GENERATORS
# ─────────────────────────────────────────────────────────────────────
def _random_rows(scope_cfg: dict, n_days: int = 5, seed: int = 42) -> pd.DataFrame:
    """Generate random fact rows for a scope."""
    rng = np.random.default_rng(seed)
    base_date = datetime(2025, 3, 15)
    dates = [base_date + timedelta(days=i) for i in range(n_days)]

    dim_keys = [d["key"] for d in scope_cfg["dimensions"]]
    meas_keys = [m["key"] for m in scope_cfg["measures"]]

    # Build all combos (subset for manageability)
    from itertools import product as cartprod
    dim_vals = [d["values"] for d in scope_cfg["dimensions"]]
    combos = list(cartprod(*dim_vals))
    # Sample a subset
    n_combos = min(len(combos), 30)
    idx = rng.choice(len(combos), size=n_combos, replace=False)
    combos = [combos[i] for i in sorted(idx)]

    rows = []
    row_id = 1
    for dt in dates:
        for combo in combos:
            row = {"ROW_ID": row_id, "AS_OF_DATE": dt.strftime("%Y-%m-%d")}
            for k, v in zip(dim_keys, combo):
                row[k] = v
            for mk in meas_keys:
                row[mk] = round(rng.normal(0, 5_000_000), 2)
            rows.append(row)
            row_id += 1
    return pd.DataFrame(rows)


def generate_fact_table(scope_id: str) -> pd.DataFrame:
    """Return fact DataFrame for the given scope."""
    cfg = SCOPES[scope_id]
    return _random_rows(cfg)


def generate_sample_adjustments(scope_id: str):
    """Return (headers, lines, history) with ready-made sample adjustments."""
    cfg = SCOPES[scope_id]
    dim_keys = [d["key"] for d in cfg["dimensions"]]
    meas_keys = [m["key"] for m in cfg["measures"]]
    first_dim = dim_keys[0]
    first_meas = meas_keys[0]
    now = datetime.now()

    headers = pd.DataFrame([
        {
            "ADJ_ID": "ADJ-001",
            "SCOPE": scope_id,
            "ADJ_TYPE": "FLATTEN",
            "STATUS": "APPLIED",
            "FREQUENCY": "ADHOC",
            "START_COB": "",
            "END_COB": "",
            "JUSTIFICATION": "Month-end P&L reset",
            "BUSINESS_DATE": "2025-03-15",
            "CREATED_BY": "Sarah",
            "CREATED_AT": (now - timedelta(hours=5)).isoformat(),
            "FILTER_JSON": f'{{{first_dim}: "{cfg["dimensions"][0]["values"][0]}"}}',
        },
        {
            "ADJ_ID": "ADJ-002",
            "SCOPE": scope_id,
            "ADJ_TYPE": "SCALE",
            "STATUS": "PENDING_APPROVAL",
            "FREQUENCY": "RECURRING",
            "START_COB": "2025-03-16",
            "END_COB": "2025-03-19",
            "JUSTIFICATION": "Quarterly rebalance",
            "BUSINESS_DATE": "2025-03-16",
            "CREATED_BY": "James",
            "CREATED_AT": (now - timedelta(hours=2)).isoformat(),
            "FILTER_JSON": f'{{{first_dim}: "{cfg["dimensions"][0]["values"][1]}"}}',
        },
        {
            "ADJ_ID": "ADJ-003",
            "SCOPE": scope_id,
            "ADJ_TYPE": "ROLL",
            "STATUS": "DRAFT",
            "FREQUENCY": "RECURRING",
            "START_COB": "2025-03-17",
            "END_COB": "",
            "JUSTIFICATION": "Forward fill missing day",
            "BUSINESS_DATE": "2025-03-17",
            "CREATED_BY": "Sarah",
            "CREATED_AT": now.isoformat(),
            "FILTER_JSON": "{}",
        },
    ])

    lines = pd.DataFrame([
        {"ADJ_ID": "ADJ-001", "LINE_ID": 1, first_dim: cfg["dimensions"][0]["values"][0],
         "MEASURE": first_meas, "ORIGINAL_VALUE": 1_000_000, "DELTA_VALUE": -1_000_000, "NEW_VALUE": 0},
        {"ADJ_ID": "ADJ-001", "LINE_ID": 2, first_dim: cfg["dimensions"][0]["values"][0],
         "MEASURE": meas_keys[-1], "ORIGINAL_VALUE": 500_000, "DELTA_VALUE": -500_000, "NEW_VALUE": 0},
        {"ADJ_ID": "ADJ-002", "LINE_ID": 1, first_dim: cfg["dimensions"][0]["values"][1],
         "MEASURE": first_meas, "ORIGINAL_VALUE": 2_000_000, "DELTA_VALUE": 200_000, "NEW_VALUE": 2_200_000},
        {"ADJ_ID": "ADJ-003", "LINE_ID": 1, first_dim: cfg["dimensions"][0]["values"][0],
         "MEASURE": first_meas, "ORIGINAL_VALUE": 0, "DELTA_VALUE": 800_000, "NEW_VALUE": 800_000},
    ])

    history = pd.DataFrame([
        {"ADJ_ID": "ADJ-001", "FROM_STATUS": "DRAFT",            "TO_STATUS": "PENDING_APPROVAL", "CHANGED_BY": "Sarah", "CHANGED_AT": (now - timedelta(hours=4)).isoformat(), "COMMENT": "Ready for review"},
        {"ADJ_ID": "ADJ-001", "FROM_STATUS": "PENDING_APPROVAL", "TO_STATUS": "APPROVED",          "CHANGED_BY": "Manager", "CHANGED_AT": (now - timedelta(hours=3)).isoformat(), "COMMENT": "Approved"},
        {"ADJ_ID": "ADJ-001", "FROM_STATUS": "APPROVED",          "TO_STATUS": "APPLIED",           "CHANGED_BY": "System", "CHANGED_AT": (now - timedelta(hours=2)).isoformat(), "COMMENT": "Auto-applied"},
        {"ADJ_ID": "ADJ-002", "FROM_STATUS": "DRAFT",            "TO_STATUS": "PENDING_APPROVAL", "CHANGED_BY": "James", "CHANGED_AT": (now - timedelta(hours=1)).isoformat(), "COMMENT": "Submitted"},
    ])

    return headers, lines, history


def get_scope_choices() -> list[dict]:
    """Return list of scope dicts for UI selectors."""
    return [{"id": s["id"], "name": s["name"], "icon": s["icon"], "desc": s["description"]}
            for s in SCOPES.values()]
