"""
Mock Data — Multi-scope fact tables and metadata
=================================================
Three realistic financial scopes with different dimension sets.
All data is generated deterministically for repeatable demos.
"""
import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime
import random

# ──────────────────────────────────────────────────────────────────────────────
# SCOPE METADATA  (drives the entire UI — metadata-driven architecture)
# ──────────────────────────────────────────────────────────────────────────────

SCOPES = {
    "PNL": {
        "key": "PNL",
        "label": "P&L",
        "full_label": "Profit & Loss",
        "description": "Daily P&L positions from MUREX front-office system",
        "icon": "📈",
        "source_system": "MUREX",
        "color": "#D50032",
        "bg_color": "#FFF0F3",
        "dimensions": [
            {
                "column": "entity_key",
                "label": "Entity",
                "required": True,
                "multi": True,
                "values": ["US_HQ", "EU_LONDON", "EU_FRANKFURT", "APAC_TOKYO", "APAC_HK"],
                "tooltip": "Legal entity or regional branch"
            },
            {
                "column": "desk_key",
                "label": "Desk",
                "required": False,
                "multi": True,
                "values": ["RATES", "FX", "EQUITY", "CREDIT", "COMMODITIES", "MONEY_MKT"],
                "tooltip": "Trading desk responsible for the position"
            },
            {
                "column": "product_key",
                "label": "Product",
                "required": False,
                "multi": True,
                "values": ["IR_SWAP", "FX_SPOT", "FX_FORWARD", "FX_OPTION", "EQUITY_SWAP", "CDS", "REPO", "T_BILL"],
                "tooltip": "Financial instrument type"
            },
            {
                "column": "currency_key",
                "label": "Currency",
                "required": False,
                "multi": True,
                "values": ["USD", "EUR", "GBP", "JPY", "CHF", "AUD", "CAD"],
                "tooltip": "Reporting currency"
            },
            {
                "column": "account_key",
                "label": "Account",
                "required": False,
                "multi": True,
                "values": ["TRADING_PNL", "BANKING_PNL", "HEDGE_PNL", "FEE_INCOME"],
                "tooltip": "P&L account classification"
            },
        ],
        "measures": [
            {"column": "amount",       "label": "P&L Amount (USD)", "fmt": "currency", "primary": True},
            {"column": "local_amount", "label": "Local Amount",      "fmt": "currency", "primary": False},
        ]
    },

    "RISK": {
        "key": "RISK",
        "label": "Risk",
        "full_label": "Risk Sensitivities",
        "description": "Greeks and sensitivities from CALYPSO risk engine",
        "icon": "⚡",
        "source_system": "CALYPSO",
        "color": "#1565C0",
        "bg_color": "#EBF3FF",
        "dimensions": [
            {
                "column": "entity_key",
                "label": "Entity",
                "required": True,
                "multi": True,
                "values": ["US_HQ", "EU_LONDON", "EU_FRANKFURT", "APAC_TOKYO", "APAC_HK"],
                "tooltip": "Legal entity or regional branch"
            },
            {
                "column": "risk_type_key",
                "label": "Risk Type",
                "required": False,
                "multi": True,
                "values": ["DELTA", "GAMMA", "VEGA", "THETA", "RHO", "DV01"],
                "tooltip": "Type of risk sensitivity (Greek)"
            },
            {
                "column": "product_key",
                "label": "Product",
                "required": False,
                "multi": True,
                "values": ["IR_SWAP", "FX_SPOT", "FX_FORWARD", "FX_OPTION", "EQUITY_SWAP", "CDS"],
                "tooltip": "Financial instrument type"
            },
            {
                "column": "currency_key",
                "label": "Currency",
                "required": False,
                "multi": True,
                "values": ["USD", "EUR", "GBP", "JPY", "CHF", "AUD", "CAD"],
                "tooltip": "Risk currency"
            },
            {
                "column": "tenor_key",
                "label": "Tenor",
                "required": False,
                "multi": True,
                "values": ["ON", "1W", "1M", "3M", "6M", "1Y", "2Y", "5Y", "10Y", "30Y"],
                "tooltip": "Tenor bucket for the sensitivity"
            },
        ],
        "measures": [
            {"column": "delta_01", "label": "DV01 (USD)",  "fmt": "number",   "primary": True},
            {"column": "gamma",    "label": "Gamma",        "fmt": "number",   "primary": False},
            {"column": "vega",     "label": "Vega (USD)",   "fmt": "currency", "primary": False},
        ]
    },

    "NOSTRO": {
        "key": "NOSTRO",
        "label": "Nostro",
        "full_label": "Nostro Positions",
        "description": "Cash positions reconciled from SWIFT messages",
        "icon": "🏦",
        "source_system": "SWIFT",
        "color": "#2E7D32",
        "bg_color": "#EBF5EB",
        "dimensions": [
            {
                "column": "entity_key",
                "label": "Entity",
                "required": True,
                "multi": True,
                "values": ["US_HQ", "EU_LONDON", "EU_FRANKFURT", "APAC_TOKYO", "APAC_HK"],
                "tooltip": "Legal entity"
            },
            {
                "column": "currency_key",
                "label": "Currency",
                "required": False,
                "multi": True,
                "values": ["USD", "EUR", "GBP", "JPY", "CHF", "AUD", "CAD"],
                "tooltip": "Account currency"
            },
            {
                "column": "account_key",
                "label": "Account Type",
                "required": False,
                "multi": True,
                "values": ["NOSTRO_ACC", "VOSTRO_ACC", "CLEARING_ACC", "SETTLEMENT_ACC"],
                "tooltip": "Type of cash account"
            },
            {
                "column": "correspondent_key",
                "label": "Correspondent Bank",
                "required": False,
                "multi": True,
                "values": ["CITI_US", "DB_DE", "HSBC_UK", "BOA_US", "JPM_US", "BNP_FR"],
                "tooltip": "Correspondent bank holding the nostro"
            },
        ],
        "measures": [
            {"column": "position_amount", "label": "Position (USD)", "fmt": "currency", "primary": True},
            {"column": "local_amount",    "label": "Local Amount",   "fmt": "currency", "primary": False},
        ]
    },
}

# ──────────────────────────────────────────────────────────────────────────────
# BUSINESS DATES
# ──────────────────────────────────────────────────────────────────────────────

def get_business_dates(n: int = 20) -> list[date]:
    dates, d = [], date.today()
    while len(dates) < n:
        if d.weekday() < 5:
            dates.append(d)
        d -= timedelta(days=1)
    return sorted(dates)

BUSINESS_DATES: list[date] = get_business_dates(20)
LATEST_COB: date = BUSINESS_DATES[-1]

# ──────────────────────────────────────────────────────────────────────────────
# FACT DATA GENERATORS
# ──────────────────────────────────────────────────────────────────────────────

FX_RATES = {"USD": 1.0, "EUR": 1.09, "GBP": 1.27, "JPY": 0.0067,
            "CHF": 1.13, "AUD": 0.65, "CAD": 0.74}


def generate_pnl_data(seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    rows = []
    dims = SCOPES["PNL"]["dimensions"]
    entities  = dims[0]["values"]
    desks     = dims[1]["values"]
    products  = dims[2]["values"]
    currencies= dims[3]["values"]
    accounts  = dims[4]["values"]
    for i in range(350):
        entity   = rng.choice(entities)
        desk     = rng.choice(desks)
        product  = rng.choice(products)
        currency = rng.choice(currencies)
        account  = rng.choice(accounts)
        bdate    = rng.choice(BUSINESS_DATES)
        amount   = float(rng.normal(0, 2_500_000))
        local    = round(amount / FX_RATES[currency], 2)
        rows.append({"fact_id": i + 1, "business_date": bdate,
                     "entity_key": entity, "desk_key": desk,
                     "product_key": product, "currency_key": currency,
                     "account_key": account, "amount": round(amount, 2),
                     "local_amount": local, "source_system": "MUREX"})
    return pd.DataFrame(rows)


def generate_risk_data(seed: int = 99) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    rows = []
    dims = SCOPES["RISK"]["dimensions"]
    entities  = dims[0]["values"]
    risk_types= dims[1]["values"]
    products  = dims[2]["values"]
    currencies= dims[3]["values"]
    tenors    = dims[4]["values"]
    for i in range(250):
        bdate = rng.choice(BUSINESS_DATES)
        rows.append({"fact_id": i + 1, "business_date": bdate,
                     "entity_key":    rng.choice(entities),
                     "risk_type_key": rng.choice(risk_types),
                     "product_key":   rng.choice(products),
                     "currency_key":  rng.choice(currencies),
                     "tenor_key":     rng.choice(tenors),
                     "delta_01":  float(round(rng.normal(0, 60_000),  2)),
                     "gamma":     float(round(rng.normal(0, 1_200),   2)),
                     "vega":      float(round(rng.normal(0, 25_000),  2)),
                     "source_system": "CALYPSO"})
    return pd.DataFrame(rows)


def generate_nostro_data(seed: int = 77) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    rows = []
    dims = SCOPES["NOSTRO"]["dimensions"]
    entities     = dims[0]["values"]
    currencies   = dims[1]["values"]
    accounts     = dims[2]["values"]
    correspondents = dims[3]["values"]
    for i in range(180):
        currency = rng.choice(currencies)
        pos = float(rng.normal(50_000_000, 15_000_000))
        local = round(pos / FX_RATES[currency], 2)
        bdate = rng.choice(BUSINESS_DATES)
        rows.append({"fact_id": i + 1, "business_date": bdate,
                     "entity_key":      rng.choice(entities),
                     "currency_key":    currency,
                     "account_key":     rng.choice(accounts),
                     "correspondent_key": rng.choice(correspondents),
                     "position_amount": round(pos, 2),
                     "local_amount":    local,
                     "source_system":   "SWIFT"})
    return pd.DataFrame(rows)


FACT_GENERATORS = {
    "PNL":    generate_pnl_data,
    "RISK":   generate_risk_data,
    "NOSTRO": generate_nostro_data,
}

# ──────────────────────────────────────────────────────────────────────────────
# SAMPLE USERS
# ──────────────────────────────────────────────────────────────────────────────

USERS = [
    {"id": "marco.silva",   "name": "Marco Silva",   "initials": "MS",
     "role": "ADJ_ADMIN",    "entity_access": ["US_HQ", "EU_LONDON", "EU_FRANKFURT", "APAC_TOKYO", "APAC_HK"]},
    {"id": "sarah.chen",    "name": "Sarah Chen",    "initials": "SC",
     "role": "ADJ_APPROVER", "entity_access": ["US_HQ", "EU_LONDON"]},
    {"id": "james.wong",    "name": "James Wong",    "initials": "JW",
     "role": "ADJ_OPERATOR", "entity_access": ["APAC_TOKYO", "APAC_HK"]},
    {"id": "lisa.mueller",  "name": "Lisa Mueller",  "initials": "LM",
     "role": "ADJ_OPERATOR", "entity_access": ["EU_LONDON", "EU_FRANKFURT"]},
    {"id": "david.kumar",   "name": "David Kumar",   "initials": "DK",
     "role": "ADJ_VIEWER",   "entity_access": ["US_HQ"]},
]

ROLE_LABELS = {
    "ADJ_ADMIN":    ("Admin",    "#6A1B9A"),
    "ADJ_APPROVER": ("Approver", "#1565C0"),
    "ADJ_OPERATOR": ("Operator", "#2E7D32"),
    "ADJ_VIEWER":   ("Viewer",   "#546E7A"),
}

# ──────────────────────────────────────────────────────────────────────────────
# SEED ADJUSTMENTS  (realistic demo data)
# ──────────────────────────────────────────────────────────────────────────────

def _dt(days_ago: int, hour: int = 9, minute: int = 0) -> datetime:
    return datetime.combine(date.today() - timedelta(days=days_ago),
                            datetime.min.time()) + timedelta(hours=hour, minutes=minute)

SEED_ADJUSTMENTS = [
    {
        "adj_id": 1, "scope_key": "PNL", "adj_type": "SCALE",
        "adj_status": "APPLIED", "frequency": "ADHOC",
        "target_date": BUSINESS_DATES[-1], "start_cob": None, "end_cob": None,
        "scale_factor": 1.05, "roll_source_date": None,
        "filter_criteria": {"entity_key": ["US_HQ"], "currency_key": ["EUR"],
                            "desk_key": ["RATES"]},
        "affected_rows": 23, "business_reason": "Q1 close: EUR rates desk reallocation approved by finance controller.",
        "ticket_reference": "FIN-2847",
        "created_by": "marco.silva", "created_at": _dt(2, 8, 30),
        "approved_by": "sarah.chen", "approved_at": _dt(2, 10, 15),
        "applied_by": "marco.silva", "applied_at": _dt(2, 10, 45),
        "reversed_by": None, "reversed_at": None,
        "ai_summary": "This SCALE adjustment increases EUR Rates P&L for US HQ by 5%. Impact: +$1.2M across 23 positions. Finance sign-off on record (FIN-2847). Risk classification: LOW.",
        "queue_status": "COMPLETED",
    },
    {
        "adj_id": 2, "scope_key": "PNL", "adj_type": "FLATTEN",
        "adj_status": "APPLIED", "frequency": "ADHOC",
        "target_date": BUSINESS_DATES[-3], "start_cob": None, "end_cob": None,
        "scale_factor": 1.0, "roll_source_date": None,
        "filter_criteria": {"entity_key": ["EU_FRANKFURT"], "product_key": ["CDS"]},
        "affected_rows": 11, "business_reason": "CDS book closure — Frankfurt entity discontinued credit trading.",
        "ticket_reference": "OPS-1193",
        "created_by": "lisa.mueller", "created_at": _dt(4, 14, 0),
        "approved_by": "sarah.chen",  "approved_at": _dt(3, 9, 20),
        "applied_by": "lisa.mueller", "applied_at": _dt(3, 9, 50),
        "reversed_by": None, "reversed_at": None,
        "ai_summary": "FLATTEN zeroed out CDS positions for EU Frankfurt. 11 records set to zero. Book closure confirmed by Risk committee. Risk classification: MEDIUM.",
        "queue_status": "COMPLETED",
    },
    {
        "adj_id": 3, "scope_key": "RISK", "adj_type": "SCALE",
        "adj_status": "PENDING_APPROVAL", "frequency": "ADHOC",
        "target_date": BUSINESS_DATES[-1], "start_cob": None, "end_cob": None,
        "scale_factor": 0.95, "roll_source_date": None,
        "filter_criteria": {"entity_key": ["APAC_TOKYO"], "risk_type_key": ["DELTA", "GAMMA"],
                            "currency_key": ["JPY"]},
        "affected_rows": 47, "business_reason": "JPY sensitivity scaling — Bank of Japan rate policy update requires 5% haircut.",
        "ticket_reference": "RISK-0442",
        "created_by": "james.wong", "created_at": _dt(0, 11, 0),
        "approved_by": None, "approved_at": None,
        "applied_by": None, "applied_at": None,
        "reversed_by": None, "reversed_at": None,
        "ai_summary": None, "queue_status": None,
    },
    {
        "adj_id": 4, "scope_key": "NOSTRO", "adj_type": "ROLL",
        "adj_status": "APPROVED", "frequency": "ADHOC",
        "target_date": BUSINESS_DATES[-1], "start_cob": None, "end_cob": None,
        "scale_factor": 1.0, "roll_source_date": BUSINESS_DATES[-2],
        "filter_criteria": {"entity_key": ["EU_LONDON"], "currency_key": ["GBP"],
                            "correspondent_key": ["HSBC_UK"]},
        "affected_rows": 8, "business_reason": "SWIFT message delayed — rolling prior day GBP HSBC positions.",
        "ticket_reference": "OPS-0891",
        "created_by": "marco.silva", "created_at": _dt(0, 8, 0),
        "approved_by": "sarah.chen", "approved_at": _dt(0, 9, 30),
        "applied_by": None, "applied_at": None,
        "reversed_by": None, "reversed_at": None,
        "ai_summary": None, "queue_status": None,
    },
    {
        "adj_id": 5, "scope_key": "PNL", "adj_type": "SCALE",
        "adj_status": "DRAFT", "frequency": "ADHOC",
        "target_date": BUSINESS_DATES[-1], "start_cob": None, "end_cob": None,
        "scale_factor": 1.02, "roll_source_date": None,
        "filter_criteria": {"entity_key": ["APAC_HK"], "desk_key": ["FX"]},
        "affected_rows": 31, "business_reason": "HK FX desk intraday correction.",
        "ticket_reference": "",
        "created_by": "james.wong", "created_at": _dt(0, 15, 0),
        "approved_by": None, "approved_at": None,
        "applied_by": None, "applied_at": None,
        "reversed_by": None, "reversed_at": None,
        "ai_summary": None, "queue_status": None,
    },
    {
        "adj_id": 6, "scope_key": "PNL", "adj_type": "SCALE",
        "adj_status": "REJECTED", "frequency": "ADHOC",
        "target_date": BUSINESS_DATES[-5], "start_cob": None, "end_cob": None,
        "scale_factor": 1.20, "roll_source_date": None,
        "filter_criteria": {"entity_key": ["US_HQ"]},
        "affected_rows": 198, "business_reason": "Proposed revaluation — rejected as too broad.",
        "ticket_reference": "FIN-2801",
        "created_by": "marco.silva", "created_at": _dt(6, 10, 0),
        "approved_by": None, "approved_at": None,
        "applied_by": None, "applied_at": None,
        "reversed_by": None, "reversed_at": None,
        "ai_summary": "Adjustment rejected: Scale factor of 1.20 is unusually large (20% increase) covering all US HQ products without desk restriction. Risk classification: HIGH.",
        "queue_status": None,
    },
    {
        "adj_id": 7, "scope_key": "PNL", "adj_type": "SCALE",
        "adj_status": "APPLIED", "frequency": "RECURRING",
        "target_date": BUSINESS_DATES[-1],
        "start_cob": BUSINESS_DATES[-10], "end_cob": BUSINESS_DATES[-1],
        "scale_factor": 1.01, "roll_source_date": None,
        "filter_criteria": {"entity_key": ["EU_LONDON"], "desk_key": ["MONEY_MKT"],
                            "currency_key": ["GBP"]},
        "affected_rows": 14, "business_reason": "Daily GBP Money Market SONIA accrual recurring adjustment.",
        "ticket_reference": "FIN-2900",
        "created_by": "lisa.mueller", "created_at": _dt(10, 9, 0),
        "approved_by": "sarah.chen",  "approved_at": _dt(10, 10, 0),
        "applied_by": "lisa.mueller", "applied_at": _dt(1, 8, 5),
        "reversed_by": None, "reversed_at": None,
        "ai_summary": "Daily recurring SONIA accrual (1% daily) for GBP Money Market positions in London. Pre-approved template — auto-applies each COB.",
        "queue_status": "COMPLETED",
    },
]

SEED_STATUS_HISTORY = [
    {"history_id": 1, "adj_id": 1, "old_status": None,              "new_status": "DRAFT",            "changed_by": "marco.silva",  "changed_at": _dt(2,  8, 30), "comment": ""},
    {"history_id": 2, "adj_id": 1, "old_status": "DRAFT",            "new_status": "PENDING_APPROVAL", "changed_by": "marco.silva",  "changed_at": _dt(2,  9, 0),  "comment": "Ready for finance review."},
    {"history_id": 3, "adj_id": 1, "old_status": "PENDING_APPROVAL", "new_status": "APPROVED",         "changed_by": "sarah.chen",   "changed_at": _dt(2, 10, 15), "comment": "Confirmed with desk head. Approved."},
    {"history_id": 4, "adj_id": 1, "old_status": "APPROVED",         "new_status": "APPLIED",          "changed_by": "marco.silva",  "changed_at": _dt(2, 10, 45), "comment": ""},
    {"history_id": 5, "adj_id": 2, "old_status": None,              "new_status": "DRAFT",            "changed_by": "lisa.mueller", "changed_at": _dt(4, 14, 0),  "comment": ""},
    {"history_id": 6, "adj_id": 2, "old_status": "DRAFT",            "new_status": "PENDING_APPROVAL", "changed_by": "lisa.mueller", "changed_at": _dt(4, 14, 30), "comment": "Book closure — urgent."},
    {"history_id": 7, "adj_id": 2, "old_status": "PENDING_APPROVAL", "new_status": "APPROVED",         "changed_by": "sarah.chen",   "changed_at": _dt(3,  9, 20), "comment": "Verified with Risk committee."},
    {"history_id": 8, "adj_id": 2, "old_status": "APPROVED",         "new_status": "APPLIED",          "changed_by": "lisa.mueller", "changed_at": _dt(3,  9, 50), "comment": ""},
    {"history_id": 9, "adj_id": 3, "old_status": None,              "new_status": "DRAFT",            "changed_by": "james.wong",   "changed_at": _dt(0, 11, 0),  "comment": ""},
    {"history_id":10, "adj_id": 3, "old_status": "DRAFT",            "new_status": "PENDING_APPROVAL", "changed_by": "james.wong",   "changed_at": _dt(0, 11, 30), "comment": "BOJ policy update — needs quick approval."},
    {"history_id":11, "adj_id": 4, "old_status": None,              "new_status": "DRAFT",            "changed_by": "marco.silva",  "changed_at": _dt(0,  8, 0),  "comment": ""},
    {"history_id":12, "adj_id": 4, "old_status": "DRAFT",            "new_status": "PENDING_APPROVAL", "changed_by": "marco.silva",  "changed_at": _dt(0,  8, 30), "comment": "SWIFT delay — standard procedure."},
    {"history_id":13, "adj_id": 4, "old_status": "PENDING_APPROVAL", "new_status": "APPROVED",         "changed_by": "sarah.chen",   "changed_at": _dt(0,  9, 30), "comment": "SWIFT confirmation received. Approved."},
    {"history_id":14, "adj_id": 5, "old_status": None,              "new_status": "DRAFT",            "changed_by": "james.wong",   "changed_at": _dt(0, 15, 0),  "comment": ""},
    {"history_id":15, "adj_id": 6, "old_status": None,              "new_status": "DRAFT",            "changed_by": "marco.silva",  "changed_at": _dt(6, 10, 0),  "comment": ""},
    {"history_id":16, "adj_id": 6, "old_status": "DRAFT",            "new_status": "PENDING_APPROVAL", "changed_by": "marco.silva",  "changed_at": _dt(6, 10, 30), "comment": ""},
    {"history_id":17, "adj_id": 6, "old_status": "PENDING_APPROVAL", "new_status": "REJECTED",         "changed_by": "sarah.chen",   "changed_at": _dt(5, 11, 0),  "comment": "Scale factor too broad — 20% on entire US HQ. Requires desk-level breakdown."},
    {"history_id":18, "adj_id": 7, "old_status": None,              "new_status": "DRAFT",            "changed_by": "lisa.mueller", "changed_at": _dt(10, 9, 0),  "comment": ""},
    {"history_id":19, "adj_id": 7, "old_status": "DRAFT",            "new_status": "PENDING_APPROVAL", "changed_by": "lisa.mueller", "changed_at": _dt(10, 9, 30), "comment": "Recurring SONIA accrual setup."},
    {"history_id":20, "adj_id": 7, "old_status": "PENDING_APPROVAL", "new_status": "APPROVED",         "changed_by": "sarah.chen",   "changed_at": _dt(10,10, 0),  "comment": "Standard recurring — pre-approved for the month."},
    {"history_id":21, "adj_id": 7, "old_status": "APPROVED",         "new_status": "APPLIED",          "changed_by": "lisa.mueller", "changed_at": _dt(1,  8, 5),  "comment": "Auto-applied by recurring scheduler."},
]

SEED_QUEUE = [
    {"queue_id": 1, "adj_id": 1, "queued_at": _dt(2, 10, 40), "started_at": _dt(2, 10, 41),
     "completed_at": _dt(2, 10, 43), "status": "COMPLETED", "progress": 100,
     "estimated_rows": 23, "processed_rows": 23, "error_message": None,
     "worker": "TASK_ADJ_PROCESSOR"},
    {"queue_id": 2, "adj_id": 2, "queued_at": _dt(3,  9, 45), "started_at": _dt(3,  9, 46),
     "completed_at": _dt(3,  9, 48), "status": "COMPLETED", "progress": 100,
     "estimated_rows": 11, "processed_rows": 11, "error_message": None,
     "worker": "TASK_ADJ_PROCESSOR"},
    {"queue_id": 3, "adj_id": 7, "queued_at": _dt(1,  8,  4), "started_at": _dt(1,  8,  4),
     "completed_at": _dt(1,  8,  5), "status": "COMPLETED", "progress": 100,
     "estimated_rows": 14, "processed_rows": 14, "error_message": None,
     "worker": "TASK_ADJ_PROCESSOR"},
]
