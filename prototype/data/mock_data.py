"""
Mock data generator — creates realistic sample DataFrames
that mirror the Snowflake tables so the prototype feels real.
"""

import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta


def generate_fact_table() -> pd.DataFrame:
    """Generate ~60 rows of realistic fact data across 3 days."""
    np.random.seed(42)

    entities = ["US_BANK", "EU_BANK", "APAC_BANK"]
    products = ["FX_SPOT", "FX_FORWARD", "IR_SWAP", "CREDIT_DEFAULT_SWAP"]
    accounts = ["TRADING_BOOK", "BANKING_BOOK"]
    currencies = ["USD", "EUR", "GBP", "JPY"]
    counterparties = [
        "CP_ALPHA", "CP_BETA", "CP_GAMMA", "CP_DELTA",
        "CP_EPSILON", "CP_ZETA", "CP_ETA", "CP_THETA",
    ]
    cost_centers = ["CC_100", "CC_200", "CC_300"]

    dates = [date(2026, 2, 22), date(2026, 2, 23), date(2026, 2, 24)]

    rows = []
    fact_id = 1
    for bdate in dates:
        for entity in entities:
            for product in np.random.choice(products, size=3, replace=False):
                for account in np.random.choice(accounts, size=1):
                    ccy = np.random.choice(currencies)
                    cp = np.random.choice(counterparties)
                    cc = np.random.choice(cost_centers)

                    amount = round(np.random.uniform(100_000, 5_000_000), 2)
                    quantity = round(np.random.uniform(100, 10_000), 0)
                    notional = round(amount * np.random.uniform(1.5, 3.0), 2)

                    rows.append({
                        "FACT_ID": fact_id,
                        "BUSINESS_DATE": bdate,
                        "ENTITY_KEY": entity,
                        "PRODUCT_KEY": product,
                        "ACCOUNT_KEY": account,
                        "CURRENCY_KEY": ccy,
                        "COUNTERPARTY_KEY": cp,
                        "COST_CENTER_KEY": cc,
                        "AMOUNT": amount,
                        "QUANTITY": quantity,
                        "NOTIONAL": notional,
                    })
                    fact_id += 1

    return pd.DataFrame(rows)


def generate_dimension_config() -> pd.DataFrame:
    """Configuration table driving the filter UI."""
    return pd.DataFrame([
        {"DIM_NAME": "Entity", "DIM_COLUMN": "ENTITY_KEY", "DISPLAY_ORDER": 1, "IS_REQUIRED": True},
        {"DIM_NAME": "Product", "DIM_COLUMN": "PRODUCT_KEY", "DISPLAY_ORDER": 2, "IS_REQUIRED": False},
        {"DIM_NAME": "Account", "DIM_COLUMN": "ACCOUNT_KEY", "DISPLAY_ORDER": 3, "IS_REQUIRED": False},
        {"DIM_NAME": "Currency", "DIM_COLUMN": "CURRENCY_KEY", "DISPLAY_ORDER": 4, "IS_REQUIRED": False},
        {"DIM_NAME": "Counterparty", "DIM_COLUMN": "COUNTERPARTY_KEY", "DISPLAY_ORDER": 5, "IS_REQUIRED": False},
        {"DIM_NAME": "Cost Center", "DIM_COLUMN": "COST_CENTER_KEY", "DISPLAY_ORDER": 6, "IS_REQUIRED": False},
    ])


def generate_measure_config() -> pd.DataFrame:
    """Configuration table for which measures can be adjusted."""
    return pd.DataFrame([
        {"MEASURE_NAME": "Amount", "MEASURE_COLUMN": "AMOUNT", "DISPLAY_ORDER": 1, "IS_ADJUSTABLE": True},
        {"MEASURE_NAME": "Quantity", "MEASURE_COLUMN": "QUANTITY", "DISPLAY_ORDER": 2, "IS_ADJUSTABLE": True},
        {"MEASURE_NAME": "Notional", "MEASURE_COLUMN": "NOTIONAL", "DISPLAY_ORDER": 3, "IS_ADJUSTABLE": True},
    ])


def empty_adj_header() -> pd.DataFrame:
    """Empty adjustment header table schema."""
    return pd.DataFrame(columns=[
        "ADJ_ID", "ADJ_TYPE", "ADJ_STATUS", "TARGET_DATE",
        "SCALE_FACTOR", "ROLL_SOURCE_DATE", "FILTER_CRITERIA",
        "AFFECTED_ROWS", "TOTAL_DELTA_AMOUNT", "BUSINESS_REASON",
        "TICKET_REFERENCE", "CREATED_BY", "CREATED_AT",
        "APPROVED_BY", "APPROVED_AT", "APPLIED_AT",
        "AI_SUMMARY",
    ])


def empty_adj_line_items() -> pd.DataFrame:
    """Empty line item table schema."""
    return pd.DataFrame(columns=[
        "LINE_ID", "ADJ_ID", "FACT_ID", "BUSINESS_DATE",
        "ENTITY_KEY", "PRODUCT_KEY", "ACCOUNT_KEY",
        "CURRENCY_KEY", "COUNTERPARTY_KEY", "COST_CENTER_KEY",
        "AMOUNT_DELTA", "QUANTITY_DELTA", "NOTIONAL_DELTA",
        "AMOUNT_ORIGINAL", "QUANTITY_ORIGINAL", "NOTIONAL_ORIGINAL",
    ])


def empty_status_history() -> pd.DataFrame:
    """Empty status history table schema."""
    return pd.DataFrame(columns=[
        "HISTORY_ID", "ADJ_ID", "OLD_STATUS", "NEW_STATUS",
        "CHANGED_BY", "CHANGED_AT", "COMMENT",
    ])


# --------------- Pre-built adjustments for demo ---------------

def generate_sample_adjustments(fact_df: pd.DataFrame):
    """Create 3 sample adjustments so the prototype feels populated."""
    now = datetime.now()

    # --- Adjustment 1: Flatten on US_BANK / FX_SPOT, 2026-02-23 (APPLIED) ---
    mask1 = (
        (fact_df["ENTITY_KEY"] == "US_BANK")
        & (fact_df["PRODUCT_KEY"] == "FX_SPOT")
        & (fact_df["BUSINESS_DATE"] == date(2026, 2, 23))
    )
    rows1 = fact_df[mask1].copy()

    lines1 = []
    for i, (_, r) in enumerate(rows1.iterrows(), start=1):
        lines1.append({
            "LINE_ID": i,
            "ADJ_ID": 1,
            "FACT_ID": r["FACT_ID"],
            "BUSINESS_DATE": r["BUSINESS_DATE"],
            "ENTITY_KEY": r["ENTITY_KEY"],
            "PRODUCT_KEY": r["PRODUCT_KEY"],
            "ACCOUNT_KEY": r["ACCOUNT_KEY"],
            "CURRENCY_KEY": r["CURRENCY_KEY"],
            "COUNTERPARTY_KEY": r["COUNTERPARTY_KEY"],
            "COST_CENTER_KEY": r["COST_CENTER_KEY"],
            "AMOUNT_DELTA": -r["AMOUNT"],
            "QUANTITY_DELTA": -r["QUANTITY"],
            "NOTIONAL_DELTA": -r["NOTIONAL"],
            "AMOUNT_ORIGINAL": r["AMOUNT"],
            "QUANTITY_ORIGINAL": r["QUANTITY"],
            "NOTIONAL_ORIGINAL": r["NOTIONAL"],
        })

    header1 = {
        "ADJ_ID": 1,
        "ADJ_TYPE": "FLATTEN",
        "ADJ_STATUS": "APPLIED",
        "TARGET_DATE": date(2026, 2, 23),
        "SCALE_FACTOR": None,
        "ROLL_SOURCE_DATE": None,
        "FILTER_CRITERIA": '{"ENTITY_KEY": "US_BANK", "PRODUCT_KEY": "FX_SPOT"}',
        "AFFECTED_ROWS": len(rows1),
        "TOTAL_DELTA_AMOUNT": round(-rows1["AMOUNT"].sum(), 2),
        "BUSINESS_REASON": "Erroneous FX bookings on 2/23 — ticket FX-4521",
        "TICKET_REFERENCE": "FX-4521",
        "CREATED_BY": "sarah.chen",
        "CREATED_AT": now - timedelta(hours=6),
        "APPROVED_BY": "james.kirk",
        "APPROVED_AT": now - timedelta(hours=5),
        "APPLIED_AT": now - timedelta(hours=4, minutes=30),
        "AI_SUMMARY": "Flatten adjustment zeroing out FX_SPOT positions for US_BANK on Feb 23 due to erroneous bookings. Total impact: -$"
                      + f"{abs(rows1['AMOUNT'].sum()):,.0f}. Ticket FX-4521.",
    }

    # --- Adjustment 2: Scale 1.1 on EU_BANK, 2026-02-24 (PENDING_APPROVAL) ---
    mask2 = (
        (fact_df["ENTITY_KEY"] == "EU_BANK")
        & (fact_df["BUSINESS_DATE"] == date(2026, 2, 24))
    )
    rows2 = fact_df[mask2].copy()

    lines2 = []
    line_id = len(lines1) + 1
    for _, r in rows2.iterrows():
        factor = 1.10
        lines2.append({
            "LINE_ID": line_id,
            "ADJ_ID": 2,
            "FACT_ID": r["FACT_ID"],
            "BUSINESS_DATE": r["BUSINESS_DATE"],
            "ENTITY_KEY": r["ENTITY_KEY"],
            "PRODUCT_KEY": r["PRODUCT_KEY"],
            "ACCOUNT_KEY": r["ACCOUNT_KEY"],
            "CURRENCY_KEY": r["CURRENCY_KEY"],
            "COUNTERPARTY_KEY": r["COUNTERPARTY_KEY"],
            "COST_CENTER_KEY": r["COST_CENTER_KEY"],
            "AMOUNT_DELTA": round(r["AMOUNT"] * (factor - 1), 2),
            "QUANTITY_DELTA": round(r["QUANTITY"] * (factor - 1), 2),
            "NOTIONAL_DELTA": round(r["NOTIONAL"] * (factor - 1), 2),
            "AMOUNT_ORIGINAL": r["AMOUNT"],
            "QUANTITY_ORIGINAL": r["QUANTITY"],
            "NOTIONAL_ORIGINAL": r["NOTIONAL"],
        })
        line_id += 1

    total_delta2 = round(sum(l["AMOUNT_DELTA"] for l in lines2), 2)
    header2 = {
        "ADJ_ID": 2,
        "ADJ_TYPE": "SCALE",
        "ADJ_STATUS": "PENDING_APPROVAL",
        "TARGET_DATE": date(2026, 2, 24),
        "SCALE_FACTOR": 1.10,
        "ROLL_SOURCE_DATE": None,
        "FILTER_CRITERIA": '{"ENTITY_KEY": "EU_BANK"}',
        "AFFECTED_ROWS": len(rows2),
        "TOTAL_DELTA_AMOUNT": total_delta2,
        "BUSINESS_REASON": "Correction for FX rate mismark — 10% uplift on EU_BANK",
        "TICKET_REFERENCE": "MKT-1122",
        "CREATED_BY": "john.doe",
        "CREATED_AT": now - timedelta(hours=2),
        "APPROVED_BY": None,
        "APPROVED_AT": None,
        "APPLIED_AT": None,
        "AI_SUMMARY": None,
    }

    # --- Adjustment 3: Roll from 2/23 → 2/24 on APAC_BANK (DRAFT) ---
    mask3_src = (
        (fact_df["ENTITY_KEY"] == "APAC_BANK")
        & (fact_df["PRODUCT_KEY"] == "IR_SWAP")
        & (fact_df["BUSINESS_DATE"] == date(2026, 2, 23))
    )
    mask3_tgt = (
        (fact_df["ENTITY_KEY"] == "APAC_BANK")
        & (fact_df["PRODUCT_KEY"] == "IR_SWAP")
        & (fact_df["BUSINESS_DATE"] == date(2026, 2, 24))
    )
    rows3_src = fact_df[mask3_src].copy()
    rows3_tgt = fact_df[mask3_tgt].copy()

    lines3 = []
    for _, tgt in rows3_tgt.iterrows():
        # Find matching source row (same entity/product/account)
        src_match = rows3_src[
            (rows3_src["ACCOUNT_KEY"] == tgt["ACCOUNT_KEY"])
        ]
        if len(src_match) > 0:
            src = src_match.iloc[0]
            lines3.append({
                "LINE_ID": line_id,
                "ADJ_ID": 3,
                "FACT_ID": tgt["FACT_ID"],
                "BUSINESS_DATE": tgt["BUSINESS_DATE"],
                "ENTITY_KEY": tgt["ENTITY_KEY"],
                "PRODUCT_KEY": tgt["PRODUCT_KEY"],
                "ACCOUNT_KEY": tgt["ACCOUNT_KEY"],
                "CURRENCY_KEY": tgt["CURRENCY_KEY"],
                "COUNTERPARTY_KEY": tgt["COUNTERPARTY_KEY"],
                "COST_CENTER_KEY": tgt["COST_CENTER_KEY"],
                "AMOUNT_DELTA": round(src["AMOUNT"] - tgt["AMOUNT"], 2),
                "QUANTITY_DELTA": round(src["QUANTITY"] - tgt["QUANTITY"], 2),
                "NOTIONAL_DELTA": round(src["NOTIONAL"] - tgt["NOTIONAL"], 2),
                "AMOUNT_ORIGINAL": tgt["AMOUNT"],
                "QUANTITY_ORIGINAL": tgt["QUANTITY"],
                "NOTIONAL_ORIGINAL": tgt["NOTIONAL"],
            })
            line_id += 1

    total_delta3 = round(sum(l["AMOUNT_DELTA"] for l in lines3), 2) if lines3 else 0
    header3 = {
        "ADJ_ID": 3,
        "ADJ_TYPE": "ROLL",
        "ADJ_STATUS": "DRAFT",
        "TARGET_DATE": date(2026, 2, 24),
        "SCALE_FACTOR": 1.0,
        "ROLL_SOURCE_DATE": date(2026, 2, 23),
        "FILTER_CRITERIA": '{"ENTITY_KEY": "APAC_BANK", "PRODUCT_KEY": "IR_SWAP"}',
        "AFFECTED_ROWS": len(lines3),
        "TOTAL_DELTA_AMOUNT": total_delta3,
        "BUSINESS_REASON": "Roll IR_SWAP positions from 2/23 to 2/24 — systems outage",
        "TICKET_REFERENCE": "INC-9900",
        "CREATED_BY": "maria.santos",
        "CREATED_AT": now - timedelta(minutes=45),
        "APPROVED_BY": None,
        "APPROVED_AT": None,
        "APPLIED_AT": None,
        "AI_SUMMARY": None,
    }

    headers_df = pd.DataFrame([header1, header2, header3])
    lines_df = pd.DataFrame(lines1 + lines2 + lines3)

    # Status history
    history = [
        {"HISTORY_ID": 1, "ADJ_ID": 1, "OLD_STATUS": None, "NEW_STATUS": "DRAFT",
         "CHANGED_BY": "sarah.chen", "CHANGED_AT": now - timedelta(hours=6), "COMMENT": "Created"},
        {"HISTORY_ID": 2, "ADJ_ID": 1, "OLD_STATUS": "DRAFT", "NEW_STATUS": "PENDING_APPROVAL",
         "CHANGED_BY": "sarah.chen", "CHANGED_AT": now - timedelta(hours=5, minutes=50), "COMMENT": "Submitted for approval"},
        {"HISTORY_ID": 3, "ADJ_ID": 1, "OLD_STATUS": "PENDING_APPROVAL", "NEW_STATUS": "APPROVED",
         "CHANGED_BY": "james.kirk", "CHANGED_AT": now - timedelta(hours=5), "COMMENT": "Approved"},
        {"HISTORY_ID": 4, "ADJ_ID": 1, "OLD_STATUS": "APPROVED", "NEW_STATUS": "APPLIED",
         "CHANGED_BY": "james.kirk", "CHANGED_AT": now - timedelta(hours=4, minutes=30), "COMMENT": "Applied to fact table"},
        {"HISTORY_ID": 5, "ADJ_ID": 2, "OLD_STATUS": None, "NEW_STATUS": "DRAFT",
         "CHANGED_BY": "john.doe", "CHANGED_AT": now - timedelta(hours=2, minutes=5), "COMMENT": "Created"},
        {"HISTORY_ID": 6, "ADJ_ID": 2, "OLD_STATUS": "DRAFT", "NEW_STATUS": "PENDING_APPROVAL",
         "CHANGED_BY": "john.doe", "CHANGED_AT": now - timedelta(hours=2), "COMMENT": "Submitted for approval"},
        {"HISTORY_ID": 7, "ADJ_ID": 3, "OLD_STATUS": None, "NEW_STATUS": "DRAFT",
         "CHANGED_BY": "maria.santos", "CHANGED_AT": now - timedelta(minutes=45), "COMMENT": "Created"},
    ]
    history_df = pd.DataFrame(history)

    return headers_df, lines_df, history_df
