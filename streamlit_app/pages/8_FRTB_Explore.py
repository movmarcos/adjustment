"""
FRTB Explore — browse and export the FRTB fact data
===================================================
Read-only explorer over the OFFICIAL FRTB fact tables (the adjusted/
published data the platform reports from):

    SBM  → FACT.FRTBSA_SENSITIVITY_MEASURES_SBM_OFFICIAL
    DRC  → FACT.FRTBSA_DRC_MEASURES_OFFICIAL
    RRAO → FACT.FRTBSA_RRAO_MEASURES_OFFICIAL

Pick the FRTB type, then COB and ENTITY first — nothing else loads until
both are chosen, because SBM holds far too much data to summarize
unfiltered. Then narrow by risk class (+ sensitivity type for SBM; RRAO:
product type), book and trade. A summary strip shows what matched; the
grid and the CSV download are capped at 1,000 rows (hard requirement).
The download name encodes the selection, e.g.
FRTB_DRC_COB20260626_MUSI_GIRR.csv.

Columns shown are the fields used to report the adjustment (mirrors the
_ADJUSTMENT fact layout), not every raw column.
"""
import re

import streamlit as st
import pandas as pd

st.set_page_config(
    page_title="FRTB Explore · MUFG", page_icon="🔍",
    layout="wide", initial_sidebar_state="expanded",
)

from utils.styles import (inject_css, render_sidebar, section_title, P,
                          bordered_container, render_data_grid, kpi_card,
                          download_csv_link)
from utils.snowflake_conn import run_query_df

inject_css()
render_sidebar()

st.markdown("## FRTB Explore")
st.markdown(
    f"<span style='color:{P['grey_700']};font-size:0.9rem'>"
    f"Browse the FRTB fact data, filter what you need, and download it as "
    f"CSV (up to 1,000 rows).</span>", unsafe_allow_html=True)
st.markdown("<br/>", unsafe_allow_html=True)

MAX_ROWS = 1000

# Per-type config: base fact table, the risk-class-like dimension, the USD
# measure for the summary, and the reporting columns (the fields used to
# report the adjustment — mirrors the _ADJUSTMENT fact layout).
_TYPES = {
    "SBM (Sensitivities)": dict(
        code="SBM", table="FACT.FRTBSA_SENSITIVITY_MEASURES_SBM_OFFICIAL",
        risk_col="RISK_CLASS", risk_label="Risk class",
        sens_col="SENSITIVITY_TYPE",
        measure="AMOUNT_IN_USD", measure_label="Amount (USD)",
        columns=["COBID", "ENTITY_CODE", "BUSINESS_ORGANIZATION_CODE", "RAPTOR_TRADE_CODE",
                 "RISK_CLASS", "SENSITIVITY_TYPE", "BUCKET", "CURVE_CODE",
                 "VERTEX", "MEASURE_TYPE_CODE", "SIMULATION_NAME",
                 "CURRENCY_CODE", "AMOUNT", "AMOUNT_IN_USD",
                 "LOAD_SET", "RAVEN_DATASET_NAME"]),
    "DRC (Default Risk Charge)": dict(
        code="DRC", table="FACT.FRTBSA_DRC_MEASURES_OFFICIAL",
        risk_col="RISK_CLASS", risk_label="Risk class",
        measure="JTD_LOSS_USD", measure_label="JTD loss (USD)",
        columns=["COBID", "ENTITY_CODE", "BUSINESS_ORGANIZATION_CODE", "RAPTOR_TRADE_CODE",
                 "RISK_CLASS", "BUCKET", "SECURITY_CODE", "ISSUER_CODE",
                 "ISSUER_RATING", "MATURITY_DATE", "JTD_RISK_DIRECTION",
                 "NOTIONAL_AMOUNT", "JTD_LOSS", "JTD_LOSS_USD",
                 "MEASURE_TYPE_CODE", "LOAD_SET"]),
    "RRAO (Residual Risk Add-On)": dict(
        code="RRAO", table="FACT.FRTBSA_RRAO_MEASURES_OFFICIAL",
        risk_col="SA_RRAO_PRODUCT_TYPE", risk_label="RRAO product type",
        measure="NOTIONAL_AMOUNT_USD", measure_label="Notional (USD)",
        columns=["COBID", "ENTITY_CODE", "BUSINESS_ORGANIZATION_CODE", "RAPTOR_TRADE_CODE",
                 "SA_RRAO_PRODUCT_TYPE", "MEASURE_TYPE_CODE",
                 "SIMULATION_NAME", "CURRENCY_CODE", "NOTIONAL_AMOUNT",
                 "NOTIONAL_AMOUNT_USD", "LOAD_SET"]),
}


def _q(sql):
    """Plain query with the app's loud-failure convention (no caching on
    SiS). Returns an empty frame on error, after showing the DB message."""
    try:
        return run_query_df(sql)
    except Exception as e:
        st.warning(f"Query failed: {e}")
        return pd.DataFrame()


def _esc(v):
    return str(v).replace("\\", "\\\\").replace("'", "''")


# ── Filters (same shape as the Adjustments page) ─────────────────────────────
with bordered_container():
    section_title("Filters", "search")
    f1, f2, f3 = st.columns([1.4, 1, 1])
    with f1:
        type_label = st.selectbox("FRTB type", list(_TYPES.keys()),
                                  key="fx_type")
    cfg = _TYPES[type_label]

    df_cobs = _q(f"SELECT DISTINCT COBID FROM {cfg['table']} "
                 f"ORDER BY COBID DESC LIMIT 60")
    cob_opts = ([int(c) for c in df_cobs["COBID"].dropna().tolist()]
                if not df_cobs.empty else [])
    with f2:
        cobid = st.selectbox("COB *", cob_opts, key=f"fx_cob_{cfg['code']}") \
            if cob_opts else None
        if not cob_opts:
            st.caption("No data found in this table.")

    ents = []
    if cobid is not None:
        df_ent = _q(f"SELECT DISTINCT ENTITY_CODE FROM {cfg['table']} "
                    f"WHERE COBID = {int(cobid)} ORDER BY 1")
        ents = (df_ent["ENTITY_CODE"].dropna().tolist()
                if not df_ent.empty else [])
    with f3:
        sel_ent = st.multiselect("Entity *", ents,
                                 key=f"fx_ent_{cfg['code']}",
                                 help="Required — the data only loads for "
                                      "the entities you pick.")

# COB + entity are the gate: nothing else queries until both are set
# (SBM holds far too much data to scan unfiltered).
if cobid is None or not sel_ent:
    st.info("Pick a **COB** and at least one **Entity** to load the data.")
    st.stop()

_ent_in = ",".join(f"'{_esc(e)}'" for e in sel_ent)
_gate = f"COBID = {int(cobid)} AND ENTITY_CODE IN ({_ent_in})"

with bordered_container():
    section_title("Narrow down", "filter")
    df_dim = _q(f"""
        SELECT DISTINCT {cfg['risk_col']} AS RISK,
               {cfg.get('sens_col') or 'NULL'} AS SENS, BUSINESS_ORGANIZATION_CODE
        FROM {cfg['table']}
        WHERE {_gate}
        LIMIT 5000""")
    risks, senss, books = [], [], []
    if not df_dim.empty:
        risks = sorted(df_dim["RISK"].dropna().unique().tolist())
        senss = sorted(df_dim["SENS"].dropna().unique().tolist())
        books = sorted(df_dim["BUSINESS_ORGANIZATION_CODE"].dropna().unique().tolist())
    n_narrow = 4 if cfg.get("sens_col") else 3
    gcols = st.columns(n_narrow)
    with gcols[0]:
        sel_risk = st.multiselect(cfg["risk_label"], risks,
                                  key=f"fx_risk_{cfg['code']}")
    sel_sens = []
    if cfg.get("sens_col"):
        with gcols[1]:
            sel_sens = st.multiselect("Sensitivity type", senss,
                                      key=f"fx_sens_{cfg['code']}")
    with gcols[-2]:
        sel_book = st.multiselect("Book", books, key=f"fx_book_{cfg['code']}")
    with gcols[-1]:
        trade = st.text_input("Trade code (contains)", key="fx_trade",
                              help="Matches RAPTOR_TRADE_CODE, case-"
                                   "insensitive. Leave empty for all trades.")

where = [_gate]
if sel_risk:
    where.append(f"{cfg['risk_col']} IN (" +
                 ",".join(f"'{_esc(r)}'" for r in sel_risk) + ")")
if sel_sens:
    where.append(f"{cfg['sens_col']} IN (" +
                 ",".join(f"'{_esc(x)}'" for x in sel_sens) + ")")
if sel_book:
    where.append("BUSINESS_ORGANIZATION_CODE IN (" +
                 ",".join(f"'{_esc(b)}'" for b in sel_book) + ")")
if trade.strip():
    where.append(f"RAPTOR_TRADE_CODE ILIKE '%{_esc(trade.strip())}%'")
where_sql = " AND ".join(where)

# ── Summary ──────────────────────────────────────────────────────────────────
df_sum = _q(f"""
    SELECT COUNT(*) AS N_ROWS,
           COUNT(DISTINCT RAPTOR_TRADE_CODE) AS N_TRADES,
           COUNT(DISTINCT BUSINESS_ORGANIZATION_CODE) AS N_BOOKS,
           SUM({cfg['measure']}) AS TOTAL_USD
    FROM {cfg['table']}
    WHERE {where_sql}""")
n_rows = int(df_sum["N_ROWS"].iloc[0]) if not df_sum.empty else 0
n_trades = int(df_sum["N_TRADES"].iloc[0]) if not df_sum.empty else 0
n_books = int(df_sum["N_BOOKS"].iloc[0]) if not df_sum.empty else 0
total = float(df_sum["TOTAL_USD"].iloc[0] or 0) if not df_sum.empty else 0.0

st.markdown(
    '<div style="display:grid;grid-template-columns:repeat(4,1fr);'
    'gap:0.8rem">'
    + kpi_card("Matching rows", f"{n_rows:,}",
               f"showing/downloading up to {MAX_ROWS:,}")
    + kpi_card("Trades", f"{n_trades:,}", "distinct RAPTOR_TRADE_CODE")
    + kpi_card("Books", f"{n_books:,}", "distinct BUSINESS_ORGANIZATION_CODE")
    + kpi_card(cfg["measure_label"], f"{total:,.2f}",
               f"sum over all {n_rows:,} matching rows")
    + "</div>", unsafe_allow_html=True)
st.markdown("<br/>", unsafe_allow_html=True)

if n_rows == 0:
    st.info("No rows match the current filters.")
    st.stop()
if n_rows > MAX_ROWS:
    st.warning(f"{n_rows:,} rows match — only the first {MAX_ROWS:,} are "
               f"shown and downloaded. Narrow the filters to get a "
               f"complete extract.")

# ── Data (capped at MAX_ROWS for both the grid and the CSV) ─────────────────
df_data = _q(f"""
    SELECT {", ".join(cfg['columns'])}
    FROM {cfg['table']}
    WHERE {where_sql}
    ORDER BY ENTITY_CODE, BUSINESS_ORGANIZATION_CODE, RAPTOR_TRADE_CODE
    LIMIT {MAX_ROWS}""")

# File name that encodes the selection: FRTB_DRC_COB20260626_MUSI_GIRR.csv
def _tag(values, all_count):
    if not values:
        return "ALL"
    if len(values) == 1:
        return re.sub(r"[^A-Za-z0-9]+", "-", str(values[0])).strip("-")
    return f"{len(values)}of{all_count}"

fname = (f"FRTB_{cfg['code']}_COB{int(cobid)}"
         f"_{_tag(sel_ent, len(ents))}"
         f"_{_tag(sel_risk, len(risks))}"
         + (f"_{_tag(sel_sens, len(senss))}" if sel_sens else "")
         + (f"_{re.sub(r'[^A-Za-z0-9]+', '-', trade.strip())}" if trade.strip() else "")
         + ".csv")

h1, h2 = st.columns([5, 1.4])
with h1:
    section_title(f"Results — {min(n_rows, MAX_ROWS):,} of {n_rows:,} rows",
                  "table")
with h2:
    # data: link, NOT st.download_button — the presigned media URL breaks
    # under the corporate proxy (Azure 'Signature fields not well formed').
    download_csv_link(
        df_data.to_csv(index=False), fname,
        help_text=f"Downloads exactly what is shown below ({fname}).")

render_data_grid(df_data, height=440)
st.caption(f"Source: {cfg['table']} · COB {int(cobid)} · file: {fname}")
