"""
Validation — compare a Dev adjustment against its Prod counterpart
==================================================================
The user supplies the two DIMENSION.ADJUSTMENT IDs (the numeric one, not the
hash) — one in DVLP_RAPTOR_NEWADJ, one in PROD_RAPTOR — and the page compares
the two adjustments: their headers and the delta each wrote to its scope's
FACT.*_ADJUSTMENT table, broken down by a dimension, showing only the diffs.

Requires a session that can SELECT from both databases.
"""
import streamlit as st
import pandas as pd

st.set_page_config(page_title="Validation · MUFG", page_icon="✅",
                   layout="wide", initial_sidebar_state="expanded")

from utils.styles import inject_css, render_sidebar, P
from utils.snowflake_conn import run_query_df

inject_css()
render_sidebar()

DEV_DB  = "DVLP_RAPTOR_NEWADJ"
PROD_DB_DEFAULT = "PROD_RAPTOR"

# Header attributes compared side by side.
HEADER_COLS = [
    "PROCESS_TYPE", "COBID", "SOURCE_COBID", "ADJUSTMENT_TYPE", "SCALE_FACTOR",
    "ENTITY_CODE", "BOOK_CODE", "CURRENCY_CODE", "RECORD_COUNT",
    "ADJUSTMENT_VALUE_IN_USD", "RUN_STATUS", "USERNAME", "CREATED_DATE",
]
# Preferred breakdown columns, in order — first that exists in the table wins.
_PREFERRED_BREAKDOWN = [
    "VAR_SUBCOMPONENT_ID", "STRESS_SIMULATION_KEY", "MEASURE_TYPE_KEY",
    "SCENARIO_DATE_ID", "BOOK_KEY",
]


def _header(db: str, adj_id: int):
    df = run_query_df(f"""
        SELECT {", ".join(HEADER_COLS)}
        FROM {db}.DIMENSION.ADJUSTMENT
        WHERE ADJUSTMENT_ID = {int(adj_id)}
    """)
    return df.iloc[0] if not df.empty else None


st.markdown("## ✅ Validation — Dev vs Prod adjustment")
st.markdown(
    f"<span style='color:{P['grey_700']};font-size:0.9rem'>"
    f"Enter the two adjustment IDs (DIMENSION.ADJUSTMENT.ADJUSTMENT_ID — the "
    f"number, not the hash). The IDs differ between databases.</span>",
    unsafe_allow_html=True)
st.markdown("<br/>", unsafe_allow_html=True)

c1, c2, c3 = st.columns([1, 1, 1])
with c1:
    dev_id = st.text_input("Dev adjustment ID", key="val_devid", placeholder="100123")
with c2:
    prod_id = st.text_input("Prod adjustment ID", key="val_prodid", placeholder="733288")
with c3:
    prod_db = st.text_input("Prod database", value=PROD_DB_DEFAULT, key="val_proddb")

if not (dev_id.strip().isdigit() and prod_id.strip().isdigit()):
    st.info("Enter both numeric adjustment IDs to compare.")
    st.stop()

dev_id_i, prod_id_i = int(dev_id), int(prod_id)

# ── Headers ───────────────────────────────────────────────────────────────────
try:
    dev_h = _header(DEV_DB, dev_id_i)
except Exception as e:
    st.error(f"Could not read {DEV_DB}.DIMENSION.ADJUSTMENT: {e}")
    st.stop()
try:
    prod_h = _header(prod_db, prod_id_i)
except Exception as e:
    st.error(f"Could not read {prod_db}.DIMENSION.ADJUSTMENT: {e}")
    st.info(f"The session must be able to read **{prod_db}** as well as **{DEV_DB}** "
            f"(a role with both grants, or `USE SECONDARY ROLES ALL`).")
    st.stop()

if dev_h is None:
    st.error(f"No adjustment {dev_id_i} found in {DEV_DB}.")
    st.stop()
if prod_h is None:
    st.error(f"No adjustment {prod_id_i} found in {prod_db}.")
    st.stop()

dev_scope, prod_scope = str(dev_h["PROCESS_TYPE"]), str(prod_h["PROCESS_TYPE"])
if dev_scope != prod_scope:
    st.warning(f"Scope differs — dev is **{dev_scope}**, prod is **{prod_scope}**. "
               f"These are different adjustment types; the comparison may not be meaningful.")

# ── Header comparison ─────────────────────────────────────────────────────────
def _val(v):
    if v is None or (hasattr(v, "__float__") and pd.isna(v)):
        return "—"
    if hasattr(v, "strftime"):
        return v.strftime("%d %b %Y %H:%M")
    return str(v)

hdr_rows = []
for col in HEADER_COLS:
    dv, pv = _val(dev_h[col]), _val(prod_h[col])
    hdr_rows.append({"Field": col, "Dev": dv, "Prod": pv,
                     "": "" if dv == pv else "≠"})
hdr_df = pd.DataFrame(hdr_rows)

st.markdown("##### Header")
st.dataframe(
    hdr_df.style.apply(
        lambda r: ['background-color:#FFEBEE' if r[""] == "≠" else '' for _ in r], axis=1),
    use_container_width=True, height=min(60 + 32 * len(hdr_df), 480), hide_index=True)

# ── Delta comparison ──────────────────────────────────────────────────────────
scope = dev_scope
try:
    cfg = run_query_df(f"""
        SELECT ADJUSTMENTS_TABLE, METRIC_USD_NAME
        FROM {DEV_DB}.ADJUSTMENT_APP.ADJUSTMENTS_SETTINGS
        WHERE UPPER(PROCESS_TYPE) = UPPER('{scope.replace("'", "''")}')
    """)
except Exception as e:
    st.error(f"Could not read ADJUSTMENTS_SETTINGS: {e}")
    st.stop()
if cfg.empty:
    st.warning(f"No settings row for scope {scope}.")
    st.stop()

adj_tbl = cfg.iloc[0]["ADJUSTMENTS_TABLE"]          # e.g. FACT.VAR_MEASURES_ADJUSTMENT
metric  = str(cfg.iloc[0]["METRIC_USD_NAME"]).upper()

# Breakdown column choices from the adjustment table's actual columns.
try:
    _schema, _table = adj_tbl.split(".")[-2], adj_tbl.split(".")[-1]
    cols_df = run_query_df(f"""
        SELECT COLUMN_NAME FROM {DEV_DB}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{_schema.upper()}' AND TABLE_NAME = '{_table.upper()}'
        ORDER BY ORDINAL_POSITION
    """)
    tbl_cols = [c.upper() for c in cols_df["COLUMN_NAME"].tolist()]
except Exception:
    tbl_cols = []

default_bk = next((c for c in _PREFERRED_BREAKDOWN if c in tbl_cols),
                  (tbl_cols[0] if tbl_cols else metric))

st.markdown("##### Impact (delta written to the adjustment table)")
breakdown = st.selectbox("Break down by", tbl_cols or [default_bk],
                         index=(tbl_cols.index(default_bk) if default_bk in tbl_cols else 0),
                         key="val_breakdown")
st.caption(f"Scope **{scope}** · table **{adj_tbl}** · metric **{metric}** · grouped by **{breakdown}**")

sql = f"""
WITH dev AS (
    SELECT {breakdown} AS K, SUM({metric}) AS V
    FROM {DEV_DB}.{adj_tbl} WHERE ADJUSTMENT_ID = {dev_id_i} GROUP BY 1
),
prod AS (
    SELECT {breakdown} AS K, SUM({metric}) AS V
    FROM {prod_db}.{adj_tbl} WHERE ADJUSTMENT_ID = {prod_id_i} GROUP BY 1
)
SELECT COALESCE(dev.K, prod.K)            AS {breakdown},
       COALESCE(dev.V, 0)                 AS DEV,
       COALESCE(prod.V, 0)                AS PROD,
       COALESCE(dev.V, 0) - COALESCE(prod.V, 0) AS DIFF
FROM dev FULL OUTER JOIN prod ON dev.K = prod.K
ORDER BY ABS(COALESCE(dev.V, 0) - COALESCE(prod.V, 0)) DESC
"""

with st.spinner("Comparing…"):
    try:
        res = run_query_df(sql)
    except Exception as e:
        st.error(f"Comparison failed: {e}")
        st.stop()

if res.empty:
    st.info("Neither adjustment wrote rows to the table.")
    st.stop()


def _match(row):
    d, pv = float(row["DEV"]), float(row["PROD"])
    return abs(d - pv) <= max(0.01, abs(pv) * 1e-6)


res["MATCH"] = res.apply(lambda r: "✅" if _match(r) else "❌", axis=1)
mism = res[res["MATCH"] == "❌"]
dev_total, prod_total = float(res["DEV"].sum()), float(res["PROD"].sum())
total_ok = abs(dev_total - prod_total) <= max(0.01, abs(prod_total) * 1e-6)

if mism.empty and total_ok:
    st.markdown(
        f'<div style="background:{P["success_lt"]};border-left:4px solid {P["success"]};'
        f'border-radius:8px;padding:0.8rem 1rem;font-size:0.9rem">'
        f'✅ <strong>Match.</strong> Dev #{dev_id_i} and Prod #{prod_id_i} agree across all '
        f'{len(res)} {breakdown} values. Total {dev_total:,.2f}.</div>', unsafe_allow_html=True)
else:
    st.markdown(
        f'<div style="background:{P["danger_lt"]};border-left:4px solid {P["danger"]};'
        f'border-radius:8px;padding:0.8rem 1rem;font-size:0.9rem">'
        f'❌ <strong>{len(mism)} mismatch(es)</strong> of {len(res)}. '
        f'Total diff {dev_total - prod_total:,.2f} (dev {dev_total:,.2f} vs prod {prod_total:,.2f}).'
        f'</div>', unsafe_allow_html=True)

m1, m2, m3 = st.columns(3)
m1.metric("Dev total", f"{dev_total:,.2f}")
m2.metric("Prod total", f"{prod_total:,.2f}")
m3.metric("Difference", f"{dev_total - prod_total:,.2f}")

only_diff = st.checkbox("Show only mismatches", value=not mism.empty)
show = mism if (only_diff and not mism.empty) else res
st.dataframe(
    show.style.apply(
        lambda r: ['background-color:#FFEBEE' if r["MATCH"] == "❌" else '' for _ in r], axis=1
    ).format({"DEV": "{:,.2f}", "PROD": "{:,.2f}", "DIFF": "{:,.2f}"}),
    use_container_width=True, height=min(80 + 32 * len(show), 480), hide_index=True)

st.download_button(
    "⬇ Download comparison (CSV)",
    res.to_csv(index=False).encode("utf-8"),
    file_name=f"validation_{scope}_dev{dev_id_i}_prod{prod_id_i}.csv", mime="text/csv")
