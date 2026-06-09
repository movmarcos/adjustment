"""
Validation — Dev vs Prod reconciliation
=======================================
Compares the post-adjustment (combined) numbers between the new engine
(DVLP_RAPTOR_NEWADJ) and production (PROD_RAPTOR) for a COB + scope, broken
down by a dimension, and shows only the differences. Replaces the per-DB
export-to-Excel-then-eyeball workflow in /business_checks.

Requires the app session to be able to SELECT from both databases (a role with
both grants, or USE SECONDARY ROLES ALL).
"""
import streamlit as st
import pandas as pd

st.set_page_config(page_title="Validation · MUFG", page_icon="✅",
                   layout="wide", initial_sidebar_state="expanded")

from utils.styles import inject_css, render_sidebar, section_title, P
from utils.snowflake_conn import run_query, run_query_df

inject_css()
render_sidebar()

DEV_DB_DEFAULT  = "DVLP_RAPTOR_NEWADJ"
PROD_DB_DEFAULT = "PROD_RAPTOR"

# Preferred breakdown columns, in order — first one that exists in the view wins.
_PREFERRED_BREAKDOWN = [
    "VAR_SUBCOMPONENT_ID", "STRESS_SIMULATION_KEY", "MEASURE_TYPE_KEY",
    "SCENARIO_DATE_ID", "CURVE_CURRENCY_KEY", "BOOK_KEY",
]


def _esc(v) -> str:
    return str(v).replace("'", "''")


st.markdown("## ✅ Validation — Dev vs Prod")
st.markdown(
    f"<span style='color:{P['grey_700']};font-size:0.9rem'>"
    f"Compare the post-adjustment (combined) numbers between the new engine and "
    f"production for a COB + scope. Only the differences matter.</span>",
    unsafe_allow_html=True)
st.markdown("<br/>", unsafe_allow_html=True)

# ── Load scope config (combined view + USD metric) ────────────────────────────
try:
    df_cfg = run_query_df("""
        SELECT PROCESS_TYPE, FACT_ADJUSTED_TABLE, FACT_TABLE, METRIC_USD_NAME
        FROM ADJUSTMENT_APP.ADJUSTMENTS_SETTINGS
        WHERE IS_ACTIVE = TRUE
        ORDER BY PROCESS_TYPE
    """)
except Exception as e:
    st.error(f"Could not read ADJUSTMENTS_SETTINGS: {e}")
    st.stop()

if df_cfg.empty:
    st.warning("No active scopes configured in ADJUSTMENTS_SETTINGS.")
    st.stop()

cfg_by_scope = {r["PROCESS_TYPE"]: r for _, r in df_cfg.iterrows()}

# ── Inputs ────────────────────────────────────────────────────────────────────
r1c1, r1c2, r1c3 = st.columns([1, 1, 1])
with r1c1:
    scope = st.selectbox("Scope", list(cfg_by_scope.keys()), key="val_scope")
with r1c2:
    cob = st.text_input("COB Date (YYYYMMDD)", key="val_cob", placeholder="20260608")
with r1c3:
    prod_db = st.text_input("Prod database", value=PROD_DB_DEFAULT, key="val_proddb")

cfg = cfg_by_scope[scope]
combined_view = cfg.get("FACT_ADJUSTED_TABLE") or cfg.get("FACT_TABLE")
metric        = (cfg.get("METRIC_USD_NAME") or "").upper()

if not combined_view:
    st.warning(f"No combined/adjusted view configured for {scope} (FACT_ADJUSTED_TABLE is empty).")
    st.stop()

# View columns (from the dev DB) → drives the breakdown + book-filter availability.
try:
    _schema, _table = combined_view.split(".")[-2], combined_view.split(".")[-1]
    cols_df = run_query_df(f"""
        SELECT COLUMN_NAME
        FROM {DEV_DB_DEFAULT}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{_esc(_schema).upper()}' AND TABLE_NAME = '{_esc(_table).upper()}'
        ORDER BY ORDINAL_POSITION
    """)
    view_cols = [c.upper() for c in cols_df["COLUMN_NAME"].tolist()]
except Exception:
    view_cols = []

has_book_key = "BOOK_KEY" in view_cols
default_breakdown = next((c for c in _PREFERRED_BREAKDOWN if c in view_cols),
                         (view_cols[0] if view_cols else "BOOK_KEY"))

r2c1, r2c2, r2c3 = st.columns([1, 1, 1])
with r2c1:
    entity = st.text_input("Entity Code (optional)", key="val_entity",
                           disabled=not has_book_key,
                           help=None if has_book_key else "View has no BOOK_KEY — entity/book filter unavailable")
with r2c2:
    book = st.text_input("Book Code (optional)", key="val_book", disabled=not has_book_key)
with r2c3:
    breakdown = st.selectbox(
        "Break down by", view_cols or [default_breakdown],
        index=(view_cols.index(default_breakdown) if default_breakdown in view_cols else 0),
        key="val_breakdown")

st.caption(f"Comparing **{DEV_DB_DEFAULT}.{combined_view}** vs **{prod_db}.{combined_view}** · "
           f"metric **{metric}** · grouped by **{breakdown}**")

run = st.button("🔍 Compare Dev vs Prod", type="primary", use_container_width=True)


# ── Build + run comparison ────────────────────────────────────────────────────
def _side_sql(db: str) -> str:
    join = ""
    where = f"WHERE m.COBID = {int(cob)}"
    if has_book_key and (entity.strip() or book.strip()):
        join = f"INNER JOIN {db}.DIMENSION.BOOK b ON m.BOOK_KEY = b.BOOK_KEY"
        if entity.strip():
            where += f" AND UPPER(b.ENTITY_CODE) = UPPER('{_esc(entity.strip())}')"
        if book.strip():
            where += f" AND UPPER(b.BOOK_CODE) = UPPER('{_esc(book.strip())}')"
    where += f" AND m.{metric} IS NOT NULL"
    return (f"SELECT m.{breakdown} AS K, SUM(m.{metric}) AS V "
            f"FROM {db}.{combined_view} m {join} {where} GROUP BY 1")


if run:
    if not (cob or "").strip().isdigit():
        st.error("Enter a numeric COB date, e.g. 20260608.")
        st.stop()

    sql = f"""
    WITH dev  AS ({_side_sql(DEV_DB_DEFAULT)}),
         prod AS ({_side_sql(prod_db)})
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
            msg = str(e)
            st.error(f"Comparison failed: {msg}")
            if "does not exist" in msg.lower() or "not authorized" in msg.lower() \
                    or "insufficient privileges" in msg.lower():
                st.info(
                    f"The app's session must be able to read **{prod_db}** as well as "
                    f"**{DEV_DB_DEFAULT}**. Grant the app role SELECT on the relevant "
                    f"{prod_db} objects, or run `USE SECONDARY ROLES ALL`.")
            st.stop()

    if res.empty:
        st.info("No rows for that COB/scope/filters in either database.")
        st.stop()

    # Match flag with a tolerance for floating-point noise.
    def _match(row):
        d, pv = float(row["DEV"]), float(row["PROD"])
        tol = max(0.01, abs(pv) * 1e-6)
        return abs(d - pv) <= tol

    res["MATCH"] = res.apply(lambda r: "✅" if _match(r) else "❌", axis=1)
    mismatches = res[res["MATCH"] == "❌"]
    dev_total, prod_total = float(res["DEV"].sum()), float(res["PROD"].sum())
    total_ok = abs(dev_total - prod_total) <= max(0.01, abs(prod_total) * 1e-6)

    # ── Headline ──────────────────────────────────────────────────────────────
    if mismatches.empty and total_ok:
        st.markdown(
            f'<div style="background:{P["success_lt"]};border-left:4px solid {P["success"]};'
            f'border-radius:8px;padding:0.8rem 1rem;margin:0.4rem 0;font-size:0.9rem">'
            f'✅ <strong>Match.</strong> Dev and Prod agree across all {len(res)} '
            f'{breakdown} values. Total {dev_total:,.2f}.</div>', unsafe_allow_html=True)
    else:
        st.markdown(
            f'<div style="background:{P["danger_lt"]};border-left:4px solid {P["danger"]};'
            f'border-radius:8px;padding:0.8rem 1rem;margin:0.4rem 0;font-size:0.9rem">'
            f'❌ <strong>{len(mismatches)} mismatch(es)</strong> of {len(res)}. '
            f'Total diff {dev_total - prod_total:,.2f} '
            f'(dev {dev_total:,.2f} vs prod {prod_total:,.2f}).</div>', unsafe_allow_html=True)

    m1, m2, m3 = st.columns(3)
    m1.metric("Dev total", f"{dev_total:,.2f}")
    m2.metric("Prod total", f"{prod_total:,.2f}")
    m3.metric("Difference", f"{dev_total - prod_total:,.2f}")

    # ── Detail (mismatches first via the ORDER BY) ─────────────────────────────
    only_diff = st.checkbox("Show only mismatches", value=not mismatches.empty)
    show = mismatches if (only_diff and not mismatches.empty) else res

    def _hl(row):
        return ['background-color: #FFEBEE' if row["MATCH"] == "❌" else '' for _ in row]

    st.dataframe(
        show.style.apply(_hl, axis=1).format({"DEV": "{:,.2f}", "PROD": "{:,.2f}", "DIFF": "{:,.2f}"}),
        use_container_width=True, height=min(80 + 32 * len(show), 480), hide_index=True)

    st.download_button(
        "⬇ Download comparison (CSV)",
        res.to_csv(index=False).encode("utf-8"),
        file_name=f"validation_{scope}_{cob}.csv", mime="text/csv")
