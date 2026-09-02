"""
Grid Lab — pick the grid style that renders cleanly on YOUR browser
===================================================================
TEMPORARY diagnostic page. The corporate network renders this app through
Menlo Security browser isolation, which paints white blocks over some page
structures while scrolling. Instead of guessing which structure survives,
this page shows the SAME data in 7 different presentation styles (A-G) —
including the exact pre-redesign style from 26 Aug. Pick a style, scroll
up and down, and report the letter(s) that stay clean.

No database access — the data is synthetic, so this page isolates pure
rendering from queries.
"""
import streamlit as st
import pandas as pd

st.set_page_config(
    page_title="Grid Lab · MUFG", page_icon="🧪",
    layout="wide", initial_sidebar_state="expanded",
)

from utils.styles import (
    inject_css, render_sidebar, P, build_activity_grid_df, render_df_table,
    bordered_container, grid_pager, render_pager,
)

inject_css()
render_sidebar()

st.markdown("## Grid Lab")
st.markdown(
    f"<span style='color:{P['grey_700']};font-size:0.9rem'>"
    f"Same data, different rendering. Pick a style, <b>scroll the page up and "
    f"down past the grid</b>, and note whether any white block covers the "
    f"data. Report the letter(s) that render clean.</span>",
    unsafe_allow_html=True)
st.markdown("<br/>", unsafe_allow_html=True)


# ── Synthetic data (no DB) ────────────────────────────────────────────────────
def _make_source(n):
    stat = ["Processed", "Failed", "Pending", "Superseded"]
    sims = ["mrm_2011_eurozone_crisis_equityprice", "MRM_RiskOn_ALL",
            "mrm_2022_ukgilt_crisis_equityprice", "MRM_2008_Financial_Crisis_ALL"]
    rows = []
    for i in range(n):
        rows.append({
            "DIMENSION_ADJ_ID": 812000 + i, "COBID": 20260626,
            "SOURCE_COBID": 20260626, "PROCESS_TYPE": "Stress",
            "ADJUSTMENT_TYPE": "Direct", "RUN_STATUS": stat[i % len(stat)],
            "IS_DELETED": (i % 7 == 0), "ENTITY_CODE": "MUSI",
            "DEPARTMENT_CODE": None, "BOOK_CODE": f"BOOK-ED{i % 4}",
            "MEASURE_TYPE_CODE": None, "SIMULATION_NAME": sims[i % len(sims)],
            "VAR_COMPONENT_ID": "1",
            "USERNAME": "MARCOS.MAGRI@MUFGSECURITIES.COM",
            "RECORD_COUNT": 1000 + i * 37,
            "START_DATE": pd.Timestamp("2026-08-17 17:18:00"),
            "PROCESS_DATE": pd.Timestamp("2026-08-17 17:22:00"),
        })
    return pd.DataFrame(rows)


def _table_html(df):
    """Bare .mgrid table body (no wrapper) for the wrapped variants."""
    import html as _hm
    th = "".join(f"<th>{_hm.escape(str(c))}</th>" for c in df.columns)
    trs = "".join(
        "<tr>" + "".join(
            f"<td>{_hm.escape(str(v))}</td>" for v in rec.values()) + "</tr>"
        for rec in df.to_dict("records"))
    return (f'<table class="mgrid"><thead><tr>{th}</tr></thead>'
            f"<tbody>{trs}</tbody></table>")


STYLES = {
    "A — Yesterday (26 Aug): st.dataframe, fixed 380px, scrolls inside":
        "The exact pre-redesign grid. The page stays short; the grid has "
        "its own internal scrollbar.",
    "B — Current: flat HTML table, flows in the page":
        "What is deployed now. The page gets long; you scroll the page.",
    "C — HTML table in a 380px scroll box":
        "HTML like B, but capped height with its own internal scrollbar, "
        "so the page stays short like A.",
    "D — st.table (Streamlit's static table)":
        "Streamlit's built-in static HTML table, default styling.",
    "E — Paginated: 25 rows + clickable page numbers below":
        "Short page, small table, no scrollbars anywhere. Click the page "
        "numbers under the grid (« 1 2 3 … ») to move through the rows.",
    "F — Markdown pipe table":
        "The simplest possible table — plain markdown, no custom HTML.",
    "G — st.dataframe sized to show ALL rows (long page)":
        "Canvas like A but full height, so you scroll the page past it.",
    "H — Style A inside a section card (like the Adjustments page)":
        "Same grid as A, but wrapped in the white card with border, shadow "
        "and rounded corners — exactly how the Adjustments page framed it.",
}

c1, c2 = st.columns([3, 1])
with c1:
    style = st.radio("Style", list(STYLES.keys()), index=0, key="lab_style")
with c2:
    n_rows = st.radio("Rows", [30, 120, 300], index=1, key="lab_rows")

st.markdown(
    f"<div style='background:#EEF2F7;border:1px solid {P['border']};"
    f"border-radius:8px;padding:8px 14px;font-size:0.85rem;margin:6px 0 14px'>"
    f"<b>{style.split('—')[0].strip()}</b> — {STYLES[style]}</div>",
    unsafe_allow_html=True)

src = _make_source(int(n_rows))
grid = build_activity_grid_df(src)
letter = style.split(" ")[0]

if letter == "A":
    st.dataframe(grid, use_container_width=True, height=380)

elif letter == "B":
    render_df_table(grid, max_rows=1000)

elif letter == "C":
    st.markdown(
        f'<div class="mgrid-wrap" style="max-height:380px;overflow:auto">'
        f"{_table_html(grid)}</div>",
        unsafe_allow_html=True)

elif letter == "D":
    st.table(grid.head(100))
    if len(grid) > 100:
        st.caption(f"st.table capped at 100 of {len(grid)} rows.")

elif letter == "E":
    per = 25
    cur, pages = grid_pager(len(grid), per, key="lab_pgnum")
    chunk = grid.iloc[cur * per:(cur + 1) * per]
    render_df_table(chunk, max_rows=per)
    st.caption(f"Rows {cur * per + 1}–{min((cur + 1) * per, len(grid))} "
               f"of {len(grid)}")
    render_pager(pages, key="lab_pgnum")

elif letter == "F":
    show = grid.head(100)
    md = ("| " + " | ".join(show.columns) + " |\n"
          + "|" + "---|" * len(show.columns) + "\n")
    for rec in show.to_dict("records"):
        md += ("| " + " | ".join(
            str(v).replace("|", "/").replace("\n", " ")
            for v in rec.values()) + " |\n")
    st.markdown(md)
    if len(grid) > 100:
        st.caption(f"Markdown table capped at 100 of {len(grid)} rows.")

elif letter == "G":
    st.dataframe(grid, use_container_width=True,
                 height=min(35 * (len(grid) + 1) + 10, 12000))

elif letter == "H":
    with bordered_container():
        st.markdown("**Results — a card header like Adjustments**")
        st.caption("Select a row to view its details and actions.")
        st.dataframe(grid, use_container_width=True, height=380)

# Content below the grid, so there is always something to scroll TO.
st.markdown("<br/>", unsafe_allow_html=True)
st.markdown(
    f"<div style='background:{P['card']};border:1px solid {P['border']};"
    f"border-radius:8px;padding:14px'>"
    f"<b>End of grid.</b> Scroll back up through the data. If no white block "
    f"covered the rows on the way down or up, this style works — report the "
    f"letter.</div>",
    unsafe_allow_html=True)
