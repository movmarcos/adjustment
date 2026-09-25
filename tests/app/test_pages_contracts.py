"""Contracts this batch introduced that no single module can assert alone.

Both are source-level on purpose: each one spans two places that must agree,
and neither page can be imported in isolation (importing a page renders it).

1. Home's KPI deep link. app.py hands the wanted statuses to the Adjustments
   page through st.session_state["_home_status_filter"] and switches page; a
   rename on either side silently stops the filter from being applied — the
   card would just open an unfiltered list and nobody would notice.
2. The AI assistant's trust boundary. The live snapshot contains free text
   users typed (REASON, COMMENT, ERRORMESSAGE), so it is fenced between
   BEGIN/END markers and the system prompt says what is inside them is data,
   never instructions. Dropping either half reopens the injection path.
"""
import os

APP = os.path.join(os.path.dirname(__file__), "..", "..", "streamlit", "adjustment_engine")


def _src(*parts):
    with open(os.path.join(APP, *parts), encoding="utf-8") as fh:
        return fh.read()


HANDOFF_KEY = '"_home_status_filter"'


def test_home_kpi_deeplink_key_matches_on_both_sides():
    home = _src("app.py")
    adjustments = _src("pages", "2_Adjustments.py")
    assert f"st.session_state[{HANDOFF_KEY}] = link_status" in home
    assert "st.switch_page(\"pages/2_Adjustments.py\")" in home
    # The receiver pops it (a stale value must not re-apply on the next visit).
    assert f"st.session_state.pop({HANDOFF_KEY}, None)" in adjustments


def test_home_kpi_cards_are_buttons_not_pre_1_30_workaround():
    home = _src("app.py")
    assert "SiS runs 1.26" not in home          # the stale justification
    assert 'st.button("View"' in home


def test_assistant_fences_live_data_and_says_it_is_not_instructions():
    doc = _src("pages", "7_Documentation.py")
    assert "=== BEGIN CURRENT STATE OF THE ENGINE" in doc
    assert "=== END CURRENT STATE OF THE ENGINE ===" in doc
    # The fence is worthless unless the system prompt says what it means.
    assert "TRUST RULE" in doc
    assert "never instructions" in doc


def test_deleted_grid_helpers_are_gone_and_styles_still_imports():
    from utils import styles
    for dead in ("render_grid", "df_from_grid", "render_step_bar",
                 "render_pipeline_diagram"):
        assert not hasattr(styles, dead), f"{dead} came back — it has no caller"
    assert hasattr(styles, "render_df_table")   # the helper that replaced them
