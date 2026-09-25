"""The Home page's dbt card must count OUTSTANDING rebuilds.

Corrected 2026-09-24 with the query Marcos supplied. The old one counted
every DUMMY_* trigger row written in the last 24 hours, so a healthy day
full of completed hand-offs lit the card up exactly like a backlog, and the
highlight ring it shares with Power BI and Overlaps meant nothing.

The right question is how much work Control-M has not picked up. That needs
the chain Control-M actually uses: the trigger row maps to a batch action,
the action maps to a condition per COB, and the applied flag on that
condition says whether it has been consumed.

These are source-level guards. app.py calls Streamlit at import, so the
query is read out of the file rather than executed.
"""
import os
import re

import pytest

APP = os.path.join(os.path.dirname(__file__), "..", "..", "streamlit", "adjustment_engine", "app.py")
# The dataset names moved out of the engine on 2026-09-24: the hand-off is
# now one procedure shared by processing and delete. This test follows the
# source of truth rather than pinning where it used to live.
HANDOFF = os.path.join(os.path.dirname(__file__), "..", "..",
                       "streamlit", "adjustment_engine", "sql",
                       "15b_sp_downstream_handoff.sql")


def _source(path):
    with open(path, encoding="utf-8") as fh:
        return fh.read()


@pytest.fixture(scope="module")
def query():
    src = _source(APP)
    # Anchored on the statement boundary, NOT on any clause under test —
    # otherwise removing a clause breaks the fixture and the suite errors
    # instead of reporting which guard failed.
    m = re.search(r'WITH LATEST_PROCESS AS \(.*?"""', src, re.S)
    assert m, "the dbt KPI query is not in app.py in the expected shape"
    return " ".join(m.group(0).split())


# ── It must measure pending work, not activity ───────────────────────────────

def test_it_filters_on_the_applied_flag(query):
    assert "WHERE C.APPLIED_FLAG = FALSE" in query, (
        "Without the applied flag this counts triggers written, not rebuilds "
        "outstanding, and every successful hand-off reads as a backlog.")


def test_it_walks_the_control_m_chain(query):
    for table in ("RAVEN.LOG_STAGE_ME_STATUS",
                  "BATCH.BATCH_ACTION_MAPPING",
                  "BATCH.BATCH_CONDITIONS"):
        assert table in query, (
            "The query no longer reads " + table + ". All three are needed "
            "to tell a consumed trigger from a pending one.")


def test_it_maps_the_dataset_name_to_a_batch_action(query):
    assert "SPLIT_PART(M.BATCH_ACTION_NAME, '.', 1)" in query, (
        "The dataset name matches only the first segment of the batch action "
        "name; comparing the whole string joins nothing.")
    assert "COLLATE 'utf8'" in query, (
        "The collation is required for the join to match.")


def test_the_condition_is_matched_per_cob(query):
    assert "C.COBID = L.COBID" in query, (
        "Without the COB in the join, a condition applied for one COB would "
        "mark another COB's rebuild as done.")


def test_it_takes_the_latest_run_per_cob_and_dataset(query):
    assert "MAX(S.START_TIMESTAMP)" in query and "GROUP BY ALL" in query, (
        "Re-runs write several trigger rows for the same COB; without the "
        "aggregation each one is counted separately.")


def test_the_window_is_48_hours(query):
    assert "DATEADD('hour', -48, CURRENT_TIMESTAMP())" in query, (
        "The window must cover an overnight batch that has not been picked "
        "up yet; 24 hours drops the previous evening's work.")


# ── It must still look at the right rows ─────────────────────────────────────

def test_it_restricts_to_the_two_adjustment_datasets(query):
    for name in ("'DUMMY_FRTB_Adjustment'", "'DUMMY_Sensitivity_Adjustment'"):
        assert name in query, "the query no longer filters on " + name


def test_the_dataset_names_match_what_the_handoff_writes(query):
    """The cross-file link that rots silently: the card would just read 0."""
    engine = _source(HANDOFF)
    block = engine[engine.index("DBT_DUMMY_DATASET = {"):]
    block = block[:block.index("}") + 1]
    written = set(re.findall(r"'(DUMMY_\w+)'", block))
    assert written, "could not read the engine's dataset mapping"
    for name in written:
        assert "'" + name + "'" in query, (
            "The hand-off writes " + name + " but the Home card does not "
            "look for it, so those rebuilds would never be counted.")


# ── The card must not describe the old behaviour ─────────────────────────────

def test_the_card_caption_no_longer_claims_a_24h_count():
    src = _source(APP)
    assert "Sens/FRTB triggers (24h)" not in src, (
        "The card still says it counts triggers over 24 hours. It now counts "
        "outstanding rebuilds over 48.")
    assert "Sens/FRTB rebuilds pending" in src


def test_the_card_still_highlights_when_work_is_outstanding():
    """Now that it means a backlog, the alert ring is the correct treatment."""
    src = _source(APP)
    m = re.search(r"alert_style = .*?else \"\"", src, re.S)
    assert m and '"dbt Rebuild"' in m.group(0), (
        "The dbt card should keep its highlight: a non-zero value now means "
        "Control-M has not picked the work up.")
