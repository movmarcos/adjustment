import json, os, sys
import pandas as pd, pytest
APP = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, APP)
from streamlit.testing.v1 import AppTest
import utils.snowflake_conn as sc

CALLS = []
class Row(list):
    def __getitem__(self, k):
        return list.__getitem__(self, 0 if isinstance(k, str) else k)
class SQL:
    def __init__(self, q): self.q = q
    def collect(self):
        CALLS.append(self.q)
        if "SP_SUBMIT_ADJUSTMENT" in self.q:
            # Realistic shape: SP_SUBMIT_ADJUSTMENT's ordinary single-scope
            # message is "Created with status '<status>'." — it also starts
            # with "Created ", which is exactly why the success screen must
            # key off an explicit "fanout" marker rather than message text
            # (see 03_sp_submit_adjustment.sql:685 and task-4 fix round 1).
            return [Row([json.dumps({"status": "Pending", "adj_id": "x",
                                     "message": "Created with status 'Pending'."})])]
        if "CURRENT_USER" in self.q.upper(): return [Row(["TESTER"])]
        return []
    def to_pandas(self): CALLS.append(self.q); return pd.DataFrame()
class Sess:
    def sql(self, q, *a, **k): return SQL(q)

@pytest.fixture(autouse=True)
def fake(monkeypatch):
    monkeypatch.setattr(sc, "get_session", lambda: Sess())
    monkeypatch.chdir(APP)
    CALLS.clear()

def _load():
    at = AppTest.from_file("pages/1_New_Adjustment.py", default_timeout=120).run()
    assert not at.exception, at.exception
    return at

def test_page_loads_without_frtball():
    at = _load()
    labels = [b.label for b in at.button]
    assert not any("FRTBALL" in l or "All FRTB" in l for l in labels)

def test_scaling_shows_type_before_scope():
    at = _load()
    # render_scaling_form returns before the Data Scope section until a type
    # is picked, so set adjustment_type too — the point of this test is the
    # SECTION ORDER (type card renders above the scope card), not gating.
    at.session_state["wiz"] = {**at.session_state["wiz"], "category": "Scaling Adjustment",
                               "adjustment_type": "Scale"}
    at.run(); assert not at.exception, at.exception
    texts = " ".join(m.value for m in at.markdown)
    assert texts.index("Adjustment Type") < texts.index("Data Scope")

def test_single_scope_success_screen_shows_original_headline_once():
    """A single-scope result (never touches _submit_fanout) must render
    exactly as before this task: the generic "Adjustment Submitted
    Successfully" headline, and the backend's own "Created with status …"
    message shown once, as the body — not duplicated as the headline too."""
    at = _load()
    at.session_state["wiz"] = {
        **at.session_state["wiz"], "step": 3, "category": "Scaling Adjustment",
        "process_types": ["VaR"],
        "result": {"status": "Pending", "adj_id": "x",
                   "message": "Created with status 'Pending'."},
    }
    at.run(); assert not at.exception, at.exception
    texts = " ".join(m.value for m in at.markdown)
    assert "Adjustment Submitted Successfully" in texts
    assert texts.count("Created with status") == 1

def test_multi_scope_fanout_success_screen_shows_count_headline():
    at = _load()
    at.session_state["wiz"] = {
        **at.session_state["wiz"], "step": 3, "category": "Scaling Adjustment",
        "process_types": ["VaR", "Stress"],
        "result": {"status": "Pending", "fanout": True, "created": 2,
                   "message": "Created 2 adjustments — one per scope (VaR, Stress). "
                              "They are queued and will be processed by their "
                              "scope pipelines."},
    }
    at.run(); assert not at.exception, at.exception
    texts = " ".join(m.value for m in at.markdown)
    assert "2 Adjustments Submitted Successfully" in texts
