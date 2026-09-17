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
            return [Row([json.dumps({"status": "Pending", "adj_id": "x"})])]
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
