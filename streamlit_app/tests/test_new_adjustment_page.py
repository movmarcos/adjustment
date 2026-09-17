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

def _click_cat(at, category):
    """Click a Category pill (keys are versioned: cat_<Category>_<wiz_v>)."""
    btn = next(b for b in at.button
               if (b.key or "").startswith(f"cat_{category}_"))
    btn.click().run()
    assert not at.exception, at.exception


def test_category_round_trip_clears_the_scope_selection():
    """process_types must be reset with process_type when the category
    changes — otherwise a Scaling draft scoped to VaR that visits Direct
    Adjustment and comes back still fans out to VaR while the form reads
    as a fresh start."""
    at = _load()
    at.session_state["wiz"] = {**at.session_state["wiz"],
                               "category": "Scaling Adjustment",
                               "adjustment_type": "Scale",
                               "process_types": ["VaR"], "process_type": "VaR"}
    at.run(); assert not at.exception, at.exception
    assert at.session_state["wiz"]["process_types"] == ["VaR"]

    _click_cat(at, "Direct Adjustment")
    assert at.session_state["wiz"]["process_types"] == []
    assert at.session_state["wiz"]["process_type"] is None

    _click_cat(at, "Scaling Adjustment")
    assert at.session_state["wiz"]["process_types"] == []


def test_scope_change_that_drops_a_filter_says_so():
    """Silently dropping a filter the user typed is not allowed: Tenor Code
    is a Sensitivity filter the engine cannot apply for VaR, so switching
    scope clears it — and must name it."""
    at = _load()
    at.session_state["wiz"] = {**at.session_state["wiz"],
                               "category": "Scaling Adjustment",
                               "adjustment_type": "Scale",
                               "process_types": ["Sensitivity"],
                               "process_type": "Sensitivity",
                               "tenor_code": "5Y"}
    at.run(); assert not at.exception, at.exception

    at.button_group(key=f"scopes_Scaling Adjustment_"
                        f"{at.session_state['_wiz_v']}").set_value(["VaR"]).run()
    assert not at.exception, at.exception

    assert at.session_state["wiz"]["tenor_code"] is None
    assert any("Cleared filters not supported by the new scope selection"
               in w.value and "Tenor Code" in w.value for w in at.warning), \
        [w.value for w in at.warning]
    # Shown once only — the note is cleared after rendering.
    assert at.session_state["wiz"].get("_purged_filters_note") is None
    at.run()
    assert not any("Cleared filters" in w.value for w in at.warning)


def _submit_scaling(at, scopes):
    """Fill a Scaling draft for `scopes` and press Submit.

    AppTest ignores a button's `disabled` flag, so this exercises the submit
    ROUTING (one SP call per scope) without having to drive every widget in
    the ticket to completion."""
    at.session_state["wiz"] = {**at.session_state["wiz"],
                               "category": "Scaling Adjustment",
                               "adjustment_type": "Scale",
                               "process_types": list(scopes),
                               "process_type": scopes[0],
                               "cobid": 20260101, "scale_factor": 1.5,
                               "entity_code": "E1", "department_code": "D1",
                               "adjustment_category": "Cat", "reason": "why",
                               "result": None, "step": 1}
    at.run(); assert not at.exception, at.exception
    assert at.session_state["wiz"]["process_types"] == list(scopes)
    CALLS.clear()
    at.button(key=f"submit_{at.session_state['_wiz_v']}").click().run()
    assert not at.exception, at.exception
    return sum(1 for c in CALLS if "SP_SUBMIT_ADJUSTMENT" in c)


def test_one_scope_submits_once_and_does_not_fan_out():
    at = _load()
    assert _submit_scaling(at, ["VaR"]) == 1
    assert not (at.session_state["wiz"]["result"] or {}).get("fanout")


def test_two_scopes_submit_once_per_scope():
    at = _load()
    assert _submit_scaling(at, ["VaR", "Stress"]) == 2
    res = at.session_state["wiz"]["result"]
    assert res["fanout"] is True and res["created"] == 2


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


# ══════════════════════════════════════════════════════════════════════════
# Entity Roll — wipe-preview reconciliation cache
# ══════════════════════════════════════════════════════════════════════════

def _n_recon(calls):
    """How many times the header/dimension reconciliation query was sent."""
    return sum(1 for c in calls if "DIMENSION.ADJUSTMENT" in c and "COUNT(*)" in c)


def _eroll_draft(at):
    at.session_state["wiz"] = {**at.session_state["wiz"],
                               "category": "Entity Roll",
                               "process_types": ["VaR"], "process_type": "VaR",
                               "cobid": 20260101, "source_cobid": 20251231,
                               "entity_code": "E1"}


def test_failed_recon_count_is_not_cached(monkeypatch):
    """The wipe panel's counts are memoised per (scope, COB, entity) for the
    session. A FAILED read must never be memoised: the panel would then show
    "n/a" for the rest of the session while the roll still deletes
    everything — the user would approve a destruction whose size was never
    shown. Only a complete (header, dimension, fact) read is cached."""
    state = {"fail": True}

    class ReconSQL(SQL):
        def collect(self):
            if "DIMENSION.ADJUSTMENT" in self.q and "COUNT(*)" in self.q:
                CALLS.append(self.q)
                if state["fail"]:
                    raise RuntimeError("Snowflake said no")
                return [Row([3, 4])]
            if "ADJUSTMENTS_TABLE FROM ADJUSTMENT_APP.ADJUSTMENTS_SETTINGS" in self.q:
                CALLS.append(self.q)
                return [Row(["FACT.VAR_MEASURES_ADJUSTMENT"])]
            if "COUNT(DISTINCT ADJUSTMENT_ID)" in self.q:
                CALLS.append(self.q)
                return [Row([4])]
            return SQL.collect(self)

    class ReconSess:
        def sql(self, q, *a, **k):
            return ReconSQL(q)

    monkeypatch.setattr(sc, "get_session", lambda: ReconSess())
    at = _load()
    _eroll_draft(at)

    at.run(); assert not at.exception, at.exception
    n1 = _n_recon(CALLS)
    assert n1 >= 1, CALLS

    # Still failing: the second render must RE-QUERY, not serve a cached
    # (None, None, None).
    at.run(); assert not at.exception, at.exception
    n2 = _n_recon(CALLS)
    assert n2 > n1, f"failed counts were cached ({n1} → {n2})"

    # Once the read succeeds it IS cached — the next render sends nothing.
    state["fail"] = False
    at.run(); assert not at.exception, at.exception
    n3 = _n_recon(CALLS)
    assert n3 > n2
    at.run(); assert not at.exception, at.exception
    assert _n_recon(CALLS) == n3, "a complete read should have been memoised"


def test_reset_wizard_drops_the_recon_cache():
    """The cache lives in session_state, OUTSIDE wiz — a wizard reset that
    only rebuilds wiz would leave the previous draft's counts behind, and the
    next Entity Roll would quote numbers read before the last roll ran."""
    at = _load()
    at.session_state["wiz"] = {
        **at.session_state["wiz"], "step": 3, "category": "Entity Roll",
        "process_types": ["VaR"],
        "result": {"status": "Pending", "adj_id": "x",
                   "message": "Created with status 'Pending'."}}
    at.session_state["_eroll_recon_cache"] = {("VaR", 20260101, "E1"): (1, 1, 1)}
    at.run(); assert not at.exception, at.exception

    at.button(key="new_adj").click().run()      # → reset_wizard()
    assert not at.exception, at.exception
    assert "_eroll_recon_cache" not in at.session_state
