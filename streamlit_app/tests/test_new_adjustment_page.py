import json, os, sys
import pandas as pd, pytest
APP = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, APP)
from streamlit.testing.v1 import AppTest
import utils.snowflake_conn as sc

CALLS = []
ORDER = []          # ("submit" | "result", sql) — async job lifecycle, in order


class Row(list):
    def __getitem__(self, k):
        return list.__getitem__(self, 0 if isinstance(k, str) else k)


class DictRow(list):
    """Row-like with NAMED columns: positional access (what _rows_to_df's
    `list(r)` needs) plus as_dict() (what it reads the column names from)."""
    def __init__(self, d):
        list.__init__(self, list(d.values()))
        self._d = dict(d)

    def __getitem__(self, k):
        return self._d[k] if isinstance(k, str) else list.__getitem__(self, k)

    def as_dict(self):
        return dict(self._d)


# Rows each scope's preview matches — deliberately different, so a summed
# total can be told apart from either half.
PREVIEW_ROWS = {"VaR": 10, "Stress": 5}


def _preview_reply(q):
    """SP_PREVIEW_ADJUSTMENT's answer for this CALL text — per mode, and per
    the process_type carried in the payload JSON."""
    scope = next((s for s in PREVIEW_ROWS if f'"process_type": "{s}"' in q), None)
    n = PREVIEW_ROWS.get(scope, 1)
    if '"mode": "summary"' in q:
        return [DictRow({"ROWS_AFFECTED": n, "NONZERO_ROWS": n,
                         "TOTAL_CURRENT_VALUE": n * 100.0,
                         "TOTAL_ADJUSTMENT_DELTA": n * 10.0,
                         "TOTAL_PROJECTED_VALUE": n * 110.0})]
    if '"mode": "sql"' in q:
        return [DictRow({"PREVIEW_SQL": f"SELECT /* {scope} */ 1"})]
    # breakdown (Transfer only): VaR has two trades with no version in the
    # target book, Stress has none.
    if scope == "VaR":
        return [DictRow({"TARGET_TRADE": "fallback: B2/Adjustment"}),
                DictRow({"TARGET_TRADE": "Fallback: B2/Adjustment"}),
                DictRow({"TARGET_TRADE": "T1"})]
    return [DictRow({"TARGET_TRADE": "T1"})]


class AsyncJob:
    """Stand-in for Snowpark's AsyncJob: the query is already "running"; the
    rows only materialise when .result() is called."""
    def __init__(self, sql): self._sql = sql

    def result(self):
        ORDER.append(("result", self._sql.q))
        return self._sql.collect()


class SQL:
    def __init__(self, q): self.q = q

    def collect_nowait(self):
        ORDER.append(("submit", self.q))
        return AsyncJob(self)

    def collect(self):
        CALLS.append(self.q)
        if "SP_PREVIEW_ADJUSTMENT" in self.q:
            return _preview_reply(self.q)
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
    ORDER.clear()

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


# ══════════════════════════════════════════════════════════════════════════
# Impact preview — concurrent per-scope calls + the per-scope split
# ══════════════════════════════════════════════════════════════════════════

def _preview_scaling(at, scopes):
    """Fill a COMPLETE two-scope Scaling draft (so the preview button
    renders — it is gated on an empty checklist) and press it."""
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
    CALLS.clear(); ORDER.clear()
    at.button(key=f"run_preview_{at.session_state['_wiz_v']}").click().run()
    assert not at.exception, at.exception
    return at.session_state["wiz"]


def test_two_scope_preview_keeps_every_scope_row_and_sums_them():
    """The ticket shows ONE impact figure; with several scopes that sum says
    nothing about where the money lands, so the full per-scope summary rows
    are kept as well — and the aggregate must still be their sum."""
    at = _load()
    w = _preview_scaling(at, ["VaR", "Stress"])

    assert set(w["_preview_scopes"]) == {"VaR", "Stress"}
    assert w["_preview_scopes"]["VaR"]["ROWS_AFFECTED"] == 10
    assert w["_preview_scopes"]["Stress"]["ROWS_AFFECTED"] == 5
    assert w["_preview_by_scope"] == {"VaR": 10, "Stress": 5}

    s = w["_preview_sum"]
    assert s["ROWS_AFFECTED"] == 15
    assert s["TOTAL_CURRENT_VALUE"] == 1500.0
    assert s["TOTAL_ADJUSTMENT_DELTA"] == 150.0
    assert s["TOTAL_PROJECTED_VALUE"] == 1650.0
    # The per-scope rows must not alias the aggregate that was built from them.
    assert w["_preview_scopes"]["VaR"]["ROWS_AFFECTED"] == 10
    # Both scopes' preview SQL, each under its own header.
    assert "-- VaR" in w["_preview_sql"] and "-- Stress" in w["_preview_sql"]


def test_two_scope_preview_renders_the_impact_split_with_a_total():
    at = _load()
    _preview_scaling(at, ["VaR", "Stress"])

    grids = [d.value for d in at.dataframe]
    grid = next((g for g in grids
                 if "Scope" in list(g.columns) and "Adjustment" in list(g.columns)),
                None)
    assert grid is not None, [list(g.columns) for g in grids]
    assert list(grid["Scope"]) == ["VaR", "Stress", "Total"]
    assert list(grid["Rows"]) == ["10", "5", "15"]
    assert list(grid["Adjustment"]) == ["100.00", "50.00", "150.00"]
    assert any("Impact by scope" in c.value for c in at.caption), \
        [c.value for c in at.caption]
    # The old one-line caption is replaced, not shown alongside.
    assert not any("Rows by scope" in c.value for c in at.caption)


def test_single_scope_preview_renders_no_split_table():
    at = _load()
    _preview_scaling(at, ["VaR"])
    assert not any("Impact by scope" in c.value for c in at.caption)
    assert not any("Scope" in list(d.value.columns) for d in at.dataframe)


def test_every_preview_job_is_submitted_before_any_result_is_read():
    """The point of the async jobs: two scopes × (summary, sql) go to
    Snowflake together and run concurrently — the gather only starts once
    every statement is in flight."""
    at = _load()
    _preview_scaling(at, ["VaR", "Stress"])

    kinds = [k for k, _ in ORDER]
    assert kinds == ["submit"] * 4 + ["result"] * 4, ORDER
    assert all("SP_PREVIEW_ADJUSTMENT" in q for _, q in ORDER)


def test_call_sp_df_async_falls_back_when_the_runtime_has_no_async_jobs(monkeypatch):
    """Snowpark without collect_nowait (or a session that refuses it) must
    still hand the caller a job whose .result_df() holds the rows — run
    synchronously at submit time."""
    class NoAsyncSQL(SQL):
        def collect_nowait(self):
            raise AttributeError("'DataFrame' object has no attribute 'collect_nowait'")

    class NoAsyncSess:
        def sql(self, q, *a, **k): return NoAsyncSQL(q)

    monkeypatch.setattr(sc, "get_session", lambda: NoAsyncSess())
    job = sc.call_sp_df_async("ADJUSTMENT_APP.SP_PREVIEW_ADJUSTMENT",
                              json.dumps({"process_type": "VaR", "mode": "summary"}))
    assert job.is_async is False
    df = job.result_df()
    assert int(df.iloc[0]["ROWS_AFFECTED"]) == 10
    assert sc.gather_dfs([job])[0] is df


def _seed_ref_data(at):
    """Reference caches the Transfer dropdowns read (the fake session returns
    no rows), so the book/trade widgets render with real options instead of
    writing None back over the draft."""
    _books = [["B1", "D1", "E1"], ["B2", "D2", "E2"]]
    at.session_state["_ref_books_current"] = list(_books)
    at.session_state["_ref_books_v2"] = list(_books)
    at.session_state["_ref_trades_B1"] = [["T1"], ["T2"]]


def test_transfer_preview_counts_fallback_trades_per_scope():
    """The breakdown call rides along with the other preview jobs; its
    per-scope fallback counts must survive the submit-then-gather rewrite,
    and the impact split must render beside them."""
    at = _load()
    _seed_ref_data(at)
    at.session_state["wiz"] = {**at.session_state["wiz"],
                               "category": "Scaling Adjustment",
                               "adjustment_type": "Transfer",
                               "process_types": ["VaR", "Stress"],
                               "process_type": "VaR",
                               "cobid": 20260101,
                               "source_book_code": "B1", "target_book_code": "B2",
                               "transfer_trade_codes": ["T1", "T2"],
                               "adjustment_category": "Cat", "reason": "why",
                               "result": None, "step": 1}
    at.run(); assert not at.exception, at.exception
    CALLS.clear(); ORDER.clear()
    at.button(key=f"run_preview_{at.session_state['_wiz_v']}").click().run()
    assert not at.exception, at.exception

    w = at.session_state["wiz"]
    assert w["_transfer_fallbacks"] == {"VaR": 2, "Stress": 0}
    assert w["_preview_sum"]["ROWS_AFFECTED"] == 15
    assert set(w["_preview_scopes"]) == {"VaR", "Stress"}
    # Six jobs (summary + sql + breakdown, twice), all in flight before the
    # first result is read.
    kinds = [k for k, _ in ORDER]
    assert kinds == ["submit"] * 6 + ["result"] * 6, ORDER
    # The ≥ 2 fallback warning fires for VaR only.
    warns = [x.value for x in at.warning]
    assert any("2 of the selected trades have no version" in x for x in warns), warns
    grid = next((d.value for d in at.dataframe if "Scope" in list(d.value.columns)), None)
    assert grid is not None and list(grid["Scope"]) == ["VaR", "Stress", "Total"]


def test_a_failing_sql_text_call_does_not_lose_the_preview():
    """The preview SQL is advisory. In fallback mode call_sp_df_async runs the
    call at submit time and re-raises, so the submission — not just the gather
    — has to tolerate it: the numbers must still render, with no SQL to show."""
    class NoSqlModeSQL(SQL):
        def collect_nowait(self):
            if '"mode": "sql"' in self.q:
                raise RuntimeError("async submit refused")
            return SQL.collect_nowait(self)

        def collect(self):
            if '"mode": "sql"' in self.q:
                CALLS.append(self.q)
                raise RuntimeError("SQL-text mode is not deployed")
            return SQL.collect(self)

    class NoSqlModeSess:
        def sql(self, q, *a, **k): return NoSqlModeSQL(q)

    monkey = pytest.MonkeyPatch()
    monkey.setattr(sc, "get_session", lambda: NoSqlModeSess())
    try:
        at = _load()
        w = _preview_scaling(at, ["VaR", "Stress"])
    finally:
        monkey.undo()

    assert w["_preview_err"] is None
    assert w["_preview_sql"] is None
    assert w["_preview_sum"]["ROWS_AFFECTED"] == 15
    assert set(w["_preview_scopes"]) == {"VaR", "Stress"}
