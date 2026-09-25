import ast, json, os, sys
import pandas as pd, pytest
APP = os.path.join(os.path.dirname(__file__), "..", "..", "streamlit", "adjustment_engine")
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

# Existing-overlap columns per scope — deliberately different counts/ids so a
# summed count and a kept-first-scope id string can be told apart from what
# the OLD (unsound) classifier did to EXISTING_ADJ_IDS (blanked it to None).
PREVIEW_OVERLAP = {
    "VaR":    {"EXISTING_ADJ_COUNT": 1, "EXISTING_ADJ_ROWS": 2,
              "EXISTING_ADJ_VALUE": 50.0, "EXISTING_ADJ_IDS": "ADJ-1"},
    "Stress": {"EXISTING_ADJ_COUNT": 2, "EXISTING_ADJ_ROWS": 3,
              "EXISTING_ADJ_VALUE": 75.0, "EXISTING_ADJ_IDS": "ADJ-2,ADJ-3"},
}


def _preview_reply(q):
    """SP_PREVIEW_ADJUSTMENT's answer for this CALL text — per mode, and per
    the process_type carried in the payload JSON."""
    scope = next((s for s in PREVIEW_ROWS if f'"process_type": "{s}"' in q), None)
    n = PREVIEW_ROWS.get(scope, 1)
    if '"mode": "summary"' in q:
        return [DictRow({"ROWS_AFFECTED": n, "NONZERO_ROWS": n,
                         "TOTAL_CURRENT_VALUE": n * 100.0,
                         "TOTAL_ADJUSTMENT_DELTA": n * 10.0,
                         "TOTAL_PROJECTED_VALUE": n * 110.0,
                         **PREVIEW_OVERLAP.get(scope, {})})]
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


def _button(at, key, *, required=True):
    """The button with this key, or None. at.button(key=...) raises when the
    widget is absent, which is exactly what a "the button is gone" assertion
    needs to check without an except block."""
    b = next((x for x in at.button if x.key == key), None)
    if required:
        assert b is not None, [x.key for x in at.button]
    return b

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

    # Scope pills are buttons: clicking VaR adds it to the selection
    # (Sensitivity + VaR); Tenor Code is not offered for that pair.
    at.button(key=f"scope_Scaling Adjustment_VaR_"
                  f"{at.session_state['_wiz_v']}").click().run()
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


def test_two_scope_preview_sums_counts_and_blanks_measures():
    """Each scope prices a different measure (VaR P&L, Stress sim P&L, a JTD
    loss, a notional, ...), so a cross-scope sum of those is not a number.
    Only COUNT columns (rows affected, non-zero rows, ...) are summed into
    _preview_sum; the MEASURE columns are None there once more than one
    scope is previewed. The full per-scope summary rows are kept as well —
    they are now the only place a measure appears."""
    at = _load()
    w = _preview_scaling(at, ["VaR", "Stress"])

    assert set(w["_preview_scopes"]) == {"VaR", "Stress"}
    assert w["_preview_scopes"]["VaR"]["ROWS_AFFECTED"] == 10
    assert w["_preview_scopes"]["Stress"]["ROWS_AFFECTED"] == 5
    assert w["_preview_by_scope"] == {"VaR": 10, "Stress": 5}
    # Each scope's own measures are untouched by the other scope's preview.
    assert w["_preview_scopes"]["VaR"]["TOTAL_CURRENT_VALUE"] == 1000.0
    assert w["_preview_scopes"]["Stress"]["TOTAL_CURRENT_VALUE"] == 500.0

    s = w["_preview_sum"]
    assert s["ROWS_AFFECTED"] == 15        # count — summed
    assert s["NONZERO_ROWS"] == 15         # count — summed
    assert s["TOTAL_CURRENT_VALUE"] is None       # measure — blanked
    assert s["TOTAL_ADJUSTMENT_DELTA"] is None    # measure — blanked
    assert s["TOTAL_PROJECTED_VALUE"] is None     # measure — blanked
    # Existing-overlap counts (VaR 1/2, Stress 2/3) are still summed …
    assert s["EXISTING_ADJ_COUNT"] == 3
    assert s["EXISTING_ADJ_ROWS"] == 5
    # … EXISTING_ADJ_VALUE is a measure and is blanked …
    assert s["EXISTING_ADJ_VALUE"] is None
    # … and EXISTING_ADJ_IDS (a string) is neither blanked nor concatenated:
    # the OLD `(a or 0) + (b or 0)` probe didn't raise on two strings (Python
    # string `+` concatenates), so it misclassified this column as numeric
    # and blanked it. It must keep the first scope's id list untouched.
    assert s["EXISTING_ADJ_IDS"] == "ADJ-1"
    # The per-scope rows must not alias the aggregate that was built from them.
    assert w["_preview_scopes"]["VaR"]["ROWS_AFFECTED"] == 10
    # The SQL text no longer rides along with the numbers — it is one extra
    # call per scope for an expander most users never open, so it is fetched
    # on request (see test_the_preview_sql_is_fetched_only_when_asked).
    assert w["_preview_sql"] is None
    assert not [c for c in CALLS if '"mode": "sql"' in c], CALLS


def test_single_scope_preview_still_carries_its_measures():
    """Exactly one scope previewed: _preview_sum behaves as before — the
    measure columns are the scope's own values, not blanked."""
    at = _load()
    w = _preview_scaling(at, ["VaR"])

    s = w["_preview_sum"]
    assert s["ROWS_AFFECTED"] == 10
    assert s["TOTAL_CURRENT_VALUE"] == 1000.0
    assert s["TOTAL_ADJUSTMENT_DELTA"] == 100.0
    assert s["TOTAL_PROJECTED_VALUE"] == 1100.0


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
    # Each scope keeps its own measure; the Total row's measure is a dash —
    # VaR's P&L and Stress's sim P&L are not a number to add together.
    assert list(grid["Adjustment"]) == ["100.00", "50.00", "—"]
    assert any("Impact by scope" in c.value for c in at.caption), \
        [c.value for c in at.caption]
    # The old one-line caption is replaced, not shown alongside.
    assert not any("Rows by scope" in c.value for c in at.caption)
    # The caption explaining why the Total row's measure is a dash.
    assert any("Measures differ per scope" in c.value for c in at.caption), \
        [c.value for c in at.caption]


def test_two_scope_ticket_shows_no_blended_money_figure():
    """The ticket's Impact preview block must not render a summed money
    figure across scopes — it points at the per-scope table instead."""
    at = _load()
    w = _preview_scaling(at, ["VaR", "Stress"])
    texts = " ".join(m.value for m in at.markdown)

    assert "per scope — see the table below" in texts
    # The blended total that used to appear here (VaR 100 + Stress 50).
    assert "150.00" not in texts
    # Sanity: the summed-money string really would have appeared under the
    # old behaviour — prove the aggregate itself carries no such figure.
    assert w["_preview_sum"]["TOTAL_ADJUSTMENT_DELTA"] is None


def test_two_scope_impact_table_total_row_shows_dash_for_money():
    at = _load()
    _preview_scaling(at, ["VaR", "Stress"])

    grids = [d.value for d in at.dataframe]
    grid = next((g for g in grids
                 if "Scope" in list(g.columns) and "Adjustment" in list(g.columns)),
                None)
    assert grid is not None, [list(g.columns) for g in grids]
    total = grid[grid["Scope"] == "Total"].iloc[0]
    assert total["Current"] == "—"
    assert total["Adjustment"] == "—"
    assert total["Projected"] == "—"
    assert total["Rows"] == "15"


def test_single_scope_preview_renders_no_split_table():
    at = _load()
    _preview_scaling(at, ["VaR"])
    assert not any("Impact by scope" in c.value for c in at.caption)
    assert not any("Scope" in list(d.value.columns) for d in at.dataframe)


def test_every_preview_job_is_submitted_before_any_result_is_read():
    """The point of the async jobs: both scopes' summaries go to Snowflake
    together and run concurrently — the gather only starts once every
    statement is in flight. A preview costs EXACTLY ONE CALL PER SCOPE now;
    the `sql` text that used to ride along is fetched on request."""
    at = _load()
    _preview_scaling(at, ["VaR", "Stress"])

    kinds = [k for k, _ in ORDER]
    assert kinds == ["submit"] * 2 + ["result"] * 2, ORDER
    assert all("SP_PREVIEW_ADJUSTMENT" in q for _, q in ORDER)
    assert all('"mode": "summary"' in q for _, q in ORDER), ORDER


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


def test_transfer_preview_counts_fallback_trades_from_the_trade_dimension():
    """Fallback trades (no version under the target book) are counted from
    DIMENSION.TRADE, pinned on BOTH the target book and the selected trade
    codes — NOT by running the preview SP in `breakdown` mode, which scanned
    the combined view once per scope just to produce this number. The count no
    longer varies by scope, so the warning fires once."""
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
                               "transfer_pick_trades": True,
                               "adjustment_category": "Cat", "reason": "why",
                               "result": None, "step": 1}
    at.run(); assert not at.exception, at.exception
    CALLS.clear(); ORDER.clear()
    at.button(key=f"run_preview_{at.session_state['_wiz_v']}").click().run()
    assert not at.exception, at.exception

    w = at.session_state["wiz"]
    # The fake session returns no rows for the dimension query, so NEITHER
    # selected trade exists under B2 — both fall back, for every scope.
    assert w["_transfer_fallbacks"] == {"VaR": 2, "Stress": 2}
    assert w["_preview_sum"]["ROWS_AFFECTED"] == 15
    assert set(w["_preview_scopes"]) == {"VaR", "Stress"}
    # TWO jobs (one summary per scope) — the breakdown calls went first, the
    # advisory `sql` ones followed (both are loaded on request now).
    kinds = [k for k, _ in ORDER]
    assert kinds == ["submit"] * 2 + ["result"] * 2, ORDER
    assert not any('"mode": "breakdown"' in c for c in CALLS), CALLS
    assert not any('"mode": "sql"' in c for c in CALLS), CALLS
    # One pinned DIMENSION.TRADE query: the target book AND the trade codes
    # are literals, so it can return at most one row per selected trade.
    dim = [c for c in CALLS if "DIMENSION.TRADE" in c]
    assert len(dim) == 1, dim
    assert "BOOK_CODE IN ('B2')" in dim[0], dim[0]
    assert "TRADE_CODE IN ('T1', 'T2')" in dim[0], dim[0]
    # Bare columns: wrapping the filtered column is what defeats pruning.
    assert "UPPER(BOOK_CODE)" not in dim[0] and "UPPER(TRADE_CODE)" not in dim[0]
    # The ≥ 2 fallback warning fires ONCE — the count is scope-independent.
    warns = [x.value for x in at.warning]
    assert len([x for x in warns
                if "of the selected trades have no version" in x]) == 1, warns
    assert any("2 of the selected trades have no version" in x for x in warns), warns
    grid = next((d.value for d in at.dataframe if "Scope" in list(d.value.columns)), None)
    assert grid is not None and list(grid["Scope"]) == ["VaR", "Stress", "Total"]


def test_a_failing_sql_text_call_does_not_lose_the_preview():
    """The preview SQL is advisory. In fallback mode call_sp_df_async runs the
    call at submit time and re-raises, so the fetch — not just the gather —
    has to tolerate it: the numbers must still stand, with no SQL to show and
    a message saying why."""
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
        _preview_scaling(at, ["VaR", "Stress"])
        v = at.session_state["_wiz_v"]
        _button(at, f"preview_sql_load_{v}").click().run()
        assert not at.exception, at.exception
        w = at.session_state["wiz"]
        warns = [x.value for x in at.warning]
    finally:
        monkey.undo()

    assert w["_preview_err"] is None
    assert w["_preview_sql"] is None
    assert w["_preview_sum"]["ROWS_AFFECTED"] == 15
    assert set(w["_preview_scopes"]) == {"VaR", "Stress"}
    assert any("Preview SQL not available" in x for x in warns), warns


# ══════════════════════════════════════════════════════════════════════════
# Preview cost — the preview only pays for what the user asked for
# (audit batch 1: C1 lazy breakdown/sample, I12 lazy SQL text,
#  C4/S6 one invalidation helper, I7 preview before the Reason)
# ══════════════════════════════════════════════════════════════════════════

def test_the_preview_sql_is_fetched_only_when_asked():
    """I12 — the `sql` mode used to be submitted once per scope inside every
    preview (three of the six calls on a three-scope draft) purely to fill an
    expander most users never open. Nothing fetches it until the button in
    that expander is pressed, and a plain rerun afterwards refetches nothing."""
    at = _load()
    w = _preview_scaling(at, ["VaR", "Stress"])
    assert w["_preview_sql"] is None
    assert not [c for c in CALLS if '"mode": "sql"' in c], CALLS

    # Opening the page again (the expander body runs on every rerun) is free.
    CALLS.clear()
    at.run()
    assert not at.exception, at.exception
    assert not [c for c in CALLS if '"mode": "sql"' in c], CALLS

    v = at.session_state["_wiz_v"]
    CALLS.clear()
    _button(at, f"preview_sql_load_{v}").click().run()
    assert not at.exception, at.exception
    sql_calls = [c for c in CALLS if '"mode": "sql"' in c]
    assert len(sql_calls) == 2, sql_calls          # one per scope, on request
    w = at.session_state["wiz"]
    assert "-- VaR" in w["_preview_sql"] and "-- Stress" in w["_preview_sql"]

    # Cached against the payload it was fetched for: the next rerun re-reads
    # nothing.
    CALLS.clear()
    at.run()
    assert not at.exception, at.exception
    assert not [c for c in CALLS if '"mode": "sql"' in c], CALLS
    assert at.session_state["wiz"]["_preview_sql"] is not None


def test_the_breakdown_and_sample_run_only_when_asked():
    """C1 — st.expander runs its body on EVERY script run, so the breakdown
    and the sample-of-1,000 used to fire two full scans of the combined
    measures view on every click anywhere on the page once a preview existed
    on a complete ticket — the Submit click included. Nothing runs until the
    button inside the expander is pressed."""
    at = _load()
    _preview_scaling(at, ["VaR"])       # single scope → the full-width block

    CALLS.clear()
    at.run()                            # a plain rerun, expanders and all
    assert not at.exception, at.exception
    assert not [c for c in CALLS
                if '"mode": "breakdown"' in c or '"mode": "sample"' in c], CALLS

    # Ticking "Requires Approval" is the rerun the audit called out by name.
    v = at.session_state["_wiz_v"]
    CALLS.clear()
    at.checkbox(key=f"approval_{v}").check().run()
    assert not at.exception, at.exception
    assert not [c for c in CALLS
                if '"mode": "breakdown"' in c or '"mode": "sample"' in c], CALLS

    for key, mode in (("scl_brk_load", "breakdown"), ("scl_sample_load", "sample")):
        CALLS.clear()
        _button(at, f"{key}_{v}").click().run()
        assert not at.exception, at.exception
        assert len([c for c in CALLS if f'"mode": "{mode}"' in c]) == 1, (mode, CALLS)


def test_a_scope_switch_clears_the_sticky_zero_row_block():
    """C4/S6 — the zero-row verdict is sticky WITHIN one filter set (editing
    a filter must not silently unlock Submit), but nothing cleared it when
    the draft changed. Submit stayed dead on a draft that shared nothing with
    the preview that produced the verdict, with no escape short of a reload.
    Every invalidation site goes through _invalidate_preview() now."""
    at = _load()
    _preview_scaling(at, ["VaR"])
    # A preview that matched 0 rows, then a filter edit: the verdict stands.
    at.session_state["wiz"].update({"_preview_sum": None, "_preview_for": None,
                                    "_zero_preview": True})
    at.run()
    assert not at.exception, at.exception
    assert any("Submit is blocked: the current filters match no data" in c.value
               for c in at.caption), [c.value for c in at.caption]
    assert any("matched **0 rows**" in x.value for x in at.warning), \
        [x.value for x in at.warning]

    # Adding a scope is a different draft — the verdict goes with the preview.
    v = at.session_state["_wiz_v"]
    _button(at, f"scope_Scaling Adjustment_Stress_{v}").click().run()
    assert not at.exception, at.exception
    assert at.session_state["wiz"]["_zero_preview"] is None
    assert not any("matched **0 rows**" in x.value for x in at.warning), \
        [x.value for x in at.warning]
    assert not any("Submit is blocked: the current filters match no data" in c.value
                   for c in at.caption), [c.value for c in at.caption]


def test_every_invalidation_site_clears_the_same_preview_state():
    """S6 — the three sites used to null four keys each by hand and had
    already drifted (only the category switch cleared `_preview_err`, none
    cleared `_preview_for`, `_zero_preview` or `_transfer_fallbacks`). Each
    one now routes through the single helper: the adjustment-type switch, the
    category switch and a scope-pill click, all checked here against the same
    key list — `_preview_sql_err` included, so a stale "Preview SQL not
    available: …" cannot outlive the preview it belonged to."""
    _stale = {"_preview_sum": {"ROWS_AFFECTED": 1}, "_preview_sql": "SELECT 1",
              "_preview_sql_for": "x", "_preview_sql_err": "SQL mode refused",
              "_preview_err": "boom",
              "_preview_by_scope": {"VaR": 0}, "_preview_scopes": {"VaR": {}},
              "_preview_for": "x", "_zero_preview": True,
              "_transfer_fallbacks": {"VaR": 2}}

    # A scope pill is keyed on the category it renders under, hence the
    # lookup rather than a literal third entry.
    for _click in ("type_Flatten", "cat_Entity Roll", "scope-pill"):
        at = _load()
        _preview_scaling(at, ["VaR"])
        at.session_state["wiz"].update(_stale)
        at.run()
        assert not at.exception, at.exception
        v = at.session_state["_wiz_v"]
        _key = (f"scope_{at.session_state['wiz']['category']}_Stress_{v}"
                if _click == "scope-pill" else f"{_click}_{v}")
        _button(at, _key).click().run()
        assert not at.exception, at.exception
        w = at.session_state["wiz"]
        assert all(w.get(k) is None for k in _stale), (_click, w)


def test_the_impact_preview_unlocks_before_the_category_and_reason():
    """I7 — the preview exists to check that the TARGETING matches rows, and
    neither the Adjustment Category nor the Reason changes a row it counts.
    Gating it on the whole checklist meant the justification had to be
    written before the filters could be sanity-checked. Submit stays gated on
    the full list."""
    at = _load()
    at.session_state["wiz"] = {**at.session_state["wiz"],
                               "category": "Scaling Adjustment",
                               "adjustment_type": "Scale",
                               "process_types": ["VaR"], "process_type": "VaR",
                               "cobid": 20260101, "scale_factor": 1.5,
                               "entity_code": "E1", "department_code": "D1",
                               "adjustment_category": None, "reason": "",
                               "result": None, "step": 1}
    at.run()
    assert not at.exception, at.exception
    v = at.session_state["_wiz_v"]
    # Submit is still locked on the full checklist …
    assert any("Submit unlocks when the ticket is complete" in c.value
               for c in at.caption), [c.value for c in at.caption]
    # … and the preview runs anyway.
    CALLS.clear()
    _button(at, f"run_preview_{v}").click().run()
    assert not at.exception, at.exception
    assert at.session_state["wiz"]["_preview_sum"]["ROWS_AFFECTED"] == 10
    assert [c for c in CALLS if '"mode": "summary"' in c], CALLS


def test_the_impact_preview_still_needs_the_targeting_fields():
    """The other half of I7: a missing COB (or scope, or entity) is a real
    blocker — the button is gone and the page says what it is waiting for."""
    at = _load()
    at.session_state["wiz"] = {**at.session_state["wiz"],
                               "category": "Scaling Adjustment",
                               "adjustment_type": "Scale",
                               "process_types": ["VaR"], "process_type": "VaR",
                               "cobid": None, "scale_factor": 1.5,
                               "entity_code": "E1", "department_code": "D1",
                               "adjustment_category": "Cat", "reason": "why",
                               "result": None, "step": 1}
    at.run()
    assert not at.exception, at.exception
    v = at.session_state["_wiz_v"]
    assert _button(at, f"run_preview_{v}", required=False) is None
    assert any("The impact preview needs the targeting rows" in c.value
               for c in at.caption), [c.value for c in at.caption]


# ══════════════════════════════════════════════════════════════════════════
# Transfer Book — APPEND semantics (Marcos, 2026-09-18)
# ══════════════════════════════════════════════════════════════════════════

# The exact column set SP_PREVIEW_ADJUSTMENT's transfer `summary` mode returns
# (04_sp_preview_adjustment.sql, is_transfer branch). The append change
# rewrote only the last two VALUES — the SHAPE the page consumes is unchanged,
# and this row is what proves it: every field the ticket reads is present and
# the page renders the whole impact block from it without error.
TRANSFER_SUMMARY = {
    "ROWS_AFFECTED": 4,
    "NONZERO_ROWS": 4,
    "SOURCE_ORIGINAL_VALUE": 800.0,
    "SOURCE_ADJUSTMENTS_VALUE": 200.0,
    "SOURCE_ADJUSTED_VALUE": 1000.0,
    # The target book's ADJUSTED total: 300 original + the 50 of adjustments
    # already on it (04 reads FACT_ADJUSTED_TABLE for a transfer's target side,
    # because a transfer supersedes none of them).
    "TOTAL_CURRENT_VALUE": 350.0,
    # append: delta = factor × source adjusted, projected = current + delta
    "TOTAL_ADJUSTMENT_DELTA": 1000.0,
    "TOTAL_PROJECTED_VALUE": 1350.0,
    "EXISTING_ADJ_COUNT": 1,
    "EXISTING_ADJ_ROWS": 2,
    "EXISTING_ADJ_VALUE": 50.0,
    "EXISTING_ADJ_IDS": "ADJ-1",
}


class TransferSQL(SQL):
    """Answers the transfer preview with the real 04 column set."""
    def collect(self):
        if "SP_PREVIEW_ADJUSTMENT" in self.q:
            CALLS.append(self.q)
            if '"mode": "summary"' in self.q:
                return [DictRow(dict(TRANSFER_SUMMARY))]
            if '"mode": "sql"' in self.q:
                return [DictRow({"PREVIEW_SQL": "SELECT 1"})]
            return [DictRow({"TRADE_CODE": "T1", "TARGET_TRADE": "T1",
                             "ROWS_AFFECTED": 4, "PROJECTED_VALUE": 1000.0})]
        return SQL.collect(self)


class TransferSess:
    def sql(self, q, *a, **k): return TransferSQL(q)


def _transfer_preview(at):
    at.session_state["wiz"] = {**at.session_state["wiz"],
                               "category": "Scaling Adjustment",
                               "adjustment_type": "Transfer",
                               "process_types": ["VaR"], "process_type": "VaR",
                               "cobid": 20260101,
                               "source_book_code": "B1", "target_book_code": "B2",
                               "transfer_trade_codes": ["T1"],
                               "transfer_pick_trades": True,
                               "adjustment_category": "Cat", "reason": "why",
                               "result": None, "step": 1}
    at.run(); assert not at.exception, at.exception
    at.button(key=f"run_preview_{at.session_state['_wiz_v']}").click().run()
    assert not at.exception, at.exception
    return at


def test_transfer_preview_summary_shape_is_unchanged_by_append():
    """The append change touched the preview's last two VALUES only. The
    payload the page sends and the columns it reads back are the same as
    before, and the whole impact block renders from that one row."""
    monkey = pytest.MonkeyPatch()
    monkey.setattr(sc, "get_session", lambda: TransferSess())
    try:
        at = _load()
        _seed_ref_data(at)
        at = _transfer_preview(at)

        prev = [c for c in CALLS if "SP_PREVIEW_ADJUSTMENT" in c]
        assert prev, CALLS
        raw = prev[0][prev[0].index("('") + 2:prev[0].rindex("')")]
        pj = json.loads(raw.replace("''", "'").replace("\\\\", "\\"))
        # Unchanged payload contract (mode aside): the two books, the trades,
        # one COB on both sides, and the factor.
        assert set(pj) == {"cobid", "process_type", "adjustment_type",
                           "source_cobid", "scale_factor", "book_code",
                           "entity_code", "source_book_code", "trade_codes",
                           "mode"}
        assert pj["adjustment_type"] == "Transfer"
        assert pj["source_cobid"] == pj["cobid"] == 20260101
        # ONE mode rides along now. `breakdown` used to be submitted here too,
        # purely to count fallback trades — a whole extra scan of the combined
        # view on every preview; it is lazy (a button inside its expander) and
        # the fallback count comes from DIMENSION.TRADE instead. `sql` used to
        # follow it, to fill an expander most users never open; it is fetched
        # on request too.
        assert {json.loads(c[c.index("('") + 2:c.rindex("')")]
                           .replace("''", "'").replace("\\\\", "\\"))["mode"]
                for c in prev} == {"summary"}

        w = at.session_state["wiz"]
        assert w["_preview_err"] is None
        # Every column 04 returns survives into the ticket's summary dict.
        assert set(TRANSFER_SUMMARY) <= set(w["_preview_sum"])
        assert w["_preview_sum"]["TOTAL_PROJECTED_VALUE"] == 1350.0
        assert w["_preview_sum"]["TOTAL_ADJUSTMENT_DELTA"] == 1000.0
    finally:
        monkey.undo()


# A second scope's Transfer summary row — deliberately different from
# TRANSFER_SUMMARY so the per-scope split and the summed counts can be told
# apart, the same way PREVIEW_ROWS/PREVIEW_OVERLAP do for Scale.
TRANSFER_SUMMARY_STRESS = {
    "ROWS_AFFECTED": 6,
    "NONZERO_ROWS": 6,
    "SOURCE_ORIGINAL_VALUE": 400.0,
    "SOURCE_ADJUSTMENTS_VALUE": 100.0,
    "SOURCE_ADJUSTED_VALUE": 500.0,
    "TOTAL_CURRENT_VALUE": 175.0,
    "TOTAL_ADJUSTMENT_DELTA": 500.0,
    "TOTAL_PROJECTED_VALUE": 675.0,
    "EXISTING_ADJ_COUNT": 2,
    "EXISTING_ADJ_ROWS": 3,
    "EXISTING_ADJ_VALUE": 75.0,
    "EXISTING_ADJ_IDS": "ADJ-2,ADJ-3",
}
TRANSFER_SUMMARY_BY_SCOPE = {"VaR": TRANSFER_SUMMARY, "Stress": TRANSFER_SUMMARY_STRESS}


class MultiScopeTransferSQL(SQL):
    """Two-scope Transfer preview — same shape as TransferSQL, but the
    summary row differs per scope so the count/measure split is exercised
    under the "Added" delta label (Transfer's own, not "Adjustment")."""
    def collect(self):
        if "SP_PREVIEW_ADJUSTMENT" in self.q:
            CALLS.append(self.q)
            scope = next((s for s in TRANSFER_SUMMARY_BY_SCOPE
                          if f'"process_type": "{s}"' in self.q), None)
            if '"mode": "summary"' in self.q:
                return [DictRow(dict(TRANSFER_SUMMARY_BY_SCOPE.get(scope, TRANSFER_SUMMARY)))]
            if '"mode": "sql"' in self.q:
                return [DictRow({"PREVIEW_SQL": f"SELECT /* {scope} */ 1"})]
            return [DictRow({"TRADE_CODE": "T1", "TARGET_TRADE": "T1",
                             "ROWS_AFFECTED": 4, "PROJECTED_VALUE": 1000.0})]
        return SQL.collect(self)


class MultiScopeTransferSess:
    def sql(self, q, *a, **k): return MultiScopeTransferSQL(q)


def test_two_scope_transfer_preview_sums_counts_and_shows_added_label():
    """Transfer Book with two scopes goes through the "Added" label path
    (the delta column header differs from Scale/Roll's "Adjustment"), and
    must go through the same count/measure split: rows and the existing-
    overlap counts summed, the money columns (incl. EXISTING_ADJ_VALUE)
    blanked, and EXISTING_ADJ_IDS kept from the first scope rather than
    blanked or concatenated."""
    monkey = pytest.MonkeyPatch()
    monkey.setattr(sc, "get_session", lambda: MultiScopeTransferSess())
    try:
        at = _load()
        _seed_ref_data(at)
        at.session_state["wiz"] = {**at.session_state["wiz"],
                                   "category": "Scaling Adjustment",
                                   "adjustment_type": "Transfer",
                                   "process_types": ["VaR", "Stress"],
                                   "process_type": "VaR",
                                   "cobid": 20260101,
                                   "source_book_code": "B1", "target_book_code": "B2",
                                   "transfer_trade_codes": ["T1"],
                                   "transfer_pick_trades": True,
                                   "adjustment_category": "Cat", "reason": "why",
                                   "result": None, "step": 1}
        at.run(); assert not at.exception, at.exception
        at.button(key=f"run_preview_{at.session_state['_wiz_v']}").click().run()
        assert not at.exception, at.exception

        w = at.session_state["wiz"]
        s = w["_preview_sum"]
        assert s["ROWS_AFFECTED"] == 10             # 4 + 6 — count, summed
        assert s["EXISTING_ADJ_COUNT"] == 3         # 1 + 2 — count, summed
        assert s["EXISTING_ADJ_ROWS"] == 5          # 2 + 3 — count, summed
        assert s["TOTAL_ADJUSTMENT_DELTA"] is None  # measure — blanked
        assert s["TOTAL_PROJECTED_VALUE"] is None   # measure — blanked
        assert s["EXISTING_ADJ_VALUE"] is None      # measure — blanked
        assert s["EXISTING_ADJ_IDS"] == "ADJ-1"     # kept, first scope's

        texts = " ".join(m.value for m in at.markdown)
        assert "per scope — see the table below" in texts

        # The "Added" delta label (Transfer's own, not "Adjustment") lives in
        # the per-scope table's column header now — the ticket's money rows
        # are dropped for >1 scope (change 2), so this is the one place it
        # still appears.
        grids = [d.value for d in at.dataframe]
        grid = next((g for g in grids
                     if "Scope" in list(g.columns) and "Added" in list(g.columns)),
                    None)
        assert grid is not None, [list(g.columns) for g in grids]
        total = grid[grid["Scope"] == "Total"].iloc[0]
        assert total["Added"] == "—"
        assert total["Rows"] == "10"
    finally:
        monkey.undo()


def test_transfer_ticket_says_existing_adjustments_are_kept_not_replaced():
    """Append: a transfer supersedes nothing (05, supersede_sql gained
    `adjust.SOURCE_BOOK_CODE IS NULL`), so the overlap note must NOT tell the
    user their earlier adjustments are replaced."""
    monkey = pytest.MonkeyPatch()
    monkey.setattr(sc, "get_session", lambda: TransferSess())
    try:
        at = _load()
        _seed_ref_data(at)
        at = _transfer_preview(at)
        texts = " ".join(m.value for m in at.markdown)
    finally:
        monkey.undo()

    assert "Already adjusted here" in texts
    assert "added on top of them and replaces nothing" in texts
    assert "Submitting replaces every one" not in texts


def test_transfer_preview_caption_reads_current_plus_added():
    monkey = pytest.MonkeyPatch()
    monkey.setattr(sc, "get_session", lambda: TransferSess())
    try:
        at = _load()
        _seed_ref_data(at)
        at = _transfer_preview(at)
        caps = " ".join(c.value for c in at.caption)
    finally:
        monkey.undo()

    assert "**projected** = current + added" in caps
    assert "nothing already there is replaced" in caps
    # "current" is the target book's total INCLUDING its own adjustments —
    # 04's tgt CTE reads the adjusted view for a transfer.
    assert "including the adjustments already on it" in caps


def test_transfer_impact_block_labels_current_as_including_adjustments():
    """The ticket's Impact preview must not call a transfer's current figure
    "Target original" — for a transfer it is the target book's ADJUSTED total,
    and the delta is what gets added on top of it."""
    monkey = pytest.MonkeyPatch()
    monkey.setattr(sc, "get_session", lambda: TransferSess())
    try:
        at = _load()
        _seed_ref_data(at)
        at = _transfer_preview(at)
        texts = " ".join(m.value for m in at.markdown)
    finally:
        monkey.undo()

    assert "Target current (incl. its adjustments)" in texts
    assert "Target original" not in texts
    # The delta row reads "Added", not "Adjustment".
    assert ">Added<" in texts


def test_the_per_trade_breakdown_is_loaded_only_on_request():
    """PERF (2026-09-18): the per-trade breakdown scans the combined view plus
    both SCD2 trade lookups. Opening the preview — and opening the expander —
    must cost nothing; the SP runs only when the user presses the button."""
    monkey = pytest.MonkeyPatch()
    monkey.setattr(sc, "get_session", lambda: TransferSess())
    try:
        at = _load()
        _seed_ref_data(at)
        at = _transfer_preview(at)
        CALLS.clear()
        at.run()
        assert not at.exception, at.exception
        assert not [c for c in CALLS if '"mode": "breakdown"' in c], CALLS

        btn = at.button(key=f"trf_brk_load_{at.session_state['_wiz_v']}")
        assert btn is not None
        CALLS.clear()
        btn.click().run()
        assert not at.exception, at.exception
        assert [c for c in CALLS if '"mode": "breakdown"' in c], CALLS
        grid = next((d.value for d in at.dataframe
                     if "Target trade" in list(d.value.columns)), None)
        assert grid is not None and list(grid["Trade"]) == ["T1"]
    finally:
        monkey.undo()


def test_fallback_count_query_tolerates_the_stored_casing():
    """ADJ_HEADER is COLLATE 'en-ci' and SP_SUBMIT stores the CALLER'S
    spelling, so a draft can carry 't1' where DIMENSION.TRADE has 'T1'. The
    pre-filter stays a bare, prunable literal list — it just carries every
    spelling instead of one, or the count would read as "all missing" and fire
    a false warning. The book goes through the same variant expansion, which is
    why its predicate is an IN-list rather than an equality.
    """
    at = _load()
    _seed_ref_data(at)
    at.session_state["_ref_trades_B1"] = [["t1"]]
    at.session_state["wiz"] = {**at.session_state["wiz"],
                               "category": "Scaling Adjustment",
                               "adjustment_type": "Transfer",
                               "process_types": ["VaR"], "process_type": "VaR",
                               "cobid": 20260101,
                               "source_book_code": "B1", "target_book_code": "B2",
                               "transfer_trade_codes": ["t1"],
                               "transfer_pick_trades": True,
                               "adjustment_category": "Cat", "reason": "why",
                               "result": None, "step": 1}
    at.run(); assert not at.exception, at.exception
    CALLS.clear()
    at.button(key=f"run_preview_{at.session_state['_wiz_v']}").click().run()
    assert not at.exception, at.exception
    dim = [c for c in CALLS if "DIMENSION.TRADE" in c]
    assert len(dim) == 1, dim
    # Both spellings of the trade code, as bare literals.
    assert "'t1'" in dim[0] and "'T1'" in dim[0], dim[0]
    # The book predicate is a literal LIST, so it can carry its variants too.
    assert "BOOK_CODE IN (" in dim[0], dim[0]
    assert "UPPER(BOOK_CODE)" not in dim[0] and "UPPER(TRADE_CODE)" not in dim[0]


def test_a_failed_fallback_query_raises_no_warning():
    """_ref_rows caches [] for a FAILED query, which reads identically to "no
    rows" — i.e. "none of these trades exist in the target book". That would
    fire a false warning for the rest of the session, so the count must
    distinguish "could not ask" from "not found" and stay silent."""
    class NoDimSQL(SQL):
        def collect(self):
            if "DIMENSION.TRADE" in self.q:
                CALLS.append(self.q)
                raise RuntimeError("dimension unavailable")
            return SQL.collect(self)

    class NoDimSess:
        def sql(self, q, *a, **k): return NoDimSQL(q)

    monkey = pytest.MonkeyPatch()
    monkey.setattr(sc, "get_session", lambda: NoDimSess())
    try:
        at = _load()
        _seed_ref_data(at)
        at.session_state["wiz"] = {**at.session_state["wiz"],
                                   "category": "Scaling Adjustment",
                                   "adjustment_type": "Transfer",
                                   "process_types": ["VaR"], "process_type": "VaR",
                                   "cobid": 20260101,
                                   "source_book_code": "B1", "target_book_code": "B2",
                                   "transfer_trade_codes": ["T1", "T2"],
                                   "transfer_pick_trades": True,
                                   "adjustment_category": "Cat", "reason": "why",
                                   "result": None, "step": 1}
        at.run(); assert not at.exception, at.exception
        at.button(key=f"run_preview_{at.session_state['_wiz_v']}").click().run()
        assert not at.exception, at.exception
        # No count at all — not a zero-filled one that reads as "all missing".
        assert at.session_state["wiz"]["_transfer_fallbacks"] == {}
        warns = [x.value for x in at.warning]
        assert not [x for x in warns
                    if "of the selected trades have no version" in x], warns
        # The numbers themselves are unaffected.
        assert at.session_state["wiz"]["_preview_sum"]["ROWS_AFFECTED"] == 10
    finally:
        monkey.undo()


# ══════════════════════════════════════════════════════════════════════════
# Filter & widget state (audit batch 2: C3 purged filters resurrecting,
#  I2 stale entity, I3 the approval checkbox, I8/S7 scope options,
#  I10 silent dropdown drops, M7 Day Type)
# ══════════════════════════════════════════════════════════════════════════

PAGE_SRC = open(os.path.join(APP, "pages", "1_New_Adjustment.py"),
                encoding="utf-8").read()
PAGE_AST = ast.parse(PAGE_SRC)


def _ss(at, key):
    """session_state[key] or None — SafeSessionState has no .get()."""
    return at.session_state[key] if key in at.session_state else None


def _module_assignments():
    """Module-level assigned names in the page (so "this constant is gone"
    can be asserted without tripping over the docstring that explains why)."""
    out = set()
    for node in PAGE_AST.body:
        for t in (node.targets if isinstance(node, ast.Assign) else
                  [node.target] if isinstance(node, ast.AnnAssign) else []):
            if isinstance(t, ast.Name):
                out.add(t.id)
    return out


def _string_constants():
    return {n.value for n in ast.walk(PAGE_AST)
            if isinstance(n, ast.Constant) and isinstance(n.value, str)}


def _fn_names():
    return {n.name for n in ast.walk(PAGE_AST)
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}


def _fn_args(name):
    fn = next(n for n in ast.walk(PAGE_AST)
              if isinstance(n, ast.FunctionDef) and n.name == name)
    a = fn.args
    return [x.arg for x in (a.posonlyargs + a.args + a.kwonlyargs)]


def _scope_click(at, scope):
    """Click a scope pill (keys are scope_<category>_<scope>_<wiz_v>)."""
    v = at.session_state["_wiz_v"]
    cat = at.session_state["wiz"].get("category")
    at.button(key=f"scope_{cat}_{scope}_{v}").click().run()
    assert not at.exception, at.exception


def _scaling_draft(at, scopes, **extra):
    at.session_state["wiz"] = {**at.session_state["wiz"],
                               "category": "Scaling Adjustment",
                               "adjustment_type": "Scale",
                               "process_types": list(scopes),
                               "process_type": scopes[0],
                               "result": None, "step": 1, **extra}
    at.run()
    assert not at.exception, at.exception
    return at.session_state["_wiz_v"]


def test_a_purged_dropdown_filter_does_not_come_back_when_its_scope_returns():
    """C3 — the headline of this batch. Simulation Name is a Stress filter
    the engine cannot apply to VaR, so adding VaR purges it (and says so).
    But `wiz[fk] = None` only clears the MODEL: Streamlit still owned
    st.session_state[_k("simulation_name")], and dropping VaR again
    re-instantiated the same widget, which restored "SIM1" and wrote it
    straight back into the draft — silently, with no second warning, and on
    into a submitted regulated adjustment.

    Round trip: Stress → SIM1 → +VaR → back to Stress alone."""
    at = _load()
    # Real options, so this exercises the DROPDOWN shape of the widget.
    at.session_state["_ref_sim_names"] = [["SIM1", "SRC1"], ["SIM2", "SRC1"]]
    v = _scaling_draft(at, ["Stress"], simulation_name="SIM1")
    assert at.session_state[f"simulation_name_{v}"] == "SIM1"

    _scope_click(at, "VaR")                       # Stress + VaR
    assert at.session_state["wiz"]["process_types"] == ["VaR", "Stress"]
    assert at.session_state["wiz"]["simulation_name"] is None
    assert any("Cleared filters not supported" in w.value
               and "Simulation Name" in w.value for w in at.warning), \
        [w.value for w in at.warning]

    _scope_click(at, "VaR")                       # back to Stress alone
    assert at.session_state["wiz"]["process_types"] == ["Stress"]
    w = at.session_state["wiz"]
    assert w["simulation_name"] != "SIM1", w["simulation_name"]
    assert not w["simulation_name"], w["simulation_name"]
    assert _ss(at, f"simulation_name_{v}") in (None, "")

    # And it stays gone on the next plain rerun (the widget is re-seeded from
    # the model, not from its own memory).
    at.run()
    assert not at.exception, at.exception
    assert not at.session_state["wiz"]["simulation_name"]


def test_a_purged_free_text_filter_does_not_come_back_either():
    """C3, the other widget shape: Tenor Code is a plain st.text_input whose
    key IS the filter key. Sensitivity → 5Y → +VaR (Tenor is not common) →
    back to Sensitivity alone."""
    at = _load()
    v = _scaling_draft(at, ["Sensitivity"], tenor_code="5Y")
    assert at.session_state[f"tenor_code_{v}"] == "5Y"

    _scope_click(at, "VaR")
    assert at.session_state["wiz"]["tenor_code"] is None

    _scope_click(at, "VaR")
    assert at.session_state["wiz"]["tenor_code"] != "5Y"
    assert not at.session_state["wiz"]["tenor_code"]


def test_the_category_switch_clears_the_entity_and_every_entity_widget():
    """I2 — a Scaling draft scoped to ENT1 that hops to Direct Adjustment
    used to keep entity_code = "ENT1", so the sign-off panel checked ENT1
    alone while the per-row Direct batch can target any entity: the panel
    read "open", Submit unlocked, and the rows came back rejected_signoff.

    The entity is edited by three different widgets across the forms
    (the main filter row, the Entity Roll form, the VaR upload form) and ANY
    of them can write the old code back, so all three stems are dropped."""
    at = _load()
    v = _scaling_draft(at, ["VaR"], entity_code="ENT1")
    # The main filter row falls back to free text here (no reference rows).
    assert at.session_state[f"entity_dd_{v}_txt"] == "ENT1"
    # The other two forms' entity widgets, as they would be after a visit.
    at.session_state[f"er_entity_dd_{v}_txt"] = "ENT1"
    at.session_state[f"var_entity_{v}"] = "ENT1"

    _click_cat(at, "Direct Adjustment")
    assert at.session_state["wiz"]["entity_code"] is None
    for stem in ("entity_dd", "er_entity_dd", "var_entity"):
        assert not _ss(at, f"{stem}_{v}"), stem
        assert not _ss(at, f"{stem}_{v}_txt"), stem

    _click_cat(at, "Scaling Adjustment")
    assert at.session_state["wiz"]["entity_code"] != "ENT1"
    assert not at.session_state["wiz"]["entity_code"]


def test_the_category_switch_clears_the_approval_checkbox():
    """I3 — `wiz.update({... "requires_approval": False ...})` was a no-op:
    the checkbox owns _k("approval") and Streamlit ignores `value=` once a
    key has stored state, so the old True was handed straight back into wiz
    on the very next rerun and the documented reset never happened."""
    at = _load()
    v = _scaling_draft(at, ["VaR"], cobid=20260101, scale_factor=1.5,
                       entity_code="E1", department_code="D1",
                       adjustment_category="Cat", reason="why")
    at.checkbox(key=f"approval_{v}").check().run()
    assert not at.exception, at.exception
    assert at.session_state["wiz"]["requires_approval"] is True

    _click_cat(at, "Direct Adjustment")
    assert at.session_state["wiz"]["requires_approval"] is False
    assert not _ss(at, f"approval_{v}")


def test_every_adjustment_type_offers_every_scope():
    """I8 / S7 — TRANSFER_SCOPES, the `options` parameter of the scope pills
    and _scope_drop_note existed only to support a restriction that does not
    exist. The branch was unreachable (the only caller passed ALL_SCOPES),
    its message contradicted the constant it was built on, and it dropped
    scopes WITHOUT calling _purge_filters_for — so the dropped scopes'
    filters survived into the payload. All three are gone."""
    from utils.styles import ALL_SCOPES
    at = _load()
    at.session_state["wiz"] = {**at.session_state["wiz"],
                               "category": "Scaling Adjustment",
                               "adjustment_type": "Transfer"}
    at.run()
    assert not at.exception, at.exception
    v = at.session_state["_wiz_v"]
    keys = {b.key for b in at.button}
    for sc in ALL_SCOPES:
        assert f"scope_Scaling Adjustment_{sc}_{v}" in keys, (sc, sorted(keys))
    # Read from the parsed module, not the source text: the docstrings
    # deliberately name what was removed and why.
    assert "TRANSFER_SCOPES" not in _module_assignments()
    assert "_scope_drop_note" not in _string_constants()
    assert not _fn_args("_render_scope_pills")      # no `options` parameter


def test_a_dropdown_value_dropped_by_a_narrowed_list_is_named():
    """I10 — `st.session_state[key] = cur if cur in opts else ""` reset the
    widget with no notice, contradicting _purge_filters_for's stated policy
    that clearing a filter the user chose is never silent. The reachable
    callers are the ones that pass value=None and let the widget own the
    field (the Direct per-row grid, where the row's Book Code list is
    narrowed by the row's Entity). Here the same state is set up directly:
    the widget holds a code the current option list does not offer."""
    at = _load()
    at.session_state["_ref_sim_names"] = [["SIM1", "SRC1"], ["SIM2", "SRC1"]]
    v = _scaling_draft(at, ["Stress"])
    at.selectbox(key=f"simulation_name_{v}").select("SIM2").run()
    assert not at.exception, at.exception
    assert at.session_state["wiz"]["simulation_name"] == "SIM2"

    # The list narrows out from under the widget while the model is not
    # carrying the value — the shape of every caller that passes value=None.
    at.session_state["_ref_sim_names"] = [["SIM1", "SRC1"]]
    at.session_state["wiz"]["simulation_name"] = None
    at.run()
    assert not at.exception, at.exception

    assert at.session_state[f"simulation_name_{v}"] == ""
    assert not at.session_state["wiz"]["simulation_name"]
    assert any("Cleared 'SIM2'" in c.value for c in at.caption), \
        [c.value for c in at.caption]


def test_a_purge_does_not_also_print_the_dropped_value_caption():
    """The other half of I10: a purge already prints its own warning naming
    every filter it cleared. It pops the widget key rather than leaving a
    stale value in it, which lands in _code_select's "widget is new" branch —
    so the two messages never double up on the same filter."""
    at = _load()
    at.session_state["_ref_sim_names"] = [["SIM1", "SRC1"], ["SIM2", "SRC1"]]
    _scaling_draft(at, ["Stress"], simulation_name="SIM1")
    _scope_click(at, "VaR")
    assert not [c.value for c in at.caption if "Cleared 'SIM1'" in c.value]
    _scope_click(at, "VaR")
    assert not [c.value for c in at.caption if "Cleared 'SIM1'" in c.value]


def test_day_type_is_an_ordinary_code_select():
    """M7 — _render_day_type reseeded st.session_state[key] from the model on
    EVERY rerun, the exact anti-pattern _code_select's docstring warns
    against thirty lines above it, and needed an on_change callback to
    survive it. Folded into _code_select, with the labels as the format."""
    at = _load()
    v = _scaling_draft(at, ["VaR"])          # Day Type is main-row, VaR only
    assert "_render_day_type" not in _fn_names()

    sb = at.selectbox(key=f"day_type_main_{v}")
    assert len(sb.options) == 3              # "— both —", 1-day, 10-day
    sb.select_index(2).run()
    assert not at.exception, at.exception
    assert at.session_state["wiz"]["day_type"] == "10"

    at.selectbox(key=f"day_type_main_{v}").select_index(0).run()
    assert not at.exception, at.exception
    # Blank is stored as None, not "" — that is what the payload builder and
    # the previous on_change callback both wrote.
    assert at.session_state["wiz"]["day_type"] is None


def test_reset_wizard_drops_the_lazy_preview_detail_frames():
    """reset_wizard already dropped _eroll_recon_cache; the lazily-loaded
    breakdown / sample / per-trade frames are memoised outside wiz in exactly
    the same way and must go with it."""
    at = _load()
    at.session_state["wiz"] = {
        **at.session_state["wiz"], "step": 3, "category": "Scaling Adjustment",
        "process_types": ["VaR"],
        "result": {"status": "Pending", "adj_id": "x",
                   "message": "Created with status 'Pending'."}}
    for key in ("_scaling_breakdown_df", "_scaling_sample_df", "_trf_breakdown_df"):
        at.session_state[key] = pd.DataFrame({"A": [1]})
        at.session_state[f"{key}_for"] = "some-old-payload"
    at.run(); assert not at.exception, at.exception

    at.button(key="new_adj").click().run()      # → reset_wizard()
    assert not at.exception, at.exception
    for key in ("_scaling_breakdown_df", "_scaling_sample_df", "_trf_breakdown_df"):
        assert key not in at.session_state
        assert f"{key}_for" not in at.session_state


def test_the_preview_sql_panel_is_gated_on_a_current_preview():
    """The SQL panel says "Exactly what the impact preview ran". Once the
    filters have moved on that is no longer true of the CURRENT payload, so
    the button would fetch SQL for one set of filters and print it beside
    numbers that came from another. The panel goes with the numbers."""
    at = _load()
    w = _preview_scaling(at, ["VaR"])
    v = at.session_state["_wiz_v"]
    assert _button(at, f"preview_sql_load_{v}", required=False) is not None

    # Same preview numbers, different payload: the panel is gone and the page
    # says why, and nothing can be fetched for the stale figures.
    at.session_state["wiz"] = {**at.session_state["wiz"], "tenor_code": "5Y"}
    at.run(); assert not at.exception, at.exception
    assert at.session_state["wiz"]["_preview_sum"] is not None
    assert _button(at, f"preview_sql_load_{v}", required=False) is None
    assert any("Filters changed since the last preview" in i.value
               for i in at.info), [i.value for i in at.info]


def test_the_page_does_not_send_its_own_reopen_notification():
    """SP_REQUEST_SIGNOFF_CHANGE notifies the approvers itself, after its own
    commit, for every approval-gated request from any page. _request_reopen
    fired the same SP_NOTIFY('reopen_requested', …) straight afterwards — so
    every re-open raised from this page sent the approvers two identical
    emails, and the page's copy went out even when the request had not
    committed. One sender, and it is the one inside the transaction."""
    assert not [s for s in _string_constants() if "SP_NOTIFY" in s]


# ── Select all / Clear on the scope pills (Marcos, 2026-09-23) ───────────────
# Six scopes took six clicks, and a Transfer Book across every scope is a
# normal thing to want. These drive the real buttons through AppTest rather
# than calling the renderer, so a renamed key or a bad Material icon name
# fails here instead of in the deployed app.

def _scope_draft(at, scopes, category="Scaling Adjustment"):
    at.session_state["wiz"] = {**at.session_state["wiz"],
                               "category": category,
                               "adjustment_type": "Scale",
                               "process_types": list(scopes),
                               "process_type": scopes[0] if scopes else None}
    at.run()
    assert not at.exception, at.exception
    return at


def test_select_all_picks_every_scope():
    from utils.styles import ALL_SCOPES
    at = _scope_draft(_load(), ["VaR"])
    at.button(key=f"scope_all_Scaling Adjustment_"
                  f"{at.session_state['_wiz_v']}").click().run()
    assert not at.exception, at.exception
    assert at.session_state["wiz"]["process_types"] == list(ALL_SCOPES)


def test_select_all_keeps_the_canonical_scope_order():
    """One adjustment per scope is created in this order, so it must be stable."""
    from utils.styles import ALL_SCOPES
    at = _scope_draft(_load(), ["FRTBDRC", "VaR"])
    at.button(key=f"scope_all_Scaling Adjustment_"
                  f"{at.session_state['_wiz_v']}").click().run()
    assert at.session_state["wiz"]["process_types"] == list(ALL_SCOPES)


def test_select_all_sets_the_legacy_single_scope_too():
    """wiz['process_type'] still feeds older readers; it must not go stale."""
    from utils.styles import ALL_SCOPES
    at = _scope_draft(_load(), [])
    at.button(key=f"scope_all_Scaling Adjustment_"
                  f"{at.session_state['_wiz_v']}").click().run()
    assert at.session_state["wiz"]["process_type"] == ALL_SCOPES[0]


def test_clear_empties_the_selection():
    at = _scope_draft(_load(), ["VaR", "Stress"])
    at.button(key=f"scope_clear_Scaling Adjustment_"
                  f"{at.session_state['_wiz_v']}").click().run()
    assert not at.exception, at.exception
    assert at.session_state["wiz"]["process_types"] == []
    assert at.session_state["wiz"]["process_type"] is None


def test_select_all_throws_away_the_previous_preview():
    """The preview belonged to the old scope set; keeping it would let a
    stale zero-row verdict skip a scope the user just added."""
    at = _load()
    at.session_state["wiz"] = {**at.session_state["wiz"],
                               "category": "Scaling Adjustment",
                               "adjustment_type": "Scale",
                               "process_types": ["VaR"], "process_type": "VaR",
                               "_preview_by_scope": {"VaR": 0},
                               "_zero_preview": True}
    at.run(); assert not at.exception, at.exception
    at.button(key=f"scope_all_Scaling Adjustment_"
                  f"{at.session_state['_wiz_v']}").click().run()
    assert not at.exception, at.exception
    assert not at.session_state["wiz"].get("_preview_by_scope")
    assert not at.session_state["wiz"].get("_zero_preview")


def test_both_bulk_buttons_render_for_entity_roll():
    """Entity Roll shares the pills, so it gets the shortcut too."""
    at = _scope_draft(_load(), ["VaR"], category="Entity Roll")
    v = at.session_state["_wiz_v"]
    assert _button(at, f"scope_all_Entity Roll_{v}") is not None
    assert _button(at, f"scope_clear_Entity Roll_{v}") is not None


# ── FRTB is shown as FRTBSBM everywhere it is displayed ─────────────────────
# The stored PROCESS_TYPE is "FRTB"; the app has called that scope "FRTBSBM"
# since 2026-09-15 (utils/styles.SCOPE_DISPLAY). The Direct Adjustment scope
# pills were still rendering the raw code, because _pill_row falls back to
# str(option) when no `fmt` is passed (Marcos, 2026-09-23).

def _direct_draft(at):
    at.session_state["wiz"] = {**at.session_state["wiz"],
                               "category": "Direct Adjustment",
                               "process_type": "VaR", "step": 1}
    at.run()
    assert not at.exception, at.exception
    return at


def test_direct_scope_pill_is_labelled_frtbsbm():
    at = _direct_draft(_load())
    v = at.session_state["_wiz_v"]
    pill = _button(at, f"direct_scope_FRTB_{v}")
    assert pill.label == "FRTBSBM", (
        "The Direct scope pill shows " + repr(pill.label) + ". The app calls "
        "this scope FRTBSBM; only the stored PROCESS_TYPE stays 'FRTB'.")


def test_direct_scope_pills_use_display_names_throughout():
    """Every Direct pill must show its display name, not its stored code."""
    from utils.styles import scope_label
    at = _direct_draft(_load())
    v = at.session_state["_wiz_v"]
    for code in ("VaR", "Stress", "Sensitivity", "FRTB", "FRTBDRC", "FRTBRRAO"):
        pill = _button(at, f"direct_scope_{code}_{v}")
        assert pill.label == scope_label(code), (code, pill.label)


def test_direct_scope_pill_keys_still_carry_the_stored_code():
    """Keys are state, not display: relabelling must not rename them.

    A renamed key resets the widget and, worse, would make the click handler
    hand a label where a PROCESS_TYPE is expected.
    """
    at = _direct_draft(_load())
    v = at.session_state["_wiz_v"]
    assert _button(at, f"direct_scope_FRTB_{v}", required=False) is not None
    assert _button(at, f"direct_scope_FRTBSBM_{v}", required=False) is None, \
        "The pill key must stay the stored code, not the display label."


def test_clicking_the_frtbsbm_pill_stores_the_code_frtb():
    """The round trip that matters: label out, code in."""
    at = _direct_draft(_load())
    at.button(key=f"direct_scope_FRTB_"
                  f"{at.session_state['_wiz_v']}").click().run()
    assert not at.exception, at.exception
    assert at.session_state["wiz"]["process_type"] == "FRTB", \
        "Selecting the pill must store the PROCESS_TYPE, never the label."
