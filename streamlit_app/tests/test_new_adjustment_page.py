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
    # Both scopes' preview SQL, each under its own header.
    assert "-- VaR" in w["_preview_sql"] and "-- Stress" in w["_preview_sql"]


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
    # FOUR jobs (summary + sql, twice) — the two breakdown calls are gone.
    kinds = [k for k, _ in ORDER]
    assert kinds == ["submit"] * 4 + ["result"] * 4, ORDER
    assert not any('"mode": "breakdown"' in c for c in CALLS), CALLS
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
        # Only the two CHEAP modes ride along now. `breakdown` used to be
        # submitted here too, purely to count fallback trades — a whole extra
        # scan of the combined view on every preview. It is lazy (a button
        # inside its expander) and the fallback count comes from
        # DIMENSION.TRADE instead.
        assert {json.loads(c[c.index("('") + 2:c.rindex("')")]
                           .replace("''", "'").replace("\\\\", "\\"))["mode"]
                for c in prev} == {"summary", "sql"}

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
