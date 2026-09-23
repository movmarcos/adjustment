"""Guards on the FRTB Direct USD-to-local currency conversion.

Marcos, 2026-09-23: the uploader should supply ONLY the USD figure, and the
system derives the local-currency amount from FACT.EXCHANGE_RATE:

    local = usd / rate      (the rate converts the row's currency INTO USD,
                             so TO_CURRENCY_CODE = 'USD')

A row already in USD needs no rate and passes through untouched.

These are source-level guards. The change is SQL in three views plus one
Python writer inside a stored procedure, none of which can run locally, so
what is testable here is the SHAPE of the SQL. Each assertion below stands
for a specific way this can go wrong silently:

- multiplying instead of dividing is invisible for currencies near parity
  and wrong by a factor of ten thousand for yen
- a duplicate exchange-rate row would fan the join out and DOUBLE an
  uploaded adjustment, with no error anywhere
- a missing rate would write an EMPTY local amount into a reported figure,
  which the engine's skip-zero rule would not catch because the USD leg is
  populated
"""
import os
import re

import pytest

VIEWS_SQL = os.path.join(os.path.dirname(__file__), "..", "..",
                         "new_adjustment_db_objects",
                         "15_direct_frtb_upload.sql")
ENGINE_SQL = os.path.join(os.path.dirname(__file__), "..", "..",
                          "new_adjustment_db_objects",
                          "05_sp_process_adjustment.sql")

#: view name -> (local metric, USD metric)
SCOPES = {
    "VW_DIRECT_FRTB_ENRICHED":      ("AMOUNT", "AMOUNT_IN_USD"),
    "VW_DIRECT_FRTBDRC_ENRICHED":   ("JTD_LOSS", "JTD_LOSS_USD"),
    "VW_DIRECT_FRTBRRAO_ENRICHED":  ("NOTIONAL_AMOUNT", "NOTIONAL_AMOUNT_USD"),
}


def _source(path):
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def _view_bodies():
    src = _source(VIEWS_SQL)
    out = {}
    for m in re.finditer(
            r"CREATE OR REPLACE VIEW ADJUSTMENT_APP\.(\w+) AS"
            r"(.*?)(?=CREATE OR REPLACE VIEW|\Z)", src, re.S):
        out[m.group(1)] = m.group(2)
    return out


def _fx_cte(body):
    """The text of the view's `fx` CTE."""
    m = re.search(r"\bfx AS \((.*?)\n\),", body, re.S)
    return m.group(1) if m else ""


@pytest.fixture(scope="module")
def views():
    bodies = _view_bodies()
    assert set(SCOPES) <= set(bodies), sorted(bodies)
    return bodies


ALL = list(SCOPES)


# ── The conversion itself ────────────────────────────────────────────────────

@pytest.mark.parametrize("view", ALL)
def test_local_amount_is_usd_divided_by_the_rate(views, view):
    local, usd = SCOPES[view]
    body = views[view]
    expected = "base." + usd + " / NULLIF(fx.EXCHANGE_RATE, 0)"
    assert expected in body, (
        view + " does not divide the USD figure by the rate. Multiplying "
        "instead is invisible for currencies near parity and wrong by "
        "10,000x for JPY.")
    assert "fx.EXCHANGE_RATE *" not in body and "* fx.EXCHANGE_RATE" not in body, (
        view + " multiplies by the exchange rate somewhere. The rate "
        "converts INTO USD, so going the other way is a division.")


@pytest.mark.parametrize("view", ALL)
def test_division_cannot_divide_by_zero(views, view):
    assert "NULLIF(fx.EXCHANGE_RATE, 0)" in views[view], (
        view + " divides by the raw rate. EXCHANGE_RATE is nullable and a "
        "zero would raise at query time for the whole batch.")


@pytest.mark.parametrize("view", ALL)
def test_a_usd_row_is_not_converted(views, view):
    local, usd = SCOPES[view]
    body = views[view]
    assert "UPPER(COALESCE(base.CURRENCY_CODE_SRC, 'USD')) = 'USD'" in body, (
        view + " has no USD short-circuit. A USD row must pass through "
        "untouched and must not depend on a rate row existing.")
    assert re.search(r"THEN base\." + re.escape(usd), body), (
        view + "'s USD branch does not pass the USD figure straight through.")


@pytest.mark.parametrize("view", ALL)
def test_the_local_amount_is_no_longer_read_from_the_upload(views, view):
    local, _ = SCOPES[view]
    body = views[view]
    assert 'PAYLOAD:"' + local + '"' not in body, (
        view + " still reads " + local + " from the uploaded row. It is now "
        "derived, and reading it back would let a stale file override the "
        "calculation.")


@pytest.mark.parametrize("view", ALL)
def test_the_usd_amount_is_still_read_from_the_upload(views, view):
    _, usd = SCOPES[view]
    assert 'PAYLOAD:"' + usd + '"' in views[view], (
        views and view + " no longer reads " + usd + ", which is the one "
        "number the uploader actually supplies.")


# ── The join cannot corrupt the batch ────────────────────────────────────────

@pytest.mark.parametrize("view", ALL)
def test_the_rate_is_pre_aggregated_so_the_join_cannot_fan_out(views, view):
    fx = _fx_cte(views[view])
    assert fx, view + " has no fx CTE"
    assert "GROUP BY" in fx, (
        view + "'s fx CTE is not aggregated. Nothing enforces the grain of "
        "FACT.EXCHANGE_RATE, so a duplicate rate row would multiply every "
        "uploaded row and silently double the adjustment.")
    assert "MAX(r.EXCHANGE_RATE)" in fx


@pytest.mark.parametrize("view", ALL)
def test_only_usd_rates_are_considered(views, view):
    fx = _fx_cte(views[view])
    assert "r.TO_CURRENCY_CODE = 'USD'" in fx, (
        view + " does not restrict to rates INTO USD, so it could pick up a "
        "cross rate and convert to the wrong currency.")


@pytest.mark.parametrize("view", ALL)
def test_the_rate_scan_is_pruned_by_cob(views, view):
    fx = _fx_cte(views[view])
    assert "r.COBID IN (SELECT DISTINCT COBID FROM base)" in fx, (
        view + "'s rate lookup is not pinned to the batch's COBs, so it "
        "scans every COB ever loaded. Standing rule: always pin what you "
        "can so the scan prunes.")


@pytest.mark.parametrize("view", ALL)
def test_the_join_matches_cob_region_and_currency(views, view):
    body = views[view]
    for clause in ("fx.COBID                = base.COBID",
                   "fx.REGION_AREA_CODE_U   = UPPER(base.REGION_AREA_CODE)",
                   "fx.FROM_CURRENCY_CODE_U = UPPER(base.CURRENCY_CODE_SRC)"):
        assert clause in body, view + " is missing join clause: " + clause


@pytest.mark.parametrize("view", ALL)
def test_the_join_is_a_left_join(views, view):
    assert "LEFT JOIN fx" in views[view], (
        view + " inner-joins the rate table, so a row with no rate would "
        "VANISH from the upload instead of being reported.")


@pytest.mark.parametrize("view", ALL)
def test_currency_has_one_definition(views, view):
    """It used to be spelled out at each use; the FX join added three more."""
    body = views[view]
    assert "AS CURRENCY_CODE_SRC" in body
    assert "base.CURRENCY_CODE_SRC AS CURRENCY_CODE" in body, (
        view + " derives the currency separately from the one the FX join "
        "uses. They must be the same expression or a row can convert using "
        "a different currency than it reports.")


# ── A missing rate is reported, never silently blank ─────────────────────────

@pytest.mark.parametrize("view", ALL)
def test_a_missing_rate_is_flagged(views, view):
    body = views[view]
    assert "AS FX_RATE_MISSING" in body, view + " does not flag a missing rate"
    assert "AS FX_RATE_USED" in body, (
        view + " does not expose the rate it used, which is the first thing "
        "anyone checks when a converted number looks wrong.")
    assert "COALESCE(fx.EXCHANGE_RATE, 0) = 0) AS FX_RATE_MISSING" in body


def test_the_writer_refuses_rows_with_no_rate():
    src = _source(ENGINE_SQL)
    assert 'if "FX_RATE_MISSING" in view_cols:' in src, (
        "The FRTB writer does not check FX_RATE_MISSING. A non-USD row with "
        "no rate would be written with an EMPTY local amount.")
    assert "no USD exchange rate at COB" in src, (
        "The refusal must say what went wrong, naming the COB and currency.")


def test_the_writer_checks_before_it_inserts():
    """Order matters: a check after the INSERT has already written the rows."""
    src = _source(ENGINE_SQL)
    check = src.index('if "FX_RATE_MISSING" in view_cols:')
    insert = src.index("INSERT INTO {fact_adj_tbl_name} ({cols})")
    assert check < insert, (
        "The FX check runs after the INSERT, so the bad rows are already in "
        "the fact table by the time it raises.")


def test_the_missing_rate_check_is_not_left_to_the_skip_zero_rule():
    """The skip-zero rule cannot catch this, and the comment must say so."""
    src = _source(ENGINE_SQL)
    assert "skip-zero predicate below would not catch it" in src, (
        "Leave the reasoning in place: the next person will otherwise assume "
        "the existing zero filter already handles a missing rate. It does "
        "not, because the USD leg is populated.")


# ── The upload template ──────────────────────────────────────────────────────

@pytest.mark.parametrize("view", ALL)
def test_the_template_no_longer_asks_for_the_local_amount(views, view):
    local, usd = SCOPES[view]
    src = _source(VIEWS_SQL)
    assert '{"name":"' + local + '","type":"number","required":true}' not in src, (
        "The upload template still requires " + local + ". Users supply the "
        "USD figure only now, and FRTB Explore builds its download from this "
        "same template.")
    assert '{"name":"' + usd + '","type":"number","required":true}' in src, (
        "The template must still require " + usd + " — it is the only "
        "number the uploader provides.")


# ── The FRTB Explore export must not carry the local amount either ──────────
# Explore's whole point is that its CSV IS the upload template: you download
# rows, edit them, upload them back. If it still exported the local amount,
# the file would be rejected as carrying an unknown column, and before that
# it would have invited people to hand-enter a number the system now derives.

import ast as _ast

EXPLORE = os.path.join(os.path.dirname(__file__), "..", "pages",
                       "8_FRTB_Explore.py")

#: PROCESS_TYPE -> (local metric, USD metric), same pairs as the views.
EXPLORE_SCOPES = {
    "FRTB":     ("AMOUNT", "AMOUNT_IN_USD"),
    "FRTBDRC":  ("JTD_LOSS", "JTD_LOSS_USD"),
    "FRTBRRAO": ("NOTIONAL_AMOUNT", "NOTIONAL_AMOUNT_USD"),
}


def _explore_fallback_columns():
    """{scope: [column, ...]} from the _TYPES config, parsed not grepped."""
    out = {}
    for node in _ast.walk(_ast.parse(_source(EXPLORE))):
        if not isinstance(node, _ast.Call):
            continue
        kw = {k.arg: k.value for k in node.keywords if k.arg}
        if "scope" not in kw or "columns" not in kw:
            continue
        try:
            out[_ast.literal_eval(kw["scope"])] = _ast.literal_eval(kw["columns"])
        except ValueError:
            continue
    return out


@pytest.mark.parametrize("scope", sorted(EXPLORE_SCOPES))
def test_explore_fallback_columns_drop_the_local_metric(scope):
    local, usd = EXPLORE_SCOPES[scope]
    cols = _explore_fallback_columns()
    assert scope in cols, sorted(cols)
    if scope == "FRTBDRC":
        # DRC's NOTIONAL_AMOUNT is not a metric pair — no USD counterpart on
        # the fact table, still supplied by hand — so only JTD_LOSS goes.
        assert "NOTIONAL_AMOUNT" in cols[scope]
    assert local not in cols[scope], (
        "FRTB Explore still exports " + local + " for " + scope + ". The "
        "export is the upload template, and uploaders supply USD only now.")
    assert usd in cols[scope], (
        "FRTB Explore must still export " + usd + " for " + scope)


def test_explore_prefers_the_upload_template_over_its_own_list():
    """The fallback exists for a missing schema row; it is not the main path."""
    src = _source(EXPLORE)
    assert "sel_cols = list(tmpl_cols)" in src, (
        "FRTB Explore no longer takes its columns from DIRECT_SCOPE_SCHEMA. "
        "That link is what keeps the export and the upload template in step.")
    assert "DIRECT_SCOPE_SCHEMA" in src


def test_the_template_row_is_refreshed_on_redeploy():
    """Dropping a column from the seed only helps if the MERGE writes it.

    DIRECT_SCOPE_SCHEMA rows already exist in every deployed environment, so
    a MERGE that only set columns on INSERT would leave the old template —
    and the local amount — in place forever.
    """
    src = _source(VIEWS_SQL)
    updates = re.findall(r"WHEN MATCHED THEN UPDATE SET(.*?)WHEN NOT MATCHED",
                         src, re.S)
    assert len(updates) == 3, "expected one MERGE per FRTB scope"
    for block in updates:
        assert "t.EXPECTED_COLUMNS = s.EXPECTED_COLUMNS" in block, (
            "A MERGE does not refresh EXPECTED_COLUMNS, so redeploying would "
            "leave the old template (and the local amount column) in place.")
