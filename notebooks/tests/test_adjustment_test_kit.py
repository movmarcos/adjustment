"""Local pytest suite for notebooks/adjustment_test_kit.py.

No network, no Snowflake, no real sleeping anywhere: every test drives `kit`
through a hand-rolled FakeSession (records every query, answers from
substring rules, can be told to raise on a query) — the same shape the app's
own tests use for Snowpark, see streamlit_app/tests/test_transfer_form.py and
streamlit_app/tests/test_sql_escape.py.
"""
import json

import pytest

import adjustment_test_kit as kit
from utils.scope_filters import SCOPE_FILTER_FIELDS as APP_SCOPE_FILTER_FIELDS
from utils.snowflake_conn import sql_escape as app_sql_escape


# ══════════════════════════════════════════════════════════════════════════
# FakeSession
# ══════════════════════════════════════════════════════════════════════════

class FakeSession:
    """A `.sql(query).collect()` double.

    - `.calls` records every query string handed to `.sql()`, in order.
    - `.when(pattern, rows)` answers a matching query with `rows` — a list of
      plain dicts (kit's `_to_dict` passes dicts through unchanged), or a
      zero-arg callable evaluated fresh per call for a query that must answer
      differently each time it runs (e.g. a polling loop's status check).
      `pattern` is a substring to look for in the query text, or a one-arg
      predicate. Rules are checked in the order added; first match wins.
    - `.raise_on(pattern, exc)` makes a matching query raise `exc` instead of
      returning rows, checked before any `.when()` rule.
    """

    def __init__(self, default_rows=None):
        self.calls = []
        self._rules = []
        self._raises = []
        self.default_rows = default_rows if default_rows is not None else []

    @staticmethod
    def _matcher(pattern):
        if callable(pattern):
            return pattern
        return lambda q, s=pattern: s in q

    def when(self, pattern, rows):
        self._rules.append((self._matcher(pattern), rows))
        return self

    def raise_on(self, pattern, exc):
        self._raises.append((self._matcher(pattern), exc))
        return self

    def sql(self, query, *a, **k):
        return _FakeResult(self, query)


class _FakeResult:
    def __init__(self, session, query):
        self.session = session
        self.query = query

    def collect(self):
        self.session.calls.append(self.query)
        for matcher, exc in self.session._raises:
            if matcher(self.query):
                raise exc
        for matcher, rows in self.session._rules:
            if matcher(self.query):
                return rows() if callable(rows) else rows
        return self.session.default_rows


# ── Fixtures / builders ─────────────────────────────────────────────────────

def _scope_cfg(**over):
    base = dict(
        process_type="VaR",
        fact_table="TESTDB.FACT_VAR",
        fact_adjusted_table="TESTDB.FACT_VAR_ADJUSTED",
        adjustments_table="TESTDB.ADJ_VAR",
        adjustments_summary_table="TESTDB.ADJ_VAR_SUMMARY",
        fact_table_pk="TRADE_KEY",
        metric_name="VALUE",
        metric_usd_name="VALUE_USD",
        is_active=True,
    )
    base.update(over)
    return kit.ScopeConfig(**base)


def _cfg(**over):
    base = dict(
        cob=kit.TEST_COB,
        allow_real_cob=False,
        seed_source_cob=20260101,
        seed_rows=100,
        book_code="UATBOOK",
        target_book_code="UATBOOK2",
        entity_code="ENT1",
        submitter="UAT_BOT_SUBMITTER",
        approver="UAT_BOT_APPROVER",
        force=False,
        timeout_s=60,
        poll_s=5,
        keep_failures=True,
        max_scan_ratio=0.80,
    )
    base.update(over)
    return kit.HarnessConfig(**base)


SIX_SCOPES = ["VaR", "Stress", "Sensitivity", "FRTB", "FRTBDRC", "FRTBRRAO"]


# ══════════════════════════════════════════════════════════════════════════
# 1. sql_escape
# ══════════════════════════════════════════════════════════════════════════

def test_sql_escape_doubles_backslashes_before_quotes():
    assert kit.sql_escape("a\\'b") == "a\\\\''b"


def test_sql_escape_none_becomes_empty_string():
    assert kit.sql_escape(None) == ""


def test_sql_escape_both_backslash_and_quote_together():
    assert kit.sql_escape("x' OR 1=1 -- C:\\temp") == \
        "x'' OR 1=1 -- C:\\\\temp"


NASTY_STRINGS = [
    "plain", "O'Brien", "C:\\temp", "a\\'b", "x' OR 1=1 --",
    "trailing\\", "\\'\\'", "''''", "\\\\\\\\", "", "line1\nline2",
    "unicode ☺ 'quote'", "a\\b\\c'd'e\\", None, 20260101, 3.5,
]


def test_sql_escape_agrees_with_the_app_sql_escape():
    for value in NASTY_STRINGS:
        kit_out = kit.sql_escape(value)
        app_out = app_sql_escape(value)
        assert kit_out == app_out, (
            "kit.sql_escape and app sql_escape disagree on " + repr(value) +
            ": kit=" + repr(kit_out) + " app=" + repr(app_out))


# ══════════════════════════════════════════════════════════════════════════
# 2. SCOPE_FILTER_FIELDS parity
# ══════════════════════════════════════════════════════════════════════════

def test_scope_filter_fields_matches_the_app_exactly():
    kit_keys = set(kit.SCOPE_FILTER_FIELDS)
    app_keys = set(APP_SCOPE_FILTER_FIELDS)
    assert kit_keys == app_keys, (
        "kit's vendored SCOPE_FILTER_FIELDS covers a different set of scopes "
        "than streamlit_app/utils/scope_filters.py: only in kit=" +
        repr(kit_keys - app_keys) + " only in app=" + repr(app_keys - kit_keys))

    for scope in app_keys:
        kit_set = set(kit.SCOPE_FILTER_FIELDS[scope])
        app_set = set(APP_SCOPE_FILTER_FIELDS[scope])
        assert kit_set == app_set, (
            "kit's vendored SCOPE_FILTER_FIELDS['" + scope + "'] has drifted "
            "from the app's copy — kit is missing " +
            repr(app_set - kit_set) + " and has extra " +
            repr(kit_set - app_set) + ". Update the vendored copy in "
            "notebooks/adjustment_test_kit.py to match "
            "streamlit_app/utils/scope_filters.py.")


# ══════════════════════════════════════════════════════════════════════════
# 3. HarnessConfig.validate
# ══════════════════════════════════════════════════════════════════════════

def test_validate_raises_when_cob_is_not_the_test_cob():
    cfg = _cfg(cob=20260101, allow_real_cob=False)
    with pytest.raises(ValueError, match="not the reserved"):
        cfg.validate()


def test_validate_passes_when_allow_real_cob_is_true():
    cfg = _cfg(cob=20260101, allow_real_cob=True)
    assert cfg.validate() is True


def test_validate_raises_when_submitter_equals_approver_case_insensitively():
    cfg = _cfg(submitter="Uat_Bot", approver="uat_bot")
    with pytest.raises(ValueError, match="must differ"):
        cfg.validate()


def test_validate_raises_on_non_positive_seed_rows():
    cfg = _cfg(seed_rows=0)
    with pytest.raises(ValueError, match="seed_rows must be positive"):
        cfg.validate()
    cfg2 = _cfg(seed_rows=-5)
    with pytest.raises(ValueError, match="seed_rows must be positive"):
        cfg2.validate()


def test_validate_passes_for_a_sane_default_config():
    assert _cfg().validate() is True


# ══════════════════════════════════════════════════════════════════════════
# 4. build_matrix
# ══════════════════════════════════════════════════════════════════════════

def test_full_mode_over_six_scopes_yields_37_scenarios():
    cfg = _cfg()
    scen = kit.build_matrix(SIX_SCOPES, mode="full", cfg=cfg)
    assert len(scen) == 37


def test_smoke_mode_yields_one_scale_per_scope():
    cfg = _cfg()
    scen = kit.build_matrix(SIX_SCOPES, mode="smoke", cfg=cfg)
    assert len(scen) == len(SIX_SCOPES)
    assert all(s.adjustment_type == "Scale" for s in scen)
    assert {s.scope for s in scen} == set(SIX_SCOPES)


def test_single_mode_yields_one_scenario():
    cfg = _cfg()
    scen = kit.build_matrix(SIX_SCOPES, mode="single", cfg=cfg)
    assert len(scen) == 1
    assert scen[0].adjustment_type == "Scale"


def test_negative_mode_delegates_to_negative_matrix():
    cfg = _cfg()
    from_build = kit.build_matrix(SIX_SCOPES, mode="negative", cfg=cfg)
    from_negative = kit.negative_matrix(SIX_SCOPES, cfg)
    assert from_build == from_negative


def test_no_generated_scale_scenario_ever_has_factor_one():
    cfg = _cfg()
    for mode in ("smoke", "single", "full"):
        for scen in kit.build_matrix(SIX_SCOPES, mode=mode, cfg=cfg):
            if scen.adjustment_type == "Scale":
                assert scen.params.get("scale_factor") != 1.0, (
                    "mode=" + mode + " generated a Scale with factor 1.0, "
                    "which SP_SUBMIT_ADJUSTMENT hard-rejects")


def test_every_roll_has_a_source_cobid_different_from_the_config_cob():
    cfg = _cfg()
    rolls = [s for s in kit.build_matrix(SIX_SCOPES, mode="full", cfg=cfg)
             if s.adjustment_type == "Roll"]
    assert rolls, "expected at least one Roll scenario in full mode"
    for s in rolls:
        assert s.params["source_cobid"] != cfg.cob


def test_every_transfer_has_two_different_book_codes():
    cfg = _cfg()
    transfers = [s for s in kit.build_matrix(SIX_SCOPES, mode="full", cfg=cfg)
                 if s.adjustment_type == "Transfer"]
    assert transfers, "expected at least one Transfer scenario in full mode"
    for s in transfers:
        assert s.params["source_book_code"] != s.params["book_code"]


def test_upload_only_generated_for_var():
    cfg = _cfg()
    uploads = [s for s in kit.build_matrix(SIX_SCOPES, mode="full", cfg=cfg)
               if s.adjustment_type == "Upload"]
    assert [s.scope for s in uploads] == ["VaR"]


# ══════════════════════════════════════════════════════════════════════════
# 5. build_payload
# ══════════════════════════════════════════════════════════════════════════

def test_erol_forces_requires_approval_and_strips_book_and_factor():
    cfg = _cfg(entity_code="ENT1")
    scenario = kit.Scenario("EROL-VAR", "VaR", "EROL",
                             {"entity_code": "ENT1", "source_cobid": 20260101,
                              "book_code": "SHOULD-BE-STRIPPED",
                              "scale_factor": 1.23})
    payload = kit.build_payload(scenario, cfg)
    assert payload["requires_approval"] is True
    assert "book_code" not in payload
    assert "scale_factor" not in payload


def test_transfer_does_not_get_entity_code_injected():
    cfg = _cfg(entity_code="ENT1")
    scenario = kit.Scenario("TRN-VAR", "VaR", "Transfer",
                             {"source_book_code": "UATBOOK",
                              "book_code": "UATBOOK2"})
    payload = kit.build_payload(scenario, cfg)
    assert "entity_code" not in payload


def test_other_types_get_the_config_entity_injected():
    cfg = _cfg(entity_code="ENT1")
    for adj_type in ("Flatten", "Scale", "Roll", "Direct"):
        scenario = kit.Scenario("X-VAR", "VaR", adj_type, {"book_code": "UATBOOK"})
        payload = kit.build_payload(scenario, cfg)
        assert payload["entity_code"] == "ENT1", adj_type


def test_build_payload_does_not_override_an_explicit_entity_code():
    cfg = _cfg(entity_code="ENT1")
    scenario = kit.Scenario("SCA-VAR", "VaR", "Scale",
                             {"book_code": "UATBOOK", "entity_code": "OTHER"})
    payload = kit.build_payload(scenario, cfg)
    assert payload["entity_code"] == "OTHER"


# ══════════════════════════════════════════════════════════════════════════
# 6. seed
# ══════════════════════════════════════════════════════════════════════════

def test_seed_refuses_a_non_test_cob():
    session = FakeSession()
    scope_cfg = _scope_cfg()
    cfg = _cfg()
    with pytest.raises(ValueError, match="refuses to write"):
        kit.seed(session, scope_cfg, cfg, book_keys=[1], cob=20260101)
    assert session.calls == []


def test_seed_refuses_when_seed_source_cob_unset():
    session = FakeSession()
    scope_cfg = _scope_cfg()
    cfg = _cfg(seed_source_cob=0)
    with pytest.raises(ValueError, match="seed_source_cob is not set"):
        kit.seed(session, scope_cfg, cfg, book_keys=[1])
    assert session.calls == []


def test_seed_refuses_with_no_book_keys():
    session = FakeSession()
    scope_cfg = _scope_cfg()
    cfg = _cfg()
    with pytest.raises(ValueError, match="needs at least one BOOK_KEY"):
        kit.seed(session, scope_cfg, cfg, book_keys=[])
    assert session.calls == []


def test_seed_returns_zero_without_inserting_when_rows_already_exist():
    session = FakeSession()
    session.when("SELECT COUNT(*) AS N FROM TESTDB.FACT_VAR", [{"N": 5}])
    scope_cfg = _scope_cfg()
    cfg = _cfg()
    result = kit.seed(session, scope_cfg, cfg, book_keys=[10, 20])
    assert result == 0
    assert not any("INSERT" in c for c in session.calls)


def test_seed_inserts_with_replace_and_pinned_book_keys_when_target_empty():
    session = FakeSession()
    session.when("SELECT COUNT(*) AS N FROM TESTDB.FACT_VAR", [{"N": 0}])
    scope_cfg = _scope_cfg()
    cfg = _cfg(seed_source_cob=20260101, seed_rows=500)
    kit.seed(session, scope_cfg, cfg, book_keys=[10, 20])

    inserts = [c for c in session.calls if "INSERT INTO" in c]
    assert len(inserts) == 1
    insert_sql = inserts[0]
    assert "REPLACE" in insert_sql
    assert "BOOK_KEY IN (10, 20)" in insert_sql
    assert "20260101" in insert_sql  # source COB clause
    assert "LIMIT 500" in insert_sql
    assert not any("DIMENSION.TRADE" in c for c in session.calls)


# ══════════════════════════════════════════════════════════════════════════
# 7. resolve_book
# ══════════════════════════════════════════════════════════════════════════

def test_resolve_book_compares_book_code_bare_not_wrapped_in_upper():
    session = FakeSession()
    session.when("DIMENSION.BOOK",
                 [{"BOOK_KEY": 42, "ENTITY_CODE": "ENT1", "N_ENT": 1}])
    info = kit.resolve_book(session, "BK1")

    assert info == {"book_key": 42, "entity_code": "ENT1", "ambiguous": False}
    assert len(session.calls) == 1
    query = session.calls[0]
    assert "BOOK_CODE = 'BK1'" in query, (
        "resolve_book must compare BOOK_CODE bare so the scan prunes; "
        "got: " + query)
    assert "UPPER(BOOK_CODE)" not in query, (
        "resolve_book must not wrap BOOK_CODE in UPPER() — that defeats "
        "pruning on DIMENSION.BOOK. Query was: " + query)


def test_resolve_book_returns_none_when_not_found():
    session = FakeSession()
    session.when("DIMENSION.BOOK", [{"BOOK_KEY": None, "ENTITY_CODE": None, "N_ENT": 0}])
    assert kit.resolve_book(session, "NOPE") is None


def test_resolve_book_flags_ambiguous_entity():
    session = FakeSession()
    session.when("DIMENSION.BOOK",
                 [{"BOOK_KEY": 1, "ENTITY_CODE": "E1", "N_ENT": 2}])
    info = kit.resolve_book(session, "BK1")
    assert info["ambiguous"] is True


# ══════════════════════════════════════════════════════════════════════════
# 8. wait_for_terminal
# ══════════════════════════════════════════════════════════════════════════

def _status_sequence(statuses):
    """A FakeSession .when() row generator that advances through `statuses`,
    repeating the last one once exhausted."""
    state = {"i": 0}

    def _rows():
        i = min(state["i"], len(statuses) - 1)
        state["i"] += 1
        return [{"RUN_STATUS": statuses[i]}]
    return _rows


def test_wait_for_terminal_returns_status_and_did_not_over_sleep():
    session = FakeSession()
    session.when("ADJ_HEADER", _status_sequence(["Pending", "Running", "Processed"]))
    cfg = _cfg(timeout_s=100, poll_s=1)
    sleeps = []

    status, waited = kit.wait_for_terminal(session, "ADJ1", cfg,
                                            sleep=lambda s: sleeps.append(s))

    assert status == "Processed"
    assert waited == 2.0
    assert sleeps == [1, 1]


def test_wait_for_terminal_times_out_without_looping_forever():
    session = FakeSession()
    session.when("ADJ_HEADER", _status_sequence(["Pending"]))
    cfg = _cfg(timeout_s=3, poll_s=1)
    sleeps = []

    status, waited = kit.wait_for_terminal(session, "ADJ1", cfg,
                                            sleep=lambda s: sleeps.append(s))

    assert status == ""
    assert waited == 3.0
    assert sleeps == [1, 1, 1]


# ══════════════════════════════════════════════════════════════════════════
# 9. verify
# ══════════════════════════════════════════════════════════════════════════

def _verify_session(dimension_adj_id=42, run_status="Processed",
                     is_deleted=False, record_count=10, written_n=10,
                     written_total=50.0, fact_total=None, fact_n=None):
    session = FakeSession()
    session.when("FROM " + kit.APP + ".ADJ_HEADER",
                 [{"RUN_STATUS": run_status, "DIMENSION_ADJ_ID": dimension_adj_id,
                   "IS_DELETED": is_deleted}])
    session.when("FROM DIMENSION.ADJUSTMENT",
                 [{"RECORD_COUNT": record_count, "RUN_STATUS": run_status,
                   "IS_DELETED": is_deleted}])
    session.when("FROM TESTDB.ADJ_VAR",
                 [{"N": written_n, "TOTAL": written_total}])
    if fact_total is not None:
        session.when("FROM TESTDB.FACT_VAR",
                     [{"N": fact_n, "TOTAL": fact_total}])
    return session


def _base_scenario_result(scope="VaR", adj_type="Scale", params=None):
    scenario = kit.Scenario("X-" + scope.upper(), scope, adj_type, params or {})
    return scenario, kit.ScenarioResult(scenario=scenario)


def test_verify_money_check_passes_when_written_total_equals_preview_delta():
    session = _verify_session(written_total=50.0)
    scope_cfg = _scope_cfg()
    scenario, result = _base_scenario_result(
        adj_type="Scale", params={"book_code": "UATBOOK", "scale_factor": 1.05})
    pv = {"rows": 10, "nonzero": 10, "current": 1000.0, "delta": 50.0,
          "projected": 1050.0, "error": ""}
    cfg = _cfg()

    kit.verify(session, scenario, scope_cfg, "ADJ1", pv, result, cfg)

    money = dict((c[0], c[1]) for c in result.checks)
    assert money["written total equals previewed delta"] is True


def test_verify_money_check_fails_when_written_total_differs_beyond_tolerance():
    session = _verify_session(written_total=999.0)
    scope_cfg = _scope_cfg()
    scenario, result = _base_scenario_result(
        adj_type="Scale", params={"book_code": "UATBOOK", "scale_factor": 1.05})
    pv = {"rows": 10, "nonzero": 10, "current": 1000.0, "delta": 50.0,
          "projected": 1050.0, "error": ""}
    cfg = _cfg()

    kit.verify(session, scenario, scope_cfg, "ADJ1", pv, result, cfg)

    money = dict((c[0], c[1]) for c in result.checks)
    assert money["written total equals previewed delta"] is False


def test_verify_flatten_expects_the_negative_of_the_base():
    session = _verify_session(written_total=-1000.0)
    scope_cfg = _scope_cfg()
    scenario, result = _base_scenario_result(
        adj_type="Flatten", params={"book_code": "UATBOOK"})
    pv = {"rows": 10, "nonzero": 10, "current": 1000.0, "delta": -1000.0,
          "projected": 0.0, "error": ""}
    cfg = _cfg()

    kit.verify(session, scenario, scope_cfg, "ADJ1", pv, result, cfg)

    checks = dict((c[0], c[1]) for c in result.checks)
    assert checks["flatten wrote the negative of the base"] is True


def test_verify_flatten_fails_when_written_is_not_the_negative_of_the_base():
    session = _verify_session(written_total=-500.0)
    scope_cfg = _scope_cfg()
    scenario, result = _base_scenario_result(
        adj_type="Flatten", params={"book_code": "UATBOOK"})
    pv = {"rows": 10, "nonzero": 10, "current": 1000.0, "delta": -500.0,
          "projected": 500.0, "error": ""}
    cfg = _cfg()

    kit.verify(session, scenario, scope_cfg, "ADJ1", pv, result, cfg)

    checks = dict((c[0], c[1]) for c in result.checks)
    assert checks["flatten wrote the negative of the base"] is False


def test_verify_scale_expects_base_times_factor_minus_one():
    session = _verify_session(written_total=50.0)
    scope_cfg = _scope_cfg()
    scenario, result = _base_scenario_result(
        adj_type="Scale", params={"book_code": "UATBOOK", "scale_factor": 1.05})
    pv = {"rows": 10, "nonzero": 10, "current": 1000.0, "delta": 50.0,
          "projected": 1050.0, "error": ""}
    cfg = _cfg()

    kit.verify(session, scenario, scope_cfg, "ADJ1", pv, result, cfg)

    checks = dict((c[0], c[1]) for c in result.checks)
    assert checks["scale wrote base x (factor - 1)"] is True


def test_verify_scale_fails_when_written_does_not_match_the_factor():
    session = _verify_session(written_total=999.0)
    scope_cfg = _scope_cfg()
    scenario, result = _base_scenario_result(
        adj_type="Scale", params={"book_code": "UATBOOK", "scale_factor": 1.05})
    pv = {"rows": 10, "nonzero": 10, "current": 1000.0, "delta": 50.0,
          "projected": 1050.0, "error": ""}
    cfg = _cfg()

    kit.verify(session, scenario, scope_cfg, "ADJ1", pv, result, cfg)

    checks = dict((c[0], c[1]) for c in result.checks)
    assert checks["scale wrote base x (factor - 1)"] is False


def test_verify_transfer_fails_when_source_book_totals_changed():
    session = _verify_session(written_total=200.0, fact_total=1500.0, fact_n=12)
    scope_cfg = _scope_cfg()
    scenario, result = _base_scenario_result(
        adj_type="Transfer",
        params={"source_book_code": "UATBOOK", "book_code": "UATBOOK2"})
    pv = {"rows": 5, "nonzero": 5, "current": 0.0, "delta": 200.0,
          "projected": 200.0, "error": ""}
    cfg = _cfg()
    context = {"book_key": 7,
               "source_before": {"rows": 12, "total": 1000.0}}

    kit.verify(session, scenario, scope_cfg, "ADJ1", pv, result, cfg, context)

    checks = dict((c[0], c[1]) for c in result.checks)
    assert checks["source book untouched by the transfer"] is False


def test_verify_transfer_passes_when_source_book_totals_are_unchanged():
    session = _verify_session(written_total=200.0, fact_total=1000.0, fact_n=12)
    scope_cfg = _scope_cfg()
    scenario, result = _base_scenario_result(
        adj_type="Transfer",
        params={"source_book_code": "UATBOOK", "book_code": "UATBOOK2"})
    pv = {"rows": 5, "nonzero": 5, "current": 0.0, "delta": 200.0,
          "projected": 200.0, "error": ""}
    cfg = _cfg()
    context = {"book_key": 7,
               "source_before": {"rows": 12, "total": 1000.0}}

    kit.verify(session, scenario, scope_cfg, "ADJ1", pv, result, cfg, context)

    checks = dict((c[0], c[1]) for c in result.checks)
    assert checks["source book untouched by the transfer"] is True


def test_verify_stops_early_when_header_never_reaches_processed():
    session = _verify_session(run_status="Failed")
    scope_cfg = _scope_cfg()
    scenario, result = _base_scenario_result(adj_type="Scale")
    pv = {"rows": 1, "nonzero": 1, "current": 1.0, "delta": 1.0,
          "projected": 2.0, "error": ""}
    cfg = _cfg()

    kit.verify(session, scenario, scope_cfg, "ADJ1", pv, result, cfg)

    assert len(result.checks) == 1
    assert result.checks[0][0] == "header reached Processed"
    assert result.checks[0][1] is False


# ══════════════════════════════════════════════════════════════════════════
# 10. close_enough
# ══════════════════════════════════════════════════════════════════════════

def test_close_enough_at_the_floor_boundary():
    assert kit.close_enough(1e-6, 0.0) is True
    assert kit.close_enough(1.0000009e-6, 0.0) is False


def test_close_enough_within_the_relative_bound_for_large_numbers():
    assert kit.close_enough(1_000_000.0, 1_000_000.5) is True


def test_close_enough_outside_the_relative_bound_for_large_numbers():
    assert kit.close_enough(1_000_000.0, 1_000_003.0) is False


def test_close_enough_both_zero():
    assert kit.close_enough(0.0, 0.0) is True


def test_close_enough_one_value_none():
    assert kit.close_enough(None, 0.0) is True
    assert kit.close_enough(None, 5.0) is False


# ══════════════════════════════════════════════════════════════════════════
# 11. cleanup
# ══════════════════════════════════════════════════════════════════════════

def test_cleanup_issues_begin_header_update_history_insert_commit_in_order():
    session = FakeSession()
    session.when("FROM " + kit.APP + ".ADJ_HEADER",
                 [{"RUN_STATUS": "Processed", "DIMENSION_ADJ_ID": None}])
    scope_cfg = _scope_cfg()
    cfg = _cfg()

    ok = kit.cleanup(session, "ADJ1", scope_cfg, cfg)

    assert ok is True
    calls = session.calls
    i_begin = calls.index("BEGIN")
    i_header = next(i for i, c in enumerate(calls)
                     if "UPDATE" in c and "ADJ_HEADER" in c)
    i_history = next(i for i, c in enumerate(calls)
                      if "INSERT INTO" in c and "ADJ_STATUS_HISTORY" in c)
    i_commit = calls.index("COMMIT")
    assert i_begin < i_header < i_history < i_commit


def test_cleanup_rolls_back_and_returns_false_on_exception_mid_transaction():
    session = FakeSession()
    session.when("FROM " + kit.APP + ".ADJ_HEADER",
                 [{"RUN_STATUS": "Processed", "DIMENSION_ADJ_ID": None}])
    session.raise_on("ADJ_STATUS_HISTORY", RuntimeError("boom"))
    scope_cfg = _scope_cfg()
    cfg = _cfg()

    ok = kit.cleanup(session, "ADJ1", scope_cfg, cfg)

    assert ok is False
    assert "ROLLBACK" in session.calls
    assert session.calls.index("ROLLBACK") > session.calls.index("BEGIN")


def test_cleanup_returns_true_early_when_already_deleted():
    session = FakeSession()
    session.when("FROM " + kit.APP + ".ADJ_HEADER",
                 [{"RUN_STATUS": "Deleted", "DIMENSION_ADJ_ID": 1}])
    scope_cfg = _scope_cfg()
    cfg = _cfg()

    ok = kit.cleanup(session, "ADJ1", scope_cfg, cfg)

    assert ok is True
    assert "BEGIN" not in session.calls
    assert "COMMIT" not in session.calls
    assert len(session.calls) == 1  # just the header_status lookup


# ══════════════════════════════════════════════════════════════════════════
# 12. check_leaks
# ══════════════════════════════════════════════════════════════════════════

def test_check_leaks_empty_when_every_count_is_zero():
    session = FakeSession(default_rows=[{"N": 0}])
    scopes = {"VaR": _scope_cfg()}
    cfg = _cfg()

    assert kit.check_leaks(session, cfg, scopes=scopes) == []


def test_check_leaks_returns_the_labelled_nonzero_counts():
    session = FakeSession(default_rows=[{"N": 0}])
    session.when("FROM " + kit.APP + ".ADJ_HEADER", [{"N": 3}])
    scopes = {"VaR": _scope_cfg()}
    cfg = _cfg()

    leaks = kit.check_leaks(session, cfg, scopes=scopes)

    assert ("ADJ_HEADER (not deleted)", 3) in leaks
    assert all(n != 0 for _, n in leaks)


def test_check_leaks_skips_a_query_that_raises_instead_of_failing():
    session = FakeSession(default_rows=[{"N": 0}])
    session.raise_on("DIMENSION.ADJUSTMENT", RuntimeError("table gone"))
    session.when("FROM " + kit.APP + ".ADJ_HEADER", [{"N": 7}])
    scopes = {"VaR": _scope_cfg()}
    cfg = _cfg()

    leaks = kit.check_leaks(session, cfg, scopes=scopes)  # must not raise

    labels = [label for label, _ in leaks]
    assert "DIMENSION.ADJUSTMENT (not deleted)" not in labels
    assert ("ADJ_HEADER (not deleted)", 7) in leaks


# ══════════════════════════════════════════════════════════════════════════
# 13. check_serialisation
# ══════════════════════════════════════════════════════════════════════════

def test_check_serialisation_two_disjoint_windows_pass():
    session = FakeSession()
    session.when("ADJ_STATUS_HISTORY",
                 [{"ADJ_ID": "A", "STARTED": 1, "ENDED": 2},
                  {"ADJ_ID": "B", "STARTED": 3, "ENDED": 4}])

    out = kit.check_serialisation(session, ["A", "B"])

    assert out["checked"] is True
    assert out["ok"] is True


def test_check_serialisation_overlapping_windows_fail_with_both_ids():
    session = FakeSession()
    session.when("ADJ_STATUS_HISTORY",
                 [{"ADJ_ID": "A", "STARTED": 1, "ENDED": 5},
                  {"ADJ_ID": "B", "STARTED": 3, "ENDED": 6}])

    out = kit.check_serialisation(session, ["A", "B"])

    assert out["checked"] is True
    assert out["ok"] is False
    assert "A" in out["detail"] and "B" in out["detail"]


def test_check_serialisation_fewer_than_two_completed_windows():
    session = FakeSession()
    session.when("ADJ_STATUS_HISTORY",
                 [{"ADJ_ID": "A", "STARTED": 1, "ENDED": None},
                  {"ADJ_ID": "B", "STARTED": None, "ENDED": None}])

    out = kit.check_serialisation(session, ["A", "B"])

    assert out["checked"] is False


def test_check_serialisation_needs_two_ids_and_never_queries_with_fewer():
    session = FakeSession()
    out = kit.check_serialisation(session, ["A"])
    assert out["checked"] is False
    assert session.calls == []


# ══════════════════════════════════════════════════════════════════════════
# 14. check_pruning
# ══════════════════════════════════════════════════════════════════════════

def test_check_pruning_unavailable_history_never_raises():
    session = FakeSession()
    session.raise_on("QUERY_HISTORY", RuntimeError("no privilege"))
    scope_cfg = _scope_cfg()
    cfg = _cfg()
    scenario, result = _base_scenario_result()
    result.adj_id = "ADJ1"

    out = kit.check_pruning(session, scope_cfg, cfg, result)  # must not raise

    assert out["checked"] is False


def test_check_pruning_flags_a_scan_ratio_above_the_limit():
    session = FakeSession()
    session.when("QUERY_HISTORY",
                 [{"QUERY_ID": "q1", "PARTITIONS_SCANNED": 90,
                   "PARTITIONS_TOTAL": 100, "SNIPPET": "SELECT ..."}])
    scope_cfg = _scope_cfg()
    cfg = _cfg(max_scan_ratio=0.80)
    scenario, result = _base_scenario_result()
    result.adj_id = "ADJ1"

    out = kit.check_pruning(session, scope_cfg, cfg, result)

    assert out["checked"] is True
    assert out["ok"] is False
    assert out["worst"] == 0.9


def test_check_pruning_passes_a_scan_ratio_below_the_limit():
    session = FakeSession()
    session.when("QUERY_HISTORY",
                 [{"QUERY_ID": "q1", "PARTITIONS_SCANNED": 5,
                   "PARTITIONS_TOTAL": 100, "SNIPPET": "SELECT ..."}])
    scope_cfg = _scope_cfg()
    cfg = _cfg(max_scan_ratio=0.80)
    scenario, result = _base_scenario_result()
    result.adj_id = "ADJ1"

    out = kit.check_pruning(session, scope_cfg, cfg, result)

    assert out["checked"] is True
    assert out["ok"] is True
    assert out["worst"] == 0.05


# ══════════════════════════════════════════════════════════════════════════
# 15. render_report
# ══════════════════════════════════════════════════════════════════════════

def _passed_result(sid="SCA-VAR"):
    scenario = kit.Scenario(sid, "VaR", "Scale", {})
    result = kit.ScenarioResult(scenario=scenario)
    result.check("ok", True)
    result.passed = True
    result.preview = {"rows": 5}
    return result


def _failed_result(sid="FLA-STRESS"):
    scenario = kit.Scenario(sid, "Stress", "Flatten", {})
    result = kit.ScenarioResult(scenario=scenario)
    result.check("money check", False, "written=1 expected=2")
    result.passed = False
    result.phase = "verify"
    result.adj_id = "ADJ99"
    return result


def test_render_report_has_pass_count_and_a_row_per_scenario():
    results = [_passed_result(), _failed_result()]
    report = kit.render_report(results, cfg=_cfg())

    assert "1 of 2 scenarios passed." in report
    assert "SCA-VAR" in report
    assert "FLA-STRESS" in report


def test_render_report_has_failures_section_only_when_something_failed():
    all_pass_report = kit.render_report([_passed_result()], cfg=_cfg())
    assert "## Failures" not in all_pass_report

    mixed_report = kit.render_report([_passed_result(), _failed_result()],
                                      cfg=_cfg())
    assert "## Failures" in mixed_report
    assert "money check" in mixed_report


# ══════════════════════════════════════════════════════════════════════════
# 16 & 17. run_scenario
# ══════════════════════════════════════════════════════════════════════════

def _submit_row(status, message="", adj_id=""):
    body = {"status": status, "message": message}
    if adj_id:
        body["adj_id"] = adj_id
    return [{"RESULT": json.dumps(body)}]


def test_run_scenario_negative_stops_after_submit_and_passes_on_expected_message():
    session = FakeSession()
    session.when("SP_SUBMIT_ADJUSTMENT",
                 _submit_row("Error", "Scale factor 1.0 makes no change"))
    scopes = {"VaR": _scope_cfg()}
    cfg = _cfg()
    scenario = kit.Scenario("NEG-01", "VaR", "Scale",
                             {"book_code": cfg.book_code, "scale_factor": 1.0},
                             expect="reject", expect_message="Scale factor 1",
                             seed=False)

    result = kit.run_scenario(session, scenario, cfg, scopes=scopes,
                               sleep=lambda s: pytest.fail("must never sleep"))

    assert result.passed is True
    assert not any("SP_PREVIEW_ADJUSTMENT" in c for c in session.calls)
    assert not any("ADJ_HEADER" in c for c in session.calls)


def test_run_scenario_negative_fails_when_it_is_wrongly_accepted():
    session = FakeSession()
    session.when("SP_SUBMIT_ADJUSTMENT", _submit_row("Pending", adj_id="X1"))
    scopes = {"VaR": _scope_cfg()}
    cfg = _cfg()
    scenario = kit.Scenario("NEG-01", "VaR", "Scale",
                             {"book_code": cfg.book_code, "scale_factor": 1.0},
                             expect="reject", expect_message="Scale factor 1",
                             seed=False)

    result = kit.run_scenario(session, scenario, cfg, scopes=scopes,
                               sleep=lambda s: pytest.fail("must never sleep"))

    assert result.passed is False


def test_run_scenario_zero_row_preview_refuses_to_submit():
    session = FakeSession()
    session.when("SP_PREVIEW_ADJUSTMENT",
                 [{"ROWS_AFFECTED": 0, "NONZERO_ROWS": 0,
                   "TOTAL_CURRENT_VALUE": 0, "TOTAL_ADJUSTMENT_DELTA": 0,
                   "TOTAL_PROJECTED_VALUE": 0, "ERROR": ""}])
    scopes = {"VaR": _scope_cfg()}
    cfg = _cfg(seed_source_cob=0)  # skip seeding entirely
    scenario = kit.Scenario("SCA-VAR", "VaR", "Scale",
                             {"book_code": cfg.book_code, "scale_factor": 1.05})

    result = kit.run_scenario(session, scenario, cfg, scopes=scopes,
                               sleep=lambda s: pytest.fail("must never sleep"))

    assert result.message == "preview returned zero rows"
    assert not any("SP_SUBMIT_ADJUSTMENT" in c for c in session.calls)
    names = [c[0] for c in result.failed_checks]
    assert "preview affects at least one row" in names
