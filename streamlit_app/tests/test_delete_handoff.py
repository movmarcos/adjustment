"""Deleting an adjustment must tell the reporting side, like processing does.

Reported 2026-09-24. Processing an adjustment queues a PowerBI refresh for
VaR/Stress, or writes a DUMMY_* trigger row in RAVEN.LOG_STAGE_ME_STATUS for
Sensitivity/FRTB that Control-M polls to start dbt. Deleting one did
neither: the rows left the warehouse immediately and nothing downstream was
told, so reports kept showing the deleted numbers until some unrelated
adjustment happened to trigger a rebuild.

SP_DOWNSTREAM_HANDOFF (15b) now does that hand-off, and the Adjustments page
calls it after a delete commits.

Most of these EXECUTE the procedure body against a fake session and assert on
the SQL it issues, rather than pattern-matching source.
"""
import os
import re

import pytest

HANDOFF_SQL = os.path.join(os.path.dirname(__file__), "..", "..",
                           "new_adjustment_db_objects",
                           "15b_sp_downstream_handoff.sql")
ENGINE_SQL = os.path.join(os.path.dirname(__file__), "..", "..",
                          "new_adjustment_db_objects",
                          "05_sp_process_adjustment.sql")
PAGE = os.path.join(os.path.dirname(__file__), "..", "pages",
                    "2_Adjustments.py")


def _source(path):
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def _body(path):
    return re.findall(r"\$\$(.*?)\$\$", _source(path), re.S)[0]


def _maps(path):
    """The three scope dictionaries out of a procedure body."""
    ns = {}
    body = _body(path)
    for name in ("PBI_INSERT_SOURCE", "PBI_DATA_GROUP", "DBT_DUMMY_DATASET"):
        block = body[body.index(name + " = {"):]
        block = block[:block.index("}") + 1]
        exec(block, ns)
    return {k: ns[k] for k in
            ("PBI_INSERT_SOURCE", "PBI_DATA_GROUP", "DBT_DUMMY_DATASET")}


def _run(process_type, cobid=20260420, pbi_returns="success"):
    """Execute the procedure and return (result, [sql issued])."""
    ns = {}
    exec(_body(HANDOFF_SQL), ns)
    issued = []

    class _Res:
        def __init__(self, rows):
            self._rows = rows

        def collect(self):
            return self._rows

    class _Session:
        def sql(self, q):
            issued.append(" ".join(q.split()))
            if "SEQ_RUN_LOG.NEXTVAL" in q:
                return _Res([{"X": 4242}])
            if "UPDATE_POWERBI_FOR_ADJUSTMENTS" in q:
                return _Res([[pbi_returns]])
            return _Res([])

    out = ns["main"](_Session(), process_type, cobid, "deleted adjustment abc")
    return out, issued


# ── The two maps must not drift from the engine's ───────────────────────────

@pytest.mark.parametrize("name", ["PBI_INSERT_SOURCE", "PBI_DATA_GROUP",
                                  "DBT_DUMMY_DATASET"])
def test_the_scope_maps_match_the_engine(name):
    """Two copies exist on purpose; this is what keeps them honest."""
    mine = _maps(HANDOFF_SQL)[name]
    theirs = _maps(ENGINE_SQL)[name]
    assert mine == theirs, (
        name + " differs between SP_DOWNSTREAM_HANDOFF and "
        "SP_PROCESS_ADJUSTMENT. A delete would then hand off to a different "
        "place than the insert did, or to nowhere.\n"
        "  handoff: " + repr(mine) + "\n  engine : " + repr(theirs))


# ── Routing ─────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("scope,source", [("VaR", "LOAD_VAR_ADJUSTMENT"),
                                          ("Stress", "LOAD_STRESS_ADJUSTMENT")])
def test_var_and_stress_queue_a_powerbi_refresh(scope, source):
    out, issued = _run(scope)
    call = [q for q in issued if "UPDATE_POWERBI_FOR_ADJUSTMENTS" in q]
    assert call, "no PowerBI refresh was queued for " + scope
    assert source in call[0]
    assert "powerbi: queued" in out
    assert not [q for q in issued if "LOG_STAGE_ME_STATUS" in q], (
        scope + " also wrote a dbt trigger row; it should take one path only.")


@pytest.mark.parametrize("scope,dataset", [
    ("Sensitivity", "DUMMY_Sensitivity_Adjustment"),
    ("FRTB", "DUMMY_FRTB_Adjustment"),
    ("FRTBDRC", "DUMMY_FRTB_Adjustment"),
    ("FRTBRRAO", "DUMMY_FRTB_Adjustment"),
])
def test_sensitivity_and_frtb_write_the_dummy_row(scope, dataset):
    out, issued = _run(scope)
    ins = [q for q in issued if "INSERT INTO RAVEN.LOG_STAGE_ME_STATUS" in q]
    assert ins, "no dbt trigger row written for " + scope
    assert "'" + dataset + "'" in ins[0]
    assert "dbt: trigger written" in out
    assert not [q for q in issued if "UPDATE_POWERBI_FOR_ADJUSTMENTS" in q]


def test_the_trigger_row_carries_the_deleted_adjustments_cob():
    _, issued = _run("FRTB", cobid=20260101)
    ins = [q for q in issued if "INSERT INTO RAVEN.LOG_STAGE_ME_STATUS" in q][0]
    assert "20260101" in ins, (
        "The trigger row must name the COB whose data changed, or dbt "
        "rebuilds the wrong day.")


def test_an_unknown_scope_is_skipped_not_guessed():
    out, issued = _run("NOSUCHSCOPE")
    assert out.startswith("skipped")
    assert issued == [], "an unknown scope must not open a run log or write"


# ── The run log ─────────────────────────────────────────────────────────────

def test_it_mints_a_fresh_run_log():
    """Reusing the deleted adjustment's run log was the tempting shortcut.

    That run is closed and describes the insert. The PowerBI proc looks its
    run up by the data group in proc_parameters, so the delete needs its own.
    """
    _, issued = _run("VaR")
    assert any("SEQ_RUN_LOG.NEXTVAL" in q for q in issued)
    opened = [q for q in issued if "CALL BATCH.LOAD_RUN_LOG(" in q]
    assert opened, "no run log was opened"
    assert "'VAR'" in opened[0], (
        "The run log's proc_parameters must carry the data group; the "
        "PowerBI proc keys its lookup on it.")


def test_the_run_log_is_opened_before_the_handoff_and_closed_after():
    _, issued = _run("VaR")
    opened = next(i for i, q in enumerate(issued) if "CALL BATCH.LOAD_RUN_LOG(" in q)
    used = next(i for i, q in enumerate(issued) if "UPDATE_POWERBI_FOR_ADJUSTMENTS" in q)
    closed = next(i for i, q in enumerate(issued) if "LOAD_RUN_LOG_END_WITH_DETAIL" in q)
    assert opened < used < closed


# ── Failure behaviour ───────────────────────────────────────────────────────

def test_a_powerbi_string_error_is_treated_as_a_failure():
    """The proc reports problems by RETURNING text, not by raising."""
    out, _ = _run("VaR", pbi_returns="Error : no Run Log IDs supplied")
    assert out.startswith("failed"), (
        "A non-success return from the PowerBI proc must be reported as a "
        "failure; it reports problems as a string, not an exception.")


def test_the_run_log_is_still_closed_after_a_failure():
    _, issued = _run("VaR", pbi_returns="Error : something")
    assert any("LOAD_RUN_LOG_END_WITH_DETAIL" in q for q in issued), (
        "A failed hand-off must not leave its run log open forever.")


# ── How the page calls it ───────────────────────────────────────────────────

def _delete_block():
    src = _source(PAGE)
    start = src.index("def _do_delete()")
    return src[start:src.index("def ", start + 10)]


def test_the_delete_calls_the_handoff():
    assert "SP_DOWNSTREAM_HANDOFF" in _delete_block(), (
        "Deleting an adjustment still tells nothing downstream.")


def test_it_runs_after_the_commit_not_inside_the_transaction():
    """The delete is durable by then; a hand-off failure must not undo it."""
    block = _delete_block()
    commit = block.index('run_query("COMMIT")')
    handoff = block.index("SP_DOWNSTREAM_HANDOFF")
    assert commit < handoff, (
        "The hand-off is inside the delete transaction, so a reporting "
        "problem would roll back a delete that should have stood.")


def test_it_only_fires_for_an_adjustment_that_reached_the_fact_tables():
    block = _delete_block()
    guard = block.index("if dim_adj_id is not None and process_type:")
    handoff = block.index("SP_DOWNSTREAM_HANDOFF")
    assert guard < handoff, (
        "Deleting a Pending ticket published nothing, so it must not trigger "
        "a report rebuild.")


def test_a_failed_handoff_warns_rather_than_claiming_success():
    block = _delete_block()
    assert "could NOT be queued" in block, (
        "A silent hand-off failure means stale reports and no clue why.")
    assert 'set_flash(_FLASH, "warning"' in block


# ── The run-log call must match the engine's, field for field ───────────────
# Reported 2026-09-24: a deleted VaR adjustment produced no pending Power BI
# refresh. FACT.UPDATE_POWERBI_FOR_ADJUSTMENTS is outside this repo and
# cannot be inspected; the only thing known about how it finds a run log is
# that the engine's exact call works. An earlier revision of the hand-off
# used its own process name and passed a reason string, for tidiness. That
# deviation bought nothing and could stop the proc finding the run.

def _run_log_args(path):
    """The arguments of the BATCH.LOAD_RUN_LOG call in a procedure body."""
    body = _body(path)
    i = body.index("CALL BATCH.LOAD_RUN_LOG(")
    block = body[i:body.index(")", body.index("'false'", i))]
    return [a.strip() for a in
            block[block.index("(") + 1:].replace("\n", " ").split(",")]


def test_the_run_log_uses_the_same_process_name_as_the_engine():
    mine = _run_log_args(HANDOFF_SQL)
    theirs = _run_log_args(ENGINE_SQL)
    assert "'FACT.SP_PROCESS_ADJUSTMENT'" in mine, (
        "The hand-off opens its run log under its own process name. The "
        "Power BI procedure may key on it, and the engine's value is the "
        "only one known to work.\n  mine:   " + repr(mine) +
        "\n  engine: " + repr(theirs))
    assert theirs[2] == mine[2], (
        "Process name drifted from the engine's: " + repr(mine[2]) +
        " vs " + repr(theirs[2]))


def test_the_run_log_trailing_argument_matches_the_engine():
    mine = _run_log_args(HANDOFF_SQL)
    assert mine[-1] == "''", (
        "The engine passes an empty trailing argument. Passing anything else "
        "is an untested deviation into a procedure we cannot read.")


def test_the_reason_is_reported_without_going_into_the_run_log():
    """The context is still useful — it just belongs in the return value."""
    out, issued = _run("VaR")
    assert "deleted adjustment" in out, (
        "The caller's reason should come back in the status string so the "
        "page can show what the hand-off was for.")
    log_call = [q for q in issued if "LOAD_RUN_LOG(" in q][0]
    assert "deleted adjustment" not in log_call
