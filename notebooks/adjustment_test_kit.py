"""Adjustment test harness — all the logic the notebook calls.

Lives on the notebook's stage so the notebook can `import adjustment_test_kit`.
Every function takes an explicit `session`, so this module unit-tests locally
against a fake session (see notebooks/tests/test_adjustment_test_kit.py) and
imports nothing from Streamlit or Snowpark.

Design: docs/superpowers/specs/2026-09-23-adjustment-test-harness-design.md

The seven phases of a scenario are seed, preview, submit, approve, wait,
verify, cleanup. `run_scenario` executes them in order and returns a
ScenarioResult; `run_suite` runs many and returns the list.
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field

__all__ = [
    "ScopeConfig", "HarnessConfig", "Scenario", "ScenarioResult",
    "load_scopes", "build_matrix", "negative_matrix", "build_payload",
    "preflight", "seed", "preview", "submit", "approve", "wait_for_terminal",
    "verify", "cleanup", "run_scenario", "run_suite",
    "check_pruning", "check_leaks", "check_serialisation", "render_report",
]

# ── Constants ────────────────────────────────────────────────────────────────

APP = "ADJUSTMENT_APP"
TEST_COB = 20991231
BOT_SUBMITTER = "UAT_BOT_SUBMITTER"
BOT_APPROVER = "UAT_BOT_APPROVER"

#: RUN_STATUS values that mean the pipeline is finished with a header.
TERMINAL_STATUSES = frozenset({
    "Processed", "Failed", "Superseded", "Replaced", "Deleted",
    "Rejected - SignedOff", "Rejected",
})

#: Statuses the harness treats as a healthy end state.
SUCCESS_STATUS = "Processed"

#: Scaling types. `Direct`, `Upload` and `EROL` are their own categories.
SCALING_TYPES = ("Flatten", "Scale", "Roll", "Transfer")
ALL_TYPES = SCALING_TYPES + ("EROL", "Direct", "Upload")

#: Scopes whose Direct flow is per-row (ADJ_DIRECT_STAGE + SP_SUBMIT_DIRECT_BATCH).
#: The FRTB scopes use the per-file flow instead (ADJ_LINE_ITEM_JSON).
DIRECT_ROW_SCOPES = ("VaR", "Stress", "Sensitivity")
DIRECT_FILE_SCOPES = ("FRTB", "FRTBDRC", "FRTBRRAO")

#: Mirror of streamlit/adjustment_engine/utils/scope_filters.py. A unit test asserts the two
#: stay in step, so a filter added to the app cannot silently go untested here.
_COMMON_FILTERS = frozenset({
    "entity_code", "source_system_code", "department_code", "book_code",
    "trader_code", "guaranteed_entity", "region_key",
    "trade_code", "strategy", "trade_typology", "instrument_code",
})
_VAR_ONLY = frozenset({"var_component_name", "var_sub_component_name", "day_type"})

SCOPE_FILTER_FIELDS = {
    "VaR": _COMMON_FILTERS | {"currency_code", "scenario_date_id"} | _VAR_ONLY,
    "Stress": _COMMON_FILTERS | {"simulation_name", "simulation_source",
                                 "product_category_attributes"},
    "Sensitivity": _COMMON_FILTERS | {"currency_code", "measure_type_code",
                                      "tenor_code", "underlying_tenor_code",
                                      "curve_code", "product_category_attributes"},
    "FRTB": _COMMON_FILTERS | {"currency_code", "measure_type_code",
                               "simulation_name", "tenor_code", "curve_code",
                               "product_category_attributes"},
    "FRTBDRC": _COMMON_FILTERS | {"currency_code", "measure_type_code",
                                  "simulation_name", "product_category_attributes"},
    "FRTBRRAO": _COMMON_FILTERS | {"currency_code", "measure_type_code",
                                   "simulation_name", "product_category_attributes"},
}


# ── SQL plumbing ─────────────────────────────────────────────────────────────

def sql_escape(value) -> str:
    """Escape a value for a single-quoted SQL literal.

    Backslashes are doubled FIRST, then quotes — the reverse order would
    re-escape the backslashes this function just inserted. Matches
    streamlit/adjustment_engine/utils/snowflake_conn.sql_escape exactly.
    """
    if value is None:
        return ""
    return str(value).replace("\\", "\\\\").replace("'", "''")


def _to_dict(row):
    """Snowpark Row, dict, or anything dict-like -> plain dict."""
    if isinstance(row, dict):
        return row
    for attr in ("as_dict", "asDict"):
        fn = getattr(row, attr, None)
        if callable(fn):
            try:
                return fn()
            except Exception:
                pass
    try:
        return dict(row)
    except Exception:
        return {}


def rows(session, query):
    """Run a query and return a list of plain dicts."""
    return [_to_dict(r) for r in session.sql(query).collect()]


def one(session, query):
    """Run a query and return the first row as a dict, or None."""
    got = rows(session, query)
    return got[0] if got else None


def call_sp(session, name, *args):
    """CALL a procedure with string arguments; return the parsed single value.

    Procedures here return a VARIANT (a JSON string once it reaches Python) or
    a plain VARCHAR. Both are handled: a JSON object comes back as a dict, a
    bare string stays a string.
    """
    literals = ", ".join("'" + sql_escape(a) + "'" for a in args)
    got = rows(session, "CALL " + name + "(" + literals + ")")
    if not got:
        return None
    value = list(got[0].values())[0]
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return value


def _num(value, default=0.0):
    """Best-effort float, treating None and unparseable values as `default`."""
    if value is None:
        return default
    try:
        return float(value)
    except Exception:
        return default


def close_enough(a, b, rel=1e-6, floor=1e-6) -> bool:
    """Compare two measures at the engine's own scale-10 precision."""
    a, b = _num(a), _num(b)
    return abs(a - b) <= max(floor, rel * max(abs(a), abs(b)))


# ── Configuration ────────────────────────────────────────────────────────────

@dataclass
class ScopeConfig:
    """One row of ADJUSTMENTS_SETTINGS."""
    process_type: str
    fact_table: str = ""
    fact_adjusted_table: str = ""
    adjustments_table: str = ""
    adjustments_summary_table: str = ""
    fact_table_pk: str = ""
    metric_name: str = ""
    metric_usd_name: str = ""
    is_active: bool = True

    @property
    def metric(self) -> str:
        """The USD measure column, which is what the preview totals."""
        return self.metric_usd_name or self.metric_name


def load_scopes(session):
    """Read every scope's configuration. Nothing scope-specific is hardcoded."""
    out = {}
    for r in rows(session, """
        SELECT PROCESS_TYPE, FACT_TABLE, FACT_ADJUSTED_TABLE, ADJUSTMENTS_TABLE,
               ADJUSTMENTS_SUMMARY_TABLE, FACT_TABLE_PK, METRIC_NAME,
               METRIC_USD_NAME, IS_ACTIVE
        FROM """ + APP + """.ADJUSTMENTS_SETTINGS
        ORDER BY PROCESS_TYPE
    """):
        pt = str(r.get("PROCESS_TYPE") or "")
        if not pt:
            continue
        out[pt] = ScopeConfig(
            process_type=pt,
            fact_table=str(r.get("FACT_TABLE") or ""),
            fact_adjusted_table=str(r.get("FACT_ADJUSTED_TABLE") or ""),
            adjustments_table=str(r.get("ADJUSTMENTS_TABLE") or ""),
            adjustments_summary_table=str(r.get("ADJUSTMENTS_SUMMARY_TABLE") or ""),
            fact_table_pk=str(r.get("FACT_TABLE_PK") or ""),
            metric_name=str(r.get("METRIC_NAME") or ""),
            metric_usd_name=str(r.get("METRIC_USD_NAME") or ""),
            is_active=bool(r.get("IS_ACTIVE", True)),
        )
    return out


@dataclass
class HarnessConfig:
    """Everything the notebook's config cell sets."""

    # Where the test writes.
    cob: int = TEST_COB
    allow_real_cob: bool = False

    # Seeding: rows are cloned from this COB into `cob`, changing only the COB.
    seed_source_cob: int = 0
    seed_rows: int = 2000
    book_code: str = "UATBOOK"
    target_book_code: str = "UATBOOK2"
    entity_code: str = ""          # resolved from book_code when blank

    # Identities. Self-approval is refused, so these must differ.
    submitter: str = BOT_SUBMITTER
    approver: str = BOT_APPROVER

    # Pipeline.
    force: bool = False            # SP_FORCE_PROCESS instead of waiting on the task
    timeout_s: int = 900
    poll_s: int = 10

    # Behaviour.
    keep_failures: bool = True     # leave a failed scenario's rows for inspection
    max_scan_ratio: float = 0.80   # pruning guard threshold
    reason: str = "Adjustment test harness"
    category: str = "Booking Error"

    def validate(self):
        """Raise on a configuration that could touch business data by accident."""
        if int(self.cob) != TEST_COB and not self.allow_real_cob:
            raise ValueError(
                "cob is set to " + str(self.cob) + ", which is not the reserved "
                "test COB " + str(TEST_COB) + ". Set allow_real_cob=True to "
                "confirm you mean to write to real data.")
        if self.submitter.strip().lower() == self.approver.strip().lower():
            raise ValueError("submitter and approver must differ — the workflow "
                             "refuses self-approval.")
        if int(self.seed_rows) <= 0:
            raise ValueError("seed_rows must be positive.")
        return True

    @property
    def is_test_cob(self) -> bool:
        return int(self.cob) == TEST_COB


# ── Scenarios ────────────────────────────────────────────────────────────────

@dataclass
class Scenario:
    id: str
    scope: str
    adjustment_type: str
    params: dict = field(default_factory=dict)
    expect: str = "processed"      # "processed" | "reject"
    expect_message: str = ""       # substring, negative scenarios only
    keep: bool = False
    seed: bool = True
    note: str = ""


@dataclass
class ScenarioResult:
    scenario: Scenario
    passed: bool = False
    phase: str = ""                # phase reached
    adj_id: str = ""
    status: str = ""
    message: str = ""
    preview: dict = field(default_factory=dict)
    checks: list = field(default_factory=list)   # (name, ok, detail)
    timings: dict = field(default_factory=dict)  # phase -> seconds
    pruning: dict = field(default_factory=dict)
    cleaned: bool = False

    def check(self, name, ok, detail=""):
        self.checks.append((name, bool(ok), str(detail)))
        return bool(ok)

    @property
    def failed_checks(self):
        return [c for c in self.checks if not c[1]]

    @property
    def elapsed(self) -> float:
        return round(sum(self.timings.values()), 1)


def build_matrix(scopes, mode="smoke", cfg=None, types=None):
    """Generate the legal scenarios for a run mode.

    `scopes` is the dict from load_scopes, or a list of PROCESS_TYPE strings.
    Only active scopes are used. The legality rules encoded here come from
    SP_SUBMIT_ADJUSTMENT's own validation.
    """
    cfg = cfg or HarnessConfig()
    names = _active_scope_names(scopes)
    mode = str(mode).lower()

    if mode == "negative":
        return negative_matrix(names, cfg)

    if mode == "smoke":
        wanted = ("Scale",)
    elif mode == "single":
        wanted = tuple(types or ("Scale",))
        names = names[:1]
    else:                                   # "full"
        wanted = tuple(types or ALL_TYPES)

    out = []
    for scope in names:
        for adj_type in wanted:
            scen = _make_scenario(scope, adj_type, cfg)
            if scen is not None:
                out.append(scen)
    return out


def _active_scope_names(scopes):
    if isinstance(scopes, dict):
        return [k for k, v in scopes.items()
                if getattr(v, "is_active", True)]
    return list(scopes or [])


def _make_scenario(scope, adj_type, cfg):
    """One legal scenario, or None when the combination does not exist."""
    adj_type = str(adj_type)
    sid = _scenario_id(scope, adj_type)
    prior_cob = _prior_cob(cfg)

    if adj_type == "Flatten":
        # source_cobid must equal cobid; the engine forces the factor to -1.
        return Scenario(sid, scope, "Flatten", {
            "book_code": cfg.book_code,
        }, note="zero out the seeded book")

    if adj_type == "Scale":
        # A factor of exactly 1.0 is rejected, so never generate one.
        return Scenario(sid, scope, "Scale", {
            "book_code": cfg.book_code,
            "scale_factor": 1.05,
        }, note="+5% on the seeded book")

    if adj_type == "Roll":
        # Cross-COB by definition: roll the prior COB's adjusted state forward.
        return Scenario(sid, scope, "Roll", {
            "book_code": cfg.book_code,
            "source_cobid": prior_cob,
            "scale_factor": 1.0,
        }, note="roll " + str(prior_cob) + " forward")

    if adj_type == "Transfer":
        # Both books required, must differ; every other filter is stripped.
        return Scenario(sid, scope, "Transfer", {
            "source_book_code": cfg.book_code,
            "book_code": cfg.target_book_code,
            "scale_factor": 1.0,
        }, note="copy source book into target book")

    if adj_type == "EROL":
        # Approval is forced on; target and source COB must differ.
        return Scenario(sid, scope, "EROL", {
            "entity_code": cfg.entity_code,
            "source_cobid": prior_cob,
        }, note="entity roll from " + str(prior_cob))

    if adj_type == "Direct":
        return Scenario(sid, scope, "Direct", {
            "book_code": cfg.book_code,
        }, note="per-row values" if scope in DIRECT_ROW_SCOPES else "file values")

    if adj_type == "Upload":
        # The legacy file feed is VaR only.
        if scope != "VaR":
            return None
        return Scenario(sid, scope, "Upload", {
            "book_code": cfg.book_code,
        }, note="VaR file upload")

    return None


def _scenario_id(scope, adj_type):
    return (adj_type[:3].upper() + "-" + scope.upper())


def _prior_cob(cfg):
    """A source COB for Roll and Entity Roll.

    Seeding clones into `cfg.cob` only, so a cross-COB scenario needs a second
    populated COB. `seed_source_cob` is that COB when it is set; otherwise the
    day before the test COB, which exists only if the caller seeded it.
    """
    if cfg.seed_source_cob:
        return int(cfg.seed_source_cob)
    return int(cfg.cob) - 1


def negative_matrix(scopes, cfg=None):
    """The submit procedure's hard rejections, one scenario each.

    Each expects status Error and a message containing `expect_message`. These
    never reach the pipeline, so they are fast and safe to run anywhere.
    """
    cfg = cfg or HarnessConfig()
    names = _active_scope_names(scopes)
    scope = names[0] if names else "VaR"

    return [
        Scenario("NEG-01", scope, "Scale",
                 {"book_code": cfg.book_code, "scale_factor": 1.0},
                 expect="reject", expect_message="Scale factor 1",
                 seed=False, note="factor of 1 changes nothing"),
        Scenario("NEG-02", scope, "Scale",
                 {"book_code": cfg.book_code, "scale_factor": 1.1,
                  "source_cobid": int(cfg.cob) - 1},
                 expect="reject", expect_message="must equal cobid",
                 seed=False, note="Scale is single-COB"),
        Scenario("NEG-03", scope, "Flatten",
                 {"book_code": cfg.book_code, "source_cobid": int(cfg.cob) - 1},
                 expect="reject", expect_message="must equal cobid",
                 seed=False, note="Flatten is single-COB"),
        Scenario("NEG-04", scope, "Transfer",
                 {"source_book_code": "", "book_code": cfg.target_book_code},
                 expect="reject", expect_message="both a source book",
                 seed=False, note="transfer needs two books"),
        Scenario("NEG-05", scope, "Transfer",
                 {"source_book_code": cfg.book_code,
                  "book_code": cfg.book_code},
                 expect="reject", expect_message="must differ",
                 seed=False, note="source and target the same"),
        Scenario("NEG-06", scope, "Transfer",
                 {"source_book_code": cfg.book_code,
                  "book_code": "ZZ-NO-SUCH-BOOK-ZZ"},
                 expect="reject", expect_message="book",
                 seed=False, note="unknown target book"),
        Scenario("NEG-07", scope, "NotAType",
                 {"book_code": cfg.book_code},
                 expect="reject", expect_message="",
                 seed=False, note="unknown adjustment type"),
        Scenario("NEG-08", "ZZNOSUCHSCOPE", "Scale",
                 {"book_code": cfg.book_code, "scale_factor": 1.1},
                 expect="reject", expect_message="",
                 seed=False, note="inactive or unknown scope"),
    ]


def build_payload(scenario, cfg):
    """The JSON payload for SP_SUBMIT_ADJUSTMENT / SP_PREVIEW_ADJUSTMENT."""
    payload = {
        "cobid": int(cfg.cob),
        "process_type": scenario.scope,
        "adjustment_type": scenario.adjustment_type,
        "username": cfg.submitter,
        "source_cobid": int(cfg.cob),
        "scale_factor": 1.0,
        "reason": cfg.reason + " — " + scenario.id,
        "adjustment_category": cfg.category,
        "adjustment_occurrence": "ADHOC",
        "requires_approval": False,
    }
    payload.update(scenario.params or {})

    # Entity defaults to the harness entity for every type that filters on one.
    if not payload.get("entity_code") and cfg.entity_code:
        if scenario.adjustment_type != "Transfer":
            payload["entity_code"] = cfg.entity_code

    # Entity Roll's approval flag is forced server-side; set it here too so the
    # harness's own expectations match what comes back.
    if scenario.adjustment_type == "EROL":
        payload["requires_approval"] = True
        payload.pop("book_code", None)
        payload.pop("scale_factor", None)

    return payload


# ── Pre-flight ───────────────────────────────────────────────────────────────

def preflight(session, cfg, scopes=None, scope_names=None):
    """Check the environment before anything is submitted.

    Returns a list of problem strings. Empty means good to go. A suspended
    pipeline task is the important one: without this check a run just waits
    for the timeout with no explanation.
    """
    problems = []
    cfg.validate()
    scopes = scopes if scopes is not None else load_scopes(session)
    wanted = [s for s in (scope_names or list(scopes))]

    # 1. Scopes exist and are active.
    for name in wanted:
        sc = scopes.get(name)
        if sc is None:
            problems.append("Scope " + name + " is not in ADJUSTMENTS_SETTINGS.")
        elif not sc.is_active:
            problems.append("Scope " + name + " is not active; submits are rejected.")
        elif not sc.adjustments_table:
            problems.append("Scope " + name + " has no ADJUSTMENTS_TABLE configured.")

    # 2. Pipeline tasks are started. Skipped when forcing, which bypasses them.
    if not cfg.force:
        try:
            task_rows = rows(session, "SHOW TASKS IN SCHEMA " + APP)
            states = {}
            for r in task_rows:
                nm = str(r.get("name") or r.get("NAME") or "")
                st = str(r.get("state") or r.get("STATE") or "").lower()
                if nm.upper().startswith("TASK_PROCESS_"):
                    states[nm.upper()] = st
            if not states:
                problems.append(
                    "No TASK_PROCESS_* tasks found. Nothing will pick a "
                    "submission up — deploy the tasks or set force=True.")
            for nm, st in sorted(states.items()):
                if st != "started":
                    problems.append(
                        "Task " + nm + " is " + (st or "unknown") + ", not started. "
                        "Resume it or set force=True.")
        except Exception as exc:
            problems.append("Could not list tasks: " + str(exc))

    # 3. Both bot identities are approvers, or approval scenarios stall.
    try:
        got = one(session, """
            SELECT COUNT(*) AS N FROM """ + APP + """.ADJ_APPROVERS
            WHERE IS_ACTIVE = TRUE
              AND UPPER(APPROVER_USERNAME) IN ('""" + sql_escape(cfg.submitter.upper()) + """',
                                               '""" + sql_escape(cfg.approver.upper()) + """')
        """)
        if int((got or {}).get("N") or 0) < 2:
            problems.append(
                "Both bot users must be active approvers. Run "
                "ensure_bots(session, cfg) first.")
    except Exception as exc:
        problems.append("Could not read ADJ_APPROVERS: " + str(exc))

    # 4. The test COB is not signed off, which would make every submit inert.
    try:
        got = one(session, """
            SELECT COUNT(*) AS N FROM """ + APP + """.ADJ_SIGNOFF_STATUS
            WHERE COBID = """ + str(int(cfg.cob)) + """
              AND UPPER(SIGNOFF_STATUS) IN ('SIGNED_OFF','SIGNOFF_REQUESTED',
                                            'REOPEN_REQUESTED')
        """)
        if int((got or {}).get("N") or 0) > 0:
            problems.append(
                "COB " + str(cfg.cob) + " has a sign-off in place. Submissions "
                "will be inserted as 'Rejected - SignedOff'.")
    except Exception:
        pass   # table may not exist in a partial deploy; not fatal

    return problems


def ensure_bots(session, cfg):
    """Make both bot identities active, scope-wide approvers.

    Tagged ADDED_BY = 'UAT_AUTOMATION' so the existing pytest cleanup removes
    them too. Idempotent.
    """
    for user in (cfg.submitter, cfg.approver):
        # ADJ_SUBMITTERS as well as ADJ_APPROVERS: once a real user is added
        # to the submitter list the bootstrap rule stops applying, and every
        # scenario the harness submits would be refused.
        try:
            session.sql("""
                MERGE INTO """ + APP + """.ADJ_SUBMITTERS t
                USING (SELECT '""" + sql_escape(user) + """' AS U) s
                  ON UPPER(t.USERNAME) = UPPER(s.U)
                WHEN MATCHED THEN UPDATE SET IS_ACTIVE = TRUE
                WHEN NOT MATCHED THEN
                  INSERT (USERNAME, PROCESS_TYPE, IS_ACTIVE, ADDED_BY)
                  VALUES (s.U, NULL, TRUE, 'UAT_AUTOMATION')
            """).collect()
        except Exception:
            pass    # table may predate this feature; bootstrap rule applies
        session.sql("""
            MERGE INTO """ + APP + """.ADJ_APPROVERS t
            USING (SELECT '""" + sql_escape(user) + """' AS U) s
              ON UPPER(t.APPROVER_USERNAME) = UPPER(s.U)
            WHEN MATCHED THEN UPDATE SET IS_ACTIVE = TRUE
            WHEN NOT MATCHED THEN
              INSERT (APPROVER_USERNAME, PROCESS_TYPE, IS_ACTIVE, ADDED_BY)
              VALUES (s.U, NULL, TRUE, 'UAT_AUTOMATION')
        """).collect()
    return True


# ── Phase 1: seed ────────────────────────────────────────────────────────────

def resolve_book(session, book_code):
    """Current BOOK_KEY and ENTITY_CODE for a book code.

    Compares BOOK_CODE bare rather than wrapping it in UPPER(), so the scan
    prunes. Values in DIMENSION.BOOK are already canonical.
    """
    got = one(session, """
        SELECT MAX(BOOK_KEY) AS BOOK_KEY, MAX(ENTITY_CODE) AS ENTITY_CODE,
               COUNT(DISTINCT ENTITY_CODE) AS N_ENT
        FROM DIMENSION.BOOK
        WHERE BOOK_CODE = '""" + sql_escape(book_code) + """'
          AND IS_CURRENT_ROW = TRUE
    """)
    if not got or got.get("BOOK_KEY") is None:
        return None
    return {"book_key": got.get("BOOK_KEY"),
            "entity_code": got.get("ENTITY_CODE"),
            "ambiguous": int(got.get("N_ENT") or 0) > 1}


def count_fact_rows(session, scope_cfg, cob, book_key=None):
    """Rows in the base fact table at a COB, optionally for one book."""
    where = "COBID = " + str(int(cob))
    if book_key is not None:
        where += " AND BOOK_KEY = " + str(int(book_key))
    got = one(session, "SELECT COUNT(*) AS N FROM " + scope_cfg.fact_table +
              " WHERE " + where)
    return int((got or {}).get("N") or 0)


def seed(session, scope_cfg, cfg, book_keys=None, cob=None):
    """Clone a pinned slice of real fact rows into the test COB.

    Only the COB changes, so every surrogate key, measure and flag stays
    consistent with real data. Idempotent: returns 0 when the target COB
    already holds rows for those books.

    Refuses to run unless the target is the reserved test COB — seeding is the
    one operation that writes to a fact table, so it never points at real data.
    """
    cob = int(cob if cob is not None else cfg.cob)
    if cob != TEST_COB:
        raise ValueError(
            "seed() refuses to write to COB " + str(cob) + ". Seeding is only "
            "ever allowed into the reserved test COB " + str(TEST_COB) + ".")
    if not cfg.seed_source_cob:
        raise ValueError("seed_source_cob is not set; nothing to clone from.")
    if not scope_cfg.fact_table:
        raise ValueError("Scope " + scope_cfg.process_type + " has no FACT_TABLE.")

    keys = [int(k) for k in (book_keys or []) if k is not None]
    if not keys:
        raise ValueError("seed() needs at least one BOOK_KEY to pin the scan.")
    key_list = ", ".join(str(k) for k in keys)

    existing = one(session, "SELECT COUNT(*) AS N FROM " + scope_cfg.fact_table +
                   " WHERE COBID = " + str(cob) +
                   " AND BOOK_KEY IN (" + key_list + ")")
    if int((existing or {}).get("N") or 0) > 0:
        return 0

    session.sql(
        "INSERT INTO " + scope_cfg.fact_table + " "
        "SELECT * REPLACE (" + str(cob) + " AS COBID) "
        "FROM " + scope_cfg.fact_table + " "
        "WHERE COBID = " + str(int(cfg.seed_source_cob)) + " "
        "  AND BOOK_KEY IN (" + key_list + ") "
        "LIMIT " + str(int(cfg.seed_rows))
    ).collect()

    after = one(session, "SELECT COUNT(*) AS N FROM " + scope_cfg.fact_table +
                " WHERE COBID = " + str(cob) +
                " AND BOOK_KEY IN (" + key_list + ")")
    return int((after or {}).get("N") or 0)


def unseed(session, scope_cfg, cob=None):
    """Remove seeded fact rows. Only ever touches the reserved test COB."""
    cob = int(cob if cob is not None else TEST_COB)
    if cob != TEST_COB:
        raise ValueError("unseed() only operates on the reserved test COB.")
    session.sql("DELETE FROM " + scope_cfg.fact_table +
                " WHERE COBID = " + str(cob)).collect()
    return True


# ── Phase 2: preview ─────────────────────────────────────────────────────────

def preview(session, payload, mode="summary"):
    """CALL SP_PREVIEW_ADJUSTMENT and return the summary row as a dict."""
    body = dict(payload)
    body["mode"] = mode
    got = rows(session, "CALL " + APP + ".SP_PREVIEW_ADJUSTMENT('" +
               sql_escape(json.dumps(body)) + "')")
    return got[0] if got else {}


def preview_totals(preview_row):
    """The four numbers the harness cares about, whatever the preview shape."""
    r = preview_row or {}
    return {
        "rows": int(_num(r.get("ROWS_AFFECTED"), 0)),
        "nonzero": int(_num(r.get("NONZERO_ROWS"), 0)),
        "current": _num(r.get("TOTAL_CURRENT_VALUE")),
        "delta": _num(r.get("TOTAL_ADJUSTMENT_DELTA")),
        "projected": _num(r.get("TOTAL_PROJECTED_VALUE")),
        "error": str(r.get("ERROR") or r.get("MESSAGE") or ""),
    }


# ── Phase 3: submit ──────────────────────────────────────────────────────────

def submit(session, payload):
    """CALL SP_SUBMIT_ADJUSTMENT. Returns the procedure's result dict."""
    out = call_sp(session, APP + ".SP_SUBMIT_ADJUSTMENT", json.dumps(payload))
    if isinstance(out, dict):
        return out
    return {"status": "Error", "message": str(out)}


def submit_direct_rows(session, scenario, cfg, values):
    """Stage per-row Direct values and submit them as one batch.

    Mirrors tests/test_direct_batch.py: INSERT into ADJ_DIRECT_STAGE under a
    batch id, then SP_SUBMIT_DIRECT_BATCH.
    """
    batch_id = "uatbot-" + str(uuid.uuid4())[:23]
    cols = ["BATCH_ID", "ROW_NUM", "ENTITY_CODE", "BOOK_CODE", "COBID",
            "VALUE_USD", "USERNAME", "REASON"]
    tuples = []
    for i, value in enumerate(values, start=1):
        tuples.append("('" + sql_escape(batch_id) + "', " + str(i) + ", '" +
                      sql_escape(cfg.entity_code) + "', '" +
                      sql_escape(scenario.params.get("book_code", cfg.book_code)) +
                      "', " + str(int(cfg.cob)) + ", '" + str(value) + "', '" +
                      sql_escape(cfg.submitter) + "', '" +
                      sql_escape(cfg.reason) + "')")
    session.sql("INSERT INTO " + APP + ".ADJ_DIRECT_STAGE (" +
                ", ".join(cols) + ") VALUES " + ", ".join(tuples)).collect()

    out = call_sp(session, APP + ".SP_SUBMIT_DIRECT_BATCH", json.dumps({
        "batch_id": batch_id,
        "cobid": int(cfg.cob),
        "process_type": scenario.scope,
        "username": cfg.submitter,
        "requires_approval": False,
        "reason": cfg.reason + " — " + scenario.id,
        "adjustment_category": cfg.category,
    }))
    if isinstance(out, dict):
        out["batch_id"] = batch_id
        return out
    return {"status": "Error", "message": str(out), "batch_id": batch_id}


def write_line_items(session, adj_id, records):
    """Write file-flow rows to ADJ_LINE_ITEM_JSON before the header insert.

    The page pre-generates the adj_id so line items and header share it; the
    harness does the same. Returns the number of rows written.
    """
    if not records:
        return 0
    selects = []
    for i, rec in enumerate(records, start=1):
        selects.append(
            "SELECT '" + sql_escape(adj_id) + "' AS ADJ_ID, " + str(i) +
            " AS ROW_NUM, PARSE_JSON('" + sql_escape(json.dumps(rec)) + "') AS PAYLOAD")
    session.sql(
        "INSERT INTO " + APP + ".ADJ_LINE_ITEM_JSON (ADJ_ID, ROW_NUM, PAYLOAD) " +
        " UNION ALL ".join(selects)).collect()
    return len(records)


def delete_line_items(session, adj_id):
    session.sql("DELETE FROM " + APP + ".ADJ_LINE_ITEM_JSON WHERE ADJ_ID = '" +
                sql_escape(adj_id) + "'").collect()
    return True


# ── Phase 4: approve ─────────────────────────────────────────────────────────

def header_status(session, adj_id):
    got = one(session, """
        SELECT RUN_STATUS, RECORD_COUNT, DIMENSION_ADJ_ID, IS_DELETED,
               PROCESS_TYPE, ADJUSTMENT_TYPE, ADJUSTMENT_ACTION,
               SCALE_FACTOR, SCALE_FACTOR_ADJUSTED, ENTITY_CODE, BOOK_CODE,
               SOURCE_BOOK_CODE, ERROR_MESSAGE
        FROM """ + APP + """.ADJ_HEADER WHERE ADJ_ID = '""" +
        sql_escape(adj_id) + """' LIMIT 1""")
    return got or {}


def approve(session, adj_id, cfg, comment=""):
    """Approve a header awaiting approval, as the approver bot."""
    return call_sp(session, APP + ".SP_DECIDE_ADJUSTMENT", adj_id, "Approved",
                   comment or (cfg.reason + " — auto-approved"), cfg.approver)


# ── Phase 5: wait ────────────────────────────────────────────────────────────

def wait_for_terminal(session, adj_id, cfg, sleep=time.sleep):
    """Poll the header until the pipeline is finished with it.

    Returns (status, seconds_waited). A status of "" means the timeout expired,
    which the caller reports as a failure rather than a pass.

    `sleep` is injectable so unit tests do not actually wait.
    """
    deadline = float(cfg.timeout_s)
    waited = 0.0
    while True:
        status = str(header_status(session, adj_id).get("RUN_STATUS") or "")
        if status in TERMINAL_STATUSES:
            return status, round(waited, 1)
        if waited >= deadline:
            return "", round(waited, 1)
        sleep(cfg.poll_s)
        waited += cfg.poll_s


def force_process(session, adj_id):
    """Push a stuck or pending adjustment through immediately."""
    return call_sp(session, APP + ".SP_FORCE_PROCESS_ADJUSTMENT", adj_id)


# ── Phase 6: verify ──────────────────────────────────────────────────────────

def adjustment_totals(session, scope_cfg, dimension_adj_id):
    """Row count and measure total written to the scope's adjustment table."""
    got = one(session, "SELECT COUNT(*) AS N, COALESCE(SUM(" +
              scope_cfg.metric + "), 0) AS TOTAL FROM " +
              scope_cfg.adjustments_table +
              " WHERE ADJUSTMENT_ID = " + str(int(dimension_adj_id)))
    return {"rows": int((got or {}).get("N") or 0),
            "total": _num((got or {}).get("TOTAL"))}


def base_total(session, scope_cfg, cob, book_key=None):
    where = "COBID = " + str(int(cob))
    if book_key is not None:
        where += " AND BOOK_KEY = " + str(int(book_key))
    got = one(session, "SELECT COUNT(*) AS N, COALESCE(SUM(" +
              scope_cfg.metric + "), 0) AS TOTAL FROM " +
              scope_cfg.fact_table + " WHERE " + where)
    return {"rows": int((got or {}).get("N") or 0),
            "total": _num((got or {}).get("TOTAL"))}


def verify(session, scenario, scope_cfg, adj_id, pv, result, cfg, context=None):
    """Assert the adjustment actually landed, and landed correctly.

    The uniform check is that the measure total written to the scope's
    adjustment table equals the delta the preview projected. Per-type checks
    add the shape-specific expectation on top.
    """
    context = context or {}
    head = header_status(session, adj_id)

    status = str(head.get("RUN_STATUS") or "")
    result.status = status
    result.check("header reached Processed", status == SUCCESS_STATUS,
                 "status=" + (status or "none") + " " +
                 str(head.get("ERROR_MESSAGE") or ""))
    if status != SUCCESS_STATUS:
        return result

    result.check("header not deleted", not bool(head.get("IS_DELETED")))

    dim_id = head.get("DIMENSION_ADJ_ID")
    if not result.check("dimension row minted", dim_id is not None):
        return result
    dim_id = int(dim_id)

    dim = one(session, """
        SELECT RECORD_COUNT, RUN_STATUS, IS_DELETED FROM DIMENSION.ADJUSTMENT
        WHERE ADJUSTMENT_ID = """ + str(dim_id) + """ LIMIT 1""") or {}
    record_count = int(_num(dim.get("RECORD_COUNT"), 0))
    result.check("dimension RECORD_COUNT above zero", record_count > 0,
                 "record_count=" + str(record_count))

    written = adjustment_totals(session, scope_cfg, dim_id)
    result.check("fact rows match RECORD_COUNT",
                 written["rows"] == record_count,
                 "fact=" + str(written["rows"]) + " header=" + str(record_count))

    # The money check.
    result.check("written total equals previewed delta",
                 close_enough(written["total"], pv["delta"]),
                 "written=" + repr(written["total"]) + " previewed=" + repr(pv["delta"]))

    _verify_by_type(session, scenario, scope_cfg, head, pv, written, result,
                    cfg, context)
    return result


def _verify_by_type(session, scenario, scope_cfg, head, pv, written, result,
                    cfg, context):
    """The shape-specific assertion for each adjustment type."""
    adj_type = scenario.adjustment_type
    book_key = context.get("book_key")

    if adj_type == "Flatten":
        # A flatten writes the exact negative of what was there.
        result.check("flatten wrote the negative of the base",
                     close_enough(written["total"], -pv["current"]),
                     "written=" + repr(written["total"]) +
                     " base=" + repr(pv["current"]))

    elif adj_type == "Scale":
        factor = _num(scenario.params.get("scale_factor"), 1.0)
        expected = pv["current"] * (factor - 1.0)
        result.check("scale wrote base x (factor - 1)",
                     close_enough(written["total"], expected),
                     "written=" + repr(written["total"]) +
                     " expected=" + repr(expected))

    elif adj_type == "Transfer":
        # A transfer is pure append, re-keyed onto the target book. The
        # meaningful check is WHERE the written rows landed: every row under
        # this adjustment must carry the target book, none the source. (The
        # base fact table is never written by any adjustment, so comparing it
        # before and after would pass no matter what the engine did.)
        src_key = context.get("source_book_key")
        tgt_key = context.get("target_book_key")
        dim_id = head.get("DIMENSION_ADJ_ID")
        if dim_id is not None and (src_key is not None or tgt_key is not None):
            spread = one(session,
                         "SELECT COUNT(*) AS N, "
                         "COUNT_IF(BOOK_KEY = " + str(int(tgt_key)) + ") AS ON_TARGET, "
                         "COUNT_IF(BOOK_KEY = " + str(int(src_key)) + ") AS ON_SOURCE "
                         "FROM " + scope_cfg.adjustments_table +
                         " WHERE ADJUSTMENT_ID = " + str(int(dim_id))
                         ) if (src_key is not None and tgt_key is not None) else None
            if spread:
                total_n = int(_num(spread.get("N"), 0))
                on_target = int(_num(spread.get("ON_TARGET"), 0))
                on_source = int(_num(spread.get("ON_SOURCE"), 0))
                result.check("every transferred row landed on the target book",
                             total_n > 0 and on_target == total_n,
                             "rows=" + str(total_n) + " on_target=" +
                             str(on_target) + " on_source=" + str(on_source))
                result.check("nothing was written against the source book",
                             on_source == 0, "on_source=" + str(on_source))
        result.check("transfer added rows to the target",
                     written["rows"] > 0, "rows=" + str(written["rows"]))

    elif adj_type == "Roll":
        result.check("roll carried rows forward", written["rows"] > 0,
                     "rows=" + str(written["rows"]))

    elif adj_type == "EROL":
        entity = str(head.get("ENTITY_CODE") or "")
        prior = one(session, """
            SELECT COUNT(*) AS N FROM """ + APP + """.ADJ_HEADER
            WHERE COBID = """ + str(int(cfg.cob)) + """
              AND ENTITY_CODE = '""" + sql_escape(entity) + """'
              AND PROCESS_TYPE = '""" + sql_escape(scenario.scope) + """'
              AND RUN_STATUS = 'Processed'
              AND IS_DELETED = FALSE
              AND ADJUSTMENT_ACTION <> 'EntityRoll'
        """) or {}
        result.check("entity roll cleared prior adjustments for the entity",
                     int(prior.get("N") or 0) == 0,
                     "remaining=" + str(prior.get("N")))

    elif adj_type in ("Direct", "Upload"):
        submitted = _num(context.get("submitted_total"))
        if context.get("submitted_total") is not None:
            result.check("written total equals the submitted values",
                         close_enough(written["total"], submitted),
                         "written=" + repr(written["total"]) +
                         " submitted=" + repr(submitted))


# ── Phase 7: cleanup ─────────────────────────────────────────────────────────

def cleanup(session, adj_id, scope_cfg, cfg):
    """Replay the app's own delete transaction for one adjustment.

    Mirrors streamlit/adjustment_engine/pages/2_Adjustments.py `_do_delete`: soft-delete the
    header, record the transition, soft-delete the dimension row, remove the
    fact adjustment and summary rows — all inside one transaction, so a
    failure leaves nothing half-deleted. Using the app's path means cleanup
    doubles as a test of delete.
    """
    head = header_status(session, adj_id)
    if not head:
        return False
    status = str(head.get("RUN_STATUS") or "")
    if status == "Deleted":
        return True
    dim_id = head.get("DIMENSION_ADJ_ID")
    user = sql_escape(cfg.submitter)
    aid = sql_escape(adj_id)

    txn = False
    try:
        session.sql("BEGIN").collect()
        txn = True
        session.sql("""
            UPDATE """ + APP + """.ADJ_HEADER
            SET IS_DELETED = TRUE, RUN_STATUS = 'Deleted',
                DELETED_BY = '""" + user + """',
                DELETED_DATE = CURRENT_TIMESTAMP()
            WHERE ADJ_ID = '""" + aid + """' AND IS_DELETED = FALSE
        """).collect()
        session.sql("""
            INSERT INTO """ + APP + """.ADJ_STATUS_HISTORY
                (ADJ_ID, OLD_STATUS, NEW_STATUS, CHANGED_BY, COMMENT)
            VALUES ('""" + aid + """', '""" + sql_escape(status) + """',
                    'Deleted', '""" + user + """', 'Test harness cleanup')
        """).collect()
        if dim_id is not None:
            dim_id = int(dim_id)
            session.sql("""
                UPDATE DIMENSION.ADJUSTMENT
                SET IS_DELETED = TRUE, RUN_STATUS = 'Deleted',
                    DELETED_BY = '""" + user + """',
                    DELETED_DATE = CURRENT_TIMESTAMP()
                WHERE ADJUSTMENT_ID = """ + str(dim_id) + """
            """).collect()
            if scope_cfg and scope_cfg.adjustments_table:
                session.sql("DELETE FROM " + scope_cfg.adjustments_table +
                            " WHERE ADJUSTMENT_ID = " + str(dim_id)).collect()
            if scope_cfg and scope_cfg.adjustments_summary_table:
                session.sql("DELETE FROM " + scope_cfg.adjustments_summary_table +
                            " WHERE ADJUSTMENT_ID = " + str(dim_id)).collect()
        session.sql("COMMIT").collect()
        txn = False
        delete_line_items(session, adj_id)
        return True
    except Exception:
        if txn:
            try:
                session.sql("ROLLBACK").collect()
            except Exception:
                pass
        return False


# ── The scenario runner ──────────────────────────────────────────────────────

def run_scenario(session, scenario, cfg, scopes=None, sleep=time.sleep):
    """Run one scenario through all seven phases."""
    cfg.validate()
    scopes = scopes if scopes is not None else load_scopes(session)
    scope_cfg = scopes.get(scenario.scope)
    result = ScenarioResult(scenario=scenario)
    context = {}
    clock = _Clock(result)

    payload = build_payload(scenario, cfg)

    # ── seed ─────────────────────────────────────────────────────────────
    if scenario.seed and scope_cfg is not None and cfg.is_test_cob \
            and cfg.seed_source_cob and scenario.expect == "processed":
        result.phase = "seed"
        with clock("seed"):
            try:
                keys = []
                for code in _books_for(scenario, cfg):
                    info = resolve_book(session, code)
                    if info and info.get("book_key") is not None:
                        keys.append(info["book_key"])
                        context.setdefault("book_key", info["book_key"])
                if keys:
                    seeded = seed(session, scope_cfg, cfg, book_keys=keys)
                    result.check("seeded or already present", True,
                                 "rows inserted=" + str(seeded))
            except Exception as exc:
                result.check("seed", False, str(exc))
                result.message = "seed failed: " + str(exc)
                return result

    # Transfer compares the source book before and after, so snapshot it now.
    if scenario.adjustment_type == "Transfer" and scope_cfg is not None \
            and scenario.expect == "processed":
        src = resolve_book(session, scenario.params.get("source_book_code",
                                                        cfg.book_code))
        tgt = resolve_book(session, scenario.params.get("book_code",
                                                        cfg.target_book_code))
        if src and src.get("book_key") is not None:
            context["book_key"] = src["book_key"]
            context["source_book_key"] = src["book_key"]
            try:
                context["source_before"] = base_total(session, scope_cfg,
                                                      cfg.cob, src["book_key"])
            except Exception:
                pass
        if tgt and tgt.get("book_key") is not None:
            context["target_book_key"] = tgt["book_key"]

    # ── preview ──────────────────────────────────────────────────────────
    pv = {"rows": 0, "nonzero": 0, "current": 0.0, "delta": 0.0,
          "projected": 0.0, "error": ""}
    if scenario.expect == "processed":
        result.phase = "preview"
        with clock("preview"):
            try:
                pv = preview_totals(preview(session, payload))
                result.preview = pv
            except Exception as exc:
                result.check("preview ran", False, str(exc))
                result.message = "preview failed: " + str(exc)
                return result
        if pv["error"]:
            result.check("preview returned no error", False, pv["error"])
            result.message = pv["error"]
            return result
        # A zero-row preview is the app's own submit gate. Honour it rather
        # than submitting something that cannot be verified.
        detail = "rows=" + str(pv["rows"])
        if pv["rows"] <= 0:
            detail += " — seed the COB or widen the filter"
        if not result.check("preview affects at least one row", pv["rows"] > 0,
                            detail):
            result.message = "preview returned zero rows"
            return result

    # ── submit ───────────────────────────────────────────────────────────
    result.phase = "submit"
    with clock("submit"):
        try:
            out = _dispatch_submit(session, scenario, cfg, payload, context)
        except Exception as exc:
            out = {"status": "Error", "message": str(exc)}
    result.message = str(out.get("message") or "")
    result.adj_id = str(out.get("adj_id") or "")
    submit_status = str(out.get("status") or "")

    # Negative scenarios end here.
    if scenario.expect == "reject":
        rejected = (submit_status.lower() == "error") or (not result.adj_id)
        result.check("submission rejected", rejected,
                     "status=" + submit_status + " message=" + result.message)
        if scenario.expect_message:
            result.check("rejection explains why",
                         scenario.expect_message.lower() in result.message.lower(),
                         "wanted '" + scenario.expect_message + "' in: " +
                         result.message)
        result.passed = not result.failed_checks
        return result

    if not result.check("submission accepted", bool(result.adj_id),
                        "status=" + submit_status + " message=" + result.message):
        return result

    try:
        # ── approve ──────────────────────────────────────────────────────
        head = header_status(session, result.adj_id)
        if str(head.get("RUN_STATUS") or "") == "Pending Approval":
            result.phase = "approve"
            with clock("approve"):
                decision = approve(session, result.adj_id, cfg)
            ok = isinstance(decision, dict) and \
                str(decision.get("status", "")).lower() != "error"
            result.check("approved", ok, str(decision))
            if not ok:
                return result

        # ── wait ─────────────────────────────────────────────────────────
        result.phase = "wait"
        with clock("wait"):
            if cfg.force:
                try:
                    force_process(session, result.adj_id)
                except Exception as exc:
                    result.check("force process accepted", False, str(exc))
            status, waited = wait_for_terminal(session, result.adj_id, cfg,
                                               sleep=sleep)
        if not result.check("reached a terminal status before the timeout",
                            bool(status),
                            "waited " + str(waited) + "s of " +
                            str(cfg.timeout_s) + "s"):
            result.message = "timed out waiting for the pipeline"
            return result

        # ── verify ───────────────────────────────────────────────────────
        result.phase = "verify"
        with clock("verify"):
            verify(session, scenario, scope_cfg, result.adj_id, pv, result,
                   cfg, context)

        # ── pruning guard ────────────────────────────────────────────────
        try:
            result.pruning = check_pruning(session, scope_cfg, cfg, result)
            if result.pruning.get("checked") and not result.pruning.get("ok"):
                result.check("engine queries pruned", False,
                             result.pruning.get("detail", ""))
        except Exception:
            result.pruning = {"checked": False, "detail": "unavailable"}

    except Exception as exc:
        # One scenario blowing up must not abandon the rest of a suite, and
        # must not skip the cleanup below.
        result.check("scenario ran without raising", False,
                     type(exc).__name__ + ": " + str(exc))
        result.message = str(exc)

    finally:
        result.passed = not result.failed_checks
        # ── cleanup ──────────────────────────────────────────────────────
        keep = scenario.keep or (cfg.keep_failures and not result.passed)
        if result.adj_id and not keep:
            result.phase = "cleanup"
            with clock("cleanup"):
                result.cleaned = cleanup(session, result.adj_id, scope_cfg, cfg)

    result.phase = "done"
    return result


def _books_for(scenario, cfg):
    """Book codes a scenario needs seeded."""
    out = []
    for key in ("source_book_code", "book_code"):
        code = scenario.params.get(key)
        if code:
            out.append(code)
    if not out:
        out.append(cfg.book_code)
    return out


def _dispatch_submit(session, scenario, cfg, payload, context):
    """Route to the right submit entry point for the adjustment type."""
    adj_type = scenario.adjustment_type

    if adj_type == "Direct" and scenario.scope in DIRECT_ROW_SCOPES:
        values = [100.5, 200.25, 300.75]
        context["submitted_total"] = sum(values)
        return submit_direct_rows(session, scenario, cfg, values)

    if adj_type in ("Upload",) or (adj_type == "Direct" and
                                   scenario.scope in DIRECT_FILE_SCOPES):
        adj_id = str(uuid.uuid4())
        records = _file_records(scenario, cfg)
        context["submitted_total"] = sum(_num(r.get("VALUE_USD")) for r in records)
        write_line_items(session, adj_id, records)
        body = dict(payload)
        body["adj_id"] = adj_id
        body["global_reference"] = scenario.id + "-" + adj_id[:8]
        body["file_name"] = scenario.id + ".csv"
        out = submit(session, body)
        if not out.get("adj_id"):
            try:
                delete_line_items(session, adj_id)
            except Exception:
                pass
        return out

    return submit(session, payload)


def _file_records(scenario, cfg):
    """Minimal file-flow rows for an Upload or FRTB Direct scenario."""
    return [
        {"COBID": int(cfg.cob), "ENTITY_CODE": cfg.entity_code,
         "BOOK_CODE": scenario.params.get("book_code", cfg.book_code),
         "VALUE_USD": 100.5},
        {"COBID": int(cfg.cob), "ENTITY_CODE": cfg.entity_code,
         "BOOK_CODE": scenario.params.get("book_code", cfg.book_code),
         "VALUE_USD": 249.5},
    ]


class _Clock:
    """Context manager recording wall clock per phase onto a result."""

    def __init__(self, result):
        self.result = result
        self.name = ""
        self.t0 = 0.0

    def __call__(self, name):
        self.name = name
        return self

    def __enter__(self):
        self.t0 = time.time()
        return self

    def __exit__(self, *exc):
        self.result.timings[self.name] = round(time.time() - self.t0, 2)
        return False


def run_suite(session, scenarios, cfg, scopes=None, sleep=time.sleep,
              on_result=None):
    """Run scenarios in order and collect the results.

    Sequential on purpose: the engine serialises combinations that write the
    same table, so submitting in parallel would only queue behind itself.
    """
    scopes = scopes if scopes is not None else load_scopes(session)
    results = []
    for scenario in scenarios:
        result = run_scenario(session, scenario, cfg, scopes=scopes, sleep=sleep)
        results.append(result)
        if on_result:
            on_result(result)
    return results


# ── Extras ───────────────────────────────────────────────────────────────────

def check_pruning(session, scope_cfg, cfg, result):
    """Fail a scenario when an engine query scanned most of a table.

    Reads the account's recent query history and looks at partitions scanned
    against partitions total. Most reliable with force=True, because the
    escape hatch runs the engine in this session; a task-run adjustment
    executes in the task's own session, which the current role may not see.
    Returns {"checked": bool, "ok": bool, "worst": float, "detail": str} and
    never raises: an unavailable history is reported, not treated as a failure.
    """
    out = {"checked": False, "ok": True, "worst": 0.0, "detail": ""}
    if not scope_cfg or not result.adj_id:
        out["detail"] = "no adjustment to inspect"
        return out

    table = scope_cfg.adjustments_table.split(".")[-1]
    try:
        history = rows(session, """
            SELECT QUERY_ID, PARTITIONS_SCANNED, PARTITIONS_TOTAL,
                   LEFT(QUERY_TEXT, 200) AS SNIPPET
            FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
                       END_TIME_RANGE_START => DATEADD('hour', -1,
                                                       CURRENT_TIMESTAMP()),
                       RESULT_LIMIT => 10000))
            WHERE QUERY_TEXT ILIKE '%""" + sql_escape(table) + """%'
              AND PARTITIONS_TOTAL > 0
            ORDER BY START_TIME DESC
            LIMIT 200
        """)
    except Exception as exc:
        out["detail"] = "query history unavailable: " + str(exc)
        return out

    if not history:
        out["detail"] = "no matching queries in history"
        return out

    worst = 0.0
    worst_snippet = ""
    for r in history:
        total = _num(r.get("PARTITIONS_TOTAL"))
        if total <= 0:
            continue
        ratio = _num(r.get("PARTITIONS_SCANNED")) / total
        if ratio > worst:
            worst = ratio
            worst_snippet = str(r.get("SNIPPET") or "")

    out["checked"] = True
    out["worst"] = round(worst, 3)
    out["ok"] = worst <= float(cfg.max_scan_ratio)
    out["detail"] = ("worst scan ratio " + str(out["worst"]) + " (limit " +
                     str(cfg.max_scan_ratio) + ")" +
                     ("" if out["ok"] else " on: " + worst_snippet))
    return out


def check_leaks(session, cfg, scopes=None):
    """After a full run, assert the test COB is empty again.

    Returns a list of (what, count) for anything left behind. An empty list
    means the run cleaned up after itself.
    """
    scopes = scopes if scopes is not None else load_scopes(session)
    cob = str(int(cfg.cob))
    leaks = []

    def _count(label, query):
        try:
            got = one(session, query)
            n = int(_num((got or {}).get("N"), 0))
            if n:
                leaks.append((label, n))
        except Exception:
            pass

    _count("ADJ_HEADER (not deleted)",
           "SELECT COUNT(*) AS N FROM " + APP + ".ADJ_HEADER WHERE COBID = " +
           cob + " AND IS_DELETED = FALSE")
    _count("DIMENSION.ADJUSTMENT (not deleted)",
           "SELECT COUNT(*) AS N FROM DIMENSION.ADJUSTMENT WHERE COBID = " +
           cob + " AND IS_DELETED = FALSE")
    _count("ADJ_LINE_ITEM_JSON (orphans)",
           "SELECT COUNT(*) AS N FROM " + APP + ".ADJ_LINE_ITEM_JSON j "
           "WHERE NOT EXISTS (SELECT 1 FROM " + APP + ".ADJ_HEADER h "
           "WHERE h.ADJ_ID = j.ADJ_ID)")

    for name, sc in scopes.items():
        if sc.adjustments_table:
            _count(sc.adjustments_table,
                   "SELECT COUNT(*) AS N FROM " + sc.adjustments_table +
                   " WHERE COBID = " + cob)
        if sc.adjustments_summary_table:
            _count(sc.adjustments_summary_table,
                   "SELECT COUNT(*) AS N FROM " + sc.adjustments_summary_table +
                   " WHERE COBID = " + cob)
        if sc.fact_table:
            _count(sc.fact_table + " (seeded rows)",
                   "SELECT COUNT(*) AS N FROM " + sc.fact_table +
                   " WHERE COBID = " + cob)
    return leaks


def check_serialisation(session, adj_ids):
    """Assert two adjustments writing the same table did not run concurrently.

    Reads each header's Running window from ADJ_STATUS_HISTORY and looks for
    an overlap. This closes one of the live-warehouse checks left open by the
    2026-09 audit: combinations sharing a target table are dispatched in
    separate waves precisely so their transactions cannot interleave.

    Returns {"checked": bool, "ok": bool, "detail": str}.
    """
    if len(adj_ids or []) < 2:
        return {"checked": False, "ok": True, "detail": "needs two adjustments"}

    quoted = ", ".join("'" + sql_escape(a) + "'" for a in adj_ids)
    try:
        windows = rows(session, """
            SELECT ADJ_ID,
                   MIN(CASE WHEN NEW_STATUS = 'Running' THEN CHANGED_DATE END) AS STARTED,
                   MAX(CASE WHEN NEW_STATUS IN ('Processed','Failed')
                            THEN CHANGED_DATE END) AS ENDED
            FROM """ + APP + """.ADJ_STATUS_HISTORY
            WHERE ADJ_ID IN (""" + quoted + """)
            GROUP BY ADJ_ID
        """)
    except Exception as exc:
        return {"checked": False, "ok": True,
                "detail": "status history unavailable: " + str(exc)}

    spans = [(str(w.get("ADJ_ID")), w.get("STARTED"), w.get("ENDED"))
             for w in windows
             if w.get("STARTED") is not None and w.get("ENDED") is not None]
    if len(spans) < 2:
        return {"checked": False, "ok": True,
                "detail": "not enough completed Running windows"}

    for i in range(len(spans)):
        for j in range(i + 1, len(spans)):
            a_id, a_start, a_end = spans[i]
            b_id, b_start, b_end = spans[j]
            if a_start < b_end and b_start < a_end:
                return {"checked": True, "ok": False,
                        "detail": (a_id + " and " + b_id +
                                   " were Running at the same time")}
    return {"checked": True, "ok": True,
            "detail": "all Running windows were disjoint"}


# ── Reporting ────────────────────────────────────────────────────────────────

def render_report(results, cfg=None, title="Adjustment harness run"):
    """Markdown summary, same shape as docs/UAT_AUTOMATION_REPORT.md."""
    lines = ["# " + title, ""]
    if cfg is not None:
        lines += [
            "COB `" + str(cfg.cob) + "`" +
            ("" if cfg.is_test_cob else "  **(real COB)**") +
            ", books `" + cfg.book_code + "` to `" + cfg.target_book_code + "`, " +
            ("forced" if cfg.force else "via the pipeline tasks") + ".",
            "",
        ]

    passed = [r for r in results if r.passed]
    lines += [
        str(len(passed)) + " of " + str(len(results)) + " scenarios passed.",
        "",
        "| Scenario | Scope | Type | Result | Rows | Elapsed | Notes |",
        "|---|---|---|---|---|---|---|",
    ]
    for r in results:
        notes = "" if r.passed else "; ".join(
            c[0] + " (" + c[2] + ")" for c in r.failed_checks[:3])
        lines.append("| " + " | ".join([
            r.scenario.id,
            r.scenario.scope,
            r.scenario.adjustment_type,
            "pass" if r.passed else "FAIL",
            str(r.preview.get("rows", "")),
            str(r.elapsed) + "s",
            (notes or r.scenario.note).replace("|", "/")[:120],
        ]) + " |")

    lines += ["", "## Timings", "",
              "| Scenario | " + " | ".join(
                  ["seed", "preview", "submit", "approve", "wait", "verify",
                   "cleanup"]) + " |",
              "|---|---|---|---|---|---|---|---|"]
    for r in results:
        cells = [str(r.timings.get(p, "")) for p in
                 ("seed", "preview", "submit", "approve", "wait", "verify",
                  "cleanup")]
        lines.append("| " + r.scenario.id + " | " + " | ".join(cells) + " |")

    failures = [r for r in results if not r.passed]
    if failures:
        lines += ["", "## Failures", ""]
        for r in failures:
            lines += ["### " + r.scenario.id + " — " + r.scenario.scope + " " +
                      r.scenario.adjustment_type,
                      "",
                      "Reached phase `" + r.phase + "`" +
                      (", adjustment `" + r.adj_id + "`" if r.adj_id else "") + ".",
                      ""]
            for name, ok, detail in r.checks:
                lines.append("- " + ("ok" if ok else "**FAIL**") + " — " +
                             name + (": " + detail if detail else ""))
            lines.append("")
    return "\n".join(lines)
