"""
UAT automation harness — MUFG Risk Adjustment Engine
=====================================================
Drives the ADJUSTMENT_APP stored procedures headlessly against the environment
in config.py, impersonating distinct users via the procs' p_caller argument
(the same identity channel the Streamlit app uses). Every test records its
verification SQL and results as EVIDENCE; a full run report mapped to the UAT
test-plan IDs is written to docs/UAT_AUTOMATION_REPORT.md after every run,
pass or fail.

Isolation: all mutating tests work on a far-future COB (UAT_FAKE_COB,
default 20991231) and entities prefixed ZUAT, then delete everything they
created. Nothing touches real COBs unless explicitly configured.

Run:  pytest tests -v
"""
import datetime as dt
import json
import os
import sys
import pathlib

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import config  # noqa: E402  (repo-root config.py)

# ── Test identities & isolation constants ────────────────────────────────────
FAKE_COB = int(os.environ.get("UAT_FAKE_COB", "20991231"))
U_SUBMIT = "UAT_BOT_SUBMITTER"
U_APPROVE = "UAT_BOT_APPROVER"
ENT_A, ENT_B, ENT_C, ENT_D = "ZUATA", "ZUATB", "ZUATC", "ZUATD"
STAGE_BATCH_PREFIX = "uatbot-"
ADJ_ID_NAMESPACE = "b1e8c9d4-3f72-4a6e-8b5a-9c0d1e2f3a4b"

_REPORT_PATH = pathlib.Path(__file__).resolve().parents[1] / "docs" / "UAT_AUTOMATION_REPORT.md"

# nodeid -> {"uat": (id, title, priority), "evidence": [...], "outcome": str,
#            "duration": float, "error": str}
_RESULTS = {}


# ── SQL helpers ──────────────────────────────────────────────────────────────
def esc(v) -> str:
    """Escape a SQL string literal (backslashes first, then quotes)."""
    return str(v).replace("\\", "\\\\").replace("'", "''")


def lit(v) -> str:
    """Render a python value as a SQL literal."""
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + esc(v) + "'"


def rows(session, query):
    """Run SQL, return list of plain dicts (column names upper-case)."""
    out = []
    for r in session.sql(query).collect():
        d = r.as_dict() if hasattr(r, "as_dict") else dict(r)
        out.append({str(k).upper(): v for k, v in d.items()})
    return out


def call_sp(session, name, *args):
    """CALL a stored procedure; returns the scalar result (parsed from JSON
    when it looks like JSON)."""
    arglist = ", ".join(lit(a) for a in args)
    res = session.sql(f"CALL {name}({arglist})").collect()
    raw = res[0][0] if res and len(res[0]) else None
    if isinstance(raw, str):
        s = raw.strip()
        if s.startswith("{") or s.startswith("["):
            try:
                return json.loads(s)
            except ValueError:
                return raw
    return raw


# ── Session fixture (same connection pattern as test_process.py/deploy.py) ──
@pytest.fixture(scope="session")
def session():
    from mufg_snowflakeconn import sfconnection as m_sf
    env = os.environ.get("UAT_SF_ENV", config.SF_CONN_ENV)
    usr = os.environ.get("UAT_SF_USER", config.DEPLOY_USER)
    conn = m_sf.MufgSnowflakeConn(env, usr)
    sess = conn.get_snowflake_session()
    sess.use_role(config.ROLE_OWNER)
    sess.use_warehouse(config.WAREHOUSE)
    sess.use_database(config.DATABASE)
    yield sess
    _cleanup(sess)


@pytest.fixture(scope="session")
def feed_table(session) -> str:
    r = rows(session, """
        SELECT CONFIG_VALUE FROM ADJUSTMENT_APP.ADJ_APP_CONFIG
        WHERE CONFIG_KEY = 'SIGNOFF_FEED_TABLE'""")
    return (r[0]["CONFIG_VALUE"] if r and r[0]["CONFIG_VALUE"]
            else "BATCH.PUBLISH_SIGNOFF_STATUS").strip()


@pytest.fixture(scope="session")
def approvers(session):
    """Register the two bot identities as universal approvers for the run.
    Removed (by ADDED_BY tag) in the session cleanup."""
    for u in (U_SUBMIT, U_APPROVE):
        session.sql(f"""
            MERGE INTO ADJUSTMENT_APP.ADJ_APPROVERS t
            USING (SELECT '{u}' AS USERNAME) s
            ON UPPER(t.USERNAME) = s.USERNAME AND t.PROCESS_TYPE IS NULL
            WHEN MATCHED THEN UPDATE SET IS_ACTIVE = TRUE
            WHEN NOT MATCHED THEN INSERT (USERNAME, PROCESS_TYPE, IS_ACTIVE, ADDED_BY)
            VALUES (s.USERNAME, NULL, TRUE, 'UAT_AUTOMATION')
        """).collect()
    return (U_SUBMIT, U_APPROVE)


def _cleanup(sess):
    """Best-effort removal of everything the suite created. Each statement is
    independent — a missing grant on one never blocks the rest."""
    feed = "BATCH.PUBLISH_SIGNOFF_STATUS"
    try:
        r = rows(sess, """SELECT CONFIG_VALUE FROM ADJUSTMENT_APP.ADJ_APP_CONFIG
                          WHERE CONFIG_KEY = 'SIGNOFF_FEED_TABLE'""")
        if r and r[0]["CONFIG_VALUE"]:
            feed = str(r[0]["CONFIG_VALUE"]).strip()
    except Exception:
        pass
    stmts = [
        f"""DELETE FROM ADJUSTMENT_APP.ADJ_LINE_ITEM_JSON WHERE ADJ_ID IN
            (SELECT ADJ_ID FROM ADJUSTMENT_APP.ADJ_HEADER WHERE COBID = {FAKE_COB})""",
        f"""DELETE FROM ADJUSTMENT_APP.ADJ_STATUS_HISTORY WHERE ADJ_ID IN
            (SELECT ADJ_ID FROM ADJUSTMENT_APP.ADJ_HEADER WHERE COBID = {FAKE_COB})""",
        f"DELETE FROM ADJUSTMENT_APP.ADJ_HEADER WHERE COBID = {FAKE_COB}",
        f"DELETE FROM ADJUSTMENT_APP.ADJ_DIRECT_STAGE WHERE BATCH_ID LIKE '{STAGE_BATCH_PREFIX}%'",
        f"DELETE FROM ADJUSTMENT_APP.ADJ_SIGNOFF_HISTORY WHERE COBID = {FAKE_COB}",
        f"DELETE FROM ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS WHERE COBID = {FAKE_COB}",
        f"DELETE FROM {feed} WHERE COBID = {FAKE_COB}",
        "DELETE FROM ADJUSTMENT_APP.ADJ_APPROVERS WHERE ADDED_BY = 'UAT_AUTOMATION'",
    ]
    for s in stmts:
        try:
            sess.sql(s).collect()
        except Exception as ex:
            print(f"[cleanup] skipped ({str(ex).splitlines()[0][:120]}): {s[:90]}")


# ── Evidence recording ───────────────────────────────────────────────────────
class Evidence:
    def __init__(self, session, bucket):
        self._s = session
        self._b = bucket

    def note(self, label, text=""):
        self._b.append({"kind": "note", "label": label, "text": str(text)})

    def sql(self, label, query, max_rows=10):
        """Run a verification query and record it + its result. Returns rows."""
        result = rows(self._s, query)
        self._b.append({"kind": "sql", "label": label,
                        "query": " ".join(query.split()),
                        "rows": result[:max_rows], "count": len(result)})
        return result

    def check(self, description, condition):
        """Record a named assertion, then enforce it."""
        self._b.append({"kind": "check", "label": description,
                        "ok": bool(condition)})
        assert condition, description


@pytest.fixture
def ev(session, request):
    bucket = _RESULTS.setdefault(request.node.nodeid, {}).setdefault("evidence", [])
    return Evidence(session, bucket)


# ── Pytest wiring: uat marker + report generation ────────────────────────────
def pytest_configure(config):  # noqa: F811 — pytest requires this exact name
    config.addinivalue_line(
        "markers",
        "uat(id, title=..., priority=...): map a test to a UAT test-plan case")


def pytest_itemcollected(item):
    """Friendly display names: every test shows as
    'Adjustment App · <area> · <UAT-ID> — <title>' in the console and the
    report, instead of the raw file::function nodeid. The area comes from
    each test module's AREA constant."""
    m = item.get_closest_marker("uat")
    if not m:
        return
    uid = m.args[0] if m.args else "—"
    title = m.kwargs.get("title", item.name)
    area = getattr(item.module, "AREA", pathlib.Path(str(item.fspath)).stem)
    item._nodeid = f"Adjustment App · {area} · {uid} — {title}"


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    rep = outcome.get_result()
    if rep.when != "call":
        # setup errors / skips still need an outcome recorded
        if rep.when == "setup" and (rep.failed or rep.skipped):
            entry = _RESULTS.setdefault(item.nodeid, {})
            entry["outcome"] = "SKIPPED" if rep.skipped else "ERROR"
            entry["duration"] = rep.duration
            entry["error"] = str(rep.longrepr)[:600] if rep.longrepr else ""
            _stamp_meta(item)
        return
    entry = _RESULTS.setdefault(item.nodeid, {})
    entry["outcome"] = ("PASSED" if rep.passed
                        else "SKIPPED" if rep.skipped else "FAILED")
    entry["duration"] = rep.duration
    entry["error"] = "" if rep.passed else (str(rep.longrepr)[:600] if rep.longrepr else "")
    _stamp_meta(item)


def _stamp_meta(item):
    m = item.get_closest_marker("uat")
    if m:
        _RESULTS[item.nodeid]["uat"] = (
            m.args[0] if m.args else "—",
            m.kwargs.get("title", item.name),
            m.kwargs.get("priority", "—"))
    else:
        _RESULTS[item.nodeid]["uat"] = ("—", item.name, "—")


def pytest_sessionfinish(session, exitstatus):
    if not _RESULTS:
        return
    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total = len(_RESULTS)
    passed = sum(1 for r in _RESULTS.values() if r.get("outcome") == "PASSED")
    failed = sum(1 for r in _RESULTS.values() if r.get("outcome") == "FAILED")
    skipped = sum(1 for r in _RESULTS.values()
                  if r.get("outcome") in ("SKIPPED", "ERROR"))

    lines = [
        "# Adjustment App — UAT Automation Report",
        "",
        f"Generated {now} local · environment `{config.DATABASE}` "
        f"(role `{config.ROLE_OWNER}`, warehouse `{config.WAREHOUSE}`) · "
        f"isolation COB `{FAKE_COB}`",
        "",
        f"**{passed} passed · {failed} failed · {skipped} skipped/errored "
        f"· {total} total.** Automated coverage of the UAT test plan; "
        "cases not listed here are manual-only (UI) — see tests/README.md.",
        "",
        "| UAT ID | Test | Priority | Result | Duration |",
        "|---|---|---|---|---|",
    ]
    ordered = sorted(_RESULTS.items(), key=lambda kv: kv[1].get("uat", ("~",))[0])
    for nodeid, r in ordered:
        uid, title, pri = r.get("uat", ("—", nodeid, "—"))
        res = r.get("outcome", "NOT RUN")
        mark = {"PASSED": "✅ PASSED", "FAILED": "❌ FAILED",
                "SKIPPED": "⏭ SKIPPED", "ERROR": "⚠️ ERROR"}.get(res, res)
        lines.append(f"| {uid} | {title} | {pri} | {mark} | "
                     f"{r.get('duration', 0):.1f}s |")

    lines += ["", "---", "", "## Evidence per test", ""]
    for nodeid, r in ordered:
        uid, title, pri = r.get("uat", ("—", nodeid, "—"))
        lines.append(f"### {uid} — {title}  ({r.get('outcome', 'NOT RUN')})")
        if r.get("error"):
            lines += ["", "```", r["error"], "```"]
        for e in r.get("evidence", []):
            if e["kind"] == "note":
                lines.append(f"- **{e['label']}** {e['text']}")
            elif e["kind"] == "check":
                lines.append(f"- {'✅' if e['ok'] else '❌'} {e['label']}")
            elif e["kind"] == "sql":
                lines.append(f"- **{e['label']}** ({e['count']} row(s))")
                lines.append(f"  - `{e['query'][:400]}`")
                if e["rows"]:
                    lines.append(f"  - `{json.dumps(e['rows'][:3], default=str)[:500]}`")
        lines.append("")

    _REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    _REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"\nUAT report written to {_REPORT_PATH}")
