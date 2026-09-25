"""Guards on the submitter permission list (ADJ_SUBMITTERS).

Marcos, 2026-09-23: "Anyone could use the new adjustment to check something
or even run the impact. But only the people on the permission list should be
able to submit an adjustment."

So this gates SUBMIT and nothing else. The draft, the filters and the impact
preview stay open to everyone, because checking a number is not the same as
booking one, and taking that away pushes people back to spreadsheets.

Two rules carry the most risk and are pinned hardest below:

- **Bootstrap.** An empty or all-inactive list means EVERYONE may submit.
  Without that, deploying the table would lock every user out of the app at
  once, with no way back except a DBA insert.
- **Fail open.** If the table cannot be read, submission is allowed. An
  unreadable permission table is a deployment fault, and blocking every
  submission until somebody notices is a worse failure than allowing them —
  every submission records its username either way, so the audit trail
  survives.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "streamlit", "adjustment_engine"))

import utils.snowflake_conn as sc

ENGINE_SQL = os.path.join(os.path.dirname(__file__), "..", "..",
                          "streamlit", "adjustment_engine", "sql",
                          "03_sp_submit_adjustment.sql")
TABLES_SQL = os.path.join(os.path.dirname(__file__), "..", "..",
                          "streamlit", "adjustment_engine", "sql", "01_tables.sql")
PAGE = os.path.join(os.path.dirname(__file__), "..", "..", "streamlit", "adjustment_engine", "pages",
                    "1_New_Adjustment.py")


def _source(path):
    with open(path, encoding="utf-8") as fh:
        return fh.read()


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def collect(self):
        return self._rows


class _Session:
    def __init__(self, rows=None, raises=False):
        self.rows = rows if rows is not None else []
        self.raises = raises
        self.queries = []

    def sql(self, query, *a, **k):
        self.queries.append(query)
        if self.raises:
            raise RuntimeError("table does not exist")
        return _Result(self.rows)


@pytest.fixture
def patched(monkeypatch):
    def _install(rows=None, raises=False):
        session = _Session(rows, raises)
        monkeypatch.setattr(sc, "get_session", lambda: session)
        return session
    return _install


# ── The bootstrap rule ───────────────────────────────────────────────────────

def test_an_empty_list_lets_everyone_submit(patched):
    patched([])
    assert sc.submit_access("anyone@mufg.com") is None
    assert sc.can_submit(None, "VaR") is True


def test_an_unreadable_table_lets_everyone_submit(patched):
    """A missing table is a deploy fault, not a reason to stop all work."""
    patched(raises=True)
    assert sc.submit_access("anyone@mufg.com") is None


# ── Who is allowed ───────────────────────────────────────────────────────────

def test_a_blank_scope_allows_every_scope(patched):
    patched([{"U": "ALICE@MUFG.COM", "PT": None}])
    access = sc.submit_access("alice@mufg.com")
    assert access == {"*"}
    for scope in ("VaR", "Stress", "FRTBRRAO"):
        assert sc.can_submit(access, scope) is True


def test_a_scoped_row_allows_only_that_scope(patched):
    patched([{"U": "BOB@MUFG.COM", "PT": "VaR"}])
    access = sc.submit_access("bob@mufg.com")
    assert access == {"VAR"}
    assert sc.can_submit(access, "VaR") is True
    assert sc.can_submit(access, "Stress") is False


def test_several_scoped_rows_accumulate(patched):
    patched([{"U": "BOB@MUFG.COM", "PT": "VaR"},
             {"U": "BOB@MUFG.COM", "PT": "Stress"}])
    access = sc.submit_access("bob@mufg.com")
    assert access == {"VAR", "STRESS"}
    assert sc.can_submit(access, "FRTB") is False


def test_a_blank_scope_row_wins_over_a_scoped_one(patched):
    """Listed for everything AND for one scope means everything."""
    patched([{"U": "BOB@MUFG.COM", "PT": "VaR"},
             {"U": "BOB@MUFG.COM", "PT": None}])
    assert sc.submit_access("bob@mufg.com") == {"*"}


def test_an_unlisted_user_is_refused_once_anyone_is_listed(patched):
    patched([{"U": "ALICE@MUFG.COM", "PT": None}])
    access = sc.submit_access("mallory@mufg.com")
    assert access == set(), (
        "An unlisted user must come back as an EMPTY set, not None. None "
        "means the bootstrap rule is in force and would let them submit.")
    assert sc.can_submit(access, "VaR") is False


@pytest.mark.parametrize("typed", ["ALICE@MUFG.COM", "alice@mufg.com",
                                   "  Alice@Mufg.Com  "])
def test_the_username_match_ignores_case_and_padding(patched, typed):
    patched([{"U": "ALICE@MUFG.COM", "PT": None}])
    assert sc.submit_access(typed) == {"*"}


def test_only_active_rows_are_considered(patched):
    session = patched([{"U": "ALICE@MUFG.COM", "PT": None}])
    sc.submit_access("alice@mufg.com")
    assert "IS_ACTIVE = TRUE" in session.queries[0], (
        "The lookup must filter on IS_ACTIVE, or deactivating a leaver would "
        "not take their submit rights away.")


def test_the_scope_comparison_is_case_insensitive():
    assert sc.can_submit({"VAR"}, "var") is True
    assert sc.can_submit({"VAR"}, " VaR ") is True


def test_can_submit_mirrors_can_sign_off():
    """Same contract, so the two are read and reasoned about the same way."""
    for access, scope in ((None, "VaR"), ({"*"}, "VaR"),
                          ({"VAR"}, "VaR"), ({"VAR"}, "Stress"),
                          (set(), "VaR")):
        assert sc.can_submit(access, scope) == sc.can_sign_off(access, scope)


# ── The gate that actually counts is server-side ─────────────────────────────

def test_the_stored_procedure_enforces_it_too():
    """The page gate is a convenience; this is the one path all submits take."""
    src = _source(ENGINE_SQL)
    assert "def _may_submit(" in src, (
        "SP_SUBMIT_ADJUSTMENT does not check the submitter list. The page "
        "gate alone is a UI convenience, and the wizard, the per-scope "
        "fan-out and the Transfer Book fan-out all reach the procedure.")
    assert "if not _may_submit(session, username, process_type):" in src


def test_the_stored_procedure_checks_before_it_writes():
    src = _source(ENGINE_SQL)
    gate = src.index("if not _may_submit(session, username, process_type):")
    insert = src.index("INSERT INTO ADJUSTMENT_APP.ADJ_HEADER")
    assert gate < insert, (
        "The permission check runs after the header insert, so a refused "
        "submission would already be recorded.")


def test_the_stored_procedure_keeps_the_bootstrap_rule():
    src = _source(ENGINE_SQL)
    body = src[src.index("def _may_submit("):]
    body = body[:body.index("def compute_scale_factor_adjusted(")]
    assert "if not listed:\n        return True" in body, (
        "The procedure does not apply the bootstrap rule. Deploying the "
        "table would lock every user out of submitting at once.")


def test_the_stored_procedure_fails_open():
    src = _source(ENGINE_SQL)
    body = src[src.index("def _may_submit("):]
    body = body[:body.index("def compute_scale_factor_adjusted(")]
    assert body.count("except Exception:\n        return True") == 2, (
        "Both lookups in _may_submit must fail open. An unreadable "
        "permission table is a deploy fault; blocking every submission until "
        "someone notices is the worse failure.")


def test_the_refusal_explains_what_to_do():
    src = _source(ENGINE_SQL)
    assert "is not on the submitter list for" in src
    assert "ask an admin to add you" in src, (
        "A refusal must say how to get access, or it just reads as a bug.")


# ── Preview stays open to everyone ───────────────────────────────────────────

def test_the_submit_button_is_gated():
    src = _source(PAGE)
    assert "or signoff_blocked or submit_blocked or direct_blocked" in src, (
        "submit_blocked is not wired into the Submit button's disabled flag.")


def test_the_permission_check_guards_nothing_but_submit():
    """Drafting and previewing must stay open to everyone.

    Enumerates every line mentioning the helpers. There should be the import
    and the single gate, and nothing else — a third call site would most
    likely be someone gating the preview too, which is exactly the thing
    Marcos asked not to happen.
    """
    src = _source(PAGE)
    hits = [ln.strip() for ln in src.splitlines()
            if "submit_access(" in ln or "can_submit(" in ln]
    assert len(hits) == 2, (
        "Expected exactly two uses of the permission helpers, the import "
        "aside: one access lookup and one per-scope check. Found "
        + str(len(hits)) + ": " + repr(hits) + ". If one of these guards the "
        "preview, the impact figures are no longer open to everyone.")
    assert any("submit_access(current_user_name())" in h for h in hits)
    assert any("can_submit(_sub_access, sc)" in h for h in hits)


def test_the_preview_does_not_consult_the_permission_list():
    """The preview trigger runs its own SQL; it must not be permission-gated."""
    src = _source(PAGE)
    gate_line = next(i for i, ln in enumerate(src.splitlines())
                     if "_sub_denied = " in ln)
    lines = src.splitlines()
    # The gate sits with the other submit-time checks, not up in the
    # preview section — if it drifts above the preview trigger, someone has
    # probably started using it there too.
    preview_line = next(i for i, ln in enumerate(lines)
                        if "Impact preview trigger" in ln)
    assert gate_line < preview_line + 40, (
        "The submitter gate has moved far from the submit controls.")
    assert "submit_blocked" not in "\n".join(lines[preview_line:preview_line + 30]), (
        "The preview section references submit_blocked. Previewing must not "
        "depend on submit permission.")


def test_the_page_tells_the_user_why_and_that_preview_still_works():
    src = _source(PAGE)
    assert "you are not on the submitter list for" in src
    assert "You can still preview the impact" in src, (
        "The blocked message must say the preview is still available, or "
        "people will assume the whole page is closed to them.")


# ── The table ────────────────────────────────────────────────────────────────

def test_the_table_exists_and_mirrors_the_other_permission_lists():
    src = _source(TABLES_SQL)
    assert "CREATE OR ALTER TABLE ADJUSTMENT_APP.ADJ_SUBMITTERS" in src
    block = src[src.index("CREATE OR ALTER TABLE ADJUSTMENT_APP.ADJ_SUBMITTERS"):]
    block = block[:block.index(";")]
    for column in ("USERNAME", "PROCESS_TYPE", "IS_ACTIVE", "ADDED_BY",
                   "ADDED_DATE"):
        assert column in block, (
            "ADJ_SUBMITTERS is missing " + column + ". It must mirror "
            "ADJ_APPROVERS and ADJ_SIGNOFF_USERS so the Admin page, the "
            "helpers and the cleanup all work the same way.")
    assert "-- NULL = all scopes" in block
