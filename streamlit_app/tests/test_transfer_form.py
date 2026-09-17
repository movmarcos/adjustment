"""Transfer Book: pure fan-out helpers + the page's form / submit routing.

The helpers (utils/transfer_book.py) are Streamlit-free, so the job list and
the partial-failure copy are exercised directly. The AppTests drive the real
page: a Transfer replaces the Dimension Filters card with Transfer Details,
and Submit makes ONE SP_SUBMIT_ADJUSTMENT call per (scope × trade).
"""
import json, os, sys

import pandas as pd
import pytest

APP = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, APP)
from streamlit.testing.v1 import AppTest
import utils.snowflake_conn as sc
from utils.transfer_book import transfer_jobs, book_entity, submit_jobs


# ══════════════════════════════════════════════════════════════════════════
# Pure helpers
# ══════════════════════════════════════════════════════════════════════════

def test_jobs_whole_book_per_scope():
    assert transfer_jobs(["VaR", "Stress"], []) == [("VaR", None), ("Stress", None)]
    assert transfer_jobs(["VaR"], None) == [("VaR", None)]
    assert transfer_jobs([], ["T1"]) == []


def test_jobs_per_scope_per_trade():
    assert transfer_jobs(["VaR"], ["T1", " T2 "]) == [("VaR", "T1"), ("VaR", "T2")]
    assert len(transfer_jobs(["VaR", "FRTB"], ["T1", "T2", "T3"])) == 6
    # Blank / whitespace-only trade codes never become a job of their own.
    assert transfer_jobs(["VaR"], ["", "  ", "T1"]) == [("VaR", "T1")]


def test_book_entity():
    rows = [("B1", "D1", "MUSI"), ("B2", "D1", None)]
    assert book_entity(rows, "b1") == "MUSI"
    assert book_entity(rows, "B2") is None
    assert book_entity(rows, "ZZ") is None
    assert book_entity(None, "B1") is None
    assert book_entity(rows, None) is None


def _is_success(res):
    return (res or {}).get("status") in ("Pending", "Pending Approval", "Approved")


def _scope_label(code):
    return {"FRTB": "FRTBSBM"}.get(code, code)


def test_submit_jobs_one_call_per_job_with_trade_swapped():
    seen = []

    def submit_one(p):
        seen.append((p["process_type"], p.get("trade_code")))
        return {"status": "Pending", "adj_id": "x"}

    out = submit_jobs(transfer_jobs(["VaR", "FRTB"], ["T1", "T2"]),
                      {"adjustment_type": "Transfer", "source_book_code": "B1"},
                      submit_one, _is_success, _scope_label)

    assert seen == [("VaR", "T1"), ("VaR", "T2"), ("FRTB", "T1"), ("FRTB", "T2")]
    assert out["status"] == "Pending"
    assert out["fanout"] is True and out["created"] == 4
    assert "VaR / T1" in out["message"] and "FRTBSBM / T2" in out["message"]


def test_submit_jobs_whole_book_sends_no_trade_code():
    seen = []

    def submit_one(p):
        seen.append(p)
        return {"status": "Pending"}

    submit_jobs(transfer_jobs(["VaR"], []), {"adjustment_type": "Transfer"},
                submit_one, _is_success, _scope_label)
    assert "trade_code" not in seen[0]


def test_submit_jobs_partial_failure_names_what_was_created():
    def submit_one(p):
        if p.get("trade_code") == "T2":
            return {"status": "Error", "message": "COB is signed off"}
        return {"status": "Pending"}

    out = submit_jobs(transfer_jobs(["VaR"], ["T1", "T2"]), {}, submit_one,
                      _is_success, _scope_label)
    assert out["status"] == "Error"
    assert out["created"] == 1 and out["fanout"] is True
    assert "VaR / T2: COB is signed off" in out["message"]
    assert "Already created: VaR / T1" in out["message"]


def test_submit_jobs_reports_progress_once_per_job():
    seen = []

    def submit_one(p):
        return {"status": "Pending"}

    jobs = transfer_jobs(["VaR", "FRTB"], ["T1", "T2"])
    out = submit_jobs(jobs, {}, submit_one, _is_success, _scope_label,
                      on_progress=lambda i, n, label: seen.append((i, n, label)))

    assert [i for i, _, _ in seen] == [1, 2, 3, 4]      # 1..n, in order
    assert {n for _, n, _ in seen} == {4}
    assert seen[0][2] == "VaR / T1" and seen[-1][2] == "FRTBSBM / T2"
    assert out["created"] == 4
    # Still optional: the default path must not need a callback.
    assert submit_jobs(jobs, {}, submit_one, _is_success,
                       _scope_label)["created"] == 4


def test_submit_jobs_pending_approval_wins_and_empty_is_guarded():
    def submit_one(p):
        return {"status": "Pending Approval" if p.get("trade_code") == "T2"
                          else "Pending"}

    out = submit_jobs(transfer_jobs(["VaR"], ["T1", "T2"]), {}, submit_one,
                      _is_success, _scope_label)
    assert out["status"] == "Pending Approval"
    assert "waiting for approval" in out["message"]
    assert "Statuses:" in out["message"]

    empty = submit_jobs([], {}, submit_one, _is_success, _scope_label)
    assert empty == {"status": "Error", "message": "No scope selected.",
                     "fanout": True, "created": 0}


# ══════════════════════════════════════════════════════════════════════════
# Page (AppTest)
# ══════════════════════════════════════════════════════════════════════════

CALLS = []


class Row(list):
    def __getitem__(self, k):
        return list.__getitem__(self, 0 if isinstance(k, str) else k)


class SQL:
    def __init__(self, q):
        self.q = q

    def collect(self):
        CALLS.append(self.q)
        if "SP_SUBMIT_ADJUSTMENT" in self.q:
            return [Row([json.dumps({"status": "Pending", "adj_id": "x",
                                     "message": "Created with status 'Pending'."})])]
        if "CURRENT_USER" in self.q.upper():
            return [Row(["TESTER"])]
        return []

    def to_pandas(self):
        CALLS.append(self.q)
        return pd.DataFrame()


class Sess:
    def sql(self, q, *a, **k):
        return SQL(q)


@pytest.fixture(autouse=True)
def fake(monkeypatch):
    monkeypatch.setattr(sc, "get_session", lambda: Sess())
    monkeypatch.chdir(APP)
    CALLS.clear()


def _load():
    at = AppTest.from_file("pages/1_New_Adjustment.py", default_timeout=120).run()
    assert not at.exception, at.exception
    return at


def _seed_ref_data(at):
    """Pre-fill the reference caches _ref_rows() reads (the fake session
    returns no rows), so the book dropdowns and the trade multiselect render
    with real options.

    The Transfer dropdowns read `_ref_books_current` (current books only);
    `_ref_books_v2` is the every-row cache the Dimension Filters card uses."""
    _books = [["B1", "D1", "E1"], ["B2", "D2", "E2"]]
    at.session_state["_ref_books_current"] = list(_books)
    at.session_state["_ref_books_v2"] = list(_books)
    at.session_state["_ref_trades_B1"] = [["T1"], ["T2"]]


def test_transfer_replaces_dimension_filters_with_transfer_details():
    at = _load()
    at.session_state["wiz"] = {**at.session_state["wiz"],
                               "category": "Scaling Adjustment",
                               "adjustment_type": "Transfer",
                               "process_types": ["VaR"], "process_type": "VaR"}
    at.run()
    assert not at.exception, at.exception
    texts = " ".join(m.value for m in at.markdown)
    assert "Transfer Details" in texts
    assert "Dimension Filters" not in texts
    # The type pill reads with its display label (type_label), not the code.
    labels = [b.label for b in at.button]
    assert "Transfer Book" in labels and "Transfer" not in labels


def test_transfer_type_forces_adhoc_and_hides_the_schedule_pills():
    at = _load()
    at.session_state["wiz"] = {**at.session_state["wiz"],
                               "category": "Scaling Adjustment",
                               "adjustment_type": "Scale", "occurrence": "RECURRING",
                               "process_types": ["VaR"], "process_type": "VaR"}
    at.run()
    assert not at.exception, at.exception
    at.button(key=f"type_Transfer_{at.session_state['_wiz_v']}").click().run()
    assert not at.exception, at.exception
    assert at.session_state["wiz"]["adjustment_type"] == "Transfer"
    assert at.session_state["wiz"]["occurrence"] == "ADHOC"
    assert not any((b.key or "").startswith("freq_") for b in at.button)


def test_switching_away_from_transfer_clears_its_fields():
    at = _load()
    _seed_ref_data(at)
    at.session_state["wiz"] = {**at.session_state["wiz"],
                               "category": "Scaling Adjustment",
                               "adjustment_type": "Transfer",
                               "process_types": ["VaR"], "process_type": "VaR",
                               "source_book_code": "B1", "target_book_code": "B2",
                               "transfer_trade_codes": ["T1"]}
    at.run()
    assert not at.exception, at.exception
    # The form keeps the generic filter keys in step with the TARGET book —
    # the ticket, the checklist and the sign-off gate all read them.
    assert at.session_state["wiz"]["book_code"] == "B2"
    assert at.session_state["wiz"]["entity_code"] == "E2"

    at.button(key=f"type_Scale_{at.session_state['_wiz_v']}").click().run()
    assert not at.exception, at.exception
    w = at.session_state["wiz"]
    assert w["source_book_code"] is None and w["target_book_code"] is None
    assert w["transfer_trade_codes"] == []
    # Cleared (the dimension dropdowns re-render them as "" — their own
    # not-selected value); the point is that the target book does NOT ride
    # into the Scale, which is what happens if the widget state survives.
    assert not w["book_code"] and not w["entity_code"]


def _submit_payloads(at):
    """The payload dict of every SP_SUBMIT_ADJUSTMENT call made."""
    out = []
    for c in CALLS:
        if "SP_SUBMIT_ADJUSTMENT" not in c:
            continue
        raw = c[c.index("('") + 2:c.rindex("')")]
        # _submit_one escapes for a Snowflake literal: backslashes doubled
        # first, then single quotes doubled — undo in reverse order.
        out.append(json.loads(raw.replace("''", "'").replace("\\\\", "\\")))
    return out


def test_transfer_with_two_trades_submits_once_per_trade():
    at = _load()
    _seed_ref_data(at)
    at.session_state["wiz"] = {**at.session_state["wiz"],
                               "category": "Scaling Adjustment",
                               "adjustment_type": "Transfer",
                               "process_types": ["VaR"], "process_type": "VaR",
                               "cobid": 20260101,
                               "source_book_code": "B1", "target_book_code": "B2",
                               "transfer_trade_codes": ["T1", "T2"],
                               "adjustment_category": "Cat", "reason": "why",
                               "result": None, "step": 1}
    at.run()
    assert not at.exception, at.exception
    assert at.session_state["wiz"]["transfer_trade_codes"] == ["T1", "T2"]

    CALLS.clear()
    at.button(key=f"submit_{at.session_state['_wiz_v']}").click().run()
    assert not at.exception, at.exception

    payloads = _submit_payloads(at)
    assert len(payloads) == 2
    for p in payloads:
        assert p["adjustment_type"] == "Transfer"
        assert p["source_book_code"] == "B1"
        assert p["book_code"] == "B2"
        assert p["process_type"] == "VaR"
        assert p["source_cobid"] == p["cobid"] == 20260101
        assert p["adjustment_occurrence"] == "ADHOC"
        # Every other dimension filter is stripped for a Transfer.
        assert "entity_code" not in p and "department_code" not in p
    assert [p["trade_code"] for p in payloads] == ["T1", "T2"]

    res = at.session_state["wiz"]["result"]
    assert res["fanout"] is True and res["created"] == 2


def test_transfer_whole_book_submits_once_without_a_trade_code():
    at = _load()
    _seed_ref_data(at)
    at.session_state["wiz"] = {**at.session_state["wiz"],
                               "category": "Scaling Adjustment",
                               "adjustment_type": "Transfer",
                               "process_types": ["VaR"], "process_type": "VaR",
                               "cobid": 20260101,
                               "source_book_code": "B1", "target_book_code": "B2",
                               "transfer_trade_codes": [],
                               "adjustment_category": "Cat", "reason": "why",
                               "result": None, "step": 1}
    at.run()
    assert not at.exception, at.exception

    CALLS.clear()
    at.button(key=f"submit_{at.session_state['_wiz_v']}").click().run()
    assert not at.exception, at.exception

    payloads = _submit_payloads(at)
    assert len(payloads) == 1
    assert "trade_code" not in payloads[0]
    assert payloads[0]["scale_factor"] == 1
    # A single job is NOT a fan-out — the success screen keeps its singular
    # headline.
    assert not (at.session_state["wiz"]["result"] or {}).get("fanout")


def _transfer_wiz(at, **over):
    base = {**at.session_state["wiz"],
            "category": "Scaling Adjustment", "adjustment_type": "Transfer",
            "process_types": ["VaR"], "process_type": "VaR",
            "cobid": 20260101,
            "source_book_code": "B1", "target_book_code": "B2",
            "transfer_trade_codes": ["T1", "T2"],
            "adjustment_category": "Cat", "reason": "why",
            "result": None, "step": 1}
    at.session_state["wiz"] = {**base, **over}


def test_transfer_book_dropdowns_offer_current_books_labelled_by_department():
    """Only IS_CURRENT_ROW books may be transferred (SP_SUBMIT rejects a
    closed book), and each option reads "<book> — <department>"."""
    at = _load()
    _seed_ref_data(at)
    # A historical-only book sits in the every-row cache but NOT in the
    # current one — it must not be offered by the Transfer dropdowns.
    at.session_state["_ref_books_v2"] = (list(at.session_state["_ref_books_v2"])
                                         + [["BOLD", "D9", "E9"]])
    _transfer_wiz(at)
    at.run()
    assert not at.exception, at.exception

    src = at.selectbox(key=f"trf_src_book_{at.session_state['_wiz_v']}")
    assert "BOLD" not in src.options
    assert src.value == "B1"                       # stored value = bare code
    assert src.format_func("B1") == "B1 — D1"      # label carries the dept
    tgt = at.selectbox(key=f"trf_tgt_book_{at.session_state['_wiz_v']}")
    assert "B1" not in tgt.options                 # never transfer onto itself
    assert tgt.format_func("B2") == "B2 — D2"


def test_transfer_preview_payload_carries_trade_codes_and_no_filters():
    at = _load()
    _seed_ref_data(at)
    _transfer_wiz(at, entity_code="E9", department_code="D9")
    at.run()
    assert not at.exception, at.exception

    CALLS.clear()
    at.button(key=f"run_preview_{at.session_state['_wiz_v']}").click().run()
    assert not at.exception, at.exception

    prev = [c for c in CALLS if "SP_PREVIEW_ADJUSTMENT" in c]
    assert prev, CALLS
    raw = prev[0][prev[0].index("('") + 2:prev[0].rindex("')")]
    pj = json.loads(raw.replace("''", "'").replace("\\\\", "\\"))
    assert pj["adjustment_type"] == "Transfer"
    assert pj["trade_codes"] == ["T1", "T2"]
    assert pj["source_book_code"] == "B1" and pj["book_code"] == "B2"
    assert pj["source_cobid"] == pj["cobid"] == 20260101
    assert pj["scale_factor"] == 1
    # The TARGET entity rides along (derived from the target book, B2 → E9
    # is overwritten with E2): the preview's "current" side must predicate on
    # the entity exactly as the engine's leg ③ flatten does.
    assert pj["entity_code"] == "E2"
    # Every OTHER stale filter from an earlier draft is still dropped — a
    # transfer is scoped by its two books, never by a leftover department.
    assert "department_code" not in pj
    assert not (set(pj) - {"cobid", "process_type", "adjustment_type",
                           "source_cobid", "scale_factor", "book_code",
                           "entity_code", "source_book_code", "trade_codes",
                           "mode"})


def test_transfer_has_no_scale_factor_field_or_checklist_row():
    at = _load()
    _seed_ref_data(at)
    _transfer_wiz(at)
    at.run()
    assert not at.exception, at.exception
    assert not any((t.key or "").startswith("sf_") for t in at.text_input)
    texts = " ".join(m.value for m in at.markdown)
    assert "Scale Factor" not in texts


def test_target_equal_to_source_fails_the_checklist_case_insensitively():
    at = _load()
    _seed_ref_data(at)
    _transfer_wiz(at, source_book_code="B1", target_book_code="b1",
                  transfer_trade_codes=[])
    at.run()
    assert not at.exception, at.exception
    texts = " ".join(m.value for m in at.markdown)
    assert "Target differs from source" in texts          # checklist row shown
    # An incomplete ticket keeps Submit locked and says so.
    assert any("Submit unlocks when the ticket is complete" in c.value
               for c in at.caption), [c.value for c in at.caption]


def test_wide_transfer_requires_a_count_confirmation_before_submit():
    at = _load()
    _seed_ref_data(at)
    _trades = [f"T{i}" for i in range(1, 26)]            # 25 > the 20 threshold
    at.session_state["_ref_trades_B1"] = [[t] for t in _trades]
    _transfer_wiz(at, transfer_trade_codes=_trades)
    at.run()
    assert not at.exception, at.exception

    box = next((c for c in at.checkbox
                if (c.key or "").startswith("fanout_confirm_")), None)
    assert box is not None, [c.key for c in at.checkbox]
    assert box.label == "I understand this will create 25 adjustments"
    assert at.button(key=f"submit_{at.session_state['_wiz_v']}").disabled

    box.check().run()
    assert not at.exception, at.exception
    assert not at.button(key=f"submit_{at.session_state['_wiz_v']}").disabled


def test_narrow_transfer_has_no_count_confirmation():
    at = _load()
    _seed_ref_data(at)
    _transfer_wiz(at)                                   # 1 scope × 2 trades
    at.run()
    assert not at.exception, at.exception
    assert not any((c.key or "").startswith("fanout_confirm_")
                   for c in at.checkbox)


def test_leaving_the_category_clears_the_transfer_draft():
    at = _load()
    _seed_ref_data(at)
    _transfer_wiz(at)
    at.run()
    assert not at.exception, at.exception
    assert at.session_state["wiz"]["book_code"] == "B2"

    btn = next(b for b in at.button
               if (b.key or "").startswith("cat_Direct Adjustment_"))
    btn.click().run()
    assert not at.exception, at.exception
    w = at.session_state["wiz"]
    assert w["source_book_code"] is None and w["target_book_code"] is None
    assert w["transfer_trade_codes"] == []
    assert not w["book_code"] and not w["entity_code"]


def test_transfer_ticket_shows_the_books_and_trade_count():
    at = _load()
    _seed_ref_data(at)
    at.session_state["wiz"] = {**at.session_state["wiz"],
                               "category": "Scaling Adjustment",
                               "adjustment_type": "Transfer",
                               "process_types": ["VaR"], "process_type": "VaR",
                               "cobid": 20260101,
                               "source_book_code": "B1", "target_book_code": "B2",
                               "transfer_trade_codes": ["T1"]}
    at.run()
    assert not at.exception, at.exception
    texts = " ".join(m.value for m in at.markdown)
    assert "B1 → B2" in texts
    assert "1 selected" in texts          # Trades row
    assert "Transfer Book" in texts       # Type row uses the display label
    assert "Source book" in texts and "Target book" in texts   # checklist


def grp_options(at, suffix=""):
    """Options of the scope pill group (a ButtonGroup). The restricted
    Transfer list renders under its own key — "..._ltd" — so Streamlit is
    never handed a stored selection that is no longer in `options`."""
    grp = at.button_group(key=f"scopes_Scaling Adjustment{suffix}_"
                              f"{at.session_state['_wiz_v']}")
    return list(grp.options)


def test_transfer_offers_all_six_scopes():
    """The v1 FRTB restriction is lifted: Transfer Book offers every scope,
    same as any other adjustment type. FRTB scopes get a new FRTBSA_*_KEY
    per transferred row (source key + target book + resolved trade), so
    leg ②T no longer collides — see _transfer_col in
    new_adjustment_db_objects/05_sp_process_adjustment.sql. No scope is
    dropped and no warning is shown, so the pill group renders under its
    plain (non "_ltd") widget key, same as every other adjustment type."""
    at = _load()
    _seed_ref_data(at)
    at.session_state["wiz"] = {**at.session_state["wiz"],
                               "category": "Scaling Adjustment",
                               "adjustment_type": "Transfer",
                               "process_types": ["VaR", "FRTB"],
                               "process_type": "VaR"}
    at.run()
    assert not at.exception, at.exception

    # ButtonGroup options come back as rendered labels, not bare codes.
    opts = " ".join(str(o) for o in grp_options(at))
    assert "VaR" in opts and "Stress" in opts and "Sensitivity" in opts
    assert "FRTB" in opts
    assert len(grp_options(at)) == 6
    assert at.session_state["wiz"]["process_types"] == ["VaR", "FRTB"]
    assert not any("not available for Transfer Book" in w.value for w in at.warning)


def test_non_transfer_types_still_offer_every_scope():
    """Every adjustment type — Transfer included — offers the full pill
    group under its original (non "_ltd") widget key; there is currently no
    scope restriction anywhere in the form."""
    at = _load()
    at.session_state["wiz"] = {**at.session_state["wiz"],
                               "category": "Scaling Adjustment",
                               "adjustment_type": "Scale",
                               "process_types": ["FRTB"], "process_type": "FRTB"}
    at.run()
    assert not at.exception, at.exception
    assert len(grp_options(at)) == 6
    assert at.session_state["wiz"]["process_types"] == ["FRTB"]
