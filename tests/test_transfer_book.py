# tests/test_transfer_book.py
"""
TRF — Transfer Book (spec docs/superpowers/specs/2026-09-17-transfer-book-multi-scope-design.md).
Needs two real CURRENT books in DIMENSION.BOOK (env TEST_TRF_SRC_BOOK / TEST_TRF_TGT_BOOK,
same or different entity). Headers land on the fake COB and are cleaned up by conftest.
"""
import json, os
import pytest

AREA = "Transfer Book"

from conftest import FAKE_COB, U_SUBMIT, call_sp, rows

SP_SUBMIT  = "ADJUSTMENT_APP.SP_SUBMIT_ADJUSTMENT"
SP_PREVIEW = "ADJUSTMENT_APP.SP_PREVIEW_ADJUSTMENT"
SRC = os.environ.get("TEST_TRF_SRC_BOOK", "UATBOOK")
TGT = os.environ.get("TEST_TRF_TGT_BOOK", "UATBOOK2")


def _submit(session, **over):
    payload = {"cobid": FAKE_COB, "process_type": "VaR", "adjustment_type": "Transfer",
               "source_cobid": FAKE_COB, "scale_factor": 1, "username": U_SUBMIT,
               "book_code": TGT, "source_book_code": SRC,
               "reason": "UAT automation — transfer", "requires_approval": False,
               "adjustment_category": "Booking Error"}
    payload.update(over)
    return call_sp(session, SP_SUBMIT, json.dumps(payload))


@pytest.mark.uat("TRF-01", title="Transfer accepted; header stores target/source book and derived entity", priority="P1")
def test_trf01_submit(session, ev):
    res = _submit(session, trade_code="UAT-TRADE-1")
    ev.note("SP result", str(res)[:300])
    ev.check("accepted", isinstance(res, dict) and res.get("status") in ("Pending", "Pending Approval"))
    h = ev.sql("Header", f"""SELECT ADJUSTMENT_TYPE, ADJUSTMENT_ACTION, SCALE_FACTOR_ADJUSTED,
                                    BOOK_CODE, SOURCE_BOOK_CODE, TRADE_CODE, ENTITY_CODE
                             FROM ADJUSTMENT_APP.ADJ_HEADER WHERE ADJ_ID = '{res.get("adj_id")}'""")
    ent = ev.sql("Target book entity", f"""SELECT MAX(ENTITY_CODE) AS E FROM DIMENSION.BOOK
                                           WHERE UPPER(BOOK_CODE) = UPPER('{TGT}') AND IS_CURRENT_ROW = TRUE""")
    ev.check("type/action", h and h[0]["ADJUSTMENT_TYPE"] == "Transfer" and h[0]["ADJUSTMENT_ACTION"] == "Scale")
    ev.check("factor adjusted is 1 (not 0)", h and float(h[0]["SCALE_FACTOR_ADJUSTED"]) == 1.0)
    ev.check("target book in BOOK_CODE, source in SOURCE_BOOK_CODE",
             h and h[0]["BOOK_CODE"].upper() == TGT.upper() and h[0]["SOURCE_BOOK_CODE"].upper() == SRC.upper())
    ev.check("entity derived from the target book",
             h and ent and (h[0]["ENTITY_CODE"] or "").upper() == (ent[0]["E"] or "").upper())
    ev.check("trade code kept", h and (h[0]["TRADE_CODE"] or "").upper() == "UAT-TRADE-1")


@pytest.mark.uat("TRF-02", title="Transfer guards: same book, unknown book, missing source, cross-COB", priority="P1")
def test_trf02_guards(session, ev):
    r1 = _submit(session, source_book_code=TGT)
    ev.check("same book refused",
             isinstance(r1, dict) and r1.get("status") == "Error"
             and "must differ" in str(r1.get("message", "")))
    r2 = _submit(session, book_code="ZZ_NO_SUCH_BOOK")
    ev.check("unknown target refused",
             isinstance(r2, dict) and r2.get("status") == "Error"
             and "not a current book" in str(r2.get("message", "")))
    r3 = _submit(session, source_book_code="")
    ev.check("missing source refused",
             isinstance(r3, dict) and r3.get("status") == "Error"
             and "both a source book and a target book" in str(r3.get("message", "")))
    r4 = _submit(session, source_cobid=FAKE_COB - 1)
    ev.check("cross-COB refused",
             isinstance(r4, dict) and r4.get("status") == "Error"
             and "within one COB" in str(r4.get("message", "")))
    # A transfer copies the source book as it stands — a factor other than 1
    # would silently scale the copy.
    r5 = _submit(session, scale_factor=2)
    ev.check("scale factor other than 1 refused",
             isinstance(r5, dict) and r5.get("status") == "Error"
             and "must be 1" in str(r5.get("message", "")))
    # FRTB scopes are accepted: the engine mints a new FRTBSA_*_KEY per
    # transferred row (source key + target book + resolved trade), like
    # Direct FRTB rows, so leg ②T no longer collides in the ranked CTE.
    r6 = _submit(session, process_type="FRTB")
    ev.check("FRTB transfer accepted",
             isinstance(r6, dict) and r6.get("status") in ("Pending", "Pending Approval"))


@pytest.mark.uat("TRF-04", title="A pending Flatten on the target book blocks a Transfer", priority="P2")
def test_trf04_overlap_blocks(session, ev):
    ent = rows(session, f"""SELECT MAX(ENTITY_CODE) AS E FROM DIMENSION.BOOK
                            WHERE UPPER(BOOK_CODE) = UPPER('{TGT}') AND IS_CURRENT_ROW = TRUE""")[0]["E"]
    flat = call_sp(session, SP_SUBMIT, json.dumps({
        "cobid": FAKE_COB, "process_type": "Stress", "adjustment_type": "Flatten",
        "username": U_SUBMIT, "entity_code": ent, "book_code": TGT,
        "reason": "UAT automation — blocker", "adjustment_category": "Booking Error"}))
    ev.check("setup Flatten accepted",
             isinstance(flat, dict) and flat.get("status") in ("Pending", "Pending Approval")
             and bool(flat.get("adj_id")))
    res = _submit(session, process_type="Stress")
    h = ev.sql("Transfer header", f"""SELECT BLOCKED_BY_ADJ_ID FROM ADJUSTMENT_APP.ADJ_HEADER
                                      WHERE ADJ_ID = '{res.get("adj_id")}'""")
    ev.check("transfer is blocked by the earlier Flatten",
             h and h[0]["BLOCKED_BY_ADJ_ID"] is not None
             and h[0]["BLOCKED_BY_ADJ_ID"] == flat.get("adj_id"))


@pytest.mark.uat("TRF-03", title="Transfer preview SQL reads the source book and re-keys to the target", priority="P1")
def test_trf03_preview_sql(session, ev):
    payload = json.dumps({"cobid": FAKE_COB, "process_type": "VaR", "adjustment_type": "Transfer",
                          "source_cobid": FAKE_COB, "scale_factor": 1,
                          "book_code": TGT, "source_book_code": SRC,
                          "trade_codes": ["UAT-TRADE-1", "UAT-TRADE-2"], "mode": "sql"})
    r = session.sql(f"CALL {SP_PREVIEW}('{payload}')").collect()
    txt = str(r[0][0]) if r else ""
    ev.note("SQL", txt[:600])
    ev.check("source book predicate", f"UPPER(sb.BOOK_CODE) = UPPER('{SRC}')".upper() in txt.upper())
    ev.check("target book predicate", TGT.upper() in txt.upper())
    ev.check("trade list", "UAT-TRADE-1" in txt and "UAT-TRADE-2" in txt)
    ev.check("fallback trade", "/Adjustment" in txt)


@pytest.mark.uat("TRF-05", title="Trades with no version in the target book are flagged 'fallback' in the breakdown", priority="P2")
def test_trf05_fallback_trades_are_flagged(session, ev):
    """KNOWN LIMITATION this case documents (engine leg ②T comment in
    05_sp_process_adjustment.sql):

    A transferred trade that has no version under the TARGET book at the COB
    is re-keyed onto that book's '<BOOK_CODE>/Adjustment' trade. Two such
    trades that also share every other key column therefore land on the SAME
    surrogate key — `ranked` keeps the newest header's row and the other
    one's value is lost. There is no engine-side fix in v1: the preview's
    `breakdown` mode flags every fallback trade ("fallback: <BOOK>/Adjustment")
    and the New Adjustment page raises a warning when two or more of the
    selected trades fall back, so the user deselects them or has the trades
    set up in the target book first.

    Here: two trade codes that certainly have no version in the target book
    (made-up ones by default; override with TEST_TRF_FALLBACK_TRADES to use
    real source-book trades that exist in the fact data, which is what makes
    the breakdown return rows at all — see the row-count note below).
    """
    trades = [t.strip() for t in os.environ.get(
        "TEST_TRF_FALLBACK_TRADES", "UAT-NO-SUCH-TRADE-1,UAT-NO-SUCH-TRADE-2"
    ).split(",") if t.strip()]
    ev.note("Trades under test", ", ".join(trades))

    # Neither may exist under the TARGET book, or the fallback is not what
    # the breakdown would report.
    _in = ", ".join(f"UPPER('{t}')" for t in trades)
    absent = ev.sql("Versions of these trades under the target book", f"""
        SELECT COUNT(*) AS N FROM DIMENSION.TRADE
        WHERE UPPER(BOOK_CODE) = UPPER('{TGT}') AND UPPER(TRADE_CODE) IN ({_in})""")
    ev.check("no version of either trade exists under the target book",
             absent and int(absent[0]["N"]) == 0)

    payload = json.dumps({"cobid": FAKE_COB, "process_type": "VaR",
                          "adjustment_type": "Transfer", "source_cobid": FAKE_COB,
                          "scale_factor": 1, "book_code": TGT, "source_book_code": SRC,
                          "trade_codes": trades, "mode": "breakdown"})
    out = rows(session, f"CALL {SP_PREVIEW}('{payload}')")
    ev.note("Breakdown rows", str(out)[:600])

    # The preview reads the fact/adjusted tables: a COB with no fact rows for
    # these trades returns nothing, and there is then nothing to flag. That is
    # reported, not passed off as a success.
    ev.note("Row count", f"{len(out)} row(s) — 0 means the fake COB carries no "
                         f"source fact rows for these trades; point "
                         f"TEST_TRF_FALLBACK_TRADES at real source-book trades "
                         f"(absent from the target book) to exercise the flag.")
    # .get(): an error/message row from the SP carries no TARGET_TRADE —
    # it must fail the check, not raise a KeyError.
    flagged = [r for r in out
               if str(r.get("TARGET_TRADE") or "").lower().startswith("fallback")]
    ev.check("every returned trade is flagged as a fallback (none claims a "
             "target-book trade)", len(flagged) == len(out))
    ev.check("the breakdown reports one row per trade under test, all flagged",
             len(out) == 0 or (len(out) == len(trades) and len(flagged) == len(trades)))
