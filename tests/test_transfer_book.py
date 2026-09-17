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


@pytest.mark.uat("TRF-02", title="Transfer guards: same book, unknown book, missing source, cross-COB", priority="P1")
def test_trf02_guards(session, ev):
    r1 = _submit(session, source_book_code=TGT)
    ev.check("same book refused", isinstance(r1, dict) and r1.get("status") == "Error")
    r2 = _submit(session, book_code="ZZ_NO_SUCH_BOOK")
    ev.check("unknown target refused", isinstance(r2, dict) and r2.get("status") == "Error")
    r3 = _submit(session, source_book_code="")
    ev.check("missing source refused", isinstance(r3, dict) and r3.get("status") == "Error")
    r4 = _submit(session, source_cobid=FAKE_COB - 1)
    ev.check("cross-COB refused", isinstance(r4, dict) and r4.get("status") == "Error")


@pytest.mark.uat("TRF-04", title="A pending Flatten on the target book blocks a Transfer", priority="P2")
def test_trf04_overlap_blocks(session, ev):
    ent = rows(session, f"""SELECT MAX(ENTITY_CODE) AS E FROM DIMENSION.BOOK
                            WHERE UPPER(BOOK_CODE) = UPPER('{TGT}') AND IS_CURRENT_ROW = TRUE""")[0]["E"]
    flat = call_sp(session, SP_SUBMIT, json.dumps({
        "cobid": FAKE_COB, "process_type": "Stress", "adjustment_type": "Flatten",
        "username": U_SUBMIT, "entity_code": ent, "book_code": TGT,
        "reason": "UAT automation — blocker", "adjustment_category": "Booking Error"}))
    res = _submit(session, process_type="Stress")
    h = ev.sql("Transfer header", f"""SELECT BLOCKED_BY_ADJ_ID FROM ADJUSTMENT_APP.ADJ_HEADER
                                      WHERE ADJ_ID = '{res.get("adj_id")}'""")
    ev.check("transfer is blocked by the earlier Flatten",
             h and h[0]["BLOCKED_BY_ADJ_ID"] == flat.get("adj_id"))
