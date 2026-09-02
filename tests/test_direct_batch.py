"""
DIR — Direct batch submit (UAT plan §3, the automatable core).

DIR-07 regression: 120 staged rows must submit in seconds via
SP_SUBMIT_DIRECT_BATCH (deterministic UUID-v5 ids, no temp tables), with one
header per valid row. Values are configurable via env (TEST_DIR_*) if the
validation views in your environment require real reference codes.
"""
import json
import os
import time
import uuid

import pytest

from conftest import (FAKE_COB, U_SUBMIT, STAGE_BATCH_PREFIX,
                      ADJ_ID_NAMESPACE, call_sp, esc)

N_ROWS = 120

ROW_DEFAULTS = {
    "ENTITY_CODE": os.environ.get("TEST_DIR_ENTITY", "MUSI"),
    "SOURCE_SYSTEM_CODE": os.environ.get("TEST_DIR_SOURCE_SYSTEM", "QP"),
    "DEPARTMENT_CODE": os.environ.get("TEST_DIR_DEPARTMENT", "TRD"),
    "BOOK_CODE": os.environ.get("TEST_DIR_BOOK", "UATBOOK"),
    "TRADE_CODE": "UAT-TRADE",
    "TRADE_TYPOLOGY": "IRS",
    "STRATEGY": "UAT",
    "INSTRUMENT_CODE": "UAT-INSTR",
    "SIMULATION_NAME": "UAT",
    "SIMULATION_SOURCE": "UAT",
    "MEASURE_TYPE_CODE": os.environ.get("TEST_DIR_MEASURE_TYPE", "PL"),
    "CURRENCY_CODE": "USD",
    "TENOR_CODE": "1Y",
    "UNDERLYING_TENOR_CODE": "1Y",
    "CURVE_CODE": "UAT",
    "PRODUCT_CATEGORY_ATTRIBUTES": "UAT",
    "REASON": "UAT automation batch",
}


@pytest.mark.uat("DIR-07", title="120-row Direct batch submits in seconds", priority="P1")
def test_dir07_batch_submit_speed(session, ev):
    batch_id = STAGE_BATCH_PREFIX + str(uuid.uuid4())[:23]
    cols = ["BATCH_ID", "ROW_NUM"] + list(ROW_DEFAULTS) + ["VALUE_USD", "USERNAME"]
    values = []
    for i in range(1, N_ROWS + 1):
        vals = [f"'{batch_id}'", str(i)]
        vals += [f"'{esc(v)}'" for v in ROW_DEFAULTS.values()]
        vals += [f"'{100 + i}.50'", f"'{U_SUBMIT}'"]
        values.append("(" + ", ".join(vals) + ")")
    session.sql(
        f"INSERT INTO ADJUSTMENT_APP.ADJ_DIRECT_STAGE ({', '.join(cols)}) "
        f"VALUES {', '.join(values)}").collect()
    ev.note("Staged", f"{N_ROWS} rows under batch {batch_id}")

    payload = json.dumps({
        "batch_id": batch_id, "cobid": FAKE_COB, "process_type": "Stress",
        "username": U_SUBMIT, "requires_approval": True,
        "reason": "UAT automation — DIR-07 batch regression",
    })
    t0 = time.time()
    res = call_sp(session, "ADJUSTMENT_APP.SP_SUBMIT_DIRECT_BATCH", payload)
    elapsed = time.time() - t0
    ev.note("Submit result", f"{str(res)[:300]} — elapsed {elapsed:.1f}s")

    ev.check("no temp-table / statement-type error",
             "TEMPORARY TABLE" not in str(res).upper())
    ev.check(f"submit completed in seconds ({elapsed:.1f}s < 60s)", elapsed < 60)

    created = res.get("created") if isinstance(res, dict) else None
    h = ev.sql("Headers created for the batch",
               f"""SELECT COUNT(*) AS N FROM ADJUSTMENT_APP.ADJ_HEADER
                   WHERE COBID = {FAKE_COB} AND REASON LIKE '%DIR-07%'""")
    n_headers = int(h[0]["N"]) if h else 0
    ev.note("Counts", f"SP reported created={created}; headers found={n_headers}")
    ev.check("headers were created (validation accepted the batch — if 0, "
             "check TEST_DIR_* env values against your reference data)",
             n_headers > 0)
    ev.check("SP 'created' count matches headers on the table",
             created == n_headers)

    # Deterministic ids: row 1's ADJ_ID must equal UUID_STRING(ns, batch:1).
    d = ev.sql("Deterministic UUID check (row 1)",
               f"""SELECT UUID_STRING('{ADJ_ID_NAMESPACE}', '{batch_id}:1') AS EXPECTED,
                          (SELECT COUNT(*) FROM ADJUSTMENT_APP.ADJ_HEADER
                           WHERE ADJ_ID = UUID_STRING('{ADJ_ID_NAMESPACE}', '{batch_id}:1')) AS FOUND""")
    if n_headers == N_ROWS:
        ev.check("row 1 header id is the deterministic UUID-v5",
                 bool(d) and int(d[0]["FOUND"]) == 1)
