"""
Iterative test script for SP_PROCESS_ADJUSTMENT with VaR Flatten.
Keeps running until adjustment data appears in FACT.VAR_MEASURES_ADJUSTMENT.
"""
import json
import time

from mufg_snowflakeconn import sfconnection as m_sf

mufgconn = m_sf.MufgSnowflakeConn('dvlp', 'apd_raptor_sfk_depl@mufgsecurities.com')
session = mufgconn.get_snowflake_session()
session.use_role("DVLP_RAPTOR_OWNER")
session.use_warehouse("DVLP_RAPTOR_WH_XS")
session.use_database("DVLP_RAPTOR_NEWADJ")

print("=" * 70)
print("STEP 0 — Inspect FACT table schemas")
print("=" * 70)

fact_cols = session.sql("SELECT * FROM FACT.VAR_MEASURES LIMIT 0").to_pandas().columns.tolist()
adj_cols  = session.sql("SELECT * FROM FACT.VAR_MEASURES_ADJUSTMENT LIMIT 0").to_pandas().columns.tolist()
print(f"\nFACT.VAR_MEASURES cols ({len(fact_cols)}):\n  {fact_cols}")
print(f"\nFACT.VAR_MEASURES_ADJUSTMENT cols ({len(adj_cols)}):\n  {adj_cols}")

print("\nSettings for VaR:")
s = session.sql("SELECT * FROM ADJUSTMENT_APP.ADJUSTMENTS_SETTINGS WHERE PROCESS_TYPE = 'VaR'").to_pandas()
print(s.to_string(index=False))

print("\n" + "=" * 70)
print("STEP 1 — Check FACT data exists for COBID=20260326, MUSI/QP/Book-RPI")
print("=" * 70)

# Check what ENTITY_CODE, SOURCE_SYSTEM_CODE, BOOK_CODE look like in VAR_MEASURES
# to understand what filter columns are available
filter_cols = [c for c in fact_cols if any(kw in c.upper() for kw in
               ['ENTITY', 'SOURCE_SYSTEM', 'BOOK', 'COBID', 'CODE'])]
print(f"Potential filter columns in FACT.VAR_MEASURES: {filter_cols}")

# Check row count for the COBID
cnt = session.sql("SELECT COUNT(*) AS CNT FROM FACT.VAR_MEASURES WHERE COBID = 20260326").collect()
print(f"\nFACT.VAR_MEASURES rows for COBID=20260326: {cnt[0]['CNT']}")

print("\n" + "=" * 70)
print("STEP 2 — Check ADJ_HEADER column list (what the SP joins on)")
print("=" * 70)
adj_header_cols = session.sql("SELECT * FROM ADJUSTMENT_APP.ADJ_HEADER LIMIT 0").to_pandas().columns.tolist()
join_candidates = [c for c in fact_cols if c in adj_header_cols]
print(f"Columns in BOTH FACT.VAR_MEASURES and ADJ_HEADER:\n  {join_candidates}")

print("\n" + "=" * 70)
print("STEP 3 — Submit test adjustment via SP_SUBMIT_ADJUSTMENT")
print("=" * 70)

adj_payload = {
    "cobid": 20260326,
    "process_type": "VaR",
    "adjustment_type": "Flatten",
    "source_cobid": 20260326,
    "scale_factor": 1.0,
    "entity_code": "MUSI",
    "source_system_code": "QP",
    "book_code": "Book-RPI",
    "reason": "Test Flatten via script",
    "username": "TEST_SCRIPT"
}

print(f"Payload: {json.dumps(adj_payload, indent=2)}")
payload_str = json.dumps(adj_payload).replace("'", "\\'")

try:
    result = session.sql(f"CALL ADJUSTMENT_APP.SP_SUBMIT_ADJUSTMENT('{payload_str}')").collect()
    print(f"Submit result: {result[0][0]}")
    submit_result = json.loads(str(result[0][0])) if result else {}
    adj_id = submit_result.get("adj_id")
    status = submit_result.get("status")
    print(f"ADJ_ID={adj_id}  STATUS={status}")
except Exception as e:
    print(f"Submit exception: {e}")
    adj_id = None

print("\n" + "=" * 70)
print("STEP 4 — Check ADJ_HEADER state after submit")
print("=" * 70)
df_h = session.sql("""
    SELECT ADJ_ID, RUN_STATUS, ADJUSTMENT_TYPE, ADJUSTMENT_ACTION,
           SCALE_FACTOR_ADJUSTED, ENTITY_CODE, SOURCE_SYSTEM_CODE, BOOK_CODE, ERRORMESSAGE
    FROM ADJUSTMENT_APP.ADJ_HEADER
    ORDER BY ADJ_ID DESC LIMIT 3
""").to_pandas()
print(df_h.to_string(index=False))

print("\n" + "=" * 70)
print("STEP 5 — Check FACT.VAR_MEASURES_ADJUSTMENT for new rows")
print("=" * 70)
df_adj_out = session.sql(f"""
    SELECT COUNT(*) AS CNT FROM FACT.VAR_MEASURES_ADJUSTMENT
    WHERE COBID = 20260326
""").to_pandas()
print(f"Rows in VAR_MEASURES_ADJUSTMENT for COBID=20260326: {df_adj_out['CNT'].iloc[0]}")

if adj_id:
    try:
        df_adj_detail = session.sql(f"""
            SELECT * FROM FACT.VAR_MEASURES_ADJUSTMENT
            WHERE ADJUSTMENT_ID = {adj_id}
            LIMIT 5
        """).to_pandas()
        print(f"Rows for ADJ_ID={adj_id}: {len(df_adj_detail)}")
        if not df_adj_detail.empty:
            print(df_adj_detail.to_string(index=False))
    except Exception as e:
        print(f"Could not check by ADJUSTMENT_ID: {e}")

print("\nDone.")
