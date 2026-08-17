-- =============================================================================
-- 14_SP_SUBMIT_DIRECT_BATCH.SQL
-- Set-based submit for Direct Adjustment CSV batches.
--
-- The per-row path (one CALL SP_SUBMIT_ADJUSTMENT per CSV row) runs ~10
-- statements per row — a 120-row file took minutes. This procedure creates
-- every header from the staged batch in a fixed number of statements
-- (~10 total, independent of row count) while enforcing the SAME rules as
-- the single-row path for a Direct submission:
--   • scope must be active (ADJUSTMENTS_SETTINGS)
--   • sign-off checked per row entity (blocked rows are still inserted with
--     RUN_STATUS 'Rejected - SignedOff' for audit, exactly like single-row)
--   • requires_approval → 'Pending Approval', else 'Pending'
--   • one ADJ_STATUS_HISTORY row per header
--   • one approval notification per batch (not per row)
-- Direct submissions skip overlap blocking and GLOBAL_REFERENCE replacement
-- in the single-row path too — nothing is lost by not porting those.
--
-- Rows are taken from ADJ_DIRECT_STAGE joined to the scope's validation view
-- (VW_DIRECT_VALIDATE_<SCOPE>) — the same verdicts the app displayed — and
-- the stage batch is consumed (deleted) in the same transaction, so the
-- whole submission is atomic: every valid row becomes a header, or none do.
--
-- Streamlit calls:
--   CALL ADJUSTMENT_APP.SP_SUBMIT_DIRECT_BATCH('{ ... JSON ... }');
-- JSON keys: batch_id (required), cobid (required), process_type (required),
--   username, reason, requires_approval, adjustment_category
-- =============================================================================

USE SCHEMA ADJUSTMENT_APP;

CREATE OR ALTER PROCEDURE ADJUSTMENT_APP.SP_SUBMIT_DIRECT_BATCH(p_batch VARCHAR)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
COMMENT = 'Set-based Direct Adjustment submit: staged batch -> one header per valid row in a fixed number of statements, same rules as SP_SUBMIT_ADJUSTMENT (scope active, per-entity sign-off, approval status, audit).'
EXECUTE AS CALLER
AS
$$
import json

STATUS_PENDING        = "Pending"
STATUS_PENDING_APPROV = "Pending Approval"
STATUS_REJECTED_SO    = "Rejected - SignedOff"

# Stage columns that map 1:1 to ADJ_HEADER columns (same list the app's
# _direct_row_payload sends per row on the single-row path).
STAGE_TO_HEADER = [
    "ENTITY_CODE", "SOURCE_SYSTEM_CODE", "DEPARTMENT_CODE", "BOOK_CODE",
    "TRADE_CODE", "TRADE_TYPOLOGY", "STRATEGY", "INSTRUMENT_CODE",
    "SIMULATION_NAME", "SIMULATION_SOURCE", "MEASURE_TYPE_CODE",
    "CURRENCY_CODE", "TENOR_CODE", "CURVE_CODE", "PRODUCT_CATEGORY_ATTRIBUTES",
]


def _esc(v):
    return str(v).replace("'", "''") if v is not None else ""


def _app_cfg(session, key, default=""):
    rows = session.sql(f"""
        SELECT CONFIG_VALUE FROM ADJUSTMENT_APP.ADJ_APP_CONFIG
        WHERE CONFIG_KEY = '{_esc(key)}'
    """).collect()
    v = rows[0]["CONFIG_VALUE"] if rows else None
    return str(v) if v is not None else default


def check_signoff(session, process_type, cobid, entity_code=None):
    """Same rules as SP_SUBMIT_ADJUSTMENT.check_signoff — see 03 for the full
    lifecycle documentation. Called once per DISTINCT entity in the batch."""
    pt_esc = _esc(process_type).upper()
    ent = str(entity_code).strip() if entity_code and str(entity_code).strip() else None
    BLOCKED = ("SIGNED_OFF", "REOPEN_REQUESTED")

    app_rows = session.sql(f"""
        SELECT UPPER(COALESCE(ENTITY_CODE, '*')) AS E, UPPER(SIGN_OFF_STATUS) AS S
        FROM ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS
        WHERE COBID = {int(cobid)} AND UPPER(PROCESS_TYPE) = '{pt_esc}'
    """).collect()
    app = {str(r["E"]): str(r["S"]) for r in app_rows}

    if ent is not None:
        s = app.get(ent.upper())
        if s in (None, "OPEN"):
            s = app.get("*")
        if s == "REOPENED":
            return False
        if s in BLOCKED:
            return True
    else:
        if any(s in BLOCKED for s in app.values()):
            return True

    if _app_cfg(session, "SIGNOFF_FEED_ENABLED", "true").strip().lower() != "true":
        return False
    feed = _app_cfg(session, "SIGNOFF_FEED_TABLE",
                    "BATCH.PUBLISH_SIGNOFF_STATUS").strip()
    if pt_esc in ("FRTB", "FRTBDRC", "FRTBRRAO"):
        pt_match = f"UPPER(u.PROCESS_TYPE) IN ('FRTB', '{pt_esc}')"
    else:
        pt_match = f"UPPER(u.PROCESS_TYPE) = '{pt_esc}'"
    ent_pred = (f"AND UPPER(u.ENTITY_CODE) = UPPER('{_esc(ent)}')"
                if ent else "")
    upstream = session.sql(f"""
        SELECT 1
        FROM {feed} u
        WHERE u.COBID = {int(cobid)}
          AND {pt_match}
          AND (u.SUB_TYPE IS NULL OR TRIM(u.SUB_TYPE) = ''
               OR UPPER(u.SUB_TYPE) = 'NONCVA')
          AND UPPER(u.PUBLISH_STATUS) = 'SIGNEDOFF'
          {ent_pred}
          AND NOT EXISTS (
              SELECT 1 FROM ADJUSTMENT_APP.ADJ_SIGNOFF_STATUS a
              WHERE a.COBID = u.COBID
                AND UPPER(a.PROCESS_TYPE) = '{pt_esc}'
                AND UPPER(a.SIGN_OFF_STATUS) = 'REOPENED'
                AND (UPPER(a.ENTITY_CODE) = UPPER(u.ENTITY_CODE)
                     OR a.ENTITY_CODE = '*')
          )
        LIMIT 1
    """).collect()
    return bool(upstream)


def main(session, p_batch):
    txn_open = False
    try:
        req = json.loads(p_batch) if isinstance(p_batch, str) else p_batch

        batch_id     = str(req["batch_id"])
        cobid        = int(req["cobid"])
        process_type = str(req["process_type"])

        username = req.get("username")
        if not username or not str(username).strip():
            try:
                row = session.sql("SELECT CURRENT_USER() AS U").collect()
                username = row[0]["U"] if row and row[0]["U"] else None
            except Exception:
                pass
        if not username or not str(username).strip():
            return {"status": "Error", "created": 0,
                    "message": "username could not be resolved."}

        reason            = str(req.get("reason") or "")
        requires_approval = bool(req.get("requires_approval", False))
        category          = req.get("adjustment_category")
        ok_status = STATUS_PENDING_APPROV if requires_approval else STATUS_PENDING

        # ── Scope must be active ─────────────────────────────────────────
        settings = session.sql(f"""
            SELECT 1 FROM ADJUSTMENT_APP.ADJUSTMENTS_SETTINGS
            WHERE UPPER(PROCESS_TYPE) = UPPER('{_esc(process_type)}')
              AND IS_ACTIVE = TRUE
        """).collect()
        if not settings:
            return {"status": "Error", "created": 0,
                    "message": f"Scope '{process_type}' is not active or not configured."}

        view = f"ADJUSTMENT_APP.VW_DIRECT_VALIDATE_{process_type.upper()}"
        b_esc = _esc(batch_id)

        # ── Sign-off verdict per DISTINCT entity in the batch ────────────
        # (usually 1-3 entities per file — this is the only per-value work).
        ents = session.sql(f"""
            SELECT DISTINCT UPPER(COALESCE(TRIM(s.ENTITY_CODE), '')) AS E
            FROM ADJUSTMENT_APP.ADJ_DIRECT_STAGE s
            JOIN {view} v ON v.BATCH_ID = s.BATCH_ID AND v.ROW_NUM = s.ROW_NUM
            WHERE s.BATCH_ID = '{b_esc}' AND v.IS_VALID
        """).collect()
        if not ents:
            return {"status": "Error", "created": 0,
                    "message": "No valid staged rows found for this batch — "
                               "parse the CSV again."}
        NULL_ENT = "#NULL#"   # marker for the no-entity (broad) rows in the CASE
        blocked = []
        for r in ents:
            e = str(r["E"])
            if check_signoff(session, process_type, cobid, e or None):
                blocked.append(e or NULL_ENT)
        if blocked:
            in_list = ", ".join(f"'{_esc(e)}'" for e in blocked)
            status_expr = (f"CASE WHEN UPPER(COALESCE(NULLIF(TRIM(s.ENTITY_CODE), "
                           f"''), '{NULL_ENT}')) IN ({in_list}) "
                           f"THEN '{STATUS_REJECTED_SO}' "
                           f"ELSE '{_esc(ok_status)}' END")
        else:
            status_expr = f"'{_esc(ok_status)}'"

        cat_expr = f"'{_esc(category)}'" if category else "NULL"
        # Per-row REASON wins over the shared wizard reason (single-row parity).
        reason_expr = (f"COALESCE(NULLIF(TRIM(s.REASON), ''), "
                       f"'{_esc(reason)}')")

        stage_cols = ", ".join(f"NULLIF(TRIM(s.{c}), '') AS {c}"
                               for c in STAGE_TO_HEADER)

        # ── Stage → header rows, atomically ──────────────────────────────
        # Temp table first so the header insert, the audit insert and the
        # result counts all read the SAME row set (incl. generated ADJ_IDs).
        # It is created BEFORE the BEGIN on purpose: CREATE TABLE is DDL and
        # DDL inside an explicit Snowflake transaction commits it — creating
        # it inside the BEGIN would silently break the batch's atomicity.
        session.sql(f"""
            CREATE OR REPLACE TEMPORARY TABLE ADJUSTMENT_APP.TMP_DIRECT_BATCH_SUBMIT AS
            SELECT
                UUID_STRING()                        AS ADJ_ID,
                s.ROW_NUM                            AS ROW_NUM,
                TRY_TO_NUMBER(s.VALUE_USD, 38, 6)    AS ADJUSTMENT_VALUE_IN_USD,
                {reason_expr}                        AS REASON,
                {status_expr}                        AS RUN_STATUS,
                {stage_cols}
            FROM ADJUSTMENT_APP.ADJ_DIRECT_STAGE s
            JOIN {view} v ON v.BATCH_ID = s.BATCH_ID AND v.ROW_NUM = s.ROW_NUM
            WHERE s.BATCH_ID = '{b_esc}' AND v.IS_VALID
        """).collect()

        session.sql("BEGIN").collect()
        txn_open = True

        header_stage_cols = ", ".join(STAGE_TO_HEADER)
        session.sql(f"""
            INSERT INTO ADJUSTMENT_APP.ADJ_HEADER (
                ADJ_ID, COBID, PROCESS_TYPE, ADJUSTMENT_TYPE, ADJUSTMENT_ACTION,
                SOURCE_COBID, SCALE_FACTOR, SCALE_FACTOR_ADJUSTED,
                ADJUSTMENT_VALUE_IN_USD, ADJUSTMENT_CATEGORY, REASON,
                RUN_STATUS, IS_POSITIVE_ADJUSTMENT, IS_DELETED, USERNAME,
                ADJUSTMENT_OCCURRENCE, {header_stage_cols}
            )
            SELECT
                t.ADJ_ID, {cobid}, '{_esc(process_type)}', 'Direct', 'Direct',
                {cobid}, 1.0, 0.0,
                t.ADJUSTMENT_VALUE_IN_USD, {cat_expr}, t.REASON,
                t.RUN_STATUS, TRUE, FALSE, '{_esc(username)}',
                'ADHOC', {header_stage_cols}
            FROM ADJUSTMENT_APP.TMP_DIRECT_BATCH_SUBMIT t
        """).collect()

        session.sql(f"""
            INSERT INTO ADJUSTMENT_APP.ADJ_STATUS_HISTORY
                (ADJ_ID, OLD_STATUS, NEW_STATUS, CHANGED_BY, COMMENT)
            SELECT t.ADJ_ID, NULL, t.RUN_STATUS, '{_esc(username)}',
                   'Submitted via Streamlit — Direct / {_esc(process_type)} (batch)'
            FROM ADJUSTMENT_APP.TMP_DIRECT_BATCH_SUBMIT t
        """).collect()

        counts = session.sql("""
            SELECT RUN_STATUS, COUNT(*) AS N,
                   ARRAY_AGG(ROW_NUM) WITHIN GROUP (ORDER BY ROW_NUM) AS ROW_NUMS
            FROM ADJUSTMENT_APP.TMP_DIRECT_BATCH_SUBMIT
            GROUP BY RUN_STATUS
        """).collect()

        # Consume the staged batch in the same transaction.
        session.sql(f"""
            DELETE FROM ADJUSTMENT_APP.ADJ_DIRECT_STAGE
            WHERE BATCH_ID = '{b_esc}'
        """).collect()

        session.sql("COMMIT").collect()
        txn_open = False

        n_ok, n_rej, rej_rows = 0, 0, []
        for r in counts:
            if str(r["RUN_STATUS"]) == STATUS_REJECTED_SO:
                n_rej += int(r["N"])
                rej_rows = (json.loads(r["ROW_NUMS"])
                            if isinstance(r["ROW_NUMS"], str)
                            else list(r["ROW_NUMS"]))
            else:
                n_ok += int(r["N"])

        # One notification for the whole batch (not one per row).
        if n_ok and ok_status == STATUS_PENDING_APPROV:
            try:
                _np = json.dumps({
                    "process_type":    process_type,
                    "cobid":           cobid,
                    "adjustment_type": "Direct",
                    "submitted_by":    username,
                }).replace("'", "''")
                session.sql(
                    f"CALL ADJUSTMENT_APP.SP_NOTIFY('approval_pending', '{_np}')"
                ).collect()
            except Exception as notify_err:
                print(f"Approval notification skipped (non-fatal): {notify_err}")

        return {"status": ok_status if n_ok else STATUS_REJECTED_SO,
                "created": n_ok, "rejected_signoff": n_rej,
                "rejected_rows": rej_rows[:50],
                "message": (f"Created {n_ok} Direct adjustment(s) with status "
                            f"'{ok_status}'."
                            + (f" {n_rej} row(s) rejected by sign-off."
                               if n_rej else ""))}

    except KeyError as ke:
        if txn_open:
            try:
                session.sql("ROLLBACK").collect()
            except Exception:
                pass
        return {"status": "Error", "created": 0,
                "message": f"Missing required field: {ke}"}
    except Exception as e:
        if txn_open:
            try:
                session.sql("ROLLBACK").collect()
            except Exception:
                pass
        return {"status": "Error", "created": 0,
                "message": f"Batch submission failed (no rows were created): {str(e)}"}
$$;

-- ═══════════════════════════════════════════════════════════════════════════
-- VERIFY
-- ═══════════════════════════════════════════════════════════════════════════
DESCRIBE PROCEDURE ADJUSTMENT_APP.SP_SUBMIT_DIRECT_BATCH(VARCHAR);
