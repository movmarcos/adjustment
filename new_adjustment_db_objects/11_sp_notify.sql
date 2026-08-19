-- =============================================================================
-- 11_SP_NOTIFY.SQL
-- Central email-notification sender. ALWAYS best-effort: it never raises, so
-- a notification problem can never fail a submission or a processing run.
--
-- Gated by ADJ_APP_CONFIG:
--   NOTIFICATIONS_ENABLED  'true'/'false' — master switch (Admin page). The
--                          proc can be deployed and called BEFORE the email
--                          integration exists; while disabled it only logs
--                          SKIPPED_DISABLED rows.
--   EMAIL_INTEGRATION      name of the DBA-created email notification
--                          integration (see docs/TICKET_email_notification_
--                          integration.md).
--
-- Events (p_event / p_payload JSON):
--   'adjustment_outcome'  {"adj_ids": ["..."], "status": "Processed"|"Failed"}
--       → one email per SUBMITTER (prefs: NOTIFY_MY_OUTCOMES) listing their
--         adjustments in the batch, with error text on failures.
--   'approval_pending'    {"process_type","cobid","adjustment_type","submitted_by"}
--       → one email to the scope's APPROVERS (prefs: NOTIFY_APPROVALS),
--         excluding the submitter (they cannot approve their own).
--   'reopen_requested'    {"process_type","cobid","requested_by","reason"}
--       → one email to the scope's APPROVERS, excluding the requester.
--   'test'                {"email": "..."} → direct test send (Admin page).
--
-- Recipients resolve through ADJ_NOTIFICATION_PREFS (active, opted-in, with
-- an email). Snowflake only delivers to VERIFIED account-user emails.
-- Every attempt (sent, failed, skipped, no recipients) lands in
-- ADJ_NOTIFICATION_LOG.
-- =============================================================================

USE SCHEMA ADJUSTMENT_APP;

CREATE OR ALTER PROCEDURE ADJUSTMENT_APP.SP_NOTIFY(p_event VARCHAR, p_payload VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
COMMENT = 'Best-effort email notifications (never raises). Gated by ADJ_APP_CONFIG.NOTIFICATIONS_ENABLED; sends via the configured email notification integration; logs every attempt to ADJ_NOTIFICATION_LOG.'
EXECUTE AS CALLER
AS
$$
import json
import html

SUBJECT_PREFIX = "[Adjustment App] "


def _esc(v):
    return str(v).replace("\\", "\\\\").replace("'", "''") if v is not None else ""


def _h(v):
    """HTML-escape user-sourced text for the email body."""
    return html.escape(str(v)) if v is not None else ""


def _cfg(session, key, default=""):
    rows = session.sql(f"""
        SELECT CONFIG_VALUE FROM ADJUSTMENT_APP.ADJ_APP_CONFIG
        WHERE CONFIG_KEY = '{_esc(key)}'
    """).collect()
    val = rows[0]["CONFIG_VALUE"] if rows else None
    return str(val) if val is not None else default


def _log(session, event, recipients, subject, status, error=None):
    try:
        session.sql(f"""
            INSERT INTO ADJUSTMENT_APP.ADJ_NOTIFICATION_LOG
                (EVENT_TYPE, RECIPIENTS, SUBJECT, STATUS, ERROR)
            VALUES ('{_esc(event)[:50]}', '{_esc(recipients)[:1990]}',
                    '{_esc(subject)[:490]}', '{_esc(status)[:30]}',
                    {"NULL" if not error else "'" + _esc(str(error)[:990]) + "'"})
        """).collect()
    except Exception as le:
        print(f"Notification log insert failed (non-fatal): {le}")


def _send(session, integration, recipients_csv, subject, body_html):
    """One SYSTEM$SEND_EMAIL call. Returns (ok, error)."""
    try:
        session.sql(f"""
            CALL SYSTEM$SEND_EMAIL(
                '{_esc(integration)}',
                '{_esc(recipients_csv)}',
                '{_esc(subject)}',
                '{_esc(body_html)}',
                'text/html')
        """).collect()
        return True, None
    except Exception as se:
        return False, str(se)


def _wrap(body_inner):
    return (
        '<div style="font-family:Segoe UI,Arial,sans-serif;font-size:14px;'
        'color:#1F2937;max-width:640px">'
        + body_inner +
        '<p style="color:#6B7280;font-size:12px;margin-top:18px">'
        'Automated message from the Adjustment application — manage your '
        'notification preferences with your admin. Do not reply.</p></div>'
    )


def _approver_recipients(session, process_type, exclude_username):
    """Emails of active approvers for the scope who opted into approval
    notifications — excluding the actor who triggered the event."""
    rows = session.sql(f"""
        SELECT DISTINCT p.EMAIL
        FROM ADJUSTMENT_APP.ADJ_APPROVERS a
        JOIN ADJUSTMENT_APP.ADJ_NOTIFICATION_PREFS p
          ON UPPER(p.USERNAME) = UPPER(a.USERNAME)
        WHERE a.IS_ACTIVE = TRUE
          AND p.IS_ACTIVE = TRUE
          AND p.NOTIFY_APPROVALS = TRUE
          AND p.EMAIL IS NOT NULL
          AND (a.PROCESS_TYPE IS NULL
               OR UPPER(a.PROCESS_TYPE) = UPPER('{_esc(process_type)}'))
          AND UPPER(p.USERNAME) <> UPPER('{_esc(exclude_username)}')
    """).collect()
    return sorted({str(r["EMAIL"]).strip() for r in rows if r["EMAIL"]})


def main(session, p_event, p_payload):
    event = str(p_event or "").strip().lower()
    result = {"event": event, "sent": 0}
    try:
        payload = json.loads(p_payload) if p_payload else {}

        enabled = _cfg(session, "NOTIFICATIONS_ENABLED", "false").strip().lower() == "true"
        integration = _cfg(session, "EMAIL_INTEGRATION", "ADJ_EMAIL_INT").strip()

        # ── Build (recipients, subject, body) groups per event ───────────────
        emails = []   # list of (recipients_csv, subject, body_html)

        if event == "adjustment_outcome":
            adj_ids = [str(a) for a in (payload.get("adj_ids") or []) if a]
            status  = str(payload.get("status") or "Processed")
            if adj_ids:
                ids_in = ", ".join(f"'{_esc(a)}'" for a in adj_ids)
                rows = session.sql(f"""
                    SELECT h.DIMENSION_ADJ_ID, h.COBID, h.PROCESS_TYPE,
                           h.ADJUSTMENT_TYPE, h.RUN_STATUS, h.ERRORMESSAGE,
                           h.USERNAME, p.EMAIL
                    FROM ADJUSTMENT_APP.ADJ_HEADER h
                    JOIN ADJUSTMENT_APP.ADJ_NOTIFICATION_PREFS p
                      ON UPPER(p.USERNAME) = UPPER(h.USERNAME)
                    WHERE h.ADJ_ID IN ({ids_in})
                      AND p.IS_ACTIVE = TRUE
                      AND p.NOTIFY_MY_OUTCOMES = TRUE
                      AND p.EMAIL IS NOT NULL
                """).collect()
                by_email = {}
                for r in rows:
                    by_email.setdefault(str(r["EMAIL"]).strip(), []).append(r)
                ok = status.lower() == "processed"
                for email, items in by_email.items():
                    color = "#15803D" if ok else "#B91C1C"
                    lis = ""
                    for it in items:
                        label = (f"#{int(it['DIMENSION_ADJ_ID'])}"
                                 if it["DIMENSION_ADJ_ID"] is not None else "(no report id)")
                        line = (f"<b>{label}</b> — {_h(it['PROCESS_TYPE'])} "
                                f"{_h(it['ADJUSTMENT_TYPE'])} · COB {_h(it['COBID'])} · "
                                f"<span style='color:{color};font-weight:700'>"
                                f"{_h(it['RUN_STATUS'])}</span>")
                        if not ok and it["ERRORMESSAGE"]:
                            line += (f"<br/><span style='font-family:monospace;"
                                     f"font-size:12px'>{_h(str(it['ERRORMESSAGE'])[:300])}</span>")
                        lis += f"<li style='margin-bottom:6px'>{line}</li>"
                    n = len(items)
                    subject = (SUBJECT_PREFIX +
                               (f"{n} adjustment(s) processed successfully" if ok
                                else f"{n} adjustment(s) FAILED — action needed"))
                    intro = ("Your adjustments finished processing:" if ok else
                             "Your adjustments failed — open the Adjustments "
                             "page and use <b>Retry</b> after checking the error:")
                    emails.append((email, subject,
                                   _wrap(f"<p>{intro}</p><ul>{lis}</ul>")))

        elif event == "approval_pending":
            pt   = str(payload.get("process_type") or "")
            subj = (SUBJECT_PREFIX +
                    f"Approval needed — {pt} adjustment for COB {payload.get('cobid', '?')}")
            recips = _approver_recipients(session, pt,
                                          str(payload.get("submitted_by") or ""))
            if recips:
                body = _wrap(
                    f"<p>A new <b>{_h(pt)}</b> "
                    f"<b>{_h(payload.get('adjustment_type') or '')}</b> adjustment "
                    f"for COB <b>{_h(payload.get('cobid') or '?')}</b>, submitted by "
                    f"<b>{_h(payload.get('submitted_by') or '?')}</b>, is waiting in "
                    f"the Approval Queue.</p>"
                    f"<p>Open the app's <b>Approval Queue</b> page to approve or "
                    f"reject it.</p>")
                emails.append((",".join(recips), subj, body))

        elif event == "reopen_requested":
            pt   = str(payload.get("process_type") or "")
            subj = (SUBJECT_PREFIX +
                    f"COB re-open requested — {pt} COB {payload.get('cobid', '?')}")
            recips = _approver_recipients(session, pt,
                                          str(payload.get("requested_by") or ""))
            if recips:
                body = _wrap(
                    f"<p><b>{_h(payload.get('requested_by') or '?')}</b> requested to "
                    f"re-open the signed-off COB <b>{_h(payload.get('cobid') or '?')}</b> "
                    f"for scope <b>{_h(pt)}</b>.</p>"
                    f"<p>Reason: {_h(payload.get('reason') or '—')}</p>"
                    f"<p>Decide it in the Approval Queue's <b>COB Re-open "
                    f"Requests</b> section.</p>")
                emails.append((",".join(recips), subj, body))

        elif event == "test":
            to = str(payload.get("email") or "").strip()
            if to:
                emails.append((to, SUBJECT_PREFIX + "Test notification",
                               _wrap("<p>Test successful — the notification "
                                     "integration is working.</p>")))

        else:
            _log(session, event, "-", "-", "FAILED", f"Unknown event '{event}'")
            result["error"] = f"Unknown event '{event}'"
            return json.dumps(result)

        # ── Gate + send ──────────────────────────────────────────────────────
        if not emails:
            _log(session, event, "-", "-", "NO_RECIPIENTS")
            result["status"] = "no_recipients"
            return json.dumps(result)

        # 'test' bypasses the master switch on purpose: its whole point is to
        # validate the DBA-created integration BEFORE notifications go live.
        if not enabled and event != "test":
            for recips, subject, _body in emails:
                _log(session, event, recips, subject, "SKIPPED_DISABLED")
            result["status"] = "skipped_disabled"
            return json.dumps(result)

        for recips, subject, body in emails:
            ok, err = _send(session, integration, recips, subject, body)
            _log(session, event, recips, subject,
                 "SENT" if ok else "FAILED", err)
            if ok:
                result["sent"] += 1
            else:
                result.setdefault("errors", []).append(err[:200] if err else "?")

        result["status"] = "ok" if result["sent"] else "failed"
        return json.dumps(result)

    except Exception as e:
        # Never raise — notifications must not break the caller.
        try:
            _log(session, event, "-", "-", "FAILED", str(e))
        except Exception:
            pass
        result["status"] = "failed"
        result["error"] = str(e)[:300]
        return json.dumps(result)
$$;

-- ═══════════════════════════════════════════════════════════════════════════
-- VERIFY
-- ═══════════════════════════════════════════════════════════════════════════
DESCRIBE PROCEDURE ADJUSTMENT_APP.SP_NOTIFY(VARCHAR, VARCHAR);
