"""Pure helpers for the Transfer Book form (no Streamlit, no Snowflake).

A Transfer Book adjustment ADDS a SOURCE book's adjusted values to a TARGET
book at one COB — the target keeps everything it already has. Selecting trade codes narrows it: the engine
takes one adjustment per trade (and one per selected scope), so the page fans
out over `transfer_jobs(...)` exactly the way multi-scope Scaling fans out
over `submit_fanout(...)` (utils/submit_fanout.py).

Kept free of Streamlit so the fan-out arithmetic and the partial-failure copy
are unit-testable without a Snowflake session.
"""


def transfer_jobs(scopes, trade_codes):
    """One submission per scope per trade code; whole book when no trade.

    Returns [(scope, trade_or_None), ...] in scope-major order (every trade of
    the first scope, then the second scope's) — the order the submissions are
    made and named in.
    """
    scopes = [s for s in (scopes or []) if s]
    trades = [str(t).strip() for t in (trade_codes or []) if t and str(t).strip()]
    if not trades:
        return [(s, None) for s in scopes]
    return [(s, t) for s in scopes for t in trades]


def book_entity(book_rows, code):
    """ENTITY_CODE of a book from DIMENSION.BOOK rows shaped
    (BOOK_CODE, DEPARTMENT_CODE, ENTITY_CODE); None when unknown.

    The entity is DERIVED from the target book rather than typed: the sign-off
    gate, the overlap check and every grid key off ENTITY_CODE, and the submit
    SP derives the very same value server-side (03_sp_submit_adjustment.sql).
    """
    code = (code or "").strip().upper()
    for r in book_rows or []:
        if r[0] is not None and str(r[0]).strip().upper() == code:
            return str(r[2]) if r[2] is not None else None
    return None


def submit_jobs(jobs, payload, submit_one, is_success, scope_label,
                on_progress=None) -> dict:
    """Transfer Book: one submit call per (scope, trade). Partial failures are named.

    Same contract as `submit_fanout`: every result — success, partial failure
    or the empty guard — carries an explicit ``"fanout": True`` marker and
    ``"created"`` (how many calls were accepted), because the success screen
    keys off that marker and NOT off message text (the backend's ordinary
    single-submit message, "Created with status '<status>'.", also starts with
    "Created "). No rollback on a partial failure: the message names what was
    created so the user can delete it from the Adjustments page.

    `on_progress(i, n, label)` — optional, called once per job (i is 1-based)
    BEFORE that submission runs, so a caller can drive a progress bar. A wide
    transfer (scopes × trades) can be dozens of sequential calls and a bare
    spinner gives no sign of where it is.
    """
    if not jobs:
        return {"status": "Error", "message": "No scope selected.",
                "fanout": True, "created": 0}

    n = len(jobs)
    created, statuses, failures = [], [], []
    for i, (sc, trade) in enumerate(jobs, 1):
        label = f"{scope_label(sc)}{' / ' + trade if trade else ''}"
        if on_progress is not None:
            on_progress(i, n, label)
        p = {**payload, "process_type": sc}
        if trade:
            p["trade_code"] = trade
        res = submit_one(p)
        if is_success(res):
            created.append(label)
            statuses.append(res.get("status"))
        else:
            failures.append(f"{label}: {(res or {}).get('message', 'not accepted')}")

    if not failures:
        # A "Pending Approval" job still needs an approver even if a sibling
        # was auto-queued as "Pending" — surface the stricter status rather
        # than picking the first one arbitrarily (mirrors submit_fanout).
        status = "Pending Approval" if "Pending Approval" in statuses else statuses[0]
        nxt = ("They are waiting for approval on the Approval Queue page."
               if status == "Pending Approval"
               else "They are queued and will be processed by their scope pipelines.")
        noun = "adjustment" if len(created) == 1 else "adjustments"
        message = (f"Created {len(created)} Transfer Book {noun} "
                   f"({', '.join(created)}). {nxt}")
        if len(set(statuses)) > 1:
            per_job = ", ".join(f"{lbl} {st}" for lbl, st in zip(created, statuses))
            message += f" Statuses: {per_job}."
        return {"status": status, "message": message,
                "fanout": True, "created": len(created)}

    partial = (f" Already created: {', '.join(created)} — delete them from the "
               f"Adjustments page if they are no longer wanted." if created else "")
    return {"status": "Error",
            "message": "Not every transfer was accepted. "
                       + " | ".join(failures) + partial,
            "fanout": True, "created": len(created)}
