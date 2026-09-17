"""Pure fan-out helper for submitting one adjustment per scope.

Kept free of Streamlit/Snowflake so it can be unit tested directly: the
page wraps this with its own `_submit_one` / `_is_submit_success` /
`scope_label` and exposes it as `_submit_fanout(payload, scopes)`.
"""


def first_scope(scopes):
    """The scope a fan-out payload is built against — the first selected one.

    The payload is built once and the scope is swapped per call (see
    `submit_fanout`), so which code it carries only matters for the scopes
    that are NOT fanned out (a single selection). Kept here, pure, so the
    page never falls back to the legacy single `wiz["process_type"]`."""
    return (list(scopes or []) or [None])[0]


def submit_fanout(payload: dict, scopes: list, submit_one, is_success, scope_label) -> dict:
    """One submit call per scope (same payload, scope swapped).
    Scopes sharing a pipeline are serialised by the engine. No rollback on a
    partial failure: the message names what was created so the user can
    delete it on the Adjustments page.

    Every result (success, partial failure, or the empty-scope guard) carries
    an explicit `"fanout": True` marker plus `"created"` (count of scopes
    successfully submitted) — the caller must not infer a fan-out result by
    matching on message text, since the backend's ordinary single-scope
    success message ("Created with status '<status>'.") also starts with
    "Created "."""
    if not scopes:
        return {"status": "Error", "message": "No scope selected.",
                "fanout": True, "created": 0}

    created, statuses, failures = [], [], []
    for sc in scopes:
        res = submit_one({**payload, "process_type": sc})
        if is_success(res):
            created.append(scope_label(sc))
            statuses.append(res.get("status"))
        else:
            failures.append(f"{scope_label(sc)}: {res.get('message', 'not accepted')}")

    if not failures:
        # A "Pending Approval" scope still needs an approver even if a
        # sibling scope was auto-queued as "Pending" — surface the stricter
        # status rather than picking the first one arbitrarily.
        status = "Pending Approval" if "Pending Approval" in statuses else statuses[0]
        # What happens next depends on the status: a "Pending Approval"
        # adjustment is NOT queued for processing until an approver actions
        # it — saying "queued" there sends users looking for a report that
        # never comes (same rule as the success screen's next-steps copy).
        nxt = ("They are waiting for approval on the Approval Queue page."
               if status == "Pending Approval"
               else "They are queued and will be processed by their scope pipelines.")
        noun = "adjustment" if len(created) == 1 else "adjustments"
        message = (f"Created {len(created)} {noun} — one per scope "
                   f"({', '.join(created)}). {nxt}")
        if len(set(statuses)) > 1:
            per_scope = ", ".join(f"{lbl} {st}" for lbl, st in zip(created, statuses))
            message += f" Statuses: {per_scope}."
        return {"status": status, "message": message,
                "fanout": True, "created": len(created)}

    partial = (f" Already created: {', '.join(created)} — delete them from the "
               f"Adjustments page if they are no longer wanted." if created else "")
    return {"status": "Error",
            "message": "Not every scope was accepted. " + " | ".join(failures) + partial,
            "fanout": True, "created": len(created)}
