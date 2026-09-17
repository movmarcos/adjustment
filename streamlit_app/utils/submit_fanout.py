"""Pure fan-out helper for submitting one adjustment per scope.

Kept free of Streamlit/Snowflake so it can be unit tested directly: the
page wraps this with its own `_submit_one` / `_is_submit_success` /
`scope_label` and exposes it as `_submit_fanout(payload, scopes)`.
"""


def submit_fanout(payload: dict, scopes: list, submit_one, is_success, scope_label) -> dict:
    """One submit call per scope (same payload, scope swapped).
    Scopes sharing a pipeline are serialised by the engine. No rollback on a
    partial failure: the message names what was created so the user can
    delete it on the Adjustments page."""
    created, failures, statuses = [], [], []
    for sc in scopes:
        res = submit_one({**payload, "process_type": sc})
        if is_success(res):
            created.append(scope_label(sc))
            statuses.append(res.get("status"))
        else:
            failures.append(f"{scope_label(sc)}: {res.get('message', 'not accepted')}")
    if not failures:
        return {"status": statuses[0],
                "message": (f"Created {len(created)} adjustments — one per scope "
                            f"({', '.join(created)}). They are queued and will be "
                            f"processed by their scope pipelines.")}
    partial = (f" Already created: {', '.join(created)} — delete them from the "
               f"Adjustments page if they are no longer wanted." if created else "")
    return {"status": "Error",
            "message": "Not every scope was accepted. " + " | ".join(failures) + partial}
