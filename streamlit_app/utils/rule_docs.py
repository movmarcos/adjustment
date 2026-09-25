"""Read the cached business description of a scope's SQL validation rules.

ADJ_RULE_DOCS is written by SP_REFRESH_RULE_DOCS (15c) and read through
VW_RULE_DOCS, which carries IS_STALE (the view's DDL hash moved since the
text was written) and IS_REVIEWED. The New Adjustment page shows the text
with those two flags as labels; the Admin page lists every row.

A scope can have more than one view (FRTB: the shared VW_DIRECT_VALIDATE
plus its enriched view); their bullets are shown one after the other.
Anything failing here degrades to "not written yet" — the panel is
informational and must never block an upload.
"""
import streamlit as st

from utils.snowflake_conn import run_query


def _fmt_ts(v) -> str:
    try:
        return v.strftime("%d %b %Y %H:%M")
    except Exception:
        return str(v or "")[:16]


def load_rule_docs(scope: str, cache: bool = True):
    """The stored description for a scope, or None when nothing is stored.

    Returns {"text", "stale", "reviewed", "reviewed_by", "generated_at",
    "model", "objects": [names]} — the shape utils.direct_rules.rules_html
    takes as `generic`.
    """
    key = f"_rule_docs_{scope}"
    if cache and key in st.session_state:
        return st.session_state[key]
    doc = None
    try:
        esc = str(scope).replace("\\", "\\\\").replace("'", "''")
        rows = run_query(f"""
            SELECT OBJECT_NAME, DESCRIPTION, MODEL, GENERATED_AT, REVIEWED_BY,
                   IS_STALE, IS_REVIEWED
            FROM ADJUSTMENT_APP.VW_RULE_DOCS
            WHERE UPPER(PROCESS_TYPE) = UPPER('{esc}')
            ORDER BY OBJECT_NAME
        """) or []
        texts = [str(r["DESCRIPTION"] or "").strip() for r in rows]
        texts = [t for t in texts if t]
        if texts:
            doc = {
                "text": "\n".join(texts),
                "stale": any(bool(r["IS_STALE"]) for r in rows),
                "reviewed": all(bool(r["IS_REVIEWED"]) for r in rows),
                "reviewed_by": ", ".join(sorted({str(r["REVIEWED_BY"]) for r in rows
                                                 if r["REVIEWED_BY"]})),
                "generated_at": _fmt_ts(max((r["GENERATED_AT"] for r in rows
                                             if r["GENERATED_AT"]), default=None)),
                "model": ", ".join(sorted({str(r["MODEL"]) for r in rows if r["MODEL"]})),
                "objects": [str(r["OBJECT_NAME"]) for r in rows],
            }
    except Exception:
        doc = None
    if cache:
        st.session_state[key] = doc
    return doc


def forget_rule_docs() -> None:
    """Drop the per-session cache (after a refresh or a review on Admin)."""
    for k in [k for k in st.session_state.keys() if str(k).startswith("_rule_docs_")]:
        del st.session_state[k]
