"""The COB and the ENTITY_CODE an upload is FOR, read from the data itself.

Marcos, 2026-09-25: one upload = one ENTITY_CODE, so that sign-off is checked
the way it is for Scaling adjustments — by scope, entity and COB — and the
header carries the entity the rows belong to. The COB has worked this way
since the file flows stopped trusting a typed value that could disagree
with the rows; the entity now follows the same rule.

Pure functions, no Streamlit: the three Direct forms call them and show the
result; the tests exercise every branch here.
"""
from collections import namedtuple

import pandas as pd

SingleValue = namedtuple("SingleValue", "value values error")
# error: None (exactly one value), "none" (column present but no usable
# value), "many" (more than one distinct value). A missing column is
# reported by file_identity as None for that key.

_BLANKS = {"", "NAN", "NONE", "NULL", "N/A"}

COB_COLUMNS = ("COBID", "COBId", "COB_ID", "COB")
ENTITY_COLUMNS = ("ENTITY_CODE", "EntityCode", "ENTITYCODE", "ENTITY")


def _clean(series: pd.Series) -> list:
    vals = []
    for v in series.tolist():
        s = "" if v is None else str(v).strip()
        if s.upper() in _BLANKS:
            continue
        vals.append(s)
    return vals


def single_value(series, numeric: bool = False) -> SingleValue:
    """Exactly one distinct usable value in the column, or why not."""
    if numeric:
        nums = pd.to_numeric(pd.Series(_clean(pd.Series(series))), errors="coerce").dropna()
        distinct = sorted({int(x) for x in nums.tolist()})
    else:
        # Case-insensitive identity: 'Mus' and 'MUS' are one entity, and the
        # FIRST spelling in the data is the one reported. Deriving it from a
        # set instead made the answer depend on the hash seed — the same file
        # could come back as 'MUS' or 'mus' between runs (caught 2026-09-25).
        first_seen = {}
        for v in _clean(pd.Series(series)):
            first_seen.setdefault(v.upper(), v)
        distinct = list(first_seen.values())
    if not distinct:
        return SingleValue(None, [], "none")
    if len(distinct) > 1:
        # Sorted only for the message listing the clashing values.
        return SingleValue(None, sorted(distinct, key=lambda x: str(x).upper()), "many")
    return SingleValue(distinct[0], distinct, None)


def find_column(df: pd.DataFrame, candidates) -> str:
    """The first column matching any candidate, case-insensitively."""
    by_upper = {str(c).strip().upper(): c for c in df.columns}
    for cand in candidates:
        hit = by_upper.get(str(cand).strip().upper())
        if hit is not None:
            return hit
    return None


def file_identity(df: pd.DataFrame) -> dict:
    """{"cob": SingleValue | None, "entity": SingleValue | None}.

    None means the column is not in the file at all; the form treats that
    as a blocking error with its own wording.
    """
    out = {"cob": None, "entity": None}
    if df is None or not len(df.columns):
        return out
    c = find_column(df, COB_COLUMNS)
    if c is not None:
        out["cob"] = single_value(df[c], numeric=True)
    e = find_column(df, ENTITY_COLUMNS)
    if e is not None:
        out["entity"] = single_value(df[e])
    return out


def identity_message(kind: str, sv, noun: str = "upload") -> str:
    """The sentence the form shows for a bad identity, or '' when fine."""
    label = "COB" if kind == "cob" else "ENTITY_CODE"
    if sv is None:
        return (f"The file has no {label} column — the {label} is taken from the "
                f"file, so submission is blocked until it is added.")
    if sv.error == "many":
        shown = ", ".join(map(str, sv.values[:5])) + ("…" if len(sv.values) > 5 else "")
        return (f"The file mixes {len(sv.values)} different {label} values "
                f"({shown}) — one {noun} covers exactly one {label}. Submission "
                f"is blocked.")
    if sv.error == "none":
        return (f"{label} has no valid value in the file — every row must carry "
                f"it. Submission is blocked.")
    return ""
