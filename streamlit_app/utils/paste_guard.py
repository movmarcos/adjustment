"""Guards against pasted or uploaded data carrying the same row twice.

Marcos, 2026-09-25: "if the user does a copy and paste, the values pasted
sometimes are slow to show and the user pastes again. The data will be
duplicated." Two separate guards, because the double paste leaves a
fingerprint plain duplicates do not:

  * repeated_paste(text) — a second paste into the same box yields
    header, rows, header, rows. A data line equal to the header is never
    real data, so a repeated header is unambiguous: keep the first copy and
    tell the user how many copies there were.
  * duplicate_rows(df) — rows identical to an earlier row after trimming
    and ignoring case, in any input mode (file uploads included). The form
    BLOCKS on these: in a per-row Direct upload each duplicate becomes a
    second adjustment with the same value, and there is no case where that
    is intended. dedupe(df) keeps the first occurrence for the one-click fix.

Pure functions, no Streamlit.
"""
import pandas as pd


def _norm_line(line: str) -> str:
    """All whitespace dropped: a header re-pasted as 'a, b ,c' is 'a,b,c'."""
    return "".join(str(line).split()).upper()


def repeated_paste(text: str):
    """(first_copy_text, copies). copies == 1 means nothing was repeated.

    The header is the first non-blank line. Any later line equal to it
    (whitespace- and case-insensitive) starts another copy.
    """
    if not text or not str(text).strip():
        return text, 1
    lines = str(text).splitlines()
    header_idx = next((i for i, ln in enumerate(lines) if ln.strip()), None)
    if header_idx is None:
        return text, 1
    header = _norm_line(lines[header_idx])
    if not header:
        return text, 1
    repeats = [i for i in range(header_idx + 1, len(lines))
               if _norm_line(lines[i]) == header]
    if not repeats:
        return text, 1
    first = "\n".join(lines[:repeats[0]]).rstrip("\n") + "\n"
    return first, len(repeats) + 1


def _key_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Every cell as a trimmed, upper-cased string; NaN and blank alike."""
    return df.astype(str).apply(
        lambda col: col.map(lambda v: "" if str(v).strip().upper() in ("", "NAN", "NONE")
                            else " ".join(str(v).strip().split()).upper()))


def duplicate_rows(df: pd.DataFrame) -> list:
    """[(row_number, first_row_number), ...] for every row that repeats an
    earlier one. Row numbers are 1-based DATA rows (the header is not a
    row), matching the ✓/✗ grid and the error messages."""
    if df is None or not len(df):
        return []
    keys = _key_frame(df)
    seen = {}
    out = []
    for i, tup in enumerate(keys.itertuples(index=False, name=None)):
        if tup in seen:
            out.append((i + 1, seen[tup] + 1))
        else:
            seen[tup] = i
    return out


def dedupe(df: pd.DataFrame) -> pd.DataFrame:
    """The frame without the rows duplicate_rows reports; first kept."""
    dups = {r for r, _ in duplicate_rows(df)}
    if not dups:
        return df
    return df.iloc[[i for i in range(len(df)) if (i + 1) not in dups]].reset_index(drop=True)


def duplicate_message(dups: list) -> str:
    if not dups:
        return ""
    rows = [r for r, _ in dups]
    shown = ", ".join(str(r) for r in rows[:8]) + ("…" if len(rows) > 8 else "")
    return (f"{len(rows)} row(s) are exact duplicates of earlier rows (row"
            f"{'s' if len(rows) > 1 else ''} {shown}). Each row becomes its own "
            f"adjustment, so a duplicate would book the same value twice. "
            f"Submission is blocked — remove them below, or fix the file.")
