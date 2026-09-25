"""utils/paste_guard — a double paste and duplicate rows.

Marcos, 2026-09-25: a slow paste made a user paste again, and the batch
carried every row twice.
"""
import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "streamlit", "adjustment_engine"))

from utils.paste_guard import (dedupe, duplicate_message, duplicate_rows,
                               repeated_paste)

CSV = "COBID,ENTITY_CODE,VALUE_USD\n20260901,MUS,10\n20260901,MUS,20\n"


# ── The reported case ────────────────────────────────────────────────────────

def test_a_double_paste_keeps_the_first_copy_and_counts_two():
    text, copies = repeated_paste(CSV + CSV)
    assert copies == 2
    assert text == CSV


def test_a_triple_paste_counts_three():
    assert repeated_paste(CSV * 3)[1] == 3


def test_a_second_copy_pasted_without_a_trailing_newline_is_still_caught():
    text, copies = repeated_paste(CSV.rstrip("\n") + "\n" + CSV)
    assert copies == 2 and text == CSV


def test_header_match_ignores_case_and_spacing():
    second = "cobid, entity_code ,value_usd\n20260901,MUS,10\n"
    text, copies = repeated_paste(CSV + second)
    assert copies == 2 and text == CSV


def test_a_single_paste_is_untouched():
    assert repeated_paste(CSV) == (CSV, 1)


def test_leading_blank_lines_do_not_confuse_the_header():
    text, copies = repeated_paste("\n\n" + CSV + CSV)
    assert copies == 2 and text.strip() == CSV.strip()


@pytest.mark.parametrize("empty", ["", "   ", "\n\n", None])
def test_empty_input_is_untouched(empty):
    assert repeated_paste(empty) == (empty, 1)


# ── Duplicate rows ───────────────────────────────────────────────────────────

def _df(rows, cols=("A", "B")):
    return pd.DataFrame(rows, columns=list(cols))


def test_exact_duplicate_rows_are_reported_with_their_first_occurrence():
    df = _df([["x", 1], ["y", 2], ["x", 1], ["y", 2], ["x", 1]])
    assert duplicate_rows(df) == [(3, 1), (4, 2), (5, 1)]


def test_trimming_and_case_do_not_hide_a_duplicate():
    df = _df([["MUS", " 10 "], ["mus", "10"]])
    assert duplicate_rows(df) == [(2, 1)]


def test_blank_and_nan_are_the_same_empty_cell():
    df = _df([["MUS", None], ["MUS", ""], ["MUS", float("nan")]])
    assert duplicate_rows(df) == [(2, 1), (3, 1)]


def test_a_differing_cell_is_not_a_duplicate():
    df = _df([["MUS", 10], ["MUS", 11]])
    assert duplicate_rows(df) == []


def test_dedupe_keeps_the_first_occurrence_in_order():
    df = _df([["x", 1], ["y", 2], ["x", 1], ["z", 3]])
    out = dedupe(df)
    assert out.values.tolist() == [["x", 1], ["y", 2], ["z", 3]]
    assert list(out.index) == [0, 1, 2]


def test_dedupe_of_a_clean_frame_is_the_same_object():
    df = _df([["x", 1]])
    assert dedupe(df) is df


def test_empty_frames_are_fine():
    assert duplicate_rows(pd.DataFrame()) == []
    assert duplicate_rows(None) == []


def test_the_message_names_rows_and_blocks():
    m = duplicate_message([(3, 1), (4, 2)])
    assert "2 row(s) are exact duplicates" in m and "rows 3, 4" in m and "blocked" in m
    assert duplicate_message([]) == ""


def test_the_message_truncates_after_eight_rows():
    m = duplicate_message([(i, 1) for i in range(2, 14)])
    assert "…" in m and "12 row(s)" in m
