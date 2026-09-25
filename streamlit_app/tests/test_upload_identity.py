"""utils/upload_identity — one COB and one ENTITY_CODE per upload."""
import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from utils.upload_identity import (file_identity, find_column, identity_message,
                                   single_value)


def _df(**cols):
    return pd.DataFrame({k: v for k, v in cols.items()})


# ── single_value ─────────────────────────────────────────────────────────────

def test_one_value_is_accepted():
    sv = single_value(pd.Series(["MUS", "MUS", "MUS"]))
    assert sv.value == "MUS" and sv.error is None


def test_case_and_padding_do_not_make_two_entities():
    sv = single_value(pd.Series([" MUS", "mus", "MUS "]))
    assert sv.error is None and sv.value == "MUS"


def test_two_entities_are_reported_with_both_values():
    sv = single_value(pd.Series(["MUS", "MSI", "MUS"]))
    assert sv.error == "many" and sv.values == ["MSI", "MUS"]


@pytest.mark.parametrize("blank", ["", " ", "nan", "NaN", "None", "NULL", "n/a", None])
def test_blanks_are_ignored_not_counted_as_a_value(blank):
    sv = single_value(pd.Series(["MUS", blank]))
    assert sv.error is None and sv.value == "MUS"


def test_only_blanks_is_none():
    assert single_value(pd.Series(["", None, "nan"])).error == "none"


def test_numeric_cob_is_read_as_int_and_padded_strings_agree():
    sv = single_value(pd.Series(["20260901", " 20260901 ", 20260901.0]), numeric=True)
    assert sv.error is None and sv.value == 20260901


def test_a_non_numeric_cob_does_not_count():
    sv = single_value(pd.Series(["20260901", "yesterday"]), numeric=True)
    assert sv.error is None and sv.value == 20260901


def test_two_cobs_are_many():
    sv = single_value(pd.Series([20260901, 20260902]), numeric=True)
    assert sv.error == "many" and sv.values == [20260901, 20260902]


# ── file_identity ────────────────────────────────────────────────────────────

def test_columns_are_matched_case_insensitively_across_the_two_templates():
    """VaR template says EntityCode/COBId; FRTB says ENTITY_CODE/COBID."""
    var = _df(COBId=[20260901], EntityCode=["MUS"])
    frtb = _df(cobid=[20260901], entity_code=["MUS"])
    for df in (var, frtb):
        ident = file_identity(df)
        assert ident["cob"].value == 20260901
        assert ident["entity"].value == "MUS"


def test_a_missing_column_is_none_not_an_error_tuple():
    ident = file_identity(_df(COBID=[1]))
    assert ident["entity"] is None and ident["cob"].value == 1


def test_an_empty_frame_gives_nothing():
    assert file_identity(pd.DataFrame()) == {"cob": None, "entity": None}
    assert file_identity(None) == {"cob": None, "entity": None}


def test_find_column_returns_the_original_spelling():
    assert find_column(_df(EntityCode=[1]), ("ENTITY_CODE", "ENTITYCODE")) == "EntityCode"


# ── Messages ─────────────────────────────────────────────────────────────────

def test_messages_name_the_column_and_block():
    assert "no ENTITY_CODE column" in identity_message("entity", None)
    m = identity_message("entity", single_value(pd.Series(["A", "B"])))
    assert "mixes 2 different ENTITY_CODE values (A, B)" in m and "blocked" in m
    assert "no valid value" in identity_message("cob", single_value(pd.Series([""]), numeric=True))
    assert identity_message("cob", single_value(pd.Series([1]), numeric=True)) == ""


def test_many_values_are_truncated_to_five():
    sv = single_value(pd.Series(list("ABCDEFG")))
    m = identity_message("entity", sv)
    assert "A, B, C, D, E…" in m and "7 different" in m
