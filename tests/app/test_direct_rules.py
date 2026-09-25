"""utils/direct_rules — the English rendering of a scope's validation rules.

The rendering is from data, not a model, so it is asserted exactly. The
fixtures are the real FRTB / DRC / RRAO rule sets from
15_direct_frtb_upload.sql, read from the SQL so a rule added there is
covered here without anyone remembering to copy it.
"""
import json
import os
import re
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "streamlit", "adjustment_engine"))

from utils.direct_rules import (condition_text, describe_columns, describe_rules,
                                field_words, rule_when, rules_html)

SQL = os.path.join(os.path.dirname(__file__), "..", "..",
                   "streamlit", "adjustment_engine", "sql", "15_direct_frtb_upload.sql")


def _rule_sets():
    """Every VALIDATION_RULES literal in the SQL, parsed, in file order."""
    with open(SQL, encoding="utf-8") as fh:
        src = fh.read()
    out = []
    for m in re.finditer(r"PARSE_JSON\('(\[\s*\{\"field\".*?\])'\)\s*AS VALIDATION_RULES", src, re.S):
        out.append(json.loads(m.group(1)))
    return out


# ── Conditions → words ───────────────────────────────────────────────────────

@pytest.mark.parametrize("cond,text", [
    (["RISK_CLASS", "FX|GIRR", False],        "Risk Class is FX or GIRR"),
    (["RISK_CLASS", "FX", False],             "Risk Class is FX"),
    (["RISK_CLASS", "EQUIT|CSR", False],      "Risk Class is Equity or CSR"),
    (["SENSITIVITY_TYPE", "VEGA", False],     "Sensitivity Type is Vega"),
    (["SENSITIVITY_TYPE", "CURVATURE", False], "Sensitivity Type is Curvature"),
    (["TRADE_CODE", "^(|NAN|NONE)$", False],  "Trade Code is empty"),
    (["TRADE_CODE", "^(|NAN|NONE)$", True],   "Trade Code is not empty"),
    (["RISK_CLASS", "NON.*SEC.*CREDIT", False], "Risk Class is Non-Sec (Credit)"),
    (["RISK_CLASS", "NON.*SEC", True],        "Risk Class is not Non-Sec"),
    (["RISK_CLASS", "SEC", False],            "Risk Class is Sec"),
])
def test_condition_text(cond, text):
    assert condition_text(cond) == text


def test_several_conditions_are_joined_with_and():
    rule = {"field": "MATURITY_OF_THE_OPTION",
            "conditions": [["RISK_CLASS", "FX", False], ["SENSITIVITY_TYPE", "VEGA", False]]}
    assert rule_when(rule) == "Risk Class is FX and Sensitivity Type is Vega"


def test_the_drc_sec_rule_reads_as_sec_but_not_non_sec():
    """'SEC' matches inside 'NON-SEC', so the SQL rule excludes it explicitly.
    The English must carry that exclusion or a reader will think Non-Sec
    rows need DEFAULT_RISK_WEIGHT under this rule too."""
    rule = {"field": "DEFAULT_RISK_WEIGHT",
            "conditions": [["RISK_CLASS", "SEC", False], ["RISK_CLASS", "NON.*SEC", True]]}
    assert rule_when(rule) == "Risk Class is Sec and Risk Class is not Non-Sec"


def test_unknown_columns_fall_back_to_title_case():
    assert field_words("DEFAULT_RISK_WEIGHT") == "Default Risk Weight"


def test_a_rule_with_no_conditions_is_always():
    assert rule_when({"field": "X"}) == "always"


# ── Grouping ─────────────────────────────────────────────────────────────────

def test_rules_are_grouped_by_condition_in_first_seen_order():
    rules = [
        {"field": "CCY1", "conditions": [["RISK_CLASS", "FX|GIRR", False]], "error": "a"},
        {"field": "CCY_AMT", "conditions": [["RISK_CLASS", "FX|GIRR", False]], "error": "b"},
        {"field": "CCY2", "conditions": [["RISK_CLASS", "FX", False]], "error": "c"},
        {"field": "BUCKET", "conditions": [["RISK_CLASS", "EQUIT|CSR", False]], "error": "d"},
    ]
    groups = describe_rules(rules)
    assert [g["when"] for g in groups] == ["Risk Class is FX or GIRR", "Risk Class is FX",
                                          "Risk Class is Equity or CSR"]
    assert groups[0]["fields"] == ["CCY1", "CCY_AMT"]
    assert groups[0]["errors"] == ["a", "b"]


def test_the_same_field_under_two_conditions_appears_in_both_groups():
    """VERTEX is required for GIRR Delta AND for CSR Delta — two rules."""
    rules = [
        {"field": "VERTEX", "conditions": [["RISK_CLASS", "GIRR", False], ["SENSITIVITY_TYPE", "DELTA", False]]},
        {"field": "VERTEX", "conditions": [["RISK_CLASS", "CSR", False], ["SENSITIVITY_TYPE", "DELTA", False]]},
    ]
    groups = describe_rules(rules)
    assert len(groups) == 2 and all(g["fields"] == ["VERTEX"] for g in groups)


def test_malformed_rules_are_skipped_not_fatal():
    assert describe_rules([None, {}, {"conditions": []}, "x"]) == []


def test_every_real_rule_set_renders_without_a_raw_regex_leaking():
    """Business readers must never see '.*' or '^(' on the page."""
    sets = _rule_sets()
    assert len(sets) >= 3, "expected the FRTB, DRC and RRAO rule sets in the SQL"
    for rules in sets:
        for g in describe_rules(rules):
            assert ".*" not in g["when"] and "^" not in g["when"] and "|" not in g["when"], g["when"]
            assert g["fields"]


def test_the_real_frtb_set_collapses_to_a_handful_of_groups():
    frtb = _rule_sets()[0]
    groups = describe_rules(frtb)
    assert len(frtb) > len(groups) * 1.5, (
        "Grouping barely reduced the FRTB rules; the panel would be as long "
        "as the raw list.")
    assert any(g["when"] == "Risk Class is Equity or CSR" and len(g["fields"]) >= 6
               for g in groups)


# ── Columns ──────────────────────────────────────────────────────────────────

def test_columns_carry_required_type_and_reverse_aliases():
    cols = describe_columns(
        [{"name": "COBID", "type": "number", "required": True},
         {"name": "UNDERLYING_TENOR_CODE", "type": "string"}],
        {"EVALUATION_DATE": "COBID", "VERTEX_UNDERLYING": "UNDERLYING_TENOR_CODE"})
    assert cols[0] == {"name": "COBID", "required": True, "type": "number",
                       "alias": "EVALUATION_DATE"}
    assert cols[1]["alias"] == "VERTEX_UNDERLYING" and cols[1]["required"] is False


def test_plain_string_columns_are_accepted():
    assert describe_columns(["a", "b"])[1]["name"] == "B"


# ── HTML ─────────────────────────────────────────────────────────────────────

def _html(**kw):
    base = dict(scope_label="FRTBSBM",
                columns=describe_columns([{"name": "COBID", "required": True}]),
                groups=describe_rules([{"field": "CCY1",
                                        "conditions": [["RISK_CLASS", "FX", False]]}]),
                generic=None)
    base.update(kw)
    return rules_html(**base)


def test_html_has_the_three_sections_in_order():
    h = _html()
    assert h.index("Columns") < h.index("Required depending on the row") < h.index("Every row must also pass")


def test_html_without_stored_text_points_to_the_admin_page():
    assert "Admin page" in _html()


def test_stored_text_renders_bullets_and_labels_draft_and_stale():
    h = _html(generic={"text": "- Value must be **numeric**\n- One `COBID` per file",
                       "stale": True, "reviewed": False, "generated_at": "2026-09-25"})
    assert "<li>Value must be <strong>numeric</strong></li>" in h
    assert "<li>One <code>COBID</code> per file</li>" in h
    assert "RULES CHANGED SINCE THIS WAS WRITTEN" in h
    assert "DRAFT" in h


def test_reviewed_text_shows_neither_badge():
    h = _html(generic={"text": "- ok", "stale": False, "reviewed": True,
                       "reviewed_by": "alice"})
    assert "DRAFT" not in h and "RULES CHANGED" not in h and "reviewed by alice" in h


def test_model_output_is_escaped():
    """The stored text is model output shown to every user: no raw HTML."""
    h = _html(generic={"text": "- <script>alert(1)</script>", "stale": False, "reviewed": True})
    assert "<script>" not in h and "&lt;script&gt;" in h
