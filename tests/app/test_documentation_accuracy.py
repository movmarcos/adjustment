"""The Documentation page must describe what the code actually does.

This file exists because the docs had already drifted: the page told users
the FRTB Explore download was capped at 1,000 rows while the constant said
5,000, and the permissions table still said anyone could submit after the
submitter list went in.

Wrong documentation is worse than none. Someone reads "anyone can submit",
finds the button greyed out, and files a bug. So the claims that can be tied
to code are tied to it here, and the ones that cannot are at least checked
for existence, so deleting the paragraph fails rather than passing quietly.

The page also feeds the AI assistant: `_KNOWLEDGE` is the text the assistant
answers from. Both it and the visible sections are checked, because an
assistant confidently repeating a stale rule is the worst version of this.
"""
import os
import re
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "streamlit", "adjustment_engine"))

DOCS = os.path.join(os.path.dirname(__file__), "..", "..", "streamlit", "adjustment_engine", "pages",
                    "7_Documentation.py")
EXPLORE = os.path.join(os.path.dirname(__file__), "..", "..", "streamlit", "adjustment_engine", "pages",
                       "8_FRTB_Explore.py")


def _source(path):
    with open(path, encoding="utf-8") as fh:
        return fh.read()


@pytest.fixture(scope="module")
def docs():
    return _source(DOCS)


@pytest.fixture(scope="module")
def knowledge(docs):
    """Just the assistant's knowledge block."""
    start = docs.index('_KNOWLEDGE = """')
    end = docs.index('""".strip()', start)
    return docs[start:end]


# ── Numbers that must track the code ─────────────────────────────────────────

def test_the_documented_export_cap_matches_the_code(docs):
    """This one had already drifted once."""
    import ast
    cap = None
    for node in ast.parse(_source(EXPLORE)).body:
        if isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Name) and t.id == "MAX_ROWS":
                    cap = ast.literal_eval(node.value)
    assert cap, "MAX_ROWS not found in the FRTB Explore page"

    quoted = set(re.findall(r"max ([\d,]+) rows", docs))
    assert quoted, "the docs no longer state the export cap at all"
    assert quoted == {f"{cap:,}"}, (
        "The documentation says " + str(sorted(quoted)) + " but the code "
        "caps at " + f"{cap:,}" + ". Update the docs when you change the cap.")


# ── Who can submit ───────────────────────────────────────────────────────────

def test_the_docs_do_not_still_say_anyone_can_submit(docs):
    assert '["Submit adjustments", "Any app user"]' not in docs, (
        "The permissions table still says any app user can submit. Since the "
        "Authorized Submitters list went in, that is wrong, and a user whose "
        "Submit button is disabled will read this and file a bug.")


def test_the_submitter_list_is_documented(docs, knowledge):
    assert "Authorized Submitters" in docs, (
        "The visible docs never mention the submitter list.")
    assert "Authorized Submitters" in knowledge, (
        "The assistant's knowledge never mentions the submitter list, so it "
        "will answer questions about who can submit from stale text.")


def test_the_docs_say_preview_is_not_gated(docs, knowledge):
    """The distinction is the whole point of the feature."""
    for text, where in ((docs, "the visible docs"),
                        (knowledge, "the assistant knowledge")):
        assert "preview" in text.lower()
        assert re.search(r"(only the Submit button is gated|"
                         r"gates the Submit button ONLY|"
                         r"Only the Submit button is gated)", text), (
            where + " does not say that only Submit is gated. Without it "
            "people assume the whole page is closed to them.")


def test_the_bootstrap_rule_is_documented(docs, knowledge):
    """Empty list = everyone. Admins must know this before they add row one."""
    assert re.search(r"[Ww]hile that list is empty|while the list is empty",
                     docs + knowledge), (
        "Neither the docs nor the assistant explain that an empty submitter "
        "list means everyone may submit. An admin adding the first user needs "
        "to know it locks the door behind them.")


# ── Multi-scope behaviour ────────────────────────────────────────────────────

def test_the_docs_say_an_empty_scope_is_skipped_not_blocking(docs, knowledge):
    assert "blocks Submit until you deselect it" not in docs, (
        "The docs still describe the OLD behaviour, where one empty scope "
        "blocked the whole draft.")
    assert re.search(r"0 rows is skipped|is SKIPPED, not blocked", docs + knowledge), (
        "Neither the docs nor the assistant mention that an empty scope is "
        "skipped and the other scopes still go through.")


def test_select_all_is_documented(docs):
    assert "Select all" in docs, (
        "The scope pills gained Select all / Clear; the docs do not say so.")


# ── FRTB currency conversion ─────────────────────────────────────────────────

def test_the_usd_only_rule_is_documented(docs, knowledge):
    for text, where in ((docs, "the visible docs"),
                        (knowledge, "the assistant knowledge")):
        assert "EXCHANGE_RATE" in text, (
            where + " does not mention where the exchange rate comes from.")
        assert "REGION_AREA_CODE" in text, (
            where + " does not mention REGION_AREA_CODE, which is the field "
            "people will have to fix when a rate is not found.")


def test_the_docs_warn_that_a_missing_rate_fails_the_adjustment(docs, knowledge):
    assert re.search(r"no rate.{0,40}fails|FAILS the\s+whole adjustment|"
                     r"adjustment fails", docs + knowledge, re.S), (
        "Neither the docs nor the assistant say what happens when no rate is "
        "found. Users need to know it fails loudly rather than writing a "
        "blank local amount.")


def test_the_docs_name_the_usd_columns_users_must_fill(docs):
    for column in ("AMOUNT_IN_USD", "JTD_LOSS_USD", "NOTIONAL_AMOUNT_USD"):
        assert column in docs, (
            "The docs do not name " + column + ", which is one of the three "
            "columns an uploader now has to fill.")


# ── The object inventory ─────────────────────────────────────────────────────

def test_the_object_inventory_lists_the_new_table(docs):
    assert "ADJ_SUBMITTERS" in docs, (
        "The Key Database Objects table does not list ADJ_SUBMITTERS.")


def test_the_page_still_compiles_as_python():
    """Cheap guard: these are large f-strings full of HTML and quotes."""
    import ast
    ast.parse(_source(DOCS))
