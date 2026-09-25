"""Escaping guarantees the whole app depends on.

Two things are tested here:

1. `utils.snowflake_conn.sql_escape` — THE single SQL-literal escape. Every
   page used to carry its own two-line copy and one of them (the Admin page's
   role lookup) had silently dropped the backslash step, so a value ending in
   `\\` escaped the closing quote and broke out of the literal.
2. `utils.styles.render_filter_chips` — the chips are rendered with
   `unsafe_allow_html=True` from columns a submitter types freely
   (TRADE_CODE, STRATEGY, CURVE_CODE, ...), so the values must be
   HTML-escaped before they reach the markup.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "streamlit", "adjustment_engine"))

import streamlit
from utils import styles
from utils.snowflake_conn import sql_escape, sql_lit


# ── sql_escape ───────────────────────────────────────────────────────────────

def test_plain_value_is_unchanged():
    assert sql_escape("ENTITY_01") == "ENTITY_01"


def test_single_quote_is_doubled():
    assert sql_escape("O'Brien") == "O''Brien"


def test_backslash_is_doubled():
    assert sql_escape("C:\\temp") == "C:\\\\temp"


def test_trailing_backslash_cannot_escape_the_closing_quote():
    # The C2 bug: doubling only the quote leaves `'...\'` — the literal runs on.
    out = sql_lit("BAD\\")
    assert out == "'BAD\\\\'"
    # An even number of backslashes before the closing quote = a closed literal.
    body = out[1:-1]
    assert (len(body) - len(body.rstrip("\\"))) % 2 == 0


def test_backslash_and_quote_together_escape_in_the_right_order():
    # Backslashes FIRST, then quotes: a backslash introduced by quote-doubling
    # must never be doubled again.
    assert sql_escape("a\\'b") == "a\\\\''b"


def test_injection_attempt_stays_inside_the_literal():
    assert sql_escape("x' OR 1=1 --") == "x'' OR 1=1 --"


def test_none_becomes_an_empty_literal():
    assert sql_escape(None) == ""
    assert sql_lit(None) == "''"


def test_non_string_values_are_stringified():
    assert sql_escape(20260101) == "20260101"


def test_escaping_is_idempotent_only_when_applied_once():
    # Guard against a call site escaping twice: the result differs, so a
    # double-escape is a visible bug, not a silent one.
    once = sql_escape("O'Brien")
    assert sql_escape(once) != once


# ── render_filter_chips ──────────────────────────────────────────────────────

@pytest.fixture
def captured_markdown(monkeypatch):
    out = []
    for mod in (streamlit, styles.st):
        monkeypatch.setattr(mod, "markdown",
                            lambda body, *a, **k: out.append(str(body)),
                            raising=False)
    return out


def test_filter_chips_escape_script_tags(captured_markdown):
    styles.render_filter_chips({"TRADE_CODE": "<script>alert(1)</script>"})
    html = "".join(captured_markdown)
    assert "<script>" not in html
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in html


def test_filter_chips_escape_quotes_and_ampersands(captured_markdown):
    styles.render_filter_chips({"STRATEGY": 'a"b&c'})
    html = "".join(captured_markdown)
    assert 'a"b&c' not in html
    assert "a&quot;b&amp;c" in html


def test_filter_chips_cannot_break_out_of_the_span(captured_markdown):
    styles.render_filter_chips(
        {"CURVE_CODE": '</span><img src=x onerror=alert(1)>'})
    html = "".join(captured_markdown)
    # No live tag survives: the whole payload is inert text inside the chip.
    assert "<img" not in html
    assert "</span><img" not in html
    assert "&lt;/span&gt;&lt;img src=x onerror=alert(1)&gt;" in html


def test_filter_chips_still_render_the_plain_value(captured_markdown):
    styles.render_filter_chips({"ENTITY_CODE": "MUS", "BOOK_CODE": "BK1"})
    html = "".join(captured_markdown)
    assert "Entity: MUS" in html
    assert "Book: BK1" in html


def test_filter_chips_with_no_dimensions_says_all_records(captured_markdown):
    styles.render_filter_chips({})
    assert "All records (no filters)" in "".join(captured_markdown)
