"""Guards on the Streamlit chrome CSS that inject_css() emits.

Why this file exists: `header[data-testid="stHeader"]` was set to
`display: none` to kill the recurring "white box" — Streamlit's chrome bar is
fixed, solid white and high z-index, so page content scrolled under it and
read as a white block over the grids.

But the control that re-opens a COLLAPSED sidebar is a child of that header.
Hiding the header therefore removed the only way back to the menu: a user who
collapsed the sidebar once had no button to restore it, and no way out short
of reloading the page. Marcos hit exactly that on 2026-09-23.

Keeping that button reachable turned out to be the wrong fix — it moves
between testids across Streamlit releases, and Marcos still could not find it.
So the failure mode is gone instead: the sidebar is not collapsible at all.

These tests pin that. Every control that collapses the menu stays hidden, a
sidebar that reports itself collapsed is forced back open, and the header is
still never removed from the layout (that is what caused the original bug, and
what the white-box rules must not go back to).
"""
import os
import re
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import streamlit
from utils import styles


@pytest.fixture(scope="module")
def css():
    """The stylesheet inject_css() writes, captured rather than rendered."""
    captured = []
    original = streamlit.markdown
    streamlit.markdown = lambda body, *a, **k: captured.append(body)
    try:
        styles.inject_css()
    finally:
        streamlit.markdown = original
    assert captured, "inject_css() emitted nothing"
    return "\n".join(captured)


def _norm(text):
    """Strip all whitespace, so `a > *` and `a>*` compare equal."""
    return re.sub(r"\s+", "", text)


def _block_for(css_text, selector):
    """Declarations of the LAST rule whose selector list contains `selector`.

    Matched against whole comma-separated selectors, not as a substring: a
    substring match would let `header[...] > *` shadow `header[...]` and make
    these assertions read the wrong rule.

    Returns "" when no rule targets it.
    """
    want = _norm(selector)
    # Comments sit between rules, so they land in the selector capture below
    # and would stop any selector from comparing equal. Strip them first.
    stripped = re.sub(r"/\*.*?\*/", " ", css_text, flags=re.S)
    out = ""
    for match in re.finditer(r"([^{}]+)\{([^{}]*)\}", stripped):
        selectors, body = match.group(1), match.group(2)
        if want in [_norm(part) for part in selectors.split(",")]:
            out = body
    return out


# ── The regression itself ────────────────────────────────────────────────────

def test_header_is_never_display_none(css):
    """display:none on the header takes the sidebar control down with it."""
    body = _block_for(css, 'header[data-testid="stHeader"]')
    assert body, "no rule targets the Streamlit header at all"
    normalised = _norm(body)
    assert "display:none" not in normalised, (
        "The Streamlit header is display:none again. The control that re-opens "
        "a collapsed sidebar (stSidebarCollapsedControl) is a CHILD of this "
        "header, so this strands the user with no menu and no way back except "
        "a page reload. Neutralise the bar instead: transparent background, "
        "zero height, pointer-events none.")


@pytest.mark.parametrize("selector", [
    '[data-testid="stSidebarCollapseButton"]',
    '[data-testid="stSidebarCollapsedControl"]',
    '[data-testid="stSidebarNavCollapseIcon"]',
    '[data-testid="collapsedControl"]',
])
def test_every_collapse_control_is_hidden(css, selector):
    """The menu must not be collapsible — that is the whole fix."""
    body = _norm(_block_for(css, selector))
    assert "display:none" in body, (
        selector + " is not hidden. If any collapse control survives, a user "
        "can hide the menu again and be stuck, because the control that "
        "reopens it moves between testids across Streamlit releases.")


def test_a_collapsed_sidebar_is_forced_back_open(css):
    """Rescues a viewer who collapsed the menu before this shipped.

    Their browser still carries the collapsed state, and with the button now
    hidden they would have no way back at all.
    """
    body = _norm(_block_for(css, '[data-testid="stSidebar"][aria-expanded="false"]'))
    assert body, "nothing overrides a sidebar that reports itself collapsed"
    assert "transform:none" in body, (
        "Streamlit slides the panel off-screen with a transform; it must be "
        "cancelled or the sidebar stays invisible.")
    assert "margin-left:0" in body, (
        "A negative left margin also hides the panel; it must be cancelled.")
    for killer in ("display:none", "visibility:hidden", "opacity:0"):
        assert killer not in body, (
            "The collapsed-state rule suppresses the sidebar with " + killer)
    assert "width:244px" in body and "min-width:244px" in body, (
        "A rescued sidebar needs a real width, or it reopens at zero pixels.")


def test_expanded_sidebar_width_is_not_forced(css):
    """Resizing by dragging the edge must still work when expanded."""
    body = _norm(_block_for(css, '[data-testid="stSidebar"]'))
    assert "width:244px!important" not in body, (
        "Forcing a width on the sidebar in every state breaks drag-to-resize. "
        "Force it only in the collapsed-state rule that rescues it.")


def test_header_does_not_reserve_vertical_space(css):
    """The white-box fix must survive: no solid bar over the content."""
    body = _norm(_block_for(css, 'header[data-testid="stHeader"]'))
    assert "height:0" in body, (
        "The header must not reserve height, or page content scrolls under a "
        "fixed bar again — the white box bug this rule originally fixed.")
    assert "background:transparent" in body, (
        "The header must be transparent, or the solid white bar returns.")


def test_header_does_not_swallow_clicks(css):
    body = _norm(_block_for(css, 'header[data-testid="stHeader"]'))
    assert "pointer-events:none" in body, (
        "A zero-height header can still cover content with an invisible hit "
        "area. It must be click-through.")


def test_children_of_the_header_stay_clickable(css):
    body = _norm(_block_for(css, 'header[data-testid="stHeader"] > *'))
    assert "pointer-events:auto" in body, (
        "pointer-events:none on the header inherits to its children, which "
        "includes the sidebar control. Children must opt back in.")


# ── The chrome we do still want gone ─────────────────────────────────────────

@pytest.mark.parametrize("testid", ["stDecoration", "stToolbar"])
def test_unwanted_chrome_is_hidden_by_name(css, testid):
    """Hide these individually, never by removing their parent header."""
    body = _norm(_block_for(css, '[data-testid="' + testid + '"]'))
    assert "display:none" in body, (
        testid + " should be hidden explicitly. Snowsight supplies its own "
        "chrome, and hiding these by name is what lets the header itself stay "
        "in the layout.")


def test_no_blanket_rule_hides_every_header_child(css):
    """A catch-all would silently take the sidebar control with it."""
    for pattern in ('header[data-testid="stHeader"] *',
                    'header[data-testid="stHeader"]*'):
        body = _norm(_block_for(css, pattern))
        assert "display:none" not in body, (
            "A blanket rule hides every child of the header, including the "
            "sidebar control. Name the elements to hide instead.")
