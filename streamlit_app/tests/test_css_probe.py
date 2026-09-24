"""The CSS probe must cost normal users nothing.

Added 2026-09-24 to identify a horizontal line running across every page,
over the menu as well as the content. Devtools is unavailable in the
corporate browser and the app is on an internal network, so neither
inspecting nor automating it is possible. The page has to report on itself.

It is temporary. These tests exist mainly to guarantee it is inert for
everyone who has not asked for it, because a diagnostic that leaks into
normal use is worse than no diagnostic.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from utils import styles


class _QP(dict):
    pass


@pytest.fixture
def captured(monkeypatch):
    """Record anything the probe would render."""
    seen = {"markdown": [], "html": []}
    monkeypatch.setattr(styles.st, "markdown",
                        lambda body, *a, **k: seen["markdown"].append(body))

    class _Components:
        @staticmethod
        def html(body, **k):
            seen["html"].append(body)

    import streamlit.components.v1 as real
    monkeypatch.setattr(real, "html", _Components.html)
    return seen


def _set_params(monkeypatch, **kw):
    monkeypatch.setattr(styles.st, "query_params", _QP(kw))


def test_it_renders_nothing_without_the_flag(monkeypatch, captured):
    _set_params(monkeypatch)
    styles.render_css_probe()
    assert captured["markdown"] == [] and captured["html"] == [], (
        "The probe rendered for a user who did not ask for it.")


@pytest.mark.parametrize("value", ["", "other", "CSSX", "1"])
def test_only_the_exact_flag_turns_it_on(monkeypatch, captured, value):
    _set_params(monkeypatch, debug=value)
    styles.render_css_probe()
    assert captured["html"] == [], "turned on by debug=" + repr(value)


@pytest.mark.parametrize("value", ["css", "CSS", " css "])
def test_the_flag_turns_it_on(monkeypatch, captured, value):
    _set_params(monkeypatch, debug=value)
    styles.render_css_probe()
    assert captured["html"], "debug=" + repr(value) + " did not enable it"


def test_a_query_param_failure_is_not_fatal(monkeypatch, captured):
    """Some runtimes raise on query_params; a probe must never break a page."""
    class _Boom:
        def get(self, *a, **k):
            raise RuntimeError("no query params here")
    monkeypatch.setattr(styles.st, "query_params", _Boom())
    styles.render_css_probe()          # must not raise
    assert captured["html"] == []


def test_it_reports_when_it_cannot_read_the_host_page(monkeypatch, captured):
    """It relies on the component frame being same-origin, which may change."""
    _set_params(monkeypatch, debug="css")
    styles.render_css_probe()
    js = captured["html"][0]
    assert "BLOCKED" in js, (
        "If the frame cannot reach the host page the probe must SAY so. "
        "Silence would read as 'nothing is drawing a line', which is the "
        "opposite of the truth.")


def test_it_looks_for_every_way_a_line_can_be_painted(monkeypatch, captured):
    """Widened after two rounds of the probe's first version finding nothing.

    A line is not only a thin filled strip or a top border. It can be a
    border on any side of a wide box, an outline, a box-shadow, or a
    pseudo-element — and pseudo-elements do not appear in querySelectorAll,
    so they have to be asked for by name.
    """
    _set_params(monkeypatch, debug="css")
    styles.render_css_probe()
    js = captured["html"][0]
    assert "window.parent.document" in js
    assert "backgroundColor" in js
    assert "'border' + side + 'Width'" in js, "borders on all four sides"
    assert "outlineWidth" in js and "boxShadow" in js
    for pseudo in ("'::before'", "'::after'"):
        assert pseudo in js, "pseudo-elements must be inspected explicitly"


def test_it_says_so_when_nothing_matches(monkeypatch, captured):
    """An empty list must read as a finding, not as a blank."""
    _set_params(monkeypatch, debug="css")
    styles.render_css_probe()
    assert "NOTHING MATCHED" in captured["html"][0], (
        "If nothing on the page can paint a line, the probe has to say that "
        "out loud — it is the result that rules the app out entirely.")


def test_it_reports_position_so_the_line_can_be_matched(monkeypatch, captured):
    _set_params(monkeypatch, debug="css")
    styles.render_css_probe()
    js = captured["html"][0]
    for field in ("y=", "h=", "testid=", "class="):
        assert field in js, "the probe output omits " + field


def test_the_sidebar_calls_it_last(monkeypatch):
    """Outside `with st.sidebar`, so the output lands in the main area."""
    import ast
    src = open(os.path.join(os.path.dirname(__file__), "..", "utils",
                            "styles.py"), encoding="utf-8").read()
    fn = next(n for n in ast.parse(src).body
              if isinstance(n, ast.FunctionDef) and n.name == "render_sidebar")
    last = fn.body[-1]
    assert (isinstance(last, ast.Expr) and isinstance(last.value, ast.Call)
            and getattr(last.value.func, "id", "") == "render_css_probe"), (
        "The probe must be the last statement of render_sidebar and outside "
        "the sidebar block, or its output is unreadable inside the menu.")
