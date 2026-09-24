"""The app must carry NO custom Streamlit theme. Ever. Here is why.

2026-09-24. A faint horizontal line crossed every page of the app: over the
sidebar as well as the content, fixed in place while the page scrolled,
visible while the app was still loading, unmoved by disabling the entire
stylesheet. Six rounds of bare control apps deployed next to the engine
located it:

  bare app                              no line
  + config file with no sections        no line
  + [client] section only               no line
  + EMPTY [theme] section               LINE
  + any single theme colour             LINE (all five, separately)
  + theme as dotted keys, no header     LINE
  + theme set from the script, no file  LINE
  same app, same runtime, run locally   no line

An empty [theme] section sends nothing to Streamlit's front-end in 1.50, so
Streamlit itself was not the difference; Snowsight was. Snowsight draws the
line whenever an app has any custom theme. It is a Snowflake defect and it
cannot be worked around from inside the app, so the app has no theme and
the Documentation page tells users it is designed for Snowsight light mode.

This file makes sure nobody puts a theme back "to fix dark mode" without
finding this history first. If Snowflake fixes their side, delete this test
along with the note in deploy.py.
"""
import ast
import glob
import os
import re

ROOT = os.path.join(os.path.dirname(__file__), "..", "..")
APP = os.path.join(ROOT, "streamlit_app")


def _source(path):
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def _py_files():
    return [f for f in glob.glob(os.path.join(APP, "**", "*.py"), recursive=True)
            if os.sep + "tests" + os.sep not in f]


def test_no_streamlit_config_file_ships_with_the_app():
    found = glob.glob(os.path.join(APP, ".streamlit", "*"))
    assert not found, (
        "streamlit_app/.streamlit/ exists: " + repr(found) + ". A [theme] "
        "section in there — even an empty one — makes Snowsight draw a line "
        "across every page. See this file's docstring before restoring it.")


def test_the_deploy_does_not_upload_a_config_directory():
    src = _source(os.path.join(ROOT, "deploy.py"))
    assert "st_cfg_dir" not in src
    assert ".glob('*.toml')" not in src, (
        "deploy.py uploads .toml files again. The theme file is the thing "
        "that produced the line.")


def test_no_theme_option_is_set_from_the_script():
    """Setting theme.* from the script reproduces the line without a file."""
    pattern = re.compile(r"set_option\(\s*[\"']theme\.")
    for f in _py_files():
        assert not pattern.search(_source(f)), (
            os.path.relpath(f, ROOT) + " sets a theme option at runtime. "
            "That reproduces the line exactly as the config file did "
            "(control app T12, 2026-09-24).")


def test_the_page_config_carries_no_theme():
    """set_page_config has no theme argument today; this catches one arriving."""
    for f in _py_files():
        tree = ast.parse(_source(f))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) \
                    and node.func.attr == "set_page_config":
                assert not any(k.arg == "theme" for k in node.keywords), (
                    os.path.relpath(f, ROOT) + " passes a theme to "
                    "set_page_config.")


def test_the_docs_tell_users_about_light_mode():
    docs = _source(os.path.join(APP, "pages", "7_Documentation.py"))
    assert "Appearance" in docs and "Light" in docs, (
        "The Troubleshooting tab no longer tells dark-mode users how to "
        "switch Snowsight to light.")
    start = docs.index('_KNOWLEDGE = """'); end = docs.index('""".strip()', start)
    assert "LIGHT mode" in docs[start:end], (
        "The assistant's knowledge no longer mentions light mode.")


def test_the_temporary_diagnostics_are_gone():
    """The Diagnostics expander and CSS probe were scaffolding for this hunt."""
    styles = _source(os.path.join(APP, "utils", "styles.py"))
    for name in ("_debug_flag", "render_css_probe", "_dbg_nocss", "_dbg_probe"):
        assert name not in styles, name + " is still in utils/styles.py."
