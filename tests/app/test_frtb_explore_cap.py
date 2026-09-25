"""Guards on FRTB Explore's row cap and its download mechanism.

The page caps the query, the grid and the CSV with one constant, MAX_ROWS.
Marcos raised it from 1,000 to 5,000 on 2026-09-23 and asked to KEEP the
visible download button, because a button is easier to find than the grid's
hover toolbar.

That keeps the ceiling tied to the download mechanism rather than to the
query. `download_csv_link` embeds the CSV as a base64 data: URI inside the
page HTML, because st.download_button serves from a presigned Azure URL that
the corporate browser-isolation proxy rewrites and breaks. Base64 adds a
third, and the payload sits in the page and crosses the websocket on every
rerun. Measured at ~234 bytes of page HTML per row on the widest layout:

    5,000 rows = 1.2 MB      10,000 = 2.3 MB      50,000 = 11.7 MB

5,000 is comfortably under Streamlit's markdown trimming and the browser's
data-link ceiling. Past roughly 10,000 the mechanism has to change, not the
number.

The page calls Streamlit at import time, so these read the source with the
ast module rather than importing it.
"""
import ast
import os
import re

PAGE = os.path.join(os.path.dirname(__file__), "..", "..", "streamlit", "adjustment_engine", "pages",
                    "8_FRTB_Explore.py")
HELPER = os.path.join(os.path.dirname(__file__), "..", "..", "streamlit", "adjustment_engine", "utils", "styles.py")

#: Above this the data: URI download stops being safe. See the module
#: docstring: raising it means changing the download, not this number.
DATA_URI_CEILING = 10000


def _source(path):
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def _module_constant(path, name):
    tree = ast.parse(_source(path))
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == name:
                    return ast.literal_eval(node.value)
    raise AssertionError(name + " is not a module-level constant in " + path)


def _calls(path):
    """Every call in the module, as dotted names — comments cannot match."""
    names = []
    for node in ast.walk(ast.parse(_source(path))):
        if not isinstance(node, ast.Call):
            continue
        fn = node.func
        if isinstance(fn, ast.Attribute) and isinstance(fn.value, ast.Name):
            names.append(fn.value.id + "." + fn.attr)
        elif isinstance(fn, ast.Name):
            names.append(fn.id)
    return names


def test_max_rows_is_five_thousand():
    assert _module_constant(PAGE, "MAX_ROWS") == 5000


def test_max_rows_stays_within_the_data_uri_ceiling():
    """The download is a data: URI, so the cap is a payload-size decision."""
    cap = _module_constant(PAGE, "MAX_ROWS")
    assert cap <= DATA_URI_CEILING, (
        "MAX_ROWS is " + str(cap) + ". The CSV is embedded in the page as a "
        "base64 data: URI (about 234 bytes of page HTML per row), so this is "
        "roughly " + str(round(cap * 234 / 1e6, 1)) + " MB added to every "
        "rerun. Past ~10,000 rows change the download mechanism instead: the "
        "results grid is already a native st.dataframe whose hover toolbar "
        "downloads CSV client-side, with no presigned URL and nothing added "
        "to the page.")
    assert cap > 0


def test_the_blurb_quotes_the_real_cap():
    """Copy and behaviour must not drift.

    The page text said "up to 1,000 rows" while the constant was the single
    source of truth everywhere else, so raising the cap left the description
    lying. Every other mention interpolates MAX_ROWS; this one is prose.
    """
    cap = _module_constant(PAGE, "MAX_ROWS")
    src = _source(PAGE)
    quoted = set(re.findall(r"up to ([\d,]+) rows", src))
    assert quoted, "the page no longer describes its row cap in the blurb"
    assert quoted == {f"{cap:,}"}, (
        "The blurb says " + str(sorted(quoted)) + " but MAX_ROWS is "
        + f"{cap:,}" + ". Update the prose when you change the constant.")


def test_the_visible_download_button_is_kept():
    """Marcos asked to keep it: a button is easier to find than a toolbar."""
    assert "download_csv_link" in _calls(PAGE), (
        "The named download link is gone. It is deliberate — the grid's own "
        "toolbar download exists, but a visible button is easier to see.")


def test_the_download_avoids_the_presigned_url_path():
    """st.download_button is what the browser-isolation proxy breaks.

    Parsed, not grepped: the constant's own comment block explains why this
    call is avoided, and a substring search matched that explanation.
    """
    assert "st.download_button" not in _calls(PAGE), (
        "st.download_button serves from a presigned Azure URL that the "
        "corporate proxy rewrites, breaking the signature (live failure "
        "2026-09-03). Use download_csv_link, which embeds the bytes.")


def test_the_cap_applies_to_the_query_not_just_the_display():
    """A client-side cap would still pull every matching row over the wire."""
    src = _source(PAGE)
    assert "LIMIT {MAX_ROWS}" in src, (
        "MAX_ROWS must bound the SQL, so a wide filter cannot drag the whole "
        "table into the app before being trimmed.")


def test_the_helper_docstring_quotes_the_same_cap():
    """download_csv_link documents what its callers may hand it."""
    doc = _source(HELPER)
    assert "~5,000 rows" in doc, (
        "utils/styles.download_csv_link still documents the old caller cap. "
        "Its docstring is where the next person looks before raising a limit.")
