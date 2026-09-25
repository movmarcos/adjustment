"""Guards on deploy.py's "skip unchanged files" upload optimisation.

Every PUT to a stage is a network round trip, and on a normal deploy almost
every one of the seventeen app files is byte-identical to what is already
staged. Comparing MD5s and uploading only what changed is most of why a
Streamlit-only deploy got faster.

The risk is asymmetric. Uploading a file that did not need it costs a second.
NOT uploading a file that did change means the deploy silently ships nothing,
and the developer stares at an unchanged app wondering why their fix did not
land. So these tests lean hard on the "must still upload" direction.

deploy.py lives in streamlit/ (beside the app folders it deploys), so this
file puts that directory on sys.path itself.
"""
import hashlib
import os
import sys

import pytest

_DEPLOY_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "streamlit"))
if _DEPLOY_DIR not in sys.path:
    sys.path.insert(0, _DEPLOY_DIR)

import deploy


# ── file_md5 ─────────────────────────────────────────────────────────────────

def test_file_md5_matches_hashlib(tmp_path):
    target = tmp_path / "app.py"
    target.write_bytes(b"import streamlit as st\n")
    assert deploy.file_md5(str(target)) == hashlib.md5(
        b"import streamlit as st\n").hexdigest()


def test_file_md5_changes_with_content(tmp_path):
    target = tmp_path / "styles.py"
    target.write_bytes(b"a")
    before = deploy.file_md5(str(target))
    target.write_bytes(b"b")
    assert deploy.file_md5(str(target)) != before, (
        "A changed file must hash differently, or the deploy will skip it.")


def test_file_md5_reads_the_whole_file(tmp_path):
    """Hashing is chunked; a difference past the first chunk must still show."""
    target = tmp_path / "big.py"
    payload = b"x" * (2 << 20)
    target.write_bytes(payload + b"A")
    before = deploy.file_md5(str(target))
    target.write_bytes(payload + b"B")
    assert deploy.file_md5(str(target)) != before, (
        "Only the first chunk is being hashed — a late edit would be skipped.")


def test_file_md5_is_binary_safe(tmp_path):
    """CRLF must not be normalised away: the staged bytes are what count."""
    unix = tmp_path / "unix.py"
    dos = tmp_path / "dos.py"
    unix.write_bytes(b"a\nb\n")
    dos.write_bytes(b"a\r\nb\r\n")
    assert deploy.file_md5(str(unix)) != deploy.file_md5(str(dos))


# ── staged_md5_map ───────────────────────────────────────────────────────────

class _Row:
    def __init__(self, data):
        self._data = data

    def asDict(self):
        return dict(self._data)


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def collect(self):
        return self._rows


class _Session:
    def __init__(self, rows=None, raises=False):
        self.rows = rows or []
        self.raises = raises
        self.queries = []

    def sql(self, query, *a, **k):
        self.queries.append(query)
        if self.raises:
            raise RuntimeError("stage does not exist")
        return _Result(self.rows)


def test_staged_md5_map_strips_the_stage_name_prefix():
    """LIST reports "stage_name/path/to/file"; keys must be the path only."""
    session = _Session([
        _Row({"name": "streamlit_adjustment_stage/app.py", "md5": "AAA"}),
        _Row({"name": "streamlit_adjustment_stage/utils/styles.py", "md5": "BBB"}),
    ])
    got = deploy.staged_md5_map(session, "ADJUSTMENT_APP.STREAMLIT_ADJUSTMENT_STAGE")
    assert got == {"app.py": "aaa", "utils/styles.py": "bbb"}


def test_staged_md5_map_returns_empty_when_list_fails():
    """A missing stage must degrade to uploading everything, not explode."""
    assert deploy.staged_md5_map(_Session(raises=True), "X.Y") == {}


def test_staged_md5_map_ignores_rows_without_an_md5():
    session = _Session([
        _Row({"name": "stage/app.py", "md5": ""}),
        _Row({"name": "stage/utils/styles.py", "md5": "BBB"}),
    ])
    got = deploy.staged_md5_map(session, "X.Y")
    assert "app.py" not in got, (
        "A row with no md5 must not become a key, or the file would compare "
        "equal to nothing and could be skipped on a later refactor.")
    assert got["utils/styles.py"] == "bbb"


# ── the comparison the upload loop performs ──────────────────────────────────

def _would_skip(session, stage, display_name, local_path):
    """Reproduces the upload loop's decision for one file."""
    staged = deploy.staged_md5_map(session, stage)
    return staged.get(display_name.lower()) == deploy.file_md5(local_path)


def test_unchanged_file_is_skipped(tmp_path):
    target = tmp_path / "styles.py"
    target.write_bytes(b"css")
    session = _Session([_Row({"name": "stage/utils/styles.py",
                              "md5": deploy.file_md5(str(target))})])
    assert _would_skip(session, "X.Y", "utils/styles.py", str(target)) is True


def test_changed_file_is_uploaded(tmp_path):
    target = tmp_path / "styles.py"
    target.write_bytes(b"css v2")
    session = _Session([_Row({"name": "stage/utils/styles.py",
                              "md5": "somethingelse"})])
    assert _would_skip(session, "X.Y", "utils/styles.py", str(target)) is False, (
        "A file whose content differs from the stage MUST upload. Skipping it "
        "is the silent-no-op failure this optimisation could introduce.")


def test_new_file_not_on_the_stage_is_uploaded(tmp_path):
    target = tmp_path / "new_page.py"
    target.write_bytes(b"new")
    assert _would_skip(_Session([]), "X.Y", "pages/new_page.py",
                       str(target)) is False


def test_same_name_in_a_different_directory_is_not_confused(tmp_path):
    """utils/config.py and .streamlit/config.py must compare separately."""
    target = tmp_path / "config.py"
    target.write_bytes(b"real")
    session = _Session([_Row({"name": "stage/utils/config.py",
                              "md5": deploy.file_md5(str(target))})])
    assert _would_skip(session, "X.Y", "utils/config.py", str(target)) is True
    assert _would_skip(session, "X.Y", "pages/config.py", str(target)) is False


def test_stage_paths_compare_case_insensitively(tmp_path):
    target = tmp_path / "App.py"
    target.write_bytes(b"x")
    session = _Session([_Row({"name": "stage/App.py",
                              "md5": deploy.file_md5(str(target)).upper()})])
    assert _would_skip(session, "X.Y", "App.py", str(target)) is True


# ── timing instrumentation ───────────────────────────────────────────────────

def test_phase_timer_records_a_labelled_entry():
    before = len(deploy._PHASE_TIMES)
    with deploy.phase_timer("unit test phase"):
        pass
    assert len(deploy._PHASE_TIMES) == before + 1
    label, secs = deploy._PHASE_TIMES[-1]
    assert label == "unit test phase"
    assert secs >= 0
    deploy._PHASE_TIMES.pop()


def test_phase_timer_records_even_when_the_phase_raises():
    before = len(deploy._PHASE_TIMES)
    with pytest.raises(ValueError):
        with deploy.phase_timer("failing phase"):
            raise ValueError("boom")
    assert len(deploy._PHASE_TIMES) == before + 1, (
        "A phase that fails is exactly the one you want timed.")
    deploy._PHASE_TIMES.pop()


def test_timing_breakdown_is_ordered_slowest_first(capsys):
    saved = list(deploy._PHASE_TIMES)
    deploy._PHASE_TIMES[:] = [("fast", 1.0), ("slowest", 90.0), ("middle", 10.0)]
    try:
        deploy.print_timing_breakdown()
        out = capsys.readouterr().out
    finally:
        deploy._PHASE_TIMES[:] = saved
    positions = [out.index(name) for name in ("slowest", "middle", "fast")]
    assert positions == sorted(positions), out
    assert "TOTAL" in out


def test_timing_breakdown_is_silent_when_nothing_was_timed(capsys):
    saved = list(deploy._PHASE_TIMES)
    deploy._PHASE_TIMES[:] = []
    try:
        deploy.print_timing_breakdown()
        assert capsys.readouterr().out == ""
    finally:
        deploy._PHASE_TIMES[:] = saved
