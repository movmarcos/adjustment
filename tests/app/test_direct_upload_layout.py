"""The Direct upload forms after 2026-09-25: rules panel, one panel for the
data + its COB/ENTITY_CODE, grid straight after, one entity per upload.

Marcos: "a user posted an adjustment, we saw that a few lines did not pass
and the grid was at the bottom and he did not see it" — so the grid moves
up, right after the upload panel. And: "don't allow multiple entity code …
where you identify the COB you will also identify the entity and then you
check the sign-off like you do for the Scaling adjustment."

Source-level: the page calls Streamlit at import and the file widgets have
no AppTest driver, so the layout is pinned by the order of the section
calls and the presence/absence of the pieces.
"""
import os
import re
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "streamlit", "adjustment_engine"))

PAGE = os.path.join(os.path.dirname(__file__), "..", "..", "streamlit", "adjustment_engine", "pages", "1_New_Adjustment.py")


@pytest.fixture(scope="module")
def src():
    with open(PAGE, encoding="utf-8") as fh:
        return fh.read()


def _fn(src, name):
    start = src.index(f"def {name}(")
    nxt = re.search(r"\n(?:def |# ─)", src[start + 10:])
    return src[start:start + 10 + (nxt.start() if nxt else len(src))]


FLOWS = {
    "per-row Direct": "render_direct_form",
    "VaR Upload":     "render_var_upload_form",
    "FRTB Direct":    "_render_frtb_direct_body",
}


# ── The rules panel ──────────────────────────────────────────────────────────

@pytest.mark.parametrize("flow,fn", FLOWS.items())
def test_every_direct_flow_opens_with_the_rules_panel(src, flow, fn):
    body = _fn(src, fn)
    assert "_render_rules_panel(" in body, flow + " has no 'What will be checked' panel"
    # Above the upload widget: the user reads the rules BEFORE choosing a file
    assert body.index("_render_rules_panel(") < body.index("st.radio("), (
        flow + ": the rules panel must sit above the input-mode radio")


def test_the_panel_is_collapsed_and_uses_the_pure_renderer(src):
    body = _fn(src, "_render_rules_panel")
    assert "expanded=False" in body
    assert "rules_html(" in body and "describe_rules(" in body
    assert "load_rule_docs(" in body


# ── One panel: data, then its COB and ENTITY_CODE, then the grid ─────────────

@pytest.mark.parametrize("flow,fn", [("VaR Upload", "render_var_upload_form"),
                                     ("FRTB Direct", "_render_frtb_direct_body")])
def test_file_flows_show_identity_inside_the_upload_card_then_the_grid(src, flow, fn):
    body = _fn(src, fn)
    card_close = body.index("_csv_card.__exit__(None, None, None)")
    assert body.index("_render_identity_fields(True)") < card_close, (
        flow + ": COB/ENTITY_CODE must be inside the upload card (one panel)")
    grid = body.index("render_data_grid(")
    assert card_close < grid < body.index('"Business Context"'), (
        flow + ": the grid must come right after the upload panel and before "
        "Business Context — a user missed failed rows when it sat last")
    assert '"Upload Details"' not in body, flow + " still has the separate Upload Details step"


def test_per_row_flow_grid_comes_before_business_context(src):
    body = _fn(src, "render_direct_form")
    assert body.index('"Staged Rows"') < body.index('"Business Context"')
    assert '"Batch Details"' not in body


def test_the_grid_never_sits_inside_a_card(src):
    """The resize-observer loop: a dataframe inside the styled card locks
    the page in the users' environment. Every grid renders after the card
    closes."""
    for fn in ("render_var_upload_form", "_render_frtb_direct_body"):
        body = _fn(src, fn)
        assert body.index("_csv_card.__exit__") < body.index("render_data_grid(")


# ── Identity: from the data only, exactly one value ──────────────────────────

def test_cob_and_entity_are_never_typed_in_the_file_flows(src):
    for fn in ("render_var_upload_form", "_render_frtb_direct_body"):
        body = _fn(src, fn)
        assert "_apply_identity(file_identity(df))" in body
        assert 'key=_k("var_entity")' not in body, (
            "the editable Entity Code box is back — the entity must come from "
            "the file, or the header can disagree with the rows")
        assert 'wiz["cobid"] = int(' not in body


def test_identity_fields_are_read_only(src):
    body = _fn(src, "_render_identity_fields")
    assert body.count("disabled=True") == 2
    assert "ENTITY_CODE" in body and "COB" in body


def test_apply_identity_blocks_on_missing_many_or_none(src):
    body = _fn(src, "_apply_identity")
    assert 'wiz["_entity_bad"] = ent is None or bool(ent.error)' in body
    assert 'wiz["_cob_bad"] = cob is None or bool(cob.error)' in body


def test_per_row_file_mode_refuses_to_stage_a_mixed_entity_batch(src):
    body = _fn(src, "render_direct_form")
    assert 'cob_bad = bool(wiz.get("_cob_bad") or wiz.get("_entity_bad")' in body
    assert "if unknown_cols or missing_cols or cob_bad or not len(ndf):" in body


def test_enter_rows_refuses_a_second_entity(src):
    body = _fn(src, "render_direct_form")
    assert "elif _have_ent and _new_ent not in _have_ent:" in body
    assert "covers exactly one ENTITY_CODE" in body


def test_switching_scope_or_mode_clears_the_identity(src):
    body = _fn(src, "render_direct_form")
    assert body.count("_clear_identity()") >= 2


# ── Completion checklist ─────────────────────────────────────────────────────

def test_every_direct_checklist_has_the_single_entity_check(src):
    body = _fn(src, "_completion_checks")
    assert body.count('not wiz.get("_entity_bad")') == 3, (
        "VaR Upload, FRTB Direct and per-row Direct must each list the "
        "single-ENTITY_CODE check")
    assert '("COB matches the file"' not in body, (
        "the typed-COB-must-match-file check is obsolete: the COB is the file's")


# ── Sign-off follows ─────────────────────────────────────────────────────────

def test_the_signoff_panel_reads_the_derived_entity(src):
    body = _fn(src, "_render_signoff_panel")
    assert 'entity = (wiz.get("entity_code") or "").strip() or None' in body
    assert "_signoff_state(scope, cobid, entity)" in body


def test_the_payload_carries_the_derived_entity_for_frtb_direct(src):
    body = _fn(src, "_build_payload")
    frtb = body[body.index('if cat == "VaR Upload" or _is_frtb_file_direct():'):]
    frtb = frtb[:frtb.index('if cat == "Entity Roll"')]
    assert '"entity_code":           wiz.get("entity_code", "")' in frtb


# ── Double paste and duplicate rows (2026-09-25) ─────────────────────────────

def test_every_paste_box_strips_a_repeated_paste(src):
    """A slow paste made a user paste again; the second copy must not count."""
    assert src.count("repeated_paste(csv_text.strip())") == 3, (
        "all three paste boxes (per-row, VaR Upload, FRTB) must go through "
        "repeated_paste")
    assert "_read_csv(StringIO(csv_text.strip()))" not in src, (
        "a paste box still parses the raw text — a double paste would go in twice")


@pytest.mark.parametrize("flow,fn", FLOWS.items())
def test_every_direct_flow_blocks_on_duplicate_rows(src, flow, fn):
    body = _fn(src, fn)
    assert "_guard_duplicates(" in body, flow + " never checks for duplicate rows"


def test_file_uploads_reset_the_paste_count(src):
    """A file after a double paste must not inherit the 'pasted twice' warning."""
    assert src.count('wiz["_paste_copies"] = 1\n') >= 4   # 3 file sites + clear


def test_the_per_row_flow_refuses_to_stage_duplicates(src):
    body = _fn(src, "render_direct_form")
    assert "or _dups_blocked)" in body


def test_the_guard_offers_a_one_click_fix_scoped_to_the_content(src):
    body = _fn(src, "_guard_duplicates")
    assert "Remove duplicate rows (keep first)" in body
    assert 'wiz["_dedupe_sig"] = sig' in body
    assert 'if wiz.get("_dedupe_sig") == sig:' in body, (
        "a de-dupe must apply to this content only; a new paste starts clean")


def test_every_direct_checklist_has_the_duplicate_check(src):
    body = _fn(src, "_completion_checks")
    assert body.count('not wiz.get("_dups_bad")') == 3
