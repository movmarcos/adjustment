# Multi-Scope Scaling Implementation Plan (Part A of 2)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** In the New Adjustment page, Scaling picks the Adjustment Type before the Data Scope, Scaling and Entity Roll accept several scopes at once (one adjustment per scope, FRTBALL removed), and the filter form only offers fields the engine can apply to every selected scope.

**Architecture:** Page-only change plus one new pure module. A new `streamlit_app/utils/scope_filters.py` holds the per-scope filter applicability table and a pure `filter_layout(scopes)` function (unit-tested without Streamlit). `1_New_Adjustment.py` gets a `wiz["process_types"]` list for Scaling/Entity Roll, a multi-select pill group, and its FRTBALL fan-out loops generalised to that list. The server is untouched (it already refuses FRTBALL and stores one PROCESS_TYPE per header).

**Tech Stack:** Streamlit 1.50 (Streamlit-in-Snowflake), pandas, pytest. Local unit tests run in the scratchpad venv which has streamlit 1.50 + pandas + pytest installed: `VENV=/private/tmp/claude-501/-Users-marcosmagri-Documents-MUFG-adjustment/202b9189-fe88-405b-a79e-040561cbbd73/scratchpad/venv/bin/python` (if missing: `python3 -m venv venv && venv/bin/pip install streamlit==1.50.0 pandas pytest`).

**Spec:** `docs/superpowers/specs/2026-09-17-transfer-book-multi-scope-design.md` (sections 1–3, 3.5). Part B (Transfer Book) is `docs/superpowers/plans/2026-09-17-transfer-book.md` and assumes this plan is done.

## Global Constraints

- Streamlit runtime is pinned to 1.50.0 (`environment.yml`); `st.pills` (≥ 1.40) may be used, guarded by `_st_version()` from `utils/styles.py` with a `st.multiselect` fallback.
- Scope CODES stay as stored (`VaR, Stress, Sensitivity, FRTB, FRTBDRC, FRTBRRAO`); the UI shows `FRTB` as `FRTBSBM` only through `scope_label()` / `scope_code()` (`utils/styles.py` l.265–288). Never store a label.
- `SCOPE_CONFIG` (styles l.254) is presentation-only; `ALL_SCOPES` (styles l.159) is the list of storable scopes.
- No environment names hard-coded outside `config.py`.
- Snowflake UAT tests (`tests/`) cannot run on this machine; page/unit tests run locally with the venv above. Never mark a Snowflake step as verified.
- Commit after every task (no push). Commit messages end with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`.
- Work from the repo root `/Users/marcosmagri/Documents/MUFG/adjustment`. Page path: `streamlit_app/pages/1_New_Adjustment.py` (3715 lines; line numbers below are from commit 5d02c2b and shift as you edit — search for the quoted code).

## File Structure

- Create `streamlit_app/utils/scope_filters.py` — filter catalogue (`FIELD_LABELS`, `SCOPE_TIER2_ORDER`, `SCOPE_FILTER_FIELDS`, `MAIN_FIELDS_SINGLE`, `MAIN_FIELDS_MULTI`) and `filter_layout(scopes) -> {"main": [...], "more": [...]}`; `allowed_filter_keys(scopes) -> set`.
- Create `streamlit_app/tests/conftest.py` + `streamlit_app/tests/test_scope_filters.py` — local unit tests (no Snowflake).
- Modify `streamlit_app/pages/1_New_Adjustment.py` — wizard defaults, scope pills, section order, filter rendering, submit/preview/sign-off fan-out, checklist, ticket.
- Modify `streamlit_app/pages/7_Documentation.py` — knowledge text (FRTBALL sentence).

---

### Task 1: Pure filter-applicability module with unit tests

**Files:**
- Create: `streamlit_app/utils/scope_filters.py`
- Create: `streamlit_app/tests/conftest.py`
- Create: `streamlit_app/tests/test_scope_filters.py`

**Interfaces:**
- Produces: `FIELD_LABELS: dict[str, tuple[str, str]]` (key → (label, placeholder)); `SCOPE_FILTER_FIELDS: dict[str, frozenset[str]]`; `MAIN_FIELDS_SINGLE: tuple`; `MAIN_FIELDS_MULTI: tuple`; `VAR_ONLY_FIELDS: frozenset`; `filter_layout(scopes: list[str]) -> dict` with keys `main: list[str]`, `more: list[str]`; `allowed_filter_keys(scopes) -> set[str]`; `EXTRA_FIELD_ORDER: list[str]`.

- [ ] **Step 1: Write the failing tests**

```python
# streamlit_app/tests/conftest.py
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))   # streamlit_app/
```

```python
# streamlit_app/tests/test_scope_filters.py
from utils.scope_filters import (filter_layout, allowed_filter_keys,
                                 SCOPE_FILTER_FIELDS, MAIN_FIELDS_SINGLE,
                                 MAIN_FIELDS_MULTI, FIELD_LABELS)

ALL6 = ["VaR", "Stress", "Sensitivity", "FRTB", "FRTBDRC", "FRTBRRAO"]

def test_every_scope_has_a_row_and_every_key_a_label():
    assert set(SCOPE_FILTER_FIELDS) == set(ALL6)
    for keys in SCOPE_FILTER_FIELDS.values():
        assert keys <= set(FIELD_LABELS), keys - set(FIELD_LABELS)

def test_dead_fields_are_offered_nowhere():
    for k in ("batch_region_area", "murex_family", "murex_group"):
        assert all(k not in SCOPE_FILTER_FIELDS[s] for s in ALL6)

def test_single_scope_layout_matches_today():
    lay = filter_layout(["Sensitivity"])
    assert lay["main"] == list(MAIN_FIELDS_SINGLE)
    # tier-2 order first (as SCOPE_FIELDS had it), then the rest alphabetically by catalogue order
    assert lay["more"][:4] == ["measure_type_code", "strategy", "trade_typology", "instrument_code"]
    assert "simulation_name" not in lay["more"]          # Stress-only
    assert "scenario_date_id" not in lay["more"]         # VaR-only

def test_var_single_scope_includes_var_fields_in_main():
    lay = filter_layout(["VaR"])
    assert lay["main"] == list(MAIN_FIELDS_SINGLE) + ["var_component_name", "var_sub_component_name", "day_type"]
    assert "measure_type_code" not in lay["more"]

def test_multi_scope_main_is_eight_fields():
    lay = filter_layout(["VaR", "Stress"])
    assert lay["main"] == list(MAIN_FIELDS_MULTI)
    assert lay["more"] == ["trader_code", "guaranteed_entity", "region_key"]

def test_multi_scope_intersection_stress_sens():
    lay = filter_layout(["Stress", "Sensitivity"])
    assert set(lay["more"]) == {"trader_code", "guaranteed_entity", "region_key",
                                "product_category_attributes"}

def test_all_six_scopes():
    lay = filter_layout(ALL6)
    assert lay["main"] == list(MAIN_FIELDS_MULTI)
    assert lay["more"] == ["trader_code", "guaranteed_entity", "region_key"]

def test_empty_selection_offers_nothing_beyond_main():
    lay = filter_layout([])
    assert lay["main"] == list(MAIN_FIELDS_SINGLE) and lay["more"] == []

def test_allowed_keys_is_main_plus_more():
    lay = filter_layout(["FRTB", "FRTBDRC"])
    assert allowed_filter_keys(["FRTB", "FRTBDRC"]) == set(lay["main"]) | set(lay["more"])
```

- [ ] **Step 2: Run to verify they fail**

Run: `cd /Users/marcosmagri/Documents/MUFG/adjustment && $VENV -m pytest streamlit_app/tests -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'utils.scope_filters'`

- [ ] **Step 3: Write the module**

```python
# streamlit_app/utils/scope_filters.py
"""Which Scaling filter fields the ENGINE can apply per scope, and the form
layout for one or several scopes.

Derived (2026-09-17) from SP_PROCESS_ADJUSTMENT's lookup rules (05:_dim_filters
— an EXISTS is emitted only when the fact table has the KEY column) and the
direct column-equality joins, checked against fact_schema.csv. Keep this table
in step with the engine: a filter offered here but not applied there would be
silently ignored. batch_region_area / murex_family / murex_group exist on no
fact table and are therefore offered nowhere (old headers keep their values).
"""

# key → (label, placeholder) — catalogue of every offerable filter.
FIELD_LABELS = {
    "entity_code":                 ("Entity Code *",              ""),
    "source_system_code":          ("Source System",              "e.g. MS"),
    "department_code":             ("Department Code †",          ""),
    "book_code":                   ("Book Code †",                ""),
    "instrument_code":             ("Instrument Code",            "e.g. US4642872422 US"),
    "strategy":                    ("Strategy",                   "e.g. SSA00306"),
    "trade_typology":              ("Trade Typology",             "e.g. EQTT"),
    "trade_code":                  ("Trade Code",                 ""),
    "var_component_name":          ("VaR Component †",            ""),
    "var_sub_component_name":      ("VaR Sub-Component",          ""),
    "day_type":                    ("Day Type",                   ""),
    "currency_code":               ("Currency Code",              "e.g. USD"),
    "simulation_name":             ("Simulation Name",            "e.g. MRM_GLB_Std_EQ_M_PriceDnVolUp"),
    "simulation_source":           ("Simulation Source",          ""),
    "measure_type_code":           ("Measure Type Code",          "e.g. FxDeltaExp"),
    "trader_code":                 ("Trader Code",                ""),
    "guaranteed_entity":           ("Guaranteed Entity",          ""),
    "region_key":                  ("Region Key",                 ""),
    "scenario_date_id":            ("Scenario Date ID",           ""),
    "tenor_code":                  ("Tenor Code",                 ""),
    "underlying_tenor_code":       ("Underlying Tenor Code",      ""),
    "curve_code":                  ("Curve Code",                 ""),
    "product_category_attributes": ("Product Category Attributes", ""),
}

MAIN_FIELDS_SINGLE = ("entity_code", "source_system_code", "department_code", "book_code")
MAIN_FIELDS_MULTI  = MAIN_FIELDS_SINGLE + ("instrument_code", "strategy",
                                           "trade_typology", "trade_code")
VAR_ONLY_FIELDS = frozenset({"var_component_name", "var_sub_component_name", "day_type"})

# Applied to every scope: direct columns or BOOK_KEY / TRADE_KEY /
# COMMON_INSTRUMENT_KEY lookups (all six fact tables carry those keys).
_COMMON = frozenset({
    "entity_code", "source_system_code", "department_code", "book_code",
    "trader_code", "guaranteed_entity", "region_key",
    "trade_code", "strategy", "trade_typology", "instrument_code",
})

SCOPE_FILTER_FIELDS = {
    "VaR":         _COMMON | {"currency_code", "scenario_date_id"} | VAR_ONLY_FIELDS,
    "Stress":      _COMMON | {"simulation_name", "simulation_source",
                              "product_category_attributes"},
    "Sensitivity": _COMMON | {"currency_code", "measure_type_code", "tenor_code",
                              "underlying_tenor_code", "curve_code",
                              "product_category_attributes"},
    "FRTB":        _COMMON | {"currency_code", "measure_type_code", "simulation_name",
                              "tenor_code", "curve_code", "product_category_attributes"},
    "FRTBDRC":     _COMMON | {"currency_code", "measure_type_code", "simulation_name",
                              "product_category_attributes"},
    "FRTBRRAO":    _COMMON | {"currency_code", "measure_type_code", "simulation_name",
                              "product_category_attributes"},
}
SCOPE_FILTER_FIELDS = {k: frozenset(v) for k, v in SCOPE_FILTER_FIELDS.items()}

# Single-scope "More filters": the scope's own fields first (the old tier-2
# order), then the rest in catalogue order.
SCOPE_TIER2_ORDER = {
    "VaR":         [],
    "Stress":      ["simulation_name", "trade_typology", "instrument_code"],
    "Sensitivity": ["measure_type_code", "strategy", "trade_typology", "instrument_code"],
    "FRTB":        ["measure_type_code", "instrument_code", "strategy"],
    "FRTBDRC":     ["measure_type_code", "instrument_code", "strategy"],
    "FRTBRRAO":    ["measure_type_code", "instrument_code", "strategy"],
}
EXTRA_FIELD_ORDER = [
    "currency_code", "trade_typology", "trade_code", "strategy", "instrument_code",
    "simulation_name", "simulation_source", "measure_type_code", "trader_code",
    "guaranteed_entity", "region_key", "scenario_date_id", "tenor_code",
    "underlying_tenor_code", "curve_code", "product_category_attributes",
]


def _ordered(keys, first):
    out = [k for k in first if k in keys]
    out += [k for k in EXTRA_FIELD_ORDER if k in keys and k not in out]
    return out


def filter_layout(scopes):
    """Form layout for the selected scope codes.

    main: fields always visible (single scope: the 4 defaults, + the 3 VaR
          fields for VaR; several scopes: the 8 defaults).
    more: the collapsed "More filters" — fields the engine applies to EVERY
          selected scope, minus main. Empty selection → main only."""
    scopes = [s for s in (scopes or []) if s in SCOPE_FILTER_FIELDS]
    if not scopes:
        return {"main": list(MAIN_FIELDS_SINGLE), "more": []}
    if len(scopes) == 1:
        s = scopes[0]
        main = list(MAIN_FIELDS_SINGLE)
        if s == "VaR":
            main += ["var_component_name", "var_sub_component_name", "day_type"]
        more_keys = SCOPE_FILTER_FIELDS[s] - set(main)
        return {"main": main, "more": _ordered(more_keys, SCOPE_TIER2_ORDER.get(s, []))}
    common = frozenset.intersection(*(SCOPE_FILTER_FIELDS[s] for s in scopes))
    main = list(MAIN_FIELDS_MULTI)
    more_keys = (common - set(main)) - VAR_ONLY_FIELDS
    return {"main": main, "more": _ordered(more_keys, [])}


def allowed_filter_keys(scopes):
    """Every filter key the current selection may carry in the payload."""
    lay = filter_layout(scopes)
    return set(lay["main"]) | set(lay["more"])
```

- [ ] **Step 4: Run tests**

Run: `$VENV -m pytest streamlit_app/tests -q`
Expected: 9 passed. If `test_single_scope_layout_matches_today` fails on order, fix `_ordered` (tier-2 order must lead), not the test.

- [ ] **Step 5: Commit**

```bash
git add streamlit_app/utils/scope_filters.py streamlit_app/tests
git commit -m "feat(scaling): per-scope filter applicability table + layout helper (unit-tested)

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 2: Multi-select scope state and pills; remove FRTBALL

**Files:**
- Modify: `streamlit_app/pages/1_New_Adjustment.py` — `_WIZ_DEFAULTS` (l.38), `FRTB_SUBTYPES`/`FRTB_SUBTYPE_CONFIG` (l.616–622), `SCOPE_FIELDS`/`ALL_EXTRA_FIELDS`/`FILTER_KEYS` (l.625–666), `_render_scope_pills` (l.1036–1077), `SCOPE_BTN_ICONS` (l.754).

**Interfaces:**
- Produces: `wiz["process_types"]: list[str]`; `_selected_scopes() -> list[str]` (Scaling/Entity Roll → `wiz["process_types"]`; other categories → `[wiz["process_type"]]` or `[]`); `_render_scope_pills()` (no parameters) renders the multi pill group and purges disallowed filters; `FILTER_KEYS` now built from `scope_filters.FIELD_LABELS`.
- Consumes: Task 1 module.

- [ ] **Step 1: Add the import and the new wizard key**

At the top of the page (after `from utils.snowflake_conn import (...)`, l.26–29) add:

```python
from utils.scope_filters import (FIELD_LABELS, MAIN_FIELDS_SINGLE, VAR_ONLY_FIELDS,
                                 filter_layout, allowed_filter_keys)
from utils.styles import ALL_SCOPES, _st_version
```
(`ALL_SCOPES` and `_st_version` exist in `utils/styles.py` l.159 / l.1984; if the existing `from utils.styles import (...)` block already imports names, append these two to it instead of a second import.)

In `_WIZ_DEFAULTS` add, right under `"process_type": None,`:
```python
    "process_types":          [],    # Scaling / Entity Roll: one adjustment per scope
```

- [ ] **Step 2: Replace the FRTB sub-type block and the filter constants**

Delete `FRTB_SUBTYPES`, `FRTB_SUBTYPE_CONFIG`, `SCOPE_FIELDS` (and its `for _fst in FRTB_SUBTYPES:` loop) and `ALL_EXTRA_FIELDS`. Replace `FILTER_KEYS` with:

```python
# Every filter key (payload + ticket chips). Order = catalogue order.
FILTER_KEYS = list(FIELD_LABELS.keys())
# Legacy keys still on old headers / wiz defaults but never offered any more.
_RETIRED_FILTER_KEYS = ("batch_region_area", "murex_family", "murex_group")
```

Then grep the page for every remaining use of `ALL_EXTRA_FIELDS`, `SCOPE_FIELDS`, `FRTB_SUBTYPES`, `FRTB_SUBTYPE_CONFIG`:
```bash
grep -n "ALL_EXTRA_FIELDS\|SCOPE_FIELDS\|FRTB_SUBTYPES\|FRTB_SUBTYPE_CONFIG\|FRTBALL" streamlit_app/pages/1_New_Adjustment.py
```
Expected hits and their fixes (each is handled in the task named):
- `_render_scope_pills` → rewritten in Step 3.
- `_render_extra_filters` / `_render_main_filters` → Task 3.
- `_do_submit` FRTBALL branch, `_run_preview`, `_signoff_scopes`, the `wiz.get("process_type") != "FRTBALL"` guard near l.3685 → Task 4.
- `_ticket_html` `label_map.update({k: l for k, l, _ in ALL_EXTRA_FIELDS})` → replace now with `label_map.update({k: v[0].rstrip(" *†") for k, v in FIELD_LABELS.items()})`.
- `_build_payload` filter loop (l.182–190) → replace the literal key list with `for key in FILTER_KEYS + list(_RETIRED_FILTER_KEYS):` (retired keys stay so an old draft can never lose a value silently; they are always None for new drafts).

- [ ] **Step 3: Rewrite `_render_scope_pills` as a multi-select**

Replace the whole function (l.1036–1077) with:

```python
def _selected_scopes() -> list:
    """Scope codes this draft submits to — one adjustment per code."""
    if wiz.get("category") in ("Scaling Adjustment", "Entity Roll"):
        return list(wiz.get("process_types") or [])
    return [wiz["process_type"]] if wiz.get("process_type") else []


def _purge_filters_for(scopes: list) -> None:
    """Drop filter values the new selection no longer offers (a VaR Component
    picked under VaR must not ride into a Stress submission invisibly)."""
    allowed = allowed_filter_keys(scopes)
    for _fk in FILTER_KEYS:
        if _fk in MAIN_FIELDS_SINGLE:
            continue
        if _fk not in allowed:
            wiz[_fk] = None
    if "VaR" not in scopes or len(scopes) > 1:
        for _wk in ("var_comp_dd", "var_sub_dd"):
            st.session_state.pop(_k(_wk), None)


def _render_scope_pills() -> None:
    """Multi-select scope pills. Sets wiz['process_types'] (list of codes);
    one adjustment is created per selected scope."""
    current = [s for s in (wiz.get("process_types") or []) if s in ALL_SCOPES]
    key = _k(f"scopes_{wiz.get('category')}")
    if _st_version() >= (1, 40):
        picked = st.pills("Data scopes", ALL_SCOPES, selection_mode="multi",
                          default=current, format_func=scope_label, key=key,
                          label_visibility="collapsed")
    else:
        picked = st.multiselect("Data scopes", ALL_SCOPES, default=current,
                                format_func=scope_label, key=key,
                                label_visibility="collapsed")
    picked = [s for s in ALL_SCOPES if s in (picked or [])]   # stable order
    if picked != current:
        wiz["process_types"] = picked
        wiz["process_type"]  = picked[0] if picked else None   # legacy readers
        wiz["_preview_sum"] = None
        wiz["_preview_sql"] = None
        _purge_filters_for(picked)
        safe_rerun()
    n = len(picked)
    st.caption("Pick one or more scopes — one adjustment is created per scope."
               + (f" **{n} adjustments** will be created." if n > 1 else ""))
```

`_st_version()` returns a tuple like `(1, 50, 0)` (see `utils/styles.py` l.1984); if it returns something else, adapt the comparison to that function's contract rather than reimplementing it.

Delete the `"FRTB"` icon-only `SCOPE_BTN_ICONS` dict if nothing else uses it (`grep -n SCOPE_BTN_ICONS`); otherwise leave it.

- [ ] **Step 4: Syntax check**

Run: `python3 -m py_compile streamlit_app/pages/1_New_Adjustment.py`
Expected: no output. (NameErrors for the removed constants surface in Tasks 3–4; do not stop here to fix them beyond what Step 2 listed.)

- [ ] **Step 5: Commit**

```bash
git add streamlit_app/pages/1_New_Adjustment.py
git commit -m "feat(scaling): multi-select scope pills, wiz['process_types'], FRTBALL pseudo-scope removed

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 3: Filter form driven by `filter_layout`; Scaling sections reordered (type before scope)

**Files:**
- Modify: `streamlit_app/pages/1_New_Adjustment.py` — `_render_main_filters` (l.1402–1457), `_render_extra_filters` (l.1459–1509), `render_scaling_form` (l.1512–1573), `_completion_checks` Scaling branch (l.989–1021).

**Interfaces:**
- Consumes: `filter_layout`, `FIELD_LABELS`, `_selected_scopes()`.
- Produces: `_render_filter_widget(fk)` (one widget for any filter key), `_render_main_filters()` and `_render_extra_filters()` with no parameters (they read `_selected_scopes()`).

- [ ] **Step 1: One widget factory for every filter key**

Insert above `_render_main_filters`:

```python
def _render_filter_widget(fk: str) -> None:
    """Render the widget for one filter key into wiz[fk]. Reference-backed
    keys get dropdowns (same sources as before); the rest are text inputs."""
    fl, ph = FIELD_LABELS[fk]
    scopes = _selected_scopes()
    if fk == "entity_code":
        wiz[fk] = _code_select(fl, _k("entity_dd"), wiz.get(fk),
                               _entity_options(), placeholder="— select entity —")
    elif fk == "department_code":
        wiz[fk] = _code_select(fl, _k("dept_dd"), wiz.get(fk),
                               _dept_options(wiz.get("entity_code")), placeholder="— any —",
                               help="Filtered by the selected Entity")
    elif fk == "book_code":
        wiz[fk] = _code_select(fl, _k("book_dd"), wiz.get(fk),
                               _book_options(wiz.get("department_code"), wiz.get("entity_code")),
                               placeholder="— any —",
                               help="Filtered by the selected Entity and Department")
    elif fk == "var_component_name":
        wiz[fk] = _code_select(fl, _k("var_comp_dd"), wiz.get(fk),
                               _var_comp_options(), placeholder="— any —")
    elif fk == "var_sub_component_name":
        wiz[fk] = _code_select(fl, _k("var_sub_dd"), wiz.get(fk),
                               _var_sub_options(wiz.get("var_component_name")),
                               placeholder="— any —",
                               help="Filtered by the selected VaR Component")
    elif fk == "day_type":
        _render_day_type("main")
    elif fk == "simulation_source":
        wiz[fk] = _code_select(fl, _k(fk), wiz.get(fk),
                               _sim_source_options(), placeholder="— any —")
    elif fk == "simulation_name":
        wiz[fk] = _code_select(fl, _k(fk), wiz.get(fk),
                               _sim_name_options(wiz.get("simulation_source")),
                               placeholder="— any —",
                               help="Narrowed by Simulation Source when one is selected")
    elif fk == "measure_type_code":
        wiz[fk] = _code_select(fl, _k(fk), wiz.get(fk),
                               _measure_type_options(scopes[0] if len(scopes) == 1 else None),
                               placeholder="— any —")
    else:
        wiz[fk] = st.text_input(fl, key=_k(fk), value=wiz.get(fk) or "", placeholder=ph)
```

Check `_measure_type_options(process_type)` (l.~1340) tolerates `None` (it prefixes FRTB codes when the scope is FRTB); if it does not, make `None` mean "no scope-specific ordering".

- [ ] **Step 2: Rewrite `_render_main_filters` and `_render_extra_filters`**

```python
def _render_main_filters() -> None:
    scopes = _selected_scopes()
    lay = filter_layout(scopes)
    main = lay["main"]
    for row_start in range(0, len(main), 4):
        cols = st.columns(4)
        for col, fk in zip(cols, main[row_start:row_start + 4]):
            with col:
                _render_filter_widget(fk)
    _is_var = scopes == ["VaR"]
    st.caption("Blank = all values for that dimension · "
               + ("† at least one of Department, Book or VaR Component"
                  if _is_var else "† at least one of Department or Book")
               + (" · fields shown are the ones every selected scope supports"
                  if len(scopes) > 1 else ""))
    _narrowed = (bool((wiz.get("department_code") or "").strip())
                 or bool((wiz.get("book_code") or "").strip())
                 or (_is_var and (bool((wiz.get("var_component_name") or "").strip())
                                  or bool((wiz.get("var_sub_component_name") or "").strip()))))
    if (wiz.get("entity_code") or "").strip() and not _narrowed:
        st.warning("Entity alone is not enough — add a Department or Book code"
                   + (" (or a VaR Component)" if _is_var else "")
                   + ". Entity-wide adjustments are not allowed.")


def _render_extra_filters() -> None:
    """Collapsed 'More filters': only fields the engine applies to EVERY
    selected scope (single scope: that scope's own fields first)."""
    scopes = _selected_scopes()
    more = filter_layout(scopes)["more"]
    if not more:
        return
    applied = sum(1 for k in more if (wiz.get(k) or "").strip())
    label = f"More filters ({applied} applied)" if applied else "More filters"
    with st.expander(label, expanded=False):
        if len(scopes) > 1:
            st.caption("Showing only filters that apply to all of: "
                       + ", ".join(scope_label(s) for s in scopes))
        ecols = st.columns(3)
        for i, fk in enumerate(more):
            with ecols[i % 3]:
                _render_filter_widget(fk)
```

`_render_day_type("more")` is no longer called; keep the function (used with `"main"`).

- [ ] **Step 3: Reorder `render_scaling_form`**

Replace the first two cards of `render_scaling_form` (Data Scope then Adjustment Type) so the order is Type → Scope, and renumber:

```python
def render_scaling_form() -> None:
    # ── Type ─────────────────────────────────────────────────────────────
    with _card():
        _sec(2, "Adjustment Type", "How should the figures change?")
        tsel = _pill_row(list(TYPE_CONFIG.keys()), wiz.get("adjustment_type"),
                         "type", icons=TYPE_BTN_ICONS,
                         descs={k: v["desc"] for k, v in TYPE_CONFIG.items()})
        if tsel and tsel != wiz.get("adjustment_type"):
            wiz["adjustment_type"] = tsel
            wiz["_preview_sum"] = None
            wiz["_preview_sql"] = None
            if tsel != "Roll":
                wiz["source_cobid"] = None
            safe_rerun()
        if wiz.get("adjustment_type"):
            st.caption(f"Formula: {TYPE_CONFIG[wiz['adjustment_type']]['formula']}")
    if not wiz.get("adjustment_type"):
        st.info("Select an adjustment type to continue.")
        return

    # ── Scope(s) ─────────────────────────────────────────────────────────
    with _card():
        _sec(3, "Data Scope", "Select one or more data scopes — one adjustment per scope.")
        _render_scope_pills()
    if not _selected_scopes():
        st.info("Select at least one data scope to continue.")
        return
```
Keep the remaining cards (Date & Schedule = 4, Dimension Filters = 5, Business Context = 6) exactly as they are, except the Dimension Filters helper text: replace `(" (VaR: or VaR Component)" if wiz.get("process_type") == "VaR" else "")` with `(" (VaR: or VaR Component)" if _selected_scopes() == ["VaR"] else "")`.

- [ ] **Step 4: Checklist**

In `_completion_checks` Scaling branch replace `("Data scope", bool(wiz.get("process_type")))` with `("Data scope", bool(_selected_scopes()))`, and every `wiz.get("process_type") == "VaR"` in that branch with `_selected_scopes() == ["VaR"]`. In the Entity Roll branch replace `("Data scope", bool(wiz.get("process_type")))` the same way.

- [ ] **Step 5: Syntax check and grep for leftovers**

```bash
python3 -m py_compile streamlit_app/pages/1_New_Adjustment.py
grep -n "ALL_EXTRA_FIELDS\|SCOPE_FIELDS\|FRTB_SUBTYPE" streamlit_app/pages/1_New_Adjustment.py
```
Expected: compiles; grep prints nothing.

- [ ] **Step 6: Commit**

```bash
git add streamlit_app/pages/1_New_Adjustment.py
git commit -m "feat(scaling): type before scope; filter form driven by per-scope applicability (intersection for several scopes)

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 4: Generalise submit, preview and sign-off fan-out to the scope list; ticket and success copy

**Files:**
- Modify: `streamlit_app/pages/1_New_Adjustment.py` — `_do_submit` (l.561–583), `_run_preview` (l.3027–3078), `_signoff_scopes` (l.3087–3091), preview-detail guard (l.3683–3686), `_ticket_html` Scaling/Entity Roll rows (l.2904–2947), `_preview_payload` (l.868).

**Interfaces:**
- Consumes: `_selected_scopes()`.
- Produces: `_submit_fanout(payload, scopes) -> dict` used again by Part B (Transfer Book fans out per scope × trade).

- [ ] **Step 1: Replace the FRTBALL branch in `_do_submit`**

Replace the block starting at the comment `# "All FRTB": FRTBALL is not a processable scope` through its `return {"status": "Error", ...}` with:

```python
        scopes = _selected_scopes()
        if wiz.get("category") in ("Scaling Adjustment", "Entity Roll") and len(scopes) > 1:
            return _submit_fanout(payload, scopes)
```

and add, above `_do_submit`:

```python
def _submit_fanout(payload: dict, scopes: list) -> dict:
    """One SP_SUBMIT_ADJUSTMENT call per scope (same payload, scope swapped).
    Scopes sharing a pipeline are serialised by the engine. No rollback on a
    partial failure: the message names what was created so the user can
    delete it on the Adjustments page."""
    created, failures, statuses = [], [], []
    for sc in scopes:
        res = _submit_one({**payload, "process_type": sc})
        if _is_submit_success(res):
            created.append(scope_label(sc))
            statuses.append(res.get("status"))
        else:
            failures.append(f"{scope_label(sc)}: {res.get('message', 'not accepted')}")
    if not failures:
        return {"status": statuses[0],
                "message": (f"Created {len(created)} adjustments — one per scope "
                            f"({', '.join(created)}). They are queued and will be "
                            f"processed by their scope pipelines.")}
    partial = (f" Already created: {', '.join(created)} — delete them from the "
               f"Adjustments page if they are no longer wanted." if created else "")
    return {"status": "Error",
            "message": "Not every scope was accepted. " + " | ".join(failures) + partial}
```

- [ ] **Step 2: `_run_preview` over the scope list**

Replace
```python
    subtypes = (["FRTB", "FRTBDRC", "FRTBRRAO"]
                if payload.get("process_type") == "FRTBALL"
                else [payload.get("process_type")])
```
with
```python
    subtypes = _selected_scopes() or [payload.get("process_type")]
```
and the docstring's FRTBALL sentence with: `Several scopes: previews each and sums the numeric totals.` Everything else in the function already loops `subtypes`.

In `_preview_payload` keep `"process_type": wiz["process_type"]` (the loop overrides it per scope).

- [ ] **Step 3: `_signoff_scopes` and the preview-detail guard**

```python
def _signoff_scopes() -> list:
    return _selected_scopes()
```
Replace `and wiz.get("process_type") != "FRTBALL" \` (l.~3685) with `and len(_selected_scopes()) == 1 \`, and the comment above it with `# (Single scope only: the summary for several scopes is an aggregate; per-row breakdown/sample only makes sense per scope.)`.

- [ ] **Step 4: Ticket rows**

In `_ticket_html`, Scaling branch: replace `kv += _ticket_row("Scope", scope_label(wiz.get("process_type")))` with
```python
        _scs = _selected_scopes()
        kv += _ticket_row("Scope" if len(_scs) <= 1 else f"Scopes ({len(_scs)} adjustments)",
                          ", ".join(scope_label(s) for s in _scs) if _scs else None, bool(_scs))
```
Entity Roll branch: same replacement for its `kv += _ticket_row("Scope", ...)` line.

- [ ] **Step 5: Entity Roll form**

In `render_entity_roll_form`: `_render_scope_pills(include_frtball=False)` → `_render_scope_pills()`; `if not wiz.get("process_type"):` → `if not _selected_scopes():`. The wipe-preview recon block reads `wiz["process_type"]` once; wrap it in a loop:

```python
    wiz["_eroll_remove_count"] = 0
    _recon_rows = []
    for _pt_code in _selected_scopes():
        if not (wiz.get("cobid") and (wiz.get("entity_code") or "").strip()):
            break
        ... existing body, with `_pt = _pt_code.replace(...)` instead of wiz["process_type"] ...
        _recon_rows.append((scope_label(_pt_code), h_cnt, d_cnt, f_cnt))
        wiz["_eroll_remove_count"] += max(d_cnt or 0, f_cnt or 0)
```
and render the red banner once after the loop, listing one `<span>` line per `_recon_rows` entry (`f"{lbl}: header {h} · dimension {d} · fact {f}"`) instead of the single `_recon` string. Keep the banner's wording.

- [ ] **Step 6: Success screen wording**

Search `wiz["step"] == 3` block (l.~3421) for text assuming one adjustment ("Adjustment created successfully"); when `result.get("message")` starts with `Created ` show that message verbatim as the headline (it already names the count). No other change.

- [ ] **Step 7: Run the local page smoke test**

Create `streamlit_app/tests/test_new_adjustment_page.py` (AppTest with a fake session — no Snowflake):

```python
import json, os, sys, re
import pandas as pd, pytest
APP = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, APP)
from streamlit.testing.v1 import AppTest
import utils.snowflake_conn as sc

CALLS = []
class Row(list):
    def __getitem__(self, k):
        return list.__getitem__(self, 0 if isinstance(k, str) else k)
class SQL:
    def __init__(self, q): self.q = q
    def collect(self):
        CALLS.append(self.q)
        if "SP_SUBMIT_ADJUSTMENT" in self.q:
            return [Row([json.dumps({"status": "Pending", "adj_id": "x"})])]
        if "CURRENT_USER" in self.q.upper(): return [Row(["TESTER"])]
        return []
    def to_pandas(self): CALLS.append(self.q); return pd.DataFrame()
class Sess:
    def sql(self, q, *a, **k): return SQL(q)

@pytest.fixture(autouse=True)
def fake(monkeypatch):
    monkeypatch.setattr(sc, "get_session", lambda: Sess())
    monkeypatch.chdir(APP)
    CALLS.clear()

def _load():
    at = AppTest.from_file("pages/1_New_Adjustment.py", default_timeout=120).run()
    assert not at.exception, at.exception
    return at

def test_page_loads_without_frtball():
    at = _load()
    labels = [b.label for b in at.button]
    assert not any("FRTBALL" in l or "All FRTB" in l for l in labels)

def test_scaling_shows_type_before_scope():
    at = _load()
    at.session_state["wiz"] = {**at.session_state["wiz"], "category": "Scaling Adjustment"}
    at.run(); assert not at.exception, at.exception
    texts = " ".join(m.value for m in at.markdown)
    assert texts.index("Adjustment Type") < texts.index("Data Scope")

def test_multi_scope_submit_calls_once_per_scope():
    at = _load()
    wiz = {**at.session_state["wiz"], "category": "Scaling Adjustment",
           "adjustment_type": "Flatten", "process_types": ["VaR", "Stress"],
           "process_type": "VaR", "cobid": 20991231, "entity_code": "MUSI",
           "book_code": "B1", "adjustment_category": "Booking Error", "reason": "t",
           "_preview_sum": {"ROWS_AFFECTED": 5}, "_preview_for": None}
    at.session_state["wiz"] = wiz
    at.run(); assert not at.exception, at.exception
    # Call the submit path directly: the button is gated by preview freshness.
    import importlib.util
    page = at._script_path if hasattr(at, "_script_path") else "pages/1_New_Adjustment.py"
    # Simplest robust check: exercise _submit_fanout via the module namespace.
    ns = {}
    src = open(page).read()
    exec(compile(src.split("# ══════════════════════════════════════════════════════════════════════════════\n# LEFT COLUMN")[0], page, "exec"), ns)  # constants + helpers only
    ns["wiz"] = wiz
    out = ns["_submit_fanout"]({"process_type": "VaR", "adjustment_type": "Flatten"}, ["VaR", "Stress"])
    assert out["status"] == "Pending" and "2 adjustments" in out["message"]
    assert sum("SP_SUBMIT_ADJUSTMENT" in c for c in CALLS) == 2
```

If the `exec`-based helper test proves brittle (module-level code before the LEFT COLUMN marker calls Streamlit at import), replace `test_multi_scope_submit_calls_once_per_scope` with a direct test of `_submit_fanout` by moving that function into `streamlit_app/utils/submit_fanout.py` (pure: takes `submit_one` and `is_success` callables as parameters) and importing it from the page — that refactor is acceptable within this task.

Run: `$VENV -m pytest streamlit_app/tests -q`
Expected: all pass.

- [ ] **Step 8: Commit**

```bash
git add streamlit_app/pages/1_New_Adjustment.py streamlit_app/tests
git commit -m "feat(scaling): one adjustment per selected scope — submit/preview/sign-off fan-out generalised; Entity Roll multi-scope

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 5: Documentation text and memory of the change

**Files:**
- Modify: `streamlit_app/pages/7_Documentation.py` — `_KNOWLEDGE` (search `"All FRTB" fan-out` / `FRTBALL retired` in the module docstring l.7 and the knowledge text).

- [ ] **Step 1: Update the knowledge text**

In `_KNOWLEDGE`, the "Adjustment categories" paragraph, add after the Scaling bullet:
```
  Scaling and Entity Roll accept SEVERAL data scopes at once — the app
  creates one adjustment per selected scope. With several scopes the filter
  form offers only the fields every selected scope supports.
```
Remove any sentence saying FRTBALL / "All FRTB" is a selectable option (module docstring line `• "All FRTB" fan-out (one adjustment per real sub-type; FRTBALL retired)` becomes `• multi-scope submission (one adjustment per selected scope; FRTBALL retired)`).

- [ ] **Step 2: Compile and commit**

```bash
python3 -m py_compile streamlit_app/pages/7_Documentation.py
git add streamlit_app/pages/7_Documentation.py
git commit -m "docs: multi-scope submission replaces the All-FRTB option

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

## Self-review (done while writing)

- Spec §1.1 type-before-scope → Task 3 Step 3. §1.2 multi-select + one header per scope + FRTBALL removed → Tasks 2, 4. §1.3 eight main fields + intersection → Tasks 1, 3. §3.4 preview per scope summed, sign-off per scope, ticket count → Task 4. §3.5 Entity Roll → Task 4 Step 5.
- Names: `filter_layout`, `allowed_filter_keys`, `FIELD_LABELS`, `MAIN_FIELDS_SINGLE`, `VAR_ONLY_FIELDS` (Task 1) are the names imported in Task 2 Step 1 and used in Tasks 3–4. `_selected_scopes()` defined in Task 2, used in 3–4. `_submit_fanout` defined in Task 4, reused by Part B.
- Deviation to note for Marcos: `_render_day_type` now only renders in the main row for VaR (was also in More filters) — same field, one place.
