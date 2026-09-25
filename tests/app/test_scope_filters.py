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
