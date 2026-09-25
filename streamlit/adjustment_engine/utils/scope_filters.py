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
