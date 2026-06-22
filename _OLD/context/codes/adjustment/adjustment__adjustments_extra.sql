{{
  config(
    materialized = 'ephemeral',
    tags=["adjustment", "frtbsa_adjustment"]
    )
}}


with dimension__adjustment as (
    select
        adjustment_id,
        cobid,
        process_type,
        adjustment_type,
        source_cobid,
        entity_code,
        source_system_code,
        department_code,
        book_code,
        tenor_code,
        currency_code,
        curve_code,
        instrument_code,
        measure_type_code,
        adjustment_value_in_usd,
        created_date,
        process_date,
        username,
        run_status,
        reason,
        murex_family,
        murex_group,
        trade_typology,
        trade_code,
        scale_factor,
        batch_region_area,
        record_count,
        simulation_name,
        trader_code,
        var_component_id,
        var_sub_component_id,
        scenario_date_id,
        errormessage,
        approval_id,
        guaranteed_entity,
        strategy,
        region_key,
        underlying_tenor_code,
        product_category_attributes,
        is_deleted,
        deleted_by,
        deleted_date,
        global_reference,
        mssql_adjustment_id,
        file_name,
        simulation_source,
        day_type,
        adjustment_occurrence
    from {{ ref("dimension__adjustment") }}
),

cte_roll as (
    select *
    from dimension__adjustment
    where
        cobid <> source_cobid
        and adjustment_type <> 'Direct'
),

cte_scale as (
    select *
    from dimension__adjustment
    where
        cobid = source_cobid
        and adjustment_type <> 'Direct'
),

cte_extra as (
    select
        HASH(s.adjustment_id, r.adjustment_id) as adjustment_id,
        s.cobid,
        s.process_type,
        s.adjustment_type,
        r.source_cobid,
        s.entity_code,
        s.source_system_code,
        s.department_code,
        s.book_code,
        s.tenor_code,
        s.currency_code,
        s.curve_code,
        s.instrument_code,
        s.measure_type_code,
        s.adjustment_value_in_usd,
        dateadd('minute', 1, r.created_date) as created_date,
        s.process_date,
        'Snosflake' as username,
        s.run_status,
        s.reason,
        s.murex_family,
        s.murex_group,
        s.trade_typology,
        s.trade_code,
        0 as scale_factor,
        s.batch_region_area,
        s.record_count,
        s.simulation_name,
        s.trader_code,
        s.var_component_id,
        s.var_sub_component_id,
        s.scenario_date_id,
        s.errormessage,
        s.approval_id,
        s.guaranteed_entity,
        s.strategy,
        s.region_key,
        s.underlying_tenor_code,
        s.product_category_attributes,
        s.is_deleted,
        s.deleted_by,
        s.deleted_date,
        (r.global_reference::varchar || '-' || s.adjustment_id::varchar)::VARCHAR(50) COLLATE 'en-ci' as global_reference,
        s.mssql_adjustment_id,
        s.file_name,
        s.simulation_source,
        s.day_type,
        s.adjustment_occurrence
    from cte_roll as r
    inner join cte_scale as s on
        r.cobid = s.cobid
        and r.process_type = s.process_type
        and r.created_date < s.created_date
        and COALESCE(s.entity_code, 'ENTITY_CODE') = COALESCE(r.entity_code, s.entity_code, 'ENTITY_CODE')
        and COALESCE(s.source_system_code, 'SOURCE_SYSTEM_CODE') = COALESCE(r.source_system_code, s.source_system_code, 'SOURCE_SYSTEM_CODE')
        and COALESCE(s.department_code, 'DEPARTMENT_CODE') = COALESCE(r.department_code, s.department_code, 'DEPARTMENT_CODE')
        and COALESCE(s.book_code, 'BOOK_CODE') = COALESCE(r.book_code, s.book_code, 'BOOK_CODE')
        and COALESCE(s.tenor_code, 'TENOR_CODE') = COALESCE(r.tenor_code, s.tenor_code, 'TENOR_CODE')
        and COALESCE(s.currency_code, 'CURRENCY_CODE') = COALESCE(r.currency_code, s.currency_code, 'CURRENCY_CODE')
        and COALESCE(s.curve_code, 'CURVE_CODE') = COALESCE(r.curve_code, s.curve_code, 'CURVE_CODE')
        and COALESCE(s.instrument_code, 'INSTRUMENT_CODE') = COALESCE(r.instrument_code, s.instrument_code, 'INSTRUMENT_CODE')
        and COALESCE(s.measure_type_code, 'MEASURE_TYPE_CODE') = COALESCE(r.measure_type_code, s.measure_type_code, 'MEASURE_TYPE_CODE')
        and COALESCE(s.murex_family, 'MUREX_FAMILY') = COALESCE(r.murex_family, s.murex_family, 'MUREX_FAMILY')
        and COALESCE(s.murex_group, 'MUREX_GROUP') = COALESCE(r.murex_group, s.murex_group, 'MUREX_GROUP')
        and COALESCE(s.trade_typology, 'TRADE_TYPOLOGY') = COALESCE(r.trade_typology, s.trade_typology, 'TRADE_TYPOLOGY')
        and COALESCE(s.trade_code, 'TRADE_CODE') = COALESCE(r.trade_code, s.trade_code, 'TRADE_CODE')
        and COALESCE(s.batch_region_area, 'BATCH_REGION_AREA') = COALESCE(r.batch_region_area, s.batch_region_area, 'BATCH_REGION_AREA')
        and COALESCE(s.simulation_name, 'SIMULATION_NAME') = COALESCE(r.simulation_name, s.simulation_name, 'SIMULATION_NAME')
        and COALESCE(s.trader_code, 'TRADER_CODE') = COALESCE(r.trader_code, s.trader_code, 'TRADER_CODE')
        and COALESCE(s.guaranteed_entity, 'GUARANTEED_ENTITY') = COALESCE(r.guaranteed_entity, s.guaranteed_entity, 'GUARANTEED_ENTITY')
        and COALESCE(s.strategy, 'STRATEGY') = COALESCE(r.strategy, s.strategy, 'STRATEGY')
        and COALESCE(s.underlying_tenor_code, 'UNDERLYING_TENOR_CODE') = COALESCE(r.underlying_tenor_code, s.underlying_tenor_code, 'UNDERLYING_TENOR_CODE')
        and COALESCE(s.product_category_attributes, 'PRODUCT_CATEGORY_ATTRIBUTES') = COALESCE(r.product_category_attributes, s.product_category_attributes, 'PRODUCT_CATEGORY_ATTRIBUTES')
        and COALESCE(s.simulation_source, 'SIMULATION_SOURCE') = COALESCE(r.simulation_source, s.simulation_source, 'SIMULATION_SOURCE')
        and COALESCE(s.var_component_id, -1) = COALESCE(r.var_component_id, s.var_component_id, -1)
        and COALESCE(s.var_sub_component_id, -1) = COALESCE(r.var_sub_component_id, s.var_sub_component_id, -1)
)
,cte_union as (
select * from cte_extra
union all
select * from dimension__adjustment
)
select * from cte_union