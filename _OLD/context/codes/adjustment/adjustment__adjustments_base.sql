{{
  config(
    materialized = 'ephemeral',
    tags=["adjustment", "frtbsa_adjustment"]
    )
}}

{% set local_force_refresh = var('force_refresh', none) %}

with adjustment as (
    select distinct
        ad.adjustment_id,
        ad.cobid,
        ad.entity_code,
        et.entity_key,
        IFF(ad.adjustment_type = 'Direct', COALESCE(ad.source_system_code, 'MS'), ad.source_system_code) as source_system_code,
        COALESCE(td.trade_key, IFF(ad.adjustment_type = 'Direct',-1,null)) as trade_key,
        ad.trade_code,
        case 
            when ad.product_category_attributes is null then IFF(ad.adjustment_type = 'Direct',-1,null)
            when ad.adjustment_type <> 'Direct' and ad.product_category_attributes like '%N/A%|%N/A%' then IFF(ad.adjustment_type = 'Direct',-1,null)
            else COALESCE(pca.product_category_attributes_key,td.product_category_attributes_key)
        end as product_category_attributes_key,
        ad.product_category_attributes as product_category_attributes_code,
        case 
            when ad.measure_type_code in ('FRTBDRC','FRTBRRAO') then ad.measure_type_code
            when ad.measure_type_code is null and ad.process_type = 'FRTB' then 'FRTBALL'
            else ad.process_type
        end as process_type,
        ad.adjustment_type,
        COALESCE(mt.measure_type_key, IFF(ad.adjustment_type = 'Direct',-1,null)) as measure_type_key,
        mt.measure_type_code,
        ad.trade_typology,
        COALESCE(bk.book_key, IFF(ad.adjustment_type = 'Direct',-1,null)) as book_key,
        ad.book_code,
        IFF(ad.adjustment_type = 'Direct', COALESCE(ad.currency_code, 'USD'), ad.currency_code) as currency_code,
        ad.tenor_code,
        COALESCE(tc.tenor_currency_key, IFF(ad.adjustment_type = 'Direct',-1,null)) as tenor_currency_key,
        ad.underlying_tenor_code,
        COALESCE(ut.underlying_tenor_currency_key, IFF(ad.adjustment_type = 'Direct',-1,null)) as underlying_tenor_currency_key,
        ad.username,
        ad.reason,
        COALESCE(ra.region_area_key, IFF(ad.adjustment_type = 'Direct',-1,null)) as region_area_key,
        ad.adjustment_value_in_usd,
        ad.adjustment_value_in_usd / COALESCE(fx.exchange_rate, 1) as adjustment_value,
        COALESCE(ad.murex_family, pca.murex_family, 'N/A') as murex_family,
        COALESCE(ad.murex_group, pca.murex_group, 'N/A') as murex_group,
        ad.curve_code,
        COALESCE(cc.curve_currency_key, IFF(ad.adjustment_type = 'Direct',-1,null)) as curve_currency_key,
        ad.instrument_code,
        COALESCE(ci.common_instrument_key, ci2.common_instrument_key, IFF(ad.adjustment_type = 'Direct',-1,null)) as common_instrument_key,        
	    COALESCE(fci.common_instrument_fcd_key, IFF(ad.adjustment_type = 'Direct',-1,null)) as common_instrument_fcd_key,
        IFF(mt.measure_usage_type in ('Reporting-NotProcessedByRMProc', 'Reporting-ProcessedByRMProc'), 1, 0) as direct_reporting_measure_type,
        case /* Find Data to dervice ProductCategoryAttributesKey */
            when ci.organisation_industry_subgroup_name = 'Sovereign' then 'Gov'
            when ci.organisation_industry_subgroup_name = 'N/A' then 'N/A'
            when ci.organisation_industry_subgroup_name is null then 'N/A'
            when ci.organisation_industry_subgroup_name <> 'Sovereign' then 'NonGov'
        end as instrument_gov_or_non_gov,
        case
            when gc.country_name is not null then 'G4'
            when ci.organisation_risk_country_code is not null then 'NonG4'
            else 'N/A'
        end as instrument_g4_or_non_g4,
        COALESCE(ci.market_sector_description, 'N/A') as instrument_market_sector_description,
        'N/A' as instrument_product_category,
        case
            when ad.book_code in (select setting_value from {{ ref ('metadata__raptor_setting') }} where setting_name = 'HedgeIncludedBookList') then 'Y'
            else 'N'
        end as book_is_hedge,
        COALESCE(ad.guaranteed_entity, 'N/A') as guaranteed_entity,
        ad.created_date,
        ad.process_date,
        ad.run_status,
        ad.global_reference,
        COALESCE(ad.source_cobid, ad.cobid) as source_cobid,
        ad.department_code,
        case
            when (ad.adjustment_type = 'Scale' and ad.cobid = ad.source_cobid) or ad.adjustment_type = 'Flatten' then 'Scale current COBID'
            when ad.adjustment_type = 'Scale' and ad.cobid <> ad.source_cobid then 'Scale source COBID'
            else ad.adjustment_type
        end as adjustment_action,
        IFF(ad.adjustment_type = 'Flatten', 0, ad.scale_factor) as scale_factor,
        case
            when (ad.adjustment_type = 'Flatten') then -1
            when (ad.adjustment_type = 'Scale' and (ad.cobid = ad.source_cobid or ad.source_cobid is null)) then ad.scale_factor -1
            when (ad.adjustment_type = 'Scale' and ad.cobid <> ad.source_cobid) then ad.scale_factor
            else 1
        end as scale_factor_adjusted,
        IFF(ad.adjustment_type = 'Direct', true, false) as is_official_source,
        ad.strategy,
        ad.adjustment_occurrence,
        ad.is_deleted,
        CURRENT_TIMESTAMP() as load_timestamp
    from {{ ref("adjustment__adjustments_extra") }} as ad
    inner join {{ ref("dimension__entity") }} as et
        on ad.entity_code = et.entity_code
    left join {{ ref("dimension__measure_type") }} as mt
        on ad.measure_type_code = mt.measure_type_code
    left join {{ ref("dimension__book") }} as bk
        on
            ad.book_code = bk.book_code
            and ad.entity_code = bk.entity_code
            and TO_DATE(ad.cobid::varchar, 'YYYYMMDD') between bk.effective_start_date and bk.effective_end_date
            and (bk.guaranteed_entity = COALESCE(ad.guaranteed_entity, 'N/A'))
            and (bk.department_code = COALESCE(ad.department_code, bk.department_code))
    left join {{ ref("dimension__region_area") }} as ra
        on bk.region_area = ra.region_area_name
    left join {{ ref("dimension__curve_currency") }} as cc
        on ad.curve_code = cc.curve_code
    left join {{ ref("dimension__tenor_currency") }} as tc
        on concat(ad.tenor_code,'_',coalesce(ad.currency_code,'USD')) = tc.tenor_currency_code
    left join {{ ref("dimension__underlying_tenor_currency") }} as ut
        on ad.underlying_tenor_code = ut.underyling_tenor_code
    left join {{ ref("dimension__trade") }} as td
        on
            td.trade_code = IFF(ad.adjustment_type = 'Direct', COALESCE(NULLIF(ad.trade_code, ''), CONCAT(ad.book_code, '/Adjustment')), ad.trade_code)
            and td.book_code = COALESCE(ad.book_code, td.book_code)
            and ad.entity_code = td.entity_code
            and TO_DATE(ad.cobid::varchar, 'YYYYMMDD') between td.effective_start_date and td.effective_end_date
    left join {{ ref("dimension__common_instrument") }} as ci
        on
            ad.instrument_code = ci.instrument_code
            and TO_DATE(ad.cobid::varchar, 'YYYYMMDD') between ci.effective_start_date and ci.effective_end_date
    left join {{ ref("dimension__common_instrument") }} as ci2
        on
            td.instrument_key = ci2.instrument_key
            and TO_DATE(ad.cobid::varchar, 'YYYYMMDD') between ci2.effective_start_date and ci2.effective_end_date
    left join {{ ref("dimension__common_instrument_fcd") }} as fci
        on
            ci.instrument_key = fci.instrument_key
            and TO_DATE(ad.cobid::varchar, 'YYYYMMDD') between fci.effective_start_date and fci.effective_end_date
    left join {{ ref("static_staging__government_country_grouping") }} as gc
        on ci.organisation_risk_country_code = gc.country_code
    left join {{ ref("dimension__product_category_attributes") }} as pca
        on
            replace(ad.product_category_attributes,' ','') = replace(pca.pca_concat_key,' ','')
    left join {{ ref("fact__exchange_rate") }} as fx
        on
            ad.cobid = fx.cobid
            and ad.currency_code = fx.from_currency_code
            and ra.region_area_key = fx.region_area_key
    left join {{ ref("dimension__stress_simulation") }}
        on
            td.trade_code = IFF(ad.adjustment_type = 'Direct', COALESCE(NULLIF(ad.trade_code, ''), CONCAT(ad.book_code, '/Adjustment')), ad.trade_code)
            and td.book_code = COALESCE(ad.book_code, td.book_code)
    where
        ad.process_type in ('Sensitivity', 'FRTB')
        --and ad.run_status = 'Pending' --comment out to refresh the issues found
)
select
    ad.*,
    case
        when ad.adjustment_type = 'Direct' then
            case
                when ad.measure_type_key is null then 'Measure Type is missing or invalid'
                when ad.entity_code is null then 'Entity Code is missing or invalid'
                when ad.book_key is null then 'Book is missing or invalid'
                when ad.adjustment_value_in_usd is null then 'Adjustment Value In USD is missing'
            end
    end as adjustment_error_message,
    IFF(adjustment_error_message is null, true, false) as is_positive_adjustment
from adjustment as ad
where
    ad.cobid = '{{ var("cobid") }}'
    {% if is_incremental() and local_force_refresh is none %}
        -- this filter will only be applied on an incremental run
        and ad.process_date is null
    {% endif %}
