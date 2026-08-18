USE SCHEMA ADJUSTMENT;
create or replace view VW_ADJUSTMENTS_DIRECT_RRAO as
with base as (
    select
        adr.*
    from ADJUSTMENT.ADJUSTMENTS_DIRECT_RRAO adr
),
enriched as (
    select
        base.*,
        MD5(
            COALESCE(CAST(base.DIRECT_RRAO_FILENAME AS VARCHAR), '_dbt_utils_surrogate_key_null_')
            || '-'
            || COALESCE(CAST(base.DIRECT_RRAO_FILE_ROW_NUMBER AS VARCHAR), '_dbt_utils_surrogate_key_null_')
            || '-'
            || COALESCE(CAST(base.DIRECT_RRAO_STAGE_TIMESTAMP AS VARCHAR), '_dbt_utils_surrogate_key_null_')
            || '-'
            || COALESCE(CAST(base.DIRECT_RRAO_DATASET_NAME AS VARCHAR), '_dbt_utils_surrogate_key_null_')
        ) as frtbsa_rrao_key,
        'DIRECT-RRAO' as load_set,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(9) as load_timestamp,
        COALESCE(NULLIF(base.CCY1, ''), base.CCY2) as currency_code,
        IFF(LEN(CONCAT(base.CCY1, '-', base.CCY2)) < 7, null, CONCAT(base.CCY1, '-', base.CCY2)) as currency_pair_code
    from base
)
select
    enriched.ADJUSTMENT_ID,
    enriched.BOOK_CODE,
    b.BOOK_KEY,
    enriched.BT_TYPE,
    enriched.BUSINESS_PRODUCT_CODE1,
    enriched.BUSINESS_PRODUCT_CODE2,
    enriched.BUSINESS_PRODUCT_CODE3,
    enriched.BUSINESS_PRODUCT_CODE4,
    enriched.PRODUCT_CATEGORY_ATTRIBUTES,
    COALESCE(pca.PRODUCT_CATEGORY_ATTRIBUTES_KEY, -1) as PRODUCT_CATEGORY_ATTRIBUTES_KEY,
    enriched.CCY_AMT,
    enriched.CCY_AMT_OFFSHORE_QUALIFIER,
    enriched.CCY1,
    enriched.CCY1_OFFSHORE_QUALIFIER,
    enriched.CCY2,
    enriched.CCY2_OFFSHORE_QUALIFIER,
    enriched.currency_code as CURRENCY_CODE,
    enriched.currency_pair_code as CURRENCY_PAIR_CODE,
    COALESCE(cp.CURRENCY_PAIR_KEY, -1) as CURRENCY_PAIR_KEY,
    enriched.COBID,
    COALESCE(td.TRADE_KEY, -1) as TRADE_KEY,
    COALESCE(td.TRADE_SOURCE_SYSTEM_CODE, 'MS') as TRADE_SOURCE_SYSTEM_CODE,
    1 as STRUCTURE_NUMBER,
    enriched.ENTITY_CODE,
    TO_DATE(enriched.COBID::varchar, 'YYYYMMDD') as EVALUATION_DATE,
    enriched.INSTRUMENT_CODE,
    enriched.INSTRUMENT_NAME,
    COALESCE(ci.COMMON_INSTRUMENT_KEY, -1) as COMMON_INSTRUMENT_KEY,
    COALESCE(fci.COMMON_INSTRUMENT_FCD_KEY, -1) as COMMON_INSTRUMENT_FCD_KEY,
    TRUE as IS_BELOW_THE_LINE_TYPOLOGY,
    TRUE as IS_FRTB_STATUS_ACTIVE,
    enriched.ISSUER_CODE,
    COALESCE(mt.MEASURE_TYPE_KEY, -1) as MEASURE_TYPE_KEY,
    enriched.MEASURE_TYPE_CODE,
    enriched.MUFG_PRODUCT_CODE,
    enriched.NOTIONAL_AMOUNT as NOTIONAL_AMOUNT_ORIGINAL,
    enriched.NOTIONAL_AMOUNT,
    enriched.NOTIONAL_AMOUNT_USD,
    enriched.NOTIONAL_AMOUNT_USD as NOTIONAL_AMOUNT_USD_ORIGINAL,
    enriched.FRTBSA_RRAO_KEY,
    enriched.TRADE_CODE,
    5 as RAPTOR_LOGIC_TEMPLATE_KEY,
    enriched.DIRECT_RRAO_FILENAME,
    enriched.DIRECT_RRAO_FILE_ROW_NUMBER,
    enriched.DIRECT_RRAO_STAGE_TIMESTAMP,
    enriched.DIRECT_RRAO_DATASET_NAME,
    enriched.REGION_AREA_CODE,
    enriched.SA_RRAO_PRODUCT_TYPE,
    enriched.SIMULATION_ID,
    enriched.SIMULATION_NAME,
    enriched.SOURCE_SYSTEM_CODE,
    enriched.LOAD_SET,
    BATCH.SEQ_RUN_LOG.nextval as RUN_LOG_ID,
    enriched.LOAD_TIMESTAMP,
    enriched.STRATEGY,
    enriched.TRADING_DESK,
    SUM(ABS(enriched.NOTIONAL_AMOUNT_USD) + ABS(IFNULL(enriched.NOTIONAL_AMOUNT_USD, 0))) OVER (PARTITION BY enriched.FRTBSA_RRAO_KEY) = 0::BOOLEAN as IS_ZERO_FILTER,
    TRUE as IS_OFFICIAL_SOURCE,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(9) as ADJUSTMENT_CREATED_TIMESTAMP
from enriched
left join DIMENSION.BOOK b
    on enriched.BOOK_CODE = b.BOOK_CODE
    and TO_DATE(enriched.COBID::varchar, 'YYYYMMDD') between b.EFFECTIVE_START_DATE and b.EFFECTIVE_END_DATE
left join DIMENSION.TRADE td
    on td.TRADE_CODE = COALESCE(NULLIF(enriched.TRADE_CODE, ''), CONCAT(enriched.BOOK_CODE, '/Adjustment'))
    and td.BOOK_CODE = COALESCE(enriched.BOOK_CODE, td.BOOK_CODE)
    and td.ENTITY_CODE = enriched.ENTITY_CODE
    and TO_DATE(enriched.COBID::varchar, 'YYYYMMDD') between td.EFFECTIVE_START_DATE and td.EFFECTIVE_END_DATE
left join DIMENSION.PRODUCT_CATEGORY_ATTRIBUTES pca
    on pca.PCA_CONCAT_KEY = REPLACE(enriched.PRODUCT_CATEGORY_ATTRIBUTES, ' ', '')
left join DIMENSION.MEASURE_TYPE mt
    on enriched.MEASURE_TYPE_CODE = mt.MEASURE_TYPE_CODE
left join DIMENSION.CURRENCY_PAIR cp
    on enriched.currency_pair_code = cp.CURRENCY_PAIR
left join DIMENSION.COMMON_INSTRUMENT ci
    on enriched.INSTRUMENT_CODE = ci.INSTRUMENT_CODE
    and TO_DATE(enriched.COBID::varchar, 'YYYYMMDD') between ci.EFFECTIVE_START_DATE and ci.EFFECTIVE_END_DATE
left join DIMENSION.COMMON_INSTRUMENT_FCD fci
    on enriched.INSTRUMENT_CODE = fci.INSTRUMENT_CODE
    and fci.IS_CURRENT_ROW = TRUE;