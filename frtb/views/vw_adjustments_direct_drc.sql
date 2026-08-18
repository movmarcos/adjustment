USE SCHEMA ADJUSTMENT;
create or replace view VW_ADJUSTMENTS_DIRECT_DRC as
with base as (
    select
        adr.*
    from ADJUSTMENT.ADJUSTMENTS_DIRECT_DRC adr
),
enriched as (
    select
        base.*,
        'DIRECT-DRC' as load_set,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(9) as load_timestamp,
        TO_DATE(base.COBID::varchar, 'YYYYMMDD') as evaluation_date_derived,
        COALESCE(NULLIF(base.CCY1, ''), base.CCY2) as currency_code,
        IFF(LEN(CONCAT(base.CCY1, '-', base.CCY2)) < 7, null, CONCAT(base.CCY1, '-', base.CCY2)) as currency_pair_code,
        MD5(
            COALESCE(CAST(base.DIRECT_DRC_FILENAME AS VARCHAR), '_dbt_utils_surrogate_key_null_')
            || '-'
            || COALESCE(CAST(base.DIRECT_DRC_FILE_ROW_NUMBER AS VARCHAR), '_dbt_utils_surrogate_key_null_')
            || '-'
            || COALESCE(CAST(base.DIRECT_DRC_STAGE_TIMESTAMP AS VARCHAR), '_dbt_utils_surrogate_key_null_')
            || '-'
            || COALESCE(CAST(base.DIRECT_DRC_DATASET_NAME AS VARCHAR), '_dbt_utils_surrogate_key_null_')
        ) as frtbsa_drc_key_derived
    from base
)
select
    enriched.ADJUSTMENT_ID,
    enriched.ATTACHMENT,
    enriched.BOOK_CODE,
    b.BOOK_KEY,
    enriched.BT_TYPE,
    enriched.BUCKET,
    enriched.BUSINESS_PRODUCT_CODE1,
    enriched.BUSINESS_PRODUCT_CODE2,
    enriched.BUSINESS_PRODUCT_CODE3,
    enriched.BUSINESS_PRODUCT_CODE4,
    COALESCE(td.PRODUCT_CATEGORY_ATTRIBUTES_KEY, -1) as PRODUCT_CATEGORY_ATTRIBUTES_KEY,
    enriched.BUY_SELL,
    enriched.CALL_PUT,
    enriched.CCY1,
    enriched.CCY1_OFFSHORE_QUALIFIER,
    enriched.CCY2,
    enriched.CCY2_OFFSHORE_QUALIFIER,
    enriched.currency_code as CURRENCY_CODE,
    enriched.currency_pair_code as CURRENCY_PAIR_CODE,
    COALESCE(cp.CURRENCY_PAIR_KEY, -1) as CURRENCY_PAIR_KEY,
    enriched.CCY_AMT,
    enriched.CCY_AMT_OFFSHORE_QUALIFIER,
    enriched.COBID,
    COALESCE(td.TRADE_KEY, -1) as TRADE_KEY,
    NULL as STRUCTURE_NUMBER,
    COALESCE(fci.COMMON_INSTRUMENT_FCD_KEY, fci2.COMMON_INSTRUMENT_FCD_KEY, -1) as COMMON_INSTRUMENT_FCD_KEY,
    COALESCE(ci.COMMON_INSTRUMENT_KEY, ci2.COMMON_INSTRUMENT_KEY, -1) as COMMON_INSTRUMENT_KEY,
    COALESCE(fi.FRTB_INSTRUMENT_KEY, -1) as FRTB_INSTRUMENT_KEY,
    TRUE as IS_BELOW_THE_LINE_TYPOLOGY,
    TRUE as IS_FRTB_STATUS_ACTIVE,
    enriched.COUNTER_PARTY_CODE_HD,
    enriched.DEFAULT_RISK_WEIGHT,
    enriched.DETACHMENT,
    enriched.EBA_BUCKET,
    enriched.EBA_BUCKET_NAME,
    enriched.ENTITY_CODE,
    enriched.evaluation_date_derived as EVALUATION_DATE,
    enriched.FED_BUCKET,
    enriched.FED_BUCKET_NAME,
    enriched.frtbsa_drc_key_derived as FRTBSA_DRC_KEY,
    enriched.GMRM_LGD,
    enriched.GMRM_PRODUCT,
    enriched.INSTRUMENT_NAME,
    enriched.ISSUE_NAME,
    enriched.ISSUER_CODE,
    enriched.ISSUER_RATING,
    enriched.JTD_LOSS,
    enriched.JTD_LOSS_ORIGINAL,
    enriched.JTD_LOSS_USD,
    enriched.JTD_LOSS_USD_ORIGINAL,
    enriched.JTD_RISK_DIRECTION,
    enriched.LGD,
    enriched.MARKET_PRICE,
    enriched.MATURITY_DATE,
    COALESCE(mt.MEASURE_TYPE_KEY, -1) as MEASURE_TYPE_KEY,
    enriched.MEASURE_TYPE_CODE,
    enriched.MUFG_PRODUCT_CODE,
    enriched.NOTIONAL,
    enriched.NOTIONAL_AMOUNT,
    enriched.TRADE_CODE,
    COALESCE(NULLIF(td.MUREX_INSTRUMENT, ''), 'N/A') as MUREX_INSTRUMENT,
    td.MUREX_VERSION as MUREX_VERSION,
    5 as RAPTOR_LOGIC_TEMPLATE_KEY,
    enriched.PRA_BUCKET,
    enriched.PRA_BUCKET_NAME,
    enriched.REGION,
    CASE
        WHEN UPPER(COALESCE(enriched.REGION, '')) IN ('MUSI', 'MUSEU') THEN 'LONDON'
        ELSE 'NEW YORK'
    END as REGION_DATA_SET_CODE,
    CASE
        WHEN UPPER(COALESCE(enriched.REGION, '')) = 'MUSI' THEN enriched.PRA_BUCKET
        WHEN UPPER(COALESCE(enriched.REGION, '')) = 'MUSEU' THEN enriched.EBA_BUCKET
        WHEN UPPER(COALESCE(enriched.REGION, '')) = 'MUSUSA' THEN enriched.FED_BUCKET
        WHEN UPPER(COALESCE(enriched.REGION, '')) = 'MUSCAN' THEN enriched.BUCKET
        ELSE NULL
    END as REGULATOR_BUCKET,
    CASE
        WHEN UPPER(COALESCE(enriched.REGION, '')) = 'MUSI' THEN enriched.PRA_BUCKET_NAME
        WHEN UPPER(COALESCE(enriched.REGION, '')) = 'MUSEU' THEN enriched.EBA_BUCKET_NAME
        WHEN UPPER(COALESCE(enriched.REGION, '')) = 'MUSUSA' THEN enriched.FED_BUCKET_NAME
        WHEN UPPER(COALESCE(enriched.REGION, '')) = 'MUSCAN' THEN NULL
        ELSE NULL
    END as REGULATOR_BUCKET_NAME,
    enriched.REGION_AREA_CODE,
    enriched.RISK_CLASS,
    enriched.SECTOR_NAME,
    enriched.SECURITY_CODE,
    enriched.SECURITY_CODE_TYPE,
    REPLACE(enriched.SECURITY_CODE, '.', '') as SABRE_SECURITY_CODE,
    enriched.SECURITY_CODE_TYPE as SABRE_SECURITY_CODE_TYPE,
    COALESCE(tim.ISIN, enriched.SECURITY_CODE) as UNDERLYING,
    enriched.SECURITY_INFORMATION1,
    enriched.SECURITY_INFORMATION2,
    enriched.SECURITY_INFORMATION3,
    enriched.SIMULATION_ID,
    enriched.SIMULATION_NAME,
    enriched.STRIKE,
    td.TRADE_TYPOLOGY as TRADE_TYPOLOGY,
    COALESCE(td.TRADE_SOURCE_SYSTEM_CODE, 'MS') as SOURCE_SYSTEM_CODE,
    enriched.TRADING_DESK,
    enriched.LOAD_SET,
    BATCH.SEQ_RUN_LOG.nextval as RUN_LOG_ID,
    enriched.LOAD_TIMESTAMP,
    IFF(SUM(ABS(enriched.JTD_LOSS_USD) + ABS(IFNULL(enriched.JTD_LOSS_USD, 0))) OVER (PARTITION BY enriched.FRTBSA_DRC_KEY) = 0, TRUE, FALSE) as IS_ZERO_FILTER,
    TRUE as IS_OFFICIAL_SOURCE,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(9) as ADJUSTMENT_CREATED_TIMESTAMP
from enriched
left join DIMENSION.BOOK b
    on enriched.BOOK_CODE = b.BOOK_CODE
    and enriched.evaluation_date_derived between b.EFFECTIVE_START_DATE and b.EFFECTIVE_END_DATE
left join DIMENSION.TRADE td
    on td.TRADE_CODE = COALESCE(NULLIF(enriched.TRADE_CODE, ''), CONCAT(enriched.BOOK_CODE, '/Adjustment'))
    and td.BOOK_CODE = COALESCE(enriched.BOOK_CODE, td.BOOK_CODE)
    and td.ENTITY_CODE = enriched.ENTITY_CODE
    and enriched.evaluation_date_derived between td.EFFECTIVE_START_DATE and td.EFFECTIVE_END_DATE
left join DIMENSION.MEASURE_TYPE mt
    on enriched.MEASURE_TYPE_CODE = mt.MEASURE_TYPE_CODE
left join (
    select
        ticker,
        max(isin) as isin
    from STATIC_STAGING.TICKER_ISIN_MAP
    group by ticker
) tim
    on enriched.SECURITY_CODE = tim.ticker
left join DIMENSION.COMMON_INSTRUMENT ci
    on COALESCE(NULLIF(enriched.SECURITY_CODE, ''), NULLIF(enriched.INSTRUMENT_NAME, '')) = ci.INSTRUMENT_CODE
    and enriched.evaluation_date_derived between ci.EFFECTIVE_START_DATE and ci.EFFECTIVE_END_DATE
left join DIMENSION.COMMON_INSTRUMENT ci2
    on td.INSTRUMENT_KEY = ci2.INSTRUMENT_KEY
    and enriched.evaluation_date_derived between ci2.EFFECTIVE_START_DATE and ci2.EFFECTIVE_END_DATE
left join DIMENSION.COMMON_INSTRUMENT_FCD fci
    on COALESCE(NULLIF(enriched.SECURITY_CODE, ''), NULLIF(enriched.INSTRUMENT_NAME, '')) = fci.INSTRUMENT_CODE
    and fci.IS_CURRENT_ROW = TRUE
left join DIMENSION.COMMON_INSTRUMENT_FCD fci2
    on td.INSTRUMENT_KEY = fci2.INSTRUMENT_KEY
    and fci2.IS_CURRENT_ROW = TRUE
left join DIMENSION.CURRENCY_PAIR cp
    on enriched.currency_pair_code = cp.CURRENCY_PAIR
left join DIMENSION.FRTB_INSTRUMENT fi
    on COALESCE(NULLIF(enriched.SECURITY_CODE, ''), NULLIF(enriched.INSTRUMENT_NAME, ''), 'NA') = fi.FRTB_INSTRUMENT_CODE
    and COALESCE(NULLIF(enriched.ISSUER_CODE, ''), 'NA') = fi.FRTB_ISSUER_CODE
    and enriched.evaluation_date_derived between fi.EFFECTIVE_START_DATE and fi.EFFECTIVE_END_DATE;