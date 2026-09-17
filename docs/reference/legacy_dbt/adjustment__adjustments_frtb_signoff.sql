{{
  config(
    materialized = 'view',
    tags=["adjustment", "frtbsa_adjustment"]
    )
}}
with batch__publish_signoff as (
    select *
    from {{ ref('batch__publish_signoff_status_exception') }}
    where
        process_type = 'FRTB'
        and sub_type = 'NonCVA'
        and publish_status = 'SignedOff'
)

select adj.*
from {{ ref('adjustment__adjustments_base_frtb') }} as adj
left join batch__publish_signoff as batch
    on
        adj.cobid = batch.cobid
        and adj.entity_code = batch.entity_code
where
    adj.created_date <= COALESCE(batch.signoff_update_time, adj.load_timestamp)
    and adj.run_status <> 'Rejected - SignedOff'
