{{
  config(
    materialized = 'incremental',
    unique_key = ['global_reference', 'cobid'],
    tags=["adjustment", "sensitivity_adjustment"]
    )
}}

select * from {{ ref('adjustment__adjustments_base') }}
where process_type = 'Sensitivity'