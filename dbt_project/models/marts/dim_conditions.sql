{{ config(materialized='table') }}

with conditions_dedup as (
    select
        patient_id,
        condition_code,
        code_system,
        condition_description,
        row_number() over (partition by patient_id, condition_code order by condition_start_date desc) as rn
    from {{ ref('stg_conditions') }}
    where patient_id is not null and condition_code is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['patient_id', 'condition_code']) }} as condition_sk,
    patient_id,
    condition_code,
    code_system,
    condition_description
from conditions_dedup
where rn = 1