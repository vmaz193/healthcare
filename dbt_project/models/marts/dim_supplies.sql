{{ config(materialized='table') }}

with supplies_dedup as (
    select
        patient_id,
        supply_code,
        supply_description,
        row_number() over (partition by patient_id, supply_code order by supply_date desc) as rn
    from {{ ref('stg_supplies') }}
    where patient_id is not null and supply_code is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['patient_id', 'supply_code']) }} as supply_sk,
    patient_id,
    supply_code,
    supply_description
from supplies_dedup
where rn = 1