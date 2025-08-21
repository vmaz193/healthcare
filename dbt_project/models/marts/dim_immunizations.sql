{{ config(materialized='table') }}

with immunizations_dedup as (
    select
        patient_id,
        immunization_code,
        immunization_description,
        row_number() over (partition by patient_id, immunization_code order by immunization_date desc) as rn
    from {{ ref('stg_immunizations') }}
    where patient_id is not null and immunization_code is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['patient_id', 'immunization_code']) }} as immunization_sk,
    patient_id,
    immunization_code,
    immunization_description
from immunizations_dedup
where rn = 1