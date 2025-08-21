{{ config(materialized='table') }}

with immunizations as (
    select
        i.immunization_code,
        p.patient_sk,
        e.encounter_id,
        cast(i.immunization_date as date) as immunization_date,
        i.immunization_description
    from {{ ref('stg_immunizations') }} i
    left join {{ ref('dim_patients') }} p on i.patient_id = p.patient_id
    left join {{ ref('fct_encounters') }} e on i.encounter_id = e.encounter_id
    left join {{ ref('dim_immunizations') }} di on i.patient_id = di.patient_id and i.immunization_code = di.immunization_code
    where p.patient_sk is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['patient_sk', 'immunization_code']) }} as immunization_event_sk,
    patient_sk,
    encounter_id,
    immunization_date,
    immunization_code,
    immunization_description
from immunizations