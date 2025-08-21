{{ config(materialized='table') }}

with careplans as (
    select
        c.careplan_id,
        p.patient_sk,
        e.encounter_id,
        cast(c.careplan_start_date as date) as careplan_start_date,
        cast(c.careplan_end_date as date) as careplan_end_date,
        c.careplan_description,
        c.reason_code,
        c.reason_description
    from {{ ref('stg_careplans') }} c
    left join {{ ref('dim_patients') }} p on c.patient_id = p.patient_id
    left join {{ ref('fct_encounters') }} e on c.encounter_id = e.encounter_id
    where p.patient_sk is not null
)

select
    careplan_id,
    patient_sk,
    encounter_id,
    careplan_start_date,
    careplan_end_date,
    careplan_description,
    reason_code,
    reason_description
from careplans