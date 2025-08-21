{{ config(materialized='table') }}

with observations as (
    select
        o.observation_id,
        p.patient_sk,
        e.encounter_id,
        cast(o.observation_date as date) as observation_date,
        o.observation_code,
        o.observation_description,
        o.observation_value,
        o.observation_units
    from {{ ref('stg_observations') }} o
    left join {{ ref('dim_patients') }} p on o.patient_id = p.patient_id
    left join {{ ref('fct_encounters') }} e on o.encounter_id = e.encounter_id
    where p.patient_sk is not null
)

select
    observation_id,
    patient_sk,
    encounter_id,
    observation_date,
    observation_code,
    observation_description,
    observation_value,
    observation_units
from observations