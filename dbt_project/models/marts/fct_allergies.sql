{{ config(materialized='table') }}

with allergies as (
    select
        a.allergy_code,
        p.patient_sk,
        e.encounter_id,
        cast(a.allergy_start_date as date) as allergy_start_date,
        a.allergy_description,
        a.allergy_category,
        a.reaction_1_description,
        a.reaction_1_severity
    from {{ ref('stg_allergies') }} a
    left join {{ ref('dim_patients') }} p on a.patient_id = p.patient_id
    left join {{ ref('fct_encounters') }} e on a.encounter_id = e.encounter_id
    left join {{ ref('dim_allergies') }} da on a.patient_id = da.patient_id and a.allergy_code = da.allergy_code
    where p.patient_sk is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['patient_sk', 'allergy_code']) }} as allergy_event_sk,
    patient_sk,
    encounter_id,
    allergy_start_date,
    allergy_code,
    allergy_description,
    allergy_category,
    reaction_1_description,
    reaction_1_severity
from allergies