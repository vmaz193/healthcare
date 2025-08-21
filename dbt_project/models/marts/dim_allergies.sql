{{ config(materialized='table') }}

with allergies_dedup as (
    select
        patient_id,
        allergy_code,
        code_system,
        allergy_description,
        allergy_type,
        allergy_category,
        reaction_1_code,
        reaction_1_description,
        reaction_1_severity,
        reaction_2_code,
        reaction_2_description,
        reaction_2_severity,
        row_number() over (partition by patient_id, allergy_code order by allergy_start_date desc) as rn
    from {{ ref('stg_allergies') }}
    where patient_id is not null and allergy_code is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['patient_id', 'allergy_code']) }} as allergy_sk,
    patient_id,
    allergy_code,
    code_system,
    allergy_description,
    allergy_type,
    allergy_category,
    reaction_1_code,
    reaction_1_description,
    reaction_1_severity,
    reaction_2_code,
    reaction_2_description,
    reaction_2_severity
from allergies_dedup
where rn = 1