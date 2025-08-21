{{ config(materialized='table') }}

with medications_dedup as (
    select
        patient_id,
        medication_code,
        medication_description,
        reason_code,
        reason_description,
        row_number() over (partition by patient_id, medication_code order by medication_start_date desc) as rn
    from {{ ref('stg_medications') }}
    where patient_id is not null and medication_code is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['patient_id', 'medication_code']) }} as medication_sk,
    patient_id,
    medication_code,
    medication_description,
    reason_code,
    reason_description
from medications_dedup
where rn = 1