{{ config(materialized='table') }}

with procedures_dedup as (
    select
        patient_id,
        procedure_code,
        code_system,
        procedure_description,
        reason_code,
        reason_description,
        row_number() over (partition by patient_id, procedure_code order by procedure_start_timestamp desc) as rn
    from {{ ref('stg_procedures') }}
    where patient_id is not null and procedure_code is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['patient_id', 'procedure_code']) }} as procedure_sk,
    patient_id,
    procedure_code,
    code_system,
    procedure_description,
    reason_code,
    reason_description
from procedures_dedup
where rn = 1