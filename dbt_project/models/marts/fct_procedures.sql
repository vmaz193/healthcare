{{ config(materialized='table') }}

with procedures as (
    select
        pr.procedure_code,
        p.patient_sk,
        e.encounter_id,
        cast(pr.procedure_start_timestamp as date) as procedure_date,
        pr.code_system,
        pr.procedure_description,
        pr.reason_code,
        pr.reason_description
    from {{ ref('stg_procedures') }} pr
    left join {{ ref('dim_patients') }} p on pr.patient_id = p.patient_id
    left join {{ ref('fct_encounters') }} e on pr.encounter_id = e.encounter_id
    left join {{ ref('dim_procedures') }} dp on pr.patient_id = dp.patient_id and pr.procedure_code = dp.procedure_code
    where p.patient_sk is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['patient_sk', 'procedure_code']) }} as procedure_event_sk,
    patient_sk,
    encounter_id,
    procedure_date,
    procedure_code,
    code_system,
    procedure_description,
    reason_code,
    reason_description
from procedures