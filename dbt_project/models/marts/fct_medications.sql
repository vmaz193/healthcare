{{ config(materialized='table') }}

with medications as (
    select
        m.medication_code,
        p.patient_sk,
        py.payer_sk,
        e.encounter_id,
        cast(m.medication_start_date as date) as medication_start_date,
        cast(m.medication_end_date as date) as medication_end_date,
        m.medication_description,
        coalesce(m.base_cost, 0) as base_cost,
        coalesce(m.payer_coverage, 0) as payer_coverage,
        coalesce(m.total_cost, 0) as total_cost,
        coalesce(m.dispenses, 0) as dispenses
    from {{ ref('stg_medications') }} m
    left join {{ ref('dim_patients') }} p on m.patient_id = p.patient_id
    left join {{ ref('dim_payers') }} py on m.payer_id = py.payer_id
    left join {{ ref('fct_encounters') }} e on m.encounter_id = e.encounter_id
    left join {{ ref('dim_medications') }} dm on m.patient_id = dm.patient_id and m.medication_code = dm.medication_code
    where p.patient_sk is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['patient_sk', 'medication_code']) }} as medication_event_sk,
    patient_sk,
    payer_sk,
    encounter_id,
    medication_start_date,
    medication_end_date,
    medication_code,
    medication_description,
    base_cost,
    payer_coverage,
    total_cost,
    dispenses
from medications