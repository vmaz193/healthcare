{{ config(materialized='table') }}

with supplies as (
    select
        s.supply_code,
        p.patient_sk,
        e.encounter_id,
        cast(s.supply_date as date) as supply_date,
        s.supply_description
    from {{ ref('stg_supplies') }} s
    left join {{ ref('dim_patients') }} p on s.patient_id = p.patient_id
    left join {{ ref('fct_encounters') }} e on s.encounter_id = e.encounter_id
    left join {{ ref('dim_supplies') }} ds on s.patient_id = ds.patient_id and s.supply_code = ds.supply_code
    where p.patient_sk is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['patient_sk', 'supply_code']) }} as supply_event_sk,
    patient_sk,
    encounter_id,
    supply_date,
    supply_code,
    supply_description
from supplies