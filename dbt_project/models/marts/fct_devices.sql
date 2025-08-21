{{ config(materialized='table') }}

with devices as (
    select
        d.device_code,
        p.patient_sk,
        e.encounter_id,
        cast(d.device_start_timestamp as date) as device_start_date,
        cast(d.device_end_timestamp as date) as device_end_date,
        d.device_description,
        d.udi
    from {{ ref('stg_devices') }} d
    left join {{ ref('dim_patients') }} p on d.patient_id = p.patient_id
    left join {{ ref('fct_encounters') }} e on d.encounter_id = e.encounter_id
    left join {{ ref('dim_devices') }} dd on d.patient_id = dd.patient_id and d.device_code = dd.device_code
    where p.patient_sk is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['patient_sk', 'device_code']) }} as device_event_sk,
    patient_sk,
    encounter_id,
    device_start_date,
    device_end_date,
    device_code,
    device_description,
    udi
from devices