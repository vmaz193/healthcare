{{ config(materialized='table') }}

with devices_dedup as (
    select
        patient_id,
        device_code,
        device_description,
        udi,
        row_number() over (partition by patient_id, device_code order by device_start_timestamp desc) as rn
    from {{ ref('stg_devices') }}
    where patient_id is not null and device_code is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['patient_id', 'device_code']) }} as device_sk,
    patient_id,
    device_code,
    device_description,
    udi
from devices_dedup
where rn = 1