with source as (

    select * from {{ source('raw_synthea', 'devices') }}

),

renamed as (

    select
        cast(start as timestamp) as device_start_timestamp,
        cast(stop as timestamp) as device_end_timestamp,
        patient as patient_id,
        encounter as encounter_id,
        code as device_code,
        description as device_description,
        udi

    from source

)

select * from renamed