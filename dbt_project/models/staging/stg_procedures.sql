with source as (

    select * from {{ source('raw_synthea', 'procedures') }}

),

renamed as (

    select
        cast(start as timestamp) as procedure_start_timestamp,
        cast(stop as timestamp) as procedure_end_timestamp,
        patient as patient_id,
        encounter as encounter_id,
        system as code_system,
        code as procedure_code,
        description as procedure_description,
        cast(base_cost as numeric) as base_cost,
        reasoncode as reason_code,
        reasondescription as reason_description

    from source

)

select * from renamed