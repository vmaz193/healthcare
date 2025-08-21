with source as (

    select * from {{ source('raw_synthea', 'conditions') }}

),

renamed as (

    select
        cast(start as date) as condition_start_date,
        cast(stop as date) as condition_end_date,
        patient as patient_id,
        encounter as encounter_id,
        system as code_system,
        code as condition_code,
        description as condition_description

    from source

)

select * from renamed