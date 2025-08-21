with source as (

    select * from {{ source('raw_synthea', 'immunizations') }}

),

renamed as (

    select
        cast(date as timestamp) as immunization_date,
        patient as patient_id,
        encounter as encounter_id,
        code as immunization_code,
        description as immunization_description,
        cast(base_cost as numeric) as base_cost

    from source

)

select * from renamed