with source as (

    select * from {{ source('raw_synthea', 'supplies') }}

),

renamed as (

    select
        cast(date as date) as supply_date,
        patient as patient_id,
        encounter as encounter_id,
        code as supply_code,
        description as supply_description,
        cast(quantity as integer) as quantity

    from source

)

select * from renamed