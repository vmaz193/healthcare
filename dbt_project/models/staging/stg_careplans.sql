with source as (

    select * from {{ source('raw_synthea', 'careplans') }}

),

renamed as (

    select
        id as careplan_id,
        cast(start as date) as careplan_start_date,
        cast(stop as date) as careplan_end_date,
        patient as patient_id,
        encounter as encounter_id,
        code as careplan_code,
        description as careplan_description,
        reasoncode as reason_code,
        reasondescription as reason_description

    from source

)

select * from renamed