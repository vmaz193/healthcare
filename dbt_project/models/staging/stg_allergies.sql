with source as (

    select * from {{ source('raw_synthea', 'allergies') }}

),

renamed as (

    select
        cast(start as date) as allergy_start_date,
        --cast(stop as date) as allergy_end_date,
        patient as patient_id,
        encounter as encounter_id,
        code as allergy_code,
        system as code_system,
        description as allergy_description,
        type as allergy_type,
        category as allergy_category,
        reaction_1 as reaction_1_code,
        description_1 as reaction_1_description,
        severity_1 as reaction_1_severity,
        reaction_2 as reaction_2_code,
        description_2 as reaction_2_description,
        severity_2 as reaction_2_severity

    from source

)

select * from renamed