with source as (

    select * from {{ source('raw_synthea', 'imaging_studies') }}

),

renamed as (

    select
        id as imaging_study_id,
        cast(date as timestamp) as imaging_study_date,
        patient as patient_id,
        encounter as encounter_id,
        series_uid,
        instance_uid,
        bodysite_code,
        bodysite_description,
        modality_code,
        modality_description,
        sop_code,
        sop_description,
        procedure_code

    from source

)

select * from renamed