with source as (

    select * from {{ source('raw_synthea', 'encounters') }}

),

renamed as (

    select
        id as encounter_id,
        cast(start as timestamp) as encounter_start_timestamp,
        cast(stop as timestamp) as encounter_end_timestamp,
        patient as patient_id,
        organization as organization_id,
        provider as provider_id,
        payer as payer_id,
        encounterclass as encounter_class,
        code as encounter_code,
        description as encounter_description,
        cast(base_encounter_cost as numeric) as base_encounter_cost,
        cast(total_claim_cost as numeric) as total_claim_cost,
        cast(payer_coverage as numeric) as payer_coverage,
        reasoncode as reason_code,
        reasondescription as reason_description

    from source

)

select * from renamed