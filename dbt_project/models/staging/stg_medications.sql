with source as (

    select * from {{ source('raw_synthea', 'medications') }}

),

renamed as (

    select
        cast(start as date) as medication_start_date,
        cast(stop as date) as medication_end_date,
        patient as patient_id,
        payer as payer_id,
        encounter as encounter_id,
        code as medication_code,
        description as medication_description,
        cast(base_cost as numeric) as base_cost,
        cast(payer_coverage as numeric) as payer_coverage,
        cast(dispenses as integer) as dispenses,
        cast(totalcost as numeric) as total_cost,
        reasoncode as reason_code,
        reasondescription as reason_description

    from source

)

select * from renamed