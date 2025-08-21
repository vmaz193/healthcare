-- models/staging/stg_payer_transitions.sql

with source as (

    select * from {{ source('raw_synthea', 'payer_transitions') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['patient', 'start_date']) }} as payer_transition_id,
        patient as patient_id,
        memberid as member_id,
        cast(start_date as timestamp) as start_date,
        cast(end_date as timestamp) as end_date,
        payer as payer_id,
        secondary_payer as secondary_payer_id,
        plan_ownership,
        owner_name

    from source

)

select * from renamed