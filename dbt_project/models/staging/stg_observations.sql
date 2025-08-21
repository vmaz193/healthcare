-- models/staging/stg_observations.sql

with source as (

    select * from {{ source('raw_synthea', 'observations') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['encounter', 'code', 'date']) }} as observation_id,
        cast(date as date) as observation_date,
        patient as patient_id,
        encounter as encounter_id,
        category as observation_category,
        code as observation_code,
        description as observation_description,
        value as observation_value,
        units as observation_units,
        type as observation_type

    from source

)

select * from renamed