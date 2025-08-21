with source as (

    select * from {{ source('raw_synthea', 'patients') }}

),

renamed as (

    select
        id as patient_id,
        first as first_name,
        middle as middle_name,
        last as last_name,
        race,
        ethnicity,
        gender,
        cast(birthdate as date) as birth_date,
        cast(deathdate as date) as death_date,
        ssn,
        drivers as drivers_license,
        passport,
        prefix as name_prefix,
        suffix as name_suffix,
        maiden as maiden_name,
        marital as marital_status,
        birthplace,
        address,
        city,
        state,
        county,
        fips as fips_code,
        zip as zipcode,
        cast(lat as float64) as latitude,
        cast(lon as float64) as longitude,
        cast(healthcare_expenses as numeric) as healthcare_expenses,
        cast(healthcare_coverage as numeric) as healthcare_coverage,
        cast(income as integer) as income

    from source

)

select * from renamed