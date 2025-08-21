with source as (

    select * from {{ source('raw_synthea', 'providers') }}

),

renamed as (

    select
        id as provider_id,
        organization as organization_id,
        name as provider_name,
        gender,
        speciality as specialty,
        address,
        city,
        state,
        zip as zipcode,
        cast(lat as float64) as latitude,
        cast(lon as float64) as longitude,
        cast(encounters as integer) as encounters,
        cast(procedures as integer) as procedures

    from source

)

select * from renamed