with source as (

    select * from {{ source('raw_synthea', 'organizations') }}

),

renamed as (

    select
        id as organization_id,
        name as organization_name,
        address,
        city,
        state,
        zip as zipcode,
        cast(lat as float64) as latitude,
        cast(lon as float64) as longitude,
        phone,
        cast(revenue as numeric) as revenue,
        cast(utilization as integer) as utilization

    from source

)

select * from renamed