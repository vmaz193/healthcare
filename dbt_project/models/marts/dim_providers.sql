{{ config(materialized='table') }}

with providers as (
    select distinct
        {{ dbt_utils.generate_surrogate_key(['provider_id']) }} as provider_sk,
        provider_id,
        organization_id,
        provider_name,
        specialty,
        city,
        state,
        zipcode,
        coalesce(encounters, 0) as encounters,
        coalesce(procedures, 0) as procedures
    from {{ ref('stg_providers') }}
    where provider_id is not null
)

select
    provider_sk,
    provider_id,
    organization_id,
    provider_name,
    specialty,
    city,
    state,
    zipcode,
    encounters,
    procedures
from providers