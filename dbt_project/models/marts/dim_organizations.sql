{{ config(materialized='table') }}

with organizations as (
    select distinct
        {{ dbt_utils.generate_surrogate_key(['organization_id']) }} as organization_sk,
        organization_id,
        organization_name,
        city,
        state,
        zipcode,
        coalesce(revenue, 0) as revenue,
        coalesce(utilization, 0) as utilization
    from {{ ref('stg_organizations') }}
    where organization_id is not null
)

select
    organization_sk,
    organization_id,
    organization_name,
    city,
    state,
    zipcode,
    revenue,
    utilization
from organizations