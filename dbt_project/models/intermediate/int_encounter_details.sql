with stg_encounters as (

    select * from {{ ref('stg_encounters') }}

),

stg_patients as (

    select * from {{ ref('stg_patients') }}

),

stg_organizations as (

    select * from {{ ref('stg_organizations') }}

)

select
    -- Encounter details
    e.encounter_id,
    e.encounter_start_timestamp,
    e.encounter_end_timestamp,
    e.encounter_class,
    e.encounter_code,
    e.encounter_description,
    e.base_encounter_cost,
    e.total_claim_cost,
    e.payer_coverage,
    e.reason_code as encounter_reason_code,
    e.reason_description as encounter_reason_description,
    
    -- Patient details
    e.patient_id,
    p.first_name,
    p.last_name,
    p.gender,
    p.race,
    p.ethnicity,
    p.birth_date,
    p.death_date,
    
    -- Organization details
    e.organization_id,
    o.organization_name,
    o.address as organization_address,
    o.city as organization_city,
    o.state as organization_state,
    o.zipcode as organization_zipcode,

    -- Provider and Payer IDs for further joins
    e.provider_id,
    e.payer_id

from stg_encounters as e
left join stg_patients as p
    on e.patient_id = p.patient_id
left join stg_organizations as o
    on e.organization_id = o.organization_id