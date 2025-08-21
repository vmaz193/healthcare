{{ config(materialized='table') }}

with encounters as (
    select
        e.encounter_id,
        p.patient_sk,
        pr.provider_sk,
        py.payer_sk,
        o.organization_sk,
        cast(e.encounter_start_timestamp as date) as encounter_start_date,
        cast(e.encounter_end_timestamp as date) as encounter_end_date,
        e.encounter_class,
        coalesce(e.base_encounter_cost, 0) as base_encounter_cost,
        coalesce(e.total_claim_cost, 0) as total_claim_cost,
        coalesce(e.payer_coverage, 0) as payer_coverage,
        e.reason_code,
        e.reason_description
    from {{ ref('stg_encounters') }} e
    left join {{ ref('dim_patients') }} p on e.patient_id = p.patient_id
    left join {{ ref('dim_providers') }} pr on e.provider_id = pr.provider_id
    left join {{ ref('dim_payers') }} py on e.payer_id = py.payer_id
    left join {{ ref('dim_organizations') }} o on e.organization_id = o.organization_id
    where p.patient_sk is not null
)

select
    encounter_id,
    patient_sk,
    provider_sk,
    payer_sk,
    organization_sk,
    encounter_start_date,
    encounter_end_date,
    encounter_class,
    base_encounter_cost,
    total_claim_cost,
    payer_coverage,
    reason_code,
    reason_description
from encounters