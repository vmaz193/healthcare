{{ config(materialized='table') }}

with claims as (
    select
        c.claim_id,
        p.patient_sk,
        pr.provider_sk,
        cast(c.service_date as date) as service_date,
        c.diagnosis_1,
        c.diagnosis_2,
        coalesce(c.outstanding_1, 0) as outstanding_1,
        coalesce(c.outstanding_2, 0) as outstanding_2,
        c.healthcare_claim_type_id_1
    from {{ ref('stg_claims') }} c
    left join {{ ref('dim_patients') }} p on c.patient_id = p.patient_id
    left join {{ ref('dim_providers') }} pr on c.provider_id = pr.provider_id
    where p.patient_sk is not null
)

select
    claim_id,
    patient_sk,
    provider_sk,
    service_date,
    diagnosis_1,
    diagnosis_2,
    outstanding_1,
    outstanding_2,
    healthcare_claim_type_id_1
from claims