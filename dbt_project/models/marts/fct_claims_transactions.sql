{{ config(materialized='table') }}

with claims_transactions as (
    select
        ct.claim_transaction_id,
        c.claim_id,
        p.patient_sk,
        pr.provider_sk,
        cast(ct.from_date as date) as from_date,
        cast(ct.to_date as date) as to_date,
        ct.transaction_type,
        coalesce(ct.transaction_amount, 0) as transaction_amount,
        ct.procedure_code,
        ct.diagnosis_ref_1,
        coalesce(ct.units, 0) as units
    from {{ ref('stg_claims_transactions') }} ct
    left join {{ ref('dim_patients') }} p on ct.patient_id = p.patient_id
    left join {{ ref('dim_providers') }} pr on ct.provider_id = pr.provider_id
    left join {{ ref('fct_claims') }} c on ct.claim_id = c.claim_id
    where p.patient_sk is not null
)

select
    claim_transaction_id,
    claim_id,
    patient_sk,
    provider_sk,
    from_date,
    to_date,
    transaction_type,
    transaction_amount,
    procedure_code,
    diagnosis_ref_1,
    units
from claims_transactions