with stg_claims as (

    select * from {{ ref('stg_claims') }}

),

stg_claims_transactions as (

    select * from {{ ref('stg_claims_transactions') }}

)

select
    -- Claim details
    c.claim_id,
    c.patient_id,
    c.provider_id,
    c.service_date,
    c.current_illness_date,
    
    -- Transaction line details
    ct.claim_transaction_id,
    ct.charge_id,
    ct.transaction_type,
    ct.transaction_amount,
    ct.procedure_code,
    ct.diagnosis_ref_1,
    ct.diagnosis_ref_2,
    ct.diagnosis_ref_3,
    ct.diagnosis_ref_4,
    ct.units,
    ct.payments,
    ct.adjustments,
    ct.outstanding

from stg_claims as c
join stg_claims_transactions as ct
    on c.claim_id = ct.claim_id