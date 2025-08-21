{{ config(materialized='table') }}

with payer_transitions as (
    select
        pt.payer_transition_id,
        p.patient_sk,
        py.payer_sk,
        cast(pt.start_date as date) as start_date,
        cast(pt.end_date as date) as end_date,
        pt.plan_ownership
    from {{ ref('stg_payer_transitions') }} pt
    left join {{ ref('dim_patients') }} p on pt.patient_id = p.patient_id
    left join {{ ref('dim_payers') }} py on pt.payer_id = py.payer_id
    where p.patient_sk is not null
)

select
    payer_transition_id,
    patient_sk,
    payer_sk,
    start_date,
    end_date,
    plan_ownership
from payer_transitions