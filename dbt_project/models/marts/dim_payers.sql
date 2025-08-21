{{ config(materialized='table') }}

with payers as (
    select distinct
        {{ dbt_utils.generate_surrogate_key(['payer_id']) }} as payer_sk,
        payer_id,
        payer_name,
        ownership,
        coalesce(amount_covered, 0) as amount_covered,
        coalesce(amount_uncovered, 0) as amount_uncovered,
        coalesce(revenue, 0) as revenue,
        coalesce(covered_encounters, 0) as covered_encounters,
        coalesce(uncovered_encounters, 0) as uncovered_encounters,
        coalesce(covered_medications, 0) as covered_medications,
        coalesce(uncovered_medications, 0) as uncovered_medications,
        coalesce(covered_procedures, 0) as covered_procedures,
        coalesce(uncovered_procedures, 0) as uncovered_procedures,
        coalesce(covered_immunizations, 0) as covered_immunizations,
        coalesce(uncovered_immunizations, 0) as uncovered_immunizations,
        coalesce(unique_customers, 0) as unique_customers,
        coalesce(qols_avg, 0) as qols_avg,
        coalesce(member_months, 0) as member_months
    from {{ ref('stg_payers') }}
    where payer_id is not null
)

select
    payer_sk,
    payer_id,
    payer_name,
    ownership,
    amount_covered,
    amount_uncovered,
    revenue,
    covered_encounters,
    uncovered_encounters,
    covered_medications,
    uncovered_medications,
    covered_procedures,
    uncovered_procedures,
    covered_immunizations,
    uncovered_immunizations,
    unique_customers,
    qols_avg,
    member_months
from payers