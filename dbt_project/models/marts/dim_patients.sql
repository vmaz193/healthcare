{{ config(materialized='table') }}

with patients as (
    select distinct
        {{ dbt_utils.generate_surrogate_key(['patient_id']) }} as patient_sk,
        patient_id,
        first_name,
        last_name,
        concat(coalesce(first_name, ''), ' ', coalesce(last_name, '')) as full_name,
        gender,
        race,
        ethnicity,
        birth_date,
        date_diff(current_date, birth_date, year) as age,
        address,
        city,
        state,
        zipcode,
        coalesce(healthcare_expenses, 0) as healthcare_expenses,
        coalesce(income, 0) as income
    from {{ ref('stg_patients') }}
    where patient_id is not null
)

select
    patient_sk,
    patient_id,
    full_name,
    gender,
    race,
    ethnicity,
    birth_date,
    age,
    address,
    city,
    state,
    zipcode,
    healthcare_expenses,
    income
from patients