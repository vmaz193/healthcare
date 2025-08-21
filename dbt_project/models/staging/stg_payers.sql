with source as (

    select * from {{ source('raw_synthea', 'payers') }}

),

renamed as (

    select
        id as payer_id,
        name as payer_name,
        ownership,
        --address,
        --city,
        ---state_headquartered,
        --zip as zipcode,
        --phone,
        cast(amount_covered as numeric) as amount_covered,
        cast(amount_uncovered as numeric) as amount_uncovered,
        cast(revenue as numeric) as revenue,
        cast(covered_encounters as integer) as covered_encounters,
        cast(uncovered_encounters as integer) as uncovered_encounters,
        cast(covered_medications as integer) as covered_medications,
        cast(uncovered_medications as integer) as uncovered_medications,
        cast(covered_procedures as integer) as covered_procedures,
        cast(uncovered_procedures as integer) as uncovered_procedures,
        cast(covered_immunizations as integer) as covered_immunizations,
        cast(uncovered_immunizations as integer) as uncovered_immunizations,
        cast(unique_customers as integer) as unique_customers,
        cast(qols_avg as numeric) as qols_avg,
        cast(member_months as integer) as member_months

    from source

)

select * from renamed