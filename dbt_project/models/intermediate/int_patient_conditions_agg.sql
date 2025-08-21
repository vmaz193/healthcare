with stg_conditions as (

    select * from {{ ref('stg_conditions') }}

)

select
    patient_id,
    condition_code,
    condition_description,
    min(condition_start_date) as first_diagnosed_date,
    max(condition_end_date) as last_resolved_date

from stg_conditions
group by 1, 2, 3