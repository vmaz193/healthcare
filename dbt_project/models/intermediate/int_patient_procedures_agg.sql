with stg_procedures as (

    select * from {{ ref('stg_procedures') }}

)

select
    patient_id,
    count(distinct procedure_code) as count_of_distinct_procedures,
    count(procedure_code) as total_procedures

from stg_procedures
group by 1