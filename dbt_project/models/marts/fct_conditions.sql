with stg_conditions as (

    select * from {{ ref('stg_conditions') }}

),

dim_conditions as (

    select * from {{ ref('dim_conditions') }}

)

select
    -- Foreign Keys
    dc.condition_sk,
    sc.patient_id,
    sc.encounter_id,

    -- Dates
    sc.condition_start_date,
    sc.condition_end_date

from stg_conditions as sc
left join dim_conditions as dc
    on sc.condition_code = dc.condition_code
    and sc.code_system = dc.code_system