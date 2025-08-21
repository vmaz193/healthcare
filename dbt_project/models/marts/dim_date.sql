{{ config(materialized='table') }}

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('1919-01-01' as date)",
        end_date="cast('2025-12-31' as date)"
    ) }}
)

select
    date_day as date,
    extract(year from date_day) as year,
    extract(quarter from date_day) as quarter,
    extract(month from date_day) as month,
    extract(day from date_day) as day,
    extract(dayofweek from date_day) as day_of_week,
    case when extract(dayofweek from date_day) in (1, 7) then true else false end as is_weekend,
    format_date('%B', date_day) as month_name,
    format_date('%A', date_day) as day_name
from date_spine