{{ config(materialized='table') }}

with bounds as (
    select
        min(request_date) as min_date,
        max(request_date) as max_date
    from {{ ref('fact_requests') }}
), date_spine as (
    select cast(date_day as date) as date_day
    from bounds,
    generate_series(min_date, max_date, interval 1 day) as series(date_day)
)

select
    date_day,
    extract(year from date_day) as calendar_year,
    extract(quarter from date_day) as calendar_quarter,
    extract(month from date_day) as calendar_month,
    strftime(date_day, '%B') as month_name,
    extract(day from date_day) as day_of_month,
    extract(isodow from date_day) as iso_day_of_week,
    strftime(date_day, '%A') as day_name,
    case when extract(isodow from date_day) in (6, 7) then true else false end as is_weekend
from date_spine
