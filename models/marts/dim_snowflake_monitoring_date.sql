-- models/dim_snowflake_monitoring_date.sql

-- Criação da dimensão de data
{{ config(
    materialized='table',
    unique_key='date',
    transient = true
) }}
with dates as (
    select 
        dateadd(day, row_number() over(order by true), '2000-01-01') as date
    from table(generator(rowcount => 10000))
)

select
    date
    , year(date) as year
    , month(date) as month
    , dayofmonth(date) as day
    , dayofweek(date) as day_of_week
    , weekofyear(date) as week_of_year
    , quarter(date) as quarter
    , DAYOFYEAR(date) as day_of_year
    , date_part('week', date) as iso_week
    , date_part('year', date) as iso_year
    , date_part('week', date) - date_part('week', date_trunc('month', date)) + 1 as week_of_month
    , case 
        when dayofweek(date) in (1,7) then 'Weekend' 
        else 'Weekday' 
        end as weekday_weekend
from dates