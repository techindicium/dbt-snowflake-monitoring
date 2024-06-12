{{ config(
    materialized='incremental', 
    unique_key=['start_time','account_name', 'warehouse_id'],
) }}

with current_rates as (
    select
        effective_rate,
        currency,
        is_latest_rate
    from {{ ref('int_daily_rates') }}
    where service_type = 'COMPUTE'
        and usage_type = 'compute'
        and is_latest_rate
)

, daily_rates as (
    select
        date,
        service_type,
        usage_type,
        effective_rate
    from {{ ref('int_daily_rates') }}
    where service_type = 'COMPUTE'
        and usage_type = 'compute'
)

select
    wm.start_time,
    wm.end_time,
    wm.account_name,
    wm.warehouse_id,
    wm.warehouse_name,
    wm.credits_used,
    wm.credits_used_compute,
    wm.credits_used_cloud_services,
    wm.credits_used_compute * dr.effective_rate as compute_cost,
    wm.credits_used_compute * dr.effective_rate + 
    (div0(wm.credits_used_cloud_services, cbd.daily_credits_used_cloud_services) * cbd.daily_billable_cloud_services) * coalesce(dr.effective_rate, cr.effective_rate) as query_cost
from {{ source('snowflake_organization_usage', 'warehouse_metering_history') }} wm
left join daily_rates dr
    on date(wm.start_time) = dr.date
left join current_rates cr
    on cr.is_latest_rate
left join (
    select
        date(start_time) as date,
        sum(credits_used_compute) as daily_credits_used_compute,
        sum(credits_used_cloud_services) as daily_credits_used_cloud_services,
        greatest(sum(credits_used_cloud_services) - sum(credits_used_compute) * 0.1, 0) as daily_billable_cloud_services
    from {{ source('snowflake_organization_usage', 'warehouse_metering_history') }}
    group by date(start_time)
) cbd
    on date(wm.start_time) = cbd.date
{% if is_incremental() %}
    where wm.end_time > (select coalesce(dateadd(day, -7, max(end_time)), '1970-01-01') from {{ this }})
{% endif %}
order by wm.start_time
