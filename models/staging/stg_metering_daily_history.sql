{{ config(materialized='view') }}

select
    usage_date as date
    , service_type
    , credits_used_cloud_services
    , credits_adjustment_cloud_services
from {{ source('snowflake_organization_usage', 'metering_daily_history') }}
