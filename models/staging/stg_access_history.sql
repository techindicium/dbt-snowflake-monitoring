{{ config(materialized='incremental') }}

select
    --CAST(query_id AS VARCHAR) AS query_id,
    to_varchar(query_id) as query_id,
    CAST(query_start_time AS DATETIME) AS query_start_time,
    CAST(user_name AS VARCHAR) AS user_name,
    --direct_objects_accessed,
    --base_objects_accessed,
   -- objects_modified
from {{ source('snowflake_account_usage', 'access_history') }}

{% if is_incremental() %}
    where query_start_time > (select coalesce(max(query_start_time), date '1970-01-01') from {{ this }})
{% endif %}

order by query_start_time asc
