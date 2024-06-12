{{ config(materialized='view') }}
-- verificar a quantidade de database em outra tabela
select
    database_id
    , database_name
    , account_name
from {{ source('snowflake_organization_usage', 'database_storage_usage_history') }}