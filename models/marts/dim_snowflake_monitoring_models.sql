WITH
    dim_snowflake_monitoring_models AS (
        SELECT 
            dbt_metadata['node_id']::string as dbt_node_id,
            coalesce(dbt_metadata['node_name']::string, replace(array_slice(split(dbt_node_id, '.'), -1, array_size(split(dbt_node_id, '.')))[0], '"')) as dbt_node_name
        FROM {{ ref('query_history_enriched') }}
    )

    , dim_snowflake_monitoring_models_sk AS (
        SELECT
            dbt_utils.generate_surrogate_key([dbt_node_id]) AS models_sk,
            *
        FROM {{ ref('query_history_enriched') }}
    )

SELECT *
FROM dim_snowflake_monitoring_models
