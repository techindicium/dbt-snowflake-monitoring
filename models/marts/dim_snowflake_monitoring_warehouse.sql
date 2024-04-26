WITH 
    warehouse AS (
        SELECT *
        FROM {{ ref('stg_query_history') }}
    ),

    dim_warehouse AS (
        SELECT
            warehouse_id,
            warehouse_name,
            warehouse_size,
            warehouse_type
        FROM {{ ref('stg_query_history') }}
    ),

    dim_warehouse_sk AS (
        SELECT
            dbt_utils.generate_surrogate_key([warehouse_id]) AS warehouse_sk,
            *
        FROM {{ ref('stg_query_history') }}
    )


SELECT *
FROM dim_warehouse_sk
