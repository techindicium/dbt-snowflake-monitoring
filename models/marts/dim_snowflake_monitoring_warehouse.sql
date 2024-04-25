WITH 
    warehouse AS (
        SELECT *
        FROM stg_query_history
    ),

    dim_warehouse AS (
        SELECT
            warehouse_id,
            warehouse_name,
            warehouse_size,
            warehouse_type
        FROM warehouse
    ),

    dim_warehouse_sk AS (
        SELECT
            dbt_utils.generate_surrogate_key([warehouse_id]) AS warehouse_sk,
            *
        FROM dim_warehouse
    )

SELECT *
FROM dim_warehouse_sk
