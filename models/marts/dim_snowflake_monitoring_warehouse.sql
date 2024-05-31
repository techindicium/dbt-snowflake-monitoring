{{ config(materialized='table', transient=true) }}

WITH dim_warehouse_sk AS (
    SELECT
        MD5(CONCAT(
            COALESCE(CAST(warehouse_id AS STRING), ''), 
            COALESCE(warehouse_name, '')
        )) AS warehouse_sk,
        ROW_NUMBER() OVER (
            PARTITION BY warehouse_id, warehouse_name 
            ORDER BY warehouse_id, warehouse_name
        ) AS unique_id,
        warehouse_id,
        warehouse_name,
        credits_used
    FROM {{ ref('stg_warehouse_metering_history') }}
    WHERE warehouse_name IS NOT NULL
)

SELECT
    MD5(CONCAT(warehouse_sk, CAST(unique_id AS STRING))) AS warehouse_sk,
    warehouse_id,
    warehouse_name,
    credits_used
FROM dim_warehouse_sk
WHERE unique_id = 1  -- Garante uma única linha por combinação de warehouse_id e warehouse_name
