{{ config(materialized='table', transient=true) }}

WITH dim_schema_sk AS (
    SELECT
        schema_name,
        MIN(schema_id) AS schema_id,
        warehouse_id
    FROM {{ ref('stg_query_history') }}
    WHERE schema_name IS NOT NULL
    GROUP BY schema_name, warehouse_id
)

SELECT
    MD5(
        COALESCE(CAST(schema_id AS STRING), '') ||
        COALESCE(schema_name, '') ||
        COALESCE(CAST(warehouse_id AS STRING), '')
    ) AS schema_sk,
    schema_id,
    schema_name,
    warehouse_id
FROM dim_schema_sk
