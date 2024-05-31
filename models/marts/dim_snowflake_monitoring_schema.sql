{{ config(materialized='table', transient=true) }}

WITH dim_schema_sk AS (
    SELECT
        schema_name,
        MIN(schema_id) AS schema_id  -- Escolher um schema_id arbitrário associado ao schema_name
    FROM {{ ref('stg_query_history') }}
    WHERE schema_name IS NOT NULL
    GROUP BY schema_name
)

SELECT
    MD5(CONCAT(
        COALESCE(CAST(schema_id AS STRING), ''),
        COALESCE(schema_name, '')
    )) AS schema_sk,
    schema_id,
    schema_name
FROM dim_schema_sk
