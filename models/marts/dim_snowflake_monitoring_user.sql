{{ config(materialized='table', transient=true) }}

WITH dim_user_sk AS (
    SELECT
        MD5(CONCAT(
            COALESCE(CAST(query_id AS STRING), ''), 
            COALESCE(warehouse_name, ''),
            COALESCE(user_name, '')
        )) AS user_sk,
        ROW_NUMBER() OVER (
            PARTITION BY user_name
            ORDER BY query_id, warehouse_name, user_name
        ) AS unique_id
        , user_name
    FROM {{ ref('stg_query_history') }}
    WHERE user_name IS NOT NULL
)

SELECT
    MD5(CONCAT(user_sk, CAST(unique_id AS STRING))) AS user_sk
    , user_name
FROM dim_user_sk
WHERE unique_id = 1  -- Garante uma única linha por usuário
