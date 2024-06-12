WITH dim_warehouse_sk AS (
    SELECT
        MD5(
            COALESCE(CAST(warehouse_id AS STRING), '') ||
            COALESCE(warehouse_name, '') ||
            COALESCE(account_name, '')
        ) AS warehouse_sk,
        ROW_NUMBER() OVER (
            PARTITION BY warehouse_id, warehouse_name, account_name
            ORDER BY warehouse_id, warehouse_name, account_name
        ) AS unique_id,
        warehouse_id,
        warehouse_name,
        account_name
    FROM {{ ref('stg_warehouse_cost_credits') }}
)

SELECT
    MD5(
        COALESCE(warehouse_sk, '') ||
        COALESCE(CAST(unique_id AS STRING), '')
    ) AS warehouse_sk,
    warehouse_id,
    warehouse_name,
    account_name
FROM dim_warehouse_sk
WHERE unique_id = 1 -- Garante uma única linha por warehouse
