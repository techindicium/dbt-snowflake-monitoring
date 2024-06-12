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
),

query_history AS (
    SELECT
        warehouse_id,
        warehouse_name
    FROM {{ ref('stg_query_history') }}
),

combined AS (
    SELECT
        d.warehouse_sk,
        d.warehouse_id,
        d.warehouse_name,
        d.account_name
    FROM dim_warehouse_sk d
    WHERE d.unique_id = 1

    UNION ALL

    SELECT
        MD5(
            COALESCE(CAST(q.warehouse_id AS STRING), '') ||
            COALESCE(q.warehouse_name, '') ||
            COALESCE(CAST('' AS STRING), '')
        ) AS warehouse_sk,
        q.warehouse_id,
        q.warehouse_name,
        NULL AS account_name
    FROM query_history q
    LEFT JOIN dim_warehouse_sk d ON q.warehouse_id = d.warehouse_id
    WHERE d.warehouse_id IS NULL
)

SELECT DISTINCT
    warehouse_sk,
    warehouse_id,
    warehouse_name,
    account_name
FROM combined


-- pronto, já foi feito
-- tem usuario que não corresponde a um determinado warehouse id que não está disponivel na stg_waregouse_cost_credits
-- unir as informações disponiveis na tabela raw query history onde se encontramm o restante dos warehouse_id com a warehouse metering story


