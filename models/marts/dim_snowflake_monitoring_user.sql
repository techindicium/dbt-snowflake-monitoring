WITH dim_user_sk AS (
    SELECT
        MD5(
            COALESCE(CAST(warehouse_id AS STRING), '') ||
            COALESCE(user_name, '')
        ) AS user_sk,
        ROW_NUMBER() OVER (
            PARTITION BY user_name, warehouse_id
            ORDER BY user_name, warehouse_id
        ) AS unique_id,
        user_name,
        warehouse_id
    FROM {{ ref('stg_query_history') }}
    WHERE user_name IS NOT NULL
)

SELECT
    MD5(
        COALESCE(user_sk, '') ||
        COALESCE(CAST(unique_id AS STRING), '')
    ) AS user_sk,
    user_name,
    warehouse_id
FROM dim_user_sk
WHERE unique_id = 1 -- Garante uma única linha por usuário

-- tem usuario que não corresponde a um determinado warehouse id que não está disponivel na stg_waregouse_cost_credits
-- unir as informações disponiveis na tabela raw query history onde se encontramm o restante dos warehouse_id com a warehouse metering story