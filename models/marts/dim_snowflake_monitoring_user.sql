WITH 
    usuario AS (
        SELECT *
        FROM {{ ref('stg_query_history') }}
    )

    , dim_usuarios AS (
        SELECT
            user_name
        FROM {{ ref('stg_query_history') }}
    )

    , dim_usuarios_sk AS (
        SELECT
            dbt_utils.generate_surrogate_key([user_name]) AS usuario_sk,
            *
        FROM {{ ref('stg_query_history') }}
    )

SELECT *
FROM dim_usuarios_sk
