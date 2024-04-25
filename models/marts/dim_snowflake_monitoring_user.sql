WITH 
    usuario AS (
        SELECT *
        FROM stg_query_history
    ),

    dim_usuarios AS (
        SELECT
            user_name
        FROM usuario
    ),

    dim_usuarios_sk AS (
        SELECT
            dbt_utils.generate_surrogate_key([user_name]) AS usuario_sk,
            *
        FROM dim_usuarios
    )

SELECT *
FROM dim_usuarios_sk
