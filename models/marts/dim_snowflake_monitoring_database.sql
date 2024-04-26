WITH 
    database AS (
        SELECT *
        FROM {{ ref('stg_query_history') }}
    ),

    dim_database AS (
        SELECT
            database_id,
            database_name
        FROM {{ ref('stg_query_history') }}
    ),

    dim_database_sk AS (
        SELECT
            dbt_utils.generate_surrogate_key([database_id]) AS database_sk,
            *
        FROM dim_database
    )

SELECT *
FROM dim_database_sk
