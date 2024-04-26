WITH 
    schema AS (
        SELECT *
        FROM {{ ref('stg_query_history') }}
    )

    , dim_schema AS (
        SELECT
            schema_id,
            schema_name
        FROM {{ ref('stg_query_history') }}
    )

    , dim_schema_sk AS (
        SELECT
            dbt_utils.generate_surrogate_key([schema_id]) AS schema_sk,
            *
        FROM {{ ref('stg_query_history') }}
    )

SELECT *
FROM dim_schema_sk
