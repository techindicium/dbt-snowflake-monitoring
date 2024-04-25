WITH 
    schema AS (
        SELECT *
        FROM stg_query_history
    ),

    dim_schema AS (
        SELECT
            schema_id,
            schema_name
        FROM schema
    ),

    dim_schema_sk AS (
        SELECT
            dbt_utils.generate_surrogate_key([schema_id]) AS schema_sk,
            *
        FROM dim_schema
    )

SELECT *
FROM dim_schema_sk
