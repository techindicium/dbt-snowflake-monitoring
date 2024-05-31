WITH 
    dim_database_sk AS (
        SELECT
            MD5(CONCAT(COALESCE(CAST(database_id AS STRING), ''), COALESCE(CAST(database_name AS STRING), ''))) AS database_sk,
            ROW_NUMBER() OVER (PARTITION BY database_id, database_name ORDER BY database_id, database_name) AS unique_id,
            database_id,
            database_name
        FROM {{ ref('stg_query_history') }}
        WHERE database_name IS NOT NULL
    )
    
SELECT
    MD5(CONCAT(database_sk, CAST(unique_id AS STRING))) AS database_sk,
    database_id,
    database_name
FROM dim_database_sk
WHERE unique_id = 1  -- Filtrar para garantir apenas uma linha por combinação de database_id e database_name
