WITH
    dim_warehouse AS (
        SELECT *
        FROM {{ ref('dim_snowflake_monitoring_warehouse') }}
    ),

    dim_database AS (
        SELECT *
        FROM {{ ref('dim_snowflake_monitoring_database') }}
    ),

    dim_dates AS (
        SELECT *
        FROM {{ ref('dim_snowflake_monitoring_date') }}
    ),

    dim_schema AS (
        SELECT *
        FROM {{ ref('dim_snowflake_monitoring_schema') }}
    ),

    dim_user as (
        select *
        from {{ref('dim_snowflake_monitoring_user')}}
    ),

    fact_table AS (
        SELECT *  
        FROM {{ ref('query_history_enriched') }}
    ),

    deduplicated_fact_table AS (
        SELECT
            fact_table.*,
            ROW_NUMBER() OVER (PARTITION BY query_id, warehouse_id, database_id, schema_id, session_id, user_name, start_time ORDER BY start_time) AS row_num
        FROM fact_table
    ),

    filtered_fact_table AS (
        SELECT *
        FROM deduplicated_fact_table
        WHERE row_num = 1
    ),

    Duplicatas AS (
        SELECT
            {{ dbt_utils.generate_surrogate_key(['filtered_fact_table.query_id', 'filtered_fact_table.warehouse_id', 'filtered_fact_table.database_id', 'filtered_fact_table.schema_id', 'filtered_fact_table.session_id', 'filtered_fact_table.user_name', 'filtered_fact_table.start_time']) }} AS metrics_sk,
            dim_dates.date AS date_fk,
            dim_warehouse.warehouse_sk AS warehouse_fk,
            dim_database.database_sk AS database_fk,
            dim_schema.schema_sk AS schema_fk,
            dim_user.user_sk as user_fk, 
            filtered_fact_table.query_id as query_id,
            filtered_fact_table.dbt_node_id AS dbt_node_id,
            filtered_fact_table.compute_credits AS compute_credits,
            filtered_fact_table.query_credits AS query_credits,
            filtered_fact_table.query_cost as query_cost,
            filtered_fact_table.compute_cost as compute_cost,
            filtered_fact_table.credits_used_cloud_services as credits_used_cloud_services,
            filtered_fact_table.start_time as start_time,
            ROW_NUMBER() OVER (PARTITION BY {{ dbt_utils.generate_surrogate_key(['filtered_fact_table.query_id', 'filtered_fact_table.warehouse_id', 'filtered_fact_table.database_id', 'filtered_fact_table.schema_id', 'filtered_fact_table.session_id', 'filtered_fact_table.user_name', 'filtered_fact_table.start_time']) }} ORDER BY filtered_fact_table.start_time) AS row_num
        FROM filtered_fact_table
        LEFT JOIN dim_database ON filtered_fact_table.database_id = dim_database.database_id
        LEFT JOIN dim_warehouse ON filtered_fact_table.warehouse_id = dim_warehouse.warehouse_id
        LEFT JOIN dim_schema ON filtered_fact_table.schema_id = dim_schema.schema_id
        LEFT JOIN dim_dates ON filtered_fact_table.start_time::date = dim_dates.date
        LEFT JOIN dim_user on filtered_fact_table.user_name = dim_user.user_name
    )
    SELECT
        metrics_sk,
        user_fk,
        date_fk,
        warehouse_fk,
        database_fk,
        schema_fk,
        dbt_node_id,
        query_id,
        compute_credits,
        query_credits,
        credits_used_cloud_services,
        compute_cost,
        query_cost,
        start_time
    FROM Duplicatas
    WHERE row_num = 1
