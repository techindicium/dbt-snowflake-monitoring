WITH
    dim_warehouse AS (
        SELECT *
        FROM {{ ref('dim_snowflake_monitoring_warehouse') }}
    )

    , dim_database AS (
        SELECT *
        FROM {{ ref('dim_snowflake_monitoring_database') }}
    )

    , dim_dates AS (
        SELECT *
        FROM {{ ref('dim_snowflake_monitoring_date') }}
    )

    , dim_schema AS (
        SELECT *
        FROM {{ ref('dim_snowflake_monitoring_schema') }}
    )

    , dim_user as (
        select *
        from {{ref('dim_snowflake_monitoring_user')}}
    )

    , query_history_enriched as (
        select *
        from {{ref('stg_query_history')}}
    )

    , fact_table AS (
        SELECT *  
        FROM {{ ref('stg_warehouse_cost_credits') }}
    )

    , deduplicated_fact_table AS (
        SELECT
            fact_table.*,
            ROW_NUMBER() OVER (PARTITION BY warehouse_id, warehouse_name, start_time ORDER BY start_time) AS row_num
        FROM fact_table
    )

    , filtered_fact_table AS (
        SELECT *
        FROM deduplicated_fact_table
        WHERE row_num = 1
    )

    , Duplicatas AS (
        SELECT
            {{ dbt_utils.generate_surrogate_key(
                [ 'filtered_fact_table.warehouse_id', 'filtered_fact_table.start_time']
                ) }} AS metrics_sk
           , dim_dates.date AS date_fk
           , dim_warehouse.warehouse_sk AS warehouse_fk
           , dim_database.database_sk AS database_fk
           , dim_schema.schema_sk AS schema_fk
           , dim_user.user_sk as user_fk
           , query_history_enriched.query_id as query_id
           , query_history_enriched.credits_used_cloud_services as credits_used_cloud_services
           , filtered_fact_table.compute_cost as compute_cost 
           , filtered_fact_table.credits_used as credits_used
           , filtered_fact_table.credits_used_compute as credits_used_compute
           , filtered_fact_table.query_cost as query_cost
           , filtered_fact_table.start_time as start_time
           , ROW_NUMBER() OVER (PARTITION BY {{ dbt_utils.generate_surrogate_key(
           ['filtered_fact_table.warehouse_id', 'filtered_fact_table.start_time']
            )}} ORDER BY filtered_fact_table.start_time) AS row_num
        FROM filtered_fact_table
        LEFT JOIN dim_warehouse
            ON filtered_fact_table.warehouse_id = dim_warehouse.warehouse_id
                and filtered_fact_table.account_name = dim_warehouse.account_name
        LEFT JOIN dim_dates 
            ON filtered_fact_table.start_time::date = dim_dates.date
        LEFT JOIN query_history_enriched 
            ON filtered_fact_table.warehouse_id = query_history_enriched.warehouse_id
        LEFT JOIN dim_schema
            ON filtered_fact_table.warehouse_id = dim_schema.warehouse_id
        LEFT JOIN dim_database
            ON filtered_fact_table.warehouse_id = dim_database.warehouse_id
        LEFT JOIN dim_user
            ON filtered_fact_table.warehouse_id = dim_user.warehouse_id         
    )
    SELECT
        metrics_sk
        , user_fk
        , date_fk
        , warehouse_fk
        , database_fk
        , schema_fk
        , query_id
        , compute_cost
        , query_cost
        , credits_used_cloud_services
        , credits_used
        , credits_used_compute
        , start_time
    FROM Duplicatas
    WHERE row_num = 1
