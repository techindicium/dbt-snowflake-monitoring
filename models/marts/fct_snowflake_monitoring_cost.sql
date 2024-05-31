{{ config(
    materialized='table',
    unique_key=['query_id', 'start_time'],
    transient = true
) }}

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

    Duplicatas AS (
        SELECT
            {{dbt_utils.generate_surrogate_key(['fact_table.query_id','fact_table.warehouse_id','fact_table.database_id','fact_table.schema_id','fact_table.session_id'])}} AS metrics_sk,
            dim_dates.date AS date_fk,
            dim_warehouse.warehouse_sk AS warehouse_fk,
            dim_database.database_sk AS database_fk,
            dim_schema.schema_sk AS schema_fk,
            dim_user.user_sk as user_fk,
            fact_table.query_id as query_id,
            fact_table.dbt_node_id AS dbt_node_id,
            fact_table.compute_cost AS compute_cost,
            fact_table.query_cost AS query_cost,
            fact_table.start_time AS start_time,
            ROW_NUMBER() OVER (PARTITION BY {{dbt_utils.generate_surrogate_key(['fact_table.query_id','fact_table.warehouse_id','fact_table.database_id','fact_table.schema_id','fact_table.session_id'])}} ORDER BY fact_table.start_time) AS row_num
        FROM fact_table
        LEFT JOIN dim_database ON fact_table.database_id = dim_database.database_id
        LEFT JOIN dim_warehouse ON fact_table.warehouse_id = dim_warehouse.warehouse_id
        LEFT JOIN dim_schema ON fact_table.schema_id = dim_schema.schema_id
        LEFT JOIN dim_dates ON fact_table.start_time::date = dim_dates.date
        LEFT JOIN dim_user on fact_table.user_name = dim_user.user_name
    )
    SELECT
        metrics_sk,
        date_fk,
        warehouse_fk,
        database_fk,
        schema_fk,
        user_fk,
        dbt_node_id,
        compute_cost,
        query_cost,
        start_time
    FROM Duplicatas
    WHERE row_num = 1
