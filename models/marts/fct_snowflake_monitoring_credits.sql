-- Tabelas de dimensão
WITH 
    dim_snowflake_monitoring_usuario AS (
        SELECT *
        FROM {{ ref('dim_snowflake_monitoring_user') }}
    ),

    dim_snowflake_monitoring_warehouse AS (
        SELECT *
        FROM {{ ref('dim_snowflake_monitoring_warehouse') }} 
    ),

    dim_snowflake_monitoring_database AS (
        SELECT *
        FROM {{ ref('dim_snowflake_monitoring_database') }}
    ),

    dim_dates AS (
        SELECT *
        FROM {{ ref('dim_snowflake_monitoring_date') }}
    ),

    dim_snowflake_monitoring_schema AS (
        SELECT *
        FROM {{ ref('dim_snowflake_monitoring_schema') }}
    ),

    dim_snowflake_monitoring_modelo AS (
        SELECT *
        FROM {{ ref('dim_snowflake_monitoring_models') }}
    ),

    fact_table AS (
        SELECT *  
        FROM {{ ref('stg_warehouse_metering_history') }}
    )

-- Junção e transformação
, joined_table AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['fact_table.warehouse_id']) }} AS metrics_sk
        ,fact_table.warehouse_id
        ,dim_snowflake_monitoring_usuario.usuario_sk AS usuario_sk
        ,dim_snowflake_monitoring_warehouse.warehouse_sk AS warehouse_sk
        ,dim_snowflake_monitoring_database.database_sk AS database_sk
        ,dim_snowflake_monitoring_schema.schema_sk AS schema_sk
        ,dim_dates.metric_date
        ,fact_table.credits_used
        ,fact_table.credits_used_compute
        ,fact_table.credits_used_cloud_services
    FROM fact_table
)

-- Consulta final
SELECT *
FROM joined_table;
