-- Tabelas de dimensão
with 
    dim_query_history as (
        select *
        from {{ ref('stg_query_history') }}
    ),
    dim_metering_history as (
        select *
        from {{ ref('stg_metering_history') }} 
    ),
    dim_daily_rates as (
        select *
        from {{ ref('daily_rates') }}
    ),

-- Tabela de fatos
fact_table as (
    select 
        {{ dbt_utils.generate_surrogate_key(['fact_table.warehouse_id']) }} AS metrics_sk,
        *
    from {{ ref('stg_warehouse_metering_history') }}
),

-- Cálculo de custo de serviços em nuvem por consulta
cloud_services_cost_per_query as (
    select
        q.query_id,
        sum(mh.credits_used_cloud_services) as total_credits_used_cloud_services,
        coalesce(dr.effective_rate, cr.effective_rate) as rate,
        sum(mh.credits_used_cloud_services) * coalesce(dr.effective_rate, cr.effective_rate) as cloud_services_cost
    from
        dim_query_history q
    join
        dim_metering_history mh on q.query_id = mh.query_id
    left join
        dim_daily_rates dr on date(q.start_time) = dr.date and dr.service_type = 'COMPUTE' and dr.usage_type = 'cloud services'
    cross join
        (select effective_rate from dim_daily_rates where is_latest_rate = true and service_type = 'COMPUTE' and usage_type = 'cloud services') cr
    group by
        q.query_id, cr.effective_rate, dr.effective_rate
),

-- Cálculo de custo de computação por consulta
compute_cost_per_query as (
    select
        q.query_id,
        sum(mh.credits_used_compute) as total_credits_used_compute,
        coalesce(dr.effective_rate, cr.effective_rate) as rate,
        sum(mh.credits_used_compute) * coalesce(dr.effective_rate, cr.effective_rate) as compute_cost
    from
        dim_query_history q
    join
        dim_metering_history mh on q.query_id = mh.query_id
    left join
        dim_daily_rates dr on date(q.start_time) = dr.date and dr.service_type = 'COMPUTE' and dr.usage_type = 'compute'
    cross join
        (select effective_rate from dim_daily_rates where is_latest_rate = true and service_type = 'COMPUTE' and usage_type = 'compute') cr
    group by
        q.query_id, cr.effective_rate, dr.effective_rate
),

-- Consulta final
final_query as (
    select
        q.query_id,
        q.start_time,
        q.end_time,
        q.execution_start_time,
        q.compute_cost,
        q.compute_credits,
        q.query_acceleration_cost,
        q.query_acceleration_credits,
        q.credits_used_cloud_services,
        q.ran_on_warehouse,
        coalesce(cloud.cloud_services_cost, 0) as cloud_services_cost,
        coalesce(compute.compute_cost, 0) as compute_cost
    from
        dim_query_history q
    left join
        cloud_services_cost_per_query cloud on q.query_id = cloud.query_id
    left join
        compute_cost_per_query compute on q.query_id = compute.query_id
)

select * from final_query;
