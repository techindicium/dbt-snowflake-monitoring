{{ config(materialized='incremental') }}

select
    query_id
    , query_text
    , database_id
    , database_name
    , schema_id
    , schema_name
    , query_type
    , session_id
    , user_name
    , role_name
    , warehouse_id
    , warehouse_name
    , warehouse_size
    , warehouse_type
    , cluster_number
    , query_tag
    , execution_status
    , error_code
    , error_message
    , start_time
    , end_time
    , total_elapsed_time
    , bytes_scanned
    , percentage_scanned_from_cache
    , bytes_written
    , bytes_written_to_result
    , bytes_read_from_result
    , rows_produced
    , rows_inserted
    , rows_updated
    , rows_deleted
    , rows_unloaded
    , bytes_deleted
    , partitions_scanned
    , partitions_total
    , bytes_spilled_to_local_storage
    , bytes_spilled_to_remote_storage
    , bytes_sent_over_the_network
    , compilation_time
    , execution_time
    , queued_provisioning_time
    , queued_repair_time
    , queued_overload_time
    , transaction_blocked_time
    , outbound_data_transfer_cloud
    , outbound_data_transfer_region
    , outbound_data_transfer_bytes
    , inbound_data_transfer_cloud
    , inbound_data_transfer_region
    , inbound_data_transfer_bytes
    , list_external_files_time
    , credits_used_cloud_services
    , release_version
    , external_function_total_invocations
    , external_function_total_sent_rows
    , external_function_total_received_rows
    , external_function_total_sent_bytes
    , external_function_total_received_bytes
    , query_load_percent
    , is_client_generated_statement
    , query_acceleration_bytes_scanned
    , query_acceleration_partitions_scanned
    , query_acceleration_upper_limit_scale_factor
    , query_hash
    , query_hash_version
    , query_parameterized_hash
    , query_parameterized_hash_version
    -- ,

    --     -- this removes comments enclosed by /* <comment text> */ and single line comments starting with -- and either ending with a new line or end of string
    --     regexp_replace(query_text, $$(\/\*(.|\n|\r)*?\*\/)|(--.*$)|(--.*(\n|\r))|;$$, '') as query_text_no_comments,
    --     try_parse_json(regexp_substr(query_text, $$\/\*\s*({(.|\n|\r)*"app":\s"dbt"(.|\n|\r)*})\s*\*\/$$, 1, 1, 'ie')) as _dbt_json_comment_meta
    --     , case
    --         when try_parse_json(query_tag)['dbt_snowflake_query_tags_version'] is not null then try_parse_json(query_tag)
    --     end as _dbt_json_query_tag_meta,
    --     case
    --         when _dbt_json_comment_meta is not null or _dbt_json_query_tag_meta is not null then
    --             {{ adapter.quote_as_configured(this.database, 'database') }}.{{ adapter.quote_as_configured(this.schema, 'schema') }}.merge_objects(coalesce(_dbt_json_comment_meta, { }), coalesce(_dbt_json_query_tag_meta, { }))
    --     end as dbt_metadata
    -- ,dbt_metadata['node_id']::string as dbt_node_id    
from {{ source('snowflake_account_usage', 'query_history') }}

{% if is_incremental() %}
    -- must use end time in case query hasn't completed
    where end_time > (select max(end_time) from {{ this }})
{% endif %}

order by start_time