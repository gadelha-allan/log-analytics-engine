{{ config(materialized='view') }}

-- This model is the dbt contract over the Polars-managed Silver lake.
-- Parsing and data-quality enforcement remain in src/processor.py.
select
    ip as client_ip,
    date as raw_request_timestamp,
    method as http_method,
    endpoint,
    cast(status as integer) as http_status_code,
    cast(size as bigint) as response_size_bytes,
    cast(dt_partition as date) as request_date,
    cast(is_error as boolean) as is_error
from {{ source('silver', 'logs') }}
