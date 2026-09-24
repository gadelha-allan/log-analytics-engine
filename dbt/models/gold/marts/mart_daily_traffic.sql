{{ config(materialized='table') }}

select
    request_date,
    count(*) as total_requests,
    count(*) filter (where is_error) as total_errors,
    round(100.0 * count(*) filter (where is_error) / nullif(count(*), 0), 2) as error_rate_pct,
    round(avg(response_size_bytes), 2) as avg_response_size_bytes,
    count(distinct client_ip) as unique_clients,
    count(distinct endpoint_key) as unique_endpoints
from {{ ref('fact_requests') }}
group by request_date
