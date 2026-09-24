{{ config(materialized='table') }}

select
    endpoints.endpoint_key,
    endpoints.endpoint,
    endpoints.endpoint_path,
    count(requests.request_key) as total_requests,
    count(*) filter (where requests.is_error) as total_errors,
    round(
        100.0 * count(*) filter (where requests.is_error) / nullif(count(requests.request_key), 0),
        2
    ) as error_rate_pct,
    round(avg(requests.response_size_bytes), 2) as avg_response_size_bytes,
    max(requests.response_size_bytes) as max_response_size_bytes,
    count(distinct requests.client_ip) as unique_clients,
    min(requests.request_date) as first_request_date,
    max(requests.request_date) as last_request_date
from {{ ref('dim_endpoint') }} as endpoints
left join {{ ref('fact_requests') }} as requests
    on endpoints.endpoint_key = requests.endpoint_key
group by
    endpoints.endpoint_key,
    endpoints.endpoint,
    endpoints.endpoint_path
