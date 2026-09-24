{{ config(materialized='table') }}

with numbered_requests as (
    select
        row_number() over (
            order by
                request_date,
                raw_request_timestamp,
                client_ip,
                http_method,
                endpoint,
                http_status_code,
                response_size_bytes
        ) as request_key,
        client_ip,
        raw_request_timestamp,
        http_method,
        endpoint,
        http_status_code,
        response_size_bytes,
        request_date,
        is_error
    from {{ ref('stg_logs') }}
)

select
    request_key,
    md5(endpoint) as endpoint_key,
    client_ip,
    raw_request_timestamp,
    http_method,
    http_status_code,
    response_size_bytes,
    request_date,
    is_error
from numbered_requests
