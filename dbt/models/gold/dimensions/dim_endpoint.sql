{{ config(materialized='table') }}

select
    md5(endpoint) as endpoint_key,
    endpoint,
    split_part(endpoint, '?', 1) as endpoint_path,
    case
        when strpos(endpoint, '?') > 0 then true
        else false
    end as has_query_string
from {{ ref('stg_logs') }}
group by endpoint
