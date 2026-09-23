-- Show the next observed date and the change in traffic for each endpoint.
WITH daily_endpoint AS (
    SELECT
        endpoint,
        dt_partition AS request_date,
        COUNT(*) AS requests
    FROM read_parquet('{{lake_path}}', hive_partitioning = true)
    GROUP BY endpoint, dt_partition
), next_observation AS (
    SELECT
        *,
        LEAD(request_date) OVER (PARTITION BY endpoint ORDER BY request_date) AS next_date,
        LEAD(requests) OVER (PARTITION BY endpoint ORDER BY request_date) AS next_requests
    FROM daily_endpoint
)
SELECT
    endpoint,
    request_date,
    requests,
    next_date,
    next_requests,
    next_requests - requests AS change_to_next_observation
FROM next_observation
ORDER BY endpoint, request_date;
