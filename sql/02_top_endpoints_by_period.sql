-- Top endpoints in the seven-day period ending at the latest available partition.
WITH period AS (
    SELECT
        MAX(dt_partition) - INTERVAL 6 DAY AS start_date,
        MAX(dt_partition) AS end_date
    FROM read_parquet('{{lake_path}}', hive_partitioning = true)
), endpoint_totals AS (
    SELECT
        logs.endpoint,
        COUNT(*) AS requests,
        COUNT(*) FILTER (WHERE logs.is_error) AS errors,
        AVG(logs.size) AS avg_response_bytes
    FROM read_parquet('{{lake_path}}', hive_partitioning = true) AS logs
    CROSS JOIN period
    WHERE logs.dt_partition BETWEEN period.start_date AND period.end_date
    GROUP BY logs.endpoint
)
SELECT
    endpoint,
    requests,
    errors,
    ROUND(100.0 * errors / NULLIF(requests, 0), 2) AS error_rate_pct,
    ROUND(avg_response_bytes, 2) AS avg_response_bytes
FROM endpoint_totals
ORDER BY requests DESC, endpoint
LIMIT 20;
