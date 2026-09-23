-- Daily request volume, error volume, error rate, and average response size.
WITH daily AS (
    SELECT
        dt_partition AS request_date,
        COUNT(*) AS requests,
        COUNT(*) FILTER (WHERE is_error) AS errors,
        AVG(size) AS avg_response_bytes
    FROM read_parquet('{{lake_path}}', hive_partitioning = true)
    GROUP BY dt_partition
)
SELECT
    request_date,
    requests,
    errors,
    ROUND(100.0 * errors / NULLIF(requests, 0), 2) AS error_rate_pct,
    ROUND(avg_response_bytes, 2) AS avg_response_bytes
FROM daily
ORDER BY request_date;
