-- Compare daily request and error counts with the previous observed day.
WITH daily AS (
    SELECT
        dt_partition AS request_date,
        COUNT(*) AS requests,
        COUNT(*) FILTER (WHERE is_error) AS errors
    FROM read_parquet('{{lake_path}}', hive_partitioning = true)
    GROUP BY dt_partition
), previous_day AS (
    SELECT
        *,
        LAG(requests) OVER (ORDER BY request_date) AS previous_requests,
        LAG(errors) OVER (ORDER BY request_date) AS previous_errors
    FROM daily
)
SELECT
    request_date,
    requests,
    requests - previous_requests AS request_change,
    ROUND(100.0 * (requests - previous_requests) / NULLIF(previous_requests, 0), 2)
        AS request_change_pct,
    errors,
    errors - previous_errors AS error_change
FROM previous_day
ORDER BY request_date;
