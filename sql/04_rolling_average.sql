-- Seven-observation rolling averages for requests, errors, and error rate.
WITH daily AS (
    SELECT
        dt_partition AS request_date,
        COUNT(*) AS requests,
        COUNT(*) FILTER (WHERE is_error) AS errors
    FROM read_parquet('{{lake_path}}', hive_partitioning = true)
    GROUP BY dt_partition
)
SELECT
    request_date,
    requests,
    errors,
    ROUND(AVG(requests) OVER seven_days, 2) AS requests_7d_avg,
    ROUND(AVG(errors) OVER seven_days, 2) AS errors_7d_avg,
    ROUND(100.0 * SUM(errors) OVER seven_days / NULLIF(SUM(requests) OVER seven_days, 0), 2)
        AS error_rate_7d_pct
FROM daily
WINDOW seven_days AS (
    ORDER BY request_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
)
ORDER BY request_date;
