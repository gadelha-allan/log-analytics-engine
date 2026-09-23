-- Response-size percentiles by endpoint, restricted to meaningful sample sizes.
WITH endpoint_sizes AS (
    SELECT endpoint, size
    FROM read_parquet('{{lake_path}}', hive_partitioning = true)
    WHERE size IS NOT NULL
)
SELECT
    endpoint,
    COUNT(*) AS requests,
    ROUND(quantile_cont(size, 0.50), 2) AS p50_bytes,
    ROUND(quantile_cont(size, 0.90), 2) AS p90_bytes,
    ROUND(quantile_cont(size, 0.95), 2) AS p95_bytes,
    ROUND(quantile_cont(size, 0.99), 2) AS p99_bytes,
    MAX(size) AS max_bytes
FROM endpoint_sizes
GROUP BY endpoint
HAVING COUNT(*) >= 5
ORDER BY p95_bytes DESC, endpoint;
