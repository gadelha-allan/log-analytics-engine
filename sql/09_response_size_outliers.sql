-- Requests whose response is larger than the endpoint-specific p95.
WITH thresholds AS (
    SELECT
        endpoint,
        quantile_cont(size, 0.95) AS p95_bytes
    FROM read_parquet('{{lake_path}}', hive_partitioning = true)
    GROUP BY endpoint
), large_responses AS (
    SELECT
        logs.dt_partition AS request_date,
        logs.endpoint,
        logs.status,
        logs.size,
        thresholds.p95_bytes
    FROM read_parquet('{{lake_path}}', hive_partitioning = true) AS logs
    JOIN thresholds USING (endpoint)
    WHERE logs.size > thresholds.p95_bytes
)
SELECT
    request_date,
    endpoint,
    status,
    size,
    ROUND(p95_bytes, 2) AS endpoint_p95_bytes,
    ROUND(size / NULLIF(p95_bytes, 0), 2) AS p95_multiple
FROM large_responses
ORDER BY p95_multiple DESC, size DESC
LIMIT 100;
