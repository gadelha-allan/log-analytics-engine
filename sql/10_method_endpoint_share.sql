-- Endpoint traffic share within each HTTP method.
WITH method_endpoint AS (
    SELECT method, endpoint, COUNT(*) AS requests
    FROM read_parquet('{{lake_path}}', hive_partitioning = true)
    GROUP BY method, endpoint
), shares AS (
    SELECT
        *,
        SUM(requests) OVER (PARTITION BY method) AS method_requests,
        ROW_NUMBER() OVER (PARTITION BY method ORDER BY requests DESC, endpoint) AS position
    FROM method_endpoint
)
SELECT
    method,
    position,
    endpoint,
    requests,
    method_requests,
    ROUND(100.0 * requests / NULLIF(method_requests, 0), 2) AS method_share_pct
FROM shares
WHERE position <= 10
ORDER BY method, position;
