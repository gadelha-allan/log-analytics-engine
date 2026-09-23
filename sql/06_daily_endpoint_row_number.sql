-- The five busiest endpoints for each day.
WITH daily_endpoint AS (
    SELECT
        dt_partition AS request_date,
        endpoint,
        COUNT(*) AS requests
    FROM read_parquet('{{lake_path}}', hive_partitioning = true)
    GROUP BY dt_partition, endpoint
), ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY request_date ORDER BY requests DESC, endpoint
        ) AS endpoint_position
    FROM daily_endpoint
)
SELECT request_date, endpoint_position, endpoint, requests
FROM ranked
WHERE endpoint_position <= 5
ORDER BY request_date, endpoint_position;
