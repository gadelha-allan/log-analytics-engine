-- Rank HTTP statuses by frequency inside each status class.
WITH status_counts AS (
    SELECT
        CAST(FLOOR(status / 100) AS INTEGER) AS status_class,
        status,
        COUNT(*) AS responses
    FROM read_parquet('{{lake_path}}', hive_partitioning = true)
    GROUP BY status_class, status
), ranked AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY status_class ORDER BY responses DESC) AS popularity_rank
    FROM status_counts
)
SELECT status_class, popularity_rank, status, responses
FROM ranked
ORDER BY status_class, popularity_rank, status;
