# DuckDB SQL performance and execution plans

This guide makes the analytical layer measurable and reproducible. Run the pipeline first so the examples can inspect real Parquet partitions:

```bash
python -m src.main --generate --lines 1000000
```

## Reading a plan

- `EXPLAIN` prints the physical plan without running the query. Use it to inspect scans, filters, joins, aggregates, and windows.
- `EXPLAIN ANALYZE` executes the query and adds observed row counts and operator timings. Results depend on volume, DuckDB version, filesystem cache, and hardware.

Start DuckDB from the repository root and enable detailed plans with `PRAGMA explain_output = 'all';`.

## Example 1: partition pruning

```sql
EXPLAIN ANALYZE
SELECT endpoint, COUNT(*) AS requests
FROM read_parquet(
    'data/processed/logs_lake/**/*.parquet',
    hive_partitioning = true
)
WHERE dt_partition BETWEEN DATE '2026-07-01' AND DATE '2026-07-07'
GROUP BY endpoint
ORDER BY requests DESC
LIMIT 20;
```

Expected operator shape:

```text
TOP_N
  HASH_GROUP_BY
    READ_PARQUET
      File Filters: dt_partition BETWEEN ...
      Scanning Files: selected/total
```

The evidence to check is a file filter and a scanned-file count below the total partition count. A direct predicate on `dt_partition` lets DuckDB discard files before reading row groups. Wrapping the partition column in a function can prevent pruning.

## Example 2: rolling requests and errors

Query [`sql/04_rolling_average.sql`](../sql/04_rolling_average.sql) reduces raw rows to one row per day before applying its window:

```sql
EXPLAIN ANALYZE
WITH daily AS (
    SELECT
        dt_partition AS request_date,
        COUNT(*) AS requests,
        COUNT(*) FILTER (WHERE is_error) AS errors
    FROM read_parquet(
        'data/processed/logs_lake/**/*.parquet',
        hive_partitioning = true
    )
    GROUP BY dt_partition
)
SELECT
    request_date,
    AVG(requests) OVER (
        ORDER BY request_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS requests_7d_avg,
    AVG(errors) OVER (
        ORDER BY request_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS errors_7d_avg
FROM daily;
```

Expected operator shape:

```text
PROJECTION
  WINDOW
    HASH_GROUP_BY
      READ_PARQUET
```

The aggregation should be below `WINDOW`, keeping window state proportional to dates rather than log records. Compare rows leaving `READ_PARQUET` and `HASH_GROUP_BY` to confirm this reduction.

## Example 3: response-size percentiles

```sql
EXPLAIN ANALYZE
SELECT
    endpoint,
    quantile_cont(size, 0.50) AS p50_bytes,
    quantile_cont(size, 0.95) AS p95_bytes,
    quantile_cont(size, 0.99) AS p99_bytes
FROM read_parquet(
    'data/processed/logs_lake/**/*.parquet',
    hive_partitioning = true
)
GROUP BY endpoint;
```

Expected operator shape:

```text
HASH_GROUP_BY
  PROJECTION (endpoint, size)
    READ_PARQUET
```

The scan should project only `endpoint` and `size`, avoiding decompression of unrelated columns. Exact continuous percentiles cost more than simple aggregates; benchmark approximate quantiles at very large scale if bounded approximation is acceptable.

## Benchmark discipline

1. Record DuckDB version, row count, Parquet size, hardware, and cache state.
2. Run each query several times and report median wall time.
3. Keep result materialization consistent; large dataframe output can dominate runtime.
4. Compare operator cardinalities before timings. Dataset or predicate changes invalidate comparisons.
5. Use fixed date bounds when comparing partition pruning across runs.

Plans describe one dataset and DuckDB release, not permanent guarantees. Re-run `EXPLAIN ANALYZE` after changes to queries, schemas, partitioning, or engine versions.
