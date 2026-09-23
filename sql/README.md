# Analytical SQL catalog

These DuckDB queries run directly against the partitioned Parquet lake. The Python runner replaces `{{lake_path}}` with the configured glob before execution.

| File | Analysis | Main SQL features |
|---|---|---|
| `01_daily_traffic.sql` | Daily requests and errors | CTE, conditional aggregation |
| `02_top_endpoints_by_period.sql` | Top endpoints in the latest seven-day period | CTEs, date filtering |
| `03_daily_change.sql` | Day-over-day request and error changes | `LAG` |
| `04_rolling_average.sql` | Seven-day moving averages | framed window |
| `05_response_size_percentiles.sql` | Response-size distribution | `quantile_cont` |
| `06_daily_endpoint_row_number.sql` | Daily endpoint leaders | `ROW_NUMBER` |
| `07_status_rank.sql` | Status popularity within class | `RANK` |
| `08_endpoint_lead.sql` | Each endpoint's next observed day | `LEAD` |
| `09_response_size_outliers.sql` | Large responses relative to endpoint p95 | CTE join |
| `10_method_endpoint_share.sql` | Endpoint share within HTTP method | partitioned window |

Run all queries with `python -m src.query_lake`, or select a query by filename stem with `--query`. Query 02 derives a concrete seven-day reporting period from the newest lake partition; edit the `period` CTE when fixed calendar bounds are required.
