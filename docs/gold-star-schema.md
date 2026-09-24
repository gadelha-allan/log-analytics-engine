# Gold star schema

The dbt Gold layer turns validated Silver logs into a dimensional model for dashboards and downstream analysis. `fact_requests` has one row for every validated HTTP request; `dim_date` and `dim_endpoint` provide conformed descriptive context.

```mermaid
erDiagram
    DIM_DATE ||--o{ FACT_REQUESTS : "request_date"
    DIM_ENDPOINT ||--o{ FACT_REQUESTS : "endpoint_key"
    FACT_REQUESTS ||--o{ MART_DAILY_TRAFFIC : "aggregates by request_date"
    FACT_REQUESTS ||--o{ MART_ENDPOINT_HEALTH : "aggregates by endpoint_key"

    DIM_DATE {
        date date_day PK
        int calendar_year
        boolean is_weekend
    }
    DIM_ENDPOINT {
        string endpoint_key PK
        string endpoint
        string endpoint_path
    }
    FACT_REQUESTS {
        bigint request_key PK
        string endpoint_key FK
        date request_date FK
        string http_method
        int http_status_code
        bigint response_size_bytes
        boolean is_error
    }
```

## Models and grain

| Model | Grain | Purpose |
|---|---|---|
| `gold.fact_requests` | One validated HTTP request | Atomic facts and dimension foreign keys. |
| `gold.dim_date` | One calendar day | Date attributes for time-series analysis. |
| `gold.dim_endpoint` | One observed endpoint | Endpoint route attributes and a deterministic key. |
| `gold.mart_daily_traffic` | One request date | Daily traffic, errors, response size, and unique-client metrics. |
| `gold.mart_endpoint_health` | One endpoint | Endpoint volume, error rate, response size, and active-date metrics. |

## Build and documentation

After generating the Silver lake and creating `dbt/profiles.yml`, run:

```bash
dbt build --project-dir dbt --profiles-dir dbt
dbt docs generate --project-dir dbt --profiles-dir dbt
dbt docs serve --project-dir dbt --profiles-dir dbt
```

`dbt build` materializes the five Gold models and executes their data-quality tests. `dbt docs generate` writes a catalog and lineage manifest to `dbt/target/`.
