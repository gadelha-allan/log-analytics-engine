import duckdb
import os

def run_analytics_queries():
    lake_path = "data/processed/logs_lake/*/*.parquet"
    
    if not os.path.exists("data/processed/logs_lake"):
        return

    con = duckdb.connect()

    df_top_endpoints = con.execute(f"""
        SELECT 
            endpoint,
            COUNT(*) AS total_requisicoes,
            AVG(size)::INT AS tamanho_medio_bytes
        FROM '{lake_path}'
        GROUP BY endpoint
        ORDER BY total_requisicoes DESC
        LIMIT 5
    """).df()

    df_error_rate = con.execute(f"""
        SELECT 
            dt_partition,
            COUNT(*) AS total,
            SUM(CASE WHEN is_error THEN 1 ELSE 0 END) AS erros,
            ROUND(SUM(CASE WHEN is_error THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS taxa_erro_pct
        FROM '{lake_path}'
        GROUP BY dt_partition
        ORDER BY dt_partition ASC
    """).df()

if __name__ == "__main__":
    run_analytics_queries()
