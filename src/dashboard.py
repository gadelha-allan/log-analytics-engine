from __future__ import annotations

import contextlib
import os

import duckdb
import plotly.express as px
import streamlit as st

LAKE_PATH = "data/processed/logs_lake/**/*.parquet"
QUARANTINE_PATH = "data/processed/quarantine/*.parquet"
GOLD_DATABASE_PATH = "data/log_analytics.duckdb"

st.set_page_config(
    page_title="Log Analytics Engine",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("Log Analytics Engine - Executive Dashboard")
st.markdown("Monitoramento do Data Lake com metricas de qualidade.")

lake_exists = os.path.exists("data/processed/logs_lake")
quarantine_exists = os.path.exists("data/processed/quarantine")

if not lake_exists:
    st.error("Data Lake nao gerado. Execute python -m src.main --generate primeiro.")
    st.stop()

con = duckdb.connect()
gold_available = False
if os.path.exists(GOLD_DATABASE_PATH):
    with contextlib.suppress(duckdb.Error):
        gold_connection = duckdb.connect(GOLD_DATABASE_PATH, read_only=True)
        gold_available = (
            gold_connection.execute(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'gold'
                  AND table_name = 'mart_daily_traffic'
                """
            ).fetchone()
            is not None
        )
        if gold_available:
            con.close()
            con = gold_connection
        else:
            gold_connection.close()

if gold_available:
    kpis = con.execute(
        """
        SELECT
            COALESCE(SUM(total_requests), 0) AS total_logs,
            COALESCE(SUM(total_errors), 0) AS total_erros,
            COALESCE(
                ROUND(
                    SUM(avg_response_size_bytes * total_requests)
                    / NULLIF(SUM(total_requests), 0),
                    2
                ),
                0.0
            ) AS tamanho_medio,
            (SELECT COUNT(*) FROM gold.dim_endpoint) AS endpoints_unicos,
            (SELECT COUNT(DISTINCT client_ip) FROM gold.fact_requests) AS ips_unicos
        FROM gold.mart_daily_traffic
        """
    ).fetchone()
else:
    kpis = con.execute(
        f"""
        SELECT
            COUNT(*) AS total_logs,
            COALESCE(SUM(CASE WHEN is_error THEN 1 ELSE 0 END), 0) AS total_erros,
            COALESCE(ROUND(AVG(size), 2), 0.0) AS tamanho_medio,
            COUNT(DISTINCT endpoint) AS endpoints_unicos,
            COUNT(DISTINCT ip) AS ips_unicos
        FROM '{LAKE_PATH}'
        """
    ).fetchone()

if kpis is None:
    st.error("Nao foi possivel obter os KPIs do Data Lake.")
    st.stop()

total_logs, total_erros, tamanho_medio, endpoints_unicos, ips_unicos = kpis
taxa_erro = round((total_erros / total_logs) * 100, 2) if total_logs else 0.0

quarantine_count = 0
if quarantine_exists:
    with contextlib.suppress(Exception):
        quarantine_row = con.execute(
            f"""
            SELECT COUNT(*) FROM '{QUARANTINE_PATH}'
        """
        ).fetchone()
        if quarantine_row is not None:
            quarantine_count = quarantine_row[0]

total_input = total_logs + quarantine_count
qualidade_score = round((total_logs / total_input) * 100, 2) if total_input else 100.0

st.subheader("KPIs Globais")
col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Total de Logs", f"{total_logs:,}")
col2.metric("Volume de Erros", f"{total_erros:,}")
col3.metric("Taxa de Erro HTTP", f"{taxa_erro}%")
col4.metric("Tamanho Medio", f"{tamanho_medio:,.0f} B")
col5.metric("Score de Qualidade", f"{qualidade_score}%")

st.divider()

col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Top 10 Endpoints")
    if gold_available:
        df_endpoints = con.execute(
            """
            SELECT endpoint, total_requests AS requisicoes
            FROM gold.mart_endpoint_health
            ORDER BY requisicoes DESC
            LIMIT 10
            """
        ).df()
    else:
        df_endpoints = con.execute(
            f"""
            SELECT endpoint, COUNT(*) AS requisicoes
            FROM '{LAKE_PATH}'
            GROUP BY endpoint
            ORDER BY requisicoes DESC
            LIMIT 10
            """
        ).df()

    fig = px.bar(
        df_endpoints,
        x="requisicoes",
        y="endpoint",
        orientation="h",
        color="requisicoes",
        color_continuous_scale="Viridis",
        labels={"requisicoes": "Requisicoes", "endpoint": "Endpoint"},
    )
    fig.update_layout(yaxis={"categoryorder": "total ascending"}, height=400)
    st.plotly_chart(fig, use_container_width=True)

with col_right:
    st.subheader("Distribuicao de Status HTTP")
    if gold_available:
        df_status = con.execute(
            """
            SELECT CAST(http_status_code AS VARCHAR) AS status_code, COUNT(*) AS total
            FROM gold.fact_requests
            GROUP BY http_status_code
            ORDER BY total DESC
            """
        ).df()
    else:
        df_status = con.execute(
            f"""
            SELECT CAST(status AS VARCHAR) AS status_code, COUNT(*) AS total
            FROM '{LAKE_PATH}'
            GROUP BY status
            ORDER BY total DESC
            """
        ).df()

    fig = px.pie(
        df_status,
        names="status_code",
        values="total",
        hole=0.4,
        color_discrete_sequence=px.colors.qualitative.Pastel,
    )
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)

st.subheader("Volume de Requisicoes ao Longo do Tempo")
if gold_available:
    df_time = con.execute(
        """
        SELECT
            request_date AS data,
            total_requests AS volume,
            total_errors AS erros
        FROM gold.mart_daily_traffic
        ORDER BY request_date
        """
    ).df()
else:
    df_time = con.execute(
        f"""
        SELECT
            dt_partition AS data,
            COUNT(*) AS volume,
            SUM(CASE WHEN is_error THEN 1 ELSE 0 END) AS erros
        FROM '{LAKE_PATH}'
        GROUP BY dt_partition
        ORDER BY dt_partition
        """
    ).df()

fig_time = px.line(
    df_time,
    x="data",
    y=["volume", "erros"],
    labels={"value": "Quantidade", "variable": "Metrica", "data": "Data"},
    markers=True,
)
fig_time.update_layout(height=350)
st.plotly_chart(fig_time, use_container_width=True)

if quarantine_exists and quarantine_count > 0:
    st.divider()
    st.subheader("Quarentena de Qualidade")
    st.warning(f"{quarantine_count:,} registros foram rejeitados.")

    df_quarantine = con.execute(
        f"""
        SELECT
            rejection_reason AS motivo,
            COUNT(*) AS quantidade,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentual
        FROM '{QUARANTINE_PATH}'
        GROUP BY rejection_reason
        ORDER BY quantidade DESC
    """
    ).df()

    st.dataframe(df_quarantine, use_container_width=True, hide_index=True)

layer_label = "Gold dbt marts" if gold_available else "Silver Parquet fallback"
st.caption(f"Log Analytics Engine v1.2.0 | {layer_label}")
