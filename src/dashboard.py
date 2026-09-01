from __future__ import annotations

import contextlib
import os

import duckdb
import plotly.express as px
import streamlit as st

LAKE_PATH = "data/processed/logs_lake/**/*.parquet"
QUARANTINE_PATH = "data/processed/quarantine/*.parquet"

st.set_page_config(
    page_title="Log Analytics Engine",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("Log Analytics Engine - Executive Dashboard")
st.markdown("Monitoramento do Data Lake com metricas de qualidade.")

lake_exists = os.path.exists('data/processed/logs_lake')
quarantine_exists = os.path.exists('data/processed/quarantine')

if not lake_exists:
    st.error("Data Lake nao gerado. Execute python -m src.main --generate primeiro.")
    st.stop()

con = duckdb.connect()

kpis = con.execute(f"""
    SELECT
        COUNT(*) AS total_logs,
        COALESCE(SUM(CASE WHEN is_error THEN 1 ELSE 0 END), 0) AS total_erros,
        COALESCE(ROUND(AVG(size), 2), 0.0) AS tamanho_medio,
        COUNT(DISTINCT endpoint) AS endpoints_unicos,
        COUNT(DISTINCT ip) AS ips_unicos
    FROM '{LAKE_PATH}'
""").fetchone()

total_logs, total_erros, tamanho_medio, endpoints_unicos, ips_unicos = kpis
taxa_erro = round((total_erros / total_logs) * 100, 2) if total_logs else 0.0

quarantine_count = 0
if quarantine_exists:
    with contextlib.suppress(Exception):
        quarantine_count = con.execute(f"""
            SELECT COUNT(*) FROM '{QUARANTINE_PATH}'
        """).fetchone()[0]

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
    df_endpoints = con.execute(f"""
        SELECT endpoint, COUNT(*) AS requisicoes
        FROM '{LAKE_PATH}'
        GROUP BY endpoint
        ORDER BY requisicoes DESC
        LIMIT 10
    """).df()

    fig = px.bar(
        df_endpoints,
        x="requisicoes", y="endpoint", orientation="h",
        color="requisicoes", color_continuous_scale="Viridis",
        labels={"requisicoes": "Requisicoes", "endpoint": "Endpoint"},
    )
    fig.update_layout(yaxis={"categoryorder": "total ascending"}, height=400)
    st.plotly_chart(fig, use_container_width=True)

with col_right:
    st.subheader("Distribuicao de Status HTTP")
    df_status = con.execute(f"""
        SELECT CAST(status AS VARCHAR) AS status_code, COUNT(*) AS total
        FROM '{LAKE_PATH}'
        GROUP BY status
        ORDER BY total DESC
    """).df()

    fig = px.pie(
        df_status,
        names="status_code", values="total", hole=0.4,
        color_discrete_sequence=px.colors.qualitative.Pastel,
    )
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)

st.subheader("Volume de Requisicoes ao Longo do Tempo")
df_time = con.execute(f"""
    SELECT
        dt_partition AS data,
        COUNT(*) AS volume,
        SUM(CASE WHEN is_error THEN 1 ELSE 0 END) AS erros
    FROM '{LAKE_PATH}'
    GROUP BY dt_partition
    ORDER BY dt_partition
""").df()

fig_time = px.line(
    df_time,
    x="data", y=["volume", "erros"],
    labels={"value": "Quantidade", "variable": "Metrica", "data": "Data"},
    markers=True,
)
fig_time.update_layout(height=350)
st.plotly_chart(fig_time, use_container_width=True)

if quarantine_exists and quarantine_count > 0:
    st.divider()
    st.subheader("Quarentena de Qualidade")
    st.warning(f"{quarantine_count:,} registros foram rejeitados.")

    df_quarantine = con.execute(f"""
        SELECT
            rejection_reason AS motivo,
            COUNT(*) AS quantidade,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentual
        FROM '{QUARANTINE_PATH}'
        GROUP BY rejection_reason
        ORDER BY quantidade DESC
    """).df()

    st.dataframe(df_quarantine, use_container_width=True, hide_index=True)

st.caption("Log Analytics Engine v1.0.0 | Polars + DuckDB + Parquet")
