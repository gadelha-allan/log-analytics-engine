import streamlit as st
import duckdb
import plotly.express as px
import os

st.set_page_config(
    page_title="Log Analytics Engine",
    page_icon="",
    layout="wide"
)

st.title("Log Analytics Engine - Executive Dashboard")
st.markdown("Visualização interativa e métricas em tempo real sobre o Data Lake Parquet.")

lake_path = "data/processed/logs_lake/**/*.parquet"

if not os.path.exists("data/processed/logs_lake"):
    st.error("O Data Lake ainda não foi gerado. Execute 'python src/main.py' primeiro para processar os logs.")
    st.stop()

con = duckdb.connect()

kpis = con.execute(f"""
    SELECT 
        COUNT(*) as total_logs,
        COALESCE(SUM(CASE WHEN is_error THEN 1 ELSE 0 END), 0) as total_erros,
        COALESCE(ROUND(AVG(size), 2), 0.0) as tamanho_medio
    FROM '{lake_path}'
""").fetchone()

total_logs, total_erros, tamanho_medio = kpis
taxa_erro = round((total_erros / total_logs) * 100, 2) if total_logs else 0.0

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total de Logs Processados", f"{total_logs:,}")
col2.metric("Volume de Erros", f"{total_erros:,}")
col3.metric("Taxa de Erro HTTP", f"{taxa_erro}%")
col4.metric("Tamanho Médio da Resposta", f"{tamanho_medio} Bytes")

st.divider()

col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Top 10 Endpoints Mais Solicitados")
    df_endpoints = con.execute(f"""
        SELECT endpoint, COUNT(*) as requisicoes
        FROM '{lake_path}'
        GROUP BY endpoint
        ORDER BY requisicoes DESC
        LIMIT 10
    """).df()

    fig_endpoints = px.bar(
        df_endpoints, 
        x="requisicoes", 
        y="endpoint", 
        orientation="h",
        color="requisicoes",
        color_continuous_scale="Viridis",
        labels={"requisicoes": "Volume de Requisições", "endpoint": "Rota / Endpoint"}
    )
    fig_endpoints.update_layout(yaxis={'categoryorder': 'total ascending'})
    st.plotly_chart(fig_endpoints, use_container_width=True)

with col_right:
    st.subheader("Distribuição dos Códigos de Status HTTP")
    df_status = con.execute(f"""
        SELECT CAST(status AS VARCHAR) as status_code, COUNT(*) as total
        FROM '{lake_path}'
        GROUP BY status
        ORDER BY total DESC
    """).df()

    fig_status = px.pie(
        df_status, 
        names="status_code", 
        values="total",
        hole=0.4,
        color_discrete_sequence=px.colors.qualitative.Pastel
    )
    st.plotly_chart(fig_status, use_container_width=True)
