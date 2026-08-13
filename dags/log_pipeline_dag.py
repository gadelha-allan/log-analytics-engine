from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'engenheiro_de_dados',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'log_analytics_pipeline',
    default_args=default_args,
    description='Orquestracao diaria do pipeline ETL de logs com Polars',
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'polars', 'logs'],
) as dag:

    run_pipeline = BashOperator(
        task_id='run_polars_etl',
        bash_command='python3 /opt/airflow/src/main.py',
    )

