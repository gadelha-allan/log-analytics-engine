FROM apache/airflow:2.9.1-python3.9


COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ /opt/airflow/src/
