FROM apache/airflow:2.8.4-python3.11

USER airflow

COPY ./airflow/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt