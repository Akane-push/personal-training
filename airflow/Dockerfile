FROM apache/airflow:2.8.4-python3.11

USER airflow

COPY --chmod=755 --chown=airflow:0 ./airflow/install_polars.sh .
RUN ./install_polars.sh

COPY ./airflow/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt