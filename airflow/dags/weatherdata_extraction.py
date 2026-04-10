from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from get_datas import GetDatas

def run_extraction(ds):
    GetDatas(ds).get_weather()

default_args = {
    'owner': 'dev_team',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='run_extraction',
    default_args=default_args,
    description='A simple ETL pipeline',
    schedule='0 1 * * *',
    start_date=datetime(2026, 4, 11),
    catchup=False,
    max_active_runs=1,
    tags=['etl']
) as dag:
    extraction_flight = PythonOperator(
        task_id="extract_weather",
        python_callable=run_extraction,
        doc_md="""Extract weather everyday""",
    )