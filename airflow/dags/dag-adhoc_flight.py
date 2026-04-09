from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from get_datas import GetDatas

def Extract_Ad_Hoc():
    GetDatas("2026-04-05").get_flights("19:00")

default_args = {
    'owner': 'dev_team',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='ad_hoc',
    default_args=default_args,
    description='A simple ETL pipeline',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=['test']
) as dag:
    ad_hoc_extract_flight = PythonOperator(
        task_id="extract_flight",
        python_callable=Extract_Ad_Hoc,
        doc_md="""Extract flight one time, change date before in script""",
    )