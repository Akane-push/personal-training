from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from get_datas import GetDatas

def run_extraction():
    GetDatas(datetime.now().strftime("%Y-%m-%d")).get_flights(datetime.now().strftime("%H:00"))

default_args = {
    'owner': 'dev_team',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='run_flight_extraction',
    default_args=default_args,
    description='A simple ETL pipeline',
    schedule='0 */3 * * *',
    start_date=datetime(2026, 4, 10),
    catchup=False,
    max_active_runs=1,
    tags=['etl']
) as dag:
    extraction_flight = PythonOperator(
        task_id="extract_flight",
        python_callable=run_extraction,
        doc_md="""Extract flight eatch 3 hours""",
    )