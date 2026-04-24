import pendulum
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from XGBoost_GridSearch import XGBGridSearch


def data_training():
    XGBGridSearch().training_model()

default_args = {
    'owner': 'dev_team',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=19),
}


with DAG(
    dag_id='training_model',
    default_args=default_args,
    description='Launch training model',
    schedule="0 2 * * 0", 
    start_date=pendulum.datetime(2024, 4, 20, tz=pendulum.timezone("Europe/Paris")),
    catchup=False,
    max_active_runs=1,
    tags=['model', 'training']
) as dag:
    
    launch_training = PythonOperator(
        task_id="launch_training",
        python_callable=data_training,
        doc_md="""training the model""",
    )

    archeving_files = BashOperator(
        task_id="archeving_files",
        bash_command="mv /opt/airflow/output/*.parquet /opt/airflow/archives",
        doc_md="""Archeving old flight and weather files""",
    )

    launch_training >> archeving_files