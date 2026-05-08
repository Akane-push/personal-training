from airflow.sdk import dag, task
from datetime import datetime, timedelta
from src.extract_data.get_datas import GetDatas

@dag(
    dag_id='ad_hoc_flight',
    description='A simple extract',
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    tags=['test', 'extract'],
    doc_md="""
    Extract flight one time, change date before in script
    """
)

def Ad_Hoc():
    
    @task
    def extract_task():
        extractor = GetDatas("2026-05-05").get_flights("15:00")
        return extractor

    extract_task()

Ad_Hoc()