from airflow.sdk import dag, task
from datetime import datetime, timedelta
from get_datas import GetDatas

@dag(
    dag_id='ad_hoc_weather',
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
    Extract weather one time, change date before in script
    """
)

def Ad_Hoc():
    
    @task
    def extract_task():
        extractor = GetDatas("2026-04-30").get_archive_weather()
        return extractor

    extract_task()

Ad_Hoc()