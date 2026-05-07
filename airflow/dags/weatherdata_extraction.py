import pendulum
from airflow.sdk import dag, task
from datetime import datetime, timedelta
from get_datas import GetDatas

@dag(
    dag_id='run_weather_extraction',
    description='A simple Extract pipeline',
    schedule='0 1 * * *',
    start_date=pendulum.datetime(2026, 4, 11, tz=pendulum.timezone("Europe/Paris")),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    tags=['elt', 'extract'],
    doc_md="""
    Extract weather everyday
    """
)

def extractor():
    
    @task(task_id="extract_weather_archive")
    def run_extraction(ds):
        extractor = GetDatas(ds).get_archive_weather()
        return extractor

    run_extraction()

extractor()