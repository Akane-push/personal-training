import httpx
import datetime
from fastapi import FastAPI, HTTPException, Request, status
import os
from dotenv import load_dotenv

app = FastAPI()

airflow_url = os.getenv("AIRFLOW_API_URL")
token = os.environ.get("AIRFLOW_TOKEN")
dag_id_flight = "ad_hoc_flight"

@app.post("/trigger-ad_hoc-flight/{date_val}/{time_val}", status_code=status.HTTP_201_CREATED)
async def trigger_dag(date_val: str, time_val: str):
    """Triggers the DAG
    Date format YYYY-MM-DD
    Time format HH:mm
    """
    now_utc = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    url = f"{airflow_url}/dags/{dag_id_flight}/dagRuns"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    dag_run_data = {
            "logical_date": now_utc,
            "conf": {
                "date_val": date_val,
                "time_val": time_val
            }
        }

    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(url, json=dag_run_data, headers=headers, timeout=10.0)

            if response.status_code not in [200, 201]:
                raise HTTPException(
                    status_code=response.status_code,
                    detail=f"[WARNING] Airflow API error: {response.text}",
                )

            return response.json()

        except httpx.RequestError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"[WARNING] Failed to connect to Airflow: {exc}",
            )

