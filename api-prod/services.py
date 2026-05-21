import datetime
import httpx
from fastapi import HTTPException, status
from exception import validate_date_format, validate_time_format

async def trigger_airflow_dag(date_val: str, time_val: str | None = None) -> dict:
    """Validates inputs and triggers the Airflow DAG."""
    validate_date_format(date_val)
    if time_val is not None:
        validate_time_format(time_val)

    now_utc = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    url = f"{AIRFLOW_URL}/dags/{DAG_ID_FLIGHT}/dagRuns"
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json"
    }
    
    conf = {"date_val": date_val}
    if time_val is not None:
        conf["time_val"] = time_val

    dag_run_data = {
        "logical_date": now_utc,
        "conf": conf
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