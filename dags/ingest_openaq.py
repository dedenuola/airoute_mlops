import os
import requests
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, date

def fetch_openaq_data(ds, **kwargs):
    base_url = "https://api.openaq.org/v3/measurements"
    params = [
        ("country", "GB"),
        ("date_from", f"{ds}T00:00:00+00:00"),
        ("date_to",   f"{ds}T23:59:59+00:00"),
        ("limit",     "10000"),
        ("parameter", "pm25"),
        ("parameter", "no2"),
        ("parameter", "o3"),
    ]
    api_key = os.environ.get("OPENAQ_API_KEY")
    headers = {"X-API-Key": api_key} if api_key else {}

    # — Debug before the request —
    print(">>>> OPENAQ_API_KEY:", "***" if api_key else None)
    print(">>>> REQUEST URL:", base_url)
    print(">>>> PARAMS:", params)
    print(">>>> HEADERS:", headers)

    # Skip future dates
    if datetime.strptime(ds, "%Y-%m-%d").date() > date.today():
        print(f"Date {ds} is in the future; skipping.")
        return

    # Make the call
    try:
        response = requests.get(base_url, params=params, headers=headers)
        print(">>>> FULL GET URL:", response.request.url)
        print(">>>> STATUS CODE:", response.status_code)
        print(">>>> RESPONSE BODY (first 200 chars):", response.text[:200])
    except Exception as e:
        print(f"Error during request: {e}")
        raise

    if response.status_code == 200:
        try:
            data = response.json().get("results", [])
        except Exception as e:
            print(f"Could not decode JSON: {e}")
            raise Exception(f"Failed to parse JSON: {response.text[:200]}") from e

        output_dir = "/opt/airflow/data/bronze/openaq"
        os.makedirs(output_dir, exist_ok=True)
        file_path = os.path.join(output_dir, f"{ds}.json")
        try:
            with open(file_path, "w") as f:
                json.dump(data, f)
            print(f"Saved {len(data)} records to {file_path}")
        except Exception as e:
            print(f"Error writing to file {file_path}: {e}")
            raise
    elif response.status_code == 404:
        print(f"No data found for {ds} (404)")
    else:
        # Try to extract error details, but do not crash if not JSON
        try:
            detail = response.json().get("detail")
        except Exception:
            detail = None
        print(f"API error for {ds}: {response.status_code}, detail: {detail}")
        raise Exception(f"Failed to fetch: {response.text[:200]}")

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ingest_openaq",
    default_args=default_args,
    description="Fetch OpenAQ PM2.5, NO2, O3 data daily for UK",
    schedule="@daily",   # use `schedule` in Airflow ≥2.8
    start_date=datetime(2025, 6, 1),
    catchup=True,
    max_active_runs=1,
    tags=["ingestion", "openaq"],
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_openaq_data",
        python_callable=fetch_openaq_data,
        # No need for provide_context in Airflow ≥2.0
    )
