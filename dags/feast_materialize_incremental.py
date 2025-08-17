# dags/feast_materialize_incremental.py
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import subprocess

# Feast repo path inside the container
FEAST_REPO = "/opt/airflow/feature_repo"

def materialize_incremental(**_):
    # ensures FEAST reads AWS creds from env (your .env is already mounted)
    env = os.environ.copy()
    # run "feast apply" once (idempotent), then materialize-incremental to now
    subprocess.run(["feast", "--cwd", FEAST_REPO, "apply"], check=True, env=env)
    subprocess.run(
        ["feast", "--cwd", FEAST_REPO, "materialize-incremental", datetime.utcnow().isoformat()],
        check=True,
        env=env,
    )

default_args = dict(owner="airflow", retries=1)
with DAG(
    dag_id="feast_materialize_incremental",
    default_args=default_args,
    schedule="@daily",         # daily is fine for MVP
    start_date=datetime(2025, 8, 1),
    catchup=False,
    max_active_runs=1,
    tags=["feast","features"],
) as dag:

    t = PythonOperator(
        task_id="feast_materialize_incremental",
        python_callable=materialize_incremental,
    )
