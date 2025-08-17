# dags/evidently_drift_daily.py
import os, glob, json
from pathlib import Path
from datetime import datetime, timedelta, timezone

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

# Evidently
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset

PRED_DIR = Path("/opt/airflow/monitoring/predictions")
OUT_DIR  = Path("/opt/airflow/monitoring/reports")
OUT_DIR.mkdir(parents=True, exist_ok=True)

def _find_baseline_and_current():
    files = sorted(glob.glob(str(PRED_DIR / "preds_*.csv")))
    if not files:
        raise FileNotFoundError(f"No prediction files found in {PRED_DIR}")
    baseline, current = files[0], files[-1]
    return baseline, current

def _load_preds(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    # normalize likely column names
    cols = {c.lower(): c for c in df.columns}
    # accept "timestamp" or "iso_ts"
    ts_col = "timestamp" if "timestamp" in cols else ("iso_ts" if "iso_ts" in cols else None)
    if ts_col:
        df[ts_col] = pd.to_datetime(df[ts_col], utc=True, errors="coerce")
    # ensure pm25_pred exists
    if "pm25_pred" not in df.columns:
        # try common variants, e.g., prediction
        alt = [c for c in df.columns if c.lower() in {"prediction","pred","pm25"}]
        if alt:
            df["pm25_pred"] = df[alt[0]]
    if "pm25_pred" not in df.columns:
        raise ValueError(f"'pm25_pred' column not found in {path}. Columns: {list(df.columns)}")
    return df[["pm25_pred"]].copy()

def compute_evidently(**_):
    baseline_path, current_path = _find_baseline_and_current()
    base_df   = _load_preds(baseline_path)
    current_df= _load_preds(current_path)

    report = Report(metrics=[DataDriftPreset()])
    report.run(reference_data=base_df, current_data=current_df)

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    html_path = OUT_DIR / f"drift_{today}.html"
    json_path = OUT_DIR / f"drift_{today}.json"

    report.save_html(html_path)
    with open(json_path, "w") as f:
        json.dump(report.as_dict(), f)

    print(f"[EVIDENTLY] Baseline: {baseline_path}")
    print(f"[EVIDENTLY] Current : {current_path}")
    print(f"[EVIDENTLY] Wrote   : {html_path} and {json_path}")

default_args = dict(
    owner="airflow",
    retries=1,
    retry_delay=timedelta(minutes=5),
)

with DAG(
    dag_id="evidently_drift_daily",
    description="Compute daily data drift on pm25_pred using Evidently",
    start_date=datetime(2025, 8, 1, tzinfo=timezone.utc),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["monitoring","evidently"],
) as dag:
    run_drift = PythonOperator(
        task_id="compute_drift",
        python_callable=compute_evidently,
    )
