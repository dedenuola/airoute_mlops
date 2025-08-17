# monitoring/run_evidently.py
import os, pandas as pd, mlflow
from evidently.report import Report
from evidently.metrics import ColumnDriftMetric, ColumnSummaryMetric
from evidently.metric_preset import DataDriftPreset

TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
JOINED_PARQUET = os.getenv("JOINED_PARQUET",
    "/opt/airflow/data/silver/joined/hourly_joined_2025_from_28jul.parquet")

TARGET = "pm25_target"
FEATURES = ["pm25_t_1","no2_t_1","o3_t_1","temp","wind","humidity"]

def main():
    mlflow.set_tracking_uri(TRACKING_URI)

    df = pd.read_parquet(JOINED_PARQUET)
    # quick, static reference vs. current split (last 24h vs earlier). MVP only.
    df["date_time"] = pd.to_datetime(df["date_time"], utc=True)
    ref = df[df["date_time"] < df["date_time"].max() - pd.Timedelta(days=1)]
    cur = df[df["date_time"] >= df["date_time"].max() - pd.Timedelta(days=1)]

    report = Report(metrics=[
        DataDriftPreset(),
        ColumnSummaryMetric(column_name=TARGET),
        ColumnDriftMetric(column_name=TARGET),
    ])
    report.run(reference_data=ref[FEATURES+[TARGET]], current_data=cur[FEATURES+[TARGET]])

    html_path = "evidently_pm25.html"
    report.save_html(html_path)

    with mlflow.start_run(run_name="evidently_pm25_report"):
        mlflow.log_artifact(html_path)

if __name__ == "__main__":
    main()
