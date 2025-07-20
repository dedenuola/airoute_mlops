# airoute_mlops
# AIRouteAQ: Live Postcode-Level Air-Quality Forecasting with MLOps

Predict the next-hour Air Quality Index (AQI) for any UK postcode, recommending the cleanest time to leave or bike.  
End-to-end reproducible MLOps: ingestion, feature store, MLflow registry, containerized serving, drift monitoring, and auto-retraining.  

## High-level folders

- `dags/` – Airflow pipelines
- `feature_repo/` – Feast feature definitions
- `models/` – Model training & artefacts
- `services/` – FastAPI app, KServe config
- `infra/` – Docker, deployment scripts

...TBC
