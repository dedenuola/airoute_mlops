# RouteAQ – End-to-End Air-Quality Forecasting MLOps Pipeline

I’m building **RouteAQ**, a one-hour-ahead (PM₂.₅,NO₂, O₃) forecasting service for UK postcodes. At a high level, I’ve completed:

1. **Data Ingestion**  
   - **DEFRA AURN** hourly pollutant CSVs (PM₂.₅, NO₂, O₃) landed daily via Airflow DAGs  
   - **Met Office** hourly forecast JSONs (temperature, wind, humidity) landed via a second Airflow task

2. **Feature Store**  
   - Defined a Feast repo with `feature_store.yaml` (local offline store & SQLite online store)  
   - Created a `FeatureView` (`aq_hourly`) that exposes t–1 pollutant lags + latest weather

3. **Data Joining & Silver Tables**  
   - Extracted raw JSON & CSV into Parquet in `/data/silver`  
   - Joined pollutant + weather features on `(site_id, timestamp)` to produce a single training table

4. **Model Training & Registry**  
   - Trained a LightGBM regressor on June–July data  
   - Logged parameters, metrics, and **model signature + input example** to MLflow  
   - Registered `routeaq_pm25` model (versions 1 & 2) in a local MLflow server

5. **Model Serving**  
   - Built a FastAPI app (`/services/api/main.py`) that:  
     - Fetches online features from Feast  
     - Loads the registered MLflow model  
     - Exposes `POST /predict` for real-time PM₂.₅ forecasts  
   - Dockerized the API and orchestrated it alongside Airflow, Postgres, and MLflow via Docker-Compose

---

## 🗂️ Project Structure

├─ dags/ # Airflow DAG definitions (ingest_defra, ingest_metoffice)

├─ data/

│ ├─ bronze/ # Raw JSON/CSV from DEFRA & Met Office

│ └─ silver/ # Parquet tables: pollutant_hourly, weather_hourly, joined

├─ feature_repo/ # Feast feature store definitions

│ ├─ feature_store.yaml

│ └─ aq_feature_view.py

├─ mlruns/ # MLflow artifact root (models + run logs)

├─ mlflow.db # MLflow SQLite backend

├─ services/

│ └─ api/

│ ├─ main.py # FastAPI application

│ ├─ requirements.txt

│ └─ Dockerfile

├─ docker-compose.yml # Dev stack: Postgres, Airflow, MLflow, API

├─ train_routeaq_pm25.py # Script/notebook for full train → log → register
 workflow
 
└─ README.md # ← you are here


---

## 🚀 How to Run Locally

1. **Prerequisites**  
   - Docker & Docker-Compose  
   - Python 3.10+/venv  

2. **Initialize Airflow & Postgres**  
   docker-compose up -d postgres webserver scheduler airflow-init
Run MLflow

Option A (quick) in your venv:

mlflow server \
  --backend-store-uri sqlite:///mlflow.db \
  --default-artifact-root ./mlruns \
  --host 0.0.0.0 --port 5000
Option B (Docker):
docker-compose up -d mlflow
Trigger Ingestion

Airflow will backfill DEFRA & Met Office DAGs from start_date=2025-06-01

Check /opt/airflow/data/bronze inside the webserver container

Train & Register Model
python train_routeaq_pm25.py
Confirm runs & model versions in MLflow UI at http://localhost:5000

Start the API
docker-compose up -d api
Health check: curl http://localhost:8000/health → {"status":"ok"}

Prediction:
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{"site_id":"ABD7","timestamp":"2025-08-05T14:00:00Z"}'
🔭 My Next Steps
Integration Tests

Write a simple pytest or bash script to automatically hit /predict and assert a valid response.

Model Monitoring

Integrate Evidently in an Airflow DAG to generate drift & performance reports

Configure alerts (e.g. email or Slack) on metric degradation.

Cloud Deployment

Push routeaq-api image to AWS ECR

Deploy Airflow & MLflow on AWS (ECS, EKS, or Cloud Composer + managed services)

Use S3 for Feast offline store & RDS for online store.

CI/CD & Best Practices

Add GitHub Actions for linting (flake8), testing, and building Docker images

Set up pre-commit hooks and a Makefile for common commands

Document environment variables and versions in requirements.txt or environment.yml

I’m excited to keep refining RouteAQ—next up, automated tests and basic monitoring so I can ensure this forecast service runs smoothly in production!