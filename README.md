# RouteAQ — cloud-hosted MLOps slice for one-hour-ahead PM₂.₅ forecasts

## Problem we’re solving

Air quality varies block-by-block and hour-by-hour. People with respiratory conditions, cyclists, and city ops teams all need short-horizon forecasts to plan routes and reduce exposure. Public data exists (pollutants from DEFRA AURN and weather from the Met Office), but turning that into a **reliable, cloud-hosted prediction service** is the hard part.

**RouteAQ** is a minimal, production-shaped solution: it ingests open data, builds features, trains a model, registers it in MLflow, and serves real-time predictions on AWS EC2 behind a simple HTTP API. The first target is **PM₂.₅** (fine particulates) one hour ahead. NO₂ and O₃ use the same pattern and are planned next.

---

## What’s working now (MVP)

- ✅ **Serving on EC2** with Docker Compose (`FastAPI` on port **8000**).
- ✅ **Model registry with MLflow** (Postgres backend, **artifacts in S3**).
- ✅ **Features via Feast 0.47** (SQLite online store in-container) **or** robust **S3 Parquet fallback** if Feast isn’t materialized.
- ✅ **PM₂.₅ model** (LightGBM) pulled from MLflow Model Registry at request time.
- ✅ **Prediction logs** written as CSV (quick demo artifact you can plot later).

> This is deliberately a thin, end-to-end vertical slice. It’s enough to review, demo live, and extend.

---

## Architecture (accurate to this repo)

```mermaid
flowchart LR
  %% ---------- Context / Security ----------
  subgraph VPC_Security["VPC / Security"]
    IAM[IAM Instance Profile<br/>EC2-S3-Access-Role<br/>(mlops-airoute-deployment-policy)]
    SG[Security Group<br/>22, 8000, 8080, 5000<br/>(restricted to your IP)]
  end

  %% ---------- Compute ----------
  subgraph EC2["EC2 t3.large (Docker Compose)"]
    %% Postgres (single container with two DBs)
    subgraph PG["Postgres :5432"]
      P1[airflow_meta DB]
      P2[mlflow backend DB]
    end

    %% Airflow
    subgraph AF["Airflow"]
      AFW[Webserver :8080]
      AFS[Scheduler]
      AFW --> AFS
    end

    %% Feature storage (Feast)
    S3[(S3 bucket<br/>routeaq-feast-offline<br/>silver/joined/*.parquet)]
    FS[Feast FileSource<br/>(S3 *.parquet glob)]
    ONLINE[Online store (SQLite)<br/>/opt/airflow/feature_repo/data/online_store]

    %% Serving API
    subgraph API["FastAPI service :8000"]
      H[GET /health<br/>returns ok]
      P[POST /predict<br/>PM2.5]
    end

    %% MLflow tracking / registry
    subgraph MLF["MLflow Tracking :5000"]
      MR[Model Registry<br/>routeaq_pm25 @ prod]
    end
  end

  %% ---------- Local dev ----------
  DEV[Local Dev / Codespaces<br/>notebooks & scripts]

  %% ---------- Data / control flows ----------
  S3 --> FS --> ONLINE
  AFS -->|materialize| ONLINE
  API -->|get_online_features(site_id)| ONLINE

  DEV -->|set_tracking_uri + log_model| MLF
  MR -->|load model<br/>models:/routeaq_pm25@prod| API

  %% ---------- Security associations (dotted) ----------
  IAM -.-> EC2
  SG  -.-> EC2
```
---

## Live demo (EC2)

On the EC2 instance:

```bash
cd ~/airoute_mlops
git pull --rebase

# Start/refresh services
docker compose up -d postgres mlflow
docker compose up -d webserver scheduler
docker compose up -d --build api

# Health
curl -s http://localhost:8000/health | jq .

# Predict (one hour ahead sample)
curl -s -X POST http://localhost:8000/predict   -H "Content-Type: application/json"   -d '{"site_id":"CLL2","timestamp":"2025-08-01T10:00:00Z"}'
# -> {"site_id":"CLL2","timestamp":"2025-08-01T10:00:00Z","pm25_pred":<float>,"source":"feast|parquet"}

# Confirm it logged
tail -n +1 monitoring/predictions/preds_$(date -u +%F).csv
```

---

## Operational sanity checks (quick checklist)

- `GET /health` returns `"status": "ok"` and shows `s3_sample` with at least one match.
- The S3 path in `/health` (`joined_path`) **matches** your bucket/glob:
  `s3://routeaq-feast-offline/silver/joined/*.parquet`.
- MLflow UI reachable on **:5000**  
  - Open SG to your IP **or** tunnel:  
    `ssh -i KEY.pem -N -L 5000:localhost:5000 ec2-user@EC2_PUBLIC_IP` → visit http://localhost:5000
- (Optional) a daily Airflow DAG writes an Evidently report to S3. A placeholder DAG is included; enable it when ready.

---

## Deploying on AWS (minimal)

**Compute:** EC2 t3.large (Amazon Linux or Ubuntu), with Docker & Docker Compose v2.  
**S3 buckets:**
- `routeaq-feast-offline` → `silver/joined/*.parquet`
- `routeaq-mlflow-artifacts` → MLflow artifacts (e.g. `mlruns/<exp>/<run>/...`)

**IAM role** attached to the EC2 instance profile:
- `s3:ListBucket` on both buckets
- `s3:GetObject/PutObject/DeleteObject` on `arn:aws:s3:::routeaq-mlflow-artifacts/*` and `arn:aws:s3:::routeaq-feast-offline/*`
- (Optional) ECR push/pull + CloudWatch Logs basic

**Security group (demo):** inbound 22, 8000, 8080, 5000 from your IP (tighten later).

---

## Local development (optional)

```bash
# Infra
docker compose up -d postgres mlflow
docker compose up -d airflow-init
docker compose up -d webserver scheduler

# API
docker compose up -d --build api

# Test
curl -s http://localhost:8000/health | jq .
```

> If you want Feast online features locally, materialize once:
> `docker compose exec webserver bash -lc 'cd /opt/airflow/feature_repo && feast apply && feast materialize "2025-07-01T00:00:00Z" "$(date -u +%Y-%m-%dT%H:%M:%S)"'`

---

## API

- `GET /health` → shows basic environment + quick S3 check (one matched file if available).
- `POST /predict`
  ```json
  {
    "site_id": "CLL2",
    "timestamp": "2025-08-01T10:00:00Z"
  }
  ```
  Response:
  ```json
  {
    "site_id": "CLL2",
    "timestamp": "2025-08-01T10:00:00Z",
    "pm25_pred": 3.94,
    "source": "feast"  // or "parquet" if Feast fallback was used
  }
  ```

---

## What’s in the repo

```
.
├─ dags/                         # Airflow DAGs (ingestion/monitoring; placeholders included)
├─ feature_repo/                 # Feast repo (aq_feature_view.py, feature_store.yaml)
├─ services/
│  └─ api/
│     ├─ main.py                 # FastAPI app (Feast first, S3 Parquet fallback)
│     ├─ requirements.txt
│     └─ Dockerfile
├─ mlflow.Dockerfile             # MLflow server image
├─ docker-compose.yml            # Postgres, MLflow, Airflow, API
└─ tests/
   └─ smoke_test.sh              # Curl health & predict; exits non-zero if broken
```

**Not checked in** (see `.gitignore`): raw data, large parquet, MLflow artifacts, prediction logs, secrets (`.env`).

---

## Data & privacy

- **DEFRA AURN** is open data; still keep raw drops out of Git.
- **Met Office** datasets have specific license/usage terms. Store credentials in `.env` (gitignored) or in AWS runtime env. Do **not** commit raw downloads.
- Keep S3 buckets private; expose only the API and the needed web UIs via your IP.

---

## Roadmap (after MVP)

- Add **NO₂** and **O₃** models:
  - replicate the PM₂.₅ training/logging flow
  - register `routeaq_no2`, `routeaq_o3` in MLflow
  - Option A: separate endpoints; Option B: `/predict?target=no2|o3|pm25`
- Monitoring:
  - daily Evidently report from the latest `preds_*.csv` (Airflow DAG → HTML/JSON into S3)
  - simple Airflow email/Slack alert if drift detected
- CI/CD:
  - GitHub Action to run `tests/smoke_test.sh` on PRs
  - build & push images (later ECR)

---

## Troubleshooting

- **/predict says “Parquet read failed …”**  
  Check `/health` → `s3_sample` must show at least one file. Verify `ROUTEAQ_JOINED_PATH` in `docker-compose.yml` and that the IAM role includes List/Get on that bucket/prefix.

- **Feast returns Nones**  
  You didn’t materialize (fine — fallback covers you). To use Feast online store, run `feast apply && feast materialize …` inside the Airflow container.

- **MLflow “unhealthy”**  
  Healthcheck hits `/api/2.0/mlflow/experiments/list`. Ensure Postgres is up and `--default-artifact-root` points to your S3 bucket.

---

## License

MIT
