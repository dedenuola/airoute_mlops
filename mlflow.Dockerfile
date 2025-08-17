FROM ghcr.io/mlflow/mlflow:latest

# Healthcheck depends on curl (and psycopg2 for Postgres backend)
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
RUN pip install psycopg2-binary
