# mlflow.Dockerfile
FROM ghcr.io/mlflow/mlflow:latest

RUN pip install psycopg2-binary

# Optional: If I want to run MLflow server with a specific version
# FROM python:3.10-slim
# RUN pip install --no-cache-dir mlflow==2.11.1 psycopg2-binary
# ENTRYPOINT ["mlflow"]
