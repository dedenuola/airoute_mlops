FROM ghcr.io/mlflow/mlflow:latest
RUN pip install psycopg2-binary && \
    apt-get update && apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*