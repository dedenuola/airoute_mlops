FROM python:3.12-slim
WORKDIR /app

# OS deps for LightGBM
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgomp1 && rm -rf /var/lib/apt/lists/*

# Ensure pkg_resources exists for mlflow
RUN pip install --no-cache-dir --upgrade pip setuptools

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
EXPOSE 8000
CMD ["uvicorn","main:app","--host","0.0.0.0","--port","8000"]


# FROM python:3.12-slim

# WORKDIR /app

# # OS deps for LightGBM
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     libgomp1 \
#  && rm -rf /var/lib/apt/lists/*

# # Fix pkg_resources missing
# RUN pip install --no-cache-dir --upgrade pip setuptools

# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt

# COPY . .

# # start the API
# CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]








