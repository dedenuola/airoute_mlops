import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from feast import FeatureStore
import mlflow.pyfunc

app = FastAPI(title="RouteAQ PM2.5 Predictor")

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MODEL_URI = os.getenv("MODEL_URI", "models:/routeaq_pm25/Production")
FEAST_REPO_PATH = os.getenv("FEAST_REPO_PATH", "/app/feature_repo")

# Do NOT swallow exceptions here—fail fast if misconfigured
STORE = FeatureStore(repo_path=FEAST_REPO_PATH)
MODEL = mlflow.pyfunc.load_model(MODEL_URI)

class PredictRequest(BaseModel):
    site_id: str
    timestamp: str

@app.get("/health")
def health():
    return {"status": "ok", "feast_repo": FEAST_REPO_PATH}

@app.post("/predict")
def predict(req: PredictRequest):
    entity_rows = [{"site_id": req.site_id}]
    features = STORE.get_online_features(
        features=[
            "aq_hourly:pm25_t_1","aq_hourly:no2_t_1","aq_hourly:o3_t_1",
            "aq_hourly:temp","aq_hourly:wind","aq_hourly:humidity"
        ],
        entity_rows=entity_rows
    ).to_df()

    if features.isnull().any(axis=1).iloc[0]:
        raise HTTPException(400, f"Missing features for {req.site_id} at {req.timestamp}")

    pred = MODEL.predict(features)[0]
    return {"site_id": req.site_id, "timestamp": req.timestamp, "pm25_pred": float(pred)}