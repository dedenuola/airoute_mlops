# services/api/main.py
import os
import csv
import logging
from pathlib import Path
import datetime as dt
import pandas as pd


from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# Import the packages but don't construct heavy objects at import time
import mlflow
import mlflow.pyfunc
from feast import FeatureStore

# ---------- config ----------
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MODEL_URI           = os.getenv("MODEL_URI", "models:/routeaq_pm25/Production")
FEAST_REPO_PATH     = os.getenv("FEAST_REPO_PATH", "/app/feature_repo")
PRED_LOG_DIR        = Path(os.getenv("PRED_LOG_DIR", "/app/monitoring/predictions"))
FEATURE_LIST = ["pm25_t_1", "no2_t_1", "o3_t_1", "temp", "wind", "humidity"]


# ---------- logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("routeaq.api")

# Ensure the predictions dir exists (this is safe)
PRED_LOG_DIR.mkdir(parents=True, exist_ok=True)
logger.info("[startup] Prediction logs directory: %s (exists=%s, writable=%s)",
            PRED_LOG_DIR, PRED_LOG_DIR.exists(), os.access(PRED_LOG_DIR, os.W_OK))

app = FastAPI(title="RouteAQ PM2.5 Predictor")

# Lazy singletons (initialized on-demand)
app.state.store = None
app.state.model = None

def get_store() -> FeatureStore:
    if app.state.store is None:
        logger.info("[init] Constructing Feast FeatureStore at %s", FEAST_REPO_PATH)
        app.state.store = FeatureStore(repo_path=FEAST_REPO_PATH)
    return app.state.store

def get_model():
    if app.state.model is None:
        logger.info("[init] Loading MLflow model: %s (tracking=%s)", MODEL_URI, MLFLOW_TRACKING_URI)
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        app.state.model = mlflow.pyfunc.load_model(MODEL_URI)
    return app.state.model

def log_prediction(site_id: str, ts_iso: str, y_pred: float):
    today = dt.datetime.utcnow().strftime("%Y-%m-%d")
    fp = PRED_LOG_DIR / f"preds_{today}.csv"
    header = ["timestamp", "site_id", "pm25_pred"]
    new_file = not fp.exists()
    with fp.open("a", newline="") as f:
        w = csv.writer(f)
        if new_file:
            w.writerow(header)
        w.writerow([ts_iso, site_id, y_pred])
    # logger.info("[predict] wrote -> %s", fp)

class PredictRequest(BaseModel):
    site_id: str
    timestamp: str  # ISO8601 string

@app.get("/health")
def health():
    return {
        "status": "ok",
        "feast_repo": FEAST_REPO_PATH,
        "pred_dir": str(PRED_LOG_DIR),
        "pred_dir_exists": PRED_LOG_DIR.exists(),
        "pred_dir_writable": os.access(PRED_LOG_DIR, os.W_OK),
    }

@app.post("/predict")
def predict(req: PredictRequest):
    # Resolve dependencies lazily and report precise errors
    try:
        store = get_store()
    except Exception as e:
        logger.exception("[error] Failed to init Feast")
        raise HTTPException(status_code=503, detail=f"Feast init failed: {e}")

    try:
        model = get_model()
    except Exception as e:
        logger.exception("[error] Failed to load MLflow model")
        raise HTTPException(status_code=503, detail=f"Model load failed: {e}")

    # 1) Request features with plain column names (no view prefix in the result)
    feature_refs = [f"aq_hourly:{f}" for f in FEATURE_LIST]
    try:
        online_feats = store.get_online_features(
            features=feature_refs,
            entity_rows=[{"site_id": req.site_id}],
            full_feature_names=False,   # <-- critical: returns pm25_t_1, not aq_hourly:pm25_t_1
        ).to_dict()
    except Exception as e:
        logger.exception("[error] get_online_features failed")
        raise HTTPException(status_code=503, detail=f"Feast online features failed: {e}")

    # 2) Build a single-row dataframe with ONLY numeric model columns in order
    try:
        row = {k: (online_feats[k][0] if isinstance(online_feats[k], list) else online_feats[k])
               for k in FEATURE_LIST}
        X = pd.DataFrame([row], columns=FEATURE_LIST).astype(float)
    except KeyError as e:
        missing = str(e).strip("'")
        raise HTTPException(status_code=400, detail=f"Missing feature: {missing}")
    except Exception as e:
        logger.exception("[error] building model input failed")
        raise HTTPException(status_code=503, detail=f"Building model input failed: {e}")

    # 3) Predict
    try:
        pred = float(model.predict(X)[0])
    except Exception as e:
        logger.exception("[error] model.predict failed")
        raise HTTPException(status_code=503, detail=f"Prediction failed: {e}")

    # 4) Best-effort logging (doesn't fail the request)
    try:
        log_prediction(req.site_id, req.timestamp, pred)
    except Exception as e:
        logger.exception("[warn] prediction logging failed: %s", e)

    return {"site_id": req.site_id, "timestamp": req.timestamp, "pm25_pred": pred}