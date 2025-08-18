# services/api/main.py
import os
import csv
import logging
from pathlib import Path
import datetime as dt
import pandas as pd

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# Heavy deps are imported but instantiated lazily
import mlflow
import mlflow.pyfunc
from feast import FeatureStore

# Fallback readers (pyarrow uses s3fs transparently)
import pyarrow.dataset as ds
import pyarrow.compute as pc

# ---------- config ----------
ROUTEAQ_JOINED_PATH = os.getenv(
    "ROUTEAQ_JOINED_PATH",
    "s3://routeaq-feast-offline/silver/joined/*.parquet",
)
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MODEL_URI = os.getenv("MODEL_URI", "models:/routeaq_pm25/Production")
FEAST_REPO_PATH = os.getenv("FEAST_REPO_PATH", "/app/feature_repo")
PRED_LOG_DIR = Path(os.getenv("PRED_LOG_DIR", "/app/monitoring/predictions"))

FEATURE_LIST = ["pm25_t_1", "no2_t_1", "o3_t_1", "temp", "wind", "humidity"]

# ---------- logging ----------
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
logger = logging.getLogger("routeaq.api")

# Ensure the predictions dir exists
PRED_LOG_DIR.mkdir(parents=True, exist_ok=True)
logger.info(
    "[startup] Prediction logs directory: %s (exists=%s, writable=%s)",
    PRED_LOG_DIR,
    PRED_LOG_DIR.exists(),
    os.access(PRED_LOG_DIR, os.W_OK),
)

app = FastAPI(title="RouteAQ PM2.5 Predictor")

# Lazy singletons (initialized on-demand)
app.state.store = None
app.state.model = None


# ---------- helpers ----------
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


def _normalize_s3_path(p: str) -> str:
    # pyarrow.dataset accepts "s3://bucket/prefix/*.parquet"
    # If someone accidentally removed the scheme, fix it.
    if p.startswith("s3://"):
        return p
    if p and ("/" in p) and (":" not in p.split("/", 1)[0]):
        return "s3://" + p
    return p


def _load_features_from_parquet(site_id: str, ts_iso: str) -> dict:
    """
    Fallback path: read from S3 parquet and return the most recent row
    with date_time <= requested timestamp, for the numeric FEATURE_LIST columns.
    Assumes columns: site_id (str), date_time (timestamp), and FEATURE_LIST.
    """
    try:
        path = _normalize_s3_path(ROUTEAQ_JOINED_PATH)
        dataset = ds.dataset(path, format="parquet")  # uses s3fs automatically

        # Parse incoming timestamp as UTC
        ts = pd.to_datetime(ts_iso, utc=True)

        # Restrict to needed columns
        cols = ["site_id", "date_time"] + FEATURE_LIST

        # Filter: exact site + date_time <= ts
        tbl = dataset.to_table(
            columns=cols,
            filter=(pc.field("site_id") == site_id) & (pc.field("date_time") <= pc.scalar(ts.to_pydatetime())),
        )

        if tbl.num_rows == 0:
            raise ValueError(f"No parquet rows for site_id={site_id} up to {ts_iso}")

        df = tbl.to_pandas()
        df["date_time"] = pd.to_datetime(df["date_time"], utc=True)
        df = df.sort_values("date_time")
        last = df.iloc[-1]

        row = {k: float(last[k]) for k in FEATURE_LIST}
        logger.info("[fallback] parquet row loaded for %s @ %s -> %s", site_id, ts_iso, row)
        return row

    except Exception as e:
        logger.exception("[fallback] parquet load failed")
        raise HTTPException(status_code=503, detail=f"Parquet read failed: {e}")


# ---------- API ----------
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

    # 1) Request features with plain column names (no view prefix)
    feature_refs = [f"aq_hourly:{f}" for f in FEATURE_LIST]
    try:
        online_feats = store.get_online_features(
            features=feature_refs,
            entity_rows=[{"site_id": req.site_id}],
            full_feature_names=False,  # return 'pm25_t_1', not 'aq_hourly:pm25_t_1'
        ).to_dict()
    except Exception as e:
        logger.exception("[error] get_online_features failed")
        raise HTTPException(status_code=503, detail=f"Feast online features failed: {e}")

    # 2) Build a single-row dataframe in the model's column order.
    try:
        row = {k: (online_feats[k][0] if isinstance(online_feats[k], list) else online_feats[k]) for k in FEATURE_LIST}

        # If Feast returned any None, fall back to parquet
        if any(v is None for v in row.values()):
            logger.warning(
                "[predict] Feast returned None(s); falling back to parquet for %s @ %s",
                req.site_id,
                req.timestamp,
            )
            row = _load_features_from_parquet(req.site_id, req.timestamp)

        X = pd.DataFrame([row], columns=FEATURE_LIST).astype(float)
    except KeyError as e:
        missing = str(e).strip("'")
        raise HTTPException(status_code=400, detail=f"Missing feature: {missing}")
    except HTTPException:
        # already raised with a good message from fallback
        raise
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
