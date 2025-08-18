# services/api/main.py
import os
import csv
import logging
from pathlib import Path
import datetime as dt
from typing import Dict, List, Optional

import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# Heavy libs loaded lazily where possible
import mlflow
import mlflow.pyfunc

# Optional deps used for S3 fallback
try:
    import s3fs  # type: ignore
    S3FS_AVAILABLE = True
except Exception:  # pragma: no cover
    S3FS_AVAILABLE = False

try:
    import pyarrow  # noqa: F401
    import pyarrow.parquet as pq  # noqa: F401
    PYARROW_AVAILABLE = True
except Exception:  # pragma: no cover
    PYARROW_AVAILABLE = False

# Feast is optional (!) – we want the API to survive even if Feast isn't ready
try:
    from feast import FeatureStore  # type: ignore
    FEAST_AVAILABLE = True
except Exception:  # pragma: no cover
    FeatureStore = None  # type: ignore
    FEAST_AVAILABLE = False


# ------------------------------- configuration -------------------------------

ROUTEAQ_JOINED_PATH = os.getenv(
    "ROUTEAQ_JOINED_PATH",
    "s3://routeaq-feast-offline/silver/joined/*.parquet",
)
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MODEL_URI = os.getenv("MODEL_URI", "models:/routeaq_pm25/Production")
FEAST_REPO_PATH = os.getenv("FEAST_REPO_PATH", "/app/feature_repo")

PRED_LOG_DIR = Path(os.getenv("PRED_LOG_DIR", "/app/monitoring/predictions"))
FEATURE_LIST: List[str] = ["pm25_t_1", "no2_t_1", "o3_t_1", "temp", "wind", "humidity"]


# --------------------------------- logging -----------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("routeaq.api")

PRED_LOG_DIR.mkdir(parents=True, exist_ok=True)
logger.info(
    "[startup] Prediction logs directory: %s (exists=%s, writable=%s)",
    PRED_LOG_DIR, PRED_LOG_DIR.exists(), os.access(PRED_LOG_DIR, os.W_OK)
)


# --------------------------------- app init ----------------------------------

app = FastAPI(title="RouteAQ PM2.5 Predictor")
app.state.store = None
app.state.model = None


# ------------------------------- helpers: feast -------------------------------

def get_store():
    if not FEAST_AVAILABLE:
        raise RuntimeError("Feast not installed in this image")
    if app.state.store is None:
        logger.info("[init] Constructing Feast FeatureStore at %s", FEAST_REPO_PATH)
        app.state.store = FeatureStore(repo_path=FEAST_REPO_PATH)
    return app.state.store


# ------------------------------- helpers: model -------------------------------

def get_model():
    if app.state.model is None:
        logger.info("[init] Loading MLflow model: %s (tracking=%s)", MODEL_URI, MLFLOW_TRACKING_URI)
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        app.state.model = mlflow.pyfunc.load_model(MODEL_URI)
    return app.state.model


# ---------------------------- helpers: s3 parquet -----------------------------

def _list_s3_matches(glob_path: str) -> List[str]:
    if not glob_path.startswith("s3://"):
        raise ValueError(f"ROUTEAQ_JOINED_PATH must start with s3:// (got: {glob_path})")
    if not S3FS_AVAILABLE:
        raise RuntimeError("s3fs is not installed")
    fs = s3fs.S3FileSystem(anon=False)
    # fs.glob returns keys without s3://
    matches = fs.glob(glob_path)
    return [f"s3://{k}" for k in matches]


def _latest_row_from_parquet(site_id: str, ts_iso: str) -> Dict[str, Optional[float]]:
    """
    Load a single row for (site_id, <= timestamp) from one Parquet file matched by the glob.
    Returns a dict with FEATURE_LIST keys. Values may be None if the columns are null in the file.
    """
    if not (S3FS_AVAILABLE and PYARROW_AVAILABLE):
        raise RuntimeError("Parquet fallback needs s3fs + pyarrow installed")

    matches = _list_s3_matches(ROUTEAQ_JOINED_PATH)
    if not matches:
        raise FileNotFoundError(f"No objects matched: {ROUTEAQ_JOINED_PATH}")

    # Prefer a stable choice (first match). You can swap to max(...) if files are partitioned by time.
    src = matches[0]
    cols = ["site_id", "date_time"] + FEATURE_LIST

    # Let pandas + pyarrow read from s3 directly
    df = pd.read_parquet(src, columns=cols)

    # Paranoid casting
    if "date_time" not in df.columns:
        raise KeyError("date_time column missing in parquet file")

    df["date_time"] = pd.to_datetime(df["date_time"], utc=True, errors="coerce")
    ts = pd.to_datetime(ts_iso, utc=True, errors="coerce")
    if pd.isna(ts):
        raise ValueError(f"timestamp is not ISO-8601: {ts_iso}")

    df = df[(df["site_id"] == site_id) & (df["date_time"] <= ts)]
    if df.empty:
        # Return empty features -> caller will raise a friendly error
        return {c: None for c in FEATURE_LIST}

    row = df.sort_values("date_time", ascending=False).iloc[0]
    return {f: (None if pd.isna(row.get(f)) else float(row.get(f))) for f in FEATURE_LIST}


def _build_X_from_row(row: Dict[str, Optional[float]]) -> pd.DataFrame:
    # Ensure column order matches training
    values = {k: row.get(k) for k in FEATURE_LIST}
    if any(v is None for v in values.values()):
        missing = [k for k, v in values.items() if v is None]
        raise ValueError(f"Missing feature values from parquet fallback: {missing}")
    return pd.DataFrame([values], columns=FEATURE_LIST).astype(float)


# --------------------------------- logging -----------------------------------

def log_prediction(site_id: str, ts_iso: str, y_pred: float):
    today = dt.datetime.utcnow().strftime("%Y-%m-%d")
    fp = PRED_LOG_DIR / f"preds_{today}.csv"
    new_file = not fp.exists()
    with fp.open("a", newline="") as f:
        w = csv.writer(f)
        if new_file:
            w.writerow(["timestamp", "site_id", "pm25_pred"])
        w.writerow([ts_iso, site_id, y_pred])


# --------------------------------- schemas -----------------------------------

class PredictRequest(BaseModel):
    site_id: str
    timestamp: str  # ISO 8601


# -------------------------------- endpoints ----------------------------------

@app.get("/health")
def health():
    info = {
        "status": "ok",
        "feast_available": FEAST_AVAILABLE,
        "feast_repo": FEAST_REPO_PATH,
        "model_uri": MODEL_URI,
        "mlflow_tracking": MLFLOW_TRACKING_URI,
        "pred_dir": str(PRED_LOG_DIR),
        "pred_dir_exists": PRED_LOG_DIR.exists(),
        "pred_dir_writable": os.access(PRED_LOG_DIR, os.W_OK),
        "joined_path": ROUTEAQ_JOINED_PATH,
        "s3fs_available": S3FS_AVAILABLE,
        "pyarrow_available": PYARROW_AVAILABLE,
    }
    # Non-fatal quick S3 check
    if S3FS_AVAILABLE and ROUTEAQ_JOINED_PATH.startswith("s3://"):
        try:
            info["s3_sample"] = _list_s3_matches(ROUTEAQ_JOINED_PATH)[:1]
        except Exception as e:  # pragma: no cover
            info["s3_error"] = str(e)
    return info


@app.post("/predict")
def predict(req: PredictRequest):
    """
    Strategy:
      1) Try Feast online features (if installed and available).
         If features are missing/None -> fall back to Parquet.
      2) Fall back to Parquet (S3) if Feast isn't available or fails.
    """
    # Load model (once)
    try:
        model = get_model()
    except Exception as e:
        logger.exception("[error] Failed to load MLflow model")
        raise HTTPException(status_code=503, detail=f"Model load failed: {e}")

    # Attempt Feast first
    feast_error = None
    if FEAST_AVAILABLE:
        try:
            store = get_store()
            feature_refs = [f"aq_hourly:{f}" for f in FEATURE_LIST]
            online = store.get_online_features(
                features=feature_refs,
                entity_rows=[{"site_id": req.site_id}],
                full_feature_names=False,
            ).to_dict()

            # Flatten feast dict & check for None
            row = {k: (online.get(k, [None])[0] if isinstance(online.get(k), list) else online.get(k))
                   for k in FEATURE_LIST}
            if all(v is None for v in row.values()):
                feast_error = "Feast returned all-None features"
            else:
                try:
                    X = _build_X_from_row(row)
                    pred = float(model.predict(X)[0])
                    log_prediction(req.site_id, req.timestamp, pred)
                    return {"site_id": req.site_id, "timestamp": req.timestamp, "pm25_pred": pred, "source": "feast"}
                except Exception as e:
                    feast_error = f"Feast features invalid: {e}"
        except Exception as e:
            feast_error = f"Feast failure: {e}"

    # Parquet fallback
    try:
        row = _latest_row_from_parquet(req.site_id, req.timestamp)
        X = _build_X_from_row(row)
    except Exception as e:
        detail = f"Parquet read failed: {e}"
        if feast_error:
            detail += f" | Feast: {feast_error}"
        logger.exception("[error] %s", detail)
        raise HTTPException(status_code=503, detail=detail)

    try:
        pred = float(model.predict(X)[0])
    except Exception as e:
        logger.exception("[error] model.predict failed")
        raise HTTPException(status_code=503, detail=f"Prediction failed: {e}")

    try:
        log_prediction(req.site_id, req.timestamp, pred)
    except Exception as e:  # pragma: no cover
        logger.warning("[warn] prediction logging failed: %s", e)

    return {"site_id": req.site_id, "timestamp": req.timestamp, "pm25_pred": pred, "source": "parquet"}
