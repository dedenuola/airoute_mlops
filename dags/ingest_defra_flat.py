"""
DAG: ingest_defra_flat
────────────────────────────────────────────────────────
Task 1  fetch_defra_flat   ➜  bronze/defra/*.csv
        ↳ active_sites.parquet  (site_id, site_name, latitude, longitude…)

Task 2  fetch_metoffice    ➜  bronze/met/*.json
        reads active_sites.parquet
"""
import io, os, re, html, time, pathlib, datetime as dt
import requests, pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta

# ────────── fixed paths ──────────────────────────────────────────────
BRONZE_DIR   = pathlib.Path("/opt/airflow/data/bronze/defra")
WEATHER_DIR  = pathlib.Path("/opt/airflow/data/bronze/met")
META_DIR     = pathlib.Path("/opt/airflow/data/metadata")

SITE_LIST_CSV   = META_DIR / "site_list.csv"
ACTIVE_PARQUET  = META_DIR / "active_sites.parquet"
NOTFOUND_CSV    = META_DIR / "site_list_notfound.csv"

TARGETS = {
    "date", "time",
    "ozone",
    "nitrogen dioxide",
    "pm2.5 particulate matter (hourly measured)",
}

# ────────── helpers ──────────────────────────────────────────────────
def normalise(col: str) -> str:
    col = html.unescape(col)
    return re.sub(r"</?sub>", "", col).lower().strip()

def fetch_raw_csv(site_id: str, year: int, max_suffix=5):
    base = "https://uk-air.defra.gov.uk/datastore/data_files/site_data/{}_{}.csv"
    for suf in [""] + list(map(str, range(1, max_suffix + 1))):
        sid = f"{site_id}{suf}"
        r   = requests.get(base.format(sid, year), timeout=60)
        if r.ok:
            if suf:
                print(f"Using {sid} for {site_id}")
            return sid, r.text
    return None, None

def parse_and_save(site_id: str, text: str, year: int):
    lines = text.splitlines()
    hdr   = next(i for i, l in enumerate(lines) if l.lower().startswith("date,time"))
    df    = pd.read_csv(io.StringIO("\n".join(lines[hdr:])))
    df.columns = [normalise(c) for c in df.columns]
    df = df[[c for c in df.columns if c in TARGETS]]

    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(BRONZE_DIR / f"{site_id}_{year}.csv", index=False)
    return True

def parse_one_site(site_id, year):
    sid, text = fetch_raw_csv(site_id, year)
    if not text:
        return False
    try:
        return parse_and_save(sid, text, year)
    except Exception as e:
        print(f"{site_id}_{year}: parse error {e}")
        return False

# ────────── Task 1  DEFRA ingestion ──────────────────────────────────
def fetch_defra_flat(ds, **_):
    year = dt.datetime.strptime(ds, "%Y-%m-%d").year

    meta = pd.read_csv(SITE_LIST_CSV)
    meta.columns = meta.columns.str.lower()          # ensure latitude/longitude lower‑case

    good, bad = [], []
    for site_id in meta["site_id"]:
        (good if parse_one_site(site_id, year) else bad).append(site_id)

    # write diagnostics
    pd.DataFrame({"site_id": bad}).to_csv(NOTFOUND_CSV, index=False)

    # store active sites with full metadata
    active_df = meta[meta["site_id"].isin(good)]
    active_df.to_parquet(ACTIVE_PARQUET, index=False)
    print(f"[DEFRA] {len(good)} saved, {len(bad)} not found ({year})")

# ────────── Task 2  Met‑Office ingestion ─────────────────────────────
def fetch_metoffice(ds, **_):
    exec_date = dt.datetime.strptime(ds, "%Y-%m-%d")
    active = pd.read_parquet(ACTIVE_PARQUET)  # contains latitude & longitude

    WEATHER_DIR.mkdir(parents=True, exist_ok=True)

    headers = {
        "apikey": os.environ["METOFFICE_API_KEY"],
        "Accept": "application/json"
    }

    base = "https://data.hub.api.metoffice.gov.uk/sitespecific/v0/point/hourly"

    for _, row in active.iterrows():
        sid, lat, lon = row["site_id"], row["latitude"], row["longitude"]
        url = (
            f"{base}"
            f"?latitude={lat}&longitude={lon}"
            f"&includeLocationName=true"
            f"&excludeParameterMetadata=true"
            f"&dataSource=BD1"
        )

        try:
            r = requests.get(url, headers=headers, timeout=30)
            if r.ok:
                output_path = WEATHER_DIR / f"{sid}_{exec_date:%Y-%m-%d}.json"
                output_path.write_text(r.text)
                print(f"[OK] {sid} written")
            else:
                print(f"[FAIL] {sid}: {r.status_code} {r.reason}")
        except Exception as e:
            print(f"[ERROR] {sid}: {e}")

        time.sleep(0.2)  # stay under 360 calls/hour

# ────────── DAG definition ───────────────────────────────────────────
default_args = dict(
    owner="airflow",
    retries=1,
    retry_delay=timedelta(minutes=3),
)

with DAG(
    dag_id="ingest_defra_flat",
    default_args=default_args,
    schedule="@daily",            # change to "@hourly" later if needed
    start_date=dt.datetime(2025, 6, 1),
    catchup=True,
    max_active_runs=1,
    tags=["ingestion", "defra"],
) as dag:

    defra_task = PythonOperator(
        task_id="fetch_defra_flat",
        python_callable=fetch_defra_flat,
    )

    met_task = PythonOperator(
        task_id="fetch_metoffice",
        python_callable=fetch_metoffice,
    )

    defra_task >> met_task
