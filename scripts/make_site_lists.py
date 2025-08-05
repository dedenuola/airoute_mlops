"""
Generate data/metadata/site_list.csv
Keeps only Urban Traffic / Urban Background / Rural Background sites
that record PM2.5, NO2 or O3.
"""

import pyreadr, io, requests, pandas as pd, pathlib

# ---------- config ---------------------------------------------------------
RAW_DIR   = pathlib.Path("data/raw")        # not in Git, large files
META_DIR  = pathlib.Path("data/metadata")   # committed to Git
META_DIR.mkdir(parents=True, exist_ok=True)
RAW_DIR.mkdir(parents=True,  exist_ok=True)

RDATA_URL = "https://uk-air.defra.gov.uk/openair/R_data/AURN_metadata.RData"
RDATA_PATH = RAW_DIR / "AURN_metadata.RData"
CSV_OUT    = META_DIR / "site_list.csv"
# ---------------------------------------------------------------------------

# download only if not present (≈ 7 MB)
if not RDATA_PATH.exists():
    print("Downloading metadata …")
    RDATA_PATH.write_bytes(requests.get(RDATA_URL, timeout=60).content)

meta = pyreadr.read_r(RDATA_PATH)["AURN_metadata"]

# normalise column names
meta = meta.rename(columns={
    "Parameter_name":   "parameter_name",
    "local_authority..":"local_authority",
})

# map parameter→pollutant key
poll_map = {
    "PM2.5 particulate matter":                 "pm25",
    "PM2.5 particulate matter (Hourly measured)":"pm25",
    "Nitrogen dioxide":                         "no2",
    "Ozone":                                    "o3",
}
def match(s):
    for k,v in poll_map.items():
        if k.lower() in str(s).lower():
            return v
    return None
meta["pollutant_key"] = meta["parameter_name"].apply(match)

# pivot to flags
pivot = (meta.dropna(subset=["pollutant_key"])
             .assign(flag=1)
             .pivot_table(index="site_id", columns="pollutant_key",
                          values="flag", aggfunc="max", fill_value=0)
             .reset_index())

core = (meta.groupby("site_id")
             .agg({"site_name":"first","location_type":"first",
                   "latitude":"first","longitude":"first"})
             .reset_index())

sites = core.merge(pivot, on="site_id", how="left").fillna(0)

# keep relevant location types
KEPT_TYPES = {"Urban Traffic","Urban Background","Rural Background"}
sites = sites[sites["location_type"].isin(KEPT_TYPES)]
sites = sites[sites[["pm25","no2","o3"]].any(axis=1)]

sites.to_csv(CSV_OUT, index=False)
print("Saved", len(sites), "sites →", CSV_OUT)
