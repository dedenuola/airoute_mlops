# feature_repo/aq_feature_view.py
# Feast 0.47.x-compatible definition for RouteAQ hourly features.

from datetime import timedelta
import os

from feast import Entity, FeatureView, Field, FileSource
from feast.types import Float32
from feast.value_type import ValueType
from feast.infra.offline_stores.file_source import ParquetFormat

# 1) Entity (set value_type to silence the deprecation warning)
station = Entity(
    name="site_id",
    join_keys=["site_id"],
    value_type=ValueType.STRING,
)

JOINED_PATH = os.getenv(
    "ROUTEAQ_JOINED_PATH",
    "s3://routeaq-feast-offline/silver/joined/*.parquet",
)

# IMPORTANT: Do NOT wrap the URI with pathlib.Path; pass the plain string.
src = FileSource(
    path=JOINED_PATH,
    timestamp_field="date_time",
    file_format=ParquetFormat(),
)

# 3) Feature view definition
aq_hourly = FeatureView(
    name="aq_hourly",
    entities=[station],
    ttl=timedelta(days=2),
    schema=[
        Field(name="pm25_t_1", dtype=Float32),
        Field(name="no2_t_1",  dtype=Float32),
        Field(name="o3_t_1",   dtype=Float32),
        Field(name="temp",     dtype=Float32),
        Field(name="wind",     dtype=Float32),
        Field(name="humidity", dtype=Float32),
    ],
    source=src,
    online=True,
)