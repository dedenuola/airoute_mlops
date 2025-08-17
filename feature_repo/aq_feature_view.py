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

# 2) Offline source URI (prefer a directory/glob; keep the scheme EXACT, e.g., "s3://")
# Examples:
#   export ROUTEAQ_JOINED_PATH="s3://routeaq-feast-offline/silver/joined/*.parquet"
#   export ROUTEAQ_JOINED_PATH="/opt/airflow/data/silver/joined/*.parquet"
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







# # feature_repo/aq_feature_view.py

# from datetime import timedelta
# import os
# from feast import Entity, FileSource, FeatureView, Field
# from feast.types import Float32
# from feast.value_type import ValueType
# from feast.infra.offline_stores.file_source import FileOptions, ParquetFormat

# # 1. Define your entity
# station = Entity(name="site_id", join_keys=["site_id"], value_type=ValueType.STRING)


# # Use a directory or glob (not a single file)
# # JOINED_PATH = os.getenv(
# #     "ROUTEAQ_JOINED_PATH",
# #     "s3://routeaq-feast-offline/silver/joined/",   # <-- directory rather than ...parquet
# #     # Or: "s3://routeaq-feast-offline/silver/joined/*.parquet"
# # )
# JOINED_PATH = os.getenv(
#     "ROUTEAQ_JOINED_PATH",
#     "s3://routeaq-feast-offline/silver/joined/*.parquet",
# )

# # Use FileOptions + ParquetFormat to avoid repo-path prefixing
# # src = FileSource(
# #     timestamp_field="date_time",
# #     file_options=FileOptions(
# #         uri=JOINED_PATH,
# #         format=ParquetFormat(),       # ← correct for Feast 0.47
# #         s3_endpoint_override=None,    # ← leave None unless using a custom S3 endpoint
# #     ),
# # )
# src = FileSource(path=JOINED_PATH, timestamp_field="date_time")


# # # 2. Point to the filtered joined Parquet
# # src = FileSource(
# #     path="/workspaces/airoute_mlops/airoute_mlops/data/silver/joined/hourly_joined_2025_from_28jul.parquet",
# #     timestamp_field="date_time",
# # )



# # 3. Define the FeatureView with underscore column names
# aq_hourly = FeatureView(
#     name="aq_hourly",
#     entities=[station],
#     ttl=timedelta(days=2),
#     schema=[
#         Field(name="pm25_t_1",   dtype=Float32),
#         Field(name="no2_t_1",    dtype=Float32),
#         Field(name="o3_t_1",     dtype=Float32),
#         Field(name="temp",       dtype=Float32),
#         Field(name="wind",       dtype=Float32),
#         Field(name="humidity",   dtype=Float32),
#     ],
#     source=src,
#     online=True,
# )
