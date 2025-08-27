"""
Centralized constants for ETL components.
"""

import os
from .settings import MINIO, SNOWFLAKE

# MinIO bucket name used across extract/transform
BUCKET_NAME: str = MINIO.bucket_name

# Default parquet output path used by DAG to pass into transform()
PARQUET_PATH: str = os.getenv("PARQUET_PATH", "/opt/airflow/etl/data/parquet")

# Expose Snowflake schema as a constant for convenience
SNOWFLAKE_SCHEMA: str = SNOWFLAKE.schema

__all__ = ["BUCKET_NAME", "PARQUET_PATH", "SNOWFLAKE_SCHEMA"]