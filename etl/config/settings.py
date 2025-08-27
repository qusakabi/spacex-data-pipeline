"""
Centralized configuration for ETL components (MinIO, Snowflake).
Loads values from environment variables to avoid hardcoded secrets.
"""

from dataclasses import dataclass
from typing import Optional, Dict
import os


@dataclass(frozen=True)
class MinIOSettings:
    endpoint_url: str
    access_key: str
    secret_key: str
    bucket_name: str


@dataclass(frozen=True)
class SnowflakeSettings:
    account: str
    user: str
    password: str
    warehouse: str
    database: str
    schema: str
    role: Optional[str] = None

    def connector_kwargs(self) -> Dict[str, str]:
        kwargs = {
            "account": self.account,
            "user": self.user,
            "password": self.password,
            "warehouse": self.warehouse,
            "database": self.database,
            "schema": self.schema,
        }
        if self.role:
            kwargs["role"] = self.role
        return kwargs

    def sqlalchemy_url(self) -> str:
        # snowflake-sqlalchemy URL form
        role_part = f"&role={self.role}" if self.role else ""
        return (
            f"snowflake://{self.user}:{self.password}@{self.account}/"
            f"{self.database}/{self.schema}?warehouse={self.warehouse}{role_part}"
        )


def load_minio_settings() -> MinIOSettings:
    return MinIOSettings(
        endpoint_url=os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        bucket_name=os.getenv("MINIO_BUCKET_NAME", "spacex-data"),
    )


def load_snowflake_settings() -> SnowflakeSettings:
    return SnowflakeSettings(
        account=os.getenv("SNOWFLAKE_ACCOUNT", ""),
        user=os.getenv("SNOWFLAKE_USER", ""),
        password=os.getenv("SNOWFLAKE_PASSWORD", ""),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", ""),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
        role=os.getenv("SNOWFLAKE_ROLE", None),
    )


# Eagerly-loaded singletons for convenience
MINIO: MinIOSettings = load_minio_settings()
SNOWFLAKE: SnowflakeSettings = load_snowflake_settings()

__all__ = [
    "MinIOSettings",
    "SnowflakeSettings",
    "MINIO",
    "SNOWFLAKE",
    "load_minio_settings",
    "load_snowflake_settings",
]