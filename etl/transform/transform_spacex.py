import boto3
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
import logging
from etl.config import MINIO
from etl.config.config import BUCKET_NAME

logger = logging.getLogger(__name__)

def transform(input_path: str, output_path: str):
    """
    Stable local Spark execution inside Airflow task process.

    Why this shape:
    - On Airflow retries or multiple runs in the same worker process, a stale active SparkSession
      in the embedded JVM can cause errors like:
        • java.lang.IllegalStateException: LiveListenerBus is stopped
        • Cannot call methods on a stopped SparkContext
      To prevent reusing a stopped SparkContext, we explicitly stop any active session
      before creating a fresh one, and always stop the session in finally.
    - We run Spark in local[*] to avoid external cluster lifecycle and driver callback issues.
    """

    # 1) Download the file from MinIO to local temp storage
    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO.endpoint_url,
        aws_access_key_id=MINIO.access_key,
        aws_secret_access_key=MINIO.secret_key,
    )
    bucket_name = BUCKET_NAME

    temp_file = "/tmp/spacex_raw.json"
    Path(temp_file).parent.mkdir(parents=True, exist_ok=True)

    s3_client.download_file(bucket_name, input_path, temp_file)
    logger.info("[Transform] Downloaded %s from MinIO bucket %s", input_path, bucket_name)

    # 2) Create a fresh SparkSession safely and read/process/write
    spark = None
    try:
        # Defensive: if there is an active session lingering in the JVM, stop it
        try:
            active = SparkSession.getActiveSession()
            if active is not None:
                logger.info("[Transform] Stopping previously active SparkSession to avoid reuse of stopped context")
                active.stop()
        except Exception:
            # Best effort; ignore if querying active session fails
            pass

        # Fresh session, UI off to reduce overhead in containers
        spark = (
            SparkSession.builder
            .appName("TransformSpaceX")
            .master("local[*]")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )

        df = spark.read.json(temp_file)

        df_clean = df.select(
            col("id"),
            col("name"),
            to_timestamp(col("date_utc")).alias("date_utc"),
            col("success"),
            col("rocket"),
        ).dropna(subset=["id", "name", "date_utc"])

        df_clean.write.mode("overwrite").parquet(output_path)
        logger.info("[Transform] Wrote parquet to: %s", output_path)

    finally:
        if spark is not None:
            try:
                spark.stop()
                logger.info("[Transform] SparkSession stopped")
            except Exception:
                # Do not surface stop errors as task failures
                pass
