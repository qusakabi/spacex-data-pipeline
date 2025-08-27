import requests
import json
from pathlib import Path
import boto3
import logging
from botocore.exceptions import NoCredentialsError
from etl.config.settings import MINIO
from etl.config.config import BUCKET_NAME

logger = logging.getLogger(__name__)
def extract_to_minio(path: str):
    # Fetch data from SpaceX API
    url = "https://api.spacexdata.com/v4/launches"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    # Save temporarily to local file
    temp_file = "/tmp/spacex_raw.json"
    Path(temp_file).parent.mkdir(parents=True, exist_ok=True)
    with open(temp_file, "w") as f:
        json.dump(data, f)

    # Upload to MinIO (using centralized settings)
    bucket_name = BUCKET_NAME

    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO.endpoint_url,
        aws_access_key_id=MINIO.access_key,
        aws_secret_access_key=MINIO.secret_key,
    )

    try:
        # Create bucket if it doesn't exist
        try:
            s3_client.head_bucket(Bucket=bucket_name)
        except Exception:
            s3_client.create_bucket(Bucket=bucket_name)

        # Upload file
        s3_client.upload_file(temp_file, bucket_name, path)
        logger.info(f"Successfully uploaded %s to MinIO bucket %s", path, bucket_name)
        return True
    except FileNotFoundError:
        logger.error("File not found: %s", temp_file, exc_info=True)
        return False
    except NoCredentialsError:
        logger.error("Credentials not available for MinIO", exc_info=True)
        return False
    except Exception as e:
        logger.error("Error uploading to MinIO: %s", e, exc_info=True)
        return False
