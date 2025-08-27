#!/usr/bin/env python3
"""
Script to initialize MinIO bucket for SpaceX ETL pipeline
"""

import boto3
import logging
from botocore.exceptions import ClientError
from etl.config import MINIO
from etl.config.config import BUCKET_NAME

logger = logging.getLogger(__name__)

def init_minio():
    """Initialize MinIO bucket"""
    
    # MinIO settings from centralized config
    bucket_name = BUCKET_NAME
    
    # Create S3 client
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO.endpoint_url,
        aws_access_key_id=MINIO.access_key,
        aws_secret_access_key=MINIO.secret_key
    )
    
    try:
        # Create bucket
        s3_client.create_bucket(Bucket=bucket_name)
        logger.info("Successfully created bucket: %s", bucket_name)
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'BucketAlreadyExists':
            logger.info("Bucket %s already exists", bucket_name)
        elif error_code == 'BucketAlreadyOwnedByYou':
            logger.info("Bucket %s already owned by you", bucket_name)
        else:
            logger.error("Error creating bucket: %s", e, exc_info=True)
            return False
    
    return True

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    init_minio()