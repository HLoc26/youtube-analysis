"""
Localstack S3 Initialization Script
Creates buckets and upload initial data
"""

import boto3
import os
import logging
from botocore.exceptions import ClientError
from mypy_boto3_s3 import S3Client

from dotenv import load_dotenv

load_dotenv()


# Configure logging
logging.basicConfig(level=logging.INFO)
logger: logging.Logger = logging.getLogger(__name__)


for var in ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_DEFAULT_REGION"]:
    if not os.getenv(var):
        logger.warning(f"Environment variable {var} is not set")


def create_s3_client() -> S3Client:
    """Create S3 client for LocalStack"""
    return boto3.client(
        "s3",
        endpoint_url="http://localhost:4566",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION"),
    )


def create_buckets(s3_client: S3Client, bucket_names: list[str]):
    """Create S3 buckets in LocalStack"""
    for bucket_name in bucket_names:
        try:
            s3_client.create_bucket(Bucket=bucket_name)
            logger.info(f"Created bucket: {bucket_name}")

            # Enable bucket versioning
            s3_client.put_bucket_versioning(Bucket=bucket_name, VersioningConfiguration={"Status": "Enabled"})
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "BucketAlreadyExists":
                logger.info(f"Bucket already exists: {bucket_name}")
            else:
                logger.error(f"Error creating bucket {bucket_name}: {e}")


def upload_dataset(s3_client: S3Client, bucket_name: str, local_path: str = "./data/raw", s3_prefix: str = "raw-data"):
    """Upload YouTube dataset files to S3"""
    try:
        data_set_files = [f for f in os.listdir(local_path) if os.path.isfile(os.path.join(local_path, f))]

        uploaded_count = 0
        for file_name in data_set_files:
            if "example" in file_name or "README" in file_name:
                continue
            file_path = os.path.join(local_path, file_name)
            s3_key = f"{s3_prefix}/{file_name}"
            s3_client.upload_file(file_path, bucket_name, s3_key)
            logger.info(f"Uploaded {file_name} -> s3://{bucket_name}/{s3_key}")
            uploaded_count += 1

        if uploaded_count < 20:
            logger.warning(f"Total files uploaded: {uploaded_count}, which is less than expected dataset (20).")
        else:
            logger.info(f"Total files uploaded: {uploaded_count}")
    except Exception as e:
        logger.error(f"Error loading dataset: {e}")
        return 0


def setup_bucket_policies(s3_client: S3Client, bucket_name: str):
    """Setup bucket policies for access control"""
    policy: dict = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowPublicRead",
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": f"arn:aws:s3:::{bucket_name}/public/*",
            }
        ],
    }

    try:
        import json

        s3_client.put_bucket_policy(Bucket=bucket_name, Policy=json.dumps(policy))
        logger.info(f"Set bucket policy for: {bucket_name}")
    except Exception as e:
        logger.warning(f"Could not set bucket policy for {bucket_name}: {e}")


def main():
    """Main init function"""
    logger.info("Starting LocalStack S3 initialization...")

    # Create S3 Client
    s3_client = create_s3_client()

    # Test connection
    try:
        s3_client.list_buckets()
        logger.info("Connected to LocalStack S3")
    except Exception as e:
        logger.error(f"Failed to connect to LocalStack: {e}")

    # Create bucket
    bucket_names: dict[str, str] = {
        "raw": "youtube-trending-raw",  # Raw dataset storage
        "processed": "youtube-trending-processed",  # Processed data
        "analytics": "youtube-trending-analytics",  # Analytics results
        "reports": "youtube-trending-reports",  # Generated reports
    }

    create_buckets(s3_client=s3_client, bucket_names=list(bucket_names.values()))

    for bucket in list(bucket_names.values()):
        setup_bucket_policies(s3_client=s3_client, bucket_name=bucket)

    # Upload data set
    dataset_path_raw = "./data/raw"

    if os.path.exists(dataset_path_raw):
        upload_dataset(s3_client, bucket_name=bucket_names["raw"], local_path=dataset_path_raw)
    else:
        logger.warning(f"Dataset path not found: {dataset_path_raw}")

    logger.info("LocalStack S3 initialization completed!")

    return True


if __name__ == "__main__":
    main()
    # local_path = "/opt/airflow/data/raw"
    # data_set_files = [f for f in os.listdir(local_path) if os.path.isfile(os.path.join(local_path, f))]
    # print(data_set_files)
