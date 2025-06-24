from datetime import datetime, timedelta
from typing import Any
from airflow import DAG
from airflow.operators.python import PythonOperator
from mypy_boto3_s3 import S3Client
import pandas as pd
import boto3

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

import logging
import os

# Configure loggin
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# LocalStack S3 Configuration
LOCALSTACK_ENDPOINT = os.getenv("S3_ENDPOINT_URL")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION")

# Bucket names
RAW_BUCKET = "youtube-trending-raw"  # Raw dataset storage
PROCESSED_BUCKET = "youtube-trending-processed"  # Processed data
ANALYTICS_BUCKET = "youtube-trending-analytics"  # Analytics results

# DAG Configuration
default_args: dict[str, Any] = {
    "owner": "dang-huu-loc",
    "depends_on_past": False,
    "start_date": datetime(2025, 6, 20),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

dag: DAG = DAG(
    dag_id="youtube_localstack_pipeline",  #
    description="YouTube Trending Analysis with LocalStack S3",
    default_args=default_args,
    schedule="@daily",
    max_active_runs=1,
    tags=["youtube", "etl", "localstack", "analytics"],
    is_paused_upon_creation=False,
)


def create_localstack_s3_client() -> S3Client:
    """Create S3 client for LocalStack"""
    return boto3.client(
        "s3",
        endpoint_url=LOCALSTACK_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_DEFAULT_REGION,
    )


def initialize_localstack(**context):
    """Initialize LocalStack S3 buckets"""
    try:
        s3_client = create_localstack_s3_client()

        # Test connection
        s3_client.list_buckets()
        logger.info("Connected to LocalStack S3")

        # Create buckets if not exist
        buckets = [RAW_BUCKET, PROCESSED_BUCKET, ANALYTICS_BUCKET]

        for bucket in buckets:
            try:
                s3_client.create_bucket(Bucket=bucket)
                logger.info(f"Created bucket: {bucket}")
            except Exception as e:
                if "BucketAlreadyExists" in str(e):
                    logger.info(f"Bucket already exists: {bucket}")
                else:
                    raise e
        # List all buckets to verify
        response = s3_client.list_buckets()
        existing_buckets = [bucket["Name"] for bucket in response["Buckets"]]
        logger.info(f"Available buckets: {existing_buckets}")

        return {
            "status": "success",
            "buckets": existing_buckets,
        }
    except Exception as e:
        logger.error(f"LocalStack initialization failed: {str(e)}")
        raise


def upload_raw_data(**context):
    """Upload raw dataset to LocalStack S3"""
    try:
        s3_client = create_localstack_s3_client()

        # Dataset files
        dataset_path = "/opt/airflow/data/raw"
        countries = ["CA", "DE", "FR", "GB", "IN", "JP", "KR", "MX", "RU", "US"]

        upload_count = 0
        file_info = {}

        for country in countries:
            file_name = f"{country}videos.csv"
            local_path = os.path.join(dataset_path, file_name)

            if os.path.exists(local_path):
                # Upload to S3
                s3_key = f"raw-data/country={country}/{file_name}"
                s3_client.upload_file(local_path, RAW_BUCKET, s3_key)

                # Get file size
                file_size = os.path.getsize(local_path)
                file_info[country] = {
                    "file_name": file_name,
                    "s3_key": s3_key,
                    "file_size_mb": round(file_size / (1024 * 1024), 2),
                }

                upload_count += 1
                logger.info(f"Uploaded {file_name} to s3://{RAW_BUCKET}/{s3_key}")
            else:
                logger.warning(f"File not found: {local_path}")

        summary = {
            "file_uploaded": upload_count,
            "total_countries": len(countries),
            "upload_details": file_info,
            "bucket": RAW_BUCKET,
        }

        logger.info(f"Upload summary: {summary}")
        return summary
    except Exception as e:
        logger.error(f"Raw data upload failed: {str(e)}")
        raise


def validate_s3_data(**context):
    """Enhanced data validation with type checking for LocalStack S3"""
    try:
        s3_client = create_localstack_s3_client()

        # List objects in raw bucket
        response = s3_client.list_objects_v2(Bucket=RAW_BUCKET, Prefix="raw-data/")

        if "Contents" not in response:
            raise ValueError(f"No files found in raw data bucket: {RAW_BUCKET}")

        validation_result = {}
        total_files = 0
        total_size = 0
        data_quality_issues = []

        # Expected columns and their expected types
        expected_columns = {
            "video_id": "string",
            "trending_date": "string",
            "title": "string",
            "channel_title": "string",
            "category_id": "numeric",
            "publish_time": "string",
            "tags": "string",
            "views": "numeric",
            "likes": "numeric",
            "dislikes": "numeric",
            "comment_count": "numeric",
            "thumbnail_link": "string",
            "comments_disabled": "boolean",
            "ratings_disabled": "boolean",
            "video_error_or_removed": "boolean",
            "description": "string",
        }

        for obj in response["Contents"]:
            s3_key = obj["Key"]
            file_size = obj["Size"]

            # Extract country from S3 key
            if "country=" in s3_key:
                country = s3_key.split("country=")[1].split("/")[0]

                # Download and validate file
                temp_file = f"/tmp/{country}_temp.csv"
                s3_client.download_file(RAW_BUCKET, s3_key, temp_file)

                # Read CSV for validation with encoding detection
                df = None
                encodings_to_try = ["utf-8", "latin-1", "cp1252", "iso-8859-1", "utf-16"]

                for encoding in encodings_to_try:
                    try:
                        df = pd.read_csv(temp_file, nrows=1000, encoding=encoding)
                        logger.info(f"Successfully read {country} file with {encoding} encoding")
                        break
                    except UnicodeDecodeError:
                        continue
                    except Exception as e:
                        logger.warning(f"Failed to read {country} with {encoding}: {str(e)}")
                        continue

                if df is None:
                    logger.error(f"Could not read {country} file with any encoding")
                    validation_result[country] = {"s3_key": s3_key, "file_size_bytes": file_size, "error": "Unable to read file with any supported encoding", "encodings_tried": encodings_to_try}
                    data_quality_issues.append(f"{country}: Could not read file with any encoding")
                    continue

                # Column validation
                missing_columns = set(expected_columns.keys()) - set(df.columns)
                extra_columns = set(df.columns) - set(expected_columns.keys())

                # Data type validation
                type_issues = {}
                numeric_columns = ["category_id", "views", "likes", "dislikes", "comment_count"]
                boolean_columns = ["comments_disabled", "ratings_disabled", "video_error_or_removed"]

                # Check numeric columns
                for col in numeric_columns:
                    if col in df.columns:
                        # Try to convert to numeric, count failures
                        try:
                            numeric_values = pd.to_numeric(df[col], errors="coerce")
                            null_count = numeric_values.isnull().sum()
                            if null_count > 0:
                                type_issues[col] = f"{null_count} non-numeric values"
                        except Exception as e:
                            type_issues[col] = f"Cannot convert to numeric: {str(e)}"

                # Check boolean columns
                for col in boolean_columns:
                    if col in df.columns:
                        unique_values = df[col].unique()
                        valid_boolean = {"True", "False", "true", "false", "TRUE", "FALSE", "1", "0", True, False}
                        invalid_values = set(str(v) for v in unique_values if v not in valid_boolean and pd.notna(v))
                        if invalid_values:
                            type_issues[col] = f"Invalid boolean values: {list(invalid_values)}"

                # Data quality checks
                quality_checks = {
                    "duplicate_video_ids": df["video_id"].duplicated().sum(),
                    "empty_titles": df["title"].isnull().sum() + (df["title"] == "").sum(),
                    "empty_channel_titles": df["channel_title"].isnull().sum() + (df["channel_title"] == "").sum(),
                    "negative_views": (pd.to_numeric(df["views"], errors="coerce") < 0).sum() if "views" in df.columns else 0,
                    "negative_likes": (pd.to_numeric(df["likes"], errors="coerce") < 0).sum() if "likes" in df.columns else 0,
                }

                validation_result[country] = {
                    "s3_key": s3_key,
                    "file_size_bytes": file_size,
                    "sample_rows": len(df),
                    "total_columns": len(df.columns),
                    "columns": list(df.columns),
                    "missing_columns": list(missing_columns),
                    "extra_columns": list(extra_columns),
                    "type_issues": type_issues,
                    "quality_checks": quality_checks,
                    "null_counts": df.isnull().sum().to_dict(),
                }

                # Track issues for summary
                if missing_columns:
                    data_quality_issues.append(f"{country}: Missing columns {list(missing_columns)}")
                if extra_columns:
                    data_quality_issues.append(f"{country}: Extra columns {list(extra_columns)}")
                if type_issues:
                    data_quality_issues.append(f"{country}: Type issues {type_issues}")

                total_files += 1
                total_size += file_size

                # Cleanup temp file
                os.remove(temp_file)

                logger.info(f"Validated {country}: {len(df)} sample rows, {len(df.columns)} columns")
                if type_issues:
                    logger.warning(f"{country} has data type issues: {type_issues}")

        summary = {
            "total_files_validated": total_files,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "data_quality_issues": data_quality_issues,
            "validation_details": validation_result,
            "schema_compliance": len(data_quality_issues) == 0,
        }

        logger.info(f"Validation summary: {summary}")

        if data_quality_issues:
            logger.warning("Data quality issues found!")
            for issue in data_quality_issues:
                logger.warning(f"  - {issue}")
        else:
            logger.info("All files passed schema validation!")

        return summary

    except Exception as e:
        logger.error(f"Data validation failed: {str(e)}")
        raise


# Define Airflow tasks

init_task = PythonOperator(
    task_id="initialize_localstack",
    python_callable=initialize_localstack,
    dag=dag,
)

upload_task = PythonOperator(
    task_id="upload_raw_data",
    python_callable=upload_raw_data,
    dag=dag,
)

validate_task = PythonOperator(
    task_id="validate_s3_data",
    python_callable=validate_s3_data,
    dag=dag,
)

process_task = SparkSubmitOperator(
    task_id="process_with_pyspark",
    name="youtube-data-processing",
    application="/opt/airflow/jobs/process_youtube.py",
    conn_id="spark_cluster",
    packages="org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.1026",
    verbose=True,
    dag=dag,
)

analytics_task = SparkSubmitOperator(
    task_id="generate_analytics_report",
    name="youtube-analytics-report",
    application="/opt/airflow/jobs/generate_analytics_report.py",
    conn_id="spark_cluster",
    packages="org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.1026",
    verbose=True,
    dag=dag,
)

# Task dependencies

init_task >> upload_task >> validate_task >> process_task >> analytics_task
