"""
YouTube Trend Analysis with LocalStack S3
Notebook for data exploration and visualization
"""

from textwrap import indent
import boto3
from pyspark.sql import SparkSession
import os

import warnings

# Type import
from mypy_boto3_s3 import S3Client

warnings.filterwarnings("ignore")

# LocalStack S3 Configuration
LOCALSTACK_ENDPOINT = os.getenv("S3_ENDPOINT_URL")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION")

# Bucket names
RAW_BUCKET = os.getenv("RAW_BUCKET")  # Raw dataset storage
PROCESSED_BUCKET = os.getenv("PROCESSED_BUCKET")  # Processed data
ANALYTICS_BUCKET = os.getenv("ANALYTICS_BUCKET")  # Analytics results


def create_s3_client() -> S3Client:
    """Create S3 client for LocalStack"""
    return boto3.client("s3", endpoint_url=LOCALSTACK_ENDPOINT, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=AWS_DEFAULT_REGION)


def create_spark_session() -> SparkSession:
    """Create Spark session with LocalStack S3 support"""
    return (
        SparkSession.builder.appName("YouTubeAnalysis_Notebook")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.endpoint", LOCALSTACK_ENDPOINT)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def export_dataframes_to_local(category_pd, channel_pd, daily_pd, output_dir="data/processed"):
    """Save DataFrames to CSV and JSON locally"""
    os.makedirs(output_dir, exist_ok=True)
    try:
        category_pd.to_csv(f"{output_dir}/category.csv", index=False)
        category_pd.to_json(f"{output_dir}/category.json", orient="records", indent=2)

        channel_pd.to_csv(f"{output_dir}/channel.csv", index=False)
        channel_pd.to_json(f"{output_dir}/channel.json", orient="records", indent=2)

        daily_pd.to_csv(f"{output_dir}/daily.csv", index=False)
        daily_pd.to_json(f"{output_dir}/daily.json", orient="records", indent=2)

        print(f"Saved CSV and JSON to '{output_dir}'")
    except Exception as e:
        print(f"Failed to export data: {e}")


def main():
    # Initialize connections
    print("Connecting to LocalStack S3...")
    s3_client = create_s3_client()

    print("Initializing Spark Session...")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    # Test S3 connection
    try:
        buckets = s3_client.list_buckets()
        print("Connected to LocalStack S3")
        print(f"Available buckets: {[b['Name'] for b in buckets['Buckets']]}")
    except Exception as e:
        print(f"Failed to connect to LocalStack: {e}")
        return

    # Load processed data from S3
    print("Loading processed data from S3...")
    try:
        category_df = spark.read.parquet(f"s3a://{PROCESSED_BUCKET}/category_analysis")
        channel_df = spark.read.parquet(f"s3a://{PROCESSED_BUCKET}/channel_analysis")
        daily_df = spark.read.parquet(f"s3a://{PROCESSED_BUCKET}/daily_trends")

        print(f"Category analysis: {category_df.count()} records")
        print(f"Channel analysis: {channel_df.count()} records")
        print(f"Daily trends: {daily_df.count()} records")
    except Exception as e:
        print(f"Error loading data: {e}")
        print("Make sure the Airflow pipeline has run successfully. Please check the DAG at http://localhost:8080")
        return

    # Convert to Pandas
    print("Converting to Pandas DataFrames...")
    category_pd = category_df.toPandas()
    channel_pd = channel_df.toPandas()
    daily_pd = daily_df.toPandas()

    print("Preview data:")
    print("-" * 60)
    print("Category:")
    print(category_pd.head())
    print("-" * 60)
    print("Channel:")
    print(channel_pd.head())
    print("-" * 60)
    print("Daily:")
    print(daily_pd.head())
    print("-" * 60)

    # Export to local files
    export_dataframes_to_local(category_pd, channel_pd, daily_pd)


if __name__ == "__main__":
    main()
