from datetime import datetime
from mypy_boto3_s3 import S3Client
import boto3
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import avg, sum, desc
import logging
import os
import json
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# LocalStack S3 Configuration
LOCALSTACK_ENDPOINT = os.getenv("S3_ENDPOINT_URL")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION")

# Bucket names
RAW_BUCKET = "youtube-trending-raw"
PROCESSED_BUCKET = "youtube-trending-processed"
ANALYTICS_BUCKET = "youtube-trending-analytics"

# Local output directory base
LOCAL_OUTPUT_BASE = "data/processed"


def create_localstack_s3_client() -> S3Client:
    """Create S3 client for LocalStack"""
    return boto3.client(
        "s3",
        endpoint_url=LOCALSTACK_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_DEFAULT_REGION,
    )


def get_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("YouTubeTrendAnalysis_LocalStack")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT_URL"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def ensure_local_directory(directory: str):
    """Create local directory if it doesn't exist"""
    if not os.path.exists(directory):
        os.makedirs(directory)
        logger.info(f"Created local directory: {directory}")


def get_daily_timestamp() -> tuple[str, str]:
    """Get current timestamp in milliseconds and formatted date"""
    current_time = datetime.now()
    timestamp_ms = int(time.time() * 1000)
    formatted_date = current_time.strftime("%Y-%m-%d")
    return str(timestamp_ms), formatted_date


def save_dataframe_to_formats(df: DataFrame, base_name: str, local_dir: str, s3_client: S3Client, bucket: str, s3_prefix: str, timestamp_ms: str):
    """Save DataFrame to both CSV and JSON formats locally and S3"""
    # Convert DataFrame to Pandas for easier manipulation
    pandas_df = df.toPandas()

    # Generate filenames with timestamp
    csv_filename = f"daily_{base_name}_{timestamp_ms}.csv"
    json_filename = f"daily_{base_name}_{timestamp_ms}.json"

    # Local paths
    csv_local_path = os.path.join(local_dir, csv_filename)
    json_local_path = os.path.join(local_dir, json_filename)

    # S3 keys
    csv_s3_key = f"{s3_prefix}/{csv_filename}"
    json_s3_key = f"{s3_prefix}/{json_filename}"

    # Save CSV
    pandas_df.to_csv(csv_local_path, index=False, encoding="utf-8")
    with open(csv_local_path, "r", encoding="utf-8") as f:
        csv_content = f.read()
    s3_client.put_object(Bucket=bucket, Key=csv_s3_key, Body=csv_content, ContentType="text/csv")
    logger.info(f"Saved CSV: {csv_local_path} -> s3://{bucket}/{csv_s3_key}")

    # Save JSON
    json_data = pandas_df.to_json(orient="records", indent=2, force_ascii=False)
    with open(json_local_path, "w", encoding="utf-8") as f:
        f.write(json_data)
    s3_client.put_object(Bucket=bucket, Key=json_s3_key, Body=json_data, ContentType="application/json")
    logger.info(f"Saved JSON: {json_local_path} -> s3://{bucket}/{json_s3_key}")

    return csv_filename, json_filename


def save_to_local_and_s3(content: str, local_path: str, s3_client: S3Client, bucket: str, s3_key: str, content_type: str = "text/plain"):
    """Save content to both local file and S3"""
    # Save to local file
    with open(local_path, "w", encoding="utf-8") as f:
        f.write(content)
    logger.info(f"Saved to local file: {local_path}")

    # Save to S3
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=content,
        ContentType=content_type,
    )
    logger.info(f"Saved to S3: s3://{bucket}/{s3_key}")


def main():
    """Generate comprehensive analytics report with daily structure"""
    try:
        # Get timestamp for this run
        timestamp_ms, formatted_date = get_daily_timestamp()

        # Create daily directory structure
        daily_dir = os.path.join(LOCAL_OUTPUT_BASE, "daily", timestamp_ms)
        ensure_local_directory(daily_dir)

        s3_client = create_localstack_s3_client()
        spark: SparkSession = get_spark_session()

        # Read processed data
        base_path = f"s3a://{PROCESSED_BUCKET}"
        category_df = spark.read.parquet(f"{base_path}/category_analysis")
        channel_df = spark.read.parquet(f"{base_path}/channel_analysis")

        # Generate insights
        insights = {}

        # Top performing categories by country
        top_categories_df = category_df.orderBy(desc("avg_views")).limit(10)
        top_categories = top_categories_df.collect()

        insights["top_categories"] = [
            {
                "country": row["country"],
                "category_id": row["category_id"],
                "category_title": row["category_title"],
                "avg_views": float(row["avg_views"]),
                "video_count": int(row["video_count"]),
            }
            for row in top_categories
        ]

        # Top channels globally
        top_channels_df = channel_df.orderBy(desc("avg_views")).limit(15)
        top_channels = top_channels_df.collect()

        insights["top_channels"] = [
            {
                "channel": row["channel_title"],
                "country": row["country"],
                "avg_views": float(row["avg_views"] or 0),
                "trending_count": int(row["trending_count"]),
            }
            for row in top_channels
        ]

        # Country performance
        country_stats_df = category_df.groupBy("country").agg(
            sum("total_views").alias("country_total_views"),
            avg("avg_engagement_rate").alias("country_avg_engagement"),
        )
        country_stats = country_stats_df.collect()

        insights["country_performance"] = [
            {
                "country": row["country"],
                "total_views": int(row["country_total_views"]),
                "avg_engagement": float(row["country_avg_engagement"]),
            }
            for row in country_stats
        ]

        # Save DataFrames to CSV and JSON formats
        s3_daily_prefix = f"daily/{timestamp_ms}"

        # Save top categories data
        categories_csv, categories_json = save_dataframe_to_formats(top_categories_df, "top_categories", daily_dir, s3_client, ANALYTICS_BUCKET, s3_daily_prefix, timestamp_ms)

        # Save top channels data
        channels_csv, channels_json = save_dataframe_to_formats(top_channels_df, "top_channels", daily_dir, s3_client, ANALYTICS_BUCKET, s3_daily_prefix, timestamp_ms)

        # Save country performance data
        country_csv, country_json = save_dataframe_to_formats(country_stats_df, "country_performance", daily_dir, s3_client, ANALYTICS_BUCKET, s3_daily_prefix, timestamp_ms)

        # Save comprehensive insights as JSON
        insights_json = json.dumps(insights, indent=2, default=str, ensure_ascii=False)
        insights_filename = f"daily_insights_{timestamp_ms}.json"
        insights_local_path = os.path.join(daily_dir, insights_filename)
        insights_s3_key = f"{s3_daily_prefix}/{insights_filename}"

        save_to_local_and_s3(insights_json, insights_local_path, s3_client, ANALYTICS_BUCKET, insights_s3_key, "application/json")

        # Generate summary report with date annotation
        summary_report = f"""# YouTube Trending Analysis Report
Generated: {datetime.now().isoformat()}
Report Date: {formatted_date}
Timestamp: {timestamp_ms}

## Key Metrics
- Top Categories: {len(insights["top_categories"])} analyzed
- Top Channels: {len(insights["top_channels"])} identified  
- Countries: {len(insights["country_performance"])} markets analyzed

## Top Performing Category
{insights["top_categories"][0]["country"]} - Category {insights["top_categories"][0]["category_id"]} ({insights["top_categories"][0]["category_title"]})
Average Views: {insights["top_categories"][0]["avg_views"]:,.0f}

## Best Performing Channel
{insights["top_channels"][0]["channel"]} ({insights["top_channels"][0]["country"]})
Average Views: {insights["top_channels"][0]["avg_views"]:,.0f}
Trending Videos: {insights["top_channels"][0]["trending_count"]}

## Market Leaders by Total Views
"""
        for country in insights["country_performance"][:5]:
            summary_report += f"- {country['country']}: {country['total_views']:,.0f} total views\n"

        # Save summary report to daily directory
        report_filename = f"daily_summary_report_{timestamp_ms}.md"
        report_local_path = os.path.join(daily_dir, report_filename)
        report_s3_key = f"{s3_daily_prefix}/{report_filename}"

        save_to_local_and_s3(summary_report, report_local_path, s3_client, ANALYTICS_BUCKET, report_s3_key, "text/markdown")

        spark.stop()

        result = {
            "timestamp": timestamp_ms,
            "report_date": formatted_date,
            "insights_generated": len(insights),
            "files_created": {"dataframes": [categories_csv, categories_json, channels_csv, channels_json, country_csv, country_json], "reports": [insights_filename, report_filename]},
            "local_output_dir": daily_dir,
            "s3_prefix": s3_daily_prefix,
            "analytics_bucket": ANALYTICS_BUCKET,
            "top_category": insights["top_categories"][0],
            "top_channel": insights["top_channels"][0],
        }

        logger.info(f"Analytics report generated: {result}")
        logger.info(f"Local files saved to: {os.path.abspath(daily_dir)}")
        return result

    except Exception as e:
        logger.error(f"Analytics report generation failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()
