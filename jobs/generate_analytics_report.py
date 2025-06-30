from datetime import datetime
from mypy_boto3_s3 import S3Client
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum, desc

# from pyspark.sql.types import
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


def main():
    """Generate comprehensive analytics report"""
    try:
        s3_client = create_localstack_s3_client()

        # Initialize Spark to read processed data
        spark: SparkSession = get_spark_session()

        # Read processed data
        base_path = f"s3a://{PROCESSED_BUCKET}"

        category_df = spark.read.parquet(f"{base_path}/category_analysis")
        channel_df = spark.read.parquet(f"{base_path}/channel_analysis")
        # daily_df = spark.read.parquet(f"{base_path}/daily_trends")

        # Generate insight
        insights = {}

        # Top performing categories by country
        top_categories = category_df.orderBy(desc("avg_views")).limit(10).collect()

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
        top_channels = channel_df.orderBy(desc("avg_views")).limit(15).collect()

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
        country_stats = (
            category_df.groupBy("country")
            .agg(
                sum("total_views").alias("country_total_views"),  #
                avg("avg_engagement_rate").alias("country_avg_engagement"),
            )
            .collect()
        )

        insights["country_performance"] = [
            {
                "country": row["country"],
                "total_views": int(row["country_total_views"]),
                "avg_engagement": float(row["country_avg_engagement"]),
            }
            for row in country_stats
        ]

        # Save insight as JSON
        import json

        insights_json = json.dumps(insights, indent=2, default=str)

        # Upload insight to S3
        insights_key = "reports/daily_insights.json"
        s3_client.put_object(
            Bucket=ANALYTICS_BUCKET,
            Key=insights_key,
            Body=insights_json,
            ContentType="application/json",
        )

        # Generate summary report
        summary_report = f"""
        # YouTube Trending Analysis Report
        Generated: {datetime.now().isoformat()}
        ## Key Metrics
        - Top Categories: {len(insights["top_categories"])} analyzed
        - Top Channels: {len(insights["top_channels"])} identified
        - Countries: {len(insights["country_performance"])} markets analyzed
        ## Top Performing Category
        {insights["top_categories"][0]["country"]} - Category {insights["top_categories"][0]["category_id"]}
        Average Views: {insights["top_categories"][0]["avg_views"]:,.0f}
        ## Best Performing Channel
        {insights["top_channels"][0]["channel"]} ({insights["top_channels"][0]["country"]})
        Average Views: {insights["top_channels"][0]["avg_views"]:,.0f}
        Trending Videos: {insights["top_channels"][0]["trending_count"]}
        ## Market Leaders by Total Views
        """
        for country in insights["country_performance"][:5]:
            summary_report += f"- {country['country']}: {country['total_views']:,.0f} total views\n"

        # Upload summary report
        report_key = "reports/summary_report.md"
        s3_client.put_object(
            Bucket=ANALYTICS_BUCKET,
            Key=report_key,
            Body=summary_report,
            ContentType="text/markdown",
        )

        spark.stop()

        result = {
            "insight_generated": len(insights),
            "reports_created": ["daily_insights.json", "summary_report.md"],
            "analytics_bucket": ANALYTICS_BUCKET,
            "top_category": insights["top_categories"][0],
            "top_channel": insights["top_channels"][0],
        }

        logger.info(f"Analytics report generated: {result}")
        return result

    except Exception as e:
        logger.error(f"Analytics report generation failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()
