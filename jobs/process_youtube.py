import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date, when, count, avg, sum, desc, min, max, regexp_replace, trim, coalesce, explode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType
from pyspark.sql.dataframe import DataFrame

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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


def define_youtube_schema() -> StructType:
    """Define consistent schema for YouTube CSV data"""
    return StructType(
        [
            StructField("video_id", StringType(), True),
            StructField("trending_date", StringType(), True),
            StructField("title", StringType(), True),
            StructField("channel_title", StringType(), True),
            StructField("category_id", StringType(), True),  # Keep as string initially
            StructField("publish_time", StringType(), True),
            StructField("tags", StringType(), True),
            StructField("views", StringType(), True),  # Keep as string initially for cleaning
            StructField("likes", StringType(), True),  # Keep as string initially for cleaning
            StructField("dislikes", StringType(), True),  # Keep as string initially for cleaning
            StructField("comment_count", StringType(), True),  # Keep as string initially for cleaning
            StructField("thumbnail_link", StringType(), True),
            StructField("comments_disabled", StringType(), True),  # Keep as string initially
            StructField("ratings_disabled", StringType(), True),  # Keep as string initially
            StructField("video_error_or_removed", StringType(), True),  # Keep as string initially
            StructField("description", StringType(), True),
        ]
    )


def define_category_schema() -> StructType:
    """Define consistent schema for category mapping"""
    return StructType([StructField("category_id", IntegerType(), False), StructField("category_title", StringType(), False)])


def load_category_mapping(spark: SparkSession, raw_bucket: str, countries: list) -> DataFrame:
    """Load and combine category mappings from all countries"""
    category_dfs = []
    category_schema = define_category_schema()

    for country in countries:
        category_json_path = f"s3a://{raw_bucket}/category/country={country}/{country}_category_id.json"
        try:
            # Read JSON with error handling
            raw_json = spark.read.option("multiline", "true").json(category_json_path)

            # Check if 'items' field exists
            if "items" not in raw_json.columns:
                logger.warning(f"No 'items' field found in category JSON for {country}")
                continue

            # Extract category data
            items_df = raw_json.select(explode("items").alias("item"))

            # Validate required fields exist
            category_df = items_df.select(col("item.id").cast("int").alias("category_id"), col("item.snippet.title").alias("category_title")).filter(
                col("category_id").isNotNull() & col("category_title").isNotNull() & (col("category_id") > 0) & (col("category_title") != "")
            )

            if category_df.count() > 0:
                category_dfs.append(category_df)
                logger.info(f"Loaded {category_df.count()} categories for {country}")
            else:
                logger.warning(f"No valid categories found for {country}")

        except Exception as e:
            logger.warning(f"Could not load category JSON for {country}: {e}")
            continue

    # Combine all category DataFrames
    if not category_dfs:
        logger.warning("No category data loaded from any country")
        # Return empty DataFrame with correct schema
        return spark.createDataFrame([], category_schema)

    # Union all category DataFrames and remove duplicates
    combined_category_df = category_dfs[0]
    for df in category_dfs[1:]:
        combined_category_df = combined_category_df.union(df)

    # Remove duplicates and keep the first occurrence
    combined_category_df = combined_category_df.dropDuplicates(["category_id"])

    logger.info(f"Combined category mapping: {combined_category_df.count()} unique categories")
    return combined_category_df


def clean_and_cast_data(df: DataFrame) -> DataFrame:
    """Clean and cast data to appropriate types"""
    logger.info("Starting data cleaning and type casting...")

    # Clean and cast numeric columns
    df_cleaned = (
        df.withColumn("views_clean", regexp_replace(col("views"), r"[^\d]", "").cast(LongType()))
        .withColumn("likes_clean", regexp_replace(col("likes"), r"[^\d]", "").cast(LongType()))
        .withColumn("dislikes_clean", regexp_replace(col("dislikes"), r"[^\d]", "").cast(LongType()))
        .withColumn("comment_count_clean", regexp_replace(col("comment_count"), r"[^\d]", "").cast(LongType()))
        .withColumn("category_id_clean", regexp_replace(col("category_id"), r"[^\d]", "").cast(IntegerType()))
    )

    # Handle boolean columns
    df_cleaned = (
        df_cleaned.withColumn(
            "comments_disabled_clean",
            when(col("comments_disabled").isin("True", "true", "TRUE", "1"), True).when(col("comments_disabled").isin("False", "false", "FALSE", "0"), False).otherwise(False),
        )
        .withColumn(
            "ratings_disabled_clean", when(col("ratings_disabled").isin("True", "true", "TRUE", "1"), True).when(col("ratings_disabled").isin("False", "false", "FALSE", "0"), False).otherwise(False)
        )
        .withColumn(
            "video_error_or_removed_clean",
            when(col("video_error_or_removed").isin("True", "true", "TRUE", "1"), True).when(col("video_error_or_removed").isin("False", "false", "FALSE", "0"), False).otherwise(False),
        )
    )

    # Clean string columns
    df_cleaned = (
        df_cleaned.withColumn("title_clean", trim(col("title")))
        .withColumn("channel_title_clean", trim(col("channel_title")))
        .withColumn("tags_clean", trim(col("tags")))
        .withColumn("description_clean", trim(col("description")))
    )

    # Handle null values for numeric columns
    df_cleaned = (
        df_cleaned.withColumn("views_final", coalesce(col("views_clean"), lit(0)))
        .withColumn("likes_final", coalesce(col("likes_clean"), lit(0)))
        .withColumn("dislikes_final", coalesce(col("dislikes_clean"), lit(0)))
        .withColumn("comment_count_final", coalesce(col("comment_count_clean"), lit(0)))
        .withColumn("category_id_final", coalesce(col("category_id_clean"), lit(0)))
    )

    # Select final columns with clean names
    df_final = df_cleaned.select(
        col("video_id"),
        col("trending_date"),
        col("title_clean").alias("title"),
        col("channel_title_clean").alias("channel_title"),
        col("category_id_final").alias("category_id"),
        col("publish_time"),
        col("tags_clean").alias("tags"),
        col("views_final").alias("views"),
        col("likes_final").alias("likes"),
        col("dislikes_final").alias("dislikes"),
        col("comment_count_final").alias("comment_count"),
        col("thumbnail_link"),
        col("comments_disabled_clean").alias("comments_disabled"),
        col("ratings_disabled_clean").alias("ratings_disabled"),
        col("video_error_or_removed_clean").alias("video_error_or_removed"),
        col("description_clean").alias("description"),
        col("country"),
    )

    logger.info("Data cleaning and type casting completed")
    return df_final


def main():
    spark = get_spark_session()

    RAW_BUCKET = os.getenv("RAW_BUCKET", "youtube-trending-raw")
    PROCESSED_BUCKET = os.getenv("PROCESSED_BUCKET", "youtube-trending-processed")

    try:
        # Define schema for consistent reading
        youtube_schema = define_youtube_schema()

        # Read data from LocalStack S3
        countries = ["CA", "DE", "FR", "GB", "IN", "JP", "KR", "MX", "RU", "US"]
        all_data: list[DataFrame] = []

        for country in countries:
            s3_path = f"s3a://{RAW_BUCKET}/raw-data/country={country}/{country}videos.csv"

            try:
                # Read CSV from S3 with defined schema
                df = spark.read.csv(s3_path, header=True, schema=youtube_schema, timestampFormat="yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", multiLine=True, escape='"')
                df = df.withColumn("country", lit(country))
                all_data.append(df)
                logger.info(f"Loaded {country} data: {df.count()} rows")
            except Exception as e:
                logger.warning(f"Could not load {country} data: {e}")

        if not all_data:
            raise ValueError("No data could be loaded from S3")

        # Load category mapping using improved function
        category_mapping = load_category_mapping(spark, RAW_BUCKET, countries)

        # Union all DataFrames
        combined_df = all_data[0]
        for df in all_data[1:]:
            combined_df = combined_df.union(df)

        logger.info(f"Combined dataset: {combined_df.count()} total rows")

        # Clean and cast data types
        cleaned_df = clean_and_cast_data(combined_df)

        # Data transformation
        cleaned_df = cleaned_df.withColumn("trending_date_parsed", to_date(col("trending_date"), "yy.dd.MM"))

        # Apply business logic filters
        cleaned_df = cleaned_df.filter(
            (col("views") > 0)
            & (col("likes") >= 0)
            & (col("dislikes") >= 0)
            & (col("comment_count") >= 0)
            & (col("category_id") > 0)
            & (col("video_id").isNotNull())
            & (col("title").isNotNull())
            & (col("channel_title").isNotNull())
        )

        # Calculate engagement metrics with proper type handling
        cleaned_df = (
            cleaned_df.withColumn("total_engagement", col("likes") + col("dislikes") + col("comment_count"))
            .withColumn("engagement_rate", when(col("views") > 0, (col("total_engagement").cast(DoubleType()) / col("views").cast(DoubleType())) * 100).otherwise(0.0))
            .withColumn("like_ratio", when((col("likes") + col("dislikes")) > 0, (col("likes").cast(DoubleType()) / (col("likes") + col("dislikes")).cast(DoubleType())) * 100).otherwise(0.0))
        )

        # Join with category mapping if available
        if category_mapping.count() > 0:
            cleaned_df = cleaned_df.join(category_mapping, on="category_id", how="left")
            logger.info("Successfully joined category titles with main dataset")
        else:
            # Add null category_title column if no mapping available
            cleaned_df = cleaned_df.withColumn("category_title", lit(None).cast(StringType()))
            logger.warning("No category mapping available, added null category_title column")

        # Cache cleaned data
        cleaned_df.cache()

        # Log data quality metrics
        total_rows = cleaned_df.count()
        null_counts = {}
        for column in cleaned_df.columns:
            null_count = cleaned_df.filter(col(column).isNull()).count()
            if null_count > 0:
                null_counts[column] = null_count

        logger.info(f"Data quality check - Total rows: {total_rows}")
        if null_counts:
            logger.info(f"Null values found: {null_counts}")

        # 1. Category analysis
        category_analysis = (
            cleaned_df.groupBy("country", "category_id", "category_title")
            .agg(
                count("video_id").alias("video_count"),
                avg("views").alias("avg_views"),
                sum("views").alias("total_views"),
                avg("engagement_rate").alias("avg_engagement_rate"),
                avg("like_ratio").alias("avg_like_ratio"),
            )
            .orderBy("country", desc("total_views"))
        )

        # 2. Channel analysis
        channel_analysis = (
            cleaned_df.groupBy("country", "channel_title")
            .agg(
                count("video_id").alias("trending_count"),
                avg("views").alias("avg_views"),
                max("views").alias("max_views"),
                avg("like_ratio").alias("avg_like_ratio"),
                avg("engagement_rate").alias("avg_engagement_rate"),
            )
            .filter(col("trending_count") >= 2)
            .orderBy("country", desc("avg_views"))
        )

        # 3. Daily trends
        daily_trends = (
            cleaned_df.groupBy("country", "trending_date_parsed")
            .agg(count("video_id").alias("daily_trending_count"), avg("views").alias("avg_daily_views"), sum("views").alias("total_daily_views"), avg("engagement_rate").alias("avg_daily_engagement"))
            .orderBy("trending_date_parsed")
        )

        # Save processed data to LocalStack S3
        base_output_path = f"s3a://{PROCESSED_BUCKET}"

        # Save with proper partitioning and compression
        cleaned_df.coalesce(10).write.mode("overwrite").option("compression", "snappy").partitionBy("country").parquet(f"{base_output_path}/cleaned_youtube_data")

        category_analysis.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(f"{base_output_path}/category_analysis")

        channel_analysis.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(f"{base_output_path}/channel_analysis")

        daily_trends.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(f"{base_output_path}/daily_trends")

        # Collect summary stats
        total_videos = cleaned_df.count()
        unique_channels = cleaned_df.select("channel_title").distinct().count()
        unique_categories = cleaned_df.select("category_id").distinct().count()

        date_range = cleaned_df.agg(
            min("trending_date_parsed").alias("start_date"),
            max("trending_date_parsed").alias("end_date"),
        ).collect()[0]

        # Get category mapping stats
        category_stats = {"total_categories_mapped": category_mapping.count(), "videos_with_category_title": cleaned_df.filter(col("category_title").isNotNull()).count()}

        # Cleanup
        spark.stop()

        processing_summary = {
            "total_videos_processed": total_videos,
            "unique_channels": unique_channels,
            "unique_categories": unique_categories,
            "category_mapping_stats": category_stats,
            "date_range": {"start": str(date_range["start_date"]), "end": str(date_range["end_date"])},
            "countries_processed": countries,
            "output_bucket": PROCESSED_BUCKET,
            "data_quality": {"null_counts": null_counts, "total_rows_after_cleaning": total_rows},
        }

        logger.info(f"PySpark processing completed: {processing_summary}")
        return processing_summary

    except Exception as e:
        logger.error(f"PySpark processing failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()
