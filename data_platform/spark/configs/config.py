import os

# MinIO/S3 Configuration
BUCKET_NAME = "datalake"
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:9000")  # MinIO endpoint
S3_ACCESS_KEY = "minioadmin"
S3_SECRET_KEY = "minioadmin"

PROJECT_PREFIX = "recsys"

BRONZE_PATH  = f"s3://{BUCKET_NAME}/{PROJECT_PREFIX}/bronze/"
STAGING_PATH = f"s3://{BUCKET_NAME}/{PROJECT_PREFIX}/staging/"
INTER_PATH   = f"s3://{BUCKET_NAME}/{PROJECT_PREFIX}/intermediate/"
SILVER_PATH  = f"s3://{BUCKET_NAME}/{PROJECT_PREFIX}/silver/"
MART_PATH    = f"s3://{BUCKET_NAME}/{PROJECT_PREFIX}/mart/"

# Spark Configuration (Dùng chung cho Platform)
SPARK_CONFIGS = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.pyspark.python": "/usr/bin/python3.12",
    "spark.pyspark.driver.python": "python3",
}
