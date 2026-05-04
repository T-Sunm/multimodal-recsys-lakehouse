import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, IntegerType, LongType, StringType
)
from pyspark.errors.exceptions.captured import AnalysisException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
log = logging.getLogger(__name__)

BUCKET   = "datalake"
RAW_BASE = f"s3a://{BUCKET}/recsys/bronze/"
CATALOG  = "nessie"
NS       = "recsys_raw"

SCHEMA_INTERACTIONS = StructType([
    StructField("user",      IntegerType(), nullable=False),
    StructField("item",      IntegerType(), nullable=False),
    StructField("timestamp", LongType(),    nullable=False),
])

SCHEMA_ITEMS = StructType([
    StructField("item",  IntegerType(), nullable=False),
    StructField("title", StringType(),  nullable=True),
])

SCHEMA_LIKES_VIEWS = StructType([
    StructField("item",       IntegerType(), nullable=False),
    StructField("like_count", LongType(),    nullable=True),
    StructField("view_count", LongType(),    nullable=True),
])



def write_iceberg(df, table: str) -> None:
    full_table = f"{CATALOG}.{NS}.{table}"
    try:
        df.writeTo(full_table).createOrReplace()
        log.info("Table %s ingested successfully. Row count: %d.", full_table, df.count())
    except AnalysisException as e:
        log.error("Failed to write table %s. Reason: %s", full_table, str(e))
        raise


def ingest_interactions(spark: SparkSession) -> None:
    log.info("Ingesting interactions from pairs.csv.")
    df = (
        spark.read
        .schema(SCHEMA_INTERACTIONS)
        .option("header", "true")
        .csv(RAW_BASE + "pairs.csv")
    )
    write_iceberg(df, "interactions_staging")


def ingest_items(spark: SparkSession) -> None:
    log.info("Ingesting item titles from titles.csv.")
    df = (
        spark.read
        .schema(SCHEMA_ITEMS)
        .option("header", "true")
        .csv(RAW_BASE + "titles.csv")
    )
    write_iceberg(df, "items_staging")


def ingest_likes_views(spark: SparkSession) -> None:
    log.info("Ingesting likes and views from likes_and_views.txt.")
    df = (
        spark.read
        .schema(SCHEMA_LIKES_VIEWS)
        .option("sep", "\t")
        .option("header", "false")
        .csv(RAW_BASE + "likes_and_views.txt")
    )
    write_iceberg(df, "likes_views_staging")


def ingest_visual_embeddings(spark: SparkSession) -> None:
    log.info("Ingesting visual embeddings from visual_embeddings.parquet.")
    df = spark.read.parquet(RAW_BASE + "visual_embeddings.parquet")
    write_iceberg(df, "visual_embeddings_staging")


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("recsys-raw-ingest")
        .getOrCreate()
    )
    log.info("Spark session initialized. Application: recsys-raw-ingest.")

    try:
        ingest_interactions(spark)
        ingest_items(spark)
        ingest_likes_views(spark)
        ingest_visual_embeddings(spark)
        log.info("All staging tables ingested successfully.")
    except Exception as e:
        log.error("Ingestion job terminated with error: %s", str(e))
        sys.exit(1)
    finally:
        spark.stop()
        log.info("Spark session terminated.")


if __name__ == "__main__":
    main()