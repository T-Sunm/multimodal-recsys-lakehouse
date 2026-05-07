"""
Producer: Ingest RecSys Raw Data → Iceberg (recsys_raw.*)
Layer   : Ingestion
Trigger : @daily
Output  : DS_RECSYS_RAW_READY

Pipeline:
  1. ingest_recsys_raw → nessie.recsys_raw.{interactions,items,likes_views,visual_embeddings}_staging
  Sau bước 1 mới emit DS_RECSYS_RAW_READY để dbt recsys_transform có thể chạy.
"""
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from pendulum import datetime
import sys, os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from datasets import DS_RECSYS_RAW_READY

SPARK_CONF = {
    "spark.submit.deployMode": "client",
    "spark.driver.host": "airflow-worker",
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.driver.extraClassPath": "/opt/airflow/jars/*",
}

JARS = ",".join([
    "/opt/airflow/jars/hadoop-aws-3.3.4.jar",
    "/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar",
    "/opt/airflow/jars/iceberg-spark-runtime-3.5_2.12-1.10.1.jar",
    "/opt/airflow/jars/nessie-spark-extensions-3.5_2.12-0.107.2.jar",
])


def emit_recsys_raw_ready(context, result=None):
    logical_date = context.get("logical_date")
    context["outlet_events"][DS_RECSYS_RAW_READY].extra = {
        "run_date": context.get("ds") or (logical_date.to_date_string() if logical_date else None),
        "batch_id": context.get("run_id"),
    }


with DAG(
    dag_id="producer_recsys_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["layer:ingestion", "domain:recsys"],
) as dag:

    ingest = SparkSubmitOperator(
        task_id="ingest_recsys_raw_to_iceberg",
        application="/opt/spark/jobs/staging/ingest_recsys_raw.py",
        conn_id="spark_default",
        properties_file="/opt/spark/conf/spark-defaults.conf",
        jars=JARS,
        conf=SPARK_CONF,
        outlets=[DS_RECSYS_RAW_READY],
        post_execute=emit_recsys_raw_ready,
    )
