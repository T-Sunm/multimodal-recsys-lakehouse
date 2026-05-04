from __future__ import annotations

import subprocess
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

_NAMESPACES = [
    "nessie.recsys_raw",
    "nessie.recsys_silver",
    "nessie.recsys_gold",
]

_NESSIE_CODE = """\
from pyhive import hive

original_execute = hive.Cursor.execute
def dummy_execute(self, operation, parameters=None, **kwargs):
    if operation.upper().startswith("USE "):
        return
    original_execute(self, operation, parameters, **kwargs)

hive.Cursor.execute = dummy_execute
conn = hive.Connection(host="spark-thrift", port=10001, username="airflow")
hive.Cursor.execute = original_execute
cur = conn.cursor()
for ns in {namespaces!r}:
    cur.execute(f"CREATE NAMESPACE IF NOT EXISTS {{ns}}")
cur.execute("SHOW NAMESPACES IN nessie")
print("Namespaces:", cur.fetchall())
cur.close()
conn.close()
""".format(namespaces=_NAMESPACES)


def _bootstrap_minio_bucket() -> None:
    import boto3
    from botocore.exceptions import ClientError

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )
    try:
        s3.create_bucket(Bucket="datalake")
        print("Bucket 'datalake' created.")
    except ClientError as e:
        if e.response["Error"]["Code"] in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            print("Bucket 'datalake' already exists. Skipping.")
        else:
            raise


def _bootstrap_nessie() -> None:
    subprocess.run(
        ["/opt/airflow/dbt_venv/bin/python", "-c", _NESSIE_CODE],
        check=True,
    )


with DAG(
    dag_id="bootstrap_infra",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["infra"],
) as dag:

    bootstrap_minio = PythonOperator(
        task_id="bootstrap_minio_bucket",
        python_callable=_bootstrap_minio_bucket,
        retries=2,
    )

    bootstrap_nessie = PythonOperator(
        task_id="bootstrap_nessie_namespaces",
        python_callable=_bootstrap_nessie,
        retries=2,
    )

    bootstrap_minio >> bootstrap_nessie
