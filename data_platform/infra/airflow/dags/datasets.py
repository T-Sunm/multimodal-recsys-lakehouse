try:
    from airflow.sdk import Asset
except ImportError:
    from airflow.datasets import Dataset as Asset

# Recsys pipeline assets
URI_RECSYS_RAW_READY  = "nessie://recsys_raw"                    # sau Spark ingest recsys xong
URI_RECSYS_GOLD_READY = "nessie://recsys_gold"                   # sau dbt recsys_transform xong

DS_RECSYS_RAW_READY  = Asset(URI_RECSYS_RAW_READY)
DS_RECSYS_GOLD_READY = Asset(URI_RECSYS_GOLD_READY)
