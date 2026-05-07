"""
Consumer: RecSys Medallion Transform (dbt via Cosmos)
Layer   : Bronze → Silver → Gold
Trigger : DS_RECSYS_RAW_READY (Dataset-driven)
Output  : DS_RECSYS_GOLD_READY

Pipeline (DbtTaskGroup — mỗi model là 1 task riêng):
  Bronze (views):
    - bronze_interactions, bronze_items, bronze_visual_embeddings
  Silver (Iceberg tables in nessie.recsys_silver):
    - silver_interactions, silver_items, silver_user_sequences, silver_visual_embeddings
  Gold (Iceberg tables in nessie.recsys_gold):
    - gold_item_features, gold_popularity_stats, gold_training_samples
"""
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import TestBehavior
from pendulum import datetime
import sys, os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from datasets import DS_RECSYS_RAW_READY, DS_RECSYS_GOLD_READY

DBT_PROJECT_PATH    = "/opt/airflow/dags/dbt/recsys_transform"
DBT_EXECUTABLE_PATH = "/opt/airflow/dbt_venv/bin/dbt"


with DAG(
    dag_id="consumer_recsys_transform",
    start_date=datetime(2024, 1, 1),
    schedule=[DS_RECSYS_RAW_READY],
    catchup=False,
    tags=["layer:lakehouse", "domain:recsys", "tool:dbt"],
) as dag:

    dbt_recsys = DbtTaskGroup(
        group_id="dbt_recsys_transform",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=ProfileConfig(
            profile_name="recsys_transform",
            target_name="dev",
            profiles_yml_filepath=f"{DBT_PROJECT_PATH}/profiles.yml",
        ),
        execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE_PATH),
        render_config=RenderConfig(test_behavior=TestBehavior.AFTER_ALL),
    )

    recsys_gold_ready = EmptyOperator(
        task_id="recsys_gold_ready",
        outlets=[DS_RECSYS_GOLD_READY],
    )

    dbt_recsys >> recsys_gold_ready
