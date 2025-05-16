from airflow import DAG
from cosmos.providers.dbt.core.operators import DbtRunOperator
from datetime import datetime

with DAG(
    dag_id="dbt_alocator_run",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt", "alocator"],
) as dag:

    run_dbt = DbtRunOperator(
        task_id="run_dbt",
        project_dir="/opt/airflow/dbt/alocator",
        profiles_dir="/opt/airflow/dbt/alocator"
    )
