from airflow.sdk import Asset, Dataset, DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

cvm_asset = Asset(
    name="cvm",
    description="Logica de processamento de dados do CVM",
    owner="alocador",
    tags=["cvm", "data-processing"]
)

def cvm_etl():
    print("ETL para o CVM")


with DAG(
    dag_id="cvm_etl",
    description="ETL para o CVM",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["cvm", "data-processing"]
):
    cvm_etl_task = PythonOperator(
        task_id="cvm_etl",
        python_callable=cvm_etl,
        inlets=[cvm_asset]
    )

    cvm_etl_task
    