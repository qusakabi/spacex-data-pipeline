from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from etl.extract.extract_spacex import extract_to_minio
from etl.load.load_spacex import load_parquet_to_snowflake
from etl.transform.transform_spacex import transform
from etl.config.config import PARQUET_PATH

default_args = {
    'owner': 'hyojin',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

def run_transform():
    # Driver runs inside Airflow container using PySpark and connects to external Spark master
    transform("spacex_raw.json", PARQUET_PATH)

with DAG(
    dag_id='spacex_etl_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract_to_minio,
        op_args=["spacex_raw.json"]
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=run_transform,
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load_parquet_to_snowflake,
        op_args=[PARQUET_PATH]
    )

    extract_task >> transform_task >> load_task
