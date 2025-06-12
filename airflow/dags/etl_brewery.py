from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='etl_brewery',
    description='ETL process for brewery data',
    schedule=None,
    default_args=default_args,
    catchup=False
) as dag:

    bronze_process = SparkSubmitOperator(
        task_id='bronze_process',
        application='/app/scripts/process_bronze.py',
        conn_id='spark_standalone_client',
        verbose=True,
        dag=dag
    )

    silver_process = SparkSubmitOperator(
        task_id='silver_process',
        application='/app/scripts/process_silver.py',
        conn_id='spark_standalone_client',
        verbose=True,
        dag=dag
    )

    gold_process = SparkSubmitOperator(
        task_id='gold_process',
        application='/app/scripts/process_gold.py',
        conn_id='spark_standalone_client',
        verbose=True,
        dag=dag
    )

    bronze_process >> silver_process >> gold_process