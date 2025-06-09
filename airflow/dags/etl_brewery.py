from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
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

    bronze_process = BashOperator(
        task_id='bronze_process',
        bash_command='docker exec spark python3 /app/scripts/process_bronze.py'
        )
    
    silver_process = BashOperator(
        task_id='silver_process',
        bash_command='docker exec spark python3 /app/scripts/process_silver.py'
        )
    
    gold_process = BashOperator(
        task_id='gold_process',
        bash_command='docker exec spark python3 /app/scripts/process_gold.py'
        )

    bronze_process >> silver_process >> gold_process