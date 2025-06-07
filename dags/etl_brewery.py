from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'email': ['solano.d.castro@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False
}

with DAG('etl_brewery',
         description='ETL process for brewery data',
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False) as dag:

    bronze_process = BashOperator(
        task_id='bronze_process',
        bash_command='docker exec bronze-process python3 /app/scripts/process_bronze.py'
        )
    
    silver_process = BashOperator(
        task_id='silver_process',
        bash_command='docker exec silver-process python3 /app/scripts/process_silver.py'
        )
    
    gold_process = BashOperator(
        task_id='gold_process',
        bash_command='docker exec gold-process python3 /app/scripts/process_gold.py'
        )

    bronze_process >> silver_process >> gold_process
