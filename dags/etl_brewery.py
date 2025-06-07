from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG('etl_spark',
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False) as dag:

    processa = BashOperator(
        task_id='executa_spark',
        bash_command='docker exec meu_projeto-spark-1 python3 /app/scripts/processa_dados.py'
    )

    processa
