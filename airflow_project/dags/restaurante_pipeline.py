from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='restaurante_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['demo']
) as dag:

    hello = BashOperator(
        task_id='hello_task',
        bash_command='echo "¡Hola Airflow desde el DAG!"'
    )