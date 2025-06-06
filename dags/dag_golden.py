from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Añadir el path a la carpeta de scripts externos
sys.path.append('/opt/airflow/apis')

from Golden_Layer import main as run_golden

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='golden_layer',
    default_args=default_args,
    start_date=datetime(2025, 4, 6, 6, 0),
    catchup=False,
    schedule_interval='@hourly',  
    tags=['api', 'horario']
) as dag:

    task_forvenue = PythonOperator(
        task_id='run_Golden_Layer',
        python_callable=run_golden,
    )