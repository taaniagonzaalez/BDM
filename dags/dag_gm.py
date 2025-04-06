from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Añadir el path a la carpeta donde está API_GM.py
sys.path.append('/opt/airflow/apis')

# Importar solo la función main (¡no se ejecuta hasta que se llama!)
from API_GM import main as run_gm

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='googlemaps_api',
    default_args=default_args,
    start_date=datetime(2025, 4, 6, 6, 0),
    catchup=False,
    schedule_interval='@hourly',
    tags=['api', 'horario']
) as dag:

    task_gm = PythonOperator(
        task_id='run_API_GM',
        python_callable=run_gm, 
    )