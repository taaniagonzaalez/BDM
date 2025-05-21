from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Añadir el path a la carpeta de scripts externos
sys.path.append('/opt/airflow/apis')

from API_YELP import main as run_yelp

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='yelp_api',
    default_args=default_args,
    start_date=datetime(2025, 4, 6, 6, 0),
    catchup=False,
    schedule_interval='@hourly',  
    tags=['api', 'horario']
) as dag:

    task_forvenue = PythonOperator(
        task_id='run_API_YELP',
        python_callable=run_yelp,
    )