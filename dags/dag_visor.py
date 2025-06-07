from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_visor_script():
    try:
        result = subprocess.run(
            ["python", "/opt/airflow/Visor/visor.py"],
            check=True,
            capture_output=True,
            text=True
        )
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print("Error running visor.py")
        print(e.stderr)
        raise

with DAG(
    dag_id='run_visor_only',
    default_args=default_args,
    start_date=datetime(2025, 6, 1),
    catchup=False,
    schedule_interval='@daily',
    tags=['dashboard', 'data-prep']
) as dag:

    run_visor = PythonOperator(
        task_id='run_visor',
        python_callable=run_visor_script,  
    )
