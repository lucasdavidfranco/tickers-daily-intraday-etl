
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.join(current_dir, '..')
sys.path.append(project_root)

from tasks.analytics_run import analytics_run
from tasks.staging_run import staging_run

default_args = {
    'owner': 'airflow',
    "depends_on_past": False,
    'start_date': days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=3)
}

with DAG(
    'tickers-etl-dag', 
    default_args=default_args, 
    schedule_interval='0 21 * * 1-5',
    catchup=False
) as dag:
    
    staging_task = PythonOperator(
        task_id='staging',
        python_callable=staging_run
    )

    analytics_task = PythonOperator(
        task_id='analytics',
        python_callable=analytics_run
    )

    staging_task >> analytics_task
