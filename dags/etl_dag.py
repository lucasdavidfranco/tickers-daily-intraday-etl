
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(current_dir, '..'))

from tasks.analytics_run import analytics_run
from tasks.staging_run import staging_run

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(0)
}

with DAG(
    'tickers-etl-dag', 
    default_args=default_args, 
    schedule_interval='@daily'
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
