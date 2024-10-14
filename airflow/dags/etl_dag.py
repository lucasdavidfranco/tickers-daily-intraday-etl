
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(current_dir, '../..'))

from staging.create_staging_tables import create_staging_tables
from staging.etl_staging_intradiary import *
'''
from staging.etl_staging_daily import *
'''
from analytics.create_analytics_tables import create_analytics_tables
'''
from analytics.etl_dim_analytics import *
'''
from analytics.etl_fact_analytics import etl_intradiary_analytics

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(0)
}

with DAG(
    'tickers-etl-dag', 
    default_args=default_args, 
    schedule_interval='@daily'
) as dag:
    
    task1 = PythonOperator(
        task_id='staging_creation',
        python_callable=create_staging_tables
    )

    task2 = PythonOperator(
        task_id='staging_intraday',
        python_callable=load_intradiary_data
    )
    
    '''
    task3 = PythonOperator(
        task_id='staging_run_3',
        python_callable=load_daily_data
    )
    '''

    task4 = PythonOperator(
        task_id='analytics_creation',
        python_callable=create_analytics_tables
    )
    
    '''
    task5 = PythonOperator(
        task_id='analytics_run_5',
        python_callable=load_dimension_data
    )
    '''
    
    task6 = PythonOperator(
        task_id='analytics_intraday',
        python_callable=etl_intradiary_analytics
    )
    
    '''
    task7 = PythonOperator(
        task_id='analytics_run_7',
        python_callable=etl_daily_analytics
    )
    '''

    # Definir la secuencia de las tareas
    task1 >> task2 >> task4 >> task6
