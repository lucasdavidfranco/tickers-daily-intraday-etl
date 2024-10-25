
import pandas as pd
import sys
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.join(current_dir, '..')
sys.path.append(project_root)
import utils.db_utils as db_utils

def load_staging_data(read_dir=None):
    
    ''' Load data retrieved from API
    
    Once we get the data filtered and in proper data types we upload it to redshift using SQL Alchemy engine
    
    Upload will only occur if transform dataframe is not null (after being filtered to only keep new records)
    
    If there is no new data to upload (api not updated or any other issue) no data is uploaded and you'll get this on Airflow log
    
    Also if new data is uploaded or there is an error you'll get this on Airflow log too.
    
    '''
    
    redshift_schema = db_utils.import_db_variables()['redshift_schema']
    connection = db_utils.connect_to_redshift()
    
    tables = {
        "staging_daily_tickers": "transform_daily_data",
        "staging_intraday_tickers": "transform_intraday_data"
    }
        
    for table_name, parquet_file in tables.items():
        
        read_dir = read_dir or os.path.join(current_dir, "data")
        parquet_read = os.path.join(read_dir, f'{parquet_file}.parquet')
        ticker_dataframe_final = pd.read_parquet(parquet_read)
            
        if not ticker_dataframe_final.empty:
            
            ticker_dataframe_final.loc[:, 'audit_datetime'] = pd.Timestamp.now()
            ticker_dataframe_final.to_sql(table_name, con = connection, index=False, if_exists='append', method='multi', schema = redshift_schema)
            print(f"Table {table_name} up to date. New records added")

        else:

            print(f"Table {table_name} up to date. No new information to upload\n")

    connection.close()
