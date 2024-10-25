
import pandas as pd
import sys
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.join(current_dir, '..')
sys.path.append(project_root)
import utils.db_utils as db_utils

def transform_staging_data(read_dir=None, write_dir=None):
    
    ''' Tranforms data retrieved from API
    
    Once we get the data, this is transformed using pandas
    
    We convert columns to numeric using pandas 
    
    Also we check on table which is last event date for each ticker to upload only new data (Incremental process)
    
    API request only allows us to request last 100 days (COMPACT) or full history. 
    
    As of to not avoid uploading data that is already on table, we use pandas to filter data 
    
    '''

    redshift_schema = db_utils.import_db_variables()['redshift_schema']
    connection = db_utils.connect_to_redshift()
    transformations = ['intraday', 'daily']
    
    transformations = {
        "intraday": "datetime",
        "daily": 'date'
    }
    
    current_dir = os.path.dirname(os.path.abspath(__file__))
    write_dir = write_dir or os.path.join(current_dir, "data")
    read_dir = read_dir or os.path.join(current_dir, "data")
    
    os.makedirs(write_dir, exist_ok=True)

    for transformation, period in transformations.items():
        
        parquet_read = os.path.join(read_dir, f'extract_{transformation}_data.parquet')
        ticker_dataframe = pd.read_parquet(parquet_read)
        numeric_columns = ['open_value', 'high_value', 'low_value', 'close_value', 'volume_amount']
        ticker_dataframe.loc[:, numeric_columns] = ticker_dataframe[numeric_columns].apply(pd.to_numeric, errors = 'coerce')
        is_incremental = f"""select ticker, max(event_{period}) as last_event_{period} from "{redshift_schema}".staging_{transformation}_tickers group by 1"""
        max_staging_data = pd.read_sql(is_incremental, connection)
        ticker_dataframe_prefilter = pd.merge(ticker_dataframe, max_staging_data, on='ticker', how='left')
        ticker_dataframe_prefilter[f'last_event_{period}'] = pd.to_datetime(ticker_dataframe_prefilter[f'last_event_{period}'], errors='coerce')

        if transformation == 'daily':

            ticker_dataframe_prefilter[f'event_{period}'] = pd.to_datetime(ticker_dataframe[f'event_{period}']).dt.date
            ticker_dataframe_prefilter[f'last_event_{period}'] = ticker_dataframe_prefilter[f'last_event_{period}'].fillna(pd.Timestamp('2000-01-01')).dt.date
            
        else:
            
            ticker_dataframe_prefilter[f'event_{period}'] = pd.to_datetime(ticker_dataframe[f'event_{period}'])
            ticker_dataframe_prefilter[f'last_event_{period}'] = ticker_dataframe_prefilter[f'last_event_{period}'].fillna(pd.Timestamp('2000-01-01'))
        
        ticker_dataframe_filter = ticker_dataframe_prefilter[ticker_dataframe_prefilter[f'event_{period}'] > ticker_dataframe_prefilter[f'last_event_{period}']].copy()
        ticker_dataframe_final = ticker_dataframe_filter.drop(columns=[f'last_event_{period}'])
        parquet_transform = os.path.join(write_dir, f'transform_{transformation}_data.parquet')
        ticker_dataframe_final.to_parquet(parquet_transform)
