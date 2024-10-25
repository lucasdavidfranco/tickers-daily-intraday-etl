
import pandas as pd
import requests
import sys
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.join(current_dir, '..')
sys.path.append(project_root)
import utils.db_utils as db_utils
import utils.api_utils as api_utils

def extract_daily_data(parquet_path=None):
    
    ''' Gets data from alphavantage API
    
    First a connection is set to our redshift schema to check if first historical upload was made or not
    
    With that input it uses as an api params COMPACT or FULL API request. Full requests is as of 2000-01-01
    
    Due to the fact that only one ticker can be requested in every api call, this process is iterated for every ticker using a for loop
    
    Once every ticker has been requested to the API, all data is concatenated on a final dataframe used in next step transform_daily_data
    
    If request can not be done, process ends with its error code 
    
    '''

    alpha_url = api_utils.import_api_variables()['alpha_url']
    alpha_key = api_utils.import_api_variables()['alpha_key']
    tickers = api_utils.import_api_variables()['tickers']
    redshift_schema = db_utils.import_db_variables()['redshift_schema']
    connection = db_utils.connect_to_redshift()
        
    ticker_params = {
        'function': 'TIME_SERIES_DAILY',
        'apikey': alpha_key
    }

    dataframe_append = [] 

    for ticker in tickers:
        
        ticker_params['symbol'] = ticker
        is_incremental = f"""select max(event_date) as q from "{redshift_schema}".staging_daily_tickers where ticker = '{ticker}'"""
        max_staging_date = connection.execute(is_incremental).fetchone()
        
        if max_staging_date[0] is not None:
            
            ticker_params['outputsize'] = 'compact'
            
        else:
            
            ticker_params['outputsize'] = 'full'

        ticker_response = requests.get(alpha_url, params = ticker_params)
        
        if ticker_response.status_code == 200:

                raw_json = ticker_response.json()
                raw_info = raw_json.get('Time Series (Daily)', {})
                ticker_dataframe = pd.DataFrame.from_dict(raw_info, orient='index')
                ticker_dataframe.reset_index(inplace=True)
                ticker_dataframe.rename(columns={'index': 'event_date'}, inplace=True)
                ticker_dataframe.loc[:, 'ticker'] = ticker
                dataframe_append.append(ticker_dataframe)

        else:
            
            print(f"Error: Could not retrieve data (State code: {ticker_response.status_code})\n")
            ticker_response.raise_for_status()
               
    ticker_dataframe_final = pd.concat(dataframe_append, ignore_index = True)
    ticker_dataframe_final.columns = ['event_date', 'open_value', 'high_value', 'low_value', 'close_value', 'volume_amount', 'ticker']
    if not parquet_path:
        parquet_path = os.path.join(current_dir, 'data/extract_daily_data.parquet')
    ticker_dataframe_final.to_parquet(parquet_path)
    

def extract_intraday_data(parquet_path=None):

    ''' Gets data from twelvedata API
    
    First a connection is set to our redshift schema to check if first historical upload was made or not
    
    API requests last 25 days 
    
    Due to the fact that only one ticker can be requested in every api call, this process is iterated for every ticker using a for loop
    
    Once every ticker has been requested to the API, all data is concatenated on a final dataframe used in next step transform_daily_data
    
    If request can not be done, process ends with its error code 
    
    '''

    twelve_url = api_utils.import_api_variables()['twelve_url']
    twelve_key = api_utils.import_api_variables()['twelve_key']
    tickers = api_utils.import_api_variables()['tickers']
        
    ticker_params = {
        'apikey':  twelve_key,
        'interval': '1min',
        'outputsize': 5000
    } 

    dataframe_append = []

    for ticker in tickers:
        
        ticker_params['symbol'] = ticker
        ticker_response = requests.get(twelve_url, params = ticker_params)
        
        if ticker_response.status_code == 200:

            raw_json = ticker_response.json()
            raw_info = raw_json.get('values')
            ticker_dataframe = pd.DataFrame(raw_info)
            ticker_dataframe.loc[:, 'ticker'] = ticker
            dataframe_append.append(ticker_dataframe)

        else:
            
            print(f"Error: Could not retrieve data (State code: {ticker_response.status_code})\n")
            ticker_response.raise_for_status()
                
    ticker_dataframe_final = pd.concat(dataframe_append, ignore_index = True)
    ticker_dataframe_final.columns = ['event_datetime', 'open_value', 'high_value', 'low_value', 'close_value', 'volume_amount', 'ticker']
    if not parquet_path:
        parquet_path = os.path.join(current_dir, 'data/extract_intraday_data.parquet')
    ticker_dataframe_final.to_parquet(parquet_path)
