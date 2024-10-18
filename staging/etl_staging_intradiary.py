
# IMPORTACION BIBLIOTECAS REQUERIDAS # 

import requests
import pandas as pd
import sys
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.join(current_dir, '..')
sys.path.append(project_root)
import utils.db_utils as db_utils

def extract_intraday_data():

    ''' Gets data from twelvedata API
    
    First a connection is set to our redshift schema to check if first historical upload was made or not
    
    API requests last 25 days 
    
    Due to the fact that only one ticker can be requested in every api call, this process is iterated for every ticker using a for loop
    
    Once every ticker has been requested to the API, all data is concatenated on a final dataframe used in next step transform_daily_data
    
    If request can not be done, process ends with its error code 
    
    '''

    twelve_url = db_utils.import_api_variables()['twelve_url']
    twelve_key = db_utils.import_api_variables()['twelve_key']
    tickers = db_utils.import_api_variables()['tickers']
        
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
            sys.exit("End of process")
                
    ticker_dataframe_final = pd.concat(dataframe_append, ignore_index = True)
    ticker_dataframe_final.columns = ['event_datetime', 'open_value', 'high_value', 'low_value', 'close_value', 'volume_amount', 'ticker']
    return ticker_dataframe_final

def transform_intradiary_data():

    ''' Tranforms data retrieved from API
    
    Once we get the data, this is transformed using pandas
    
    We convert columns to numeric using pandas 
    
    Also we check on table which is last event datetime for each ticker to upload only new data (Incremental process)
    
    As of to not avoid uploading data that is already on table, we use pandas to filter data 
    
    '''

    redshift_schema = db_utils.import_db_variables()['redshift_schema']
    connection = db_utils.connect_to_redshift()

    is_incremental = f"""select ticker, max(event_datetime) as last_event_datetime from "{redshift_schema}".staging_intraday_tickers group by 1"""
    max_staging_datetime_df = pd.read_sql(is_incremental, connection)

    ticker_dataframe = extract_intraday_data()
    numeric_columns = ['open_value', 'high_value', 'low_value', 'close_value', 'volume_amount']
    ticker_dataframe.loc[:, numeric_columns] = ticker_dataframe[numeric_columns].apply(pd.to_numeric, errors = 'coerce')
    ticker_dataframe['event_datetime'] = pd.to_datetime(ticker_dataframe['event_datetime'])
    ticker_dataframe.sort_values(by='event_datetime', ascending = False, inplace=True)
    ticker_dataframe_prefilter = pd.merge(ticker_dataframe, max_staging_datetime_df, on='ticker', how='left')
    ticker_dataframe_prefilter['last_event_datetime'] = ticker_dataframe_prefilter['last_event_datetime'].fillna(pd.Timestamp('2000-01-01'))
    ticker_dataframe_filter = ticker_dataframe_prefilter[ticker_dataframe_prefilter['event_datetime'] > ticker_dataframe_prefilter['last_event_datetime']].copy()
    return ticker_dataframe_filter

def load_intradiary_data():
    
    ''' Load data retrieved from API
    
    Once we get the data filtered and in proper data types we upload it to redshift using SQL Alchemy engine
    
    Upload will only occur if transform dataframe is not null (after being filtered to only keep new records)
    
    If there is no new data to upload (api not updated or any other issue) no data is uploaded and you'll get this on Airflow log
    
    Also if new data is uploaded or there is an error you'll get this on Airflow log too.
    
    '''
        
    redshift_schema = db_utils.import_db_variables()['redshift_schema']
    connection = db_utils.connect_to_redshift()
    ticker_dataframe_filter = transform_intradiary_data()
    
    if ticker_dataframe_filter.empty == False:
        
        ticker_dataframe_final = ticker_dataframe_filter.drop(columns=['last_event_datetime'])
        ticker_dataframe_final.loc[:, 'audit_datetime'] = pd.Timestamp.now()
        
        try:
            
            ticker_dataframe_final.to_sql('staging_intraday_tickers', con = connection, index=False, if_exists='append', method='multi', schema = redshift_schema)
            print("Table staging_intraday_tickers up to date. New records added")
            connection.close()

        except Exception as e:
                
            print(f"Could not update staging_intraday_tickers: {e}\n")
            connection.close()
            sys.exit("End of process")
        
    else:

        print(f"Table staging_intraday_tickers up to date. No new information to upload\n") 
        connection.close()
