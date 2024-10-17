
# IMPORTACION BIBLIOTECAS REQUERIDAS # 

import pandas as pd
import requests
import sys
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.join(current_dir, '..')
sys.path.append(project_root)
import utils.db_utils as db_utils

def extract_daily_data():

    alpha_url = db_utils.import_api_variables()['alpha_url']
    alpha_key = db_utils.import_api_variables()['alpha_key']
    tickers = db_utils.import_api_variables()['tickers']
    redshift_schema = db_utils.import_db_variables()['redshift_schema']
    connection = db_utils.connect_to_redshift()
        
    # VARIABLES DE LA API DE TICKERS # 

    ticker_params = {
        'function': 'TIME_SERIES_DAILY',
        'apikey': alpha_key
    }

    dataframe_append = [] # AQUI UNIREMOS LOS RESULTADOS DE CADA REQUEST PARA HACER SOLO 1 CARGA POR VEZ A LA TABLA REDSHIFT #

    # ITERACION PARA PODER CORRER MULTIPLES REQUESTS (1 POR TICKER) #

    for ticker in tickers:
        
        print(f"Getting data for {ticker}\n")
        ticker_params['symbol'] = ticker
        is_incremental = f"""select max(event_date) as q from "{redshift_schema}".staging_daily_tickers where ticker = '{ticker}'"""
        max_staging_date = connection.execute(is_incremental).fetchone()
        print(ticker)
        
        if max_staging_date[0] is not None:
            
            ticker_params['outputsize'] = 'compact'
            
        else:
            
            ticker_params['outputsize'] = 'full'

        ticker_response = requests.get(alpha_url, params = ticker_params)
        
        if ticker_response.status_code == 200:  # VERIFICAMOS CONEXION CORRECTA A LA API # 

                raw_json = ticker_response.json()
                raw_info = raw_json.get('Time Series (Daily)', {})
                ticker_dataframe = pd.DataFrame.from_dict(raw_info, orient='index')
                ticker_dataframe.reset_index(inplace=True)
                ticker_dataframe.rename(columns={'index': 'event_date'}, inplace=True)
                ticker_dataframe.loc[:, 'ticker'] = ticker
                dataframe_append.append(ticker_dataframe)

        else:
            
            print(f"Error: Could not retrieve data (State code: {ticker_response.status_code})\n")
            sys.exit("End of process")
               
    ticker_dataframe_final = pd.concat(dataframe_append, ignore_index = True)
    ticker_dataframe_final.columns = ['event_date', 'open_value', 'high_value', 'low_value', 'close_value', 'volume_amount', 'ticker']
    return ticker_dataframe_final

def transform_daily_data():

    redshift_schema = db_utils.import_db_variables()['redshift_schema']
    connection = db_utils.connect_to_redshift()
    
    # QUERY PARA OBTENER DF QUE PERMITE DETERMINAR SI ES CARGA HISTORICA O INCREMENTAL #S

    is_incremental = f"""select ticker, max(event_date) as last_event_date from "{redshift_schema}".staging_daily_tickers group by 1"""
    max_staging_date_df = pd.read_sql(is_incremental, connection)
        
    # TRANSFORMACIONES A LA RAW DATA PARA OBTENER UN DATAFRAME CON COLUMNAS DESEADAS Y FORMATO DESEADO # 

    ticker_dataframe = extract_daily_data()
    numeric_columns = ['open_value', 'high_value', 'low_value', 'close_value', 'volume_amount']
    ticker_dataframe.loc[:, numeric_columns] = ticker_dataframe[numeric_columns].apply(pd.to_numeric, errors = 'coerce')
    ticker_dataframe['event_date'] = pd.to_datetime(ticker_dataframe['event_date']).dt.date
    ticker_dataframe_prefilter = pd.merge(ticker_dataframe, max_staging_date_df, on='ticker', how='left')
    ticker_dataframe_prefilter['last_event_date'] = ticker_dataframe_prefilter['last_event_date'].fillna(pd.Timestamp('2000-01-01'))
    ticker_dataframe_filter = ticker_dataframe_prefilter[ticker_dataframe_prefilter['event_date'] > ticker_dataframe_prefilter['last_event_date']].copy()
    return ticker_dataframe_filter

def load_daily_data():
    
    redshift_schema = db_utils.import_db_variables()['redshift_schema']
    connection = db_utils.connect_to_redshift()
    ticker_dataframe_filter = transform_daily_data()
    
    if ticker_dataframe_filter.empty == False:
        
        ticker_dataframe_final = ticker_dataframe_filter.drop(columns=['last_event_date'])
        ticker_dataframe_final.loc[:, 'audit_datetime'] = pd.Timestamp.now()
        
        try:
            
            ticker_dataframe_final.to_sql('staging_daily_tickers', con = connection, index=False, if_exists='append', method='multi', schema = redshift_schema)
            print("Data uploaded to staging daily")

        except Exception as e:
                
            print(f"Could not upload data to staging daily table: {e}\n")
            connection.close()
            sys.exit("End of process")
        
    else:

        print(f"No new information to upload\n")
