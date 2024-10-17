
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

    twelve_url = db_utils.import_api_variables()['twelve_url']
    twelve_key = db_utils.import_api_variables()['twelve_key']
    tickers = db_utils.import_api_variables()['tickers']
        
    # VARIABLES DE LA API DE TICKERS # 

    ticker_params = {
        'apikey':  twelve_key,
        'interval': '1min',
        'outputsize': 5000
    } 

    dataframe_append = [] # AQUI UNIREMOS LOS RESULTADOS DE CADA REQUEST PARA HACER SOLO 1 CARGA POR VEZ A LA TABLA REDSHIFT #

    # ITERACION PARA PODER CORRER MULTIPLES REQUESTS (1 POR TICKER) #

    for ticker in tickers:
        
        print(f"Getting data for {ticker}\n")
        ticker_params['symbol'] = ticker
        ticker_response = requests.get(twelve_url, params = ticker_params)
        
        if ticker_response.status_code == 200:  # VERIFICAMOS CONEXION CORRECTA A LA API # 

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

    redshift_schema = db_utils.import_db_variables()['redshift_schema']
    connection = db_utils.connect_to_redshift()
    
    # QUERY PARA OBTENER DF QUE PERMITE DETERMINAR SI ES CARGA HISTORICA O INCREMENTAL #S

    is_incremental = f"""select ticker, max(event_datetime) as last_event_datetime from "{redshift_schema}".staging_intraday_tickers group by 1"""
    max_staging_datetime_df = pd.read_sql(is_incremental, connection)
        
    # TRANSFORMACIONES A LA RAW DATA PARA OBTENER UN DATAFRAME CON COLUMNAS DESEADAS Y FORMATO DESEADO # 

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
        
    redshift_schema = db_utils.import_db_variables()['redshift_schema']
    connection = db_utils.connect_to_redshift()
    ticker_dataframe_filter = transform_intradiary_data()
    
    if ticker_dataframe_filter.empty == False:
        
        ticker_dataframe_final = ticker_dataframe_filter.drop(columns=['last_event_datetime'])
        ticker_dataframe_final.loc[:, 'audit_datetime'] = pd.Timestamp.now()
        
        try:
            
            ticker_dataframe_final.to_sql('staging_intraday_tickers', con = connection, index=False, if_exists='append', method='multi', schema = redshift_schema)
            print("Data uploaded to staging intradiary")

        except Exception as e:
                
            print(f"Could not upload data to staging intraday table: {e}\n")
            connection.close()
            sys.exit("End of process")
        
    else:

        print(f"No new information to upload\n")
