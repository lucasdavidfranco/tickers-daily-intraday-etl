
# IMPORTACION BIBLIOTECAS REQUERIDAS # 

import requests
import pandas as pd
import sys
import db_utils
import get_api_data as ga

def get_intraday_data():

    # DEFINICION DE LA CONEXION A REDSHIFT #

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
            
        # TRANSFORMACIONES A LA RAW DATA PARA OBTENER UN DATAFRAME CON COLUMNAS DESEADAS Y FORMATO DESEADO # 
    
    ticker_dataframe_final = pd.concat(dataframe_append, ignore_index = True)

    return ticker_dataframe_final

def get_daily_data():

    # DEFINICION DE LA CONEXION A REDSHIFT #

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
            
        # TRANSFORMACIONES A LA RAW DATA PARA OBTENER UN DATAFRAME CON COLUMNAS DESEADAS Y FORMATO DESEADO # 
    
    ticker_dataframe_final = pd.concat(dataframe_append, ignore_index = True)

    return ticker_dataframe_final

def get_dimension_data():
    
    alpha_url = db_utils.import_api_variables()['alpha_url']
    alpha_key = db_utils.import_api_variables()['alpha_key']
    tickers = db_utils.import_api_variables()['tickers']
    redshift_schema = db_utils.import_db_variables()['redshift_schema']
    connection = db_utils.connect_to_redshift()
    
    ticker_params = {
        'function': 'OVERVIEW',
        'apikey':  alpha_key,
    }
    
    dataframe_append = []

    for ticker in tickers:
        
        print(f"Getting data for {ticker}\n")
        
        ticker_params['symbol'] = ticker

        ticker_response = requests.get(alpha_url, params = ticker_params)
                
        if ticker_response.status_code == 200:  # VERIFICAMOS CONEXION CORRECTA A LA API # 

            raw_json = ticker_response.json()
            
            ticker_dataframe = pd.DataFrame.from_dict(raw_json, orient='index').T
            
            dataframe_append.append(ticker_dataframe)

        else:
            
            print(f"Error: Could not retrieve data (State code: {ticker_response.status_code})\n")
            
            connection.close()
            
            sys.exit("End of process")
            
    ticker_dataframe_final = pd.concat(dataframe_append, ignore_index = True)

    return ticker_dataframe_final
