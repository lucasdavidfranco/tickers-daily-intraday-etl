
# IMPORTACION BIBLIOTECAS REQUERIDAS # 

import requests
import pandas as pd
from datetime import datetime
import sys
import utils

# DEFINICION DE LA CONEXION A REDSHIFT #

redshift_schema = utils.import_db_variables()['redshift_schema']
connection = utils.connect_to_redshift()
alpha_url = utils.import_api_variables()['alpha_url']
alpha_key = utils.import_api_variables()['alpha_key']
tickers = utils.import_api_variables()['tickers']

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
        
        max_staging_date_value = max_staging_date[0]
        
    else:
        
        ticker_params['outputsize'] = 'full'
        
        max_staging_date_value = pd.to_datetime('1999-12-31').date()

    ticker_response = requests.get(alpha_url, params = ticker_params)
       
    if ticker_response.status_code == 200:  # VERIFICAMOS CONEXION CORRECTA A LA API # 

            raw_json = ticker_response.json()
            
            raw_info = raw_json.get('Time Series (Daily)', {})

    else:
        
        print(f"Error: Could not retrieve data (State code: {ticker_response.status_code})\n")
            
        sys.exit("End of process")
        
    # TRANSFORMACIONES A LA RAW DATA PARA OBTENER UN DATAFRAME CON COLUMNAS DESEADAS Y FORMATO DESEADO # 

    ticker_dataframe = pd.DataFrame.from_dict(raw_info, orient='index')

    ticker_dataframe.columns = ['open_value', 'high_value', 'low_value', 'close_value', 'volume_amount']

    ticker_dataframe = ticker_dataframe.apply(pd.to_numeric, errors = 'coerce')

    ticker_dataframe.reset_index(inplace=True)

    ticker_dataframe.rename(columns={'index': 'event_date'}, inplace=True)

    ticker_dataframe['event_date'] = pd.to_datetime(ticker_dataframe['event_date']).dt.date
    
    ticker_dataframe = ticker_dataframe[ticker_dataframe['event_date'] > max_staging_date_value]
    
    if ticker_dataframe.empty:
        
        print(f"No new information for {ticker}\n")
        
    else:

        ticker_dataframe.sort_values(by='event_date', ascending = False, inplace=True)

        ticker_dataframe.loc[:, 'ticker'] = ticker

        ticker_dataframe.loc[:, 'audit_datetime'] = pd.Timestamp.now()
        
        dataframe_append.append(ticker_dataframe)

# LUEGO DE FINALIZAR EL BUCLE FOR CONCATENAMOS EN UN DATAFRAME FINAL # 

if len(dataframe_append) != 0:
    
    ticker_dataframe_final = pd.concat(dataframe_append, ignore_index = True)

    try:
        
        ticker_dataframe_final.to_sql('staging_daily_tickers', con = connection, index=False, if_exists='append', method='multi', schema = redshift_schema)

        print("Data uploaded to staging daily table\n")

    except Exception as e:
            
        print(f"Could not upload data to staging  daily table: {e}\n")
        
        connection.close()
        
        sys.exit("End of process")

else: 
        
    print(f"No new information of any ticker\n")
        
    connection.close()
        
    sys.exit("End of process")
