
# IMPORTACION BIBLIOTECAS REQUERIDAS # 

import requests
import pandas as pd
import sys
import utils

# DEFINICION DE LA CONEXION A REDSHIFT #

redshift_schema = utils.import_db_variables()['redshift_schema']
connection = utils.connect_to_redshift()
twelve_url = utils.import_api_variables()['twelve_url']
twelve_key = utils.import_api_variables()['twelve_key']
tickers = utils.import_api_variables()['tickers']
    
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
    
    is_incremental = f"""select max(event_datetime) as q from "{redshift_schema}".staging_intraday_tickers where ticker = '{ticker}'"""

    max_staging_datetime = connection.execute(is_incremental).fetchone()
    
    if max_staging_datetime[0] is not None:
        
        max_staging_datetime_value = max_staging_datetime[0]
    
    else: 
        
        max_staging_datetime_value = pd.to_datetime('1999-12-31')

    ticker_response = requests.get(twelve_url, params = ticker_params)
            
    if ticker_response.status_code == 200:  # VERIFICAMOS CONEXION CORRECTA A LA API # 

        raw_json = ticker_response.json()
        
        raw_info = raw_json.get('values')

    else:
        
        print(f"Error: Could not retrieve data (State code: {ticker_response.status_code})\n")
        
        sys.exit("End of process")
        
    # TRANSFORMACIONES A LA RAW DATA PARA OBTENER UN DATAFRAME CON COLUMNAS DESEADAS Y FORMATO DESEADO # 

    ticker_dataframe = pd.DataFrame(raw_info)

    ticker_dataframe.columns = ['event_datetime', 'open_value', 'high_value', 'low_value', 'close_value', 'volume_amount']
    
    numeric_columns = ['open_value', 'high_value', 'low_value', 'close_value', 'volume_amount']
    
    ticker_dataframe.loc[:, numeric_columns] = ticker_dataframe[numeric_columns].apply(pd.to_numeric, errors = 'coerce')

    ticker_dataframe['event_datetime'] = pd.to_datetime(ticker_dataframe['event_datetime'])

    ticker_dataframe.sort_values(by='event_datetime', ascending = False, inplace=True)

    ticker_dataframe_filter = ticker_dataframe[ticker_dataframe['event_datetime'] > max_staging_datetime_value].copy()
    
    if ticker_dataframe_filter.empty:
        
        print(f"No new information for {ticker}\n")
    
    else:

        ticker_dataframe_filter.loc[:, 'ticker'] = ticker

        ticker_dataframe_filter.loc[:, 'audit_datetime'] = pd.Timestamp.now()
        
        dataframe_append.append(ticker_dataframe_filter)

# LUEGO DE FINALIZAR EL BUCLE FOR CONCATENAMOS EN UN DATAFRAME FINAL # 

if len(dataframe_append) != 0:
    
    ticker_dataframe_final = pd.concat(dataframe_append, ignore_index = True)

    try:
        
        ticker_dataframe_final.to_sql('staging_intraday_tickers', con = connection, index=False, if_exists='append', method='multi', schema = redshift_schema)

        print("Data uploaded to staging intraday table\n")

    except Exception as e:
            
        print(f"Could not upload data to staging intraday table: {e}\n")
        
        connection.close()
        
        sys.exit("End of process")

else: 
        
    print(f"No new information of any ticker\n")
        
    connection.close()
        
    sys.exit("End of process")
