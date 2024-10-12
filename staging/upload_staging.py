
# IMPORTACION BIBLIOTECAS REQUERIDAS # 

import pandas as pd
import get_api_data as ga
import sys
import db_utils

def upload_intraday_data():

    redshift_schema = db_utils.import_db_variables()['redshift_schema']
    
    connection = db_utils.connect_to_redshift()

    is_incremental = f"""select ticker, max(event_datetime) as last_event_datetime from "{redshift_schema}".staging_intraday_tickers group by 1"""

    max_staging_datetime_df = pd.read_sql(is_incremental, connection)
        
    # TRANSFORMACIONES A LA RAW DATA PARA OBTENER UN DATAFRAME CON COLUMNAS DESEADAS Y FORMATO DESEADO # 

    ticker_dataframe = ga.get_intraday_data()

    ticker_dataframe.columns = ['event_datetime', 'open_value', 'high_value', 'low_value', 'close_value', 'volume_amount', 'ticker']
        
    numeric_columns = ['open_value', 'high_value', 'low_value', 'close_value', 'volume_amount']
        
    ticker_dataframe.loc[:, numeric_columns] = ticker_dataframe[numeric_columns].apply(pd.to_numeric, errors = 'coerce')

    ticker_dataframe['event_datetime'] = pd.to_datetime(ticker_dataframe['event_datetime'])

    ticker_dataframe.sort_values(by='event_datetime', ascending = False, inplace=True)
    
    ticker_dataframe_prefilter = pd.merge(ticker_dataframe, max_staging_datetime_df, on='ticker', how='left')
    
    ticker_dataframe_prefilter['last_event_datetime'] = ticker_dataframe_prefilter['last_event_datetime'].fillna(pd.Timestamp('2000-01-01'))

    ticker_dataframe_filter = ticker_dataframe_prefilter[ticker_dataframe_prefilter['event_datetime'] > ticker_dataframe_prefilter['last_event_datetime']].copy()
        
    if ticker_dataframe_filter.empty:
            
        print(f"No new information to upload\n")
        
    else:

        ticker_dataframe_final = ticker_dataframe_filter.drop(columns=['last_event_datetime'])
        
        ticker_dataframe_final.loc[:, 'audit_datetime'] = pd.Timestamp.now()

        try:
            
            ticker_dataframe_final.to_sql('staging_intraday_tickers', con = connection, index=False, if_exists='append', method='multi', schema = redshift_schema)

            print("Data uploaded to staging intraday table\n")

        except Exception as e:
                
            print(f"Could not upload data to staging intraday table: {e}\n")
            
            connection.close()
            
            sys.exit("End of process")

def upload_daily_data():

    redshift_schema = db_utils.import_db_variables()['redshift_schema']
    connection = db_utils.connect_to_redshift()

    is_incremental = f"""select ticker, max(event_date) as last_event_date from "{redshift_schema}".staging_daily_tickers group by 1"""

    max_staging_date_df = pd.read_sql(is_incremental, connection)
        
    # TRANSFORMACIONES A LA RAW DATA PARA OBTENER UN DATAFRAME CON COLUMNAS DESEADAS Y FORMATO DESEADO # 

    ticker_dataframe = ga.get_daily_data()

    ticker_dataframe.columns = ['event_date', 'open_value', 'high_value', 'low_value', 'close_value', 'volume_amount', 'ticker']
    
    numeric_columns = ['open_value', 'high_value', 'low_value', 'close_value', 'volume_amount']
        
    ticker_dataframe.loc[:, numeric_columns] = ticker_dataframe[numeric_columns].apply(pd.to_numeric, errors = 'coerce')

    ticker_dataframe['event_date'] = pd.to_datetime(ticker_dataframe['event_date']).dt.date
    
    ticker_dataframe_prefilter = pd.merge(ticker_dataframe, max_staging_date_df, on='ticker', how='left')
    
    ticker_dataframe_prefilter['last_event_date'] = ticker_dataframe_prefilter['last_event_date'].fillna(pd.Timestamp('2000-01-01'))

    ticker_dataframe_filter = ticker_dataframe_prefilter[ticker_dataframe_prefilter['event_date'] > ticker_dataframe_prefilter['last_event_date']].copy()
        
    if ticker_dataframe_filter.empty:
            
        print(f"No new information to upload\n")
        
    else:

        ticker_dataframe_final = ticker_dataframe_filter.drop(columns=['last_event_date'])
        
        ticker_dataframe_final.loc[:, 'audit_datetime'] = pd.Timestamp.now()

        try:
            
            ticker_dataframe_final.to_sql('staging_daily_tickers', con = connection, index=False, if_exists='append', method='multi', schema = redshift_schema)

            print("Data uploaded to staging daily table\n")

        except Exception as e:
                
            print(f"Could not upload data to staging daily table: {e}\n")
            
            connection.close()
            
            sys.exit("End of process")
            
upload_intraday_data()

upload_daily_data()
